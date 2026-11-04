// SPDX-License-Identifier: AGPL-3.0-only
//! The `spec` of a node's `desired_state` (`node-agent-protocol.md` section
//! 5.11).
//!
//! The snapshot is complete: every `vms` row placed on the node, ordered by
//! id. Image digests come from the [`DigestResolver`] and are cached for the
//! process lifetime, so a tag that moves never silently changes a running VM.
//!
//! An app VM carries what only the app knows: its container `port`, its
//! decrypted `env` (never logged), the parsed `health_check` and the
//! `deployment_id` that produced it. A VM of a built image also carries the
//! `build_id` it was prepared from, and its digest comes from the build's row
//! rather than from a registry (ADR 0118).
//!
//! `spec.builds` carries every build of this node that is not terminal, from
//! the snapshot of the app's source the build started from, with the plaintext
//! values of the app's environment variables it names as BuildKit secrets
//! (section 5.11, ADR 0117).
//!
//! Routes are built from the app's `domains` rows, pointing at the app's
//! running VMs on this node.

use crate::api::apps::AppSource;
use crate::app_env;
use crate::crypto::DataKey;
use crate::deploy;
use crate::{DigestResolver, NodeNetwork, store::Store};
use quoro_proto::wire::{
    BuildLimits, DesiredStatus, HealthCheck, Spec, SpecBuild, SpecIngress, SpecNetwork, SpecRoute,
    SpecTunnel, SpecVm,
};
use sqlx::sqlite::SqliteConnection;
use sqlx::{Row, SqlitePool};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

/// The resolved digests, keyed by image reference, shared by every session.
pub type DigestCache = Arc<Mutex<HashMap<String, String>>>;

/// An app VM that an image did not resolve for in this build, with the
/// resolver's error (which never holds an environment value).
#[derive(Clone, Debug)]
pub struct HeldBack {
    /// The VM left out of the snapshot.
    pub vm_id: String,
    /// The app it belongs to; `None` for a standalone VM.
    pub app_id: Option<String>,
    /// Why the resolver failed.
    pub error: String,
}

/// One built snapshot and the VMs it had to leave out.
#[derive(Clone, Debug)]
pub struct BuiltSpec {
    /// The snapshot to send.
    pub spec: Spec,
    /// App VMs held back while their image does not resolve.
    pub held_back: Vec<HeldBack>,
}

/// What an app row contributes to its VMs and routes.
struct AppInfo {
    port: u16,
    health_check: Option<HealthCheck>,
    current_deployment_id: Option<String>,
}

/// Builds the full snapshot for `node_id`.
///
/// # Errors
///
/// A database error, an undecryptable `app_env` value, an invalid app
/// `health_check`, or an image reference of a VM the node may already run that
/// the resolver could not resolve. On an error the caller keeps the snapshot it
/// already sent and retries.
///
/// A VM that is still `creating` and was not in `previous` (the snapshot this
/// session sent last) is left out while its image does not resolve: the node
/// cannot hold it yet, so leaving it out deletes nothing, and one bad
/// reference does not freeze the rest of the node.
pub async fn build_spec(
    store: &Store,
    node_id: &str,
    network: &NodeNetwork,
    resolver: &dyn DigestResolver,
    cache: &DigestCache,
    data_key: &DataKey,
    previous: Option<&Spec>,
) -> Result<BuiltSpec, String> {
    let rows = sqlx::query(
        "SELECT id, project_id, app_id, name, image, vcpus, memory_mb, disk_gb, \
         desired_status, observed_status, restart_generation FROM vms WHERE node_id = ? ORDER BY id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the desired state of {node_id}: {e}"))?;

    let apps = load_apps(store.pool()).await?;
    let deployments = deploy::deployment_ids_for_node(store, node_id)
        .await
        .map_err(|e| format!("cannot link the VMs to their deployments: {e}"))?;
    let mut conn = store
        .pool()
        .acquire()
        .await
        .map_err(|e| format!("cannot open a connection: {e}"))?;
    let mut env_cache: HashMap<String, BTreeMap<String, String>> = HashMap::new();

    let mut vms = Vec::with_capacity(rows.len());
    let mut held_back = Vec::new();
    for row in &rows {
        let vm_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let app_id: Option<String> = row.try_get("app_id").map_err(|e| e.to_string())?;
        let image: String = row.try_get("image").map_err(|e| e.to_string())?;
        // A built image never leaves the node that built it: its digest comes
        // from the build's row, never from a registry (ADR 0118).
        let build_id = crate::build::build_id_of_image(&image).map(str::to_string);
        let known_digest = match &build_id {
            Some(build_id) => {
                let digest: Option<(Option<String>,)> =
                    sqlx::query_as("SELECT image_digest FROM builds WHERE id = ?")
                        .bind(build_id)
                        .fetch_optional(&mut *conn)
                        .await
                        .map_err(|e| e.to_string())?;
                match digest.and_then(|(digest,)| digest) {
                    Some(digest) => Some(digest),
                    None => return Err(format!("the build of {image} has no image digest")),
                }
            }
            None => None,
        };
        let image_digest = match known_digest {
            Some(digest) => digest,
            None => match resolve_digest(resolver, cache, &image).await {
                Ok(digest) => digest,
                Err(e) => {
                    let sent = previous
                        .and_then(|spec| spec.vms.iter().find(|vm| vm.vm_id == vm_id))
                        .map(|vm| vm.image_digest.clone());
                    let observed: String =
                        row.try_get("observed_status").map_err(|e| e.to_string())?;
                    match sent {
                        // The node already has it: keep the digest it was given.
                        Some(digest) => digest,
                        None if observed == "creating" => {
                            tracing::warn!(vm_id, image, error = %e, "cannot resolve the image; the VM waits for it");
                            held_back.push(HeldBack {
                                vm_id,
                                app_id,
                                error: e,
                            });
                            continue;
                        }
                        None => return Err(format!("cannot resolve {image} for {vm_id}: {e}")),
                    }
                }
            },
        };
        let desired_status: String = row.try_get("desired_status").map_err(|e| e.to_string())?;
        let vcpus: i64 = row.try_get("vcpus").map_err(|e| e.to_string())?;
        let memory_mb: i64 = row.try_get("memory_mb").map_err(|e| e.to_string())?;
        let disk_gb: i64 = row.try_get("disk_gb").map_err(|e| e.to_string())?;
        let restart_generation: i64 = row
            .try_get("restart_generation")
            .map_err(|e| e.to_string())?;

        let (port, health_check, env, deployment_id) = match &app_id {
            Some(app_id) => {
                let app = apps
                    .get(app_id)
                    .ok_or_else(|| format!("app {app_id} of {vm_id} is missing"))?;
                let env = app_env_of(&mut conn, data_key, app_id, &mut env_cache).await?;
                (
                    Some(app.port),
                    app.health_check.clone(),
                    env,
                    deployments.get(&vm_id).cloned(),
                )
            }
            None => (None, None, BTreeMap::new(), None),
        };

        vms.push(SpecVm {
            vm_id,
            project_id: row.try_get("project_id").map_err(|e| e.to_string())?,
            app_id,
            deployment_id,
            name: row.try_get("name").map_err(|e| e.to_string())?,
            image,
            image_digest,
            vcpus: u32::try_from(vcpus).map_err(|e| format!("vms.vcpus: {e}"))?,
            memory_mb: u32::try_from(memory_mb).map_err(|e| format!("vms.memory_mb: {e}"))?,
            disk_gb: u32::try_from(disk_gb).map_err(|e| format!("vms.disk_gb: {e}"))?,
            port,
            env,
            health_check,
            desired_status: if desired_status == "running" {
                DesiredStatus::Running
            } else {
                DesiredStatus::Stopped
            },
            restart_generation: u64::try_from(restart_generation.max(0)).unwrap_or(0),
            stateful: false,
            lease_id: None,
            fencing_token: None,
            volumes: Vec::new(),
            build_id,
        });
    }
    let builds = build_specs(&mut conn, data_key, node_id, &mut env_cache).await?;
    drop(conn);

    let routes = build_routes(store.pool(), &apps, &vms).await?;
    let ingress = build_ingress(store.pool(), data_key, node_id).await?;

    Ok(BuiltSpec {
        spec: Spec {
            vms,
            routes,
            network: SpecNetwork {
                subnet: network.subnet.clone(),
                gateway: network.gateway.clone(),
            },
            ingress,
            builds,
        },
        held_back,
    })
}

/// `spec.builds`: every build of this node that is not terminal, from the
/// snapshot of its source, with the plaintext values of the app's environment
/// variables the source names as BuildKit secrets (section 5.11, 6.6).
///
/// It also keeps the newest build of an app when that build ran on this node
/// and ended `failed` or `cancelled`. The node keeps an app's build log and
/// cache disk only while the app appears in `spec.vms` or `spec.builds`
/// (section 6.6), and a Git app whose build failed has no VM yet: without the
/// entry, the log of the failure would be deleted as soon as the build left
/// the snapshot. The kept entry is marked `cancelled` and carries no secrets,
/// so the node never runs it again, even if it lost its row; a report about
/// it changes nothing, because the build is already terminal. It leaves the
/// snapshot when a newer build of the app starts, or when the app is deleted.
///
/// A secret name that is not an environment variable of the app is left out:
/// `startBuild` already fails such a build in `preparing`, and this is the
/// defensive path if the variable is removed later.
async fn build_specs(
    conn: &mut SqliteConnection,
    data_key: &DataKey,
    node_id: &str,
    env_cache: &mut HashMap<String, BTreeMap<String, String>>,
) -> Result<Vec<SpecBuild>, String> {
    let rows = sqlx::query(
        "SELECT b.id, b.app_id, a.project_id, b.source, b.error, \
                b.status IN ('failed', 'cancelled') AS kept \
         FROM builds b JOIN apps a ON a.id = b.app_id \
         WHERE b.node_id = ? AND ( \
             b.status NOT IN ('succeeded', 'failed', 'cancelled') \
             OR (b.status IN ('failed', 'cancelled') AND b.id = ( \
                 SELECT n.id FROM builds n \
                 WHERE n.app_id = b.app_id AND n.started_at IS NOT NULL \
                 ORDER BY n.id DESC LIMIT 1))) \
         ORDER BY b.id",
    )
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| format!("cannot read the builds of {node_id}: {e}"))?;

    let mut builds = Vec::with_capacity(rows.len());
    for row in &rows {
        let build_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
        let project_id: String = row.try_get("project_id").map_err(|e| e.to_string())?;
        let source: String = row.try_get("source").map_err(|e| e.to_string())?;
        let error: Option<String> = row.try_get("error").map_err(|e| e.to_string())?;
        let kept: bool = row.try_get("kept").map_err(|e| e.to_string())?;
        let source: AppSource = serde_json::from_str(&source)
            .map_err(|e| format!("the source snapshot of {build_id} is invalid: {e}"))?;
        let env = app_env_of(conn, data_key, &app_id, env_cache).await?;
        let mut secrets = BTreeMap::new();
        // A kept terminal build never runs again: it needs no secrets.
        let names: &[String] = if kept { &[] } else { &source.secrets };
        for name in names {
            match env.get(name) {
                Some(value) => {
                    secrets.insert(name.clone(), value.clone());
                }
                None => tracing::warn!(
                    build_id,
                    app_id,
                    name,
                    "a build secret is not an environment variable of the app"
                ),
            }
        }
        builds.push(SpecBuild {
            build_id,
            app_id,
            project_id,
            repo_url: source.repo_url,
            git_ref: source.git_ref,
            dockerfile_path: source
                .dockerfile_path
                .unwrap_or_else(|| "Dockerfile".to_string()),
            context_dir: source.context_dir.unwrap_or_else(|| ".".to_string()),
            target: source.target,
            build_args: source
                .build_args
                .into_iter()
                .map(|arg| (arg.name, arg.value))
                .collect(),
            secrets,
            limits: BuildLimits::default(),
            cancelled: kept || error.as_deref() == Some(crate::build::CANCEL_REQUESTED),
        });
    }
    Ok(builds)
}

/// Every app row, keyed by app id.
async fn load_apps(pool: &SqlitePool) -> Result<HashMap<String, AppInfo>, String> {
    let rows = sqlx::query("SELECT id, port, health_check, current_deployment_id FROM apps")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("cannot read the apps: {e}"))?;
    let mut apps = HashMap::with_capacity(rows.len());
    for row in &rows {
        let id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let port: i64 = row.try_get("port").map_err(|e| e.to_string())?;
        let health_json: String = row.try_get("health_check").map_err(|e| e.to_string())?;
        let health_check: HealthCheck = serde_json::from_str(&health_json)
            .map_err(|e| format!("apps.health_check of {id} is invalid: {e}"))?;
        let current_deployment_id: Option<String> = row
            .try_get("current_deployment_id")
            .map_err(|e| e.to_string())?;
        apps.insert(
            id,
            AppInfo {
                port: u16::try_from(port).map_err(|e| format!("apps.port: {e}"))?,
                health_check: Some(health_check),
                current_deployment_id,
            },
        );
    }
    Ok(apps)
}

/// The decrypted environment of `app_id`, cached per build. A value that does
/// not decrypt fails the whole build; the error names the variable, never the
/// value.
async fn app_env_of(
    conn: &mut SqliteConnection,
    data_key: &DataKey,
    app_id: &str,
    cache: &mut HashMap<String, BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, String> {
    if let Some(env) = cache.get(app_id) {
        return Ok(env.clone());
    }
    let loaded = app_env::load_plain(conn, data_key, app_id)
        .await
        .map_err(|e| {
            tracing::warn!(app_id, error = %e, "cannot read the app environment");
            e.to_string()
        })?;
    let env: BTreeMap<String, String> = loaded
        .into_iter()
        .map(|(name, value)| (name, value.expose().to_string()))
        .collect();
    cache.insert(app_id.to_string(), env.clone());
    Ok(env)
}

/// `spec.routes`: every domain of an app with at least one running VM on this
/// node, sorted by hostname so the snapshot is deterministic. Each route names
/// its domain (`domain_id`) so the node's `certificate.*` events can carry it.
async fn build_routes(
    pool: &SqlitePool,
    apps: &HashMap<String, AppInfo>,
    vms: &[SpecVm],
) -> Result<Vec<SpecRoute>, String> {
    let mut by_app: HashMap<&str, Vec<&SpecVm>> = HashMap::new();
    for vm in vms {
        if let Some(app_id) = &vm.app_id {
            by_app.entry(app_id.as_str()).or_default().push(vm);
        }
    }

    let rows = sqlx::query("SELECT id, hostname, app_id, tls_mode FROM domains ORDER BY hostname")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("cannot read the domains: {e}"))?;
    let mut routes = Vec::new();
    for row in &rows {
        let domain_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let hostname: String = row.try_get("hostname").map_err(|e| e.to_string())?;
        let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
        let tls_mode: String = row.try_get("tls_mode").map_err(|e| e.to_string())?;
        let Some(app_vms) = by_app.get(app_id.as_str()) else {
            continue;
        };
        let app = apps
            .get(&app_id)
            .ok_or_else(|| format!("app {app_id} of domain {hostname} is missing"))?;
        // The VM of the app's current deployment first, then the rest in the
        // snapshot's order.
        let current = app.current_deployment_id.as_deref();
        let mut ordered: Vec<&SpecVm> = app_vms
            .iter()
            .copied()
            .filter(|vm| vm.desired_status == DesiredStatus::Running)
            .collect();
        if let Some(current) = current {
            ordered.sort_by_key(|vm| vm.deployment_id.as_deref() != Some(current));
        }
        let vm_ids: Vec<String> = ordered.iter().map(|vm| vm.vm_id.clone()).collect();
        if vm_ids.is_empty() {
            continue;
        }
        routes.push(SpecRoute {
            hostname,
            app_id,
            vm_ids,
            port: app.port,
            tls_mode,
            domain_id: Some(domain_id),
        });
    }
    routes.sort_by(|a, b| a.hostname.cmp(&b.hostname));
    Ok(routes)
}

/// The `tunnel` hostnames this node serves: the domains of apps with at least
/// one running VM on the node (the same rule as [`build_routes`]). The tunnel
/// manager uses it to reconcile the provider's ingress rules and CNAMEs.
///
/// # Errors
///
/// A database error.
pub async fn tunnel_hostnames(
    pool: &SqlitePool,
    node_id: &str,
) -> Result<BTreeSet<String>, String> {
    let rows = sqlx::query(
        "SELECT DISTINCT d.hostname FROM domains d \
         JOIN vms v ON v.app_id = d.app_id \
         WHERE d.tls_mode = 'tunnel' AND v.node_id = ? AND v.desired_status = 'running' \
         ORDER BY d.hostname",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the tunnel domains of {node_id}: {e}"))?;
    rows.iter()
        .map(|row| {
            row.try_get::<String, _>("hostname")
                .map_err(|e| e.to_string())
        })
        .collect()
}

/// `spec.ingress` (section 5.11, ADR 0094): present only when the node has a
/// tunnel that exists at the provider. This is the one place the run token is
/// decrypted; a token that does not decrypt leaves the node without ingress
/// and logs the node id only.
async fn build_ingress(
    pool: &SqlitePool,
    data_key: &DataKey,
    node_id: &str,
) -> Result<Option<SpecIngress>, String> {
    let row =
        sqlx::query("SELECT tunnel_id, token_enc, origin_port FROM node_tunnels WHERE node_id = ?")
            .bind(node_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("cannot read the tunnel of {node_id}: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let tunnel_id: Option<String> = row.try_get("tunnel_id").map_err(|e| e.to_string())?;
    let token_enc: Option<Vec<u8>> = row.try_get("token_enc").map_err(|e| e.to_string())?;
    let origin_port: i64 = row.try_get("origin_port").map_err(|e| e.to_string())?;
    let (Some(tunnel_id), Some(token_enc)) = (tunnel_id, token_enc) else {
        return Ok(None);
    };
    let token = match crate::tunnel::decrypt_token(data_key, node_id, &token_enc) {
        Ok(token) => token,
        Err(error) => {
            tracing::warn!(node_id, error = %error, "cannot decrypt the tunnel run token");
            return Ok(None);
        }
    };
    Ok(Some(SpecIngress {
        tunnel: Some(SpecTunnel {
            tunnel_id,
            token: token.expose().to_string(),
            origin_port: u16::try_from(origin_port)
                .map_err(|e| format!("node_tunnels.origin_port of {node_id}: {e}"))?,
        }),
    }))
}

/// Resolves `reference`, or returns the cached digest. A VM's image must not
/// silently change under a running one, so the first resolution wins for the
/// life of the process.
async fn resolve_digest(
    resolver: &dyn DigestResolver,
    cache: &DigestCache,
    reference: &str,
) -> Result<String, String> {
    let cached = {
        let cache = cache.lock().expect("digest cache lock");
        cache.get(reference).cloned()
    };
    if let Some(digest) = cached {
        return Ok(digest);
    }
    let digest = resolver.resolve(reference).await?;
    cache
        .lock()
        .expect("digest cache lock")
        .insert(reference.to_string(), digest.clone());
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReferenceAsDigest;
    use quoro_types::{Prefix, new_id};

    async fn insert_vm(store: &Store, node_id: &str, name: &str) -> String {
        let project_id = new_id(Prefix::Project);
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&project_id)
        .bind(name)
        .bind(name)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let vm_id = new_id(Prefix::Vm);
        sqlx::query(
            "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
             image, desired_status, observed_status, ip, restart_generation, restart_count, \
             last_exit_code, created_at, updated_at) \
             VALUES (?, ?, NULL, ?, ?, 2, 512, 4, ?, 'running', 'creating', NULL, 0, 0, NULL, ?, ?)",
        )
        .bind(&vm_id)
        .bind(&project_id)
        .bind(node_id)
        .bind(name)
        .bind(format!("ghcr.io/example/{name}:1"))
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        vm_id
    }

    #[tokio::test]
    async fn the_snapshot_is_complete_and_ordered() {
        let store = Store::open_in_memory().await.unwrap();
        let node_id = new_id(Prefix::Node);
        let project_id = new_id(Prefix::Project);
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, 'p', 'p', ?, ?)",
        )
        .bind(&project_id)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at) \
             VALUES (?, 'n', '[\"node\"]', NULL, 'pending', '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, 0, NULL, ?)",
        )
        .bind(&node_id)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let a = insert_vm(&store, &node_id, "a").await;
        let b = insert_vm(&store, &node_id, "b").await;
        let network = NodeNetwork {
            subnet: "10.42.1.0/24".to_string(),
            gateway: "10.42.1.1".to_string(),
        };
        let cache = DigestCache::default();
        let key = DataKey::ephemeral();
        let built = build_spec(
            &store,
            &node_id,
            &network,
            &ReferenceAsDigest,
            &cache,
            &key,
            None,
        )
        .await
        .unwrap();
        let spec = built.spec;
        let mut ids: Vec<&str> = spec.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        ids.sort_unstable();
        let mut expected = vec![a.as_str(), b.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);
        assert_eq!(spec.network.subnet, "10.42.1.0/24");
        assert!(spec.routes.is_empty());
        assert!(cache.lock().unwrap().len() == 2);
    }

    /// Fails for every reference that names the image `b`.
    struct FailsOnB;

    impl DigestResolver for FailsOnB {
        fn resolve<'a>(
            &'a self,
            reference: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>>
        {
            Box::pin(async move {
                if reference.contains("/b:") {
                    Err("registry unreachable".to_string())
                } else {
                    Ok(format!("sha256:{reference}"))
                }
            })
        }
    }

    #[tokio::test]
    async fn an_unresolvable_image_holds_back_only_its_own_new_vm() {
        let store = Store::open_in_memory().await.unwrap();
        let node_id = new_id(Prefix::Node);
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at) \
             VALUES (?, 'n', '[\"node\"]', NULL, 'pending', '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, 0, NULL, ?)",
        )
        .bind(&node_id)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let a = insert_vm(&store, &node_id, "a").await;
        let b = insert_vm(&store, &node_id, "b").await;
        let network = NodeNetwork {
            subnet: "10.42.1.0/24".to_string(),
            gateway: "10.42.1.1".to_string(),
        };
        let key = DataKey::ephemeral();

        // A new VM waits; the other one is sent.
        let built = build_spec(
            &store,
            &node_id,
            &network,
            &FailsOnB,
            &DigestCache::default(),
            &key,
            None,
        )
        .await
        .unwrap();
        let ids: Vec<&str> = built.spec.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        assert_eq!(ids, [a.as_str()]);
        assert_eq!(built.held_back.len(), 1);
        assert_eq!(built.held_back[0].vm_id, b);

        // Once the node reported it, leaving it out would delete it: refuse.
        sqlx::query("UPDATE vms SET observed_status = 'running' WHERE id = ?")
            .bind(&b)
            .execute(store.pool())
            .await
            .unwrap();
        assert!(
            build_spec(
                &store,
                &node_id,
                &network,
                &FailsOnB,
                &DigestCache::default(),
                &key,
                None
            )
            .await
            .is_err()
        );

        // A digest this session already sent is kept.
        let mut previous = built.spec.clone();
        let mut sent = previous.vms[0].clone();
        sent.vm_id.clone_from(&b);
        sent.image_digest = "sha256:earlier".to_string();
        previous.vms.push(sent);
        let built = build_spec(
            &store,
            &node_id,
            &network,
            &FailsOnB,
            &DigestCache::default(),
            &key,
            Some(&previous),
        )
        .await
        .unwrap();
        let kept = built
            .spec
            .vms
            .iter()
            .find(|vm| vm.vm_id == b)
            .expect("b is sent");
        assert_eq!(kept.image_digest, "sha256:earlier");
    }
}

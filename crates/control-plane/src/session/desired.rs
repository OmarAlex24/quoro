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
//! `spec.volumes` carries every volume row on the node: `present` while it is
//! `creating`, `ready` or `restoring`, and `deleted` once a delete was
//! accepted (section 5.11, ADR 0145). A VM of an app with a `ready` or
//! `creating` volume here is `stateful`: it carries the app's `local` lease,
//! its fencing token and the one volume to attach, because the volume may be
//! attached to one VM at a time.
//!
//! A restore onto another node moves the app away from its node from the
//! moment it is accepted (`openapi.yaml` `restoreBackup`): while the app has a
//! `restoring` volume on another node, its VMs are left out of this node's
//! snapshot, so the node deletes them like any VM that leaves its spec. Kept
//! in, without the detached volume, they would be recreated stopped (section
//! 6.2 step 3, issue #287). The rows move to the target node when the restore
//! is `ready`.
//!
//! Routes are built from the app's `domains` rows: the domains of an app with
//! one replica point at the app's running VMs on this node, and the domains of
//! an app with more replicas go to the edge machine as `spec.edge` (ADR 0143).
//! A `tunnel` route that just moved to another machine's tunnel stays on the
//! machine it left for a short window, marked `draining_until` ([`drain`],
//! ADR 0194).
//!
//! [`drain`]: super::drain

use crate::api::apps::AppSource;
use crate::app_env;
use crate::crypto::DataKey;
use crate::deploy;
use crate::{DigestResolver, store::Store};
use base64::Engine as _;
use quoro_proto::wire::{
    BuildLimits, DesiredStatus, EdgeBackend, EdgeRoute, HealthCheck, Spec, SpecBackup,
    SpecBackupRepository, SpecBuild, SpecEdge, SpecIngress, SpecNetwork, SpecRestore, SpecRoute,
    SpecTunnel, SpecVm, SpecVolume, SpecVolumeFile, SpecVolumeState, WarmRole,
};
use sqlx::sqlite::SqliteConnection;
use sqlx::{Row, SqlitePool};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::drain::{self, Drains};
use super::mesh::{MeshLookup, build_mesh};

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
    /// `apps.replicas`: a route of an app with one replica belongs to the
    /// node that runs it, one above that to the edge (ADR 0142, ADR 0143).
    replicas: i64,
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
///
/// `mesh_lookup` supplies the session source addresses and the advertised
/// URLs that `spec.mesh` needs (ADR 0144). `drains` holds the departed
/// `tunnel` routes the machine keeps for a while (ADR 0194); a `tunnel` route
/// of `previous` that this snapshot drops can start one. `warm_copies` holds
/// the `(volume_id, app_id)` of each copy the node last reported holding as
/// a warm node, from the primary's memory (section 5.3): one that no failover
/// can need any more gets a `deleted` entry.
#[allow(clippy::too_many_arguments)]
pub async fn build_spec(
    store: &Store,
    node_id: &str,
    resolver: &dyn DigestResolver,
    cache: &DigestCache,
    data_key: &DataKey,
    previous: Option<&Spec>,
    mesh_lookup: &MeshLookup<'_>,
    drains: &Drains,
    warm_copies: &[(String, String)],
) -> Result<BuiltSpec, String> {
    // An edge-only machine runs no VM and has no guest subnet (ADR 0144); a
    // node still waiting for its subnet cannot be sent a snapshot yet.
    let (subnet, roles): (Option<String>, String) =
        sqlx::query_as("SELECT subnet, roles FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_one(store.pool())
            .await
            .map_err(|e| format!("cannot read the subnet of {node_id}: {e}"))?;
    let roles: Vec<String> = serde_json::from_str(&roles)
        .map_err(|e| format!("cannot read the roles of {node_id}: {e}"))?;
    let network = if roles.iter().any(|role| role == "node") {
        let subnet = subnet.ok_or_else(|| format!("node {node_id} has no guest subnet yet"))?;
        let gateway = crate::subnet::gateway_of(&subnet)?;
        Some(SpecNetwork { subnet, gateway })
    } else {
        None
    };

    let rows = sqlx::query(
        "SELECT id, project_id, app_id, name, image, vcpus, memory_mb, disk_gb, \
         desired_status, observed_status, restart_generation FROM vms WHERE node_id = ? ORDER BY id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the desired state of {node_id}: {e}"))?;

    let apps = load_apps(store.pool()).await?;
    let moving_away = apps_restoring_elsewhere(store.pool(), node_id).await?;
    let (volumes, attachments) = load_node_volumes(store.pool(), node_id).await?;
    let leases = load_app_leases(store.pool(), node_id).await?;
    let deployments = deploy::deployments_for_node(store, node_id)
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
        // A restore is moving the app to another node: the VM leaves this
        // node now, before its image is even looked at.
        if app_id
            .as_ref()
            .is_some_and(|app_id| moving_away.contains(app_id))
        {
            continue;
        }
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
        // A VM's digest is fixed when it is created (section 6.2): the one its
        // deployment recorded with the first snapshot that named it. Only a
        // new deployment, with a new VM id, resolves the reference again, so
        // a restart with an empty cache never moves a running VM to the
        // tag's new digest (issue #354).
        let known_digest = known_digest.or_else(|| {
            deployments
                .get(&vm_id)
                .and_then(|deployment| deployment.image_digest.clone())
        });
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
                    deployments
                        .get(&vm_id)
                        .map(|deployment| deployment.id.clone()),
                )
            }
            None => (None, None, BTreeMap::new(), None),
        };

        // A stateful VM attaches the app's volume on this node and runs only
        // while it holds the app's lease (sections 5.11, 6.4, ADR 0145).
        let (stateful, lease_id, fencing_token, attached) = match &app_id {
            Some(app_id) => match (attachments.get(app_id), leases.get(app_id)) {
                (Some((volume_id, mount_path)), Some((lease_id, token))) => (
                    true,
                    Some(lease_id.clone()),
                    Some(*token),
                    vec![SpecVolume {
                        volume_id: volume_id.clone(),
                        serial: "quoro-vol-0".to_string(),
                        mount_path: mount_path.clone(),
                    }],
                ),
                (Some((volume_id, _)), None) => {
                    tracing::warn!(
                        vm_id,
                        app_id,
                        volume_id,
                        "a volume has no lease: the VM stays stateless"
                    );
                    (false, None, None, Vec::new())
                }
                _ => (false, None, None, Vec::new()),
            },
            None => (false, None, None, Vec::new()),
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
            standby: false,
            restart_generation: u64::try_from(restart_generation.max(0)).unwrap_or(0),
            stateful,
            lease_id,
            fencing_token,
            volumes: attached,
            build_id,
        });
    }
    let builds = build_specs(&mut conn, data_key, node_id, &mut env_cache).await?;
    drop(conn);
    let (backups, restores, backup_repository) = backup_jobs(store, data_key, node_id).await?;
    // A `restoring` volume writes its file from the backup instead of creating
    // it empty (section 5.11).
    let mut volumes = volumes;
    for file in &mut volumes {
        file.restore = restores.get(&file.volume_id).cloned();
    }
    let pairs = crate::warm::load_pairs(store.pool()).await?;
    warm_volumes(store.pool(), node_id, &pairs, warm_copies, &mut volumes).await?;

    let mut routes = build_routes(store.pool(), &apps, &vms).await?;
    let ingress = build_ingress(store.pool(), data_key, node_id).await?;
    let mesh = build_mesh(store, mesh_lookup, &pairs, node_id).await?;
    // The edge's own tunnel is the one `spec.ingress` would carry for it
    // (section 5.11): one row in `node_tunnels`, decrypted once here.
    let tunnel = ingress.as_ref().and_then(|ingress| ingress.tunnel.clone());
    let mut edge = build_edge(store.pool(), node_id, tunnel).await?;
    drain_routes(
        store.pool(),
        drains,
        node_id,
        previous,
        &apps,
        &vms,
        &mut routes,
        &mut edge,
    )
    .await?;
    // The edge runs its own tunnel only for `tunnel` routes, draining ones
    // included: they are served from that tunnel until they leave.
    if let Some(edge) = &mut edge
        && !edge.routes.iter().any(|route| route.tls_mode == "tunnel")
    {
        edge.tunnel = None;
    }
    // The warm node never serves the app (ADR 0174). A standby entry is
    // `stopped`, which the routes and the drains skip already; it is added
    // after them all the same, so neither ever reads it.
    vms.extend(standby_vms(store, data_key, node_id, &pairs, &apps, resolver, cache).await?);

    Ok(BuiltSpec {
        spec: Spec {
            vms,
            routes,
            network,
            ingress,
            builds,
            mesh,
            edge,
            backup_repository,
            backups,
            volumes,
            // The voters' side of section 6.12 is sent once the primary's
            // side of section 8.8 exists; until then no machine is a voter.
            votes: None,
            witness: None,
        },
        held_back,
    })
}

/// The warm parts of `spec.volumes` on `node_id` (sections 5.11 and 6.13,
/// ADR 0173):
/// - the source's `present` volume gets `warm` with `role: source`;
/// - the warm node gets a `present` entry for the copy, with `role: target`
///   and the volume's `size_gb`;
/// - a copy the node reported holding (`reported`) that no failover can need
///   any more gets a `deleted` entry ([`crate::warm::stale_copies`]). It
///   carries no `warm`: the node removes it as its warm copy because it holds
///   that copy and no volume row of its own. A copy whose pair is only
///   incomplete gets no entry at all, and the node keeps it.
///
/// Entries stay ordered by volume id.
async fn warm_volumes(
    pool: &SqlitePool,
    node_id: &str,
    pairs: &[crate::warm::Pair],
    reported: &[(String, String)],
    volumes: &mut Vec<SpecVolumeFile>,
) -> Result<(), String> {
    for file in volumes.iter_mut() {
        if file.state != SpecVolumeState::Present {
            continue;
        }
        if let Some(pair) = pairs
            .iter()
            .find(|pair| pair.source == node_id && pair.volume_id == file.volume_id)
        {
            file.warm = Some(pair.entry(WarmRole::Source));
        }
    }
    let stale = crate::warm::stale_copies(pool, node_id, reported, pairs).await?;
    for pair in pairs.iter().filter(|pair| pair.target == node_id) {
        if volumes.iter().any(|file| file.volume_id == pair.volume_id) {
            continue;
        }
        volumes.push(SpecVolumeFile {
            volume_id: pair.volume_id.clone(),
            app_id: pair.app_id.clone(),
            size_gb: pair.size_gb,
            state: SpecVolumeState::Present,
            restore: None,
            warm: Some(pair.entry(WarmRole::Target)),
        });
    }
    for copy in stale {
        // A volume of the node's own is named already and never stale; this
        // only keeps one entry per volume.
        if volumes.iter().any(|file| file.volume_id == copy.volume_id) {
            continue;
        }
        volumes.push(SpecVolumeFile {
            volume_id: copy.volume_id,
            app_id: copy.app_id,
            size_gb: copy.size_gb,
            state: SpecVolumeState::Deleted,
            restore: None,
            warm: None,
        });
    }
    volumes.sort_by(|a, b| a.volume_id.cmp(&b.volume_id));
    Ok(())
}

/// The warm node's `standby` entries (section 5.11, ADR 0174): for each pair
/// whose warm node is `node_id`, the app's VM as its source runs it, with
/// `standby` set, `desired_status` `stopped`, the app's lease and the copy
/// attached. The node prepares its image and creates nothing.
///
/// - The VM is the one of the app's current deployment on the source, or the
///   source's newest VM of the app while none matches.
/// - Its digest is the one the source VM was given: the build's, or the one
///   its deployment recorded, and resolved only while neither exists.
/// - `env` is the app's, decrypted for this node, as for any VM entry.
/// - An entry whose digest cannot be found yet is left out with a warning,
///   never held back: a held-back VM fails its deployment, and the source VM
///   runs on.
async fn standby_vms(
    store: &Store,
    data_key: &DataKey,
    node_id: &str,
    pairs: &[crate::warm::Pair],
    apps: &HashMap<String, AppInfo>,
    resolver: &dyn DigestResolver,
    cache: &DigestCache,
) -> Result<Vec<SpecVm>, String> {
    let mut vms = Vec::new();
    let mut env_cache: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    for pair in pairs.iter().filter(|pair| pair.target == node_id) {
        let rows = sqlx::query(
            "SELECT id, project_id, name, image, vcpus, memory_mb, disk_gb, restart_generation \
             FROM vms WHERE app_id = ? AND node_id = ? ORDER BY id",
        )
        .bind(&pair.app_id)
        .bind(&pair.source)
        .fetch_all(store.pool())
        .await
        .map_err(|e| format!("cannot read the source VM of {}: {e}", pair.app_id))?;
        let Some(app) = apps.get(&pair.app_id) else {
            continue;
        };
        let deployments = deploy::deployments_for_node(store, &pair.source)
            .await
            .map_err(|e| format!("cannot link the source VMs to their deployments: {e}"))?;
        let mut current = None;
        for row in &rows {
            let vm_id: String = row.try_get("id").map_err(|e| e.to_string())?;
            if deployments.get(&vm_id).is_some_and(|deployment| {
                app.current_deployment_id.as_deref() == Some(deployment.id.as_str())
            }) {
                current = Some(row);
                break;
            }
        }
        let Some(row) = current.or_else(|| rows.last()) else {
            continue;
        };
        let vm_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let image: String = row.try_get("image").map_err(|e| e.to_string())?;
        let build_id = crate::build::build_id_of_image(&image).map(str::to_string);
        let deployment = deployments.get(&vm_id);
        let known = match &build_id {
            Some(build_id) => {
                let digest: Option<Option<String>> =
                    sqlx::query_scalar("SELECT image_digest FROM builds WHERE id = ?")
                        .bind(build_id)
                        .fetch_optional(store.pool())
                        .await
                        .map_err(|e| e.to_string())?;
                digest.flatten()
            }
            None => None,
        }
        .or_else(|| deployment.and_then(|deployment| deployment.image_digest.clone()));
        let image_digest = match known {
            Some(digest) => digest,
            None if build_id.is_some() => {
                tracing::warn!(vm_id, image, "the build of a standby VM has no digest yet");
                continue;
            }
            None => match resolve_digest(resolver, cache, &image).await {
                Ok(digest) => digest,
                Err(e) => {
                    tracing::warn!(vm_id, image, error = %e, "cannot resolve the image of a standby VM yet");
                    continue;
                }
            },
        };
        let vcpus: i64 = row.try_get("vcpus").map_err(|e| e.to_string())?;
        let memory_mb: i64 = row.try_get("memory_mb").map_err(|e| e.to_string())?;
        let disk_gb: i64 = row.try_get("disk_gb").map_err(|e| e.to_string())?;
        let restart_generation: i64 = row
            .try_get("restart_generation")
            .map_err(|e| e.to_string())?;
        // A connection only for the values, as the other queries here use
        // the pool.
        let env = {
            let mut conn = store
                .pool()
                .acquire()
                .await
                .map_err(|e| format!("cannot open a connection: {e}"))?;
            app_env_of(&mut conn, data_key, &pair.app_id, &mut env_cache).await?
        };
        vms.push(SpecVm {
            vm_id,
            project_id: row.try_get("project_id").map_err(|e| e.to_string())?,
            app_id: Some(pair.app_id.clone()),
            deployment_id: deployment.map(|deployment| deployment.id.clone()),
            name: row.try_get("name").map_err(|e| e.to_string())?,
            image,
            image_digest,
            vcpus: u32::try_from(vcpus).map_err(|e| format!("vms.vcpus: {e}"))?,
            memory_mb: u32::try_from(memory_mb).map_err(|e| format!("vms.memory_mb: {e}"))?,
            disk_gb: u32::try_from(disk_gb).map_err(|e| format!("vms.disk_gb: {e}"))?,
            port: Some(app.port),
            env,
            health_check: app.health_check.clone(),
            desired_status: DesiredStatus::Stopped,
            standby: true,
            restart_generation: u64::try_from(restart_generation.max(0)).unwrap_or(0),
            stateful: true,
            lease_id: Some(pair.lease_id.clone()),
            fencing_token: Some(pair.fencing_token),
            volumes: vec![SpecVolume {
                volume_id: pair.volume_id.clone(),
                serial: "quoro-vol-0".to_string(),
                mount_path: pair.mount_path.clone(),
            }],
            build_id,
        });
    }
    Ok(vms)
}

/// `spec.backups`, `spec.volumes[].restore` and `spec.backup_repository`
/// (section 5.11, ADR 0147).
///
/// Every `queued` or `running` volume backup of `node_id` carries the app's
/// `id_key` and `data_key`, never the repository key: the repository key is
/// decrypted from `backup_repositories.repo_key_enc` for this snapshot and
/// only the two derived source keys leave the process. `vm_id` is the app's VM
/// on this node, which a `pause_copy` snapshot pauses. A `restoring` volume
/// carries the same keys in its `restore` object, with the manifest the seal
/// recorded.
///
/// The repository entry is present while the node has a backup job or a
/// restore (`node-agent-protocol.md` section 5.11).
async fn backup_jobs(
    store: &Store,
    data_key: &DataKey,
    node_id: &str,
) -> Result<
    (
        Vec<SpecBackup>,
        HashMap<String, SpecRestore>,
        Option<SpecBackupRepository>,
    ),
    String,
> {
    let rows = sqlx::query(
        "SELECT b.id, b.app_id, b.volume_id, b.fencing_token, b.repository_id, \
                (SELECT v.id FROM vms v \
                 WHERE v.app_id = b.app_id AND v.node_id = b.node_id \
                 ORDER BY v.id DESC LIMIT 1) AS vm_id \
         FROM backups b \
         WHERE b.node_id = ? AND b.status IN ('queued', 'running') \
           AND b.source_kind = 'volume' \
         ORDER BY b.id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the backups of {node_id}: {e}"))?;
    let restores = sqlx::query(
        "SELECT v.id AS volume_id, v.app_id, b.id AS backup_id, b.manifest_key, \
                b.manifest_digest, b.logical_bytes, b.repository_id \
         FROM volumes v JOIN backups b ON b.id = v.restored_from_backup_id \
         WHERE v.node_id = ? AND v.state = 'restoring' \
         ORDER BY v.id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the restores of {node_id}: {e}"))?;
    if rows.is_empty() && restores.is_empty() {
        return Ok((Vec::new(), HashMap::new(), None));
    }
    let first: String = if let Some(row) = rows.first() {
        row.try_get("repository_id").map_err(|e| e.to_string())?
    } else {
        restores[0]
            .try_get("repository_id")
            .map_err(|e| e.to_string())?
    };
    let repository = sqlx::query(
        "SELECT id, provider_id, prefix, repo_key_enc FROM backup_repositories WHERE id = ?",
    )
    .bind(&first)
    .fetch_optional(store.pool())
    .await
    .map_err(|e| format!("cannot read repository {first}: {e}"))?
    .ok_or_else(|| format!("the repository {first} of a backup job is missing"))?;
    let repository_id: String = repository.try_get("id").map_err(|e| e.to_string())?;
    let provider_id: String = repository
        .try_get("provider_id")
        .map_err(|e| e.to_string())?;
    let prefix: String = repository.try_get("prefix").map_err(|e| e.to_string())?;
    let repo_key_enc: Vec<u8> = repository
        .try_get("repo_key_enc")
        .map_err(|e| e.to_string())?;
    let target = crate::backups::repo::target_of_provider(
        store,
        data_key,
        &repository_id,
        &provider_id,
        &prefix,
    )
    .await
    .map_err(|e| {
        tracing::warn!(repository_id = %repository_id, error = ?e, "cannot reach the backup repository");
        format!("cannot reach the backup repository {repository_id}: {:?}", e.code)
    })?;
    let repo_key = crate::backups::decrypt_repo_key(data_key, &repository_id, &repo_key_enc)
        .map_err(|e| {
            tracing::warn!(repository_id = %repository_id, "cannot decrypt the repository key");
            format!("cannot decrypt the repository key: {:?}", e.code)
        })?;

    let mut backups = Vec::with_capacity(rows.len());
    for row in &rows {
        let backup_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
        let volume_id: String = row.try_get("volume_id").map_err(|e| e.to_string())?;
        let fencing_token: i64 = row.try_get("fencing_token").map_err(|e| e.to_string())?;
        let vm_id: Option<String> = row.try_get("vm_id").map_err(|e| e.to_string())?;
        let keys = repo_key.source_keys(&app_id);
        backups.push(SpecBackup {
            backup_id,
            app_id,
            volume_id,
            vm_id,
            fencing_token: u64::try_from(fencing_token)
                .map_err(|e| format!("backups.fencing_token: {e}"))?,
            id_key: base64::engine::general_purpose::STANDARD.encode(keys.id_key.as_bytes()),
            data_key: base64::engine::general_purpose::STANDARD.encode(keys.data_key.as_bytes()),
        });
    }
    let mut restore_entries = HashMap::with_capacity(restores.len());
    for row in &restores {
        let volume_id: String = row.try_get("volume_id").map_err(|e| e.to_string())?;
        let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
        let backup_id: String = row.try_get("backup_id").map_err(|e| e.to_string())?;
        let manifest_key: Option<String> =
            row.try_get("manifest_key").map_err(|e| e.to_string())?;
        let manifest_digest: Option<String> =
            row.try_get("manifest_digest").map_err(|e| e.to_string())?;
        let logical_bytes: Option<i64> = row.try_get("logical_bytes").map_err(|e| e.to_string())?;
        let (Some(manifest_key), Some(manifest_digest)) = (manifest_key, manifest_digest) else {
            return Err(format!(
                "the backup {backup_id} of volume {volume_id} has no manifest"
            ));
        };
        let keys = repo_key.source_keys(&app_id);
        restore_entries.insert(
            volume_id,
            SpecRestore {
                backup_id,
                manifest_key,
                manifest_digest,
                logical_bytes: logical_bytes
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .unwrap_or(0),
                id_key: base64::engine::general_purpose::STANDARD.encode(keys.id_key.as_bytes()),
                data_key: base64::engine::general_purpose::STANDARD
                    .encode(keys.data_key.as_bytes()),
            },
        );
    }
    let repository = if needs_backup_repository(&backups, &restore_entries) {
        Some(SpecBackupRepository {
            repository_id,
            endpoint: target.endpoint,
            region: target.region,
            bucket: target.bucket,
            root: target.root,
            access_key_id: target.access_key_id,
            secret_access_key: target.secret_access_key.expose().to_string(),
        })
    } else {
        None
    };
    Ok((backups, restore_entries, repository))
}

/// Whether `spec.backup_repository` must be sent: while the node has a backup
/// job or a volume it restores from a backup (section 5.11).
fn needs_backup_repository(
    backups: &[SpecBackup],
    restores: &HashMap<String, SpecRestore>,
) -> bool {
    !backups.is_empty() || !restores.is_empty()
}

/// The apps with a volume `restoring` on a node other than `node_id`: a
/// restore is moving them there, so their VMs have left `node_id`
/// (`openapi.yaml` `restoreBackup`). A restore onto the same node is not a
/// move and leaves the app's VMs where they are.
async fn apps_restoring_elsewhere(
    pool: &SqlitePool,
    node_id: &str,
) -> Result<HashSet<String>, String> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT app_id FROM volumes WHERE state = 'restoring' AND node_id <> ?",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the restores away from {node_id}: {e}"))?;
    Ok(rows.into_iter().map(|(app_id,)| app_id).collect())
}

/// The volume entries for `node_id` and the volumes its VMs may attach:
/// `app_id` to `(volume_id, mount_path)` for the app's `ready` or `creating`
/// volume here (section 5.11, ADR 0145). At most one volume per app is
/// attached, so the first by id wins.
async fn load_node_volumes(
    pool: &SqlitePool,
    node_id: &str,
) -> Result<(Vec<SpecVolumeFile>, HashMap<String, (String, String)>), String> {
    let rows = sqlx::query(
        "SELECT id, app_id, size_gb, mount_path, state FROM volumes \
         WHERE node_id = ? ORDER BY id",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the volumes of {node_id}: {e}"))?;
    let mut files = Vec::new();
    let mut attachments = HashMap::new();
    for row in &rows {
        let volume_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
        let size_gb: i64 = row.try_get("size_gb").map_err(|e| e.to_string())?;
        let mount_path: String = row.try_get("mount_path").map_err(|e| e.to_string())?;
        let state: String = row.try_get("state").map_err(|e| e.to_string())?;
        let spec_state = match state.as_str() {
            "creating" | "ready" | "restoring" => SpecVolumeState::Present,
            "deleting" => SpecVolumeState::Deleted,
            // A `failed` volume left no file behind; a `detached` one stays
            // untouched on the node and is reported `orphaned` (section 6.7).
            _ => continue,
        };
        // Only a `creating` or `ready` volume is attached (section 6.4: the
        // volume must be `ready` before a VM starts, and a `creating` one
        // turns ready on the same node).
        if matches!(state.as_str(), "creating" | "ready") {
            attachments
                .entry(app_id.clone())
                .or_insert((volume_id.clone(), mount_path));
        }
        files.push(SpecVolumeFile {
            volume_id,
            app_id,
            size_gb: u32::try_from(size_gb).map_err(|e| format!("volumes.size_gb: {e}"))?,
            state: spec_state,
            restore: None,
            warm: None,
        });
    }
    Ok((files, attachments))
}

/// The app leases `node_id` holds, `app_id` to `(lease_id, token)` (sections
/// 5.12 and 8.6). A lease held by another node never makes a VM here stateful.
async fn load_app_leases(
    pool: &SqlitePool,
    node_id: &str,
) -> Result<HashMap<String, (String, u64)>, String> {
    let rows = sqlx::query(
        "SELECT id, resource_id, fencing_token FROM leases \
         WHERE resource_type = 'app' AND holder_node_id = ? AND revoked_at IS NULL",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the app leases of {node_id}: {e}"))?;
    let mut leases = HashMap::new();
    for row in &rows {
        let lease_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let app_id: String = row.try_get("resource_id").map_err(|e| e.to_string())?;
        let token: i64 = row.try_get("fencing_token").map_err(|e| e.to_string())?;
        let token = u64::try_from(token).map_err(|e| format!("leases.fencing_token: {e}"))?;
        leases.insert(app_id, (lease_id, token));
    }
    Ok(leases)
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
        "SELECT b.id, b.app_id, a.project_id, b.source, b.error, b.refresh_base, \
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
        let refresh_base: bool = row.try_get("refresh_base").map_err(|e| e.to_string())?;
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
            refresh_base,
        });
    }
    Ok(builds)
}

/// Every app row, keyed by app id.
async fn load_apps(pool: &SqlitePool) -> Result<HashMap<String, AppInfo>, String> {
    let rows =
        sqlx::query("SELECT id, port, health_check, current_deployment_id, replicas FROM apps")
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
        let replicas: i64 = row.try_get("replicas").map_err(|e| e.to_string())?;
        apps.insert(
            id,
            AppInfo {
                port: u16::try_from(port).map_err(|e| format!("apps.port: {e}"))?,
                health_check: Some(health_check),
                current_deployment_id,
                replicas,
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

/// `spec.routes`: every domain of an app with one replica that has at least
/// one running VM on this node, sorted by hostname so the snapshot is
/// deterministic. Each route names its domain (`domain_id`) so the node's
/// `certificate.*` events can carry it. A domain of an app with more than one
/// replica goes to the edge instead (`spec.edge`, ADR 0143).
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
        let app = apps
            .get(&app_id)
            .ok_or_else(|| format!("app {app_id} of domain {hostname} is missing"))?;
        // A domain of a replicated app is served by the edge, never by the
        // node that happens to run one of its instances.
        if app.replicas != 1 {
            continue;
        }
        let Some(app_vms) = by_app.get(app_id.as_str()) else {
            continue;
        };
        let vm_ids = running_vm_ids(app, app_vms);
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
            draining_until: None,
        });
    }
    routes.sort_by(|a, b| a.hostname.cmp(&b.hostname));
    Ok(routes)
}

/// The ids of `app_vms` that are meant to run: the VM of the app's current
/// deployment first, then the rest in the snapshot's order.
fn running_vm_ids(app: &AppInfo, app_vms: &[&SpecVm]) -> Vec<String> {
    let mut ordered: Vec<&SpecVm> = app_vms
        .iter()
        .copied()
        .filter(|vm| vm.desired_status == DesiredStatus::Running)
        .collect();
    if let Some(current) = app.current_deployment_id.as_deref() {
        ordered.sort_by_key(|vm| vm.deployment_id.as_deref() != Some(current));
    }
    ordered.iter().map(|vm| vm.vm_id.clone()).collect()
}

/// The departed `tunnel` routes this machine keeps for a while (section 5.11
/// "Draining tunnel routes", ADR 0194): added to `routes` on a node and to
/// `edge` on the edge machine, each marked `draining_until`.
///
/// First, every `tunnel` route of `previous` that is not draining and that
/// this snapshot no longer carries starts draining when it moved to another
/// machine's tunnel ([`drain::departed`]). Then every open drain of the
/// machine that still has a backend is added: on a node with the app's VMs
/// here that are meant to run, which stay instances of the app; on the edge
/// with the app's running instances. A hostname the machine serves as usual
/// is never a draining route too, and a drain left with no backend is not
/// sent.
#[allow(clippy::too_many_arguments)]
async fn drain_routes(
    pool: &SqlitePool,
    drains: &Drains,
    node_id: &str,
    previous: Option<&Spec>,
    apps: &HashMap<String, AppInfo>,
    vms: &[SpecVm],
    routes: &mut Vec<SpecRoute>,
    edge: &mut Option<SpecEdge>,
) -> Result<(), String> {
    let mut normal: HashSet<String> = routes.iter().map(|route| route.hostname.clone()).collect();
    if let Some(edge) = edge.as_ref() {
        normal.extend(edge.routes.iter().map(|route| route.hostname.clone()));
    }
    let now = time::OffsetDateTime::now_utc();
    if let Some(previous) = previous {
        let sent = previous
            .routes
            .iter()
            .filter(|route| route.tls_mode == "tunnel" && route.draining_until.is_none())
            .map(|route| &route.hostname)
            .chain(
                previous
                    .edge
                    .iter()
                    .flat_map(|edge| edge.routes.iter())
                    .filter(|route| route.tls_mode == "tunnel" && route.draining_until.is_none())
                    .map(|route| &route.hostname),
            );
        for hostname in sent {
            if normal.contains(hostname) {
                continue;
            }
            if let Some(app_id) = drain::departed(pool, node_id, hostname).await? {
                drains.start(node_id, hostname, &app_id, now);
            }
        }
    }

    let is_edge = edge.is_some();
    for drain in drain::live(pool, drains, node_id, is_edge, &normal, now).await? {
        let Some(app) = apps.get(&drain.app_id) else {
            continue;
        };
        match edge.as_mut() {
            Some(edge) => {
                let backends = running_backends(pool, &drain.app_id, app.port).await?;
                let Some(health_check) = app.health_check.clone() else {
                    continue;
                };
                if backends.is_empty() {
                    continue;
                }
                edge.routes.push(EdgeRoute {
                    hostname: drain.hostname,
                    domain_id: drain.domain_id,
                    app_id: drain.app_id,
                    tls_mode: "tunnel".to_string(),
                    health_check,
                    backends,
                    draining_until: Some(drain.until),
                });
            }
            None => {
                let app_vms: Vec<&SpecVm> = vms
                    .iter()
                    .filter(|vm| vm.app_id.as_deref() == Some(drain.app_id.as_str()))
                    .collect();
                let vm_ids = running_vm_ids(app, &app_vms);
                if vm_ids.is_empty() {
                    continue;
                }
                routes.push(SpecRoute {
                    hostname: drain.hostname,
                    app_id: drain.app_id,
                    vm_ids,
                    port: app.port,
                    tls_mode: "tunnel".to_string(),
                    domain_id: Some(drain.domain_id),
                    draining_until: Some(drain.until),
                });
            }
        }
    }
    routes.sort_by(|a, b| a.hostname.cmp(&b.hostname));
    if let Some(edge) = edge.as_mut() {
        edge.routes.sort_by(|a, b| a.hostname.cmp(&b.hostname));
    }
    Ok(())
}

/// One backend per instance of `app_id` that is meant to run and has a guest
/// address, ordered by VM id: the backends of a draining edge route.
async fn running_backends(
    pool: &SqlitePool,
    app_id: &str,
    port: u16,
) -> Result<Vec<EdgeBackend>, String> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT id, node_id, ip FROM vms \
         WHERE app_id = ? AND desired_status = 'running' AND ip IS NOT NULL ORDER BY id",
    )
    .bind(app_id)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the instances of {app_id}: {e}"))?;
    Ok(rows
        .into_iter()
        .map(|(vm_id, node_id, address)| EdgeBackend {
            vm_id,
            node_id,
            address,
            port,
        })
        .collect())
}

/// `spec.edge` (section 5.11, ADR 0143): the routes the edge machine serves
/// with remote backends. `None` on every other machine, including one whose
/// role is `edge` but that the rule in [`edge_node_id`] does not pick.
///
/// One route per `domains` row whose app has more than one replica, ordered by
/// hostname, with the app's health check and one backend per instance that is
/// meant to run and has a guest address, ordered by VM id. A route with no
/// backend is still sent: the edge answers 502 for it (ADR 0143). `tunnel` is the
/// machine's own tunnel; [`build_spec`] keeps it only when at least one route,
/// draining ones included, is a `tunnel` route.
///
/// [`edge_node_id`]: super::mesh::edge_node_id
async fn build_edge(
    pool: &SqlitePool,
    node_id: &str,
    tunnel: Option<SpecTunnel>,
) -> Result<Option<SpecEdge>, String> {
    if super::mesh::edge_node_id(pool).await?.as_deref() != Some(node_id) {
        return Ok(None);
    }
    let apps = load_apps(pool).await?;
    let rows = sqlx::query(
        "SELECT d.id AS domain_id, d.hostname, d.app_id, d.tls_mode, \
                v.id AS vm_id, v.node_id AS vm_node_id, v.ip \
         FROM domains d JOIN apps a ON a.id = d.app_id \
         LEFT JOIN vms v ON v.app_id = d.app_id AND v.desired_status = 'running' \
             AND v.ip IS NOT NULL \
         WHERE a.replicas > 1 \
         ORDER BY d.hostname, v.id",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the edge routes of {node_id}: {e}"))?;

    let mut routes: Vec<EdgeRoute> = Vec::new();
    for row in &rows {
        let domain_id: String = row.try_get("domain_id").map_err(|e| e.to_string())?;
        let new_route = match routes.last() {
            Some(route) => route.domain_id != domain_id,
            None => true,
        };
        if new_route {
            let hostname: String = row.try_get("hostname").map_err(|e| e.to_string())?;
            let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
            let tls_mode: String = row.try_get("tls_mode").map_err(|e| e.to_string())?;
            let app = apps
                .get(&app_id)
                .ok_or_else(|| format!("app {app_id} of domain {hostname} is missing"))?;
            let Some(health_check) = app.health_check.clone() else {
                return Err(format!(
                    "app {app_id} of domain {hostname} has no health check"
                ));
            };
            routes.push(EdgeRoute {
                hostname,
                domain_id,
                app_id,
                tls_mode,
                health_check,
                backends: Vec::new(),
                draining_until: None,
            });
        }
        let vm_id: Option<String> = row.try_get("vm_id").map_err(|e| e.to_string())?;
        let Some(vm_id) = vm_id else {
            continue;
        };
        let vm_node_id: String = row.try_get("vm_node_id").map_err(|e| e.to_string())?;
        let address: String = row.try_get("ip").map_err(|e| e.to_string())?;
        let app_id: String = row
            .try_get::<String, _>("app_id")
            .map_err(|e| e.to_string())?;
        let app = apps
            .get(&app_id)
            .ok_or_else(|| format!("app {app_id} of {vm_id} is missing"))?;
        if let Some(route) = routes.last_mut() {
            route.backends.push(EdgeBackend {
                vm_id,
                node_id: vm_node_id,
                address,
                port: app.port,
            });
        }
    }

    Ok(Some(SpecEdge { routes, tunnel }))
}

/// The `tunnel` hostnames this machine's tunnel serves: on a node, the domains
/// of apps with one replica and at least one running VM on the node (the same
/// rule as [`build_routes`]); on the edge, also every `tunnel` domain of an app
/// with more than one replica, which the edge answers itself. The tunnel
/// manager uses it to reconcile the provider's ingress rules and CNAMEs. It
/// leaves out the hostnames the machine only drains ([`drain`], ADR 0194):
/// those keep their ingress rule but never their CNAME.
///
/// [`drain`]: super::drain
///
/// # Errors
///
/// A database error.
pub async fn tunnel_hostnames(
    pool: &SqlitePool,
    node_id: &str,
) -> Result<BTreeSet<String>, String> {
    let is_edge = super::mesh::edge_node_id(pool).await?.as_deref() == Some(node_id);
    let one_replica = "SELECT 1 FROM vms v WHERE v.app_id = d.app_id AND v.node_id = ?1 \
                       AND v.desired_status = 'running'";
    let sql = if is_edge {
        format!(
            "SELECT DISTINCT d.hostname FROM domains d JOIN apps a ON a.id = d.app_id \
             WHERE d.tls_mode = 'tunnel' \
               AND (a.replicas > 1 OR (a.replicas = 1 AND EXISTS ({one_replica}))) \
             ORDER BY d.hostname"
        )
    } else {
        format!(
            "SELECT DISTINCT d.hostname FROM domains d JOIN apps a ON a.id = d.app_id \
             WHERE d.tls_mode = 'tunnel' AND a.replicas = 1 AND EXISTS ({one_replica}) \
             ORDER BY d.hostname"
        )
    };
    let rows = sqlx::query(&sql)
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
    use quoro_proto::wire::SpecWarm;
    use quoro_types::{Prefix, new_id};

    /// A mesh lookup that knows no session address and no advertised URL, so
    /// these snapshots have no `spec.mesh`.
    fn no_mesh_lookup() -> MeshLookup<'static> {
        fn none(_: &str) -> Option<std::net::IpAddr> {
            None
        }
        MeshLookup {
            session_ip: &none,
            advertised_urls: Vec::new(),
        }
    }

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
             log_seq, last_seen_at, created_at, subnet) \
             VALUES (?, 'n', '[\"node\"]', NULL, 'pending', '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, 0, NULL, ?, '10.42.1.0/24')",
        )
        .bind(&node_id)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let a = insert_vm(&store, &node_id, "a").await;
        let b = insert_vm(&store, &node_id, "b").await;
        let cache = DigestCache::default();
        let key = DataKey::ephemeral();
        let built = build_spec(
            &store,
            &node_id,
            &ReferenceAsDigest,
            &cache,
            &key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
        )
        .await
        .unwrap();
        let spec = built.spec;
        let mut ids: Vec<&str> = spec.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        ids.sort_unstable();
        let mut expected = vec![a.as_str(), b.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);
        assert_eq!(spec.network.as_ref().unwrap().subnet, "10.42.1.0/24");
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
             log_seq, last_seen_at, created_at, subnet) \
             VALUES (?, 'n', '[\"node\"]', NULL, 'pending', '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, 0, NULL, ?, '10.42.1.0/24')",
        )
        .bind(&node_id)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let a = insert_vm(&store, &node_id, "a").await;
        let b = insert_vm(&store, &node_id, "b").await;
        let key = DataKey::ephemeral();

        // A new VM waits; the other one is sent.
        let built = build_spec(
            &store,
            &node_id,
            &FailsOnB,
            &DigestCache::default(),
            &key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
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
                &FailsOnB,
                &DigestCache::default(),
                &key,
                None,
                &no_mesh_lookup(),
                &Drains::default(),
                &[],
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
            &FailsOnB,
            &DigestCache::default(),
            &key,
            Some(&previous),
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
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

    /// A project row for the app tests.
    async fn insert_project(store: &Store, slug: &str) -> String {
        let project_id = new_id(Prefix::Project);
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&project_id)
        .bind(slug)
        .bind(slug)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        project_id
    }

    /// A node row with the given roles, and the given subnet when it has one.
    async fn insert_node(store: &Store, id: &str, roles: &[&str], subnet: Option<&str>) {
        let now = crate::store::now_rfc3339();
        let roles = serde_json::to_string(roles).unwrap();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at, subnet) \
             VALUES (?, ?, ?, NULL, 'pending', '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, \
             0, NULL, ?, ?)",
        )
        .bind(id)
        .bind(id)
        .bind(roles)
        .bind(&now)
        .bind(subnet)
        .execute(store.pool())
        .await
        .unwrap();
    }

    /// An app row with `replicas` and the contract's default health check.
    async fn insert_app(store: &Store, project_id: &str, app_id: &str, name: &str, replicas: i64) {
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO apps (id, project_id, name, stateful, ha_policy, image, port, health_check, \
             resources, placement, status, current_deployment_id, restart_generation, created_at, \
             updated_at, replicas) \
             VALUES (?, ?, ?, 0, 'none', 'img@sha256:1', 8080, \
             '{\"path\":\"/\",\"interval_s\":10,\"timeout_s\":2,\"failures\":3}', '{}', '{}', \
             'running', NULL, 0, ?, ?, ?)",
        )
        .bind(app_id)
        .bind(project_id)
        .bind(name)
        .bind(&now)
        .bind(&now)
        .bind(replicas)
        .execute(store.pool())
        .await
        .unwrap();
    }

    /// One instance of an app, with a guest address when it has one.
    async fn insert_app_vm(
        store: &Store,
        project_id: &str,
        app_id: &str,
        node_id: &str,
        vm_id: &str,
        name: &str,
        ip: Option<&str>,
    ) {
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
             image, desired_status, observed_status, ip, restart_generation, restart_count, \
             last_exit_code, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, 2, 512, 4, 'img@sha256:1', 'running', 'running', ?, 0, 0, \
             NULL, ?, ?)",
        )
        .bind(vm_id)
        .bind(project_id)
        .bind(app_id)
        .bind(node_id)
        .bind(name)
        .bind(ip)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
    }

    /// A domain of an app.
    async fn insert_domain(
        store: &Store,
        domain_id: &str,
        app_id: &str,
        hostname: &str,
        tls_mode: &str,
    ) {
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO domains (id, app_id, hostname, tls_mode, cert_status, cert_expires_at, \
             created_at) VALUES (?, ?, ?, ?, 'pending', NULL, ?)",
        )
        .bind(domain_id)
        .bind(app_id)
        .bind(hostname)
        .bind(tls_mode)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_replicated_app_route_moves_to_the_edge() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = "nod_0000000000000000000000000E";
        let node = "nod_00000000000000000000000001";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, node, &["node"], Some("10.42.1.0/24")).await;
        let project_id = insert_project(&store, "rep").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "web", 2).await;
        let domain_id = deploy::new_id_with_prefix("dom");
        insert_domain(&store, &domain_id, &app_id, "rep.example.com", "acme").await;
        let first = new_id(Prefix::Vm);
        let second = new_id(Prefix::Vm);
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            node,
            &first,
            "web-a",
            Some("10.42.1.11"),
        )
        .await;
        insert_app_vm(&store, &project_id, &app_id, node, &second, "web-b", None).await;

        let cache = DigestCache::default();
        let key = DataKey::ephemeral();
        let on_node = build_spec(
            &store,
            node,
            &ReferenceAsDigest,
            &cache,
            &key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
        )
        .await
        .unwrap()
        .spec;
        assert!(
            on_node.routes.is_empty(),
            "a replicated app's route never lands on its node"
        );
        assert!(on_node.edge.is_none(), "a node is not the edge");

        let on_edge = build_spec(
            &store,
            edge,
            &ReferenceAsDigest,
            &cache,
            &key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
        )
        .await
        .unwrap()
        .spec;
        assert!(on_edge.network.is_none(), "the edge has no guest subnet");
        let edge_spec = on_edge.edge.expect("the edge gets spec.edge");
        assert_eq!(edge_spec.routes.len(), 1);
        let route = &edge_spec.routes[0];
        assert_eq!(route.hostname, "rep.example.com");
        assert_eq!(route.domain_id, domain_id);
        assert_eq!(route.app_id, app_id);
        assert_eq!(route.tls_mode, "acme");
        assert_eq!(route.health_check.failures, Some(3));
        assert_eq!(route.backends.len(), 1, "the VM without an address waits");
        assert_eq!(route.backends[0].vm_id, first);
        assert_eq!(route.backends[0].node_id, node);
        assert_eq!(route.backends[0].address, "10.42.1.11");
        assert_eq!(route.backends[0].port, 8080);
        assert!(edge_spec.tunnel.is_none(), "an acme route needs no tunnel");
    }

    #[tokio::test]
    async fn a_single_replica_app_keeps_its_node_route_and_the_edge_has_no_route() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = "nod_0000000000000000000000000E";
        let node = "nod_00000000000000000000000001";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, node, &["node"], Some("10.42.1.0/24")).await;
        let project_id = insert_project(&store, "one").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "blog", 1).await;
        let domain_id = deploy::new_id_with_prefix("dom");
        insert_domain(&store, &domain_id, &app_id, "blog.example.com", "acme").await;
        let vm_id = new_id(Prefix::Vm);
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            node,
            &vm_id,
            "blog-1",
            Some("10.42.1.12"),
        )
        .await;

        let cache = DigestCache::default();
        let key = DataKey::ephemeral();
        let on_node = build_spec(
            &store,
            node,
            &ReferenceAsDigest,
            &cache,
            &key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
        )
        .await
        .unwrap()
        .spec;
        assert_eq!(on_node.routes.len(), 1);
        assert_eq!(on_node.routes[0].hostname, "blog.example.com");
        assert_eq!(on_node.routes[0].vm_ids, vec![vm_id]);

        let on_edge = build_spec(
            &store,
            edge,
            &ReferenceAsDigest,
            &cache,
            &key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
        )
        .await
        .unwrap()
        .spec;
        let edge_spec = on_edge.edge.expect("the edge still gets spec.edge");
        assert!(edge_spec.routes.is_empty());
    }

    #[tokio::test]
    async fn tunnel_hostnames_put_a_replicated_app_on_the_edge_only() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = "nod_0000000000000000000000000E";
        let node = "nod_00000000000000000000000001";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, node, &["node"], Some("10.42.1.0/24")).await;
        let project_id = insert_project(&store, "tun").await;
        let replicated = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &replicated, "rep", 2).await;
        insert_domain(
            &store,
            &deploy::new_id_with_prefix("dom"),
            &replicated,
            "rep.example.com",
            "tunnel",
        )
        .await;
        let single = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &single, "one", 1).await;
        insert_domain(
            &store,
            &deploy::new_id_with_prefix("dom"),
            &single,
            "one.example.com",
            "tunnel",
        )
        .await;
        insert_app_vm(
            &store,
            &project_id,
            &single,
            node,
            &new_id(Prefix::Vm),
            "one-1",
            Some("10.42.1.13"),
        )
        .await;

        let on_edge = tunnel_hostnames(store.pool(), edge).await.unwrap();
        assert_eq!(
            on_edge.iter().cloned().collect::<Vec<_>>(),
            ["rep.example.com"]
        );
        let on_node = tunnel_hostnames(store.pool(), node).await.unwrap();
        assert_eq!(
            on_node.iter().cloned().collect::<Vec<_>>(),
            ["one.example.com"]
        );
    }

    /// The snapshot of `node_id`, after `previous` when the session sent one.
    async fn spec_on(store: &Store, node_id: &str, previous: Option<&Spec>) -> Spec {
        build_spec(
            store,
            node_id,
            &ReferenceAsDigest,
            &DigestCache::default(),
            &DataKey::ephemeral(),
            previous,
            &no_mesh_lookup(),
            &Drains::default(),
            &[],
        )
        .await
        .unwrap()
        .spec
    }

    /// A volume of `app_id` on `node_id` in `state`.
    async fn insert_volume(store: &Store, app_id: &str, node_id: &str, state: &str) -> String {
        let volume_id = deploy::new_id_with_prefix("vol");
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
             allocated_bytes, restored_from_backup_id, error, created_at, updated_at) \
             VALUES (?, ?, ?, 1, '/data', ?, NULL, NULL, NULL, ?, ?)",
        )
        .bind(&volume_id)
        .bind(app_id)
        .bind(node_id)
        .bind(state)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        volume_id
    }

    #[tokio::test]
    async fn a_restore_onto_another_node_takes_the_app_vm_off_its_node() {
        let store = Store::open_in_memory().await.unwrap();
        let old = "nod_00000000000000000000000001";
        let target = "nod_00000000000000000000000002";
        insert_node(&store, old, &["node"], Some("10.42.1.0/24")).await;
        insert_node(&store, target, &["node"], Some("10.42.2.0/24")).await;
        let project_id = insert_project(&store, "moved").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "db", 1).await;
        let domain_id = deploy::new_id_with_prefix("dom");
        insert_domain(&store, &domain_id, &app_id, "db.example.com", "acme").await;
        let other_app = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &other_app, "web", 1).await;
        let vm_id = new_id(Prefix::Vm);
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            old,
            &vm_id,
            "db-1",
            Some("10.42.1.10"),
        )
        .await;
        let other_vm = new_id(Prefix::Vm);
        insert_app_vm(
            &store,
            &project_id,
            &other_app,
            old,
            &other_vm,
            "web-1",
            Some("10.42.1.11"),
        )
        .await;
        let before = spec_on(&store, old, None).await;
        assert_eq!(before.vms.len(), 2, "the old node was sent both VMs");

        // While the volume restores on the target, the app's VM is gone from
        // its old node, although that node was sent it before, and so is its
        // route. The other app's VM stays.
        let volume_id = insert_volume(&store, &app_id, target, "restoring").await;
        let on_old = spec_on(&store, old, Some(&before)).await;
        let ids: Vec<&str> = on_old.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        assert_eq!(ids, [other_vm.as_str()]);
        assert!(on_old.routes.is_empty(), "no route to the moved VM");
        // The target gets the volume to restore, and the VM only once the
        // restore is ready and the row moves there.
        let on_target = spec_on(&store, target, None).await;
        assert!(on_target.vms.is_empty());
        assert_eq!(on_target.volumes.len(), 1);
        assert_eq!(on_target.volumes[0].volume_id, volume_id);

        // A restore onto the VM's own node is not a move: the VM stays.
        sqlx::query("UPDATE volumes SET node_id = ? WHERE id = ?")
            .bind(old)
            .bind(&volume_id)
            .execute(store.pool())
            .await
            .unwrap();
        let on_old = spec_on(&store, old, Some(&before)).await;
        let mut ids: Vec<&str> = on_old.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        ids.sort_unstable();
        let mut expected = vec![vm_id.as_str(), other_vm.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);
        assert_eq!(on_old.routes.len(), 1);
        assert_eq!(on_old.routes[0].vm_ids, vec![vm_id]);
    }

    // -----------------------------------------------------------------------
    // Draining tunnel routes (ADR 0194)
    // -----------------------------------------------------------------------

    /// A `tunnel` provider and a tunnel of it for each of `machines`, with a
    /// run token encrypted under `key`.
    async fn insert_tunnels(store: &Store, key: &DataKey, machines: &[&str]) {
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO users (id, email, name, role, password_hash, disabled, created_at) \
             VALUES ('usr_tunnel', 'tunnel@example.com', 'tunnel', 'owner', 'x', 0, ?)",
        )
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO providers (id, kind, name, config_json, secret_enc, created_by, \
             created_at, rotated_at) \
             VALUES ('prv_tunnel', 'tunnel', 'tunnel', '{}', x'00', 'usr_tunnel', ?, NULL)",
        )
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        for machine in machines {
            sqlx::query(
                "INSERT INTO node_tunnels (node_id, provider_id, tunnel_id, token_enc, \
                 origin_port, created_at, updated_at) \
                 VALUES (?, 'prv_tunnel', ?, ?, 8081, ?, ?)",
            )
            .bind(machine)
            .bind(format!("tun-{machine}"))
            .bind(crate::tunnel::encrypt_token(key, machine, "run-token"))
            .bind(&now)
            .bind(&now)
            .execute(store.pool())
            .await
            .unwrap();
        }
    }

    /// The snapshot of `node_id` after `previous`, with `drains` and `key`.
    async fn drained_spec(
        store: &Store,
        key: &DataKey,
        drains: &Drains,
        node_id: &str,
        previous: Option<&Spec>,
    ) -> Spec {
        build_spec(
            store,
            node_id,
            &ReferenceAsDigest,
            &DigestCache::default(),
            key,
            previous,
            &no_mesh_lookup(),
            drains,
            &[],
        )
        .await
        .unwrap()
        .spec
    }

    async fn set_replicas(store: &Store, app_id: &str, replicas: i64) {
        sqlx::query("UPDATE apps SET replicas = ? WHERE id = ?")
            .bind(replicas)
            .bind(app_id)
            .execute(store.pool())
            .await
            .unwrap();
    }

    async fn stop_vm(store: &Store, vm_id: &str) {
        sqlx::query("UPDATE vms SET desired_status = 'stopped' WHERE id = ?")
            .bind(vm_id)
            .execute(store.pool())
            .await
            .unwrap();
    }

    /// Ends every drain whose CNAME `machine` took over for `hostname`.
    fn repoint_and_end(drains: &Drains, machine: &str, hostname: &str) {
        let names: BTreeSet<String> = [hostname.to_string()].into();
        let past = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
        drains.record_cnames(machine, &names, &names, past);
        drains.expire(time::OffsetDateTime::now_utc());
    }

    #[tokio::test]
    async fn a_tunnel_route_that_moves_to_the_edge_drains_on_its_node() {
        let store = Store::open_in_memory().await.unwrap();
        let key = DataKey::ephemeral();
        let drains = Drains::default();
        let edge = "nod_0000000000000000000000000E";
        let node = "nod_00000000000000000000000001";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, node, &["node"], Some("10.42.1.0/24")).await;
        insert_tunnels(&store, &key, &[edge, node]).await;
        let project_id = insert_project(&store, "move").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "web", 1).await;
        let domain_id = deploy::new_id_with_prefix("dom");
        insert_domain(&store, &domain_id, &app_id, "move.example.com", "tunnel").await;
        let vm_id = new_id(Prefix::Vm);
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            node,
            &vm_id,
            "web-1",
            Some("10.42.1.11"),
        )
        .await;

        let first = drained_spec(&store, &key, &drains, node, None).await;
        assert_eq!(first.routes.len(), 1);
        assert_eq!(first.routes[0].draining_until, None);

        // Two replicas: the edge serves the hostname, and the node keeps its
        // route, with its own VM, until the window ends.
        set_replicas(&store, &app_id, 2).await;
        let second = drained_spec(&store, &key, &drains, node, Some(&first)).await;
        assert_eq!(second.routes.len(), 1, "{:?}", second.routes);
        let route = &second.routes[0];
        assert_eq!(route.hostname, "move.example.com");
        assert_eq!(route.domain_id.as_deref(), Some(domain_id.as_str()));
        assert_eq!(route.vm_ids, vec![vm_id.clone()]);
        assert_eq!(route.tls_mode, "tunnel");
        let until = route.draining_until.clone().expect("a draining route");
        time::OffsetDateTime::parse(&until, &time::format_description::well_known::Rfc3339)
            .expect("draining_until is RFC 3339");
        let on_edge = drained_spec(&store, &key, &drains, edge, None).await;
        let edge_spec = on_edge.edge.expect("spec.edge");
        assert_eq!(edge_spec.routes.len(), 1);
        assert_eq!(edge_spec.routes[0].draining_until, None);
        assert!(edge_spec.tunnel.is_some());

        // Built again, the snapshot is the same: the end does not move.
        let third = drained_spec(&store, &key, &drains, node, Some(&second)).await;
        assert_eq!(third.routes, second.routes);

        // The edge's tunnel took the CNAME over long enough ago: the route
        // leaves, and a draining route never starts draining again.
        repoint_and_end(&drains, edge, "move.example.com");
        let fourth = drained_spec(&store, &key, &drains, node, Some(&third)).await;
        assert!(fourth.routes.is_empty(), "{:?}", fourth.routes);
        let fifth = drained_spec(&store, &key, &drains, node, Some(&fourth)).await;
        assert!(fifth.routes.is_empty());
    }

    #[tokio::test]
    async fn a_drain_that_waits_for_its_cname_ends_once_no_other_tunnel_serves_it() {
        let store = Store::open_in_memory().await.unwrap();
        let key = DataKey::ephemeral();
        let drains = Drains::default();
        let edge = "nod_0000000000000000000000000E";
        let node = "nod_00000000000000000000000001";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, node, &["node"], Some("10.42.1.0/24")).await;
        insert_tunnels(&store, &key, &[edge, node]).await;
        let project_id = insert_project(&store, "wait").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "web", 1).await;
        insert_domain(
            &store,
            &deploy::new_id_with_prefix("dom"),
            &app_id,
            "wait.example.com",
            "tunnel",
        )
        .await;
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            node,
            &new_id(Prefix::Vm),
            "web-1",
            Some("10.42.1.11"),
        )
        .await;
        let first = drained_spec(&store, &key, &drains, node, None).await;
        set_replicas(&store, &app_id, 2).await;
        let second = drained_spec(&store, &key, &drains, node, Some(&first)).await;
        assert!(second.routes[0].draining_until.is_some());

        // The edge's tunnel never took the CNAME: at its provisional end the
        // drain is checked again and, the edge still serving the hostname,
        // gets a new end.
        let later = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
        assert_eq!(
            drain::sweep(store.pool(), &drains, later).await,
            BTreeSet::from([node.to_string()])
        );
        assert!(drains.next_end().is_some_and(|end| end > later));
        let third = drained_spec(&store, &key, &drains, node, Some(&second)).await;
        assert_eq!(third.routes.len(), 1);
        assert_ne!(
            third.routes[0].draining_until,
            second.routes[0].draining_until
        );

        // The edge's tunnel is removed: no other tunnel serves the hostname,
        // so the next check ends the drain.
        sqlx::query("DELETE FROM node_tunnels WHERE node_id = ?")
            .bind(edge)
            .execute(store.pool())
            .await
            .unwrap();
        let even_later = later + time::Duration::hours(1);
        assert_eq!(
            drain::sweep(store.pool(), &drains, even_later).await,
            BTreeSet::from([node.to_string()])
        );
        assert!(drains.next_end().is_none());
        let fourth = drained_spec(&store, &key, &drains, node, Some(&third)).await;
        assert!(fourth.routes.is_empty(), "{:?}", fourth.routes);
    }

    #[tokio::test]
    async fn a_route_back_on_one_replica_drains_on_the_edge_with_the_remaining_instance() {
        let store = Store::open_in_memory().await.unwrap();
        let key = DataKey::ephemeral();
        let drains = Drains::default();
        let edge = "nod_0000000000000000000000000E";
        let kept = "nod_00000000000000000000000001";
        let other = "nod_00000000000000000000000002";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, kept, &["node"], Some("10.42.1.0/24")).await;
        insert_node(&store, other, &["node"], Some("10.42.2.0/24")).await;
        insert_tunnels(&store, &key, &[edge, kept]).await;
        let project_id = insert_project(&store, "back").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "web", 2).await;
        insert_domain(
            &store,
            &deploy::new_id_with_prefix("dom"),
            &app_id,
            "back.example.com",
            "tunnel",
        )
        .await;
        let a = new_id(Prefix::Vm);
        let b = new_id(Prefix::Vm);
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            kept,
            &a,
            "web-a",
            Some("10.42.1.11"),
        )
        .await;
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            other,
            &b,
            "web-b",
            Some("10.42.2.11"),
        )
        .await;

        let first = drained_spec(&store, &key, &drains, edge, None).await;
        assert_eq!(first.edge.as_ref().unwrap().routes[0].backends.len(), 2);

        // Back to one replica on the node with a tunnel: the edge keeps the
        // route with the remaining instance, and its tunnel with it.
        set_replicas(&store, &app_id, 1).await;
        stop_vm(&store, &b).await;
        let second = drained_spec(&store, &key, &drains, edge, Some(&first)).await;
        let edge_spec = second.edge.clone().expect("spec.edge");
        assert_eq!(edge_spec.routes.len(), 1, "{:?}", edge_spec.routes);
        let route = &edge_spec.routes[0];
        assert!(route.draining_until.is_some());
        let backends: Vec<&str> = route.backends.iter().map(|b| b.vm_id.as_str()).collect();
        assert_eq!(backends, [a.as_str()]);
        assert!(
            edge_spec.tunnel.is_some(),
            "the draining route needs the tunnel"
        );
        let on_node = drained_spec(&store, &key, &drains, kept, None).await;
        assert_eq!(on_node.routes.len(), 1);
        assert_eq!(on_node.routes[0].draining_until, None);

        // With no instance left the edge gets no draining route, and no
        // tunnel for it.
        stop_vm(&store, &a).await;
        let third = drained_spec(&store, &key, &drains, edge, Some(&second)).await;
        let edge_spec = third.edge.expect("spec.edge");
        assert!(edge_spec.routes.is_empty(), "{:?}", edge_spec.routes);
        assert!(edge_spec.tunnel.is_none());
    }

    #[tokio::test]
    async fn a_route_that_no_other_tunnel_serves_leaves_at_once() {
        let store = Store::open_in_memory().await.unwrap();
        let key = DataKey::ephemeral();
        let drains = Drains::default();
        let edge = "nod_0000000000000000000000000E";
        let node = "nod_00000000000000000000000001";
        insert_node(&store, edge, &["edge"], None).await;
        insert_node(&store, node, &["node"], Some("10.42.1.0/24")).await;
        // Only the node has a tunnel: the edge cannot take the hostname.
        insert_tunnels(&store, &key, &[node]).await;
        let project_id = insert_project(&store, "gone").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(&store, &project_id, &app_id, "web", 1).await;
        insert_domain(
            &store,
            &deploy::new_id_with_prefix("dom"),
            &app_id,
            "gone.example.com",
            "tunnel",
        )
        .await;
        insert_domain(
            &store,
            &deploy::new_id_with_prefix("dom"),
            &app_id,
            "acme.example.com",
            "acme",
        )
        .await;
        insert_app_vm(
            &store,
            &project_id,
            &app_id,
            node,
            &new_id(Prefix::Vm),
            "web-1",
            Some("10.42.1.11"),
        )
        .await;

        let first = drained_spec(&store, &key, &drains, node, None).await;
        assert_eq!(first.routes.len(), 2);
        set_replicas(&store, &app_id, 2).await;
        let second = drained_spec(&store, &key, &drains, node, Some(&first)).await;
        assert!(second.routes.is_empty(), "{:?}", second.routes);
        assert!(drains.next_end().is_none());
    }

    const SOURCE: &str = "nod_0000000000000000000000000S";
    const TARGET: &str = "nod_0000000000000000000000000T";

    /// Two `reflink` nodes on the mesh and a `warm` app on the first: its VM,
    /// a `ready` volume, a lease the first node holds with token 4, and the
    /// second node as its warm node. Returns `(app_id, vm_id, volume_id)`.
    async fn warm_app(store: &Store) -> (String, String, String) {
        insert_node(store, SOURCE, &["node"], Some("10.42.1.0/24")).await;
        insert_node(store, TARGET, &["node"], Some("10.42.2.0/24")).await;
        for (node, address) in [(SOURCE, "10.43.0.2"), (TARGET, "10.43.0.3")] {
            sqlx::query(
                "UPDATE nodes SET mesh_address = ?, volume_snapshot = 'reflink' WHERE id = ?",
            )
            .bind(address)
            .bind(node)
            .execute(store.pool())
            .await
            .unwrap();
        }
        let project_id = insert_project(store, "warm").await;
        let app_id = deploy::new_id_with_prefix("app");
        insert_app(store, &project_id, &app_id, "db", 1).await;
        sqlx::query(
            "UPDATE apps SET stateful = 1, ha_policy = 'warm', warm_node_id = ? WHERE id = ?",
        )
        .bind(TARGET)
        .bind(&app_id)
        .execute(store.pool())
        .await
        .unwrap();
        let vm_id = "vm_0000000000000000000000000A".to_owned();
        insert_app_vm(
            store,
            &project_id,
            &app_id,
            SOURCE,
            &vm_id,
            "db-1",
            Some("10.42.1.5"),
        )
        .await;
        let volume_id = insert_volume(store, &app_id, SOURCE, "ready").await;
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, granted_at) VALUES ('lse_w', 'app', ?, ?, 'local', 4, ?)",
        )
        .bind(&app_id)
        .bind(SOURCE)
        .bind(crate::store::now_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();
        (app_id, vm_id, volume_id)
    }

    /// The snapshot of a node that reported holding `copies` as a warm node.
    async fn spec_with_copies(store: &Store, node_id: &str, copies: &[(String, String)]) -> Spec {
        spec_keyed(store, &DataKey::ephemeral(), node_id, copies).await
    }

    async fn spec_keyed(
        store: &Store,
        key: &DataKey,
        node_id: &str,
        copies: &[(String, String)],
    ) -> Spec {
        build_spec(
            store,
            node_id,
            &ReferenceAsDigest,
            &DigestCache::default(),
            key,
            None,
            &no_mesh_lookup(),
            &Drains::default(),
            copies,
        )
        .await
        .unwrap()
        .spec
    }

    #[tokio::test]
    async fn both_nodes_of_a_warm_app_get_their_part_and_the_warm_node_a_standby_vm() {
        let store = Store::open_in_memory().await.unwrap();
        let (app_id, vm_id, volume_id) = warm_app(&store).await;
        insert_domain(&store, "dom_w", &app_id, "db.example.com", "acme").await;
        let key = DataKey::ephemeral();
        sqlx::query(
            "INSERT INTO app_env (app_id, name, value_enc, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(&app_id)
        .bind("DATABASE_PASSWORD")
        .bind(crate::app_env::encrypt_value(
            &key,
            &app_id,
            "DATABASE_PASSWORD",
            "s3cret",
        ))
        .bind(crate::store::now_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();

        let source = spec_keyed(&store, &key, SOURCE, &[]).await;
        assert_eq!(source.volumes.len(), 1);
        assert_eq!(
            source.volumes[0].warm,
            Some(SpecWarm {
                role: WarmRole::Source,
                peer_node_id: TARGET.to_owned(),
                peer_address: "10.43.0.3".to_owned(),
                port: 51822,
                interval_s: 30,
                lease_id: "lse_w".to_owned(),
                fencing_token: 4,
            })
        );
        let running = &source.vms[0];
        assert!(!running.standby);
        assert_eq!(running.desired_status, DesiredStatus::Running);
        assert_eq!(source.routes.len(), 1, "the source serves the app");

        let target = spec_keyed(&store, &key, TARGET, &[]).await;
        assert_eq!(
            target.volumes,
            vec![SpecVolumeFile {
                volume_id: volume_id.clone(),
                app_id: app_id.clone(),
                size_gb: 1,
                state: SpecVolumeState::Present,
                restore: None,
                warm: Some(SpecWarm {
                    role: WarmRole::Target,
                    peer_node_id: SOURCE.to_owned(),
                    peer_address: "10.43.0.2".to_owned(),
                    port: 51822,
                    interval_s: 30,
                    lease_id: "lse_w".to_owned(),
                    fencing_token: 4,
                }),
            }]
        );
        assert_eq!(target.vms.len(), 1);
        let standby = &target.vms[0];
        assert_eq!(standby.vm_id, vm_id, "the app's VM, under its own id");
        assert!(standby.standby);
        assert_eq!(standby.desired_status, DesiredStatus::Stopped);
        assert_eq!(standby.image_digest, running.image_digest);
        assert_eq!(
            (standby.vcpus, standby.memory_mb, standby.disk_gb),
            (running.vcpus, running.memory_mb, running.disk_gb)
        );
        assert!(standby.stateful);
        assert_eq!(standby.lease_id.as_deref(), Some("lse_w"));
        assert_eq!(standby.fencing_token, Some(4));
        assert_eq!(
            standby.volumes,
            vec![SpecVolume {
                volume_id,
                serial: "quoro-vol-0".to_owned(),
                mount_path: "/data".to_owned(),
            }]
        );
        assert_eq!(standby.env, running.env, "section 5.11: the app's values");
        assert_eq!(
            standby.env.get("DATABASE_PASSWORD").map(String::as_str),
            Some("s3cret")
        );
        assert!(
            target.routes.is_empty(),
            "the warm node never serves the app"
        );
    }

    #[tokio::test]
    async fn a_warm_app_gets_no_warm_entries_until_its_pair_is_complete() {
        // Each case undoes one part of a complete pair.
        let cases: [(&str, &str); 7] = [
            ("no lease", "DELETE FROM leases"),
            (
                "a revoked lease",
                "UPDATE leases SET revoked_at = '2026-11-07T10:00:00Z'",
            ),
            (
                "a lease another node holds",
                "UPDATE leases SET holder_node_id = 'nod_0000000000000000000000000T'",
            ),
            (
                "a warm node that snapshots with pause_copy",
                "UPDATE nodes SET volume_snapshot = 'pause_copy' \
                 WHERE id = 'nod_0000000000000000000000000T'",
            ),
            (
                "a source whose snapshot method is not known yet",
                "UPDATE nodes SET volume_snapshot = NULL \
                 WHERE id = 'nod_0000000000000000000000000S'",
            ),
            (
                "a warm node without a mesh address",
                "UPDATE nodes SET mesh_address = NULL WHERE id = 'nod_0000000000000000000000000T'",
            ),
            (
                "a volume that is not ready",
                "UPDATE volumes SET state = 'creating'",
            ),
        ];
        for (case, undo) in cases {
            let store = Store::open_in_memory().await.unwrap();
            warm_app(&store).await;
            sqlx::query(undo).execute(store.pool()).await.unwrap();
            let source = spec_on(&store, SOURCE, None).await;
            assert!(
                source.volumes.iter().all(|file| file.warm.is_none()),
                "{case}: {:?}",
                source.volumes
            );
            let target = spec_on(&store, TARGET, None).await;
            assert!(target.volumes.is_empty(), "{case}: {:?}", target.volumes);
            assert!(target.vms.is_empty(), "{case}: {:?}", target.vms);
        }
    }

    /// `(volume_id, state, carries warm)` of each entry of `spec.volumes`.
    fn volume_states(spec: &Spec) -> Vec<(&str, SpecVolumeState, bool)> {
        spec.volumes
            .iter()
            .map(|file| (file.volume_id.as_str(), file.state, file.warm.is_some()))
            .collect()
    }

    #[tokio::test]
    async fn a_copy_the_node_reported_but_no_longer_targets_is_deleted() {
        let store = Store::open_in_memory().await.unwrap();
        let (app_id, _, volume_id) = warm_app(&store).await;
        let reported = vec![
            (volume_id.clone(), app_id.clone()),
            ("vol_gone".to_owned(), "app_gone".to_owned()),
        ];
        let target = spec_with_copies(&store, TARGET, &reported).await;
        let mut expected = vec![
            (volume_id.as_str(), SpecVolumeState::Present, true),
            ("vol_gone", SpecVolumeState::Deleted, false),
        ];
        expected.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(volume_states(&target), expected);

        // The app leaves `warm`: its copy on the old warm node is deleted,
        // with the volume's size, and the standby VM goes.
        sqlx::query("UPDATE volumes SET size_gb = 3")
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE apps SET ha_policy = 'none', warm_node_id = NULL")
            .execute(store.pool())
            .await
            .unwrap();
        let target = spec_with_copies(&store, TARGET, &reported[..1]).await;
        assert_eq!(target.volumes.len(), 1);
        assert_eq!(target.volumes[0].state, SpecVolumeState::Deleted);
        assert_eq!(target.volumes[0].size_gb, 3);
        assert!(target.vms.is_empty());
        let source = spec_on(&store, SOURCE, None).await;
        assert_eq!(source.volumes[0].warm, None);
        assert_eq!(source.volumes[0].state, SpecVolumeState::Present);

        // The source's own volume is never deleted for a report of it.
        let source = spec_with_copies(&store, SOURCE, &reported[..1]).await;
        assert_eq!(source.volumes[0].state, SpecVolumeState::Present);
    }

    #[tokio::test]
    async fn the_warm_node_keeps_its_copy_until_no_failover_can_need_it() {
        let store = Store::open_in_memory().await.unwrap();
        let (app_id, _, volume_id) = warm_app(&store).await;
        let held = vec![(volume_id.clone(), app_id.clone())];
        let run = |sql: &'static str| {
            let pool = store.pool().clone();
            async move { sqlx::query(sql).execute(&pool).await.unwrap() }
        };
        let kept = |spec: &Spec, why: &str| {
            assert!(spec.volumes.is_empty(), "{why}: {:?}", spec.volumes);
            assert!(spec.vms.is_empty(), "{why}: {:?}", spec.vms);
        };

        // A fence or a restore revokes the lease, and the source's volume
        // fails: the copy may be the only good data, so it gets no entry at
        // all, and the warm node keeps it.
        run("UPDATE leases SET revoked_at = '2026-11-07T10:00:00Z'").await;
        kept(
            &spec_with_copies(&store, TARGET, &held).await,
            "a revoked lease",
        );
        run("UPDATE volumes SET state = 'failed'").await;
        kept(
            &spec_with_copies(&store, TARGET, &held).await,
            "a failed volume",
        );
        run("UPDATE apps SET warm_node_id = NULL").await;
        kept(
            &spec_with_copies(&store, TARGET, &held).await,
            "no warm node",
        );
        // Leaving `warm` while the source is fenced keeps it too.
        run("UPDATE apps SET ha_policy = 'none'").await;
        kept(
            &spec_with_copies(&store, TARGET, &held).await,
            "left warm, fenced",
        );

        // A healthy source and another warm node: the copy is deleted. The
        // new warm node starts its own first copy from the source.
        run("UPDATE leases SET revoked_at = NULL").await;
        run("UPDATE volumes SET state = 'ready'").await;
        insert_node(&store, "nod_0000000000000000000000000X", &["node"], None).await;
        run("UPDATE apps SET ha_policy = 'warm', warm_node_id = 'nod_0000000000000000000000000X'")
            .await;
        let target = spec_with_copies(&store, TARGET, &held).await;
        assert_eq!(
            volume_states(&target),
            vec![(volume_id.as_str(), SpecVolumeState::Deleted, false)]
        );
    }

    #[tokio::test]
    async fn the_standby_vm_is_the_sources_newest_vm_until_one_belongs_to_the_current_deployment() {
        let store = Store::open_in_memory().await.unwrap();
        let (app_id, first, _) = warm_app(&store).await;
        let project_id: String = sqlx::query_scalar("SELECT project_id FROM apps WHERE id = ?")
            .bind(&app_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        let newer = "vm_0000000000000000000000000B".to_owned();
        assert!(newer > first);
        insert_app_vm(&store, &project_id, &app_id, SOURCE, &newer, "db-2", None).await;
        let target = spec_on(&store, TARGET, None).await;
        assert_eq!(target.vms.len(), 1);
        assert_eq!(target.vms[0].vm_id, newer);
    }
}

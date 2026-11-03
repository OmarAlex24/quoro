// SPDX-License-Identifier: AGPL-3.0-only
//! The `spec` of a node's `desired_state` (`node-agent-protocol.md` section
//! 5.11).
//!
//! The snapshot is complete: every `vms` row placed on the node, ordered by
//! id. Image digests come from the [`DigestResolver`] and are cached for the
//! process lifetime, so a tag that moves never silently changes a running VM.

use crate::{DigestResolver, NodeNetwork, store::Store};
use quoro_proto::wire::{DesiredStatus, Spec, SpecNetwork, SpecVm};
use sqlx::Row;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// The resolved digests, keyed by image reference, shared by every session.
pub type DigestCache = Arc<Mutex<HashMap<String, String>>>;

/// Builds the full snapshot for `node_id`.
///
/// # Errors
///
/// A database error, or an image reference of a VM the node may already run
/// that the resolver could not resolve. On an error the caller keeps the
/// snapshot it already sent and retries.
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
    previous: Option<&Spec>,
) -> Result<Spec, String> {
    let rows = sqlx::query(
        "SELECT id, project_id, app_id, name, image, vcpus, memory_mb, disk_gb, \
         desired_status, observed_status, restart_generation FROM vms WHERE node_id = ? ORDER BY id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the desired state of {node_id}: {e}"))?;

    let mut vms = Vec::with_capacity(rows.len());
    for row in &rows {
        let vm_id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let image: String = row.try_get("image").map_err(|e| e.to_string())?;
        let image_digest = match resolve_digest(resolver, cache, &image).await {
            Ok(digest) => digest,
            Err(e) => {
                let sent = previous
                    .and_then(|spec| spec.vms.iter().find(|vm| vm.vm_id == vm_id))
                    .map(|vm| vm.image_digest.clone());
                let observed: String = row.try_get("observed_status").map_err(|e| e.to_string())?;
                match sent {
                    // The node already has it: keep the digest it was given.
                    Some(digest) => digest,
                    None if observed == "creating" => {
                        tracing::warn!(vm_id, image, error = %e, "cannot resolve the image; the VM waits for it");
                        continue;
                    }
                    None => return Err(format!("cannot resolve {image} for {vm_id}: {e}")),
                }
            }
        };
        let desired_status: String = row.try_get("desired_status").map_err(|e| e.to_string())?;
        let vcpus: i64 = row.try_get("vcpus").map_err(|e| e.to_string())?;
        let memory_mb: i64 = row.try_get("memory_mb").map_err(|e| e.to_string())?;
        let disk_gb: i64 = row.try_get("disk_gb").map_err(|e| e.to_string())?;
        let restart_generation: i64 = row
            .try_get("restart_generation")
            .map_err(|e| e.to_string())?;
        vms.push(SpecVm {
            vm_id,
            project_id: row.try_get("project_id").map_err(|e| e.to_string())?,
            app_id: row.try_get("app_id").map_err(|e| e.to_string())?,
            deployment_id: None,
            name: row.try_get("name").map_err(|e| e.to_string())?,
            image,
            image_digest,
            vcpus: u32::try_from(vcpus).map_err(|e| format!("vms.vcpus: {e}"))?,
            memory_mb: u32::try_from(memory_mb).map_err(|e| format!("vms.memory_mb: {e}"))?,
            disk_gb: u32::try_from(disk_gb).map_err(|e| format!("vms.disk_gb: {e}"))?,
            port: None,
            env: BTreeMap::new(),
            health_check: None,
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
        });
    }

    Ok(Spec {
        vms,
        // No domains exist before the app workstream; routes are empty in M2.
        routes: Vec::new(),
        network: SpecNetwork {
            subnet: network.subnet.clone(),
            gateway: network.gateway.clone(),
        },
    })
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
        let spec = build_spec(&store, &node_id, &network, &ReferenceAsDigest, &cache, None)
            .await
            .unwrap();
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

        // A new VM waits; the other one is sent.
        let spec = build_spec(
            &store,
            &node_id,
            &network,
            &FailsOnB,
            &DigestCache::default(),
            None,
        )
        .await
        .unwrap();
        let ids: Vec<&str> = spec.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        assert_eq!(ids, [a.as_str()]);

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
                None
            )
            .await
            .is_err()
        );

        // A digest this session already sent is kept.
        let mut previous = spec.clone();
        let mut sent = previous.vms[0].clone();
        sent.vm_id.clone_from(&b);
        sent.image_digest = "sha256:earlier".to_string();
        previous.vms.push(sent);
        let spec = build_spec(
            &store,
            &node_id,
            &network,
            &FailsOnB,
            &DigestCache::default(),
            Some(&previous),
        )
        .await
        .unwrap();
        let kept = spec.vms.iter().find(|vm| vm.vm_id == b).expect("b is sent");
        assert_eq!(kept.image_digest, "sha256:earlier");
    }
}

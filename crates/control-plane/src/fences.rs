// SPDX-License-Identifier: AGPL-3.0-only
//! Failed fences of nodes and their acknowledgement (`node-agent-protocol.md`
//! section 8.5, ADR 0074, ADR 0079; migration 0001).
//!
//! A node whose self-fence could not confirm that every bound VM stopped
//! reports `node.fence_failed`, or answers a `lease_revoke` with
//! `fence_failed`. The primary then lists the lease, app and VM ids in the
//! node's `fence_failure`, and grants none of those leases to any node until a
//! human acknowledges them (`POST /api/v1/nodes/{node_id}/fence-ack`). The
//! acknowledgement is delivered to the node as `command` kind `fence_ack` by
//! its session (`session/mod.rs`); `fence_ack_pending` holds it until the
//! node answers `ok`.
//!
//! Both columns are replicated state: every write goes through the change log
//! in the caller's transaction.

use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::SqliteConnection;

use crate::changelog::{self, Actor, ActorType, Op};
use crate::driver;
use crate::error::ApiError;
use crate::store::{Store, now_rfc3339};

/// How long a session start holds back the first desired state for the
/// stale-lease revokes and the pending `fence_ack` to be answered (section
/// 4.2 rules 3 to 5). The node still gets its desired state after it; a
/// `fence_ack` already sent is still cleared by a late `ok`.
pub(crate) const FENCE_ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// `nodes.fence_failure` (migration 0001, `NodeFenceFailure`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FenceFailure {
    pub detected_at: String,
    pub lease_ids: Vec<String>,
    pub app_ids: Vec<String>,
    pub vm_ids: Vec<String>,
}

/// `nodes.fence_ack_pending` (migration 0001).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FenceAckPending {
    pub acked_at: String,
    pub lease_ids: Vec<String>,
}

/// The ids one failure report names.
#[derive(Clone, Debug, Default)]
pub(crate) struct Failed<'a> {
    pub lease_ids: &'a [String],
    pub app_ids: &'a [String],
    pub vm_ids: &'a [String],
}

fn extend_unique(into: &mut Vec<String>, ids: &[String]) -> bool {
    let mut changed = false;
    for id in ids {
        if !into.contains(id) {
            into.push(id.clone());
            changed = true;
        }
    }
    changed
}

fn parse<T: for<'de> Deserialize<'de>>(text: Option<String>) -> Result<Option<T>, ApiError> {
    text.map(|text| {
        serde_json::from_str(&text)
            .map_err(|e| ApiError::internal(&format!("a stored fence column is unreadable: {e}")))
    })
    .transpose()
}

/// The node's unacknowledged failed fences, or `None`.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn failure_of(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Option<FenceFailure>, ApiError> {
    let text: Option<Option<String>> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await?;
    parse(text.flatten())
}

/// The node's acknowledgement that it has not confirmed yet, or `None`.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn pending_of(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Option<FenceAckPending>, ApiError> {
    let text: Option<Option<String>> =
        sqlx::query_scalar("SELECT fence_ack_pending FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await?;
    parse(text.flatten())
}

/// Adds a failed fence of `node_id` to its `fence_failure`; ids accumulate
/// until acknowledged (section 8.5 "Acknowledging a failed fence" step 1).
/// Returns whether anything changed.
///
/// A reported lease also leaves `fence_ack_pending`: the node failed to fence
/// it again after the operator checked, so the acknowledgement no longer
/// covers it and must not clear the new failure on the node. A late copy of
/// a report the operator already acknowledged lists the lease again, which
/// costs one more acknowledgement and is the safe side.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn record_failure(
    conn: &mut SqliteConnection,
    node_id: &str,
    failed: &Failed<'_>,
    actor: &Actor,
) -> Result<bool, ApiError> {
    if failed.lease_ids.is_empty() {
        return Ok(false);
    }
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_optional(&mut *conn)
        .await?;
    if exists.is_none() {
        return Ok(false);
    }
    let current = failure_of(conn, node_id).await?;
    let is_new = current.is_none();
    let mut failure = current.unwrap_or_else(|| FenceFailure {
        detected_at: now_rfc3339(),
        ..FenceFailure::default()
    });
    let mut changed = is_new;
    changed |= extend_unique(&mut failure.lease_ids, failed.lease_ids);
    changed |= extend_unique(&mut failure.app_ids, failed.app_ids);
    changed |= extend_unique(&mut failure.vm_ids, failed.vm_ids);
    let mut pending = pending_of(conn, node_id).await?;
    if let Some(pending) = pending.as_mut() {
        let before = pending.lease_ids.len();
        pending
            .lease_ids
            .retain(|lease_id| !failed.lease_ids.contains(lease_id));
        changed |= pending.lease_ids.len() != before;
    }
    if !changed {
        return Ok(false);
    }
    let text = serde_json::to_string(&failure)
        .map_err(|e| ApiError::internal(&format!("cannot encode a fence failure: {e}")))?;
    sqlx::query("UPDATE nodes SET fence_failure = ?, fence_ack_pending = ? WHERE id = ?")
        .bind(text)
        .bind(pending_text(pending.as_ref())?)
        .bind(node_id)
        .execute(&mut *conn)
        .await?;
    let payload = driver::node_payload(conn, node_id).await?;
    changelog::append(conn, actor, "node", node_id, Op::Update, payload).await?;
    Ok(true)
}

/// The stored form of `fence_ack_pending`: `NULL` once no lease is left.
///
/// # Errors
///
/// An internal error when the value cannot be encoded.
pub(crate) fn pending_text(pending: Option<&FenceAckPending>) -> Result<Option<String>, ApiError> {
    match pending {
        Some(pending) if !pending.lease_ids.is_empty() => serde_json::to_string(pending)
            .map(Some)
            .map_err(|e| ApiError::internal(&format!("cannot encode a fence ack: {e}"))),
        _ => Ok(None),
    }
}

/// Every lease listed in any node's `fence_failure`: the primary grants none
/// of them to any node (section 8.5).
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn listed_leases(
    conn: &mut SqliteConnection,
) -> Result<HashSet<String>, ApiError> {
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE fence_failure IS NOT NULL")
            .fetch_all(&mut *conn)
            .await?;
    let mut listed = HashSet::new();
    for text in rows {
        if let Some(failure) = parse::<FenceFailure>(Some(text))? {
            listed.extend(failure.lease_ids);
        }
    }
    Ok(listed)
}

/// Clears the acknowledged `lease_ids` from the node's `fence_ack_pending`
/// once the node answered the `fence_ack` with `ok` (section 8.5 step 4). An
/// acknowledgement recorded meanwhile for other leases stays pending.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn confirm(
    store: &Store,
    node_id: &str,
    lease_ids: &[String],
) -> Result<(), ApiError> {
    let mut w = store.begin_write().await?;
    let Some(mut pending) = pending_of(&mut w.tx, node_id).await? else {
        return Ok(());
    };
    let before = pending.lease_ids.len();
    pending
        .lease_ids
        .retain(|lease_id| !lease_ids.contains(lease_id));
    if pending.lease_ids.len() == before {
        return Ok(());
    }
    sqlx::query("UPDATE nodes SET fence_ack_pending = ? WHERE id = ?")
        .bind(pending_text(Some(&pending))?)
        .bind(node_id)
        .execute(&mut *w.tx)
        .await?;
    let payload = driver::node_payload(&mut w.tx, node_id).await?;
    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.to_string(),
    };
    changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload).await?;
    w.commit().await?;
    Ok(())
}

/// The `params` of a `fence_ack` command (section 5.17).
#[must_use]
pub(crate) fn command_params(lease_ids: &[String]) -> serde_json::Value {
    json!({ "lease_ids": lease_ids })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{Capacity, Kvm, LocalNode};

    async fn store_with_node() -> (Store, String, std::path::PathBuf) {
        let data_dir = std::env::temp_dir().join(format!(
            "quoro-fences-{}",
            &crate::store::random_secret()[..16]
        ));
        std::fs::create_dir_all(&data_dir).unwrap();
        let store = Store::open(&data_dir.join("quoro.db")).await.unwrap();
        let node_id = crate::driver::register_local_node(
            &store,
            &LocalNode {
                name: "node-a".to_string(),
                capacity: Capacity {
                    cpus: 2,
                    memory_mb: 2048,
                    disk_gb: 20,
                },
                kvm: Kvm {
                    available: true,
                    kernel: "6.8.0".to_string(),
                    nested: None,
                },
                engine_version: "test".to_string(),
                agent_version: "test".to_string(),
            },
        )
        .await
        .unwrap();
        (store, node_id, data_dir)
    }

    fn node_actor(node_id: &str) -> Actor {
        Actor {
            kind: ActorType::Node,
            id: node_id.to_string(),
        }
    }

    #[tokio::test]
    async fn a_listed_lease_is_granted_to_no_node_until_acknowledged() {
        let (store, node_id, data_dir) = store_with_node().await;
        let actor = node_actor(&node_id);
        let mut w = store.begin_write().await.unwrap();
        let lease = crate::leases::grant_app_lease(&mut w.tx, "app_x", &node_id, &actor)
            .await
            .unwrap();
        assert_eq!(lease.fencing_token, 1);
        let lease_ids = [lease.id.clone()];
        let app_ids = ["app_x".to_string()];
        let failed = Failed {
            lease_ids: &lease_ids,
            app_ids: &app_ids,
            vm_ids: &[],
        };
        assert!(
            record_failure(&mut w.tx, &node_id, &failed, &actor)
                .await
                .unwrap()
        );
        // The same report again adds nothing.
        assert!(
            !record_failure(&mut w.tx, &node_id, &failed, &actor)
                .await
                .unwrap()
        );
        assert!(listed_leases(&mut w.tx).await.unwrap().contains(&lease.id));

        let refused = crate::leases::grant_app_lease(&mut w.tx, "app_x", &node_id, &actor)
            .await
            .unwrap_err();
        assert_eq!(refused.code, crate::error::Code::Conflict);

        // The acknowledgement's token step, then the failure cleared: the
        // next grant carries a strictly higher token again.
        // A lease held by another node is left alone.
        assert!(
            crate::leases::bump_tokens(&mut w.tx, &lease_ids, "nod_other", &actor)
                .await
                .unwrap()
                .is_empty()
        );
        let bumped = crate::leases::bump_tokens(&mut w.tx, &lease_ids, &node_id, &actor)
            .await
            .unwrap();
        assert_eq!(bumped.len(), 1);
        assert_eq!(bumped[0].fencing_token, 2);
        assert!(bumped[0].revoked_at.is_some());
        assert_eq!(bumped[0].holder_node_id, node_id);
        sqlx::query("UPDATE nodes SET fence_failure = NULL WHERE id = ?")
            .bind(&node_id)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let granted = crate::leases::grant_app_lease(&mut w.tx, "app_x", &node_id, &actor)
            .await
            .unwrap();
        assert_eq!(granted.fencing_token, 3);
        w.commit().await.unwrap();
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    #[tokio::test]
    async fn a_new_failure_of_an_acknowledged_lease_lists_it_and_leaves_the_ack() {
        let (store, node_id, data_dir) = store_with_node().await;
        let actor = node_actor(&node_id);
        let mut w = store.begin_write().await.unwrap();
        let pending = FenceAckPending {
            acked_at: now_rfc3339(),
            lease_ids: vec!["lse_acked".to_string(), "lse_kept".to_string()],
        };
        sqlx::query("UPDATE nodes SET fence_ack_pending = ? WHERE id = ?")
            .bind(serde_json::to_string(&pending).unwrap())
            .bind(&node_id)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        // The node failed to fence `lse_acked` again after the operator
        // checked: it is listed again, and the acknowledgement no longer
        // covers it, so it cannot clear the new failure on the node.
        let acked = ["lse_acked".to_string()];
        assert!(
            record_failure(
                &mut w.tx,
                &node_id,
                &Failed {
                    lease_ids: &acked,
                    app_ids: &[],
                    vm_ids: &["vm_1".to_string()],
                },
                &actor,
            )
            .await
            .unwrap()
        );
        let failure = failure_of(&mut w.tx, &node_id).await.unwrap().unwrap();
        assert_eq!(failure.lease_ids, acked.to_vec());
        assert_eq!(failure.vm_ids, vec!["vm_1".to_string()]);
        let left = pending_of(&mut w.tx, &node_id).await.unwrap().unwrap();
        assert_eq!(left.lease_ids, vec!["lse_kept".to_string()]);

        // The same report again changes nothing.
        assert!(
            !record_failure(
                &mut w.tx,
                &node_id,
                &Failed {
                    lease_ids: &acked,
                    app_ids: &[],
                    vm_ids: &["vm_1".to_string()],
                },
                &actor,
            )
            .await
            .unwrap()
        );

        // Confirming the delivery clears only what it carried; the last
        // lease leaves the column `NULL`.
        w.commit().await.unwrap();
        confirm(&store, &node_id, &["lse_kept".to_string()])
            .await
            .unwrap();
        let mut conn = store.pool().acquire().await.unwrap();
        assert!(pending_of(&mut conn, &node_id).await.unwrap().is_none());
        drop(conn);
        std::fs::remove_dir_all(&data_dir).unwrap();
    }
}

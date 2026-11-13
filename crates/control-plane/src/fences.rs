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
//! in the caller's transaction. [`write_failure_row`] is the one exception:
//! it writes the row only, and its caller must append the node's whole-row
//! entry in the same transaction before it commits.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::SqliteConnection;

use crate::changelog::{self, Actor, ActorType, Op};
use crate::driver;
use crate::error::ApiError;
use crate::store::{Store, now_rfc3339};

/// `nodes.fence_failure` (migration 0001, `NodeFenceFailure`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FenceFailure {
    pub detected_at: String,
    pub lease_ids: Vec<String>,
    pub app_ids: Vec<String>,
    pub vm_ids: Vec<String>,
}

/// `nodes.fence_ack_pending` (migration 0001), with the token stored for
/// each lease (section 8.5 step 3, ADR 0245).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FenceAckPending {
    pub acked_at: String,
    pub lease_ids: Vec<String>,
    /// The acknowledged token of each lease in `lease_ids`: for a lease the
    /// node still holds, its `fencing_token` after the acknowledgement's
    /// increment; for one another node holds, that lease's token plus one
    /// (ADR 0247), at most [`UNKNOWN_LEASE_TOKEN`] (2^53 − 1, the cap of
    /// section 8.6; #947); for one with no row, [`UNKNOWN_LEASE_TOKEN`]. An
    /// acknowledgement a primary before ADR 0245 recorded has none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fencing_tokens: BTreeMap<String, u64>,
}

/// The acknowledged token of a listed lease the primary has no row for
/// (P-0273). Lease rows are never deleted, so this is a lease only a node
/// knows, such as one a restore from an older backup lost. The primary
/// grants it to no node, and sends the node nothing about it but the revoke
/// of section 5.1 at each session start, which the node answers before the
/// `fence_ack` goes out (section 8.5 step 4). So, as for a lease another
/// node holds (ADR 0247), every failed fence the node records for it before
/// the acknowledgement is delivered is one the operator saw or one its
/// revoke answer lists again, and the highest token clears them all.
///
/// It is 2^53 - 1, the highest integer canonical JSON carries
/// (`quoro_proto::jcs`): `fence_ack_pending` is part of the node's
/// change-log payload. Every grant writes its lease row, token included, to
/// the change log too, so no primary issues a token above it.
pub(crate) const UNKNOWN_LEASE_TOKEN: u64 = (1 << 53) - 1;

/// One delivery of the pending acknowledgement (section 8.5 step 4): the
/// lease ids and the tokens stored for them when it went out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Delivery {
    pub lease_ids: Vec<String>,
    /// The stored token of each listed lease that has one.
    pub tokens: BTreeMap<String, u64>,
}

impl Delivery {
    /// The `fencing_tokens` this delivery carries (section 5.17): its tokens
    /// when every listed lease has one, else none, and the `fence_ack` goes
    /// out as a primary before ADR 0245 sends it. A node answers a
    /// `fence_ack` whose tokens miss a listed lease with `error`.
    #[must_use]
    pub(crate) fn fencing_tokens(&self) -> Option<&BTreeMap<String, u64>> {
        self.lease_ids
            .iter()
            .all(|lease_id| self.tokens.contains_key(lease_id))
            .then_some(&self.tokens)
    }
}

impl FenceAckPending {
    /// The next `fence_ack` of this acknowledgement (section 8.5 step 4), the
    /// same on every delivery while nothing changes. A lease without a stored
    /// token, which only an acknowledgement recorded before ADR 0245 leaves,
    /// goes out in a `fence_ack` of its own without `fencing_tokens`, and the
    /// node clears it by lease id. The leases with
    /// a token follow, with their tokens, once the node answered it: one
    /// without tokens would clear them by lease id too, a failed fence the
    /// operator never saw included.
    #[must_use]
    pub(crate) fn delivery(&self) -> Delivery {
        let untokened: Vec<String> = self
            .lease_ids
            .iter()
            .filter(|lease_id| !self.fencing_tokens.contains_key(*lease_id))
            .cloned()
            .collect();
        if !untokened.is_empty() {
            return Delivery {
                lease_ids: untokened,
                tokens: BTreeMap::new(),
            };
        }
        Delivery {
            lease_ids: self.lease_ids.clone(),
            tokens: self
                .lease_ids
                .iter()
                .filter_map(|lease_id| {
                    self.fencing_tokens
                        .get(lease_id)
                        .map(|token| (lease_id.clone(), *token))
                })
                .collect(),
        }
    }

    /// Takes `lease_id` out, with its token.
    fn remove(&mut self, lease_id: &str) {
        self.lease_ids.retain(|listed| listed != lease_id);
        self.fencing_tokens.remove(lease_id);
    }
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
    if !write_failure_row(conn, node_id, failed).await? {
        return Ok(false);
    }
    let payload = driver::node_payload(conn, node_id).await?;
    changelog::append(conn, actor, "node", node_id, Op::Update, payload).await?;
    Ok(true)
}

/// [`record_failure`]'s write of the node's row, without its change-log
/// entry. For `removeNode` only, which writes the node's whole-row removal
/// entry later in the same transaction: that one entry then carries the
/// failure and the removal together (`crate::removal`). Any other caller
/// must also append the node's whole-row entry before it commits, or the
/// change log misses the write (ADR 0297). Returns whether the row changed.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn write_failure_row(
    conn: &mut SqliteConnection,
    node_id: &str,
    failed: &Failed<'_>,
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
        for lease_id in failed.lease_ids {
            pending.remove(lease_id);
        }
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

/// The nodes whose `fence_failure` lists `lease_id`, sorted.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn nodes_listing(
    conn: &mut SqliteConnection,
    lease_id: &str,
) -> Result<Vec<String>, ApiError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, fence_failure FROM nodes WHERE fence_failure IS NOT NULL ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut listing = Vec::new();
    for (node_id, text) in rows {
        if parse::<FenceFailure>(Some(text))?
            .is_some_and(|failure| failure.lease_ids.iter().any(|id| id == lease_id))
        {
            listing.push(node_id);
        }
    }
    Ok(listing)
}

/// Each `(node, lease)` whose failed fence the node reported in a
/// `lease_revoke` answered `fence_failed` while this primary could not record
/// it in `fence_failure` (section 8.5 step 1, issue #392). In memory only: an
/// entry is added when that record fails, removed only when a later
/// `fence_failed` answer of the same node for the same lease is recorded, and
/// lost when the primary restarts.
#[derive(Debug, Default)]
pub(crate) struct UnrecordedFailures(Mutex<HashSet<(String, String)>>);

impl UnrecordedFailures {
    /// Notes whether the failed fence of `lease_id` that `node_id` reported
    /// is recorded.
    pub(crate) fn note(&self, node_id: &str, lease_id: &str, recorded: bool) {
        let mut unrecorded = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (node_id.to_string(), lease_id.to_string());
        if recorded {
            unrecorded.remove(&key);
        } else {
            unrecorded.insert(key);
        }
    }

    /// Whether `node_id` reported a failed fence of `lease_id` that is not
    /// recorded.
    pub(crate) fn of_node(&self, node_id: &str, lease_id: &str) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&(node_id.to_string(), lease_id.to_string()))
    }

    /// Whether `node_id` reported a failed fence, of any lease, that is not
    /// recorded.
    pub(crate) fn any_of_node(&self, node_id: &str) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(failed_on, _)| failed_on == node_id)
    }

    /// Every lease `node_id` reported a failed fence of that is not
    /// recorded, sorted: what `removeNode` writes into the node's
    /// `fence_failure` before the node is removed, so the failures outlive a
    /// restart of the primary (`crate::removal`).
    pub(crate) fn leases_of_node(&self, node_id: &str) -> Vec<String> {
        let mut leases: Vec<String> = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(failed_on, _)| failed_on == node_id)
            .map(|(_, failed)| failed.clone())
            .collect();
        leases.sort_unstable();
        leases
    }

    /// Every lease some node reported a failed fence of that is not
    /// recorded.
    fn leases(&self) -> HashSet<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(_, failed)| failed.clone())
            .collect()
    }
}

/// Whether `lease_id` has a failed fence that no operator acknowledged,
/// recorded or not: listed in a node's `fence_failure` ([`listed_leases`]),
/// or reported while this primary could not record it
/// ([`UnrecordedFailures`]). Either way its VMs may still run, so no grant of
/// it goes out and no new token of it is written (section 8.5 step 1). The
/// one test of it for the grant paths, so they agree; [`failed_leases`]
/// gives the same answer for every lease at once.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn has_failed_fence(
    conn: &mut SqliteConnection,
    unrecorded: &UnrecordedFailures,
    lease_id: &str,
) -> Result<bool, ApiError> {
    Ok(failed_leases(conn, unrecorded).await?.contains(lease_id))
}

/// Every lease [`has_failed_fence`] answers `true` for.
///
/// The in-memory set is read before the database, and the order matters on
/// a connection outside the writer: the set drops an entry only after its
/// record has committed, so a failure recorded between the two reads is in
/// the snapshot or in the rows. Read the other way, it could be in neither.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn failed_leases(
    conn: &mut SqliteConnection,
    unrecorded: &UnrecordedFailures,
) -> Result<HashSet<String>, ApiError> {
    let mut failed = unrecorded.leases();
    failed.extend(listed_leases(conn).await?);
    Ok(failed)
}

/// Whether `node_id` has a failed fence that no operator acknowledged,
/// recorded in its `fence_failure` or reported while this primary could not
/// record it ([`UnrecordedFailures`]): its VMs may still run (section 8.5
/// step 1, `openapi.yaml` `failoverApp`'s `fence_failed`). The in-memory set
/// is read before the database, for the reason [`failed_leases`] gives.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn node_has_failed_fence(
    conn: &mut SqliteConnection,
    unrecorded: &UnrecordedFailures,
    node_id: &str,
) -> Result<bool, ApiError> {
    if unrecorded.any_of_node(node_id) {
        return Ok(true);
    }
    Ok(failure_of(conn, node_id).await?.is_some())
}

/// Clears what `delivered` carried from the node's `fence_ack_pending` once
/// the node answered the `fence_ack` with `ok` (section 8.5 step 4): each
/// delivered lease still pending with the token stored when it went out, or
/// still without one when it went out without. One acknowledged again since,
/// after a new failure listed it, with another token stays pending (ADR
/// 0238, ADR 0245), and so do the leases the delivery did not carry.
///
/// # Errors
///
/// A database error, or a stored value that does not parse.
pub(crate) async fn confirm(
    store: &Store,
    node_id: &str,
    delivered: &Delivery,
) -> Result<(), ApiError> {
    let mut w = store.begin_write().await?;
    let Some(mut pending) = pending_of(&mut w.tx, node_id).await? else {
        return Ok(());
    };
    let before = pending.clone();
    for lease_id in &delivered.lease_ids {
        if pending.lease_ids.contains(lease_id)
            && pending.fencing_tokens.get(lease_id) == delivered.tokens.get(lease_id)
        {
            pending.remove(lease_id);
        }
    }
    if pending == before {
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

/// The `params` of a `fence_ack` command (section 5.17): `fencing_tokens`
/// only when the delivery has a token for every lease (ADR 0245).
#[must_use]
pub(crate) fn command_params(delivery: &Delivery) -> serde_json::Value {
    match delivery.fencing_tokens() {
        Some(tokens) => json!({ "lease_ids": delivery.lease_ids, "fencing_tokens": tokens }),
        None => json!({ "lease_ids": delivery.lease_ids }),
    }
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

    /// The lease row of a new-token grant that no gate held back. A grant
    /// that waits ([`crate::leases::NewGrant::Waiting`]) writes its row too,
    /// which is all these tests read.
    async fn grant(
        w: &mut crate::store::WriteTx,
        node_id: &str,
        actor: &Actor,
    ) -> Result<crate::leases::Lease, crate::error::ApiError> {
        let authority = crate::authority::Authority::new(std::sync::Arc::new(
            crate::authority::SystemMonotonic::default(),
        ));
        match crate::leases::grant_app_lease(
            &mut w.tx,
            &authority,
            &UnrecordedFailures::default(),
            "app_x",
            node_id,
            actor,
            false,
        )
        .await?
        {
            crate::leases::NewGrant::Granted(lease) | crate::leases::NewGrant::Waiting(lease) => {
                Ok(*lease)
            }
            crate::leases::NewGrant::GateOpen(remaining) => panic!("a gate open for {remaining:?}"),
        }
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
        let lease = grant(&mut w, &node_id, &actor).await.unwrap();
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

        let refused = grant(&mut w, &node_id, &actor).await.unwrap_err();
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
        let granted = grant(&mut w, &node_id, &actor).await.unwrap();
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
            fencing_tokens: BTreeMap::from([
                ("lse_acked".to_string(), 4),
                ("lse_kept".to_string(), 7),
            ]),
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
        assert_eq!(
            left.fencing_tokens,
            BTreeMap::from([("lse_kept".to_string(), 7)]),
            "the token leaves with its lease"
        );

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
        confirm(&store, &node_id, &left.delivery()).await.unwrap();
        let mut conn = store.pool().acquire().await.unwrap();
        assert!(pending_of(&mut conn, &node_id).await.unwrap().is_none());
        drop(conn);
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    async fn set_pending(store: &Store, node_id: &str, pending: &FenceAckPending) {
        sqlx::query("UPDATE nodes SET fence_ack_pending = ? WHERE id = ?")
            .bind(serde_json::to_string(pending).unwrap())
            .bind(node_id)
            .execute(store.pool())
            .await
            .unwrap();
    }

    async fn pending(store: &Store, node_id: &str) -> Option<FenceAckPending> {
        let mut conn = store.pool().acquire().await.unwrap();
        pending_of(&mut conn, node_id).await.unwrap()
    }

    /// Section 5.17 (ADR 0245): a delivery carries `fencing_tokens` only
    /// when every pending lease has one. An acknowledgement a primary before
    /// ADR 0245 recorded has none, and goes out as that primary sent it.
    #[test]
    fn a_delivery_carries_the_tokens_only_when_every_lease_has_one() {
        let mut pending = FenceAckPending {
            acked_at: now_rfc3339(),
            lease_ids: vec!["lse_a".to_string(), "lse_b".to_string()],
            fencing_tokens: BTreeMap::from([("lse_a".to_string(), 3), ("lse_b".to_string(), 9)]),
        };
        let delivery = pending.delivery();
        assert_eq!(
            command_params(&delivery),
            json!({
                "lease_ids": ["lse_a", "lse_b"],
                "fencing_tokens": {"lse_a": 3, "lse_b": 9},
            })
        );
        // A lease without a token goes out alone, without tokens.
        pending.fencing_tokens.remove("lse_b");
        assert_eq!(pending.delivery().fencing_tokens(), None);
        assert_eq!(
            command_params(&pending.delivery()),
            json!({ "lease_ids": ["lse_b"] })
        );

        // A row written before ADR 0245 reads with no tokens, and is written
        // back as it was.
        let legacy = r#"{"acked_at":"2026-11-01T00:00:00Z","lease_ids":["lse_a"]}"#;
        let read: FenceAckPending = serde_json::from_str(legacy).unwrap();
        assert!(read.fencing_tokens.is_empty());
        assert_eq!(read.delivery().fencing_tokens(), None);
        assert_eq!(serde_json::to_string(&read).unwrap(), legacy);
    }

    /// Section 8.5 step 4 (ADR 0238, ADR 0245): an `ok` clears a delivered
    /// lease only while the pending acknowledgement still carries the token
    /// that went out. A lease a new failure listed again and an operator
    /// acknowledged again meanwhile carries a newer token, and stays pending.
    #[tokio::test]
    async fn an_ok_clears_only_what_went_out_with_the_same_token() {
        let (store, node_id, data_dir) = store_with_node().await;
        let sent = FenceAckPending {
            acked_at: now_rfc3339(),
            lease_ids: vec!["lse_a".to_string(), "lse_b".to_string()],
            fencing_tokens: BTreeMap::from([("lse_a".to_string(), 3), ("lse_b".to_string(), 9)]),
        };
        let delivery = sent.delivery();
        // `lse_a` was acknowledged again at token 5 before the answer came,
        // and `lse_c` was acknowledged meanwhile.
        set_pending(
            &store,
            &node_id,
            &FenceAckPending {
                acked_at: now_rfc3339(),
                lease_ids: vec![
                    "lse_a".to_string(),
                    "lse_b".to_string(),
                    "lse_c".to_string(),
                ],
                fencing_tokens: BTreeMap::from([
                    ("lse_a".to_string(), 5),
                    ("lse_b".to_string(), 9),
                    ("lse_c".to_string(), 2),
                ]),
            },
        )
        .await;
        confirm(&store, &node_id, &delivery).await.unwrap();
        let left = pending(&store, &node_id).await.unwrap();
        assert_eq!(
            left.lease_ids,
            vec!["lse_a".to_string(), "lse_c".to_string()]
        );
        assert_eq!(
            left.fencing_tokens,
            BTreeMap::from([("lse_a".to_string(), 5), ("lse_c".to_string(), 2)])
        );

        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    /// P-0273: the token of a lease with no row is the highest integer the
    /// change log can carry.
    #[test]
    fn the_token_of_a_lease_with_no_row_is_the_highest_the_change_log_carries() {
        assert_eq!(UNKNOWN_LEASE_TOKEN, 9_007_199_254_740_991);
        let pending = FenceAckPending {
            acked_at: now_rfc3339(),
            lease_ids: vec!["lse_gone".to_string()],
            fencing_tokens: BTreeMap::from([("lse_gone".to_string(), UNKNOWN_LEASE_TOKEN)]),
        };
        let value = serde_json::to_value(&pending).unwrap();
        assert!(crate::jcs::canonicalize(&value).is_ok());
        assert!(crate::jcs::canonicalize(&json!(UNKNOWN_LEASE_TOKEN + 1)).is_err());
    }

    /// Section 8.5 step 4 and section 5.17: an acknowledgement with leases
    /// both with and without a token goes out in two `fence_ack`s. The leases
    /// without one go first, without tokens, and their `ok` clears them; the
    /// leases with one follow with their tokens, so no `fence_ack` clears
    /// them by lease id.
    #[tokio::test]
    async fn a_mixed_acknowledgement_goes_out_in_two_deliveries() {
        let (store, node_id, data_dir) = store_with_node().await;
        let mixed = FenceAckPending {
            acked_at: now_rfc3339(),
            lease_ids: vec![
                "lse_new".to_string(),
                "lse_old".to_string(),
                "lse_more".to_string(),
            ],
            fencing_tokens: BTreeMap::from([
                ("lse_new".to_string(), 4),
                ("lse_more".to_string(), 7),
            ]),
        };
        set_pending(&store, &node_id, &mixed).await;
        let first = mixed.delivery();
        assert_eq!(command_params(&first), json!({ "lease_ids": ["lse_old"] }));
        confirm(&store, &node_id, &first).await.unwrap();
        let left = pending(&store, &node_id).await.unwrap();
        assert_eq!(
            left.lease_ids,
            vec!["lse_new".to_string(), "lse_more".to_string()]
        );
        let second = left.delivery();
        assert_eq!(
            command_params(&second),
            json!({
                "lease_ids": ["lse_new", "lse_more"],
                "fencing_tokens": {"lse_new": 4, "lse_more": 7},
            })
        );
        confirm(&store, &node_id, &second).await.unwrap();
        assert!(pending(&store, &node_id).await.is_none());
        std::fs::remove_dir_all(&data_dir).unwrap();
    }
}

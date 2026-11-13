// SPDX-License-Identifier: AGPL-3.0-only
//! `removeNode`: removing a node from the cluster (`openapi.yaml`
//! `removeNode`, `node-agent-protocol.md` section 10).
//!
//! The call runs in up to three steps after its authorization:
//!
//! 1. **Refusals** ([`refuse`]), checked again under the writer in each
//!    transaction below: an unknown node is `not_found`; a node already
//!    removed, the node that holds the primary status, a node that still
//!    hosts running instances, and a voter change the voter-set gate refuses
//!    (section 8.10, [`crate::session::NodeHub::voter_removal_gate`]) are
//!    `conflict`, in that order.
//! 2. **The fence, for a connected node** ([`fence`]). Every lease row that
//!    names the node and that the node has not released at its token is
//!    revoked in the primary's view, with its change-log entry and its
//!    `lease.revoke` system record, in one transaction. Then a `lease_revoke`
//!    goes out for each on the node's session, and the call waits for each
//!    answer ([`wait_fenced`]). Only an `ok` answer at the row's token
//!    releases a lease. A `fence_failed` answer, recorded or not, a refused
//!    revoke, a session that ends first, or no answer in time answers
//!    `conflict` and removes nothing; the rows stay revoked, so a retry goes
//!    on from there.
//! 3. **The removal**, one transaction ([`remove`]): the failed fences the
//!    node reported that the primary could not record are written into its
//!    `fence_failure` first ([`record_unrecorded`]), so they outlive a
//!    restart of the primary. For a node that is not connected, the token of
//!    every lease row that names it and that it has not released at its
//!    token goes up by one and the row is recorded revoked, as declaring the
//!    node lost does for the rows it covers ([`crate::leases::bump_tokens`],
//!    section 8.3), except a row already revoked at the token cap, which is
//!    left as it is; a connected node must hold no lease it has not released
//!    by then. The node turns `removed`; its change-log entry is the removal
//!    entry the voter set and the required set of section 8.4 read, and it
//!    carries the failed fences written above. The records of the
//!    quarantined copies it keeps are dropped
//!    ([`crate::quarantines::drop_node_copies`]), its custody seals are
//!    deleted ([`crate::custody::delete_seals`]), and the call's audit
//!    record names the removal entry. Right before the commit the hub marks
//!    the node removed, so it reads not connected from the commit on
//!    ([`crate::session::NodeHub::mark_removed`]); after the commit its
//!    session, if any, ends with the fatal `error` `node_removed`
//!    ([`crate::session::NodeHub::node_removed`]).
//!
//! The node turns `removed` before its session ends, not after: from the
//! commit on, a `hello` of the node is refused under the writer gate, so a
//! node that dials again between the two cannot be written back to
//! `connected` (`session::connect_node`), and every check of whether it is
//! connected answers no.
//!
//! Releasing what a removed node held is [`crate::leases::is_released`]'s,
//! unchanged: a removed holder reads as gone, and an `expiring` lease still
//! waits for its gate.

use crate::AppState;
use crate::api::nodes::{Node, node_view};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::leases::{self, LEASE_COLUMNS, Lease, lease_from_row};
use crate::request::CallCtx;
use crate::session::{Fault, RemovalStep, VoterRemoval};
use quoro_proto::RevokeReason;
use quoro_proto::wire::ControlMessage;
use quoro_types::{Prefix, Scope, is_id};
use serde_json::{Value, json};
use sqlx::SqliteConnection;
use std::time::Duration;
use tokio::time::Instant;

/// The `reason` of the `lease_revoke` a removal sends a connected node, and
/// of its `lease.revoke` system record.
///
/// The contract does not name one for a removal yet: section 5.13 lists
/// `moved`, `deleted`, `declared_lost` and `superseded`, and section 10 does
/// not say which applies. `declared_lost` is the closest: an operator states
/// that the node leaves the cluster, and section 10 removes a node that is
/// not connected "exactly like declaring the node lost". The node fences the
/// same way whatever the reason (section 5.13). Pending a decision.
pub(crate) const REVOKE_REASON: RevokeReason = RevokeReason::DeclaredLost;

/// [`REVOKE_REASON`] as the `lease.revoke` record's `params` carry it.
const REVOKE_REASON_TEXT: &str = "declared_lost";

/// How long the call waits for a connected node to answer its revokes, as a
/// failover waits for its source (`crate::failover`). A test shortens it to
/// one second with [`Fault::ShortRemovalWait`].
const REVOKE_WAIT: Duration = Duration::from_secs(180);

/// How often the wait checks again when nothing wakes it.
const RECHECK: Duration = Duration::from_millis(250);

/// What a refusal after the fence revoked the node's leases adds to its
/// detail.
const STAYS_REVOKED: &str = "the node's leases stay revoked and nothing was removed; a retry goes \
                             on from there";

/// Runs `removeNode` for `node_id` after the authorization check, and
/// answers the node as `getNode` shows it, now `removed`.
///
/// # Errors
///
/// `not_found` for an unknown node; `conflict` for the refusals of
/// [`refuse`], for a fence that is not confirmed, or for a node that took a
/// lease or connected while it was being removed; or a database error.
pub(crate) async fn run(
    state: &AppState,
    ctx: &CallCtx,
    node_id: &str,
    params: &Value,
) -> Result<Node, ApiError> {
    if !is_id(Prefix::Node, node_id) {
        return Err(ApiError::not_found("node"));
    }
    let revoked = if state.hub.is_connected(node_id) {
        fence(state, ctx, node_id).await?
    } else {
        false
    };
    if let Some(hold) = state.hub.take_removal_hold(node_id, RemovalStep::Checked) {
        hold.hold().await;
    }
    match remove(state, ctx, node_id, params).await {
        Ok(node) => Ok(node),
        Err(mut error) if revoked => {
            if let Some(detail) = error.detail.as_mut() {
                detail.push_str("; ");
                detail.push_str(STAYS_REVOKED);
            }
            Err(error)
        }
        Err(error) => Err(error),
    }
}

/// The refusals of `removeNode`, in order, read on `conn`. Returns what the
/// removal does to the voter set.
///
/// - `not_found` for an unknown node.
/// - `conflict` for a node already removed, unless the change log still
///   counts it as a voter and disagrees with the rows on it alone: removing
///   it again writes the removal entry that ends the disagreement (ADR 0321
///   item 4). Checked before the voter-set gate, so an already removed node
///   answers so whatever the gate would say.
/// - `conflict` for the node that holds the primary status: the contract
///   does not say what removing it means, so it is refused, the side that
///   changes less. Pending a decision.
/// - `conflict` while the node hosts a running instance, as the API shows
///   it: an instance its node last reported `running`, while that node is
///   `connected`. A node that is not connected reports nothing, and its
///   instances read `unknown` (`api::vms::vm_as_seen`, issue #342).
/// - the voter-set gate ([`crate::session::NodeHub::voter_removal_gate`]).
async fn refuse(
    state: &AppState,
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<VoterRemoval, ApiError> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT status, cp_status FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((status, cp_status)) = row else {
        return Err(ApiError::not_found("node"));
    };
    let set = state.hub.voter_set_on_write(conn).await?;
    if status == "removed"
        && crate::session::voter_removal(&set, node_id) != (VoterRemoval::Repairs { in_log: true })
    {
        return Err(ApiError::conflict("the node is already removed"));
    }
    if cp_status.as_deref() == Some("primary") {
        return Err(ApiError::conflict(
            "the node holds the primary status; it cannot be removed",
        ));
    }
    let running: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM vms JOIN nodes ON nodes.id = vms.node_id \
         WHERE vms.node_id = ? AND nodes.status = 'connected' \
         AND vms.observed_status = 'running')",
    )
    .bind(node_id)
    .fetch_one(&mut *conn)
    .await?;
    if running {
        return Err(ApiError::conflict(
            "the node still hosts running instances; stop or move them first",
        ));
    }
    state.hub.voter_removal_gate(&set, node_id)
}

/// Every lease row that names `node_id` and that the node has not released
/// at its token: a row that is not revoked, or a revoked one without an
/// `ok` answer at its token or an operator's acknowledgement pending
/// delivery at it ([`leases::released_by_holder`]). Oldest first.
async fn unreleased(
    state: &AppState,
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Vec<Lease>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {LEASE_COLUMNS} FROM leases WHERE holder_node_id = ? ORDER BY id"
    ))
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut held = Vec::new();
    for row in &rows {
        let lease = lease_from_row(row)?;
        if !lease.is_revoked()
            || !leases::released_by_holder(conn, state.hub.releases(), &lease).await?
        {
            held.push(lease);
        }
    }
    Ok(held)
}

/// Section 10 for a connected node: revokes in one transaction every lease
/// it has not released ([`unreleased`]) that is not revoked yet, with its
/// change-log entry and its `lease.revoke` system record, then sends a
/// `lease_revoke` at the row's token for each, the revoked ones before too,
/// and waits for every answer ([`wait_fenced`]). The refusals are checked
/// first, under the writer, so a refused removal revokes nothing. Returns
/// whether it wrote a revoke.
///
/// Every `lease_revoke` follows a committed write of its row (as the other
/// revokes of the hub do), so a renewal or a regrant on the session reads
/// the row revoked.
async fn fence(state: &AppState, ctx: &CallCtx, node_id: &str) -> Result<bool, ApiError> {
    let mut w = state.store.begin_write().await?;
    refuse(state, &mut w.tx, node_id).await?;
    let rows = unreleased(state, &mut w.tx, node_id).await?;
    if rows.is_empty() {
        return Ok(false);
    }
    let actor = ctx.identity.actor();
    let mut wrote = false;
    let mut revokes = Vec::with_capacity(rows.len());
    for mut lease in rows {
        if !lease.is_revoked() {
            let now = crate::store::now_rfc3339();
            sqlx::query("UPDATE leases SET revoked_at = ? WHERE id = ?")
                .bind(&now)
                .bind(&lease.id)
                .execute(&mut *w.tx)
                .await?;
            lease.revoked_at = Some(now);
            let seq = changelog::append(
                &mut w.tx,
                &actor,
                "lease",
                &lease.id,
                Op::Update,
                lease.payload(),
            )
            .await?;
            // audit-log-format.md sections 3.5 and 3.7: a lease revoked by
            // the system is recorded with its node, token and reason, as a
            // failover's revoke is. Only app leases exist (`leases`).
            if lease.resource_type == "app" {
                crate::audit::append(
                    &mut w.tx,
                    &crate::backups::restore::system_record(
                        "lease.revoke",
                        &lease.resource_id,
                        json!({
                            "node_id": node_id,
                            "fencing_token": lease.fencing_token,
                            "reason": REVOKE_REASON_TEXT,
                        }),
                        Some(seq),
                    ),
                )
                .await?;
            }
            wrote = true;
        }
        revokes.push(lease);
    }
    w.commit().await?;
    for lease in &revokes {
        state.hub.send_control(
            node_id,
            &ControlMessage::LeaseRevoke(lease.revoke(REVOKE_REASON)),
        );
    }
    let ids: Vec<String> = revokes.into_iter().map(|lease| lease.id).collect();
    match wait_fenced(state, node_id, &ids).await {
        Ok(()) => Ok(wrote),
        Err(mut error) if wrote => {
            if let Some(detail) = error.detail.as_mut() {
                detail.push_str("; ");
                detail.push_str(STAYS_REVOKED);
            }
            Err(error)
        }
        Err(error) => Err(error),
    }
}

/// Waits, for at most [`REVOKE_WAIT`], until the node released every lease
/// of `lease_ids` that still names it ([`fenced`]).
///
/// # Errors
///
/// `conflict` when a fence is not confirmed (see [`fenced`]), or when the
/// node has not answered every revoke in time.
async fn wait_fenced(
    state: &AppState,
    node_id: &str,
    lease_ids: &[String],
) -> Result<(), ApiError> {
    // Test seam: a wait a test can outlast.
    let wait = if state.hub.take_fault(&Fault::ShortRemovalWait {
        node_id: node_id.to_owned(),
    }) {
        Duration::from_secs(1)
    } else {
        REVOKE_WAIT
    };
    let deadline = Instant::now() + wait;
    loop {
        // Registered before the checks, so an answer between a check and
        // the wait still wakes it.
        let notified = state.hub.revoke_answers().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if fenced(state, node_id, lease_ids).await? {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ApiError::conflict(
                "the node has not answered every lease_revoke yet",
            ));
        }
        tokio::select! {
            () = &mut notified => {}
            () = tokio::time::sleep(RECHECK.min(deadline - now)) => {}
        }
    }
}

/// One check of [`wait_fenced`]: `true` once the node released every lease
/// of `lease_ids` that still names it, `false` while a revoke waits for its
/// answer on its live session.
///
/// Only the node's `ok` answer at the row's token releases a lease here
/// ([`leases::released_by_holder`]). Neither the node's removal nor a
/// declaration counts, since the node is not removed yet: whatever is not
/// confirmed stays held, and the call removes nothing.
///
/// # Errors
///
/// `conflict` when a lease's fence failed on this node (`fence_failed`,
/// recorded or not), when another node's failed fence lists the lease (the
/// refusal names that node), when the session ended before the node
/// answered, or when the node refused a revoke or holds a lease again.
async fn fenced(state: &AppState, node_id: &str, lease_ids: &[String]) -> Result<bool, ApiError> {
    let hub = &state.hub;
    let mut conn = state.store.pool().acquire().await?;
    // Read before the rows: a revoke still pending on a session that ends
    // after this read is not taken for a refusal.
    let connected = hub.is_connected(node_id);
    let mut waiting = false;
    for lease_id in lease_ids {
        let Some(lease) = leases::by_id(&mut conn, lease_id).await? else {
            continue;
        };
        if lease.holder_node_id != node_id {
            continue;
        }
        // The in-memory failures before the rows, as
        // `fences::failed_leases` reads them: one recorded in between is in
        // one of the two.
        let unrecorded = hub.unrecorded_failures().of_node(node_id, lease_id);
        let unanswered = hub.revoke_unanswered(node_id, lease_id);
        let listing = crate::fences::nodes_listing(&mut conn, lease_id).await?;
        if listing.iter().any(|id| id == node_id) {
            return Err(ApiError::conflict(format!(
                "the node could not confirm that the VMs of {lease_id} stopped (fence_failed): \
                 acknowledge the failed fence first"
            )));
        }
        // Not in `fence_failure` yet, so `ackNodeFence` would find nothing
        // to acknowledge. The check runs before the node answers a revoke
        // this attempt sent again, so a retry while the node is connected is
        // refused here too; the node's answer to that revoke records the
        // failure, and a later attempt finds it. A removal that goes on
        // writes every unrecorded failure in its own transaction
        // ([`record_unrecorded`]).
        if unrecorded {
            return Err(ApiError::conflict(format!(
                "the node could not confirm that the VMs of {lease_id} stopped (fence_failed), \
                 and the primary has not recorded that failure yet, so there is no failed fence \
                 to acknowledge; a retry sends the lease_revoke again, and the node's answer to it \
                 records the failure for the attempt after that"
            )));
        }
        // Section 8.5: a lease another node's failed fence lists is granted
        // to no node until an operator acknowledges it; the removal waits
        // for that too, the side that changes less.
        if let Some(other) = listing.first() {
            return Err(ApiError::conflict(format!(
                "{lease_id} is listed in the failed fence of node {other} (fence_failed): \
                 acknowledge that failed fence first"
            )));
        }
        if lease.is_revoked()
            && !unanswered
            && leases::released_by_holder(&mut conn, hub.releases(), &lease).await?
        {
            continue;
        }
        if !connected {
            return Err(ApiError::conflict(format!(
                "the node's session ended before it answered the lease_revoke of {lease_id}"
            )));
        }
        if !unanswered {
            return Err(ApiError::conflict(format!(
                "the node did not release {lease_id}: it refused the lease_revoke or holds the \
                 lease again"
            )));
        }
        waiting = true;
    }
    Ok(!waiting)
}

/// The node's failed fences that the primary could not record
/// ([`crate::fences::UnrecordedFailures`]), written into its
/// `fence_failure` on `conn`, the removal's transaction, as the session
/// records a `fence_failed` answer: the lease and its app, no VM. Until now
/// they lived only in memory, and a restart of the primary after the
/// removal would lose them and let [`leases::is_released`] free a lease
/// whose VMs may still run (section 8.5 step 1). A failed write fails the
/// removal: nothing is removed. Returns the leases written, which the caller
/// notes recorded once the transaction committed.
///
/// Only the row is written here ([`crate::fences::write_failure_row`]); the
/// removal entry, the whole row, carries the failure into the change log.
/// One entry for both means that no prefix of the log, which a promoted
/// follower may hold, has the node removed without its failure. It also
/// keeps the voter-set version on the removal entry: a separate whole-row
/// entry before it would already carry the `status` or `roles` the rows
/// hold in the repair of ADR 0321 item 4, and move the version itself
/// ([`crate::session::NodeHub::voter_removal_landed`]).
///
/// # Errors
///
/// A database error, or the test seam [`Fault::RemovalFailureRecord`].
async fn record_unrecorded(
    state: &AppState,
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Vec<String>, ApiError> {
    let lease_ids = state.hub.unrecorded_failures().leases_of_node(node_id);
    if lease_ids.is_empty() {
        return Ok(lease_ids);
    }
    if state.hub.take_fault(&Fault::RemovalFailureRecord {
        node_id: node_id.to_owned(),
    }) {
        return Err(ApiError::internal(&"a test seam failed the record"));
    }
    let mut app_ids = Vec::new();
    for lease_id in &lease_ids {
        if let Some(lease) = leases::by_id(conn, lease_id).await?
            && lease.resource_type == "app"
        {
            app_ids.push(lease.resource_id);
        }
    }
    crate::fences::write_failure_row(
        conn,
        node_id,
        &crate::fences::Failed {
            lease_ids: &lease_ids,
            app_ids: &app_ids,
            vm_ids: &[],
        },
    )
    .await?;
    Ok(lease_ids)
}

/// The removal, in one transaction (step 3 of the [module docs](self)).
/// The refusals run first, under the writer. Returns the node as `getNode`
/// shows it.
async fn remove(
    state: &AppState,
    ctx: &CallCtx,
    node_id: &str,
    params: &Value,
) -> Result<Node, ApiError> {
    let mut w = state.store.begin_write().await?;
    let removal = refuse(state, &mut w.tx, node_id).await?;
    let actor = ctx.identity.actor();
    let recorded = record_unrecorded(state, &mut w.tx, node_id).await?;
    // The predicate `failoverApp` and `declareNodeLost` use for a
    // connected node, read under the writer: a session that attaches after
    // it cannot record its `hello` before this commits, and is then
    // refused (`session::connect_node`).
    if state.hub.is_connected(node_id) {
        // Section 10: a connected node's leases are revoked over its
        // session first ([`fence`]); one it has not released by now was
        // granted, or answered, since.
        if let Some(lease) = unreleased(state, &mut w.tx, node_id).await?.first() {
            return Err(ApiError::conflict(format!(
                "the node holds {} and has not released it; try again",
                lease.id
            )));
        }
    } else {
        // Section 10: a node that is not connected has the token of each of
        // its leases raised, as declaring it lost does (section 8.3). That
        // is every row that names it and that it has not released at its
        // token ([`unreleased`]), a revoked one included: a revoke of an
        // earlier attempt that its session ended before answering left the
        // row revoked at the token the node may still run under.
        // `bump_tokens` skips a row another node holds.
        //
        // A row already revoked at the cap ([`leases::MAX_TOKEN`]) is left
        // as it is: no grant ever issues a token past the cap, so no other
        // holder can come after the node, and the row is never granted
        // again (section 8.6, "The cap"). Raising it could never succeed,
        // and would block the removal of a dead node on every retry. An
        // unrevoked row at the cap is still raised: the token cap of ADR
        // 0325 refuses that removal once PR #903 has added its check to
        // `bump_tokens`.
        let held: Vec<String> = unreleased(state, &mut w.tx, node_id)
            .await?
            .into_iter()
            .filter(|lease| !(lease.is_revoked() && lease.fencing_token >= leases::MAX_TOKEN))
            .map(|lease| lease.id)
            .collect();
        leases::bump_tokens(&mut w.tx, &held, node_id, &actor).await?;
    }
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(node_id)
        .execute(&mut *w.tx)
        .await?;
    // The removal entry: the whole row, `status` and `roles` included, so
    // the voter set's replay and the required set of section 8.4 read the
    // machine removed from this `seq` on (ADR 0261, ADR 0280).
    let payload = crate::driver::node_payload(&mut w.tx, node_id).await?;
    let seq = changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload).await?;
    state
        .hub
        .voter_removal_landed(&mut w.tx, removal, seq)
        .await?;
    crate::quarantines::drop_node_copies(&mut w.tx, &actor, node_id).await?;
    crate::custody::delete_seals(&mut w.tx, node_id)
        .await
        .map_err(|e| ApiError::internal(&e))?;
    let target = Target {
        kind: "node".to_string(),
        id: node_id.to_string(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::NodeRemove),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    let node = node_view(&mut w.tx, state, node_id).await?;
    // Still under the writer: a check that waits on it behind this removal
    // reads the node not connected, although its session ends only after
    // the commit. Undone if the commit fails.
    state.hub.mark_removed(node_id);
    if let Err(error) = w.commit().await {
        state.hub.unmark_removed(node_id);
        return Err(error.into());
    }
    for lease_id in &recorded {
        state.hub.note_failure_recorded(node_id, lease_id, true);
    }
    if let Some(hold) = state.hub.take_removal_hold(node_id, RemovalStep::Committed) {
        hold.hold().await;
    }
    crate::quarantines::forget_listed(&state.hub, node_id);
    state.hub.node_removed(node_id);
    Ok(node)
}

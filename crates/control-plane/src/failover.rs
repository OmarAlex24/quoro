// SPDX-License-Identifier: AGPL-3.0-only
//! `failoverApp`: moving a `warm` app's lease to its warm node (`openapi.yaml`
//! `failoverApp`, `node-agent-protocol.md` sections 6.13 and 8.4, ADR 0175).
//!
//! The call runs in four steps. Every refusal of step 1 comes before any
//! write; the steps after it check what may have changed again:
//!
//! 1. **Refusals**, in the contract's order ([`refuse`]).
//! 2. **The revoke.** When the source is connected and its lease is not
//!    revoked yet, one transaction checks the refusals that read the database
//!    again, revokes the lease (reason `moved`), stops the app's instances
//!    and writes the `lease.revoke` system record; then the revoke goes out
//!    on the source's session, followed by the snapshots that stop the app.
//! 3. **The wait** ([`wait_for_release`]) until the source released the
//!    lease: it answered the revoke `ok`, or, not connected, it can no longer
//!    hold it (section 8.4, "Moving the lease").
//! 4. **The change**, one transaction ([`change`]): the warm node's copy is
//!    checked again, the warm node holds the lease `expiring` with a higher
//!    token, the volume and the instances move there, the old source becomes
//!    the warm node, `app.failed_over` is emitted. After the commit the warm
//!    node gets its grant, then every node concerned its new desired state,
//!    and nothing after the commit fails the call.
//!
//! A refusal after this call revoked the source says that the source's lease
//! stays revoked and its app stopped: a retry goes on from there.
//!
//! **Held grants.** This build sends a failover's grant only when
//! [`leases::grant_app_lease`] can grant at once: the row carries no unsettled
//! mark and already reads `expiring` for at least `D`, and the primary holds
//! majority support. A grant that would wait for its write to be replicated
//! over the machines with the `node` role ([`leases::NewGrant::Waiting`]) is
//! refused with `no_support` before anything is written ([`held_grant`]),
//! where ADR 0292 would answer `202` and send the grant later. The shared
//! machinery that holds such a grant is not built yet. Support that lapses
//! after the source answered its revoke is found only by the grant, so that
//! call also answers `no_support`, with the source's lease revoked; a retry
//! goes straight to the grant.
//!
//! **One at a time.** A failover of an app holds [`Running`] from its
//! `in_progress` check until it answers, so a second call of the same app
//! answers `in_progress`. The set is in memory only, as the call is.

use crate::AppState;
use crate::api::apps::{
    APP_COLUMNS, App, AppPlacement, AppStatus, HaPolicy, app_from_row, app_payload, app_target,
};
use crate::api::volumes::{VOLUME_COLUMNS, Volume, volume_from_row};
use crate::authority::{Authority, DEFAULT_DURATION_MS};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Reason};
use crate::events::{self, NewEvent, Source};
use crate::leases::{self, Lease, NewGrant};
use crate::request::CallCtx;
use crate::store::now_rfc3339;
use quoro_proto::RevokeReason;
use quoro_proto::wire::{ControlMessage, WarmRole};
use quoro_types::Scope;
use serde_json::{Value, json};
use sqlx::SqliteConnection;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::time::Instant;

/// How long the call waits for the source to answer its revoke.
const REVOKE_WAIT: Duration = Duration::from_secs(180);

/// How often the wait checks again when nothing wakes it.
const RECHECK: Duration = Duration::from_millis(250);

/// How many times the change waits for an open gate before it gives up.
const GATE_TRIES: u32 = 10;

/// The apps whose failover is running (`in_progress`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Running(Arc<Mutex<HashSet<String>>>);

impl Running {
    /// Marks `app_id`'s failover running until the guard drops, or `None`
    /// when one already runs.
    fn start(&self, app_id: &str) -> Option<RunningGuard> {
        let mut running = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        running.insert(app_id.to_string()).then(|| RunningGuard {
            running: Arc::clone(&self.0),
            app_id: app_id.to_string(),
        })
    }
}

/// Holds an app's failover running ([`Running::start`]).
struct RunningGuard {
    running: Arc<Mutex<HashSet<String>>>,
    app_id: String,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.app_id);
    }
}

/// A `409 conflict` with `reason`.
fn conflict(reason: Reason, detail: impl Into<String>) -> ApiError {
    ApiError {
        reason: Some(reason),
        ..ApiError::conflict(detail)
    }
}

/// What a failover the refusals let through moves.
struct Plan {
    app_id: String,
    /// The warm node, which gets the lease.
    warm: String,
    /// The volume's node, which holds the lease.
    source: String,
    /// The app's `ready` volume, on `source`.
    volume_id: String,
    /// The app's lease as the refusals read it.
    lease: Lease,
}

/// What the change moved, for what follows its commit.
struct Moved {
    lease: Lease,
    /// The nodes the app's instances left.
    vm_nodes: Vec<String>,
    /// The app as the change wrote it, without its cost, lease and warm
    /// views.
    app: App,
}

/// What a refusal after this call revoked the source adds to its detail.
const STAYS_REVOKED: &str = "the source's lease stays revoked and its app stopped, and a retry \
                             goes on from there";

/// Runs `failoverApp` for `app_id` onto `node_id` after the authorization
/// check, and answers the app as `getApp` shows it.
///
/// # Errors
///
/// `not_found` for an unknown app, `validation_failed` for a `node_id` that
/// is not the app's warm node, `conflict` with the contract's reasons, or a
/// database error.
pub(crate) async fn run(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    node_id: &str,
    params: &Value,
) -> Result<App, ApiError> {
    if !crate::deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let app = app_from_row(&row)?;
    if app.ha_policy != HaPolicy::Warm {
        return Err(conflict(Reason::NotWarm, "the app's ha_policy is not warm"));
    }
    let Some(_running) = state.failovers.start(app_id) else {
        return Err(conflict(
            Reason::InProgress,
            "a failover of the app is already running",
        ));
    };
    let plan = refuse(state, &app, node_id).await?;

    let revoked = state.hub.is_connected(&plan.source)
        && !plan.lease.is_revoked()
        && revoke(state, ctx, &plan).await?;
    let moved = match release_and_change(state, ctx, &plan, params).await {
        Ok(moved) => moved,
        Err(error) if revoked => return Err(stays_revoked(state, &plan, error).await),
        Err(error) => return Err(error),
    };

    // The move is committed: nothing below fails the call.
    // The grant reaches the warm node before the snapshot that starts the
    // app there (section 6.13), as a restore's does (section 8.3).
    state
        .hub
        .send_control(&plan.warm, &ControlMessage::LeaseGrant(moved.lease.grant()));
    let mut wake = vec![plan.warm.clone(), plan.source.clone()];
    wake.extend(moved.vm_nodes);
    for node_id in crate::warm::with_copy_holders(state, app_id, wake) {
        state.hub.node_changed(&node_id);
    }
    Ok(answer(state, moved.app).await)
}

/// The wait for the source's release, then the change.
async fn release_and_change(
    state: &AppState,
    ctx: &CallCtx,
    plan: &Plan,
    params: &Value,
) -> Result<Moved, ApiError> {
    wait_for_release(state, plan).await?;
    change(state, ctx, plan, params).await
}

/// `error`, a refusal after this call revoked the source, with
/// [`STAYS_REVOKED`] added when the lease row still reads revoked at the
/// plan's holder and token. The read is best-effort: it never replaces
/// `error`.
async fn stays_revoked(state: &AppState, plan: &Plan, mut error: ApiError) -> ApiError {
    if error.reason.is_none() {
        return error;
    }
    let lease = match state.store.pool().acquire().await {
        Ok(mut conn) => leases::app_lease(&mut conn, &plan.app_id)
            .await
            .ok()
            .flatten(),
        Err(_) => None,
    };
    let still = lease.is_some_and(|lease| {
        lease.is_revoked()
            && lease.holder_node_id == plan.source
            && lease.fencing_token == plan.lease.fencing_token
    });
    if still && let Some(detail) = error.detail.as_mut() {
        detail.push_str("; ");
        detail.push_str(STAYS_REVOKED);
    }
    error
}

/// The refusals of `failoverApp` that follow `not_warm` and `in_progress`,
/// in the contract's order, before anything is written.
async fn refuse(state: &AppState, app: &App, node_id: &str) -> Result<Plan, ApiError> {
    let mut conn = state.store.pool().acquire().await?;
    let warm_node_id: Option<String> =
        sqlx::query_scalar("SELECT warm_node_id FROM apps WHERE id = ?")
            .bind(&app.id)
            .fetch_one(&mut *conn)
            .await?;
    if warm_node_id.as_deref() != Some(node_id) {
        return Err(ApiError::validation(
            "node_id is not the app's warm node (App.warm.node_id)",
        ));
    }
    let volume = ready_volume(&mut conn, &app.id)
        .await?
        .ok_or_else(|| ApiError::conflict("the app's volume is not ready"))?;
    let source = volume.node_id.clone();

    if complete_copy(state, node_id, &volume.id, &app.id).is_none() {
        return Err(conflict(
            Reason::NoCopy,
            "the warm node has reported no complete copy of the app's volume",
        ));
    }

    if !state.hub.is_connected(node_id) {
        return Err(conflict(
            Reason::NodeDisconnected,
            "the warm node is not connected",
        ));
    }

    let lease = leases::app_lease(&mut conn, &app.id).await?;
    if let Some(lease) = &lease
        && crate::fences::listed_leases(&mut conn)
            .await?
            .contains(&lease.id)
    {
        return Err(conflict(
            Reason::FenceFailed,
            "the app's lease has a failed fence that no operator acknowledged",
        ));
    }
    if crate::fences::failure_of(&mut conn, node_id)
        .await?
        .is_some()
    {
        return Err(conflict(
            Reason::FenceFailed,
            "the warm node has a failed fence that no operator acknowledged",
        ));
    }

    let authority = state.hub.authority();
    if !authority.supports(DEFAULT_DURATION_MS) {
        return Err(conflict(
            Reason::NoSupport,
            "the primary does not hold the support of a majority of voters, so it cannot grant \
             an expiring lease",
        ));
    }

    if let Some(lease) = &lease
        && !state.hub.is_connected(&source)
        && let Some(why) = held_while_away(&mut conn, authority, lease).await?
    {
        return Err(conflict(Reason::LeaseHeld, why));
    }

    if let Some(lease) = &lease
        && let Some(held) = held_grant(lease, leases::is_marked(&mut conn, &lease.id).await?)
    {
        return Err(conflict(Reason::NoSupport, held.detail()));
    }

    let Some(lease) = lease else {
        return Err(ApiError::conflict("the app has no lease to move"));
    };
    // The volume's node holds the app's lease (section 6.13); a row that
    // names another node is not one this call can move.
    if lease.holder_node_id != source {
        return Err(ApiError::conflict(
            "the app's lease is not held by the node of its volume",
        ));
    }
    Ok(Plan {
        app_id: app.id.clone(),
        warm: node_id.to_string(),
        source,
        volume_id: volume.id,
        lease,
    })
}

/// The complete copy of `volume_id` that `warm` reports for `app_id`: its
/// `snapshot_seq`, and when the source took that snapshot. The warm node's
/// own report, not `App.warm.state`: a complete copy is one with a
/// `snapshot_seq` (section 6.13).
fn complete_copy(
    state: &AppState,
    warm: &str,
    volume_id: &str,
    app_id: &str,
) -> Option<(u64, Option<String>)> {
    state
        .hub
        .warm_reports()
        .get(warm)?
        .iter()
        .find_map(|entry| {
            let target = entry.role == WarmRole::Target
                && entry.volume_id == volume_id
                && entry.app_id == app_id;
            target.then_some(())?;
            Some((entry.snapshot_seq?, entry.snapshot_taken_at.clone()))
        })
}

/// The app's row and its `warm_node_id`.
async fn app_and_warm(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<(App, Option<String>), ApiError> {
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let app = app_from_row(&row)?;
    let warm_node_id: Option<String> =
        sqlx::query_scalar("SELECT warm_node_id FROM apps WHERE id = ?")
            .bind(app_id)
            .fetch_one(&mut *conn)
            .await?;
    Ok((app, warm_node_id))
}

/// A `fence_failed` for a source that is not connected and answered the
/// revoke `fence_failed` while the primary could not record it: its VMs may
/// still run, so the lease does not move (section 8.5, issue #392).
fn unrecorded_fence_failure() -> ApiError {
    conflict(
        Reason::FenceFailed,
        "the source answered the revoke fence_failed and the primary could not record it yet",
    )
}

/// Why a source that is not connected may still hold `lease`, so the lease
/// cannot move yet (`lease_held`, section 8.4, "Moving the lease"):
///
/// - a row that carries the unsettled mark, or reads `local`, moves only
///   through a revoke its holder answered or once its holder is declared
///   lost;
/// - a row that reads `expiring` moves once its gate has passed on the
///   authority's clock.
async fn held_while_away(
    conn: &mut SqliteConnection,
    authority: &Authority,
    lease: &Lease,
) -> Result<Option<&'static str>, ApiError> {
    let marked = leases::is_marked(conn, &lease.id).await?;
    if marked || lease.mode == "local" {
        let lost_at: Option<Option<String>> =
            sqlx::query_scalar("SELECT lost_at FROM nodes WHERE id = ?")
                .bind(&lease.holder_node_id)
                .fetch_optional(&mut *conn)
                .await?;
        if lost_at.flatten().is_none() {
            return Ok(Some(if marked {
                "the source is not connected and the lease row carries the unsettled mark: it \
                 moves only once its holder answers a revoke or is declared lost"
            } else {
                "the source is not connected and its lease is local: it moves only once the \
                 source answers a revoke or is declared lost"
            }));
        }
    }
    if lease.mode == "expiring" && !authority.passed(&lease.id) {
        return Ok(Some(
            "the source is not connected and the lease's gate has not passed: the source may \
             still hold it",
        ));
    }
    Ok(None)
}

/// Why the warm node's grant would wait for its write to be replicated over
/// the machines with the `node` role ([`leases::NewGrant::Waiting`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Held {
    /// The row carries the unsettled mark (section 8.4, "Grants of a marked
    /// row", ADR 0280).
    Mark,
    /// The row does not read `expiring` for at least `D`, so the grant raises
    /// the stored duration (section 8.4, ADR 0268).
    Raise,
}

impl Held {
    fn detail(self) -> String {
        match self {
            Self::Mark => "the lease row carries the unsettled mark, so the grant to the warm \
                           node waits until the row's newest write is replicated over every \
                           machine with the node role, which the primary does not do for a \
                           failover yet"
                .to_string(),
            Self::Raise => format!(
                "the lease row does not read expiring for at least {DEFAULT_DURATION_MS} ms, so \
                 the grant to the warm node raises the stored duration and waits until that \
                 write is replicated over every machine with the node role, which the primary \
                 does not do for a failover yet"
            ),
        }
    }
}

/// Whether [`leases::grant_app_lease`] would hold the warm node's grant
/// with majority support, from the same conditions it reads: the mark, and
/// a raise of the stored duration. A `warm` app's new token is always
/// `expiring`, so these are the only two. A guess that misses is still
/// caught by the grant itself, which answers `no_support` then.
fn held_grant(lease: &Lease, marked: bool) -> Option<Held> {
    if marked {
        return Some(Held::Mark);
    }
    let d = i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX);
    let stores_d = lease.mode == "expiring" && lease.duration_ms.is_some_and(|stored| stored >= d);
    (!stores_d).then_some(Held::Raise)
}

/// The app's `ready` volume, first by id.
async fn ready_volume(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<Option<Volume>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {VOLUME_COLUMNS} FROM volumes WHERE app_id = ? AND state = 'ready' \
         ORDER BY id LIMIT 1"
    ))
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    row.as_ref().map(volume_from_row).transpose()
}

/// The newest change-log `seq`, read inside the writer's transaction right
/// after its own append, so it is that append's.
async fn newest_seq(conn: &mut SqliteConnection) -> Result<Option<i64>, ApiError> {
    Ok(sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(&mut *conn)
        .await?)
}

/// Revokes the source's lease with reason `moved`, stops the app's
/// instances as `stopApp` does, and writes the `lease.revoke` system record
/// (`audit-log-format.md` section 3.5), in one transaction. Then it sends the
/// revoke on the source's session (section 5.13), and only after it the
/// snapshots that stop the app, as a restore does (section 8.3).
///
/// Stopping the instances here means a revoke that does not lead to the
/// move (the source refuses it, fails its fence, leaves, or support lapses
/// before the grant) leaves the app stopped. Otherwise the source's next
/// desired state would name the VM `running` without its lease, so without
/// its volume. The change's [`crate::backups::restore::move_vms`] turns the
/// instances `running` on the warm node.
///
/// The refusals that read the database are checked again under the writer
/// first, with their reasons, so a held grant is still refused before any
/// revoke. Returns whether this call revoked the lease.
async fn revoke(state: &AppState, ctx: &CallCtx, plan: &Plan) -> Result<bool, ApiError> {
    let mut w = state.store.begin_write().await?;
    let (app, warm_node_id) = app_and_warm(&mut w.tx, &plan.app_id).await?;
    if app.ha_policy != HaPolicy::Warm {
        return Err(conflict(Reason::NotWarm, "the app's ha_policy is not warm"));
    }
    if warm_node_id.as_deref() != Some(plan.warm.as_str()) {
        return Err(ApiError::validation(
            "node_id is not the app's warm node (App.warm.node_id)",
        ));
    }
    let volume = ready_volume(&mut w.tx, &plan.app_id).await?;
    if volume.is_none_or(|volume| volume.id != plan.volume_id || volume.node_id != plan.source) {
        return Err(ApiError::conflict(
            "the app's volume changed while the failover started; try again",
        ));
    }
    let Some(current) = leases::app_lease(&mut w.tx, &plan.app_id)
        .await?
        .filter(|lease| {
            lease.holder_node_id == plan.source && lease.fencing_token == plan.lease.fencing_token
        })
    else {
        return Err(ApiError::conflict(
            "the app's lease changed while the failover started; try again",
        ));
    };
    if let Some(held) = held_grant(&current, leases::is_marked(&mut w.tx, &current.id).await?) {
        return Err(conflict(Reason::NoSupport, held.detail()));
    }
    let Some(lease) =
        leases::revoke_app_lease(&mut w.tx, &plan.app_id, &ctx.identity.actor()).await?
    else {
        // Revoked meanwhile: the wait finds out whether it was released.
        return Ok(false);
    };
    let change_seq = newest_seq(&mut w.tx).await?;
    let vm_nodes =
        crate::backups::restore::stop_app(&mut w.tx, &plan.app_id, &ctx.identity.actor()).await?;
    crate::audit::append(
        &mut w.tx,
        &crate::backups::restore::system_record(
            "lease.revoke",
            &plan.app_id,
            json!({
                "node_id": lease.holder_node_id,
                "fencing_token": lease.fencing_token,
                "reason": "moved",
            }),
            change_seq,
        ),
    )
    .await?;
    w.commit().await?;
    // The revoke reaches the source before the snapshot that stops the app
    // there (section 8.3).
    state.hub.send_control(
        &plan.source,
        &ControlMessage::LeaseRevoke(lease.revoke(RevokeReason::Moved)),
    );
    for node_id in &vm_nodes {
        state.hub.node_changed(node_id);
    }
    Ok(true)
}

/// Waits until the lease may move to the warm node, for at most
/// [`REVOKE_WAIT`]. It may once the source released it: it answered the
/// revoke `ok`, or another way of [`leases::is_released`]; or, once the
/// source is not connected, when [`held_while_away`] finds nothing.
///
/// # Errors
///
/// `fence_failed` when the source answered the revoke `fence_failed`;
/// `lease_held` when the source refused the revoke, lost its session while
/// it may still hold the lease, or did not answer in time.
async fn wait_for_release(state: &AppState, plan: &Plan) -> Result<(), ApiError> {
    let deadline = Instant::now() + REVOKE_WAIT;
    loop {
        // Registered before the checks, so an answer between a check and
        // the wait still wakes it.
        let notified = state.hub.revoke_answers().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if released(state, plan).await? {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(conflict(
                Reason::LeaseHeld,
                "the source has not answered the revoke yet",
            ));
        }
        tokio::select! {
            () = &mut notified => {}
            () = tokio::time::sleep(RECHECK.min(deadline - now)) => {}
        }
    }
}

/// One check of [`wait_for_release`]: `true` once the lease may move,
/// `false` while the revoke waits for its answer.
async fn released(state: &AppState, plan: &Plan) -> Result<bool, ApiError> {
    let hub = &state.hub;
    let mut conn = state.store.pool().acquire().await?;
    let lease = leases::app_lease(&mut conn, &plan.app_id)
        .await?
        .ok_or_else(|| ApiError::conflict("the app has no lease to move"))?;
    // Read together, the session first: a revoke still pending on a session
    // that ends after this read is not taken for an unrecorded failure.
    let connected = hub.is_connected(&plan.source);
    let unanswered = hub.revoke_unanswered(&plan.source, &lease.id);
    if leases::is_released(
        &mut conn,
        hub.releases(),
        hub.authority(),
        unanswered,
        &lease,
    )
    .await?
    {
        return Ok(true);
    }
    if crate::fences::listed_leases(&mut conn)
        .await?
        .contains(&lease.id)
    {
        return Err(conflict(
            Reason::FenceFailed,
            "the source could not confirm that the app stopped (fence_failed): the lease waits \
             for an operator's acknowledgement",
        ));
    }
    // Without a session nothing is pending, so what stays unanswered is a
    // failed fence the primary could not record.
    if unanswered && !connected {
        return Err(unrecorded_fence_failure());
    }
    if !connected {
        return match held_while_away(&mut conn, hub.authority(), &lease).await? {
            None => Ok(true),
            Some(why) => Err(conflict(Reason::LeaseHeld, why)),
        };
    }
    if !unanswered {
        return Err(conflict(
            Reason::LeaseHeld,
            "the source did not release the lease: it refused the revoke or holds the lease \
             again",
        ));
    }
    Ok(false)
}

/// The change that moves the lease, in one transaction: the grant to the
/// warm node, the volume, the instances, the app, `app.failed_over` and the
/// audit records. An open gate rolls it back and tries again once it passed,
/// at most [`GATE_TRIES`] times.
async fn change(
    state: &AppState,
    ctx: &CallCtx,
    plan: &Plan,
    params: &Value,
) -> Result<Moved, ApiError> {
    let hub = &state.hub;
    let actor = ctx.identity.actor();
    for _ in 0..GATE_TRIES {
        let mut w = state.store.begin_write().await?;
        let (mut app, warm_node_id) = app_and_warm(&mut w.tx, &plan.app_id).await?;
        let volume = ready_volume(&mut w.tx, &plan.app_id).await?;
        let Some(mut volume) =
            volume.filter(|volume| volume.id == plan.volume_id && volume.node_id == plan.source)
        else {
            return Err(conflict(
                Reason::InProgress,
                "the app's volume moved while the failover waited",
            ));
        };
        if app.ha_policy != HaPolicy::Warm || warm_node_id.as_deref() != Some(plan.warm.as_str()) {
            return Err(conflict(
                Reason::InProgress,
                "the app's warm settings changed while the failover waited",
            ));
        }

        // The lease may still move: checked again under the writer gate,
        // so a source that came back and was given its lease again in the
        // meantime keeps it.
        let lease = leases::app_lease(&mut w.tx, &plan.app_id)
            .await?
            .ok_or_else(|| ApiError::conflict("the app has no lease to move"))?;
        if lease.holder_node_id != plan.source || lease.fencing_token != plan.lease.fencing_token {
            return Err(conflict(
                Reason::LeaseHeld,
                "the app's lease changed while the failover waited",
            ));
        }
        if crate::fences::listed_leases(&mut w.tx)
            .await?
            .contains(&lease.id)
        {
            return Err(conflict(
                Reason::FenceFailed,
                "the app's lease has a failed fence that no operator acknowledged",
            ));
        }
        // Read together, the session first, as in `released`.
        let connected = hub.is_connected(&plan.source);
        let unanswered = hub.revoke_unanswered(&plan.source, &lease.id);
        let free = leases::is_released(
            &mut w.tx,
            hub.releases(),
            hub.authority(),
            unanswered,
            &lease,
        )
        .await?
            || (!connected
                && !unanswered
                && held_while_away(&mut w.tx, hub.authority(), &lease)
                    .await?
                    .is_none());
        if !free {
            if unanswered && !connected {
                return Err(unrecorded_fence_failure());
            }
            return Err(conflict(
                Reason::LeaseHeld,
                "the source holds the lease again",
            ));
        }

        // The warm node may have lost its copy, its session or its fence
        // while the call waited: checked again under the writer, in the
        // refusals' order, and the event names the copy read here.
        let Some((snapshot_seq, snapshot_taken_at)) =
            complete_copy(state, &plan.warm, &plan.volume_id, &plan.app_id)
        else {
            return Err(conflict(
                Reason::NoCopy,
                "the warm node no longer reports a complete copy of the app's volume",
            ));
        };
        if !hub.is_connected(&plan.warm) {
            return Err(conflict(
                Reason::NodeDisconnected,
                "the warm node is not connected",
            ));
        }
        if crate::fences::failure_of(&mut w.tx, &plan.warm)
            .await?
            .is_some()
        {
            return Err(conflict(
                Reason::FenceFailed,
                "the warm node has a failed fence that no operator acknowledged",
            ));
        }

        let lease = match leases::grant_app_lease(
            &mut w.tx,
            hub.authority(),
            &plan.app_id,
            &plan.warm,
            &actor,
            false,
        )
        .await?
        {
            NewGrant::Granted(lease) => *lease,
            NewGrant::GateOpen(remaining) => {
                drop(w);
                tokio::time::sleep(remaining).await;
                continue;
            }
            // The interim rule of ADR 0303 Q1: a grant that must wait is
            // refused, and dropping `w` rolls its write back.
            NewGrant::Waiting(_) => {
                return Err(conflict(
                    Reason::NoSupport,
                    "the primary does not hold the support of a majority of voters, so it \
                     cannot grant the warm node an expiring lease",
                ));
            }
        };
        let lease_seq = newest_seq(&mut w.tx).await?;

        // The volume moves with the lease. Its state stays `ready`: the warm
        // node's complete copy is the volume now (section 6.13).
        volume.node_id = plan.warm.clone();
        volume.updated_at = now_rfc3339();
        sqlx::query("UPDATE volumes SET node_id = ?, updated_at = ? WHERE id = ?")
            .bind(&volume.node_id)
            .bind(&volume.updated_at)
            .bind(&volume.id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "volume",
            &volume.id,
            Op::Update,
            volume.payload()?,
        )
        .await?;

        // The standby VM turns `running` on the warm node, under the new
        // token (section 6.13).
        let vm_nodes =
            crate::backups::restore::move_vms(&mut w.tx, &plan.app_id, &plan.warm, &actor).await?;

        // The old source keeps its copy and becomes the warm node. The
        // scheduler did not pick it, and no operator pinned it.
        app.placement = AppPlacement {
            node_ids: vec![plan.warm.clone()],
        };
        app.status = AppStatus::Deploying;
        app.updated_at = now_rfc3339();
        let placement = serde_json::to_string(&app.placement)
            .map_err(|error| ApiError::internal(&format!("placement: {error}")))?;
        sqlx::query(
            "UPDATE apps SET placement = ?, status = ?, warm_node_id = ?, warm_node_pinned = 0, \
             updated_at = ? WHERE id = ?",
        )
        .bind(&placement)
        .bind(app.status.as_str())
        .bind(&plan.source)
        .bind(&app.updated_at)
        .bind(&app.id)
        .execute(&mut *w.tx)
        .await?;
        let payload = app_payload(&mut w.tx, &app).await?;
        changelog::append(&mut w.tx, &actor, "app", &app.id, Op::Update, payload).await?;

        let mut data = json!({
            "app_id": app.id,
            "from_node_id": plan.source,
            "to_node_id": plan.warm,
            "fencing_token": lease.fencing_token,
            "snapshot_seq": snapshot_seq,
            "grant_pending": false,
        });
        if let Some(taken_at) = &snapshot_taken_at {
            data["snapshot_taken_at"] = json!(taken_at);
        }
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "app.failed_over",
                source: Source::ControlPlane,
                subject_type: "app",
                subject_id: &app.id,
                project_id: Some(&app.project_id),
                request_id: Some(&ctx.request_id),
                actor: Some(&actor),
                data,
            },
        )
        .await?;

        // audit-log-format.md section 3.7: the new token's `lease.grant`
        // system record, then the call's own.
        crate::audit::append(
            &mut w.tx,
            &crate::backups::restore::system_record(
                "lease.grant",
                &app.id,
                json!({
                    "node_id": lease.holder_node_id,
                    "fencing_token": lease.fencing_token,
                    "mode": lease.mode,
                    "duration_ms": lease.duration_ms,
                }),
                lease_seq,
            ),
        )
        .await?;
        ctx.append_ok(
            &mut w.tx,
            Some(Scope::AppFailover),
            Some(&app_target(&app.id)),
            Some(&app.project_id),
            params,
            lease_seq,
        )
        .await?;
        w.commit().await?;
        return Ok(Moved {
            lease,
            vm_nodes,
            app,
        });
    }
    Err(conflict(
        Reason::LeaseHeld,
        "the lease's gate has not passed yet: the source may still hold it",
    ))
}

/// `app`, as the change wrote it, as `getApp` shows it: its cost, lease and
/// warm copy attached. The move is committed, so a view that cannot be read
/// is left out with a warning rather than failing the call.
async fn answer(state: &AppState, mut app: App) -> App {
    let apps = std::slice::from_mut(&mut app);
    if let Err(error) = crate::backups::cost::attach(state, apps).await {
        tracing::warn!(app_id = %apps[0].id, code = %error.code.as_str(), "failoverApp answers without the app's cost");
    }
    if let Err(error) = crate::api::leases::attach(state, apps).await {
        tracing::warn!(app_id = %apps[0].id, code = %error.code.as_str(), "failoverApp answers without the app's lease");
    }
    if let Err(error) = crate::warm::attach(state, apps).await {
        tracing::warn!(app_id = %apps[0].id, code = %error.code.as_str(), "failoverApp answers without the app's warm copy");
    }
    app
}

#[cfg(test)]
mod tests {
    use super::{Held, held_grant};
    use crate::authority::{Authority, SystemMonotonic, VoterSupport};
    use crate::changelog::{Actor, ActorType};
    use crate::driver::{Capacity, Kvm};
    use crate::leases::{self, NewGrant};
    use crate::store::Store;
    use std::sync::Arc;

    #[derive(Debug)]
    struct EverySupport;

    impl VoterSupport for EverySupport {
        fn supports(&self, _duration_ms: u64) -> bool {
            true
        }
    }

    /// The held-grant guess of `failoverApp` agrees with what
    /// `grant_app_lease` does with majority support, for each row state:
    /// marked, `local`, `expiring` below `D`, and `expiring` at `D`
    /// unmarked. Each case runs in a transaction that is rolled back.
    #[tokio::test]
    async fn the_held_grant_guess_agrees_with_the_grant() {
        let data_dir = std::env::temp_dir().join(format!(
            "quoro-failover-{}",
            &crate::store::random_secret()[..16]
        ));
        std::fs::create_dir_all(&data_dir).unwrap();
        let store = Store::open(&data_dir.join("quoro.db")).await.unwrap();
        let node_id = crate::driver::register_local_node(
            &store,
            &crate::driver::LocalNode {
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
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) \
             VALUES ('prj_f', 'f', 'f', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO apps (id, project_id, name, stateful, ha_policy, image, port, \
             health_check, resources, placement, status, created_at, updated_at) \
             VALUES ('app_f', 'prj_f', 'db', 1, 'warm', 'img', 8080, '{}', '{}', \
             '{\"node_ids\":[]}', 'running', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let actor = Actor {
            kind: ActorType::Node,
            id: node_id.clone(),
        };

        for (case, mode, duration_ms, unsettled, expected) in [
            ("marked", "expiring", Some(15_000), 1, Some(Held::Mark)),
            ("local", "local", None, 0, Some(Held::Raise)),
            (
                "expiring below D",
                "expiring",
                Some(3_000),
                0,
                Some(Held::Raise),
            ),
            ("expiring at D", "expiring", Some(15_000), 0, None),
        ] {
            let mut w = store.begin_write().await.unwrap();
            sqlx::query(
                "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
                 fencing_token, duration_ms, granted_at, revoked_at, unsettled) \
                 VALUES ('lse_f', 'app', 'app_f', ?, ?, 4, ?, ?, ?, ?)",
            )
            .bind(&node_id)
            .bind(mode)
            .bind(duration_ms)
            .bind(&now)
            .bind(&now)
            .bind(unsettled)
            .execute(&mut *w.tx)
            .await
            .unwrap();
            let lease = leases::app_lease(&mut w.tx, "app_f")
                .await
                .unwrap()
                .unwrap();
            let marked = leases::is_marked(&mut w.tx, &lease.id).await.unwrap();
            let guess = held_grant(&lease, marked);
            assert_eq!(guess, expected, "{case}");

            // A fresh authority, so no gate of an earlier case is open.
            let authority = Authority::new(Arc::new(SystemMonotonic::default()));
            authority.set_support(Arc::new(EverySupport));
            let granted =
                leases::grant_app_lease(&mut w.tx, &authority, "app_f", &node_id, &actor, false)
                    .await
                    .unwrap();
            match (&granted, guess) {
                (NewGrant::Waiting(_), Some(_)) => {}
                (NewGrant::Granted(lease), None) => {
                    assert_eq!(lease.fencing_token, 5, "{case}");
                    assert_eq!(lease.mode, "expiring", "{case}");
                }
                _ => panic!("{case}: guessed {guess:?}, the grant answered {granted:?}"),
            }
            drop(w);
        }
        std::fs::remove_dir_all(&data_dir).unwrap();
    }
}

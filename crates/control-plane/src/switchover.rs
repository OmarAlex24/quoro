// SPDX-License-Identifier: AGPL-3.0-only
//! `switchoverApp`: a planned switchover of a `warm` app to its warm node
//! (`openapi.yaml` `switchoverApp`, `node-agent-protocol.md` sections 5.17
//! and 6.13, ADR 0177).
//!
//! The call answers `202` once the switchover started; its steps then run in
//! their own task:
//!
//! 1. **Refusals**, in the contract's order ([`refuse`]), before anything is
//!    written. Then the call's audit record is written, and the call answers.
//! 2. **The freeze.** The source gets `command` kind `switchover_freeze` and
//!    the primary waits for its `command_result`, for at most
//!    [`freeze_bound`]. An `ok` answer carries the final snapshot that the
//!    warm node confirmed, and the source's VM stays paused.
//! 3. **The revoke**, as `failoverApp`'s: one transaction revokes the lease
//!    (reason `moved`), stops the app's instances and writes the
//!    `lease.revoke` record, then the revoke goes out on the source's
//!    session. Right before it, both nodes' sessions, the warm node's copy,
//!    the fences and majority support are checked again: the warm node must
//!    report a complete copy at the final snapshot or later
//!    ([`final_copy_reported`]).
//! 4. **The wait and the change**, as `failoverApp`'s, emitting
//!    `app.switched_over` with the final snapshot's `snapshot_seq`.
//!
//! A step before the revoke that fails sends the source `switchover_thaw`,
//! so the app resumes on its node, and emits `app.switchover_failed` with
//! `step` `freeze` or `revoke`: nothing moved. From the revoke on the
//! switchover only goes forward: a later step that fails leaves the app's
//! lease revoked and its app stopped, logged as an error, and a
//! `failoverApp` call to the same warm node completes the move. The final
//! snapshot is on the warm node, and the source's volume holds the same data,
//! so that move loses nothing.
//!
//! **One at a time.** A switchover holds the same in-memory set as
//! `failoverApp` ([`crate::failover::Running`]) from its `in_progress` check
//! until its last step, so a failover or a second switchover of the app
//! answers `in_progress` meanwhile, and the reverse.
//!
//! **After a primary restart** no switchover of this process runs, so a
//! source that reports `frozen: true` gets `switchover_thaw`
//! ([`crate::session::NodeHub`], section 5.17). That thaw holds the same set
//! until it is answered, so no switchover's freeze can reach the node ahead
//! of it.

use crate::AppState;
use crate::api::apps::{APP_COLUMNS, App, HaPolicy, WarmState, app_from_row, app_target};
use crate::authority::DEFAULT_DURATION_MS;
use crate::error::{ApiError, Reason};
use crate::events::{self, NewEvent, Source};
use crate::failover::{self, Outcome, Plan, RunningGuard, conflict};
use crate::leases;
use crate::request::CallCtx;
use crate::session::{Fault, HubError};
use quoro_proto::CommandStatus;
use quoro_types::Scope;
use serde_json::{Value, json};
use std::time::Duration;

/// The shortest freeze bound (see [`freeze_bound`]).
const FREEZE_BOUND_MIN_S: i64 = 120;

/// How long the primary waits, after the source's `ok`, for the warm node
/// to report the final snapshot as its complete copy. The warm node reports
/// on every change and at least every 30 s (section 5.3), and its report may
/// reach the primary just after the source's answer.
const COPY_WAIT: Duration = Duration::from_secs(35);

/// How often that wait reads the warm node's report again.
const COPY_RECHECK: Duration = Duration::from_millis(250);

/// How long the primary waits for the source to answer `switchover_thaw`.
/// The thaw is best-effort: a source that does not answer and reports
/// `frozen: true` later is thawed then (section 5.17).
pub(crate) const THAW_WAIT: Duration = Duration::from_secs(10);

/// The detail of the refusal of an app whose lease is revoked.
const REVOKED: &str = "the app's lease is revoked: a switchover stopped after its revoke; \
                       failoverApp to the same warm node completes the move";

/// How long the primary waits for the source's answer to
/// `switchover_freeze`: `max(120 s, 4 × interval_s)`.
///
/// An `in_sync` copy lags by at most two intervals (section 6.13), so the
/// final cycle ships at most about two intervals of writes. The bound gives
/// it twice that, and never less than two minutes. On the bound the primary
/// thaws the source and the switchover fails at its `freeze` step; a freeze
/// that answers `ok` after the thaw went out is resumed by that thaw, which
/// the node handles after the freeze.
fn freeze_bound(interval_s: i64) -> Duration {
    let seconds = interval_s.saturating_mul(4).max(FREEZE_BOUND_MIN_S);
    Duration::from_secs(u64::try_from(seconds).unwrap_or(u64::MAX))
}

/// What the steps after the answer need.
struct Steps {
    plan: Plan,
    project_id: String,
    /// The app's VM on the source, which the freeze pauses.
    vm_id: String,
    bound: Duration,
    /// How long the warm node may take to report the final snapshot
    /// ([`COPY_WAIT`]).
    copy_wait: Duration,
}

/// Where a switchover that moved nothing stopped (`app.switchover_failed`
/// `step`).
#[derive(Clone, Copy, Debug)]
enum Step {
    Freeze,
    Revoke,
}

impl Step {
    fn as_str(self) -> &'static str {
        match self {
            Self::Freeze => "freeze",
            Self::Revoke => "revoke",
        }
    }
}

/// Runs `switchoverApp` for `app_id` onto `node_id` after the authorization
/// check: the refusals, the call's audit record, then the steps in their own
/// task. Answers the app as `getApp` shows it.
///
/// # Errors
///
/// `not_found` for an unknown app, `validation_failed` for a `node_id` that
/// is not the app's warm node, `conflict` with the contract's reasons, or a
/// database error.
pub(crate) async fn start(
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
    let mut app = app_from_row(&row)?;
    if app.ha_policy != HaPolicy::Warm {
        return Err(conflict(Reason::NotWarm, "the app's ha_policy is not warm"));
    }
    let Some(running) = state.hub.running().start(app_id) else {
        return Err(conflict(
            Reason::InProgress,
            "a failover or switchover of the app is already running",
        ));
    };
    // As for `failoverApp`: a grant of the app's lease that waits answers
    // `in_progress`, checked before `node_id` (ADR 0292).
    let mut conn = state.store.pool().acquire().await?;
    let lease = leases::app_lease(&mut conn, app_id).await?;
    drop(conn);
    if lease.is_some_and(|lease| state.hub.row_grant_pending(&lease)) {
        return Err(conflict(
            Reason::InProgress,
            "the grant of the app's lease to its holder still waits (App.lease.grant_pending)",
        ));
    }
    let mut steps = refuse(state, &mut app, node_id).await?;
    // Test seam: waits a test can outlast.
    if state.hub.take_fault(&Fault::ShortSwitchoverWaits {
        app_id: app_id.to_owned(),
    }) {
        steps.bound = Duration::from_secs(1);
        steps.copy_wait = Duration::from_secs(1);
    }

    // The call's own record, when the switchover starts
    // (`audit-log-format.md`): its outcome is the event that ends it.
    let mut w = state.store.begin_write().await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppFailover),
        Some(&app_target(app_id)),
        Some(&steps.project_id),
        params,
        None,
    )
    .await?;
    w.commit().await?;

    let answer = failover::answer(state, app, "switchoverApp").await;
    // The steps run in their own task, which holds `running` until the
    // last of them: a client that gives up does not stop a switchover
    // halfway.
    tokio::spawn(run_steps(state.clone(), ctx.clone(), steps, running));
    Ok(answer)
}

/// The refusals of `switchoverApp` that follow `not_warm` and
/// `in_progress`, in the contract's order, before anything is written:
/// `node_id`, `not_in_sync`, `node_disconnected`, `fence_failed`,
/// `no_support`.
///
/// Before `not_in_sync`, an app whose lease is revoked is refused with a
/// plain `conflict` ([`REVOKED`]): a switchover that stopped after its
/// revoke, as after a primary restart, leaves the app stopped, and a freeze
/// would find no lease to check.
///
/// A node that is not connected reads `not_in_sync` first, since
/// `App.warm.state` is `failed` meanwhile (section 6.13); `node_disconnected`
/// answers a session that ended between the two reads.
async fn refuse(state: &AppState, app: &mut App, node_id: &str) -> Result<Steps, ApiError> {
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

    let lease = leases::app_lease(&mut conn, &app.id).await?;
    if lease.as_ref().is_some_and(leases::Lease::is_revoked) {
        return Err(ApiError::conflict(REVOKED));
    }

    crate::warm::attach(state, std::slice::from_mut(app)).await?;
    let Some(warm) = app.warm.as_ref() else {
        return Err(conflict(Reason::NotInSync, "the app has no warm copy"));
    };
    if warm.state != WarmState::InSync {
        let state = serde_json::to_value(warm.state)
            .ok()
            .and_then(|state| state.as_str().map(str::to_owned))
            .unwrap_or_default();
        return Err(conflict(
            Reason::NotInSync,
            format!("App.warm.state is {state}, not in_sync"),
        ));
    }
    let interval_s = warm.interval_s;

    let volume = failover::ready_volume(&mut conn, &app.id)
        .await?
        .ok_or_else(|| ApiError::conflict("the app's volume is not ready"))?;
    let source = volume.node_id.clone();

    if !state.hub.is_connected(&source) {
        return Err(conflict(
            Reason::NodeDisconnected,
            "the source node is not connected",
        ));
    }
    if !state.hub.is_connected(node_id) {
        return Err(conflict(
            Reason::NodeDisconnected,
            "the warm node is not connected",
        ));
    }

    if let Some(lease) = &lease
        && crate::fences::has_failed_fence(&mut conn, state.hub.unrecorded_failures(), &lease.id)
            .await?
    {
        return Err(conflict(
            Reason::FenceFailed,
            "the app's lease has a failed fence that no operator acknowledged",
        ));
    }
    if crate::fences::node_has_failed_fence(&mut conn, state.hub.unrecorded_failures(), node_id)
        .await?
    {
        return Err(conflict(
            Reason::FenceFailed,
            "the warm node has a failed fence that no operator acknowledged",
        ));
    }

    if !state.hub.authority().supports(DEFAULT_DURATION_MS) {
        return Err(conflict(
            Reason::NoSupport,
            "the primary does not hold the support of a majority of voters, so it cannot grant \
             an expiring lease",
        ));
    }

    let Some(lease) = lease else {
        return Err(ApiError::conflict("the app has no lease to move"));
    };
    if lease.holder_node_id != source {
        return Err(ApiError::conflict(
            "the app's lease is not held by the node of its volume",
        ));
    }
    let vm_id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM vms WHERE app_id = ? AND node_id = ? ORDER BY id LIMIT 1",
    )
    .bind(&app.id)
    .bind(&source)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(vm_id) = vm_id else {
        return Err(ApiError::conflict(
            "the app has no VM on the node of its volume",
        ));
    };
    Ok(Steps {
        plan: Plan {
            app_id: app.id.clone(),
            warm: node_id.to_string(),
            source,
            volume_id: volume.id,
            lease,
        },
        project_id: app.project_id.clone(),
        vm_id,
        bound: freeze_bound(interval_s),
        copy_wait: COPY_WAIT,
    })
}

/// The steps after the answer: the freeze, the revoke, the wait and the
/// change. `_running` holds the app in the shared set until they end.
async fn run_steps(state: AppState, ctx: CallCtx, steps: Steps, _running: RunningGuard) {
    let plan = &steps.plan;
    let snapshot_seq = match freeze(&state, &steps).await {
        Ok(seq) => seq,
        Err(error) => return give_up(&state, &ctx, &steps, Step::Freeze, &error).await,
    };
    if let Err(error) = before_revoke(&state, &steps, snapshot_seq).await {
        return give_up(&state, &ctx, &steps, Step::Revoke, &error).await;
    }
    // Revoked by this call or meanwhile, the lease no longer comes back to
    // the source: from here the switchover only goes forward.
    if let Err(error) = failover::revoke(&state, &ctx, plan).await {
        let detail = error
            .detail
            .unwrap_or_else(|| error.code.as_str().to_owned());
        return give_up(&state, &ctx, &steps, Step::Revoke, &detail).await;
    }
    let outcome = Outcome::Switchover { snapshot_seq };
    match failover::release_and_change(&state, &ctx, plan, &outcome).await {
        Ok(moved) => {
            failover::after_commit(&state, plan, moved);
        }
        Err(error) => {
            tracing::error!(
                app_id = %plan.app_id,
                from_node_id = %plan.source,
                to_node_id = %plan.warm,
                code = %error.code.as_str(),
                reason = ?error.reason,
                detail = error.detail.as_deref().unwrap_or(""),
                "the switchover stopped after its revoke: the app's lease stays revoked and its \
                 app stopped; failoverApp to the same warm node completes the move"
            );
        }
    }
}

/// Sends `switchover_freeze` and waits for its answer: the final snapshot's
/// `snapshot_seq`, or why the freeze failed.
async fn freeze(state: &AppState, steps: &Steps) -> Result<u64, String> {
    let plan = &steps.plan;
    let params = json!({
        "app_id": plan.app_id,
        "vm_id": steps.vm_id,
        "volume_id": plan.volume_id,
        "lease_id": plan.lease.id,
        "fencing_token": plan.lease.fencing_token,
    });
    match state
        .hub
        .command(&plan.source, "switchover_freeze", params, steps.bound)
        .await
    {
        // `snapshot_seq` is at least 1 (`events.schema.json`): a 0 names no
        // snapshot.
        Ok(result) if result.status == CommandStatus::Ok => result
            .snapshot_seq
            .filter(|seq| *seq >= 1)
            .ok_or_else(|| {
                "the source answered the freeze without a snapshot_seq of at least 1".to_owned()
            }),
        Ok(result) => Err(result
            .error
            .map(|error| format!("{}: {}", code_name(error.code), error.message))
            .or(result.detail)
            .unwrap_or_else(|| "the source refused the freeze".to_owned())),
        Err(HubError::Timeout) => Err(format!(
            "the source did not answer the freeze within {} s",
            steps.bound.as_secs()
        )),
        Err(error) => Err(error.to_string()),
    }
}

/// The wire name of `code`, as the `command_result` carried it.
fn code_name(code: quoro_proto::ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|code| code.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// What may have changed while the source froze, checked again right
/// before the revoke: both nodes' sessions, the warm node's copy of the
/// final snapshot `final_seq`, the fences and majority support.
async fn before_revoke(state: &AppState, steps: &Steps, final_seq: u64) -> Result<(), String> {
    let hub = &state.hub;
    let plan = &steps.plan;
    if !hub.is_connected(&plan.source) {
        return Err("the source node is no longer connected".to_owned());
    }
    if !hub.is_connected(&plan.warm) {
        return Err("the warm node is no longer connected".to_owned());
    }
    final_copy_reported(state, steps, final_seq).await?;
    let mut conn = state
        .store
        .pool()
        .acquire()
        .await
        .map_err(|error| format!("cannot read the database: {error}"))?;
    let failed =
        crate::fences::has_failed_fence(&mut conn, hub.unrecorded_failures(), &plan.lease.id)
            .await
            .map_err(|error| format!("cannot read the fences: {}", error.code.as_str()))?
            || crate::fences::node_has_failed_fence(
                &mut conn,
                hub.unrecorded_failures(),
                &plan.warm,
            )
            .await
            .map_err(|error| format!("cannot read the fences: {}", error.code.as_str()))?;
    if failed {
        return Err("a failed fence that no operator acknowledged appeared".to_owned());
    }
    if !hub.authority().supports(DEFAULT_DURATION_MS) {
        return Err("the primary no longer holds the support of a majority of voters".to_owned());
    }
    Ok(())
}

/// Waits until the warm node reports a complete copy at `final_seq` or
/// later, for at most `steps.copy_wait`: the change moves the lease onto
/// that copy, and an older one would lose the writes the final snapshot
/// shipped.
async fn final_copy_reported(
    state: &AppState,
    steps: &Steps,
    final_seq: u64,
) -> Result<(), String> {
    let plan = &steps.plan;
    let deadline = tokio::time::Instant::now() + steps.copy_wait;
    loop {
        let reported = failover::complete_copy(state, &plan.warm, &plan.volume_id, &plan.app_id)
            .map(|(seq, _)| seq);
        if reported.is_some_and(|seq| seq >= final_seq) {
            return Ok(());
        }
        if !state.hub.is_connected(&plan.warm) {
            return Err("the warm node is no longer connected".to_owned());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(match reported {
                Some(seq) => format!(
                    "the warm node reports its complete copy at snapshot {seq}, not at the final \
                     snapshot {final_seq}"
                ),
                None => format!(
                    "the warm node reports no complete copy, not the final snapshot {final_seq}"
                ),
            });
        }
        tokio::time::sleep(COPY_RECHECK).await;
    }
}

/// A step before the revoke failed: the source gets `switchover_thaw`, then
/// `app.switchover_failed` is emitted. Nothing moved. A thaw that was not
/// delivered is said in the event's `error`: the source may still be
/// frozen, until a later report of `frozen: true` gets its thaw.
async fn give_up(state: &AppState, ctx: &CallCtx, steps: &Steps, step: Step, error: &str) {
    let plan = &steps.plan;
    let error = match state
        .hub
        .thaw(&plan.source, &plan.app_id, &steps.vm_id)
        .await
    {
        Ok(()) => error.to_owned(),
        Err(thaw) => {
            tracing::warn!(app_id = %plan.app_id, node_id = %plan.source, error = %thaw, "the source did not answer switchover_thaw; a later report of frozen: true thaws it");
            format!("{error}; the thaw was not delivered: {thaw}")
        }
    };
    let error = error.as_str();
    let actor = ctx.identity.actor();
    let emitted = async {
        let mut w = state.store.begin_write().await?;
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "app.switchover_failed",
                source: Source::ControlPlane,
                subject_type: "app",
                subject_id: &plan.app_id,
                project_id: Some(&steps.project_id),
                request_id: Some(&ctx.request_id),
                actor: Some(&actor),
                data: json!({
                    "app_id": plan.app_id,
                    "from_node_id": plan.source,
                    "to_node_id": plan.warm,
                    "step": step.as_str(),
                    "error": error,
                }),
            },
        )
        .await?;
        w.commit().await?;
        Ok::<(), ApiError>(())
    }
    .await;
    if let Err(failed) = emitted {
        tracing::error!(app_id = %plan.app_id, step = step.as_str(), error, code = %failed.code.as_str(), "cannot emit app.switchover_failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_freeze_bound_is_four_intervals_and_never_below_two_minutes() {
        assert_eq!(freeze_bound(30), Duration::from_secs(120));
        assert_eq!(freeze_bound(30 * 60), Duration::from_secs(4 * 30 * 60));
        assert_eq!(freeze_bound(31), Duration::from_secs(124));
        assert_eq!(freeze_bound(i64::MAX), Duration::from_secs(u64::MAX >> 1));
    }
}

//! The expiry sweeper.
//!
//! A grant or approval is already unusable at `expires_at` because the
//! decision procedure compares the timestamp on every call
//! (permission-model.md section 6.3), so the sweeper is about the stored
//! status and the event stream: every 15 s it moves stored `active` grants and
//! `pending` approvals past their TTL to `expired`, appends the change-log
//! entry and emits `grant.expired` (`reason: ttl`) or `approval.expired`.
//!
//! In a build that accepts `ha_policy` `warm` (ADR 0277), each tick also
//! picks a warm node again for a `warm` app whose warm node is lost for good,
//! and one for a `warm` app that has none once a node is eligible
//! ([`repick_warm_nodes`]).

use crate::AppState;
use crate::error::ApiError;
use std::time::Duration;

/// How often the sweeper runs.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

/// Starts the sweeper task. Errors are logged, never propagated: a failed
/// sweep is retried on the next tick.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            ticker.tick().await;
            if let Err(error) = crate::service::grants::expire_due(&state).await {
                tracing::warn!(error = ?error, "grant expiry sweep failed");
            }
            if let Err(error) = crate::service::approvals::expire_due(&state).await {
                tracing::warn!(error = ?error, "approval expiry sweep failed");
            }
            if let Err(error) = repick_warm_nodes(&state).await {
                tracing::warn!(error = ?error, "warm node sweep failed");
            }
        }
    });
}

/// Picks a warm node again where the scheduler must (`openapi.yaml`
/// `WarmCopy.node_id`, ADR 0278): for each `warm` app whose warm node is
/// removed, lost the `node` role, snapshots with `pause_copy` or holds the
/// app's volume, and for each `warm` app with none. Does nothing in a build
/// that refuses `warm` (ADR 0277).
///
/// # Errors
///
/// A database error.
pub async fn repick_warm_nodes(state: &AppState) -> Result<(), ApiError> {
    crate::warm::repick_warm_nodes(state).await
}

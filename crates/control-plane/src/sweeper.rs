//! The expiry sweeper.
//!
//! A grant or approval is already unusable at `expires_at` because the
//! decision procedure compares the timestamp on every call
//! (permission-model.md section 6.3), so the sweeper is about the stored
//! status and the event stream: every 15 s it moves stored `active` grants and
//! `pending` approvals past their TTL to `expired`, appends the change-log
//! entry and emits `grant.expired` (`reason: ttl`) or `approval.expired`.

use crate::AppState;
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
        }
    });
}

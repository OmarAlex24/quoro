// SPDX-License-Identifier: AGPL-3.0-only
//! The API view of a lease (`openapi.yaml` `Lease`), carried by every read of
//! an app as `App.lease` from M5b.
//!
//! The row is the one `crate::leases` writes. The times of an `expiring`
//! lease are the primary's in-memory view (`node-agent-protocol.md` section
//! 8.4); no `expiring` lease exists before the M5b lease work, so `gate_at`
//! is always null here and the other times are the row's.

use crate::AppState;
use crate::api::apps::App;
use crate::error::ApiError;
use serde::Serialize;
use utoipa::ToSchema;

/// What a lease is on (`Lease.resource_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum LeaseResourceType {
    App,
    Primary,
}

/// `local` or `expiring` (`Lease.mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum LeaseModeView {
    Local,
    Expiring,
}

/// A lease on an app or on the primary status (`openapi.yaml` `Lease`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[schema(title = "Lease")]
pub struct Lease {
    /// Lease id.
    #[schema(pattern = "^lse_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// What the lease is on.
    #[schema(inline)]
    pub resource_type: LeaseResourceType,
    /// Id of the leased resource (an app id or a cluster id).
    pub resource_id: String,
    /// Node holding the lease.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub holder_node_id: String,
    /// Local leases need no renewal; expiring leases do.
    #[schema(inline)]
    pub mode: LeaseModeView,
    /// Changes only on ownership change; never reused.
    #[schema(minimum = 1)]
    pub fencing_token: i64,
    /// Lease duration in milliseconds; expiring leases only.
    pub duration_ms: Option<i64>,
    /// When the lease was granted.
    #[schema(format = "date-time")]
    pub granted_at: String,
    /// When the primary last answered a renewal; expiring leases only.
    pub renewed_at: Option<String>,
    /// When the holder has fenced at the latest if it renews no more.
    pub expires_at: Option<String>,
    /// The earliest time the primary may grant this lease with a new token.
    pub gate_at: Option<String>,
    /// When the lease was revoked.
    pub revoked_at: Option<String>,
    /// True while the row reads `local` after the app left `warm` but the
    /// primary has not yet sent the grant that ends the holder's renewals
    /// (`node-agent-protocol.md` section 8.4, ADR 0261).
    pub local_pending: bool,
}

impl Lease {
    /// The API view of a `leases` row.
    ///
    /// # Errors
    ///
    /// `internal` for a row whose `resource_type` or `mode` the schema does
    /// not allow.
    pub(crate) fn from_row(row: crate::leases::Lease) -> Result<Self, ApiError> {
        let resource_type = match row.resource_type.as_str() {
            "app" => LeaseResourceType::App,
            "primary" => LeaseResourceType::Primary,
            other => {
                return Err(ApiError::internal(&format!(
                    "leases.resource_type is invalid: {other}"
                )));
            }
        };
        let mode = match row.mode.as_str() {
            "local" => LeaseModeView::Local,
            "expiring" => LeaseModeView::Expiring,
            other => {
                return Err(ApiError::internal(&format!(
                    "leases.mode is invalid: {other}"
                )));
            }
        };
        Ok(Self {
            id: row.id,
            resource_type,
            resource_id: row.resource_id,
            holder_node_id: row.holder_node_id,
            mode,
            fencing_token: row.fencing_token,
            duration_ms: row.duration_ms,
            granted_at: row.granted_at,
            renewed_at: row.renewed_at,
            expires_at: row.expires_at,
            // The gate is kept in memory with the expiring grants (section
            // 8.4), which the M5b lease work adds; a `local` lease has none.
            gate_at: None,
            revoked_at: row.revoked_at,
            // Set from the replication of the row's `local` write (section
            // 8.4), which the M5b lease work adds; until then no lease is
            // pending.
            local_pending: false,
        })
    }
}

/// Fills `lease` on every app of `apps` (`openapi.yaml` `App.lease`).
///
/// # Errors
///
/// A database error.
pub(crate) async fn attach(state: &AppState, apps: &mut [App]) -> Result<(), ApiError> {
    if apps.is_empty() {
        return Ok(());
    }
    let mut conn = state.store.pool().acquire().await?;
    for app in apps {
        app.lease = crate::leases::app_lease(&mut conn, &app.id)
            .await?
            .map(Lease::from_row)
            .transpose()?;
    }
    Ok(())
}

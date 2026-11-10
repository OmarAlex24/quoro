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
    /// True while the row reads `local` and carries the unsettled mark, that
    /// is, until the primary knows the holder took the row's `local` grant: it
    /// usually turns true with the write that ends the holder's renewals, when
    /// the app leaves `warm` (`node-agent-protocol.md` section 8.4, ADR 0261,
    /// ADR 0280).
    pub local_pending: bool,
    /// True while the row carries the unsettled mark (`leases.unsettled`):
    /// the row may disagree with its holder's mode, so the lease moves only
    /// as a `local` lease does (`node-agent-protocol.md` section 8.4, "The
    /// mark", ADR 0280).
    pub unsettled: bool,
    /// True while the primary holds a grant of this lease that it has not
    /// sent, because it waits for the row's newest write to be replicated and
    /// for majority support (`node-agent-protocol.md` section 8.4, "Grants of
    /// a marked row", ADR 0292). Derived on read; never stored.
    pub grant_pending: bool,
    /// While `grant_pending` is true, the machines with the `node` role that
    /// have not acknowledged the row's newest write; empty otherwise (ADR
    /// 0292). Derived on read; never stored.
    #[schema(value_type = Vec<super::auth::NodeIdItem>, inline)]
    pub grant_waiting_on: Vec<String>,
}

impl Lease {
    /// The API view of a `leases` row, with `grant_pending` and
    /// `grant_waiting_on` from the hub's held grants
    /// ([`crate::session::NodeHub::grant_view`]).
    ///
    /// # Errors
    ///
    /// `internal` for a row whose `resource_type` or `mode` the schema does
    /// not allow.
    pub(crate) fn from_row(
        row: crate::leases::Lease,
        grant_pending: bool,
        grant_waiting_on: Vec<String>,
    ) -> Result<Self, ApiError> {
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
            // "Expiring leases only": a `local` row can keep the duration of
            // its last `expiring` grant (section 8.4, "Ending the renewals"
            // step 1, and the holder's `local` report), which the view does
            // not show. The stored column is unchanged.
            duration_ms: row.duration_ms.filter(|_| mode == LeaseModeView::Expiring),
            granted_at: row.granted_at,
            renewed_at: row.renewed_at,
            expires_at: row.expires_at,
            // The gate is kept in memory with the expiring grants (section
            // 8.4), which the M5b lease work adds; a `local` lease has none.
            gate_at: None,
            revoked_at: row.revoked_at,
            // Section 8.4, "The mark": the row reads `local` while it carries
            // the mark, until the primary knows the holder took that grant.
            local_pending: mode == LeaseModeView::Local && row.unsettled,
            // `leases.unsettled` (migration 0013).
            unsettled: row.unsettled,
            // Derived from the grants the hub holds back (section 8.4,
            // "Grants of a marked row"; ADR 0292).
            grant_pending,
            grant_waiting_on,
        })
    }
}

/// Fills `lease` on every app of `apps` (`openapi.yaml` `App.lease`). A
/// read with no write: the held grants come from the hub, the required set
/// and the newest write from the same connection.
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
        app.lease = match crate::leases::app_lease(&mut conn, &app.id).await? {
            Some(row) => {
                let (pending, waiting_on) = state.hub.grant_view(&mut conn, &row).await?;
                Some(Lease::from_row(row, pending, waiting_on)?)
            }
            None => None,
        };
    }
    Ok(())
}

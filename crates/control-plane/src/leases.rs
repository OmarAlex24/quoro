// SPDX-License-Identifier: AGPL-3.0-only
//! Lease rows (`leases`; `node-agent-protocol.md` section 8).
//!
//! M5a writes one `local` lease per stateful app: the node that holds the
//! app's volume holds it, so the app runs there and nowhere else (section 8.3,
//! ADR 0145). The row is the authority: [`Lease::grant`] and
//! [`Lease::revoke`] build the wire messages the node receives on its session,
//! and the same `fencing_token` is echoed in the VM spec (section 5.11).
//!
//! Every write goes through the change log in the caller's transaction, like
//! the other replicated tables.

use crate::changelog::{self, Actor, Op};
use crate::deploy;
use crate::error::ApiError;
use crate::store::now_rfc3339;
use quoro_proto::{LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, ResourceType, RevokeReason};
use serde_json::{Value, json};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};
use std::collections::HashMap;
use std::sync::Mutex;

/// Columns of [`Lease`], in the order [`lease_from_row`] expects them.
pub(crate) const LEASE_COLUMNS: &str = "id, resource_type, resource_id, holder_node_id, mode, \
     fencing_token, duration_ms, granted_at, renewed_at, expires_at, revoked_at";

/// One `leases` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Lease {
    pub id: String,
    /// `app` or `primary`.
    pub resource_type: String,
    /// The `app_` or `nod_` id the lease protects.
    pub resource_id: String,
    pub holder_node_id: String,
    /// `local` or `expiring`; M5a writes `local` only.
    pub mode: String,
    pub fencing_token: i64,
    pub duration_ms: Option<i64>,
    pub granted_at: String,
    pub renewed_at: Option<String>,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

impl Lease {
    /// Whether the lease already ended.
    #[must_use]
    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    /// The change-log payload of the row, so a replica rebuilds it exactly.
    #[must_use]
    pub(crate) fn payload(&self) -> Value {
        json!({
            "id": self.id,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "holder_node_id": self.holder_node_id,
            "mode": self.mode,
            "fencing_token": self.fencing_token,
            "duration_ms": self.duration_ms,
            "granted_at": self.granted_at,
            "renewed_at": self.renewed_at,
            "expires_at": self.expires_at,
            "revoked_at": self.revoked_at,
        })
    }

    /// The `lease_grant` body the holder receives (section 5.12). A `local`
    /// lease carries no `duration_ms`.
    #[must_use]
    pub(crate) fn grant(&self) -> LeaseGrant {
        LeaseGrant {
            lease_id: LeaseId(self.id.clone()),
            resource_type: if self.resource_type == "primary" {
                ResourceType::Primary
            } else {
                ResourceType::App
            },
            resource_id: self.resource_id.clone(),
            mode: if self.mode == "expiring" {
                LeaseMode::Expiring
            } else {
                LeaseMode::Local
            },
            fencing_token: u64::try_from(self.fencing_token.max(1)).unwrap_or(1),
            duration_ms: self
                .duration_ms
                .and_then(|ms| u64::try_from(ms.max(1)).ok()),
            granted_at: self.granted_at.clone(),
        }
    }

    /// The `lease_revoke` body that ends the lease with `reason` (section
    /// 5.13). The token is the lease's own, which is at least the node's.
    #[must_use]
    pub(crate) fn revoke(&self, reason: RevokeReason) -> LeaseRevoke {
        LeaseRevoke {
            lease_id: LeaseId(self.id.clone()),
            fencing_token: u64::try_from(self.fencing_token.max(1)).unwrap_or(1),
            reason,
        }
    }
}

/// Builds a [`Lease`] from a row selected with [`LEASE_COLUMNS`].
///
/// # Errors
///
/// A database error for a missing column.
pub(crate) fn lease_from_row(row: &SqliteRow) -> Result<Lease, ApiError> {
    Ok(Lease {
        id: row.try_get("id")?,
        resource_type: row.try_get("resource_type")?,
        resource_id: row.try_get("resource_id")?,
        holder_node_id: row.try_get("holder_node_id")?,
        mode: row.try_get("mode")?,
        fencing_token: row.try_get("fencing_token")?,
        duration_ms: row.try_get("duration_ms")?,
        granted_at: row.try_get("granted_at")?,
        renewed_at: row.try_get("renewed_at")?,
        expires_at: row.try_get("expires_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

async fn read_one(
    conn: &mut SqliteConnection,
    sql: &str,
    id: &str,
) -> Result<Option<Lease>, ApiError> {
    let row = sqlx::query(&format!("SELECT {LEASE_COLUMNS} FROM leases WHERE {sql}"))
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;
    row.as_ref().map(lease_from_row).transpose()
}

/// The lease of an app, revoked or not.
///
/// # Errors
///
/// A database error.
pub(crate) async fn app_lease(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<Option<Lease>, ApiError> {
    read_one(
        conn,
        "resource_type = 'app' AND resource_id = ? ORDER BY id LIMIT 1",
        app_id,
    )
    .await
}

/// One lease by id, revoked or not.
///
/// # Errors
///
/// A database error.
pub(crate) async fn by_id(
    conn: &mut SqliteConnection,
    lease_id: &str,
) -> Result<Option<Lease>, ApiError> {
    read_one(conn, "id = ?", lease_id).await
}

/// Every lease the primary has revoked while `node_id` still is its last
/// holder, oldest first.
///
/// # Errors
///
/// A database error.
pub(crate) async fn revoked_from_node(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Vec<Lease>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {LEASE_COLUMNS} FROM leases \
         WHERE holder_node_id = ? AND revoked_at IS NOT NULL ORDER BY id"
    ))
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(lease_from_row).collect()
}

/// What this primary has learned, in memory, about revoked leases whose last
/// holder no longer runs anything under them (`node-agent-protocol.md`
/// sections 5.13 and 8.5, ADR 0047): for each lease and holder, the highest
/// token at which that is known.
///
/// A holder releases a lease at a token when it answers a `lease_revoke` of
/// that token `ok`: its bound VMs are stopped and recorded. So does a holder
/// whose `hello` does not name a lease the primary had already revoked from
/// it, since a node names every lease it knows, whatever its state (section
/// 5.1); and an operator's acknowledgement of the lease's failed fence, which
/// increments the token as declaring the node lost does (section 8.5 step 3).
/// Nothing here survives a restart of the primary. Every node then starts a
/// new session, and its `hello` either names the lease, which the primary
/// revokes again, or releases it.
#[derive(Debug, Default)]
pub(crate) struct Releases(Mutex<HashMap<(String, String), i64>>);

impl Releases {
    /// `holder` released `lease_id` at `token`.
    pub(crate) fn record(&self, lease_id: &str, holder: &str, token: i64) {
        let mut released = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = released
            .entry((lease_id.to_string(), holder.to_string()))
            .or_insert(token);
        *entry = (*entry).max(token);
    }

    /// Whether the holder `lease` names released it at its token or a higher
    /// one. A release at a lower token says nothing about a later grant.
    fn covers(&self, lease: &Lease) -> bool {
        let released = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        released
            .get(&(lease.id.clone(), lease.holder_node_id.clone()))
            .is_some_and(|token| *token >= lease.fencing_token)
    }
}

/// Whether `lease` may be granted to another holder, or to the same one with
/// a higher token: it is revoked in the primary's view, and its last holder
/// released it ([`Releases`]), was declared lost (`nodes.lost_at`, ADR 0183)
/// or was removed (section 10). A lease its holder still holds is not
/// released. An `expiring` lease is released the same way; the gate of
/// section 8.4 (ADR 0203) is not kept yet, so it never releases one sooner.
/// A lease listed in a node's `fence_failure` is refused by
/// [`grant_app_lease`] whatever this says.
///
/// # Errors
///
/// A database error.
pub(crate) async fn is_released(
    conn: &mut SqliteConnection,
    releases: &Releases,
    lease: &Lease,
) -> Result<bool, ApiError> {
    if !lease.is_revoked() {
        return Ok(false);
    }
    if releases.covers(lease) {
        return Ok(true);
    }
    let holder: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT lost_at, status FROM nodes WHERE id = ?")
            .bind(&lease.holder_node_id)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(holder.is_some_and(|(lost_at, status)| lost_at.is_some() || status == "removed"))
}

/// Every unrevoked lease `node_id` holds, oldest first.
///
/// # Errors
///
/// A database error.
pub(crate) async fn held_by_node(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Vec<Lease>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {LEASE_COLUMNS} FROM leases \
         WHERE holder_node_id = ? AND revoked_at IS NULL ORDER BY id"
    ))
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(lease_from_row).collect()
}

/// Increments the `fencing_token` of each listed lease `holder` holds and
/// records it revoked in the primary's view, with its holder still named, as
/// when an operator declares a node lost (`node-agent-protocol.md` sections
/// 8.3 and 8.5 step 3). Writes one change-log entry per lease and returns the
/// rows; an id with no row, or held by another node, is skipped.
///
/// # Errors
///
/// A database error.
pub(crate) async fn bump_tokens(
    conn: &mut SqliteConnection,
    lease_ids: &[String],
    holder: &str,
    actor: &Actor,
) -> Result<Vec<Lease>, ApiError> {
    let now = now_rfc3339();
    let mut bumped = Vec::new();
    for lease_id in lease_ids {
        let Some(mut lease) = by_id(conn, lease_id).await? else {
            continue;
        };
        if lease.holder_node_id != holder {
            continue;
        }
        lease.fencing_token = lease.fencing_token.max(0) + 1;
        if lease.revoked_at.is_none() {
            lease.revoked_at = Some(now.clone());
        }
        sqlx::query("UPDATE leases SET fencing_token = ?, revoked_at = ? WHERE id = ?")
            .bind(lease.fencing_token)
            .bind(&lease.revoked_at)
            .bind(&lease.id)
            .execute(&mut *conn)
            .await?;
        changelog::append(conn, actor, "lease", &lease.id, Op::Update, lease.payload()).await?;
        bumped.push(lease);
    }
    Ok(bumped)
}

/// Grants the app's `local` lease to `node_id` and writes its change-log
/// entry, returning the row. The token is one more than whatever the app's
/// lease last carried, so ownership changes never reuse a token (section 8.6).
///
/// # Errors
///
/// `conflict` while the lease is listed in a node's `fence_failure`, or a
/// database error.
pub(crate) async fn grant_app_lease(
    conn: &mut SqliteConnection,
    app_id: &str,
    node_id: &str,
    actor: &Actor,
) -> Result<Lease, ApiError> {
    let now = now_rfc3339();
    let previous = app_lease(conn, app_id).await?;
    // Section 8.5: a lease listed in a node's `fence_failure` is granted to
    // no node until an operator acknowledges the failed fence.
    if let Some(previous) = &previous
        && crate::fences::listed_leases(conn)
            .await?
            .contains(&previous.id)
    {
        return Err(ApiError::conflict(format!(
            "lease {} has a failed fence that no operator has acknowledged",
            previous.id
        )));
    }
    let token = previous
        .as_ref()
        .map_or(1, |lease| lease.fencing_token.max(0) + 1);
    let op = if previous.is_some() {
        Op::Update
    } else {
        Op::Create
    };
    let id = previous.as_ref().map_or_else(
        || deploy::new_id_with_prefix("lse"),
        |lease| lease.id.clone(),
    );
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, fencing_token, \
         duration_ms, granted_at, renewed_at, expires_at, revoked_at) \
         VALUES (?, 'app', ?, ?, 'local', ?, NULL, ?, NULL, NULL, NULL) \
         ON CONFLICT (resource_type, resource_id) DO UPDATE SET \
           holder_node_id = excluded.holder_node_id, mode = 'local', \
           fencing_token = excluded.fencing_token, duration_ms = NULL, \
           granted_at = excluded.granted_at, renewed_at = NULL, expires_at = NULL, \
           revoked_at = NULL",
    )
    .bind(&id)
    .bind(app_id)
    .bind(node_id)
    .bind(token)
    .bind(&now)
    .execute(&mut *conn)
    .await?;
    let lease = Lease {
        id,
        resource_type: "app".to_string(),
        resource_id: app_id.to_string(),
        holder_node_id: node_id.to_string(),
        mode: "local".to_string(),
        fencing_token: token,
        duration_ms: None,
        granted_at: now,
        renewed_at: None,
        expires_at: None,
        revoked_at: None,
    };
    changelog::append(conn, actor, "lease", &lease.id, op, lease.payload()).await?;
    Ok(lease)
}

/// Revokes the app's lease and writes its change-log entry; the caller builds
/// the `lease_revoke` with a [`RevokeReason`]. Returns `None` when the app has
/// no lease or it was already revoked.
///
/// # Errors
///
/// A database error.
pub(crate) async fn revoke_app_lease(
    conn: &mut SqliteConnection,
    app_id: &str,
    actor: &Actor,
) -> Result<Option<Lease>, ApiError> {
    let Some(mut lease) = app_lease(conn, app_id).await? else {
        return Ok(None);
    };
    if lease.is_revoked() {
        return Ok(None);
    }
    let now = now_rfc3339();
    sqlx::query("UPDATE leases SET revoked_at = ? WHERE id = ?")
        .bind(&now)
        .bind(&lease.id)
        .execute(&mut *conn)
        .await?;
    lease.revoked_at = Some(now);
    changelog::append(conn, actor, "lease", &lease.id, Op::Update, lease.payload()).await?;
    Ok(Some(lease))
}

#[cfg(test)]
mod tests {
    use super::{Lease, Releases};

    fn revoked(holder: &str, token: i64) -> Lease {
        Lease {
            id: "lse_a".to_string(),
            resource_type: "app".to_string(),
            resource_id: "app_a".to_string(),
            holder_node_id: holder.to_string(),
            mode: "local".to_string(),
            fencing_token: token,
            duration_ms: None,
            granted_at: "2026-11-07T10:00:00Z".to_string(),
            renewed_at: None,
            expires_at: None,
            revoked_at: Some("2026-11-07T11:00:00Z".to_string()),
        }
    }

    /// Issue #392: a release covers the lease only at its token or a lower
    /// one, and only for the holder that released it.
    #[test]
    fn a_release_covers_only_its_holder_up_to_its_token() {
        let releases = Releases::default();
        assert!(!releases.covers(&revoked("nod_a", 3)));
        releases.record("lse_a", "nod_a", 3);
        assert!(releases.covers(&revoked("nod_a", 3)));
        assert!(releases.covers(&revoked("nod_a", 2)));
        assert!(
            !releases.covers(&revoked("nod_a", 4)),
            "a later grant to the same node needs its own release"
        );
        assert!(!releases.covers(&revoked("nod_b", 3)), "another holder");
        releases.record("lse_a", "nod_a", 1);
        assert!(
            releases.covers(&revoked("nod_a", 3)),
            "a lower token never lowers it"
        );
    }
}

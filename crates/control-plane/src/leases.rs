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

/// Whether the lease row carries the unsettled mark (`leases.unsettled`,
/// section 8.4, "The mark"). A lease with no row carries none.
///
/// # Errors
///
/// A database error.
pub(crate) async fn is_marked(
    conn: &mut SqliteConnection,
    lease_id: &str,
) -> Result<bool, ApiError> {
    let unsettled: Option<i64> = sqlx::query_scalar("SELECT unsettled FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_optional(conn)
        .await?;
    Ok(unsettled.is_some_and(|unsettled| unsettled != 0))
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

/// Every app lease the primary has revoked while `node_id` still is its
/// last holder and that a restore waits for: its app's volume is still
/// `restoring` from a backup, and the restore grants the lease again only
/// once `node_id` released it ([`is_released`], issue #392).
///
/// # Errors
///
/// A database error.
pub(crate) async fn awaited_by_restores(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<Vec<Lease>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {LEASE_COLUMNS} FROM leases \
         WHERE holder_node_id = ? AND revoked_at IS NOT NULL \
         AND resource_type = 'app' \
         AND EXISTS (SELECT 1 FROM volumes WHERE volumes.app_id = leases.resource_id \
         AND volumes.state = 'restoring' AND volumes.restored_from_backup_id IS NOT NULL) \
         ORDER BY id"
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
/// that token `ok`: its bound VMs are stopped and recorded (section 5.13). So
/// does an operator's acknowledgement of the lease's failed fence, which
/// increments the token as declaring the node lost does (section 8.5 step 3).
/// Nothing here survives a restart of the primary. Every node then starts a
/// new session, at whose start the primary revokes again every lease it has
/// revoked from that node, so the holder's answer releases it again; and an
/// acknowledgement not yet delivered stays in `fence_ack_pending`, which
/// [`is_released`] reads.
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

    /// Forgets what `holder` released of `lease_id`: a `fence_failed` answer
    /// says its VMs may run again (section 8.5).
    pub(crate) fn forget(&self, lease_id: &str, holder: &str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(lease_id.to_string(), holder.to_string()));
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
/// a higher token. It must be revoked in the primary's view, with no
/// `lease_revoke` of it unanswered on its holder's live session
/// (`unanswered`): until the answer, the holder's fence may still run, and a
/// `fence_failed` answer lists the lease again (ADR 0238). Its last holder
/// must also have released it at its token ([`Releases`]); or have an
/// operator's acknowledgement of it pending delivery (`fence_ack_pending`,
/// section 8.5 step 3) at its token, made once the lease was revoked, and so
/// after its grant, so that the acknowledgement's increment produced that
/// token; or,
/// for a lease revoked by then, have been declared lost (`nodes.lost_at`,
/// ADR 0183) or removed (section 10). Such an `expiring` lease is released
/// only once its gate has passed on the authority's clock (section 8.4, ADR
/// 0202): until then its holder may still run the app. A lease listed in a
/// node's `fence_failure` is refused by [`grant_app_lease`] whatever this
/// says, and every new token also waits for the lease's gate there.
///
/// # Errors
///
/// A database error.
pub(crate) async fn is_released(
    conn: &mut SqliteConnection,
    releases: &Releases,
    authority: &crate::authority::Authority,
    unanswered: bool,
    lease: &Lease,
) -> Result<bool, ApiError> {
    if !lease.is_revoked() || unanswered {
        return Ok(false);
    }
    if releases.covers(lease) {
        return Ok(true);
    }
    // The acknowledgement's time is that of the latest one added to it, so
    // a lease granted to its holder again after an earlier acknowledgement
    // at that token (ADR 0247) is told apart only while no later one came.
    if let Some(pending) = crate::fences::pending_of(conn, &lease.holder_node_id).await?
        && pending.lease_ids.contains(&lease.id)
        && pending
            .fencing_tokens
            .get(&lease.id)
            .is_some_and(|token| i64::try_from(*token).ok() == Some(lease.fencing_token))
        && lease
            .revoked_at
            .as_deref()
            .is_some_and(|revoked_at| revoked_at <= pending.acked_at.as_str())
    {
        return Ok(true);
    }
    let holder: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT lost_at, status FROM nodes WHERE id = ?")
            .bind(&lease.holder_node_id)
            .fetch_optional(&mut *conn)
            .await?;
    let revoked_at = lease.revoked_at.as_deref().unwrap_or_default();
    let gone = holder.is_some_and(|(lost_at, status)| {
        status == "removed" || lost_at.is_some_and(|lost_at| revoked_at <= lost_at.as_str())
    });
    Ok(gone && (lease.mode == "local" || authority.passed(&lease.id)))
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

/// Raises the stored `duration_ms` of `lease_id` to `duration_ms`, before a
/// grant that long goes out, and writes its change-log entry. The stored
/// value is the longest duration the primary may have granted, which a
/// restarted primary counts every gate from (section 8.4, ADR 0203). Returns
/// the row when it changed; a lease with no row, or one whose stored
/// duration is already at least as long, is left as it is.
///
/// Every `expiring` grant calls it, even one no longer than the stored
/// value, inside the single-writer transaction `w` in which it records the
/// grant ([`Authority::record_grant`]). The caller commits `w` before it
/// sends the grant, so a restart after the send counts the raised value,
/// and a lowering ([`lower_duration`]) never runs between the raise and the
/// record.
///
/// [`Authority::record_grant`]: crate::authority::Authority::record_grant
///
/// # Errors
///
/// A duration outside 3 000 to 600 000 ms, or a database error.
pub(crate) async fn raise_duration(
    w: &mut crate::store::WriteTx,
    lease_id: &str,
    duration_ms: i64,
    actor: &Actor,
) -> Result<Option<Lease>, ApiError> {
    check_duration(duration_ms)?;
    let conn = &mut *w.tx;
    let Some(lease) = by_id(conn, lease_id).await? else {
        return Ok(None);
    };
    if lease
        .duration_ms
        .is_some_and(|stored| stored >= duration_ms)
    {
        return Ok(None);
    }
    write_duration(conn, lease, duration_ms, actor)
        .await
        .map(Some)
}

/// Lowers the stored `duration_ms` of `lease_id` to `duration_ms` once the
/// gate of every longer grant has passed ([`Authority::may_lower`]), and
/// writes its change-log entry. Returns the row when it changed. The check
/// and the write run in the single-writer transaction `w`, which also holds
/// every grant's raise and record ([`raise_duration`]), so no longer grant
/// is recorded between them.
///
/// [`Authority::may_lower`]: crate::authority::Authority::may_lower
///
/// # Errors
///
/// A duration outside 3 000 to 600 000 ms, or a database error.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "lowered after a longer grant, which nothing sends yet"
    )
)]
pub(crate) async fn lower_duration(
    w: &mut crate::store::WriteTx,
    authority: &crate::authority::Authority,
    lease_id: &str,
    duration_ms: i64,
    actor: &Actor,
) -> Result<Option<Lease>, ApiError> {
    let to_ms = check_duration(duration_ms)?;
    let conn = &mut *w.tx;
    let Some(lease) = by_id(conn, lease_id).await? else {
        return Ok(None);
    };
    if !lease.duration_ms.is_some_and(|stored| stored > duration_ms)
        || !authority.may_lower(lease_id, to_ms)
    {
        return Ok(None);
    }
    write_duration(conn, lease, duration_ms, actor)
        .await
        .map(Some)
}

/// A stored duration the authority may grant: 3 000 to 600 000 ms (sections
/// 5.12 and 8.4). Anything else is a caller's bug.
fn check_duration(duration_ms: i64) -> Result<u64, ApiError> {
    use crate::authority::{MAX_DURATION_MS, MIN_DURATION_MS};
    u64::try_from(duration_ms)
        .ok()
        .filter(|d| (MIN_DURATION_MS..=MAX_DURATION_MS).contains(d))
        .ok_or_else(|| {
            ApiError::internal(&format!(
                "a lease duration of {duration_ms} ms is outside {MIN_DURATION_MS} to \
                 {MAX_DURATION_MS} ms"
            ))
        })
}

async fn write_duration(
    conn: &mut SqliteConnection,
    mut lease: Lease,
    duration_ms: i64,
    actor: &Actor,
) -> Result<Lease, ApiError> {
    sqlx::query("UPDATE leases SET duration_ms = ? WHERE id = ?")
        .bind(duration_ms)
        .bind(&lease.id)
        .execute(&mut *conn)
        .await?;
    lease.duration_ms = Some(duration_ms);
    changelog::append(conn, actor, "lease", &lease.id, Op::Update, lease.payload()).await?;
    Ok(lease)
}

/// What [`grant_app_lease`] did.
#[derive(Debug)]
pub(crate) enum NewGrant {
    /// The row is written. The caller commits, then sends `lease.grant()`.
    Granted(Box<Lease>),
    /// The previous holder's gate is open for this long (section 8.4).
    /// Nothing is written or recorded.
    GateOpen(std::time::Duration),
    /// The grant could go out only once its write is replicated over the
    /// machines with the `node` role: the write would carry the row's
    /// unsettled mark (section 8.4, "The mark", ADR 0280, ADR 0285), whose
    /// grant also needs majority voter support ("Grants of a marked row"), or
    /// it raises the row's `duration_ms` (ADR 0268). An `expiring` new token
    /// without majority support is held too. This build counts support
    /// ([`crate::support`]) and follow positions, but does not hold a grant
    /// to send it once both allow it (#703), so nothing is written or
    /// recorded, and the caller tries again later.
    Held,
}

/// Grants the app's lease to `node_id` with a new token and writes its
/// change-log entry. The token is one more than whatever the app's lease last
/// carried, so ownership changes never reuse a token (section 8.6).
///
/// - **Mode.** A `warm` or `hot` app's lease is `expiring` with duration `D`
///   (D-0047) while a majority of voters supports `D` (section 8.8); any
///   other app's is `local`. The row's `duration_ms` is `D`, written with the
///   grant: the raise before the grant (ADR 0203). The previous holder's gate
///   has passed, so nothing it was granted is still running.
/// - **The mark.** A write that would carry the row's unsettled mark is
///   [`NewGrant::Held`] (section 8.4, "The mark"): a `local` new token of a
///   `warm` or `hot` app's lease, which sets it (ADR 0285), so a `warm` app
///   without support waits rather than run `local`; a `local` new token over
///   a row that reads `expiring`, which sets it too; and any new token of a
///   row that already carries it, because no write with a new token clears
///   it.
/// - **The raise.** An `expiring` new token over a row that does not read
///   `expiring` for at least `D` is [`NewGrant::Held`] too: it is a grant
///   longer than the duration a successor would count, which goes out only
///   once the raise is replicated over the machines with the `node` role
///   (section 8.4, "What the authority must enforce", ADR 0268). A successor
///   that lacked the write would count the old row's shorter gate, or none
///   for a row that reads `local`, also one promoted with `--ack-fence`
///   (section 7.3). So in this build an `expiring` new token goes out only
///   over a row that already reads `expiring` for at least `D`.
/// - **Gate.** Whatever the new mode, a new token waits until the previous
///   `expiring` holder's gate has passed on the authority's clock (section
///   8.4): [`NewGrant::GateOpen`].
/// - **ADR 0258.** A new token reaches a node that does not hold the lease
///   (`held` or `uncertain`), so its mode is free. Every caller grants only
///   once the previous lease ended for that node: a restore once the old
///   holder released it, a regrant once the node reported its expiry, and
///   `createVolume` once its deletion revoked the app's lease. A revoked
///   lease's `lease_revoke` reached the node's session before this grant, or
///   its next `hello` names the lease and gets one first (section 9). The
///   node holds a grant of that lease back until it has answered the revoke,
///   and the lease is then `revoked`, which takes either mode (sections 4.2
///   rule 3, 5.12 and 5.13). An `expiring` lease whose gate has passed is
///   held by no node, because its holder has fenced. The one exception is
///   `keep_local`: the caller passes it when the node still holds the
///   previous lease `local`, unrevoked, and the new token then stays `local`
///   (section 8.4, "Changing the mode"), which for a `warm` or `hot` app is
///   [`NewGrant::Held`].
///
/// The authority records the grant before the caller commits. A failed
/// commit leaves it ahead of the row, so a later gate ends later than it
/// must, never earlier.
///
/// # Errors
///
/// `conflict` while the lease is listed in a node's `fence_failure`, or a
/// database error.
pub(crate) async fn grant_app_lease(
    conn: &mut SqliteConnection,
    authority: &crate::authority::Authority,
    app_id: &str,
    node_id: &str,
    actor: &Actor,
    keep_local: bool,
) -> Result<NewGrant, ApiError> {
    use crate::authority::{DEFAULT_DURATION_MS, Grant, GrantKind, Refused};
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
    let policy: Option<String> = sqlx::query_scalar("SELECT ha_policy FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let warm = matches!(policy.as_deref(), Some("warm" | "hot"));
    // Section 8.4, "The mark": checked before the authority records anything.
    let marked = match &previous {
        Some(previous) => is_marked(conn, &previous.id).await?,
        None => false,
    };
    let was_expiring = previous
        .as_ref()
        .is_some_and(|previous| previous.mode == "expiring");
    // ADR 0268: an `expiring` grant longer than the stored duration goes out
    // only once the raise is replicated over the machines with the `node`
    // role, so that every possible next primary counts its gate. A successor
    // counts a gate only for a row that reads `expiring`, so a row must read
    // it for at least `D`.
    let stores_d = was_expiring
        && previous
            .as_ref()
            .and_then(|previous| previous.duration_ms)
            .is_some_and(|stored| stored >= i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX));
    let raises = warm && !stores_d;
    if marked || (warm && keep_local) || (!warm && was_expiring) || raises {
        return Ok(NewGrant::Held);
    }
    let sent_wall = time::OffsetDateTime::now_utc();
    let refused = |refused: Refused| {
        ApiError::internal(&format!(
            "the authority refused a new token of lease {id}: {refused:?}"
        ))
    };
    if warm {
        match authority.record_grant(&Grant {
            lease_id: &id,
            holder: node_id,
            duration_ms: DEFAULT_DURATION_MS,
            kind: GrantKind::NewToken,
            sent_wall,
        }) {
            Ok(()) => {}
            Err(Refused::GateOpen { remaining }) => return Ok(NewGrant::GateOpen(remaining)),
            // Without support for `D` the new token would be `local`, a write
            // that sets the mark (ADR 0285).
            Err(Refused::NoSupport) => return Ok(NewGrant::Held),
            Err(other) => return Err(refused(other)),
        }
    } else {
        match authority.record_local_grant(&id) {
            Ok(()) => {}
            Err(Refused::GateOpen { remaining }) => return Ok(NewGrant::GateOpen(remaining)),
            Err(other) => return Err(refused(other)),
        }
    }
    let expiring = warm;
    let mode = if expiring { "expiring" } else { "local" };
    let duration_ms = expiring.then(|| i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX));
    let now = crate::store::format_rfc3339(sent_wall);
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, fencing_token, \
         duration_ms, granted_at, renewed_at, expires_at, revoked_at) \
         VALUES (?, 'app', ?, ?, ?, ?, ?, ?, NULL, NULL, NULL) \
         ON CONFLICT (resource_type, resource_id) DO UPDATE SET \
           holder_node_id = excluded.holder_node_id, mode = excluded.mode, \
           fencing_token = excluded.fencing_token, duration_ms = excluded.duration_ms, \
           granted_at = excluded.granted_at, renewed_at = NULL, expires_at = NULL, \
           revoked_at = NULL",
    )
    .bind(&id)
    .bind(app_id)
    .bind(node_id)
    .bind(mode)
    .bind(token)
    .bind(duration_ms)
    .bind(&now)
    .execute(&mut *conn)
    .await?;
    let lease = Lease {
        id,
        resource_type: "app".to_string(),
        resource_id: app_id.to_string(),
        holder_node_id: node_id.to_string(),
        mode: mode.to_string(),
        fencing_token: token,
        duration_ms,
        granted_at: now,
        renewed_at: None,
        expires_at: None,
        revoked_at: None,
    };
    changelog::append(conn, actor, "lease", &lease.id, op, lease.payload()).await?;
    Ok(NewGrant::Granted(Box::new(lease)))
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
    use crate::authority::{
        Authority, Grant, GrantKind, ManualClock, MonotonicClock, VoterSupport,
    };
    use crate::changelog::{Actor, ActorType};
    use crate::driver::{Capacity, Kvm};
    use crate::store::Store;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct EverySupport;

    impl VoterSupport for EverySupport {
        fn supports(&self, _duration_ms: u64) -> bool {
            true
        }
    }

    /// A store with one node and an app lease on it.
    async fn store_with_lease() -> (Store, Lease, Actor, std::path::PathBuf) {
        let data_dir = std::env::temp_dir().join(format!(
            "quoro-leases-{}",
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
        let actor = Actor {
            kind: ActorType::Node,
            id: node_id.clone(),
        };
        let mut w = store.begin_write().await.unwrap();
        let authority = Authority::new(Arc::new(crate::authority::SystemMonotonic::default()));
        let super::NewGrant::Granted(lease) =
            super::grant_app_lease(&mut w.tx, &authority, "app_x", &node_id, &actor, false)
                .await
                .unwrap()
        else {
            panic!("a first lease has no gate");
        };
        let lease = *lease;
        w.commit().await.unwrap();
        (store, lease, actor, data_dir)
    }

    /// ADR 0203: the stored duration rises before a longer grant, never
    /// falls with a raise, and falls only once the longer grant's gate has
    /// passed.
    #[tokio::test]
    async fn a_stored_duration_rises_before_a_longer_grant_and_falls_after_its_gate() {
        let (store, lease, actor, data_dir) = store_with_lease().await;
        let clock = Arc::new(ManualClock::default());
        let authority = Authority::new(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        authority.set_support(Arc::new(EverySupport));

        let mut w = store.begin_write().await.unwrap();
        let raised = super::raise_duration(&mut w, &lease.id, 180_000, &actor)
            .await
            .unwrap();
        assert_eq!(raised.unwrap().duration_ms, Some(180_000));
        assert!(
            super::raise_duration(&mut w, &lease.id, 15_000, &actor)
                .await
                .unwrap()
                .is_none(),
            "a raise never lowers"
        );
        for out_of_range in [2_999, 600_001, -1] {
            assert!(
                super::raise_duration(&mut w, &lease.id, out_of_range, &actor)
                    .await
                    .is_err()
            );
        }
        authority
            .record_grant(&Grant {
                lease_id: &lease.id,
                holder: &lease.holder_node_id,
                duration_ms: 180_000,
                kind: GrantKind::NewToken,
                sent_wall: time::OffsetDateTime::now_utc(),
            })
            .unwrap();
        w.commit().await.unwrap();

        let mut w = store.begin_write().await.unwrap();
        assert!(
            super::lower_duration(&mut w, &authority, &lease.id, 15_000, &actor)
                .await
                .unwrap()
                .is_none(),
            "the longer grant's gate is open"
        );
        clock.advance(Duration::from_secs(216));
        assert!(
            super::lower_duration(&mut w, &authority, &lease.id, 2_999, &actor)
                .await
                .is_err()
        );
        let lowered = super::lower_duration(&mut w, &authority, &lease.id, 15_000, &actor)
            .await
            .unwrap();
        assert_eq!(lowered.unwrap().duration_ms, Some(15_000));
        w.commit().await.unwrap();
        let stored: Option<i64> = sqlx::query_scalar("SELECT duration_ms FROM leases WHERE id = ?")
            .bind(&lease.id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(stored, Some(15_000));
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

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

    /// Section 8.4 and ADR 0202: a revoked `expiring` lease of a holder
    /// declared lost or removed is released only once its gate has passed. A
    /// new token waits for the gate, and is held whatever the gate when its
    /// write would carry the row's unsettled mark. Nothing is written
    /// meanwhile.
    #[tokio::test]
    async fn a_new_token_and_a_lost_holders_release_wait_for_the_gate() {
        let (store, lease, actor, data_dir) = store_with_lease().await;
        let clock = Arc::new(ManualClock::default());
        let authority = Authority::new(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        let releases = Releases::default();
        let pool = store.pool();
        sqlx::query(
            "UPDATE leases SET mode = 'expiring', duration_ms = 15000, \
             revoked_at = '2026-11-09T10:00:00.000Z' WHERE id = ?",
        )
        .bind(&lease.id)
        .execute(pool)
        .await
        .unwrap();
        let revoked = super::by_id(&mut pool.acquire().await.unwrap(), &lease.id)
            .await
            .unwrap()
            .unwrap();
        let holder = &lease.holder_node_id;
        let gone = [
            (
                "declared lost",
                "UPDATE nodes SET lost_at = '2026-11-09T10:00:00.000Z' WHERE id = ?",
            ),
            (
                "removed",
                "UPDATE nodes SET lost_at = NULL, status = 'removed' WHERE id = ?",
            ),
        ];
        for (case, sql) in gone {
            sqlx::query(sql).bind(holder).execute(pool).await.unwrap();
            // D = 15 s and M = 1.5 s: the gate passes 18 s after the seed.
            authority.seed(&lease.id, holder, Some(15_000));
            let mut w = store.begin_write().await.unwrap();
            assert!(
                !super::is_released(&mut w.tx, &releases, &authority, false, &revoked)
                    .await
                    .unwrap(),
                "{case}: the gate is open"
            );
            clock.advance(Duration::from_millis(17_999));
            assert!(
                !super::is_released(&mut w.tx, &releases, &authority, false, &revoked)
                    .await
                    .unwrap(),
                "{case}: 1 ms before the gate"
            );
            clock.advance(Duration::from_millis(1));
            assert!(
                super::is_released(&mut w.tx, &releases, &authority, false, &revoked)
                    .await
                    .unwrap(),
                "{case}: the gate has passed"
            );
            w.commit().await.unwrap();
        }

        // No app row, so no policy wants `expiring`: a new token is `local`.
        // Over a row that reads `expiring` that write would set the row's
        // unsettled mark (section 8.4, "The mark"), so it is held, before
        // the gate and after it, and nothing is written.
        authority.seed(&lease.id, holder, Some(15_000));
        let mut w = store.begin_write().await.unwrap();
        for when in ["before the gate", "after the gate"] {
            let held =
                super::grant_app_lease(&mut w.tx, &authority, "app_x", holder, &actor, false)
                    .await
                    .unwrap();
            assert!(matches!(held, super::NewGrant::Held), "{when}: {held:?}");
            let row = super::by_id(&mut w.tx, &lease.id).await.unwrap().unwrap();
            assert_eq!(row.fencing_token, lease.fencing_token, "{when}");
            clock.advance(Duration::from_millis(18_000));
        }

        // A row that reads `local` while the authority holds a gate for it,
        // as a commit that failed after the authority recorded an `expiring`
        // grant leaves: the new token waits for the gate, writing nothing.
        sqlx::query("UPDATE leases SET mode = 'local', duration_ms = NULL WHERE id = ?")
            .bind(&lease.id)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        authority.seed(&lease.id, holder, Some(15_000));
        let open = super::grant_app_lease(&mut w.tx, &authority, "app_x", holder, &actor, false)
            .await
            .unwrap();
        assert!(
            matches!(open, super::NewGrant::GateOpen(remaining) if remaining == Duration::from_millis(18_000)),
            "{open:?}"
        );
        let row = super::by_id(&mut w.tx, &lease.id).await.unwrap().unwrap();
        assert_eq!(row.fencing_token, lease.fencing_token);
        clock.advance(Duration::from_millis(18_000));
        let super::NewGrant::Granted(granted) =
            super::grant_app_lease(&mut w.tx, &authority, "app_x", holder, &actor, false)
                .await
                .unwrap()
        else {
            panic!("the gate has passed");
        };
        assert_eq!(granted.fencing_token, lease.fencing_token + 1);
        assert_eq!(granted.mode, "local");
        assert_eq!(granted.duration_ms, None);
        assert!(authority.passed(&lease.id));

        // A row that carries the mark: no new token clears it, so any new
        // token is held, and nothing is written.
        sqlx::query("UPDATE leases SET unsettled = 1 WHERE id = ?")
            .bind(&lease.id)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let held = super::grant_app_lease(&mut w.tx, &authority, "app_x", holder, &actor, false)
            .await
            .unwrap();
        assert!(matches!(held, super::NewGrant::Held), "{held:?}");
        let row = super::by_id(&mut w.tx, &lease.id).await.unwrap().unwrap();
        assert_eq!(row.fencing_token, lease.fencing_token + 1);
        w.commit().await.unwrap();
        std::fs::remove_dir_all(&data_dir).unwrap();
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

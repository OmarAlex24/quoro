// SPDX-License-Identifier: AGPL-3.0-only
//! The primary's side of quarantined copies of a volume (`volume_quarantines`,
//! migration 0011; node-agent-protocol.md sections 5.3, 5.11, 9 and 10; ADR
//! 0176, ADR 0215).
//!
//! A node that lost a `warm` app's lease to a failover keeps its divergent
//! volume as a quarantined copy on its own disk and lists it in
//! `status_report.quarantines`. The first time a report lists a copy with its
//! `sha256`, [`apply_report`] records it: the row, its change-log entry, the
//! system audit record `volume.quarantine` (`audit-log-format.md` section 3.8)
//! and the event `volume.quarantined`, in one write transaction. A copy of a
//! volume that is gone or `deleting` gets the audit record and the event
//! without a row, once per `quarantine_id` ([`record_gone`], section 9, ADR
//! 0316): the row needs the volume and would hold back its deletion. A copy
//! with a row stays recorded until an operator deletes it with `deleteVolumeQuarantine`
//! ([`delete_copy`]), which sets `deleting_at` and puts the id in its node's
//! `spec.quarantine_deletes` ([`deletes_on`]); the row goes once that node's
//! report no longer lists the copy. `removeNode` drops every record of the
//! node it removes in its own transaction ([`drop_node_copies`], section 10),
//! then forgets the copies that node listed ([`forget_listed`]); the files
//! stay on its disk. Nothing else removes a record. `deleteVolume` and
//! `deleteApp` are refused with
//! `has_quarantines` while a record names the volume, or while a node's
//! latest report lists a copy of it that could get a row, hashed or not
//! ([`ListedCopies`], [`refuse_volume_with_copies`],
//! [`refuse_app_with_copies`]).

use crate::AppState;
use crate::api::volumes::{
    VOLUME_COLUMNS, Volume, VolumeQuarantine, VolumeQuarantineState, volume_from_row, volume_target,
};
use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::deploy::is_id_with_prefix;
use crate::error::{ApiError, Reason};
use crate::events::{self, NewEvent, Source};
use crate::request::CallCtx;
use crate::session::NodeHub;
use crate::store::{Store, now_rfc3339};
use quoro_proto::wire;
use quoro_types::Scope;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row as _, SqliteConnection, SqlitePool};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// The change-log `entity_type` of a `volume_quarantines` row: the table's
/// name in the singular, like `volume`, `backup` and `audit_export`.
pub const ENTITY_TYPE: &str = "volume_quarantine";

/// The system actor that records a copy (`audit-log-format.md` section 3.8).
const ACTOR: &str = "volumes";

/// The largest integer a change-log payload carries: RFC 8785 refuses a
/// larger one (node-agent-protocol.md section 7.1).
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// An error on the report path, logged with its text.
type Failure = Box<dyn std::error::Error + Send + Sync>;

/// Columns of a `volume_quarantines` row, in the table's order.
const COLUMNS: &str = "id, volume_id, app_id, node_id, size_bytes, sha256, created_at, deleting_at";

/// One `volume_quarantines` row.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CopyRow {
    id: String,
    volume_id: String,
    app_id: String,
    node_id: String,
    size_bytes: i64,
    sha256: String,
    created_at: String,
    deleting_at: Option<String>,
}

impl CopyRow {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            volume_id: row.try_get("volume_id")?,
            app_id: row.try_get("app_id")?,
            node_id: row.try_get("node_id")?,
            size_bytes: row.try_get("size_bytes")?,
            sha256: row.try_get("sha256")?,
            created_at: row.try_get("created_at")?,
            deleting_at: row.try_get("deleting_at")?,
        })
    }

    /// The whole row under the table's column names: the payload of its
    /// change-log `create` and `delete` (node-agent-protocol.md section 5.14).
    fn payload(&self) -> Value {
        json!({
            "id": self.id,
            "volume_id": self.volume_id,
            "app_id": self.app_id,
            "node_id": self.node_id,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "created_at": self.created_at,
            "deleting_at": self.deleting_at,
        })
    }

    /// `params` of the audit record and `data` of the event.
    fn facts(&self) -> Value {
        json!({
            "app_id": self.app_id,
            "volume_id": self.volume_id,
            "node_id": self.node_id,
            "quarantine_id": self.id,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        })
    }
}

/// Why a reported entry can never get a row, whatever the database holds
/// (section 5.3). An absent `sha256` is not one: the copy is recorded once
/// its node reports the hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flaw {
    /// `quarantine_id` is not a well-formed `qrn_` id.
    Id,
    /// `sha256` is present and not 64 lowercase hex characters.
    Sha256,
    /// `created_at` is not an RFC 3339 date-time.
    CreatedAt,
    /// `size_bytes` is above 2^53 - 1, which the change log cannot carry.
    SizeBytes,
}

/// The first [`Flaw`] of `entry`, or `None`. Recording and
/// [`ListedCopies`] both check it, so a copy blocks a delete only while it
/// could get a row.
fn flaw(entry: &wire::QuarantineStatus) -> Option<Flaw> {
    if !is_id_with_prefix("qrn", &entry.quarantine_id) {
        Some(Flaw::Id)
    } else if entry
        .sha256
        .as_deref()
        .is_some_and(|sha256| !is_lower_hex_64(sha256))
    {
        Some(Flaw::Sha256)
    } else if parse_time(&entry.created_at).is_none() {
        Some(Flaw::CreatedAt)
    } else if entry.size_bytes > MAX_SAFE_INTEGER {
        Some(Flaw::SizeBytes)
    } else {
        None
    }
}

/// One copy a node listed in `status_report.quarantines`, hashed or not.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ListedCopy {
    quarantine_id: String,
    volume_id: String,
    app_id: String,
}

/// A copy [`ListedCopies::find`] found: the node that lists it and its id.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Found {
    node_id: String,
    quarantine_id: String,
}

/// The quarantined copies each node listed in its latest `status_report`,
/// hashed or not, for every entry without a [`Flaw`]: the entries that could
/// get a row. The refusals read it, so a copy blocks `deleteVolume` and
/// `deleteApp` from the first report that lists it, before its row exists:
/// the node may take minutes to hash a large copy (`openapi.yaml`
/// `deleteVolume`). A refusal counts a listed copy only while it names a
/// volume that is not `deleting`, of the copy's own app, as recording does.
/// Each report of a node replaces its set before anything else of the report
/// applies, an absent or empty field leaving none; a disconnect keeps it,
/// because the copy is still on the node's disk.
///
/// A node that `removeNode` removed is forgotten for good ([`Self::forget`]):
/// a report of it that is still being applied lists nothing any more.
///
/// It lives in memory only, as `status_report.warm` does: after a primary
/// restart or a promotion it is empty, so deletes are not refused for an
/// unhashed copy until its node reports again; until then only the recorded
/// rows refuse. A removed node cannot open a session again, so it never
/// reports after a restart.
#[derive(Debug, Default)]
pub(crate) struct ListedCopies(Mutex<ListedState>);

#[derive(Debug, Default)]
struct ListedState {
    by_node: HashMap<String, Vec<ListedCopy>>,
    /// The nodes [`ListedCopies::forget`] forgot: their reports list nothing.
    removed: HashSet<String>,
}

impl ListedCopies {
    /// Replaces the set of `node_id` with the copies its report lists,
    /// leaving out each entry with a [`Flaw`]. Silent: recording logs the
    /// bad entries. Does nothing for a node that was forgotten.
    pub(crate) fn replace(&self, node_id: &str, reported: &[wire::QuarantineStatus]) {
        let copies: Vec<ListedCopy> = reported
            .iter()
            .filter(|entry| flaw(entry).is_none())
            .map(|entry| ListedCopy {
                quarantine_id: entry.quarantine_id.clone(),
                volume_id: entry.volume_id.clone(),
                app_id: entry.app_id.clone(),
            })
            .collect();
        let mut listed = self.0.lock().expect("listed copies lock");
        if listed.removed.contains(node_id) {
            return;
        }
        if copies.is_empty() {
            listed.by_node.remove(node_id);
        } else {
            listed.by_node.insert(node_id.to_string(), copies);
        }
    }

    /// Forgets the copies `node_id` listed, for good: the node was removed,
    /// so a report of it still being applied lists nothing.
    #[cfg_attr(not(test), expect(dead_code, reason = "removeNode (#740) calls it"))]
    pub(crate) fn forget(&self, node_id: &str) {
        let mut listed = self.0.lock().expect("listed copies lock");
        listed.by_node.remove(node_id);
        listed.removed.insert(node_id.to_string());
    }

    /// A copy some node lists that `wanted` accepts. Never held across an
    /// `.await`: the lock is taken and released here.
    fn find(&self, wanted: impl Fn(&ListedCopy) -> bool) -> Option<Found> {
        self.0
            .lock()
            .expect("listed copies lock")
            .by_node
            .iter()
            .flat_map(|(node_id, copies)| copies.iter().map(move |copy| (node_id, copy)))
            .find(|(_, copy)| wanted(copy))
            .map(|(node_id, copy)| Found {
                node_id: node_id.clone(),
                quarantine_id: copy.quarantine_id.clone(),
            })
    }
}

/// What one report changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Applied {
    recorded: usize,
    dropped: usize,
}

/// Applies `status_report.quarantines` from `node_id` (section 5.3). Called
/// for every report, even one without the field: it records each new hashed
/// copy, and drops each record that is `deleting` and that the node no
/// longer lists, whose id then leaves the node's `spec.quarantine_deletes` in
/// the next revision. The node's listed copies ([`ListedCopies`]) are
/// replaced earlier, at the top of the report. A failure is logged, never
/// returned, so the rest of the report applies.
pub(crate) async fn apply_report(
    store: &Store,
    hub: &NodeHub,
    node_id: &str,
    reported: &[wire::QuarantineStatus],
) {
    let applied = apply(store, node_id, reported).await;
    if applied != Applied::default() {
        tracing::info!(
            node_id,
            recorded = applied.recorded,
            dropped = applied.dropped,
            "applied the reported quarantined copies"
        );
    }
    if applied.dropped > 0 {
        hub.node_changed(node_id);
    }
}

/// Reads first, and gives each record and each drop its own write
/// transaction, opened only when there is one to write: reports come every
/// 30 s, and one write that fails must not block the others. Each write
/// checks again what the read found.
async fn apply(store: &Store, node_id: &str, reported: &[wire::QuarantineStatus]) -> Applied {
    let actor = Actor::system(ACTOR);
    let mut applied = Applied::default();
    // Every id the report names counts as listed, even in an entry skipped
    // below: a record is dropped only once its node stops naming the copy.
    let listed: HashSet<&str> = reported
        .iter()
        .map(|entry| entry.quarantine_id.as_str())
        .collect();
    for entry in reported {
        let outcome = match new_copy(store.pool(), node_id, entry).await {
            Ok(Some(NewCopy::Row(copy))) => record_one(store, &actor, &copy).await,
            Ok(Some(NewCopy::Gone(copy))) => record_gone_one(store, &actor, &copy).await,
            Ok(None) => continue,
            Err(error) => Err(error),
        };
        match outcome {
            Ok(true) => applied.recorded += 1,
            Ok(false) => {}
            Err(error) => tracing::warn!(
                node_id,
                quarantine_id = %entry.quarantine_id,
                %error,
                "cannot record a reported quarantined copy"
            ),
        }
    }
    let deleting: Vec<String> = match sqlx::query_scalar(
        "SELECT id FROM volume_quarantines WHERE node_id = ? AND deleting_at IS NOT NULL \
         ORDER BY id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    {
        Ok(ids) => ids,
        Err(error) => {
            tracing::warn!(
                node_id,
                %error,
                "cannot read the deleted quarantined copies of a node"
            );
            return applied;
        }
    };
    for id in deleting.iter().filter(|id| !listed.contains(id.as_str())) {
        match drop_one(store, &actor, node_id, id).await {
            Ok(true) => applied.dropped += 1,
            Ok(false) => {}
            Err(error) => tracing::warn!(
                node_id,
                quarantine_id = %id,
                %error,
                "cannot drop the record of a deleted quarantined copy"
            ),
        }
    }
    applied
}

/// Records one copy in a write transaction of its own.
async fn record_one(store: &Store, actor: &Actor, copy: &CopyRow) -> Result<bool, Failure> {
    let mut w = store.begin_write().await?;
    let recorded = record(&mut w.tx, actor, copy).await?;
    if recorded {
        w.commit().await?;
    }
    Ok(recorded)
}

/// Records one copy of a volume that is gone in a write transaction of its
/// own.
async fn record_gone_one(store: &Store, actor: &Actor, copy: &CopyRow) -> Result<bool, Failure> {
    let mut w = store.begin_write().await?;
    let recorded = record_gone(&mut w.tx, actor, copy).await?;
    if recorded {
        w.commit().await?;
    }
    Ok(recorded)
}

/// Drops one record in a write transaction of its own.
async fn drop_one(store: &Store, actor: &Actor, node_id: &str, id: &str) -> Result<bool, Failure> {
    let mut w = store.begin_write().await?;
    let dropped = drop_deleted(&mut w.tx, actor, node_id, id).await?;
    if dropped {
        w.commit().await?;
    }
    Ok(dropped)
}

/// What a reported entry adds.
#[derive(Clone, Debug, PartialEq, Eq)]
enum NewCopy {
    /// A copy of a live volume of its own app: its row, with the audit
    /// record and the event ([`record`]).
    Row(CopyRow),
    /// A copy of a volume that is gone or `deleting`: the audit record and
    /// the event only, once per `quarantine_id` ([`record_gone`]). Its
    /// `deleting_at` is never used.
    Gone(CopyRow),
}

/// What a reported entry adds, or `None` when it adds nothing: a bad entry
/// (logged and skipped), a copy whose hash the node is still computing, or
/// one already recorded with a row.
async fn new_copy(
    pool: &SqlitePool,
    node_id: &str,
    entry: &wire::QuarantineStatus,
) -> Result<Option<NewCopy>, Failure> {
    let id = entry.quarantine_id.as_str();
    if let Some(flaw) = flaw(entry) {
        warn_flaw(node_id, entry, flaw);
        return Ok(None);
    }
    let Some(sha256) = entry.sha256.as_deref() else {
        // Recorded once the node reports the hash (section 5.3).
        return Ok(None);
    };
    // At most 2^53 - 1 ([`Flaw::SizeBytes`]), so it fits.
    let size_bytes = i64::try_from(entry.size_bytes)?;
    let holder: Option<(String,)> =
        sqlx::query_as("SELECT node_id FROM volume_quarantines WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    if let Some((holder,)) = holder {
        if holder != node_id {
            tracing::warn!(
                node_id,
                quarantine_id = id,
                recorded_node_id = %holder,
                "a node reports a quarantined copy recorded for another node; ignored"
            );
        }
        return Ok(None);
    }
    let copy = CopyRow {
        id: id.to_string(),
        volume_id: entry.volume_id.clone(),
        app_id: entry.app_id.clone(),
        node_id: node_id.to_string(),
        size_bytes,
        sha256: sha256.to_string(),
        created_at: entry.created_at.clone(),
        deleting_at: None,
    };
    Ok(match volume_of(pool, node_id, entry).await? {
        Named::Live => Some(NewCopy::Row(copy)),
        Named::Gone => Some(NewCopy::Gone(copy)),
        Named::OtherApp => None,
    })
}

/// Logs why a reported entry is skipped.
fn warn_flaw(node_id: &str, entry: &wire::QuarantineStatus, flaw: Flaw) {
    let quarantine_id = entry.quarantine_id.as_str();
    match flaw {
        Flaw::Id => tracing::warn!(
            node_id,
            quarantine_id,
            "a reported quarantined copy has a malformed id; skipped"
        ),
        Flaw::Sha256 => tracing::warn!(
            node_id,
            quarantine_id,
            "a reported quarantined copy has a malformed sha256; skipped"
        ),
        Flaw::CreatedAt => tracing::warn!(
            node_id,
            quarantine_id,
            created_at = %entry.created_at,
            "a reported quarantined copy has a created_at that is not an RFC 3339 date-time; \
             skipped"
        ),
        Flaw::SizeBytes => tracing::warn!(
            node_id,
            quarantine_id,
            size_bytes = entry.size_bytes,
            "a reported quarantined copy has a size_bytes above 2^53 - 1, which the change \
             log cannot carry; skipped"
        ),
    }
}

/// What the volume a reported copy names is, for that copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Named {
    /// It exists, is not `deleting` and belongs to the copy's app.
    Live,
    /// It is gone, or `deleting`. A row naming a volume already `deleting`
    /// would make the foreign key fail the volume sweep (`DELETE FROM
    /// volumes` when its node reports it `deleted`), which the sweep must
    /// never meet (migration 0011), so such a copy gets no row (ADR 0316).
    Gone,
    /// It belongs to another app than the copy's: a bad entry, skipped with
    /// a warning.
    OtherApp,
}

/// What the volume `entry` names is ([`Named`]). A gone volume's app is unknown, so
/// the copy is taken as reported.
async fn volume_of(
    pool: &SqlitePool,
    node_id: &str,
    entry: &wire::QuarantineStatus,
) -> Result<Named, Failure> {
    let volume: Option<(String, String)> =
        sqlx::query_as("SELECT app_id, state FROM volumes WHERE id = ?")
            .bind(&entry.volume_id)
            .fetch_optional(pool)
            .await?;
    Ok(match volume {
        Some((app_id, _)) if app_id != entry.app_id => {
            tracing::warn!(
                node_id,
                quarantine_id = %entry.quarantine_id,
                volume_id = %entry.volume_id,
                reported_app_id = %entry.app_id,
                "a reported quarantined copy names another app than its volume's; skipped"
            );
            Named::OtherApp
        }
        Some((_, state)) if state != "deleting" => Named::Live,
        _ => Named::Gone,
    })
}

/// Records one copy in the caller's write transaction: the row, its
/// change-log `create`, the event `volume.quarantined` and the system audit
/// record `volume.quarantine`. Returns `false`, writing nothing, when the
/// row appeared or the volume went since the read, or when the node was
/// removed since: [`drop_node_copies`] dropped its records, and a report
/// still being applied must not bring one back.
async fn record(
    conn: &mut SqliteConnection,
    actor: &Actor,
    copy: &CopyRow,
) -> Result<bool, Failure> {
    let status: Option<(String,)> = sqlx::query_as("SELECT status FROM nodes WHERE id = ?")
        .bind(&copy.node_id)
        .fetch_optional(&mut *conn)
        .await?;
    if !status.is_some_and(|(status,)| status != "removed") {
        return Ok(false);
    }
    let exists: Option<(String,)> =
        sqlx::query_as("SELECT id FROM volume_quarantines WHERE id = ?")
            .bind(&copy.id)
            .fetch_optional(&mut *conn)
            .await?;
    if exists.is_some() {
        return Ok(false);
    }
    // As at the read: only a live volume of the copy's app gets a record.
    let volume: Option<(String, String)> =
        sqlx::query_as("SELECT app_id, state FROM volumes WHERE id = ?")
            .bind(&copy.volume_id)
            .fetch_optional(&mut *conn)
            .await?;
    if !volume.is_some_and(|(app_id, state)| app_id == copy.app_id && state != "deleting") {
        return Ok(false);
    }
    sqlx::query(&format!(
        "INSERT INTO volume_quarantines ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)"
    ))
    .bind(&copy.id)
    .bind(&copy.volume_id)
    .bind(&copy.app_id)
    .bind(&copy.node_id)
    .bind(copy.size_bytes)
    .bind(&copy.sha256)
    .bind(&copy.created_at)
    .execute(&mut *conn)
    .await?;
    let change_seq = changelog::append(
        &mut *conn,
        actor,
        ENTITY_TYPE,
        &copy.id,
        Op::Create,
        copy.payload(),
    )
    .await?;
    announce(conn, actor, copy, Some(change_seq)).await?;
    Ok(true)
}

/// Records one copy of a volume that is gone or `deleting` in the caller's
/// write transaction: the event `volume.quarantined` and the system audit
/// record `volume.quarantine`, with no row and no change-log entry of its
/// own (section 9, ADR 0316). Returns `false`, writing nothing, when the
/// copy got its audit record before, also before a restart of the primary,
/// or now has a row, or its volume is live since the read (the next report
/// then records a row).
async fn record_gone(
    conn: &mut SqliteConnection,
    actor: &Actor,
    copy: &CopyRow,
) -> Result<bool, Failure> {
    let row: Option<(String,)> = sqlx::query_as("SELECT id FROM volume_quarantines WHERE id = ?")
        .bind(&copy.id)
        .fetch_optional(&mut *conn)
        .await?;
    if row.is_some() {
        return Ok(false);
    }
    let live: Option<(String,)> =
        sqlx::query_as("SELECT id FROM volumes WHERE id = ? AND state != 'deleting'")
            .bind(&copy.volume_id)
            .fetch_optional(&mut *conn)
            .await?;
    if live.is_some() {
        return Ok(false);
    }
    // Once per `quarantine_id`: the audit chain is the record, and
    // `idx_audit_target` finds it by the volume.
    let recorded: Option<(i64,)> = sqlx::query_as(
        "SELECT seq FROM audit_log WHERE target_id = ? AND operation = 'volume.quarantine' \
         AND json_extract(params, '$.quarantine_id') = ? LIMIT 1",
    )
    .bind(&copy.volume_id)
    .bind(&copy.id)
    .fetch_optional(&mut *conn)
    .await?;
    if recorded.is_some() {
        return Ok(false);
    }
    announce(conn, actor, copy, None).await?;
    Ok(true)
}

/// Emits `volume.quarantined` and appends the system audit record
/// `volume.quarantine` for `copy`, with `change_seq` the row's change-log
/// entry, or `None` for a copy without a row.
async fn announce(
    conn: &mut SqliteConnection,
    actor: &Actor,
    copy: &CopyRow,
    change_seq: Option<i64>,
) -> Result<(), Failure> {
    let project: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
        .bind(&copy.app_id)
        .fetch_optional(&mut *conn)
        .await?;
    events::emit(
        &mut *conn,
        NewEvent {
            kind: "volume.quarantined",
            source: Source::ControlPlane,
            subject_type: "volume",
            subject_id: &copy.volume_id,
            project_id: project.as_ref().map(|(project_id,)| project_id.as_str()),
            request_id: None,
            actor: None,
            data: copy.facts(),
        },
    )
    .await?;
    let record = Record {
        actor: actor.clone(),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: "volume.quarantine".to_string(),
        scope: None,
        target_type: Some("volume".to_string()),
        target_id: Some(copy.volume_id.clone()),
        project_id: None,
        params: copy.facts(),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: quoro_types::new_id(quoro_types::Prefix::Request),
        result: Outcome::Ok,
        result_detail: None,
        change_seq,
    };
    audit::append(&mut *conn, &record).await?;
    Ok(())
}

/// Drops the record of a copy `node_id` no longer reports, when it is still
/// that node's and `deleting`, with a change-log `delete`. Returns whether
/// it did.
async fn drop_deleted(
    conn: &mut SqliteConnection,
    actor: &Actor,
    node_id: &str,
    id: &str,
) -> Result<bool, Failure> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM volume_quarantines \
         WHERE id = ? AND node_id = ? AND deleting_at IS NOT NULL"
    ))
    .bind(id)
    .bind(node_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let copy = CopyRow::from_row(&row)?;
    sqlx::query("DELETE FROM volume_quarantines WHERE id = ?")
        .bind(&copy.id)
        .execute(&mut *conn)
        .await?;
    changelog::append(
        &mut *conn,
        actor,
        ENTITY_TYPE,
        &copy.id,
        Op::Delete,
        copy.payload(),
    )
    .await?;
    Ok(true)
}

/// Drops every record of a quarantined copy that `node_id` keeps, `deleting`
/// or not, each with a change-log `delete` under `actor`, in the caller's
/// transaction: `removeNode` calls it in the transaction that marks the node
/// `removed` (section 10), so a node that is gone for good no longer blocks
/// `deleteVolume` or `deleteApp`. The copies stay on the node's disk. No
/// audit record or event is written per copy: the call's own audit record
/// covers them. Returns the dropped ids, by id. After the transaction
/// commits, the caller calls [`forget_listed`]. A report of the node still
/// being applied records no row afterwards ([`record`] reads the node's
/// status in its own write transaction).
///
/// # Errors
///
/// A database or change-log error; the caller's transaction then rolls back
/// with nothing dropped.
#[cfg_attr(not(test), expect(dead_code, reason = "removeNode (#740) calls it"))]
pub(crate) async fn drop_node_copies(
    conn: &mut SqliteConnection,
    actor: &Actor,
    node_id: &str,
) -> Result<Vec<String>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM volume_quarantines WHERE node_id = ? ORDER BY id"
    ))
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut dropped = Vec::with_capacity(rows.len());
    for row in &rows {
        let copy = CopyRow::from_row(row)?;
        sqlx::query("DELETE FROM volume_quarantines WHERE id = ?")
            .bind(&copy.id)
            .execute(&mut *conn)
            .await?;
        changelog::append(
            &mut *conn,
            actor,
            ENTITY_TYPE,
            &copy.id,
            Op::Delete,
            copy.payload(),
        )
        .await?;
        dropped.push(copy.id);
    }
    Ok(dropped)
}

/// Forgets the copies `node_id` listed in its latest report
/// ([`ListedCopies`]), so a removed node's unhashed copies no longer refuse
/// `deleteVolume` and `deleteApp`. `removeNode` calls it once the transaction
/// that ran [`drop_node_copies`] has committed.
#[expect(dead_code, reason = "removeNode (#740) calls it")]
pub(crate) fn forget_listed(hub: &NodeHub, node_id: &str) {
    hub.listed_copies().forget(node_id);
}

/// `spec.quarantine_deletes` of `node_id` (section 5.11): the ids of the
/// copies it keeps that an operator deleted, by id.
///
/// # Errors
///
/// A database error, as text for the snapshot builder.
pub(crate) async fn deletes_on(pool: &SqlitePool, node_id: &str) -> Result<Vec<String>, String> {
    sqlx::query_scalar(
        "SELECT id FROM volume_quarantines WHERE node_id = ? AND deleting_at IS NOT NULL \
         ORDER BY id",
    )
    .bind(node_id)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the quarantined copies to delete on {node_id}: {e}"))
}

/// One element of the `quarantines` column of [`VOLUME_COLUMNS`].
#[derive(Deserialize)]
struct Listed {
    id: String,
    node_id: String,
    size_bytes: i64,
    sha256: String,
    created_at: String,
    deleting_at: Option<String>,
}

/// `Volume.quarantines` from the JSON array the `quarantines` column of
/// [`VOLUME_COLUMNS`] holds: oldest first by instant, whatever RFC 3339 form
/// the node wrote, then by id, with `state` `deleting` once `deleting_at` is
/// set.
///
/// # Errors
///
/// An internal error when the column is not that array.
pub(crate) fn from_column(text: &str) -> Result<Vec<VolumeQuarantine>, ApiError> {
    let listed: Vec<Listed> = serde_json::from_str(text)
        .map_err(|error| ApiError::internal(&format!("volume quarantines: {error}")))?;
    // Every recorded `created_at` parses: [`new_copy`] skips one that does not.
    let mut keyed: Vec<_> = listed
        .into_iter()
        .map(|copy| ((parse_time(&copy.created_at), copy.id.clone()), copy))
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(keyed
        .into_iter()
        .map(|(_, copy)| VolumeQuarantine {
            id: copy.id,
            node_id: copy.node_id,
            size_bytes: copy.size_bytes,
            sha256: copy.sha256,
            state: if copy.deleting_at.is_some() {
                VolumeQuarantineState::Deleting
            } else {
                VolumeQuarantineState::Kept
            },
            created_at: copy.created_at,
        })
        .collect())
}

/// `conflict` with `reason: has_quarantines` (`openapi.yaml` `Problem`).
fn has_quarantines(detail: &str) -> ApiError {
    ApiError {
        reason: Some(Reason::HasQuarantines),
        ..ApiError::conflict(detail)
    }
}

/// `has_quarantines` for a copy a node lists that has no row yet, naming
/// the node and the copy.
fn listed_not_recorded(found: &Found) -> ApiError {
    has_quarantines(&format!(
        "the quarantined copy {} on node {} is not recorded yet because its node is still \
         hashing it: once it is recorded, a human deletes it first (deleteVolumeQuarantine)",
        found.quarantine_id, found.node_id
    ))
}

/// Refuses `deleteVolume` while a quarantined copy of `volume_id` is
/// recorded, or while a node's latest report lists one that could get a row
/// ([`ListedCopies`]): the volume is not `deleting` and the copy names the
/// volume's app (`openapi.yaml` `deleteVolume`, ADR 0176).
///
/// # Errors
///
/// `conflict` with `reason: has_quarantines`, whose detail names the node and
/// the copy when the copy has no row yet, or a database error.
pub(crate) async fn refuse_volume_with_copies(
    conn: &mut SqliteConnection,
    listed: &ListedCopies,
    volume_id: &str,
) -> Result<(), ApiError> {
    let kept: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM volume_quarantines WHERE volume_id = ? LIMIT 1")
            .bind(volume_id)
            .fetch_optional(&mut *conn)
            .await?;
    if kept.is_some() {
        return Err(has_quarantines(
            "the volume keeps a quarantined copy: a human deletes it first \
             (deleteVolumeQuarantine)",
        ));
    }
    let volume: Option<(String, String)> =
        sqlx::query_as("SELECT app_id, state FROM volumes WHERE id = ?")
            .bind(volume_id)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((app_id, state)) = volume else {
        return Ok(());
    };
    if state == "deleting" {
        return Ok(());
    }
    match listed.find(|copy| copy.volume_id == volume_id && copy.app_id == app_id) {
        Some(found) => Err(listed_not_recorded(&found)),
        None => Ok(()),
    }
}

/// Refuses `deleteApp` while a quarantined copy of any volume of `app_id` is
/// recorded, or while a node's latest report lists one that could get a row
/// ([`ListedCopies`]): the copy names the app and one of its volumes that is
/// not `deleting` (`openapi.yaml` `deleteApp`, ADR 0176).
///
/// # Errors
///
/// `conflict` with `reason: has_quarantines`, whose detail names the node and
/// the copy when the copy has no row yet, or a database error.
pub(crate) async fn refuse_app_with_copies(
    conn: &mut SqliteConnection,
    listed: &ListedCopies,
    app_id: &str,
) -> Result<(), ApiError> {
    let kept: Option<(i64,)> = sqlx::query_as(
        "SELECT 1 FROM volume_quarantines q JOIN volumes v ON v.id = q.volume_id \
         WHERE v.app_id = ? LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    if kept.is_some() {
        return Err(has_quarantines(
            "a volume of the app keeps a quarantined copy: a human deletes it first \
             (deleteVolumeQuarantine)",
        ));
    }
    let live_volume_ids: HashSet<String> =
        sqlx::query_scalar("SELECT id FROM volumes WHERE app_id = ? AND state != 'deleting'")
            .bind(app_id)
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .collect();
    match listed.find(|copy| copy.app_id == app_id && live_volume_ids.contains(&copy.volume_id)) {
        Some(found) => Err(listed_not_recorded(&found)),
        None => Ok(()),
    }
}

/// The copy [`delete_copy`] turned `deleting`.
pub(crate) struct DeletedCopy {
    /// The copy's volume, as the call answers it.
    pub volume: Volume,
    /// The node that keeps the copy, whose desired state now names it.
    pub node_id: String,
}

/// `deleteVolumeQuarantine` after its authorization: sets `deleting_at` with
/// a change-log `update` of the key and `deleting_at`, and writes the call's
/// audit record, in one write transaction. A copy already `deleting` changes
/// nothing and answers again, with its own audit record.
///
/// # Errors
///
/// `not_found` for an unknown or malformed volume or copy, or a copy of
/// another volume, or a database error.
pub(crate) async fn delete_copy(
    state: &AppState,
    ctx: &CallCtx,
    volume_id: &str,
    quarantine_id: &str,
    project_id: Option<&str>,
    params: &Value,
) -> Result<DeletedCopy, ApiError> {
    if !is_id_with_prefix("vol", volume_id) {
        return Err(ApiError::not_found("volume"));
    }
    if !is_id_with_prefix("qrn", quarantine_id) {
        return Err(ApiError::not_found("quarantined copy"));
    }
    let mut w = state.store.begin_write().await?;
    let volume: Option<(String,)> = sqlx::query_as("SELECT id FROM volumes WHERE id = ?")
        .bind(volume_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if volume.is_none() {
        return Err(ApiError::not_found("volume"));
    }
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM volume_quarantines WHERE id = ? AND volume_id = ?"
    ))
    .bind(quarantine_id)
    .bind(volume_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("quarantined copy"))?;
    let copy = CopyRow::from_row(&row)?;
    let change_seq = match &copy.deleting_at {
        Some(_) => None,
        None => {
            let deleting_at = now_rfc3339();
            sqlx::query("UPDATE volume_quarantines SET deleting_at = ? WHERE id = ?")
                .bind(&deleting_at)
                .bind(&copy.id)
                .execute(&mut *w.tx)
                .await?;
            Some(
                changelog::append(
                    &mut w.tx,
                    &ctx.identity.actor(),
                    ENTITY_TYPE,
                    &copy.id,
                    Op::Update,
                    json!({ "id": copy.id, "deleting_at": deleting_at }),
                )
                .await?,
            )
        }
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppDeleteQuarantine),
        Some(&volume_target(volume_id)),
        project_id,
        params,
        change_seq,
    )
    .await?;
    let row = sqlx::query(&format!(
        "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
    ))
    .bind(volume_id)
    .fetch_one(&mut *w.tx)
    .await?;
    let volume = volume_from_row(&row)?;
    w.commit().await?;
    Ok(DeletedCopy {
        volume,
        node_id: copy.node_id,
    })
}

/// `text` as an RFC 3339 date-time, or `None` when it is not one.
fn parse_time(text: &str) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
}

/// Whether `text` is 64 lowercase hexadecimal characters.
fn is_lower_hex_64(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sha256_is_64_lowercase_hex_characters() {
        assert!(is_lower_hex_64(&"0a".repeat(32)));
        assert!(!is_lower_hex_64(&"0A".repeat(32)), "uppercase");
        assert!(!is_lower_hex_64(&"0a".repeat(31)), "short");
        assert!(!is_lower_hex_64(&"0a".repeat(33)), "long");
        assert!(!is_lower_hex_64(&format!("{}g", "0".repeat(63))), "not hex");
    }

    #[test]
    fn copies_read_oldest_first_with_their_state() {
        let text = json!([
            { "id": "qrn_B", "node_id": "nod_1", "size_bytes": 2, "sha256": "b",
              "created_at": "2026-11-09T10:00:00Z", "deleting_at": "2026-11-09T11:00:00Z" },
            // The same instant as qrn_C, written another way: the id decides.
            { "id": "qrn_D", "node_id": "nod_2", "size_bytes": 4, "sha256": "d",
              "created_at": "2026-11-09T10:30:00.000Z", "deleting_at": null },
            { "id": "qrn_A", "node_id": "nod_1", "size_bytes": 1, "sha256": "a",
              "created_at": "2026-11-09T11:00:00+02:00", "deleting_at": null },
            { "id": "qrn_C", "node_id": "nod_1", "size_bytes": 3, "sha256": "c",
              "created_at": "2026-11-09T10:30:00Z", "deleting_at": null },
        ])
        .to_string();
        let copies = from_column(&text).unwrap();
        let order: Vec<(&str, VolumeQuarantineState)> = copies
            .iter()
            .map(|copy| (copy.id.as_str(), copy.state))
            .collect();
        assert_eq!(
            order,
            [
                ("qrn_A", VolumeQuarantineState::Kept),
                ("qrn_B", VolumeQuarantineState::Deleting),
                ("qrn_C", VolumeQuarantineState::Kept),
                ("qrn_D", VolumeQuarantineState::Kept),
            ]
        );
        assert!(from_column("[]").unwrap().is_empty());
    }

    #[test]
    fn a_report_replaces_its_nodes_listed_copies() {
        let entry = |id: &str, volume_id: &str, app_id: &str| wire::QuarantineStatus {
            quarantine_id: id.to_string(),
            volume_id: volume_id.to_string(),
            app_id: app_id.to_string(),
            created_at: "2026-11-09T10:00:00Z".to_string(),
            size_bytes: MAX_SAFE_INTEGER,
            sha256: None,
        };
        let id = |n: u32| format!("qrn_01JC9Z6Q5W8N3K4T7R2M{n:06}");
        let found = |node_id: &str, quarantine_id: &str| {
            Some(Found {
                node_id: node_id.to_string(),
                quarantine_id: quarantine_id.to_string(),
            })
        };
        let hashed = wire::QuarantineStatus {
            sha256: Some("a".repeat(64)),
            ..entry(&id(2), "vol_2", "app_1")
        };
        let bad_sha256 = wire::QuarantineStatus {
            sha256: Some("A".repeat(64)),
            ..entry(&id(3), "vol_3", "app_1")
        };
        let bad_created_at = wire::QuarantineStatus {
            created_at: "2026-11-09".to_string(),
            ..entry(&id(4), "vol_4", "app_1")
        };
        let oversized = wire::QuarantineStatus {
            size_bytes: MAX_SAFE_INTEGER + 1,
            ..entry(&id(5), "vol_5", "app_1")
        };
        let listed = ListedCopies::default();
        listed.replace(
            "nod_a",
            &[
                entry(&id(1), "vol_1", "app_1"),
                hashed,
                entry("qrn_bad", "vol_6", "app_1"),
                bad_sha256,
                bad_created_at,
                oversized,
            ],
        );
        listed.replace("nod_b", &[entry(&id(7), "vol_7", "app_7")]);
        assert_eq!(
            listed.find(|copy| copy.volume_id == "vol_1"),
            found("nod_a", &id(1))
        );
        assert_eq!(
            listed.find(|copy| copy.volume_id == "vol_2"),
            found("nod_a", &id(2)),
            "a hashed copy is listed too"
        );
        for (volume_id, case) in [
            ("vol_3", "a malformed sha256"),
            ("vol_4", "a created_at that is not an RFC 3339 date-time"),
            ("vol_5", "a size_bytes above 2^53 - 1"),
            ("vol_6", "a malformed id"),
        ] {
            assert_eq!(
                listed.find(|copy| copy.volume_id == volume_id),
                None,
                "{case}"
            );
        }
        assert_eq!(
            listed.find(|copy| copy.app_id == "app_7"),
            found("nod_b", &id(7))
        );
        // A report without the field leaves none for that node only.
        listed.replace("nod_a", &[]);
        assert_eq!(listed.find(|copy| copy.app_id == "app_1"), None);
        assert_eq!(
            listed.find(|copy| copy.volume_id == "vol_7"),
            found("nod_b", &id(7))
        );
    }

    #[test]
    fn a_created_at_must_be_an_rfc_3339_date_time() {
        for good in ["2026-11-09T10:00:00Z", "2026-11-09T10:00:00.123+02:00"] {
            assert!(parse_time(good).is_some(), "{good}");
        }
        for bad in ["", "yesterday", "2026-11-09", "2026-11-09T10:00:00"] {
            assert!(parse_time(bad).is_none(), "{bad}");
        }
    }

    /// A store with two nodes, one app with a volume, and three recorded
    /// copies: two on `nod_a` (one of them `deleting`), one on `nod_b`.
    async fn store_with_copies() -> Store {
        let store = Store::open_in_memory().await.unwrap();
        let pool = store.pool();
        let now = "2026-11-10T10:00:00Z";
        for node_id in ["nod_a", "nod_b"] {
            sqlx::query(
                "INSERT INTO nodes (id, name, roles, status, public_key, behind_nat, labels, \
                 capacity, allocated, kvm, created_at) \
                 VALUES (?, ?, '[\"node\"]', 'disconnected', 'key', 0, '{}', '{}', '{}', '{}', ?)",
            )
            .bind(node_id)
            .bind(node_id)
            .bind(now)
            .execute(pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) \
             VALUES ('prj_1', 'p', 'p', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO apps (id, project_id, name, image, port, health_check, resources, \
             placement, status, created_at, updated_at) \
             VALUES ('app_1', 'prj_1', 'db', 'image', 8080, '{}', '{}', '{}', 'running', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
             created_at, updated_at) VALUES ('vol_1', 'app_1', 'nod_b', 1, '/data', 'ready', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
        for (id, node_id, deleting_at) in [
            ("qrn_2", "nod_a", None),
            ("qrn_1", "nod_a", Some(now)),
            ("qrn_3", "nod_b", None),
        ] {
            sqlx::query(&format!(
                "INSERT INTO volume_quarantines ({COLUMNS}) \
                 VALUES (?, 'vol_1', 'app_1', ?, 7, ?, ?, ?)"
            ))
            .bind(id)
            .bind(node_id)
            .bind("a".repeat(64))
            .bind(now)
            .bind(deleting_at)
            .execute(pool)
            .await
            .unwrap();
        }
        store
    }

    /// The `(entity_type, entity_id, op, payload)` of every change-log entry.
    async fn log_entries(store: &Store) -> Vec<(String, String, String, Value)> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT entity_type, entity_id, op, payload FROM change_log ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        rows.into_iter()
            .map(|(kind, id, op, payload)| (kind, id, op, serde_json::from_str(&payload).unwrap()))
            .collect()
    }

    async fn remaining(store: &Store) -> Vec<String> {
        sqlx::query_scalar("SELECT id FROM volume_quarantines ORDER BY id")
            .fetch_all(store.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn removing_a_node_drops_all_its_records_and_logs_each_delete() {
        let store = store_with_copies().await;
        let before = log_entries(&store).await.len();
        let actor = Actor::system("nodes");

        let mut w = store.begin_write().await.unwrap();
        let dropped = drop_node_copies(&mut w.tx, &actor, "nod_a").await.unwrap();
        w.commit().await.unwrap();

        // Both of the node's copies, `deleting` or not, by id; the other
        // node's copy stays.
        assert_eq!(dropped, ["qrn_1", "qrn_2"]);
        assert_eq!(remaining(&store).await, ["qrn_3"]);

        let entries = log_entries(&store).await;
        let new = &entries[before..];
        assert_eq!(new.len(), 2, "{new:?}");
        for ((kind, id, op, payload), expected) in new.iter().zip(["qrn_1", "qrn_2"]) {
            assert_eq!(
                (kind.as_str(), id.as_str(), op.as_str()),
                (ENTITY_TYPE, expected, "delete")
            );
            assert_eq!(payload["id"], expected);
            assert_eq!(payload["node_id"], "nod_a");
            assert_eq!(payload["volume_id"], "vol_1");
        }
        let (deleting_at, sha256) = (&new[0].3["deleting_at"], &new[0].3["sha256"]);
        assert_eq!(
            deleting_at, "2026-11-10T10:00:00Z",
            "the whole row is the payload"
        );
        assert_eq!(sha256, &json!("a".repeat(64)));

        // No audit record per copy: the caller's own record covers them.
        let audited: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(audited, 0);
    }

    #[tokio::test]
    async fn a_node_without_records_drops_nothing() {
        let store = store_with_copies().await;
        let before = log_entries(&store).await.len();
        let mut w = store.begin_write().await.unwrap();
        let dropped = drop_node_copies(&mut w.tx, &Actor::system("nodes"), "nod_c")
            .await
            .unwrap();
        w.commit().await.unwrap();
        assert!(dropped.is_empty());
        assert_eq!(remaining(&store).await, ["qrn_1", "qrn_2", "qrn_3"]);
        assert_eq!(log_entries(&store).await.len(), before);
    }

    #[tokio::test]
    async fn a_transaction_that_does_not_commit_keeps_every_record() {
        let store = store_with_copies().await;
        let before = log_entries(&store).await.len();
        {
            let mut w = store.begin_write().await.unwrap();
            let dropped = drop_node_copies(&mut w.tx, &Actor::system("nodes"), "nod_a")
                .await
                .unwrap();
            assert_eq!(dropped.len(), 2);
            // Dropped without a commit, as a later refusal in removeNode would.
        }
        assert_eq!(remaining(&store).await, ["qrn_1", "qrn_2", "qrn_3"]);
        assert_eq!(log_entries(&store).await.len(), before);
    }

    #[test]
    fn forgetting_a_node_leaves_the_other_nodes_listed_copies() {
        let entry = |volume_id: &str| wire::QuarantineStatus {
            quarantine_id: "qrn_01JC9Z6Q5W8N3K4T7R2M000001".to_string(),
            volume_id: volume_id.to_string(),
            app_id: "app_1".to_string(),
            created_at: "2026-11-09T10:00:00Z".to_string(),
            size_bytes: 1,
            sha256: None,
        };
        let listed = ListedCopies::default();
        listed.replace("nod_a", &[entry("vol_a")]);
        listed.replace("nod_b", &[entry("vol_b")]);
        listed.forget("nod_a");
        assert_eq!(listed.find(|copy| copy.volume_id == "vol_a"), None);
        assert!(listed.find(|copy| copy.volume_id == "vol_b").is_some());
        // Forgetting a node that listed nothing changes nothing.
        listed.forget("nod_c");
        assert!(listed.find(|copy| copy.volume_id == "vol_b").is_some());
        // A report of a forgotten node still being applied lists nothing.
        listed.replace("nod_a", &[entry("vol_a")]);
        assert_eq!(listed.find(|copy| copy.volume_id == "vol_a"), None);
        // Another node's report still replaces its own set.
        listed.replace("nod_b", &[entry("vol_c")]);
        assert_eq!(listed.find(|copy| copy.volume_id == "vol_b"), None);
        assert!(listed.find(|copy| copy.volume_id == "vol_c").is_some());
    }

    const LATE: &str = "qrn_01JC9Z6Q5W8N3K4T7R2M000009";

    /// A hashed copy of `vol_1` that `nod_a` reports.
    fn late_copy() -> wire::QuarantineStatus {
        wire::QuarantineStatus {
            quarantine_id: LATE.to_string(),
            volume_id: "vol_1".to_string(),
            app_id: "app_1".to_string(),
            created_at: "2026-11-10T10:00:00Z".to_string(),
            size_bytes: 7,
            sha256: Some("b".repeat(64)),
        }
    }

    /// What `removeNode` does to `node_id`'s records: one transaction marks
    /// the node `removed` and drops them.
    async fn remove_node(store: &Store, node_id: &str) -> Vec<String> {
        let mut w = store.begin_write().await.unwrap();
        sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
            .bind(node_id)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let dropped = drop_node_copies(&mut w.tx, &Actor::system("nodes"), node_id)
            .await
            .unwrap();
        w.commit().await.unwrap();
        dropped
    }

    async fn audited(store: &Store) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_report_applied_after_the_drop_does_not_record_the_copy_again() {
        let store = store_with_copies().await;
        // The report records the copy while the node is in the cluster (and
        // drops `qrn_1`, which is `deleting` and no longer listed).
        let applied = apply(&store, "nod_a", &[late_copy()]).await;
        assert_eq!((applied.recorded, applied.dropped), (1, 1));
        assert_eq!(remaining(&store).await, [LATE, "qrn_2", "qrn_3"]);

        let dropped = remove_node(&store, "nod_a").await;
        assert_eq!(dropped, [LATE, "qrn_2"]);
        let before = (log_entries(&store).await.len(), audited(&store).await);

        // The same report, still being applied once the removal committed,
        // records nothing: no row, no change-log entry, no audit record.
        let applied = apply(&store, "nod_a", &[late_copy()]).await;
        assert_eq!(applied, Applied::default());
        assert_eq!(remaining(&store).await, ["qrn_3"]);
        assert_eq!(
            (log_entries(&store).await.len(), audited(&store).await),
            before
        );
    }

    #[tokio::test]
    async fn the_change_log_replayed_after_the_drop_leaves_no_row_of_the_node() {
        let store = store_with_copies().await;
        apply(&store, "nod_a", &[late_copy()]).await;
        remove_node(&store, "nod_a").await;
        apply(&store, "nod_a", &[late_copy()]).await;

        // Replayed in `seq` order, as a promotion rebuilds the table: the
        // copy's `create` is followed by its `delete`, and nothing after.
        let mut rebuilt: Vec<String> = Vec::new();
        let mut ops = Vec::new();
        for (kind, id, op, _) in log_entries(&store).await {
            if kind != ENTITY_TYPE {
                continue;
            }
            if id == LATE {
                ops.push(op.clone());
            }
            match op.as_str() {
                "create" => rebuilt.push(id),
                "delete" => rebuilt.retain(|kept| *kept != id),
                other => panic!("unexpected op {other}"),
            }
        }
        assert_eq!(ops, ["create", "delete"]);
        assert!(rebuilt.is_empty(), "{rebuilt:?}");
    }
}

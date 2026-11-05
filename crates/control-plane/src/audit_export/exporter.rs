// SPDX-License-Identifier: AGPL-3.0-only
//! The background exporter of the off-site copy (`audit-log-format.md`
//! sections 8.1 and 8.3).
//!
//! Every [`EXPORT_INTERVAL`] the loop closes a segment when the audit log
//! holds 1,000 unexported records or the oldest of them is five minutes old,
//! writes it and its head with `If-None-Match: *`, and only then records the
//! progress in `audit_exports`. Before cutting a new segment it looks for a
//! segment a previous run wrote but did not acknowledge and adopts it when its
//! bytes and head verify, so a crash never overwrites or re-cuts a range
//! (section 8.1). Once a day the loop also re-probes the lock and compares the
//! local log with the copy (section 8.3).
//!
//! Every object store error becomes `audit_exports.last_error`; the pass never
//! fails the loop. A store that is unreachable is retried on the next tick.

use crate::AppState;
use crate::audit;
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::jcs;
use quoro_providers::object_store::{ObjectStore, ObjectStoreError};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::{
    DAILY_INTERVAL, EXPORT_INTERVAL, ExportRow, SEGMENT_MAX_AGE, SEGMENT_MAX_RECORDS,
    compliance_lock, format_rfc3339, head_key, key_root, load, parse_head_key, parse_segment_key,
    read_object, segment_key, set_last_error, store_for_export,
};

/// Starts the exporter task. A failed pass is logged and retried on the next
/// tick.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        let mut last_verify: Option<OffsetDateTime> = None;
        let mut ticker = tokio::time::interval(EXPORT_INTERVAL);
        loop {
            ticker.tick().await;
            let now = OffsetDateTime::now_utc();
            if let Err(error) = cycle(&state, now, &mut last_verify).await {
                tracing::warn!(error = ?error, "audit export pass failed");
            }
        }
    });
}

/// One pass: export what is due, then the daily probe and copy comparison.
async fn cycle(
    state: &AppState,
    now: OffsetDateTime,
    last_verify: &mut Option<OffsetDateTime>,
) -> Result<(), ApiError> {
    let Some(row) = load(&state.store).await? else {
        return Ok(());
    };
    let store = match store_for_export(state, &row).await {
        Ok(store) => store,
        Err(error) => {
            let detail = error
                .detail
                .clone()
                .unwrap_or_else(|| error.code.as_str().to_string());
            set_last_error(state, &row.id, &format!("provider unavailable: {detail}")).await?;
            return Ok(());
        }
    };
    step(state, &store, now).await?;

    let Some(row) = load(&state.store).await? else {
        return Ok(());
    };
    if probe_due(&row, now) {
        super::probe::run_with(state, &row, &store, now).await?;
    }
    if last_verify.is_none_or(|at| now - at >= day()) {
        super::verify::verify_copy(state, &store, now).await?;
        *last_verify = Some(now);
    }
    Ok(())
}

/// [`DAILY_INTERVAL`] as a `time` duration.
fn day() -> time::Duration {
    time::Duration::seconds(DAILY_INTERVAL.as_secs() as i64)
}

/// Whether the daily probe is due, from the stored `probed_at`.
fn probe_due(row: &ExportRow, now: OffsetDateTime) -> bool {
    match row
        .probed_at
        .as_deref()
        .and_then(|at| OffsetDateTime::parse(at, &Rfc3339).ok())
    {
        Some(at) => now - at >= day(),
        None => true,
    }
}

/// Exports the next segment when one is due. `now` is injected so the
/// five-minute rule is testable.
///
/// # Errors
///
/// A database error. Store errors are recorded in `last_error` and end the
/// pass without failing it.
pub async fn step<S: ObjectStore>(
    state: &AppState,
    store: &S,
    now: OffsetDateTime,
) -> Result<(), ApiError> {
    let Some(mut row) = load(&state.store).await? else {
        return Ok(());
    };
    let root = key_root(state, &row);

    // Nothing unexported means there is nothing to adopt either: a segment a
    // previous run wrote but did not acknowledge always leaves its records
    // pending locally. Returning here keeps the 30-second tick from listing
    // the copy when there is no work.
    let mut pending = pending_rows(state, row.last_exported_seq).await?;
    if pending.is_empty() {
        return Ok(());
    }

    match recover(state, store, &row, &root, now).await? {
        Recovery::Stop => return Ok(()),
        Recovery::None => {}
        Recovery::Adopted { last, hash } => {
            update_progress(state, &row.id, last, &hash, now).await?;
            row.last_exported_seq = last;
            row.last_exported_hash = Some(hash);
        }
    }

    // The recovered progress may cover some of the records selected above.
    pending = pending_rows(state, row.last_exported_seq).await?;
    if pending.is_empty() {
        return Ok(());
    }
    let due_by_count = pending.len() as i64 >= SEGMENT_MAX_RECORDS;
    let due_by_age = oldest_expired(&pending[0], now);
    if !due_by_count && !due_by_age {
        return Ok(());
    }
    let take = pending.len().min(SEGMENT_MAX_RECORDS as usize);
    let rows = &pending[..take];
    let first = row.last_exported_seq + 1;
    let last = first + take as i64 - 1;
    if !seqs_are_contiguous(rows, first) {
        set_last_error(state, &row.id, "the local audit log has a sequence gap").await?;
        return Ok(());
    }

    let body = match segment_body(rows) {
        Ok(body) => body,
        Err(error) => {
            let detail = error
                .detail
                .clone()
                .unwrap_or_else(|| error.code.as_str().to_string());
            set_last_error(
                state,
                &row.id,
                &format!("segment serialization failed: {detail}"),
            )
            .await?;
            return Ok(());
        }
    };
    let head_hash = match last_hash(rows) {
        Ok(hash) => hash,
        Err(error) => {
            let detail = error
                .detail
                .clone()
                .unwrap_or_else(|| error.code.as_str().to_string());
            set_last_error(
                state,
                &row.id,
                &format!("segment serialization failed: {detail}"),
            )
            .await?;
            return Ok(());
        }
    };
    let lock = (row.lock_strength == quoro_providers::object_store::LockStrength::Compliance)
        .then(|| compliance_lock(now));
    let segment = segment_key(&root, first, last);
    if let Err(error) = store.put_new(&segment, body, lock.as_ref()).await {
        match error {
            ObjectStoreError::AlreadyExists => {
                // A previous run wrote this range but did not acknowledge it.
                if let Recovery::Adopted {
                    last: adopted,
                    hash,
                } = recover(state, store, &row, &root, now).await?
                    && adopted >= last
                {
                    update_progress(state, &row.id, adopted, &hash, now).await?;
                    return Ok(());
                }
            }
            other => {
                set_last_error(state, &row.id, &format!("segment put failed: {other}")).await?;
                return Ok(());
            }
        }
        set_last_error(
            state,
            &row.id,
            &format!("segment {segment} exists and does not verify"),
        )
        .await?;
        return Ok(());
    }

    let head = head_key(&root, last);
    let body = head_body(last, &head_hash, now);
    if let Err(error) = store.put_new(&head, body, lock.as_ref()).await {
        match error {
            ObjectStoreError::AlreadyExists => {
                if let Recovery::Adopted {
                    last: adopted,
                    hash,
                } = recover(state, store, &row, &root, now).await?
                    && adopted >= last
                {
                    update_progress(state, &row.id, adopted, &hash, now).await?;
                    return Ok(());
                }
            }
            other => {
                set_last_error(state, &row.id, &format!("head put failed: {other}")).await?;
                return Ok(());
            }
        }
        set_last_error(
            state,
            &row.id,
            &format!("head {head} exists and does not verify"),
        )
        .await?;
        return Ok(());
    }

    update_progress(state, &row.id, last, &head_hash, now).await?;
    Ok(())
}

/// What the crash-recovery pass found (section 8.1).
enum Recovery {
    /// Nothing was left behind.
    None,
    /// A segment and head exist and verify; the bookkeeping now names them.
    Adopted { last: i64, hash: String },
    /// Something exists that does not verify; the export stopped.
    Stop,
}

/// Adopts a segment and head a previous run wrote without acknowledging it.
///
/// The segment's bytes must equal the local canonical records exactly, and the
/// head must name the last local hash; otherwise the export stops and never
/// overwrites or re-cuts the range.
async fn recover<S: ObjectStore>(
    state: &AppState,
    store: &S,
    row: &ExportRow,
    root: &str,
    now: OffsetDateTime,
) -> Result<Recovery, ApiError> {
    let next = row.last_exported_seq + 1;
    // Only the segment that starts at `next` can overlap the unacknowledged
    // range; listing its 20-digit prefix is the store's start-after (section
    // 8.1). Heads are listed by their own prefix.
    let segments: Vec<(i64, i64, String)> = match store.list(&format!("{root}{next:020}")).await {
        Ok(objects) => objects
            .iter()
            .filter_map(|object| {
                parse_segment_key(root, &object.key)
                    .map(|(first, last)| (first, last, object.key.clone()))
            })
            .collect(),
        Err(error) => {
            set_last_error(state, &row.id, &format!("list failed: {error}")).await?;
            return Ok(Recovery::Stop);
        }
    };
    let heads: Vec<i64> = match store
        .list(&format!("{root}{}", head_scan_prefix(next)))
        .await
    {
        Ok(objects) => objects
            .iter()
            .filter_map(|object| parse_head_key(root, &object.key))
            .collect(),
        Err(error) => {
            set_last_error(state, &row.id, &format!("list failed: {error}")).await?;
            return Ok(Recovery::Stop);
        }
    };

    if let Some((first, last, key)) = segments
        .iter()
        .find(|(first, _, _)| *first == next)
        .cloned()
    {
        let bytes = match read_object(store, &key, row.lock_strength).await {
            Ok(bytes) => bytes,
            Err(error) => {
                set_last_error(state, &row.id, &format!("segment read failed: {error}")).await?;
                return Ok(Recovery::Stop);
            }
        };
        let local = local_segment_bytes(state, first, last).await?;
        if local.as_deref() != Some(bytes.as_slice()) {
            set_last_error(
                state,
                &row.id,
                &format!("segment {key} does not match the local log"),
            )
            .await?;
            return Ok(Recovery::Stop);
        }
        let hash = match local_last_hash(state, last).await? {
            Some(hash) => hash,
            None => {
                set_last_error(
                    state,
                    &row.id,
                    &format!("segment {key} names no local record"),
                )
                .await?;
                return Ok(Recovery::Stop);
            }
        };
        let head = head_key(root, last);
        if heads.contains(&last) {
            let body = match read_object(store, &head, row.lock_strength).await {
                Ok(body) => body,
                Err(error) => {
                    set_last_error(state, &row.id, &format!("head read failed: {error}")).await?;
                    return Ok(Recovery::Stop);
                }
            };
            if !head_matches(&body, last, &hash) {
                set_last_error(
                    state,
                    &row.id,
                    &format!("head {head} does not match the local record"),
                )
                .await?;
                return Ok(Recovery::Stop);
            }
        } else {
            // The segment made it but its head did not; writing the missing
            // head is not an overwrite.
            let lock = (row.lock_strength
                == quoro_providers::object_store::LockStrength::Compliance)
                .then(|| compliance_lock(now));
            if let Err(error) = store
                .put_new(&head, head_body(last, &hash, now), lock.as_ref())
                .await
            {
                set_last_error(state, &row.id, &format!("head put failed: {error}")).await?;
                return Ok(Recovery::Stop);
            }
        }
        return Ok(Recovery::Adopted { last, hash });
    }

    // A head beyond the bookkeeping without its segment is a range the local
    // log cannot re-cut without overlapping.
    if heads.iter().any(|&head| head > row.last_exported_seq) {
        set_last_error(state, &row.id, "a head object exists without its segment").await?;
        return Ok(Recovery::Stop);
    }
    Ok(Recovery::None)
}

/// Writes `last_exported_seq`, `last_exported_hash` and clears `last_error`,
/// with one change-log entry.
async fn update_progress(
    state: &AppState,
    export_id: &str,
    last_seq: i64,
    hash: &str,
    now: OffsetDateTime,
) -> Result<(), ApiError> {
    let at = format_rfc3339(now);
    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "UPDATE audit_exports SET last_exported_seq = ?, last_exported_hash = ?, \
         last_error = NULL, updated_at = ? WHERE id = ?",
    )
    .bind(last_seq)
    .bind(hash)
    .bind(&at)
    .bind(export_id)
    .execute(&mut *w.tx)
    .await?;
    changelog::append(
        &mut w.tx,
        &Actor::system("audit_export"),
        "audit_export",
        export_id,
        Op::Update,
        json!({
            "id": export_id,
            "last_exported_seq": last_seq,
            "last_exported_hash": hash,
            "last_error": null,
            "updated_at": at,
        }),
    )
    .await?;
    w.commit().await?;
    Ok(())
}

/// The unexported records, at most one segment's worth.
async fn pending_rows(state: &AppState, after: i64) -> Result<Vec<SqliteRow>, ApiError> {
    Ok(sqlx::query(&format!(
        "SELECT {} FROM audit_log WHERE seq > ? ORDER BY seq LIMIT ?",
        audit::COLUMNS
    ))
    .bind(after)
    .bind(SEGMENT_MAX_RECORDS)
    .fetch_all(state.store.pool())
    .await?)
}

/// Whether the oldest unexported record has waited [`SEGMENT_MAX_AGE`].
fn oldest_expired(row: &SqliteRow, now: OffsetDateTime) -> bool {
    let Ok(time) = row.try_get::<String, _>("time") else {
        return true;
    };
    match OffsetDateTime::parse(&time, &Rfc3339) {
        Ok(at) => at + time::Duration::seconds(SEGMENT_MAX_AGE.as_secs() as i64) <= now,
        Err(_) => true,
    }
}

/// The local canonical JSON Lines of `first..=last`, or `None` when the range
/// does not hold exactly those records.
async fn local_segment_bytes(
    state: &AppState,
    first: i64,
    last: i64,
) -> Result<Option<Vec<u8>>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {} FROM audit_log WHERE seq BETWEEN ? AND ? ORDER BY seq",
        audit::COLUMNS
    ))
    .bind(first)
    .bind(last)
    .fetch_all(state.store.pool())
    .await?;
    if rows.len() as i64 != last - first + 1 || !seqs_are_contiguous(&rows, first) {
        return Ok(None);
    }
    Ok(Some(segment_body(&rows)?))
}

/// The `hash` of the local record at `seq`.
async fn local_last_hash(state: &AppState, seq: i64) -> Result<Option<String>, ApiError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
        .bind(seq)
        .fetch_optional(state.store.pool())
        .await?;
    Ok(row.map(|(hash,)| hash))
}

/// Whether the rows are exactly `first`, `first + 1`, ...
fn seqs_are_contiguous(rows: &[SqliteRow], first: i64) -> bool {
    rows.iter().enumerate().all(|(index, row)| {
        row.try_get::<i64, _>("seq")
            .is_ok_and(|seq| seq == first + index as i64)
    })
}

/// Canonical JSON Lines of the rows: one RFC 8785 object per line, in `seq`
/// order (section 8.1).
fn segment_body(rows: &[SqliteRow]) -> Result<Vec<u8>, ApiError> {
    let mut out = Vec::new();
    for row in rows {
        let value = audit::record_value_from_row(row)?;
        let line = jcs::canonicalize(&value)
            .map_err(|error| ApiError::internal(&format!("canonical record: {error:?}")))?;
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

/// The `hash` of the last row.
fn last_hash(rows: &[SqliteRow]) -> Result<String, ApiError> {
    let row = rows
        .last()
        .ok_or_else(|| ApiError::internal(&"an empty segment"))?;
    let value = audit::record_value_from_row(row)?;
    value
        .get("hash")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| ApiError::internal(&"a record without a hash"))
}

/// `{"last_seq", "head_hash", "time"}` (section 8.1).
fn head_body(last_seq: i64, head_hash: &str, now: OffsetDateTime) -> Vec<u8> {
    json!({
        "last_seq": last_seq,
        "head_hash": head_hash,
        "time": format_rfc3339(now),
    })
    .to_string()
    .into_bytes()
}

/// Whether a stored head object names `last_seq` and `hash`.
fn head_matches(body: &[u8], last_seq: i64, hash: &str) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    value.get("last_seq").and_then(Value::as_i64) == Some(last_seq)
        && value.get("head_hash").and_then(Value::as_str) == Some(hash)
        && value.get("time").and_then(Value::as_str).is_some()
}

/// The longest common prefix of the head names a segment starting at `first`
/// can produce.
///
/// A segment that starts at `first` ends at most at
/// `first + SEGMENT_MAX_RECORDS - 1`, so a head left behind by it lies under
/// this prefix. Recovery lists only that prefix instead of every head the
/// copy has ever written (section 8.1).
fn head_scan_prefix(first: i64) -> String {
    let start = format!("head-{first:020}");
    let end = format!("head-{:020}", first + SEGMENT_MAX_RECORDS - 1);
    start
        .chars()
        .zip(end.chars())
        .take_while(|(start, end)| start == end)
        .map(|(start, _)| start)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_head_scan_prefix_covers_the_segment_range() {
        assert_eq!(head_scan_prefix(1), "head-0000000000000000");
        assert_eq!(head_scan_prefix(5_000), "head-00000000000000005");
        // A range that crosses a decade (995 to 1,994) still shares the
        // leading digits.
        assert_eq!(head_scan_prefix(995), "head-0000000000000000");
        for first in [1, 995, 5_000, 20_000, 123_456] {
            let prefix = head_scan_prefix(first);
            for last in [first, first + SEGMENT_MAX_RECORDS - 1] {
                let key = format!("head-{last:020}");
                assert!(
                    key.starts_with(&prefix),
                    "{key} does not start with {prefix}"
                );
            }
        }
    }
}

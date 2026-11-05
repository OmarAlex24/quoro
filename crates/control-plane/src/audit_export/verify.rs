// SPDX-License-Identifier: AGPL-3.0-only
//! Verification against the off-site copy (`audit-log-format.md` section
//! 8.3).
//!
//! The local chain catches an edit in the middle but not a truncation of the
//! tail or a full rewrite (section 7). The copy's head objects are the only
//! record outside the server: the newest head names the last exported `seq`,
//! and every head names the hash of its record. A local log that ends before
//! the newest head is `tail_truncated`; a local hash that differs from a
//! head's is `copy_mismatch`.
//!
//! The work is bounded by the head objects plus the one segment that holds
//! the first missing record. On a `compliance` bucket every object is read by
//! its first version, so a newer version or a delete marker cannot hide the
//! original.

use crate::AppState;
use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::store::GENESIS_HASH;
use quoro_providers::object_store::ObjectStore;
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use time::OffsetDateTime;

use super::{
    ExportRow, format_rfc3339, head_key, key_root, load, parse_head_key, parse_segment_key,
    read_object, set_last_error, short_error, store_for_export,
};

/// Why the local log and the copy disagree (section 8.3).
pub const TAIL_TRUNCATED: &str = "tail_truncated";
pub const COPY_MISMATCH: &str = "copy_mismatch";

/// The first record that differs, and the two hashes that show it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyBreak {
    pub reason: &'static str,
    pub broken_at_seq: i64,
    /// The copy's hash (or the chain link the copy implies).
    pub expected_hash: String,
    /// The local hash, or 64 zeros for a record the local log no longer has.
    pub found_hash: String,
}

impl CopyBreak {
    /// `tail_truncated at 42`, the verification run's `result_detail`.
    #[must_use]
    pub fn detail(&self) -> String {
        format!("{} at {}", self.reason, self.broken_at_seq)
    }
}

/// What one comparison found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyResult {
    /// The local log agrees with every head object.
    Match,
    /// A head proves the local log wrong.
    Broken(CopyBreak),
    /// The provider could not be read. This is not evidence of tampering, so
    /// it sets `last_error` and raises no event (section 8.3).
    Unavailable(String),
}

/// Compares the local log with the copy without writing anything.
///
/// # Errors
///
/// A database error while reading the local hashes.
pub async fn compare<S: ObjectStore>(
    state: &AppState,
    store: &S,
    row: &ExportRow,
) -> Result<CopyResult, ApiError> {
    let root = key_root(state, row);
    let objects = match store.list(&root).await {
        Ok(objects) => objects,
        Err(error) => return Ok(CopyResult::Unavailable(format!("list failed: {error}"))),
    };
    let mut heads: Vec<i64> = objects
        .iter()
        .filter_map(|object| parse_head_key(&root, &object.key))
        .collect();
    heads.sort_unstable();
    let local_last = local_last_seq(state).await?;

    if heads.is_empty() {
        if row.last_exported_seq == 0 {
            return Ok(CopyResult::Match);
        }
        return missing_head(store, row, &root).await;
    }

    let newest = *heads.last().unwrap_or(&0);
    if local_last < newest {
        let missing = local_last + 1;
        // Section 8.3: "The verifier reads the newest head object. If the
        // local log ends before its `last_seq`, the chain is broken with
        // reason `tail_truncated`." The newest head alone proves the
        // truncation, so a segment that is gone or does not hold the record
        // only changes where `expected_hash` comes from: the record's hash in
        // the segment when it is readable, otherwise the newest head's
        // `head_hash`. Only a store read error is `Unavailable`.
        let segment = objects.iter().filter_map(|object| {
            parse_segment_key(&root, &object.key)
                .map(|(first, last)| (first, last, object.key.clone()))
        });
        let mut expected = None;
        if let Some((_, _, key)) = segment
            .into_iter()
            .find(|(first, last, _)| *first <= missing && missing <= *last)
        {
            match read_object(store, &key, row.lock_strength).await {
                Ok(bytes) => expected = hash_in_segment(&bytes, missing).ok().flatten(),
                // The segment itself is gone or holds no first version; the
                // head still proves the truncation.
                Err(quoro_providers::object_store::ObjectStoreError::NotFound) => {}
                Err(error) => {
                    return Ok(CopyResult::Unavailable(format!(
                        "segment read failed: {error}"
                    )));
                }
            }
        }
        let expected = match expected {
            Some(expected) => expected,
            None => match newest_head_hash(store, &root, newest, row.lock_strength).await {
                Ok(hash) => hash,
                Err(error) => return Ok(CopyResult::Unavailable(error)),
            },
        };
        return Ok(CopyResult::Broken(CopyBreak {
            reason: TAIL_TRUNCATED,
            broken_at_seq: missing,
            expected_hash: expected,
            found_hash: GENESIS_HASH.to_string(),
        }));
    }

    // The head of the last acknowledged export is gone even though an older
    // head still lists. Section 8.3: that is evidence only when the object is
    // gone (404), not when the provider is unreachable.
    if row.last_exported_seq > 0 && !heads.contains(&row.last_exported_seq) {
        return missing_head(store, row, &root).await;
    }

    // Every head is at a seq the local log still has; compare each record's
    // hash. Oldest first, so the earliest divergence is the one reported.
    for seq in heads {
        let key = head_key(&root, seq);
        let bytes = match read_object(store, &key, row.lock_strength).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Ok(CopyResult::Unavailable(format!(
                    "head read failed: {error}"
                )));
            }
        };
        let head = match parse_head(&bytes) {
            Ok(head) => head,
            Err(error) => {
                return Ok(CopyResult::Unavailable(format!(
                    "head {key} is invalid: {error}"
                )));
            }
        };
        if head.last_seq != seq {
            return Ok(CopyResult::Unavailable(format!(
                "head {key} names seq {}",
                head.last_seq
            )));
        }
        let local: Option<(String,)> = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
            .bind(seq)
            .fetch_optional(state.store.pool())
            .await?;
        let Some((local_hash,)) = local else {
            // The head names a seq the local log no longer has, and the log
            // goes past it: a hole is evidence of a rewrite, not an outage.
            return Ok(CopyResult::Broken(CopyBreak {
                reason: COPY_MISMATCH,
                broken_at_seq: seq,
                expected_hash: head.head_hash,
                found_hash: GENESIS_HASH.to_string(),
            }));
        };
        if local_hash != head.head_hash {
            return Ok(CopyResult::Broken(CopyBreak {
                reason: COPY_MISMATCH,
                broken_at_seq: seq,
                expected_hash: head.head_hash,
                found_hash: local_hash,
            }));
        }
    }
    Ok(CopyResult::Match)
}

/// The stored head object.
struct Head {
    last_seq: i64,
    head_hash: String,
}

fn parse_head(bytes: &[u8]) -> Result<Head, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let last_seq = value
        .get("last_seq")
        .and_then(Value::as_i64)
        .ok_or_else(|| "no last_seq".to_string())?;
    let head_hash = value
        .get("head_hash")
        .and_then(Value::as_str)
        .filter(|hash| hash.len() == 64)
        .ok_or_else(|| "no head_hash".to_string())?;
    Ok(Head {
        last_seq,
        head_hash: head_hash.to_string(),
    })
}

/// The `head_hash` of the newest head object, used as `expected_hash` when
/// the segment that holds the first missing record is not usable.
async fn newest_head_hash<S: ObjectStore>(
    store: &S,
    root: &str,
    last_seq: i64,
    strength: quoro_providers::object_store::LockStrength,
) -> Result<String, String> {
    let key = head_key(root, last_seq);
    let bytes = read_object(store, &key, strength)
        .await
        .map_err(|error| format!("head read failed: {error}"))?;
    Ok(parse_head(&bytes)
        .map_err(|error| format!("head {key} is invalid: {error}"))?
        .head_hash)
}

/// No head was listed. When `last_exported_seq` says one should exist, the
/// head's absence is only evidence when the object is gone (404); an
/// unreachable provider is not (section 8.3).
async fn missing_head<S: ObjectStore>(
    store: &S,
    row: &ExportRow,
    root: &str,
) -> Result<CopyResult, ApiError> {
    let key = head_key(root, row.last_exported_seq);
    match store.head(&key).await {
        Err(quoro_providers::object_store::ObjectStoreError::NotFound) => {
            Ok(CopyResult::Broken(CopyBreak {
                // The copy and the local log disagree; no other contract
                // reason describes a head the copy no longer has.
                reason: COPY_MISMATCH,
                broken_at_seq: row.last_exported_seq,
                expected_hash: row
                    .last_exported_hash
                    .clone()
                    .unwrap_or_else(|| GENESIS_HASH.to_string()),
                found_hash: GENESIS_HASH.to_string(),
            }))
        }
        Err(error) => Ok(CopyResult::Unavailable(format!(
            "head read failed: {error}"
        ))),
        Ok(_) => Ok(CopyResult::Unavailable(format!(
            "head {key} exists but was not listed"
        ))),
    }
}

/// The `hash` of `seq` in one segment's JSON Lines, or `None` when the
/// segment does not hold it.
fn hash_in_segment(bytes: &[u8], seq: i64) -> Result<Option<String>, String> {
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let value: Value = serde_json::from_slice(line).map_err(|error| error.to_string())?;
        if value.get("seq").and_then(Value::as_i64) == Some(seq) {
            return Ok(value
                .get("hash")
                .and_then(Value::as_str)
                .map(str::to_string));
        }
    }
    Ok(None)
}

/// The last local audit `seq`, or 0 for an empty log.
async fn local_last_seq(state: &AppState) -> Result<i64, ApiError> {
    let (last,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM audit_log")
        .fetch_one(state.store.pool())
        .await?;
    Ok(last)
}

/// The daily comparison. A break is recorded as an error verification run
/// and emits `audit.chain_broken` in the same transaction; an unreachable
/// provider only sets `last_error`. Returns whether the copy agrees.
///
/// # Errors
///
/// A database error.
pub async fn verify_copy<S: ObjectStore>(
    state: &AppState,
    store: &S,
    now: OffsetDateTime,
) -> Result<bool, ApiError> {
    let Some(row) = load(&state.store).await? else {
        return Ok(true);
    };
    match compare(state, store, &row).await? {
        CopyResult::Match => Ok(true),
        CopyResult::Broken(copy_break) => {
            record_run(state, &row, now, Some(&copy_break), None).await?;
            Ok(false)
        }
        CopyResult::Unavailable(error) => {
            record_run(state, &row, now, None, Some(&error)).await?;
            Ok(true)
        }
    }
}

/// [`compare`] with the export's own provider, for `verifyAudit`. A provider
/// that cannot be read sets `last_error` and returns no break; a break is
/// returned so the caller records it as any other verification failure.
///
/// # Errors
///
/// A database error.
pub async fn compare_via_provider(state: &AppState) -> Result<Option<CopyBreak>, ApiError> {
    let Some(row) = load(&state.store).await? else {
        return Ok(None);
    };
    let store = match store_for_export(state, &row).await {
        Ok(store) => store,
        Err(error) => {
            let detail = error
                .detail
                .clone()
                .unwrap_or_else(|| error.code.as_str().to_string());
            set_last_error(state, &row.id, &format!("provider unavailable: {detail}")).await?;
            return Ok(None);
        }
    };
    match compare(state, &store, &row).await? {
        CopyResult::Match => Ok(None),
        CopyResult::Broken(copy_break) => {
            set_last_error(state, &row.id, &copy_break.detail()).await?;
            Ok(Some(copy_break))
        }
        CopyResult::Unavailable(error) => {
            set_last_error(state, &row.id, &short_error(&error)).await?;
            Ok(None)
        }
    }
}

/// Writes a verification run: an error record, the event when the copy proves
/// the local log wrong, and `last_error`.
async fn record_run(
    state: &AppState,
    row: &ExportRow,
    now: OffsetDateTime,
    copy_break: Option<&CopyBreak>,
    unavailable: Option<&str>,
) -> Result<(), ApiError> {
    let detail = copy_break
        .map(CopyBreak::detail)
        .or_else(|| unavailable.map(short_error));
    let at = format_rfc3339(now);
    let mut w = state.store.begin_write().await?;
    sqlx::query("UPDATE audit_exports SET last_error = ?, updated_at = ? WHERE id = ?")
        .bind(&detail)
        .bind(&at)
        .bind(&row.id)
        .execute(&mut *w.tx)
        .await?;
    changelog::append(
        &mut w.tx,
        &Actor::system("audit_export"),
        "audit_export",
        &row.id,
        Op::Update,
        json!({ "id": row.id, "last_error": detail, "updated_at": at }),
    )
    .await?;
    let record = Record {
        actor: Actor::system("audit_export"),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: "POST /api/v1/audit/verify".to_string(),
        scope: None,
        target_type: None,
        target_id: None,
        project_id: None,
        params: json!({}),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: new_id(Prefix::Request),
        result: Outcome::Error,
        result_detail: detail.clone(),
        change_seq: None,
    };
    audit::append(&mut w.tx, &record).await?;
    if let Some(copy_break) = copy_break {
        let actor = Actor::system("audit_export");
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "audit.chain_broken",
                source: Source::ControlPlane,
                subject_type: "cluster",
                subject_id: &state.cluster_id,
                project_id: None,
                request_id: None,
                actor: Some(&actor),
                data: json!({
                    "broken_at_seq": copy_break.broken_at_seq,
                    "expected_hash": copy_break.expected_hash,
                    "found_hash": copy_break.found_hash,
                    "reason": copy_break.reason,
                }),
            },
        )
        .await?;
    }
    w.commit().await?;
    Ok(())
}

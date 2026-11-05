//! Audit reads and chain verification (`audit-log-format.md` sections 2.4
//! and 6).
//!
//! Reads go through [`CallCtx::authorize_read`], so an agent's list page is
//! filtered by its selectors (permission-model.md section 5.1) and its read
//! is audited; human reads are not. A verification run is always audited, for
//! humans too (section 6), and its record and the `audit.chain_broken` event
//! commit in one transaction.

use crate::AppState;
use crate::api::audit::{
    AuditList, AuditRecord, AuditVerificationRequest, AuditVerificationResult, ListAuditQuery,
};
use crate::audit::{self, Verification};
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::request::{CallCtx, Resource};
use crate::store::GENESIS_HASH;
use quoro_types::Scope;
use serde_json::{Value, json};
use sqlx::sqlite::SqliteRow;

/// The stored record as the API's [`AuditRecord`].
fn record_from_row(row: &SqliteRow) -> Result<AuditRecord, ApiError> {
    let value = audit::record_value_from_row(row)?;
    serde_json::from_value(value).map_err(|e| ApiError::internal(&format!("audit record: {e}")))
}

/// Describes an audit record to [`crate::request::ReadAccess::filter`]
/// (permission-model.md section 5.1): its target, the app or node the target
/// id names, and its project.
fn resource_of(record: &AuditRecord) -> Resource<'_> {
    let target_type = record.target_type.as_deref();
    let target_id = record.target_id.as_deref();
    Resource {
        kind: target_type.unwrap_or(""),
        id: target_id.unwrap_or(""),
        app_id: target_id.filter(|_| target_type == Some("app")),
        project_id: record.project_id.as_deref(),
        node_id: target_id.filter(|_| target_type == Some("node")),
    }
}

/// `limit`, validated against the shared 1-500 range.
fn limit_of(query: &ListAuditQuery) -> Result<i64, ApiError> {
    match query.limit {
        None => Ok(crate::api::page::DEFAULT_LIMIT),
        Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
        Some(n) => Err(ApiError::validation(format!(
            "limit {n} is outside 1-{}",
            crate::api::page::MAX_LIMIT
        ))),
    }
}

/// The `seq` the page starts after: the larger of `after_seq` and the cursor.
fn after_of(query: &ListAuditQuery) -> Result<i64, ApiError> {
    let after_seq = query.after_seq.unwrap_or(0);
    if after_seq < 0 {
        return Err(ApiError::validation("after_seq must not be negative"));
    }
    let cursor = match query.cursor.as_deref() {
        None | Some("") => 0,
        Some(cursor) => cursor.parse::<i64>().map_err(|_| {
            ApiError::validation(format!("cursor {cursor:?} is not a sequence number"))
        })?,
    };
    Ok(cursor.max(after_seq))
}

/// Lists one page of audit records, oldest first. The page cursor is computed
/// from the unfiltered rows, then the page is filtered for an agent's
/// selector, so a filtered page never ends the listing early.
///
/// # Errors
///
/// `forbidden` without `audit:read`, `validation_failed` for a bad limit,
/// cursor, filter or result value, or a database error.
pub async fn list_audit(
    state: &AppState,
    ctx: &CallCtx,
    query: &ListAuditQuery,
) -> Result<AuditList, ApiError> {
    let limit = limit_of(query)?;
    let after = after_of(query)?;
    if let Some(result) = query.result.as_deref()
        && !matches!(result, "ok" | "refused" | "error")
    {
        return Err(ApiError::validation(format!(
            "result {result:?} is not ok, refused or error"
        )));
    }
    let params = json!({
        "actor_id": query.actor_id,
        "target_id": query.target_id,
        "result": query.result,
        "incident_id": query.incident_id,
        "after_seq": query.after_seq,
        "limit": query.limit,
        "cursor": query.cursor,
    });
    let access = ctx
        .authorize_read(state, Scope::AuditRead, None, &params)
        .await?;
    let result: Result<_, ApiError> = async {
        let rows = sqlx::query(&format!(
            "SELECT {} FROM audit_log WHERE seq > ? \
             AND (? IS NULL OR actor_id = ?) \
             AND (? IS NULL OR target_id = ?) \
             AND (? IS NULL OR result = ?) \
             AND (? IS NULL OR incident_id = ?) \
             ORDER BY seq LIMIT ?",
            audit::COLUMNS
        ))
        .bind(after)
        .bind(&query.actor_id)
        .bind(&query.actor_id)
        .bind(&query.target_id)
        .bind(&query.target_id)
        .bind(&query.result)
        .bind(&query.result)
        .bind(&query.incident_id)
        .bind(&query.incident_id)
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
        let records = rows
            .iter()
            .map(record_from_row)
            .collect::<Result<Vec<_>, ApiError>>()?;
        let (items, next_cursor) =
            crate::api::page::finish(records, limit, |record| record.seq.to_string());
        let visible = access.filter(state, items, resource_of).await?;
        Ok(AuditList {
            items: visible,
            next_cursor,
        })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One record by `seq`. An agent whose selector does not cover it gets
/// `not_found`, the same answer as an unknown seq (section 2.4).
///
/// # Errors
///
/// `forbidden` without `audit:read`, `not_found` for an unknown or filtered
/// `seq`, or a database error.
pub async fn get_audit_record(
    state: &AppState,
    ctx: &CallCtx,
    seq: i64,
) -> Result<AuditRecord, ApiError> {
    let params = json!({ "seq": seq });
    let access = ctx
        .authorize_read(state, Scope::AuditRead, None, &params)
        .await?;
    let result: Result<_, ApiError> = async {
        if seq < 1 {
            return Err(ApiError::not_found("audit record"));
        }
        let row = sqlx::query(&format!(
            "SELECT {} FROM audit_log WHERE seq = ?",
            audit::COLUMNS
        ))
        .bind(seq)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("audit record"))?;
        let record = record_from_row(&row)?;
        if access.sees_all() {
            return Ok(record);
        }
        let mut visible = access.filter(state, vec![record], resource_of).await?;
        visible
            .pop()
            .ok_or_else(|| ApiError::not_found("audit record"))
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Verification run's `result_detail`, in the contract's shape: the reason and
/// the seq it broke at, for example `hash_mismatch at 518`.
fn failure_detail(verification: &Verification) -> Option<String> {
    Some(format!(
        "{} at {}",
        verification.reason?, verification.broken_at_seq?
    ))
}

/// The hash record `first_seq - 1` carries, or 64 zeros for a chain that
/// starts at 1 (section 6).
async fn seed_hash(state: &AppState, first_seq: i64) -> Result<String, ApiError> {
    if first_seq == 1 {
        return Ok(GENESIS_HASH.to_string());
    }
    let row: Option<(String,)> = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
        .bind(first_seq - 1)
        .fetch_optional(state.store.pool())
        .await?;
    row.map(|(hash,)| hash).ok_or_else(|| {
        ApiError::validation(format!("from_seq {first_seq} has no record before it"))
    })
}

/// Re-walks a range of the chain (the whole log by default). The run is always
/// audited, for humans too; a failure also emits `audit.chain_broken` in the
/// same transaction (section 6).
///
/// # Errors
///
/// `forbidden` without `audit:read`, `validation_failed` for a bound that is
/// not positive, a reversed range, or a `from_seq` without a predecessor, or a
/// database error.
pub async fn verify_audit(
    state: &AppState,
    ctx: &CallCtx,
    body: Option<AuditVerificationRequest>,
) -> Result<AuditVerificationResult, ApiError> {
    let body = body.unwrap_or_default();
    let from_seq = body.from_seq.unwrap_or(1);
    if from_seq < 1 {
        return Err(ApiError::validation("from_seq must be at least 1"));
    }
    if let Some(to_seq) = body.to_seq
        && (to_seq < 1 || to_seq < from_seq)
    {
        return Err(ApiError::validation(
            "to_seq must be at least 1 and not before from_seq",
        ));
    }
    let params = json!({ "from_seq": body.from_seq, "to_seq": body.to_seq });
    // `authorize_read` audits an agent's refusal; the run itself is recorded
    // below for every identity, so `finish_read` is deliberately not used (it
    // would record an agent's run twice and could only say `ok`).
    let access = ctx
        .authorize_read(state, Scope::AuditRead, None, &params)
        .await?;
    // Section 5.1: `audit:read` is matched by any non-empty selector, so a
    // caller whose grants all carry `{}` reads nothing, the chain included.
    if access.reads_nothing() {
        let error = ApiError::forbidden(
            crate::error::Reason::SelectorMismatch,
            Some(Scope::AuditRead.as_str()),
        );
        ctx.audit_error(state, Some(Scope::AuditRead), None, None, &params, &error)
            .await?;
        return Err(error);
    }

    let seed = seed_hash(state, from_seq).await?;
    let to_seq = body.to_seq.unwrap_or(i64::MAX);
    let rows = sqlx::query(&format!(
        "SELECT {} FROM audit_log WHERE seq >= ? AND seq <= ? ORDER BY seq",
        audit::COLUMNS
    ))
    .bind(from_seq)
    .bind(to_seq)
    .fetch_all(state.store.pool())
    .await?;
    let records: Vec<Value> = rows
        .iter()
        .map(|row| Ok(audit::record_value_from_row(row)?))
        .collect::<Result<_, ApiError>>()?;

    let mut verification = audit::verify_records(records, from_seq, &seed);
    // Section 8.3: while an export exists, the same run also compares the
    // local log with the copy. A provider that cannot be read is recorded in
    // `audit_exports.last_error`; only a disagreement with the copy is a
    // verification failure here.
    if verification.ok
        && let Some(copy_break) = crate::audit_export::verify::compare_via_provider(state).await?
    {
        verification.ok = false;
        verification.broken_at_seq = Some(copy_break.broken_at_seq);
        verification.reason = Some(copy_break.reason);
        verification.expected_hash = Some(copy_break.expected_hash);
        verification.found_hash = Some(copy_break.found_hash);
    }
    let detail = failure_detail(&verification);
    let outcome = if verification.ok {
        crate::audit::Outcome::Ok
    } else {
        crate::audit::Outcome::Error
    };
    let record = ctx.record(
        Some(Scope::AuditRead),
        None,
        None,
        &params,
        outcome,
        detail,
        None,
    );
    let mut w = state.store.begin_write().await?;
    audit::append(&mut w.tx, &record).await?;
    if !verification.ok {
        let actor = ctx.identity.actor();
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "audit.chain_broken",
                source: Source::ControlPlane,
                subject_type: "cluster",
                subject_id: &state.cluster_id,
                project_id: None,
                request_id: Some(&ctx.request_id),
                actor: Some(&actor),
                data: json!({
                    "broken_at_seq": verification.broken_at_seq,
                    "expected_hash": verification.expected_hash,
                    "found_hash": verification.found_hash,
                    "reason": verification.reason,
                }),
            },
        )
        .await?;
    }
    w.commit().await?;

    Ok(AuditVerificationResult {
        ok: verification.ok,
        checked: verification.checked,
        first_seq: verification.first_seq,
        last_seq: verification.last_seq,
        head_hash: verification.head_hash,
        broken_at_seq: verification.broken_at_seq,
        reason: verification.reason.map(str::to_string),
    })
}

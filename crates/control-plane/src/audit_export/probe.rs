// SPDX-License-Identifier: AGPL-3.0-only
//! The lock-strength probe of the off-site copy (`audit-log-format.md`
//! section 8.2).
//!
//! The probe writes a small object under `<root>probe/`, tries to overwrite
//! it and to delete it, and reads it back. [`quoro_providers::object_store::probe_lock`]
//! turns those steps into a [`LockStrength`] that is never stronger than what
//! the bucket showed. This module stores the result on the export, appends the
//! change-log entry (the strength is in it) and writes the system
//! `audit_export.probe` record (section 3.5).

use crate::AppState;
use crate::api::audit_exports::AuditExport;
use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::error::{ApiError, Target};
use crate::request::CallCtx;
use quoro_providers::object_store::{
    ComplianceLock, LockProbe, LockStrength as StoreLockStrength, ObjectStore, probe_lock,
};
use quoro_types::{Prefix, Scope, new_id};
use serde_json::{Value, json};
use time::OffsetDateTime;

use super::{ExportRow, format_rfc3339, key_root, load_by_id, store_for_export};

/// The operation string of the system record.
pub const PROBE_OPERATION: &str = "audit_export.probe";

/// The actor id of the subsystem, as section 3.5 writes it.
pub const ACTOR: &str = "audit_export";

/// Runs the probe for `export_id` and stores what it observed.
///
/// # Errors
///
/// `not_found` for an unknown id, or a database error.
pub async fn run(state: &AppState, export_id: &str) -> Result<AuditExport, ApiError> {
    let row = load_by_id(&state.store, export_id)
        .await?
        .ok_or_else(|| ApiError::not_found("audit export"))?;
    let now = OffsetDateTime::now_utc();
    match store_for_export(state, &row).await {
        Ok(store) => run_with(state, &row, &store, now).await,
        Err(error) => {
            let detail = error
                .detail
                .clone()
                .unwrap_or_else(|| error.code.as_str().to_string());
            let probe = LockProbe {
                strength: StoreLockStrength::None,
                detail: format!("probe unavailable: {detail}"),
            };
            finish(state, &row, &probe, now, None).await?;
            reload(state, export_id).await
        }
    }
}

/// [`run`] for a `probeAuditExport` call: the human call's own audit record
/// commits with the probe's update and change-log entry.
///
/// # Errors
///
/// `not_found` for an unknown id, or a database error.
pub async fn run_for_call(
    state: &AppState,
    ctx: &CallCtx,
    export_id: &str,
    params: &Value,
) -> Result<AuditExport, ApiError> {
    let row = load_by_id(&state.store, export_id)
        .await?
        .ok_or_else(|| ApiError::not_found("audit export"))?;
    let now = OffsetDateTime::now_utc();
    match store_for_export(state, &row).await {
        Ok(store) => run_with_call(state, &row, &store, now, ctx, params).await,
        Err(error) => {
            let detail = error
                .detail
                .clone()
                .unwrap_or_else(|| error.code.as_str().to_string());
            let probe = LockProbe {
                strength: StoreLockStrength::None,
                detail: format!("probe unavailable: {detail}"),
            };
            finish(state, &row, &probe, now, Some((ctx, params))).await?;
            reload(state, export_id).await
        }
    }
}

/// The probe against an injected store, for the create path and the daily
/// run.
///
/// # Errors
///
/// A database error.
pub async fn run_with<S: ObjectStore>(
    state: &AppState,
    row: &ExportRow,
    store: &S,
    now: OffsetDateTime,
) -> Result<AuditExport, ApiError> {
    run_with_call_inner(state, row, store, now, None).await
}

/// [`run_with`] for a call, so the call record and the system record commit
/// together.
async fn run_with_call<S: ObjectStore>(
    state: &AppState,
    row: &ExportRow,
    store: &S,
    now: OffsetDateTime,
    ctx: &CallCtx,
    params: &Value,
) -> Result<AuditExport, ApiError> {
    run_with_call_inner(state, row, store, now, Some((ctx, params))).await
}

async fn run_with_call_inner<S: ObjectStore>(
    state: &AppState,
    row: &ExportRow,
    store: &S,
    now: OffsetDateTime,
    call: Option<(&CallCtx, &Value)>,
) -> Result<AuditExport, ApiError> {
    let root = key_root(state, row);
    let probe_prefix = format!("{root}probe/");
    // Section 8.2: a one-day retention on a compliance bucket.
    let lock = ComplianceLock {
        retain_until: format_rfc3339(now + time::Duration::days(1)),
    };
    // A demoted primary writes no object, and a probe object stays locked for
    // a day (`node-agent-protocol.md` section 7.4): checked right before the
    // probe's put, as the demotion may land during an export pass.
    if state.store.stops_for_demotion("the audit export probe") {
        return Err(ApiError::new(
            crate::error::Code::NotPrimary,
            crate::store::DEMOTED_REFUSAL,
        ));
    }
    let probe = probe_lock(
        store,
        &probe_prefix,
        &format_rfc3339(now),
        &lock.retain_until,
    )
    .await;
    finish(state, row, &probe, now, call).await?;
    reload(state, &row.id).await
}

/// Stores the probe result: the row, its change-log entry and the system
/// record (plus the call's own record when there is one).
async fn finish(
    state: &AppState,
    row: &ExportRow,
    probe: &LockProbe,
    now: OffsetDateTime,
    call: Option<(&CallCtx, &Value)>,
) -> Result<(), ApiError> {
    let at = format_rfc3339(now);
    let strength = match probe.strength {
        StoreLockStrength::None => "none",
        StoreLockStrength::BucketRule => "bucket_rule",
        StoreLockStrength::Compliance => "compliance",
    };
    let failed = probe.detail.contains("put failed")
        || probe.detail.contains("head failed")
        || probe.detail.starts_with("probe unavailable");
    let detail = super::short_error(&probe.detail);

    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "UPDATE audit_exports SET lock_strength = ?, probed_at = ?, probe_detail = ?, \
         updated_at = ? WHERE id = ?",
    )
    .bind(strength)
    .bind(&at)
    .bind(&detail)
    .bind(&at)
    .bind(&row.id)
    .execute(&mut *w.tx)
    .await?;
    let change_seq = changelog::append(
        &mut w.tx,
        &Actor::system(ACTOR),
        "audit_export",
        &row.id,
        Op::Update,
        json!({
            "id": row.id,
            "lock_strength": strength,
            "probed_at": at,
            "probe_detail": detail,
            "updated_at": at,
        }),
    )
    .await?;

    let system = Record {
        actor: Actor::system(ACTOR),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: PROBE_OPERATION.to_string(),
        scope: None,
        target_type: Some("audit_export".to_string()),
        target_id: Some(row.id.clone()),
        project_id: None,
        params: json!({}),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: new_id(Prefix::Request),
        result: if failed { Outcome::Error } else { Outcome::Ok },
        result_detail: failed.then(|| "probe_failed".to_string()),
        change_seq: Some(change_seq),
    };
    audit::append(&mut w.tx, &system).await?;

    if let Some((ctx, params)) = call {
        ctx.append_ok(
            &mut w.tx,
            Some(Scope::ProviderManage),
            Some(&Target {
                kind: "audit_export".to_string(),
                id: row.id.clone(),
            }),
            None,
            params,
            Some(change_seq),
        )
        .await?;
    }
    w.commit().await?;
    Ok(())
}

async fn reload(state: &AppState, export_id: &str) -> Result<AuditExport, ApiError> {
    let row = load_by_id(&state.store, export_id)
        .await?
        .ok_or_else(|| ApiError::not_found("audit export"))?;
    Ok(AuditExport::from_row(&row))
}

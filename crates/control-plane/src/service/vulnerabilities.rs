// SPDX-License-Identifier: AGPL-3.0-only
//! The two vulnerability reads (ADR 0150): one app's report and the cluster
//! summary.
//!
//! Rows come from `vulnerability_findings`, the age of the data from
//! `osv_mirror`, and the image inventory from the primary's cache. Both calls
//! authorize through [`CallCtx::authorize_read`] and end in
//! [`CallCtx::finish_read`], so an agent's read is audited and a human's is not.

use crate::AppState;
use crate::api::apps::app_target;
use crate::api::vulnerabilities::{
    AppVulnerabilities, AppVulnerabilitiesQuery, AppVulnerabilitySummary, FindingSubject,
    NodeKernelVulnerabilities, QuoroVulnerabilities, ResolvedReason, VulnerabilityCounts,
    VulnerabilityDataSource, VulnerabilityFinding, VulnerabilitySeverity, VulnerabilitySummary,
    VulnerabilitySummaryQuery,
};
use crate::deploy::is_id_with_prefix;
use crate::error::{ApiError, Target};
use crate::request::{CallCtx, Resource};
use crate::vulns::status;
use quoro_types::{Prefix, Scope, is_id};
use serde_json::{Map, Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

/// Columns of [`VulnerabilityFinding`], in the order [`finding_from_row`]
/// expects them.
const FINDING_COLUMNS: &str = "id, subject_kind, app_id, image_digest, node_id, component, \
     vuln_id, aliases, summary, ecosystem, package, source_package, installed_version, \
     fixed_version, severity, cvss_score, cvss_vector, kev, epss, first_seen_at, resolved_at, \
     resolved_reason";

/// Most severe first, then KEV, then the id and the package.
const MOST_SEVERE_FIRST: &str = "CASE severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 \
     WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC, kev DESC, vuln_id ASC, package ASC";

/// The prefix of the offset cursor this module returns and validates.
const CURSOR_PREFIX: &str = "v1:";

/// The cursor for the next page, from the number of rows already skipped.
fn encode_cursor(offset: i64) -> String {
    format!("{CURSOR_PREFIX}{offset}")
}

/// The offset a cursor names: no cursor is the first page.
///
/// # Errors
///
/// `validation_failed` for a cursor this module did not write.
fn decode_cursor(cursor: Option<&str>) -> Result<i64, ApiError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    cursor
        .strip_prefix(CURSOR_PREFIX)
        .and_then(|rest| rest.parse::<i64>().ok())
        .filter(|offset| *offset >= 0)
        .ok_or_else(|| ApiError::validation("cursor is not a valid page cursor"))
}

/// The query fields of the app report, as audit params.
fn app_params(app_id: &str, query: &AppVulnerabilitiesQuery) -> Value {
    let mut params = Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    if let Some(severity) = query.severity {
        params.insert("severity".to_string(), json!(severity.as_str()));
    }
    if let Some(kev) = query.kev {
        params.insert("kev".to_string(), json!(kev));
    }
    if query.include_resolved == Some(true) {
        params.insert("include_resolved".to_string(), json!(true));
    }
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &query.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    Value::Object(params)
}

/// The query fields of the summary, as audit params.
fn summary_params(query: &VulnerabilitySummaryQuery) -> Value {
    let mut params = Map::new();
    if let Some(project_id) = &query.project_id {
        params.insert("project_id".to_string(), json!(project_id));
    }
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &query.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    Value::Object(params)
}

/// One finding row from `vulnerability_findings`.
fn finding_from_row(row: &SqliteRow) -> Result<VulnerabilityFinding, ApiError> {
    let subject_kind: String = row.try_get("subject_kind")?;
    let subject_kind = match subject_kind.as_str() {
        "app" => FindingSubject::App,
        "quoro" => FindingSubject::Quoro,
        "host_kernel" => FindingSubject::HostKernel,
        other => {
            return Err(ApiError::internal(&format!(
                "unknown finding subject_kind {other:?}"
            )));
        }
    };
    let aliases: String = row.try_get("aliases")?;
    let aliases: Vec<String> = serde_json::from_str(&aliases)
        .map_err(|error| ApiError::internal(&format!("stored finding aliases: {error}")))?;
    let severity: String = row.try_get("severity")?;
    let severity = VulnerabilitySeverity::parse(&severity)
        .ok_or_else(|| ApiError::internal(&format!("unknown finding severity {severity:?}")))?;
    let resolved_reason: Option<String> = row.try_get("resolved_reason")?;
    let resolved_reason = match resolved_reason {
        Some(reason) => Some(ResolvedReason::parse(&reason).ok_or_else(|| {
            ApiError::internal(&format!("unknown finding resolved_reason {reason:?}"))
        })?),
        None => None,
    };
    let kev: i64 = row.try_get("kev")?;
    Ok(VulnerabilityFinding {
        id: row.try_get("id")?,
        subject_kind,
        app_id: row.try_get("app_id")?,
        image_digest: row.try_get("image_digest")?,
        node_id: row.try_get("node_id")?,
        component: row.try_get("component")?,
        vuln_id: row.try_get("vuln_id")?,
        aliases,
        summary: row.try_get("summary")?,
        ecosystem: row.try_get("ecosystem")?,
        package: row.try_get("package")?,
        source_package: row.try_get("source_package")?,
        installed_version: row.try_get("installed_version")?,
        fixed_version: row.try_get("fixed_version")?,
        severity,
        cvss_score: row.try_get("cvss_score")?,
        cvss_vector: row.try_get("cvss_vector")?,
        kev: kev != 0,
        epss: row.try_get("epss")?,
        upstream_version_only: (subject_kind == FindingSubject::HostKernel).then_some(true),
        first_seen_at: row.try_get("first_seen_at")?,
        resolved_at: row.try_get("resolved_at")?,
        resolved_reason,
    })
}

/// Open findings of one subject, most severe first.
async fn open_findings(
    state: &AppState,
    condition: &str,
    bind: Option<&str>,
) -> Result<Vec<VulnerabilityFinding>, ApiError> {
    let sql = format!(
        "SELECT {FINDING_COLUMNS} FROM vulnerability_findings WHERE {condition} \
         ORDER BY {MOST_SEVERE_FIRST}"
    );
    let mut statement = sqlx::query(&sql);
    if let Some(value) = bind {
        statement = statement.bind(value);
    }
    let rows = statement.fetch_all(state.store.pool()).await?;
    rows.iter().map(finding_from_row).collect()
}

/// Counts from `(severity, n, kev)` rows grouped by severity.
fn counts_from_rows(rows: &[SqliteRow]) -> Result<VulnerabilityCounts, ApiError> {
    let mut counts = VulnerabilityCounts::default();
    for row in rows {
        let severity: String = row.try_get("severity")?;
        let count: i64 = row.try_get("n")?;
        let kev: i64 = row.try_get("kev")?;
        counts.kev += kev;
        match severity.as_str() {
            "critical" => counts.critical += count,
            "high" => counts.high += count,
            "medium" => counts.medium += count,
            "low" => counts.low += count,
            "unknown" => counts.unknown += count,
            other => {
                return Err(ApiError::internal(&format!(
                    "unknown finding severity {other:?}"
                )));
            }
        }
    }
    Ok(counts)
}

/// Open findings of one app's current image, by severity.
async fn app_counts(
    state: &AppState,
    app_id: &str,
    image_digest: Option<&str>,
) -> Result<VulnerabilityCounts, ApiError> {
    let Some(digest) = image_digest else {
        return Ok(VulnerabilityCounts::default());
    };
    let rows = sqlx::query(
        "SELECT severity, COUNT(*) AS n, COALESCE(SUM(kev), 0) AS kev \
         FROM vulnerability_findings \
         WHERE subject_kind = 'app' AND app_id = ? AND image_digest = ? AND resolved_at IS NULL \
         GROUP BY severity",
    )
    .bind(app_id)
    .bind(digest)
    .fetch_all(state.store.pool())
    .await?;
    counts_from_rows(&rows)
}

/// Open findings matching one subject condition, by severity.
async fn subject_counts(
    state: &AppState,
    condition: &str,
    bind: &str,
) -> Result<VulnerabilityCounts, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT severity, COUNT(*) AS n, COALESCE(SUM(kev), 0) AS kev \
         FROM vulnerability_findings WHERE {condition} GROUP BY severity"
    ))
    .bind(bind)
    .fetch_all(state.store.pool())
    .await?;
    counts_from_rows(&rows)
}

/// The running release's own open findings.
async fn quoro_report(state: &AppState) -> Result<QuoroVulnerabilities, ApiError> {
    let items = open_findings(
        state,
        "subject_kind = 'quoro' AND resolved_at IS NULL",
        None,
    )
    .await?;
    let counts = subject_counts(
        state,
        "subject_kind = 'quoro' AND resolved_at IS NULL",
        "quoro",
    )
    .await?;
    Ok(QuoroVulnerabilities {
        version: env!("CARGO_PKG_VERSION").to_string(),
        counts,
        items,
    })
}

/// Every node's host kernel with its open findings.
async fn kernel_report(state: &AppState) -> Result<Vec<NodeKernelVulnerabilities>, ApiError> {
    let rows =
        sqlx::query("SELECT id, roles, kvm FROM nodes WHERE status <> 'removed' ORDER BY id")
            .fetch_all(state.store.pool())
            .await?;
    let mut out = Vec::new();
    for row in &rows {
        let node_id: String = row.try_get("id")?;
        let roles: String = row.try_get("roles")?;
        let roles: Vec<String> = serde_json::from_str(&roles)
            .map_err(|error| ApiError::internal(&format!("stored node roles: {error}")))?;
        if !roles.iter().any(|role| role == "node") {
            continue;
        }
        let kvm: String = row.try_get("kvm")?;
        let kvm: Value = serde_json::from_str(&kvm)
            .map_err(|error| ApiError::internal(&format!("stored node kvm: {error}")))?;
        let kernel = kvm
            .get("kernel")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let items = open_findings(
            state,
            "subject_kind = 'host_kernel' AND node_id = ? AND resolved_at IS NULL",
            Some(&node_id),
        )
        .await?;
        let counts = subject_counts(
            state,
            "subject_kind = 'host_kernel' AND node_id = ? AND resolved_at IS NULL",
            &node_id,
        )
        .await?;
        out.push(NodeKernelVulnerabilities {
            node_id,
            kernel,
            counts,
            items,
        });
    }
    Ok(out)
}

/// Every data source the primary holds, ordered by ecosystem.
async fn all_data_sources(state: &AppState) -> Result<Vec<VulnerabilityDataSource>, ApiError> {
    let rows = sqlx::query(
        "SELECT ecosystem, source, data_modified_at, refreshed_at, last_error \
         FROM osv_mirror ORDER BY ecosystem",
    )
    .fetch_all(state.store.pool())
    .await?;
    rows.iter()
        .map(|row| {
            let source: String = row.try_get("source")?;
            let source = crate::api::vulnerabilities::VulnerabilitySource::parse(&source)
                .ok_or_else(|| {
                    ApiError::internal(&format!("unknown osv_mirror source {source:?}"))
                })?;
            Ok(VulnerabilityDataSource {
                ecosystem: row.try_get("ecosystem")?,
                source,
                data_modified_at: row.try_get("data_modified_at")?,
                refreshed_at: row.try_get("refreshed_at")?,
                last_error: row.try_get("last_error")?,
            })
        })
        .collect()
}

/// One row of the summary's app page.
struct AppRow {
    id: String,
    project_id: String,
    name: String,
    image_digest: Option<String>,
}

/// An app's vulnerability report, with one page of findings.
///
/// `counts`, `inventory` and `data` describe the whole report and ignore the
/// filters; `items` is the filtered page.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed app
/// id, `validation_failed` for a bad cursor, or a database error.
pub async fn app_vulnerabilities(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    query: &AppVulnerabilitiesQuery,
) -> Result<AppVulnerabilities, ApiError> {
    let params = app_params(app_id, query);
    let target = app_target(app_id);
    let access = ctx
        .authorize_read(state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<AppVulnerabilities, ApiError> = async {
        if !is_id_with_prefix("app", app_id) {
            return Err(ApiError::not_found("app"));
        }
        let row = sqlx::query(
            "SELECT a.id, d.image_digest FROM apps a \
             LEFT JOIN deployments d ON d.id = a.current_deployment_id WHERE a.id = ?",
        )
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
        let image_digest: Option<String> = row.try_get("image_digest")?;

        let limit = query.limit()?;
        let offset = decode_cursor(query.cursor.as_deref())?;
        let include_resolved = query.include_resolved == Some(true);
        let min_rank = query.severity.map_or(-1, VulnerabilitySeverity::rank);
        let has_severity = i64::from(query.severity.is_some());
        let kev_only = i64::from(query.kev == Some(true));
        // An app with no image has no open findings: an empty digest matches
        // no row.
        let digest = image_digest.clone().unwrap_or_default();
        let rows = sqlx::query(&format!(
            "SELECT {FINDING_COLUMNS} FROM vulnerability_findings \
             WHERE subject_kind = 'app' AND app_id = ? \
               AND ((resolved_at IS NULL AND image_digest = ?) \
                    OR (? = 1 AND resolved_at IS NOT NULL)) \
               AND (? = 0 OR CASE severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 \
                    WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END >= ?) \
               AND (? = 0 OR kev = 1) \
             ORDER BY (resolved_at IS NULL) DESC, \
               CASE WHEN resolved_at IS NULL THEN CASE severity WHEN 'critical' THEN 4 \
                    WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END \
                    ELSE 0 END DESC, \
               CASE WHEN resolved_at IS NULL THEN kev ELSE 0 END DESC, \
               COALESCE(resolved_at, '') DESC, vuln_id ASC, package ASC \
             LIMIT ? OFFSET ?"
        ))
        .bind(app_id)
        .bind(&digest)
        .bind(i64::from(include_resolved))
        .bind(has_severity)
        .bind(min_rank)
        .bind(kev_only)
        .bind(limit + 1)
        .bind(offset)
        .fetch_all(state.store.pool())
        .await?;
        let mut findings = rows
            .iter()
            .map(finding_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if i64::try_from(findings.len()).unwrap_or(0) > limit {
            findings.truncate(usize::try_from(limit).unwrap_or(0));
            Some(encode_cursor(offset.saturating_add(limit)))
        } else {
            None
        };

        let counts = app_counts(state, app_id, image_digest.as_deref()).await?;
        let inventory = status::inventory_status(state, image_digest.as_deref()).await?;
        let data = status::data_sources_for(state, image_digest.as_deref()).await?;
        Ok(AppVulnerabilities {
            app_id: app_id.to_string(),
            inventory,
            counts,
            data,
            items: findings,
            next_cursor,
        })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// The cluster's vulnerability summary, with one page of apps.
///
/// `quoro` and `host_kernels` are node data: present only when the caller
/// also holds cluster-wide `node:read`, otherwise left out.
///
/// # Errors
///
/// `forbidden` without `app:read`, `validation_failed` for a malformed filter
/// or cursor, or a database error.
pub async fn vulnerability_summary(
    state: &AppState,
    ctx: &CallCtx,
    query: &VulnerabilitySummaryQuery,
) -> Result<VulnerabilitySummary, ApiError> {
    let params = summary_params(query);
    let mut access = ctx
        .authorize_read(state, Scope::AppRead, None, &params)
        .await?;
    // The summary is a cluster-level read: it is authorized like a list (not
    // refused for a narrow selector) but recorded against the cluster.
    access.set_target(Target {
        kind: "cluster".to_string(),
        id: state.cluster_id.clone(),
    });
    let result: Result<VulnerabilitySummary, ApiError> = async {
        if let Some(project_id) = &query.project_id
            && !is_id(Prefix::Project, project_id)
        {
            return Err(ApiError::validation("project_id is not a valid project id"));
        }
        let limit = query.limit()?;
        let offset = decode_cursor(query.cursor.as_deref())?;
        let project_id = query.project_id.clone().unwrap_or_default();
        let rows = sqlx::query(
            "SELECT a.id, a.project_id, a.name, d.image_digest, \
               EXISTS(SELECT 1 FROM vulnerability_findings f \
                      WHERE f.subject_kind = 'app' AND f.app_id = a.id \
                        AND f.resolved_at IS NULL \
                        AND (f.severity = 'critical' OR f.kev = 1)) AS urgent \
             FROM apps a LEFT JOIN deployments d ON d.id = a.current_deployment_id \
             WHERE (? = '' OR a.project_id = ?) \
             ORDER BY urgent DESC, a.id ASC LIMIT ? OFFSET ?",
        )
        .bind(&project_id)
        .bind(&project_id)
        .bind(limit + 1)
        .bind(offset)
        .fetch_all(state.store.pool())
        .await?;
        let mut apps: Vec<AppRow> = Vec::with_capacity(rows.len());
        for row in &rows {
            apps.push(AppRow {
                id: row.try_get("id")?,
                project_id: row.try_get("project_id")?,
                name: row.try_get("name")?,
                image_digest: row.try_get("image_digest")?,
            });
        }
        let next_cursor = if i64::try_from(apps.len()).unwrap_or(0) > limit {
            apps.truncate(usize::try_from(limit).unwrap_or(0));
            Some(encode_cursor(offset.saturating_add(limit)))
        } else {
            None
        };
        // Keep the order: the filter only drops rows the caller may not see.
        let apps = access
            .filter(state, apps, |app| Resource {
                kind: "app",
                id: &app.id,
                app_id: Some(&app.id),
                project_id: Some(&app.project_id),
                node_id: None,
            })
            .await?;
        let mut summaries = Vec::with_capacity(apps.len());
        for app in apps {
            let inventory = status::inventory_status(state, app.image_digest.as_deref()).await?;
            let counts = app_counts(state, &app.id, app.image_digest.as_deref()).await?;
            summaries.push(AppVulnerabilitySummary {
                app_id: app.id,
                project_id: app.project_id,
                name: app.name,
                inventory_status: inventory.status,
                counts,
            });
        }

        let data = all_data_sources(state).await?;
        let (quoro, host_kernels) = if ctx.holds_cluster_wide(state, Scope::NodeRead).await? {
            (
                Some(quoro_report(state).await?),
                Some(kernel_report(state).await?),
            )
        } else {
            (None, None)
        };
        Ok(VulnerabilitySummary {
            data,
            apps: summaries,
            quoro,
            host_kernels,
            next_cursor,
        })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

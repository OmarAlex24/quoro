// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps/{app_id}/vulnerabilities` and `/api/v1/vulnerabilities`: the
//! two reads of vulnerability tracking (ADR 0138, ADR 0150).
//!
//! The app report says what is known about the image an app runs and lists its
//! findings. The summary lists every app the caller can read, plus the running
//! release's own components and every node's host kernel when the caller also
//! holds cluster-wide `node:read`. Both are reads under `app:read`; an agent's
//! read is audited, a human's is not.

use crate::AppState;
use crate::error::ApiError;
use crate::request::{CallCtx, RequestId};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, Query, State};
use axum::http::HeaderMap;
use axum::routing::get;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Severity from a record's CVSS base score (`openapi.yaml`
/// `VulnerabilitySeverity`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VulnerabilitySeverity {
    Critical,
    High,
    Medium,
    Low,
    Unknown,
}

impl VulnerabilitySeverity {
    /// The wire form stored in `vulnerability_findings.severity`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::Unknown => "unknown",
        }
    }

    /// Parses the wire form.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "critical" => Some(Self::Critical),
            "high" => Some(Self::High),
            "medium" => Some(Self::Medium),
            "low" => Some(Self::Low),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    /// The ordering rank: `critical` is the most severe, `unknown` the least.
    #[must_use]
    pub const fn rank(self) -> i64 {
        match self {
            Self::Critical => 4,
            Self::High => 3,
            Self::Medium => 2,
            Self::Low => 1,
            Self::Unknown => 0,
        }
    }
}

/// What a finding is about (`VulnerabilityFinding.subject_kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FindingSubject {
    App,
    Quoro,
    HostKernel,
}

/// Why a finding stopped applying (`VulnerabilityFinding.resolved_reason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedReason {
    ImageChanged,
    DataChanged,
    AppDeleted,
    NodeRemoved,
    Upgraded,
}

impl ResolvedReason {
    /// Parses the wire form.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "image_changed" => Some(Self::ImageChanged),
            "data_changed" => Some(Self::DataChanged),
            "app_deleted" => Some(Self::AppDeleted),
            "node_removed" => Some(Self::NodeRemoved),
            "upgraded" => Some(Self::Upgraded),
            _ => None,
        }
    }
}

/// What is known about an app's current image (`ImageInventoryStatus.status`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InventoryStatus {
    Inventoried,
    Pending,
    NotInventoried,
    #[default]
    NoImage,
    PendingData,
}

/// One vulnerability in one package of one subject (`VulnerabilityFinding`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VulnerabilityFinding {
    /// Finding id.
    #[schema(pattern = "^vln_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// What the finding is about.
    #[schema(inline)]
    pub subject_kind: FindingSubject,
    /// The app, when `subject_kind` is `app`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false)]
    pub app_id: Option<String>,
    /// Digest of the app's image the package was found in.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub image_digest: Option<String>,
    /// The node, when `subject_kind` is `host_kernel`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false)]
    pub node_id: Option<String>,
    /// The Quoro component, when `subject_kind` is `quoro`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub component: Option<String>,
    /// The OSV id, for example a CVE, DSA or GHSA id.
    #[schema(min_length = 1, max_length = 200)]
    pub vuln_id: String,
    /// Other ids of the same vulnerability.
    pub aliases: Vec<String>,
    /// One-line summary from the record, when it has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub summary: Option<String>,
    /// OSV ecosystem with its release, for example `Debian:13`.
    pub ecosystem: String,
    /// The installed package's name.
    pub package: String,
    /// The source package the record names, when it differs from `package`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub source_package: Option<String>,
    /// Version installed.
    pub installed_version: String,
    /// The lowest version that fixes it, when the record names one.
    #[schema(required = true)]
    pub fixed_version: Option<String>,
    /// Severity from the CVSS base score.
    pub severity: VulnerabilitySeverity,
    /// CVSS base score, when the record carries a vector.
    #[schema(minimum = 0, maximum = 10, required = true)]
    pub cvss_score: Option<f64>,
    /// The CVSS vector the score comes from.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub cvss_vector: Option<String>,
    /// Whether the vulnerability is in CISA's KEV catalog.
    pub kev: bool,
    /// EPSS probability of exploitation, when the primary holds EPSS data.
    #[schema(minimum = 0, maximum = 1, required = true)]
    pub epss: Option<f64>,
    /// True for a host kernel matched by its upstream version.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub upstream_version_only: Option<bool>,
    /// When the finding was first recorded.
    #[schema(format = "date-time")]
    pub first_seen_at: String,
    /// When it stopped applying; null while open.
    #[schema(format = "date-time", required = true)]
    pub resolved_at: Option<String>,
    /// Why it stopped applying; null while open.
    #[schema(inline, required = true)]
    pub resolved_reason: Option<ResolvedReason>,
}

/// Open findings by severity, and how many of them are in the KEV catalog
/// (`VulnerabilityCounts`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(title = "VulnerabilityCounts")]
pub struct VulnerabilityCounts {
    #[schema(minimum = 0)]
    pub critical: i64,
    #[schema(minimum = 0)]
    pub high: i64,
    #[schema(minimum = 0)]
    pub medium: i64,
    #[schema(minimum = 0)]
    pub low: i64,
    #[schema(minimum = 0)]
    pub unknown: i64,
    #[schema(minimum = 0)]
    pub kev: i64,
}

/// How the primary got one ecosystem's data (`VulnerabilityDataSource.source`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VulnerabilitySource {
    Download,
    Import,
}

impl VulnerabilitySource {
    /// Parses the wire form.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "download" => Some(Self::Download),
            "import" => Some(Self::Import),
            _ => None,
        }
    }
}

/// The age of the primary's copy of one OSV ecosystem
/// (`VulnerabilityDataSource`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct VulnerabilityDataSource {
    /// OSV ecosystem name.
    pub ecosystem: String,
    /// `download` or `import`.
    #[schema(inline)]
    pub source: VulnerabilitySource,
    /// Newest record modification in the data held.
    #[schema(format = "date-time", required = true)]
    pub data_modified_at: Option<String>,
    /// When the primary last brought this ecosystem up to date.
    #[schema(format = "date-time", required = true)]
    pub refreshed_at: Option<String>,
    /// The last refresh error, cleared by a successful refresh.
    #[schema(required = true)]
    pub last_error: Option<String>,
}

/// What is known about the packages of the image an app runs
/// (`ImageInventoryStatus`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ImageInventoryStatus {
    /// Which of the five states the image is in.
    #[schema(inline)]
    pub status: InventoryStatus,
    /// The digest the status is about.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub image_digest: Option<String>,
    /// Distribution and release from the image's os-release.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub os: Option<String>,
    /// Packages in the inventory.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, nullable = false)]
    pub package_count: Option<i64>,
    /// OSV ecosystems the inventory covers.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub ecosystems: Option<Vec<String>>,
    /// Kinds of content the node saw and could not inventory.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub not_inventoried: Option<Vec<String>>,
    /// When the node built the inventory.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(format = "date-time", nullable = false)]
    pub inventoried_at: Option<String>,
}

/// The vulnerability report of one app (`AppVulnerabilities`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AppVulnerabilities {
    /// The app.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// What is known about the image the app runs.
    pub inventory: ImageInventoryStatus,
    /// Open findings by severity.
    pub counts: VulnerabilityCounts,
    /// The data sources the report used, with their age.
    pub data: Vec<VulnerabilityDataSource>,
    /// Findings, open ones first.
    pub items: Vec<VulnerabilityFinding>,
    /// Cursor for the next page, or null.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// One app's line in the cluster summary (`AppVulnerabilitySummary`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AppVulnerabilitySummary {
    /// The app.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Its project.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: String,
    /// The app's name.
    pub name: String,
    /// `ImageInventoryStatus.status` of the app's current image.
    #[schema(inline)]
    pub inventory_status: InventoryStatus,
    /// Open findings of the app by severity.
    pub counts: VulnerabilityCounts,
}

/// Open findings of one node's host kernel (`NodeKernelVulnerabilities`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NodeKernelVulnerabilities {
    /// The node.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// The kernel release the node reported.
    pub kernel: String,
    /// Open findings by severity.
    pub counts: VulnerabilityCounts,
    /// Open findings, most severe first.
    pub items: Vec<VulnerabilityFinding>,
}

/// Open findings of the running release's own components
/// (`QuoroVulnerabilities`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct QuoroVulnerabilities {
    /// The running Quoro version.
    pub version: String,
    /// Open findings by severity.
    pub counts: VulnerabilityCounts,
    /// Open findings, most severe first.
    pub items: Vec<VulnerabilityFinding>,
}

/// The cluster's vulnerability summary (`VulnerabilitySummary`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VulnerabilitySummary {
    /// Every data source the primary holds, with its age.
    pub data: Vec<VulnerabilityDataSource>,
    /// One line per app, apps with critical or KEV findings first.
    pub apps: Vec<AppVulnerabilitySummary>,
    /// The running release's own components, when the caller may read node data.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub quoro: Option<QuoroVulnerabilities>,
    /// One entry per node with the `node` role, when the caller may read node data.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub host_kernels: Option<Vec<NodeKernelVulnerabilities>>,
    /// Cursor for the next page of `apps`, or null.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// `severity`, `kev`, `include_resolved`, `limit` and `cursor` of
/// `getAppVulnerabilities`.
#[derive(Clone, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AppVulnerabilitiesQuery {
    /// Only findings of this severity or more severe.
    pub severity: Option<VulnerabilitySeverity>,
    /// Only findings in the KEV catalog.
    pub kev: Option<bool>,
    /// Also list resolved findings of the app, newest first after the open ones.
    #[param(default = false)]
    pub include_resolved: Option<bool>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

fn parse_bool(key: &str, value: &str) -> Result<bool, ApiError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ApiError::validation(format!("{key} is not true or false"))),
    }
}

fn parse_limit(value: &str) -> Result<i64, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::validation("limit is not a number"))
}

impl AppVulnerabilitiesQuery {
    /// Reads the query from the raw request pairs, so an unknown severity or a
    /// malformed flag is a `validation_failed`.
    ///
    /// # Errors
    ///
    /// `validation_failed` for an unknown severity, a flag that is not true or
    /// false, or a limit that is not a number.
    pub fn parse(pairs: &[(String, String)]) -> Result<Self, ApiError> {
        let mut query = Self {
            severity: None,
            kev: None,
            include_resolved: None,
            limit: None,
            cursor: None,
        };
        for (key, value) in pairs {
            match key.as_str() {
                "severity" => {
                    query.severity =
                        Some(VulnerabilitySeverity::parse(value).ok_or_else(|| {
                            ApiError::validation(format!("unknown severity {value:?}"))
                        })?);
                }
                "kev" => query.kev = Some(parse_bool("kev", value)?),
                "include_resolved" => {
                    query.include_resolved = Some(parse_bool("include_resolved", value)?);
                }
                "limit" => query.limit = Some(parse_limit(value)?),
                "cursor" => query.cursor = Some(value.clone()),
                _ => {}
            }
        }
        Ok(query)
    }

    /// The validated limit.
    ///
    /// # Errors
    ///
    /// `validation_failed` outside 1-500.
    pub fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }
}

/// `project_id`, `limit` and `cursor` of `getVulnerabilitySummary`.
#[derive(Clone, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct VulnerabilitySummaryQuery {
    /// Only apps of this project.
    #[param(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: Option<String>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl VulnerabilitySummaryQuery {
    /// Reads the query from the raw request pairs.
    ///
    /// # Errors
    ///
    /// `validation_failed` for a limit that is not a number.
    pub fn parse(pairs: &[(String, String)]) -> Result<Self, ApiError> {
        let mut query = Self {
            project_id: None,
            limit: None,
            cursor: None,
        };
        for (key, value) in pairs {
            match key.as_str() {
                "project_id" => query.project_id = Some(value.clone()),
                "limit" => query.limit = Some(parse_limit(value)?),
                "cursor" => query.cursor = Some(value.clone()),
                _ => {}
            }
        }
        Ok(query)
    }

    /// The validated limit.
    ///
    /// # Errors
    ///
    /// `validation_failed` outside 1-500.
    pub fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }
}

/// Every vulnerability route.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/apps/{app_id}/vulnerabilities",
            get(get_app_vulnerabilities),
        )
        .route("/api/v1/vulnerabilities", get(get_vulnerability_summary))
}

/// An app's vulnerability report.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed app
/// id, `validation_failed` for a bad filter or cursor, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/vulnerabilities",
    tag = "vulnerabilities",
    operation_id = "getAppVulnerabilities",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        AppVulnerabilitiesQuery,
    ),
    responses(
        (status = 200, description = "The app's report, with one page of findings.",
            body = AppVulnerabilities,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_app_vulnerabilities(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<AppVulnerabilities>, ApiError> {
    let query = AppVulnerabilitiesQuery::parse(&pairs)?;
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/vulnerabilities",
        &headers,
    );
    let report =
        crate::service::vulnerabilities::app_vulnerabilities(&state, &ctx, &app_id, &query).await?;
    Ok(Json(report))
}

/// The cluster's vulnerability summary.
///
/// # Errors
///
/// `forbidden` without `app:read`, `validation_failed` for a bad filter or
/// cursor, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/vulnerabilities",
    tag = "vulnerabilities",
    operation_id = "getVulnerabilitySummary",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(VulnerabilitySummaryQuery),
    responses(
        (status = 200, description = "The summary, with one page of apps.",
            body = VulnerabilitySummary,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_vulnerability_summary(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<VulnerabilitySummary>, ApiError> {
    let query = VulnerabilitySummaryQuery::parse(&pairs)?;
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/vulnerabilities",
        &headers,
    );
    let summary =
        crate::service::vulnerabilities::vulnerability_summary(&state, &ctx, &query).await?;
    Ok(Json(summary))
}

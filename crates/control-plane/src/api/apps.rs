// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps*`: apps, their environment and their lifecycle actions.
//!
//! [`routes`] also mounts the deployment ([`super::deployments`]) and domain
//! ([`super::domains`]) routes, and [`AppsDoc`] carries the OpenAPI paths and
//! schemas of all three modules; `crate::openapi` merges it into the
//! generated document.

use crate::AppState;
use crate::api::deployments::{
    DEPLOYMENT_COLUMNS, Deployment, DeploymentTrigger, deployment_from_row,
};
use crate::api::domains::{DOMAIN_COLUMNS, Domain, domain_from_row};
use crate::api::page::PageQuery;
use crate::api::vms::{VM_COLUMNS, Vm, vm_from_row};

use crate::changelog::{self, Op};
use crate::deploy;
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::store::now_rfc3339;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use quoro_types::{Prefix, Scope, is_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// Columns of [`App`], in the order [`app_from_row`] expects them.
pub(crate) const APP_COLUMNS: &str = "id, project_id, name, stateful, ha_policy, backup_schedule, \
     replicas, image, source, port, health_check, resources, placement, status, current_deployment_id, \
     restart_generation, created_at, updated_at";

/// `apps.status` (`App.status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AppStatus {
    Deploying,
    Running,
    Degraded,
    Down,
    Stopped,
}

impl AppStatus {
    /// The wire form stored in `apps.status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deploying => "deploying",
            Self::Running => "running",
            Self::Degraded => "degraded",
            Self::Down => "down",
            Self::Stopped => "stopped",
        }
    }

    /// Parses `apps.status`; `None` is an internal inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "deploying" => Some(Self::Deploying),
            "running" => Some(Self::Running),
            "degraded" => Some(Self::Degraded),
            "down" => Some(Self::Down),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

/// `apps.ha_policy` (`App.ha_policy`). M5a accepts `none` and `cold`; `warm`
/// and `hot` are refused until M5b and M6 (ADR 0020).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HaPolicy {
    #[default]
    None,
    Cold,
    Warm,
    Hot,
}

impl HaPolicy {
    /// The wire form stored in `apps.ha_policy`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Hot => "hot",
        }
    }
}

/// When scheduled backups run and how many are kept (`openapi.yaml`
/// `BackupSchedule`, ADR 0147). Stored in `apps.backup_schedule` as JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "BackupSchedule",
    description = "When scheduled backups run and how many are kept (ADR 0147)."
)]
pub struct BackupSchedule {
    /// Hours between two scheduled backups, counted from the last succeeded one.
    #[schema(minimum = 1, maximum = 168)]
    pub interval_h: i64,
    /// Succeeded backups kept per source.
    #[schema(minimum = 1, maximum = 100)]
    pub keep: i64,
}

impl Default for BackupSchedule {
    fn default() -> Self {
        Self {
            interval_h: 24,
            keep: 7,
        }
    }
}

/// How a node snapshots a volume (`openapi.yaml` `AppHaCost.snapshot_method`,
/// ADR 0146).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMethod {
    Reflink,
    PauseCopy,
}

/// Why an HA policy is not available to an app (`openapi.yaml`
/// `HaPolicyCost.unavailable_reason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HaUnavailableReason {
    Stateless,
    NoRepository,
    NotInThisRelease,
}

/// Capacity an HA policy reserves on other nodes (`openapi.yaml` `HaCapacity`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "HaCapacity",
    description = "Capacity an HA policy reserves on other nodes for this app."
)]
pub struct HaCapacity {
    /// Virtual CPUs kept for the app on other nodes.
    #[schema(minimum = 0)]
    pub vcpus: i64,
    /// Memory in mebibytes kept for the app on other nodes.
    #[schema(minimum = 0)]
    pub memory_mb: i64,
    /// Disk in gibibytes kept for the app on other nodes.
    #[schema(minimum = 0)]
    pub disk_gb: i64,
}

/// What one HA policy costs an app (`openapi.yaml` `HaPolicyCost`, ADR 0020).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "HaPolicyCost",
    description = "What one HA policy costs this app (ADR 0020). Estimates, not reservations."
)]
pub struct HaPolicyCost {
    /// The HA policy this entry prices.
    #[schema(inline)]
    pub policy: HaPolicy,
    /// Whether the app may switch to this policy now.
    pub available: bool,
    /// Why `available` is false, or null.
    #[serde(default)]
    #[schema(inline, required = false)]
    pub unavailable_reason: Option<HaUnavailableReason>,
    /// Capacity reserved on other nodes.
    pub reserved: HaCapacity,
    /// Bytes this policy keeps in object storage, or null when not computed.
    #[schema(minimum = 0, required = true)]
    pub storage_bytes: Option<i64>,
    /// For `cold`, seconds to restore the latest backup; null otherwise.
    #[schema(minimum = 0, required = true)]
    pub restore_estimate_s: Option<i64>,
    /// For `cold`, how long each backup pauses the app; null otherwise.
    #[schema(minimum = 0, required = true)]
    pub pause_estimate_ms: Option<i64>,
}

/// The cost of each HA policy for one app (`openapi.yaml` `AppHaCost`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "AppHaCost",
    description = "The cost of each HA policy for one app (ADR 0020)."
)]
pub struct AppHaCost {
    /// How the node that holds the app's volume snapshots it, or null.
    #[schema(inline, required = true)]
    pub snapshot_method: Option<SnapshotMethod>,
    /// One entry per policy, in the order `none`, `cold`, `warm`, `hot`.
    pub policies: Vec<HaPolicyCost>,
    /// When the primary computed these values.
    #[schema(format = "date-time")]
    pub computed_at: String,
}

/// Health check an app declares (`openapi.yaml` `HealthCheck`); the node probes
/// it and reports events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "HealthCheck",
    description = "Health check an app declares; the node probes it and reports events."
)]
pub struct HealthCheck {
    /// HTTP path probed on the app port.
    #[schema(default = "/")]
    pub path: String,
    /// Seconds between probes.
    #[schema(minimum = 1, default = 10)]
    pub interval_s: i64,
    /// Seconds a probe may take.
    #[schema(minimum = 1, default = 2)]
    pub timeout_s: i64,
    /// Consecutive failures before the app is declared down.
    #[schema(minimum = 1, default = 3)]
    pub failures: i64,
}

fn default_health_path() -> String {
    "/".to_string()
}

const fn default_health_interval() -> i64 {
    10
}

const fn default_health_timeout() -> i64 {
    2
}

const fn default_health_failures() -> i64 {
    3
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self {
            path: default_health_path(),
            interval_s: default_health_interval(),
            timeout_s: default_health_timeout(),
            failures: default_health_failures(),
        }
    }
}

/// Resources of one app instance (`openapi.yaml` `AppResources`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(title = "AppResources", description = "Resources of one app instance.")]
pub struct AppResources {
    /// Number of virtual CPUs.
    #[schema(minimum = 1)]
    pub vcpus: i64,
    /// Memory in mebibytes.
    #[schema(minimum = 1)]
    pub memory_mb: i64,
    /// Disk in gibibytes.
    #[schema(minimum = 1)]
    pub disk_gb: i64,
}

impl Default for AppResources {
    fn default() -> Self {
        Self {
            vcpus: 1,
            memory_mb: 256,
            disk_gb: 1,
        }
    }
}

/// Placement constraint of an app (`openapi.yaml` `AppPlacement`); an empty
/// `node_ids` lets the scheduler pick.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "AppPlacement",
    description = "Placement constraint of an app; an empty `node_ids` lets the scheduler pick."
)]
pub struct AppPlacement {
    /// Nodes the app may run on; empty means any node.
    #[schema(value_type = Vec<NodeIdItem>, inline)]
    pub node_ids: Vec<String>,
}

/// A node id item with the contract pattern, inlined into `AppPlacement`.
struct NodeIdItem;

impl utoipa::PartialSchema for NodeIdItem {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, Type};
        ObjectBuilder::new()
            .schema_type(Type::String)
            .pattern(Some("^nod_[0-9A-HJKMNP-TV-Z]{26}$"))
            .into()
    }
}

impl ToSchema for NodeIdItem {}

/// A deployable service (`openapi.yaml` `App`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "App",
    description = "A deployable service. It runs as one microVM per instance (D-0008); in M1-M4 an app has one instance. `ha_policy` exists from M0; M5a accepts `none` and `cold` (ADR 0020, ADR 0147). An app either deploys images given by reference (`source` null) or builds them from a Git source (ADR 0117). An app is stateful exactly when it has a volume (ADR 0145)."
)]
pub struct App {
    /// App id.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Project the app belongs to.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: String,
    /// Name, unique in the project.
    #[schema(pattern = "^[a-z0-9-]{1,40}$")]
    pub name: String,
    /// Whether the app has a volume; derived (ADR 0145).
    pub stateful: bool,
    /// HA policy. M5a accepts `none` and `cold` (ADR 0020, ADR 0147).
    #[schema(inline)]
    pub ha_policy: HaPolicy,
    /// The app's `cold` backup schedule (ADR 0147).
    pub backup_schedule: BackupSchedule,
    /// Instances of the app, at most one per node (ADR 0142).
    #[schema(minimum = 1, maximum = 64)]
    pub replicas: i64,
    /// What each HA policy costs this app; absent until the primary computes it.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    pub ha_cost: Option<AppHaCost>,
    /// Current OCI reference of the app. For an app with a Git source, the
    /// reference of its last successful build, or `""` before the first one.
    pub image: String,
    /// The Git source the app builds its images from, or null for an app that
    /// deploys images given by reference (ADR 0117).
    #[serde(default)]
    #[schema(required = false)]
    pub source: Option<AppSource>,
    /// Container port the app listens on.
    #[schema(minimum = 1, maximum = 65535)]
    pub port: i64,
    /// Health check the node probes.
    pub health_check: HealthCheck,
    /// Resources of one instance.
    pub resources: AppResources,
    /// Placement constraint.
    pub placement: AppPlacement,
    /// Aggregated status of the app and its instances.
    #[schema(inline)]
    pub status: AppStatus,
    /// The deployment whose image the app runs, or null before the first one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(pattern = "^dep_[0-9A-HJKMNP-TV-Z]{26}$", required = false)]
    pub current_deployment_id: Option<String>,
    /// Counter bumped by a restart or redeploy.
    #[schema(minimum = 0)]
    pub restart_generation: i64,
    /// When the app was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the app record last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

/// An app together with its instances and its domains (`openapi.yaml`
/// `AppDetail`).
#[derive(Debug, Serialize)]
pub struct AppDetail {
    /// The app fields, flattened into this object.
    #[serde(flatten)]
    pub app: App,
    /// VMs whose `app_id` is this app.
    pub instances: Vec<Vm>,
    /// Hostnames routed to this app.
    pub domains: Vec<Domain>,
    /// The edge's view of the app's instances (ADR 0143), from the edge's
    /// newest `status_report`; empty while no edge serves the app.
    pub edge_backends: Vec<EdgeBackend>,
}

/// One instance of an app as the edge that serves it sees it (`openapi.yaml`
/// `EdgeBackend`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[schema(title = "EdgeBackend")]
pub struct EdgeBackend {
    /// The machine with the `edge` role that reported it.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub edge_node_id: String,
    /// The instance.
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub vm_id: String,
    /// Node that runs the instance.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Whether the edge sends it requests.
    pub in_rotation: bool,
    /// When `in_rotation` last changed, or when the edge first saw the
    /// instance.
    #[schema(format = "date-time")]
    pub since: String,
    /// Why it is out of rotation (`health_check` or `connect_failed`); null
    /// while in rotation.
    pub reason: Option<String>,
    /// One line from the last failed probe or connection; null when none.
    #[schema(max_length = 500)]
    pub last_error: Option<String>,
}

impl utoipa::PartialSchema for AppDetail {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::Ref;
        use utoipa::openapi::schema::{AllOfBuilder, ArrayBuilder, ObjectBuilder, Type};
        let extra = ObjectBuilder::new()
            .schema_type(Type::Object)
            .property(
                "instances",
                ArrayBuilder::new().items(Ref::from_schema_name("Vm")),
            )
            .property(
                "domains",
                ArrayBuilder::new().items(Ref::from_schema_name("Domain")),
            )
            .property(
                "edge_backends",
                ArrayBuilder::new().items(Ref::from_schema_name("EdgeBackend")),
            )
            .required("instances")
            .required("domains");
        AllOfBuilder::new()
            .schema_type(Type::Object)
            .item(Ref::from_schema_name("App"))
            .item(extra)
            .into()
    }
}

impl ToSchema for AppDetail {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("AppDetail")
    }
}

/// One page of apps (`openapi.yaml` `AppList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(title = "AppList", description = "One page of apps.")]
pub struct AppList {
    pub items: Vec<App>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// The kind of an app source (`openapi.yaml` `AppSource.kind`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AppSourceKind {
    /// A Git repository built with its Dockerfile.
    #[default]
    Git,
}

/// One `--build-arg` of a Git source (`openapi.yaml` `BuildArg`).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "BuildArg",
    description = "One `--build-arg` of a Git source. Not a secret."
)]
pub struct BuildArg {
    /// Argument name.
    #[schema(pattern = "^[A-Za-z_][A-Za-z0-9_]*$")]
    pub name: String,
    /// Argument value.
    #[schema(max_length = 4096)]
    pub value: String,
}

/// The Git source an app builds its images from (`openapi.yaml` `AppSource`,
/// ADR 0117). It is stored in `apps.source` as JSON; a build then runs from a
/// snapshot of it.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "AppSource",
    description = "The Git source an app builds its images from (ADR 0117)."
)]
pub struct AppSource {
    /// Kind of source. Only `git` in M4.
    #[schema(inline)]
    pub kind: AppSourceKind,
    /// HTTPS URL of a public Git repository.
    #[schema(pattern = "^https://[^\\s]+$", max_length = 2000)]
    pub repo_url: String,
    /// Branch, tag or full commit SHA to build.
    #[serde(rename = "ref")]
    #[schema(min_length = 1, max_length = 255)]
    pub git_ref: String,
    /// Path of the Dockerfile, relative to `context_dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, min_length = 1, max_length = 500, default = "Dockerfile", required = false)]
    pub dockerfile_path: Option<String>,
    /// Build context, relative to the repository root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, min_length = 1, max_length = 500, default = ".", required = false)]
    pub context_dir: Option<String>,
    /// Stage of a multi-stage Dockerfile to build, or null for the last stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(max_length = 128, required = false)]
    pub target: Option<String>,
    /// Build arguments, passed as `--build-arg`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(max_items = 100, required = false)]
    pub build_args: Vec<BuildArg>,
    /// Names of the app's environment variables exposed as BuildKit secrets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(schema_with = source_secrets_schema, required = false)]
    pub secrets: Vec<String>,
}

/// `AppSource.secrets`: unique environment-variable names, at most 50.
fn source_secrets_schema() -> utoipa::openapi::schema::Array {
    use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, Type};
    ArrayBuilder::new()
        .items(
            ObjectBuilder::new()
                .schema_type(Type::String)
                .pattern(Some("^[A-Za-z_][A-Za-z0-9_]*$")),
        )
        .max_items(Some(50))
        .unique_items(true)
        .build()
}

/// Body of `createApp` (`openapi.yaml` `CreateAppRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "CreateAppRequest",
    description = "Create an app. Exactly one of `image` and `source` is given. The first deployment or build is started separately."
)]
pub struct CreateAppRequest {
    /// Project the app belongs to.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: String,
    /// Name, unique in the project.
    #[schema(pattern = "^[a-z0-9-]{1,40}$")]
    pub name: String,
    /// OCI reference of the app image. Required unless `source` is given.
    #[serde(default)]
    #[schema(value_type = String, required = false)]
    pub image: Option<String>,
    /// The Git source to build the app from (ADR 0117).
    #[serde(default)]
    #[schema(value_type = AppSource, required = false)]
    pub source: Option<AppSource>,
    /// Container port the app listens on.
    #[schema(minimum = 1, maximum = 65535)]
    pub port: i64,
    /// Kept for compatibility; only `false` is accepted (ADR 0145).
    #[serde(default)]
    #[schema(default = false, required = false)]
    pub stateful: bool,
    /// HA policy. Only `none` at creation (ADR 0145).
    #[serde(default)]
    #[schema(inline, default = "none", required = false)]
    pub ha_policy: HaPolicy,
    /// The app's `cold` backup schedule (ADR 0147).
    #[serde(default)]
    #[schema(value_type = BackupSchedule, required = false)]
    pub backup_schedule: Option<BackupSchedule>,
    /// Instances of the app, one per node (ADR 0142).
    #[serde(default = "default_replicas")]
    #[schema(minimum = 1, maximum = 64, default = 1, required = false)]
    pub replicas: i64,
    /// Health check the node probes.
    #[serde(default)]
    #[schema(value_type = HealthCheck, required = false)]
    pub health_check: Option<HealthCheck>,
    /// Resources of one instance.
    #[serde(default)]
    #[schema(value_type = AppResources, required = false)]
    pub resources: Option<AppResources>,
    /// Placement constraint.
    #[serde(default)]
    #[schema(value_type = AppPlacement, required = false)]
    pub placement: Option<AppPlacement>,
}

/// Body of `updateApp` (`openapi.yaml` `UpdateAppRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "UpdateAppRequest",
    description = "Change app settings. The image is not changed here: a new image is rolled out as a deployment. Absent fields stay unchanged."
)]
pub struct UpdateAppRequest {
    /// New name, unique in the project.
    #[serde(default)]
    #[schema(value_type = String, pattern = "^[a-z0-9-]{1,40}$", required = false)]
    pub name: Option<String>,
    /// New container port.
    #[serde(default)]
    #[schema(value_type = i64, minimum = 1, maximum = 65535, required = false)]
    pub port: Option<i64>,
    /// Replaces the whole Git source of an app that has one (ADR 0117).
    #[serde(default)]
    #[schema(value_type = AppSource, required = false)]
    pub source: Option<AppSource>,
    /// HA policy: `none`, or `cold` for a stateful app with a backup
    /// repository (ADR 0020, ADR 0147).
    #[serde(default)]
    #[schema(value_type = HaPolicy, inline, required = false)]
    pub ha_policy: Option<HaPolicy>,
    /// New `cold` backup schedule (ADR 0147).
    #[serde(default)]
    #[schema(value_type = BackupSchedule, required = false)]
    pub backup_schedule: Option<BackupSchedule>,
    /// New instance count (ADR 0142).
    #[serde(default)]
    #[schema(value_type = i64, minimum = 1, maximum = 64, required = false)]
    pub replicas: Option<i64>,
    /// New health check.
    #[serde(default)]
    #[schema(value_type = HealthCheck, required = false)]
    pub health_check: Option<HealthCheck>,
    /// New resources.
    #[serde(default)]
    #[schema(value_type = AppResources, required = false)]
    pub resources: Option<AppResources>,
    /// New placement constraint.
    #[serde(default)]
    #[schema(value_type = AppPlacement, required = false)]
    pub placement: Option<AppPlacement>,
}

/// The readable part of an environment variable (`openapi.yaml` `AppEnvVar`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "AppEnvVar",
    description = "The readable part of an environment variable: its `name` and when it changed. Values are write-only and never returned (they are stored encrypted at rest with the cluster key)."
)]
pub struct AppEnvVar {
    /// App the variable belongs to.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Variable name.
    #[schema(pattern = "^[A-Za-z_][A-Za-z0-9_]*$")]
    pub name: String,
    /// When the value last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

/// One variable in a full replacement of an app's environment
/// (`openapi.yaml` `AppEnvVarInput`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "AppEnvVarInput",
    description = "One variable in a full replacement of an app's environment."
)]
pub struct AppEnvVarInput {
    /// Variable name.
    #[schema(pattern = "^[A-Za-z_][A-Za-z0-9_]*$")]
    pub name: String,
    /// Variable value. It is stored encrypted and never returned.
    #[schema(write_only = true)]
    pub value: String,
}

/// Names and change times of an app's environment variables
/// (`openapi.yaml` `AppEnvList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    title = "AppEnvList",
    description = "Names and change times of an app's environment variables."
)]
pub struct AppEnvList {
    pub items: Vec<AppEnvVar>,
}

/// Body of `replaceAppEnv` (`openapi.yaml` `AppEnvReplaceRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "AppEnvReplaceRequest",
    description = "Full replacement of an app's environment. Variables not listed are removed. Names must be unique in the list."
)]
pub struct AppEnvReplaceRequest {
    pub items: Vec<AppEnvVarInput>,
}

/// Query of `listApps`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListAppsQuery {
    /// Only apps of this project.
    #[param(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: Option<String>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListAppsQuery {
    /// `limit` and `cursor` in the shared page shape.
    #[must_use]
    pub(crate) fn page(&self) -> PageQuery {
        PageQuery {
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Builds an [`App`] from an `apps` row.
pub(crate) fn app_from_row(row: &SqliteRow) -> Result<App, ApiError> {
    let ha_policy: String = row.try_get("ha_policy")?;
    let ha_policy = match ha_policy.as_str() {
        "none" => HaPolicy::None,
        "cold" => HaPolicy::Cold,
        "warm" => HaPolicy::Warm,
        "hot" => HaPolicy::Hot,
        other => return Err(ApiError::internal(&format!("unknown ha_policy {other:?}"))),
    };
    let status: String = row.try_get("status")?;
    let status = AppStatus::parse(&status)
        .ok_or_else(|| ApiError::internal(&format!("unknown app status {status:?}")))?;
    let health_check: String = row.try_get("health_check")?;
    let health_check = serde_json::from_str(&health_check)
        .map_err(|error| ApiError::internal(&format!("stored health_check: {error}")))?;
    let resources: String = row.try_get("resources")?;
    let resources = serde_json::from_str(&resources)
        .map_err(|error| ApiError::internal(&format!("stored resources: {error}")))?;
    let placement: String = row.try_get("placement")?;
    let placement = serde_json::from_str(&placement)
        .map_err(|error| ApiError::internal(&format!("stored placement: {error}")))?;
    let backup_schedule: String = row.try_get("backup_schedule")?;
    let backup_schedule = serde_json::from_str(&backup_schedule)
        .map_err(|error| ApiError::internal(&format!("stored backup_schedule: {error}")))?;
    let source: Option<String> = row.try_get("source")?;
    let source = match source {
        Some(source) => Some(
            serde_json::from_str(&source)
                .map_err(|error| ApiError::internal(&format!("stored source: {error}")))?,
        ),
        None => None,
    };
    Ok(App {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        name: row.try_get("name")?,
        stateful: row.try_get("stateful")?,
        ha_policy,
        backup_schedule,
        replicas: row.try_get("replicas")?,
        ha_cost: None,
        image: row.try_get("image")?,
        source,
        port: row.try_get("port")?,
        health_check,
        resources,
        placement,
        status,
        current_deployment_id: row.try_get("current_deployment_id")?,
        restart_generation: row.try_get("restart_generation")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Audit/MCP operation string of `createApp`; also its idempotency key scope.
const CREATE_APP: &str = "POST /api/v1/apps";

pub(crate) fn app_target(app_id: &str) -> crate::error::Target {
    crate::error::Target {
        kind: "app".to_string(),
        id: app_id.to_string(),
    }
}

fn body_value<T: Serialize>(body: &T) -> Value {
    serde_json::to_value(body).unwrap_or(Value::Null)
}

/// The `Idempotency-Key` of the call, when present and well-formed.
pub(crate) fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ApiError::validation("Idempotency-Key is not valid ASCII"))?;
    if key.is_empty() {
        return Ok(None);
    }
    if key.chars().count() > 128 {
        return Err(ApiError::validation(
            "Idempotency-Key is longer than 128 characters",
        ));
    }
    Ok(Some(key.to_string()))
}

/// The project id of the call when it names an existing project. Audit rows
/// carry a foreign key to `projects`, so an unknown id is omitted.
async fn known_project(state: &AppState, project_id: Option<&str>) -> Option<String> {
    let id = project_id?;
    if !is_id(Prefix::Project, id) {
        return None;
    }
    sqlx::query_as::<_, (String,)>("SELECT id FROM projects WHERE id = ?")
        .bind(id)
        .fetch_optional(state.store.pool())
        .await
        .ok()
        .flatten()
        .map(|row| row.0)
}

/// Whether `name` matches `^[a-z0-9-]{1,40}$`.
fn valid_name(name: &str) -> bool {
    (1..=40).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn validate_health(health: &HealthCheck) -> Result<(), ApiError> {
    if !health.path.starts_with('/') {
        return Err(ApiError::validation("health_check.path must start with /"));
    }
    if health.interval_s < 1 {
        return Err(ApiError::validation(
            "health_check.interval_s must be at least 1",
        ));
    }
    if health.timeout_s < 1 {
        return Err(ApiError::validation(
            "health_check.timeout_s must be at least 1",
        ));
    }
    if health.failures < 1 {
        return Err(ApiError::validation(
            "health_check.failures must be at least 1",
        ));
    }
    Ok(())
}

fn default_replicas() -> i64 {
    1
}

/// Checks of a requested replica count (ADR 0142). More than one instance
/// needs a machine with the `edge` role that serves the app's domains
/// (ADR 0143). Until the edge epic ships no edge serves anything, so above 1
/// is always a conflict.
fn validate_replicas(replicas: i64) -> Result<(), ApiError> {
    if !(1..=64).contains(&replicas) {
        return Err(ApiError::validation("replicas is outside 1-64"));
    }
    if replicas > 1 {
        return Err(ApiError::conflict(
            "replicas above 1 need an enrolled machine with the edge role",
        ));
    }
    Ok(())
}

fn validate_backup_schedule(schedule: &BackupSchedule) -> Result<(), ApiError> {
    if !(1..=168).contains(&schedule.interval_h) {
        return Err(ApiError::validation(
            "backup_schedule.interval_h is outside 1-168",
        ));
    }
    if !(1..=100).contains(&schedule.keep) {
        return Err(ApiError::validation(
            "backup_schedule.keep is outside 1-100",
        ));
    }
    Ok(())
}

fn validate_resources(resources: &AppResources) -> Result<(), ApiError> {
    if !(1..=255).contains(&resources.vcpus) {
        return Err(ApiError::validation(format!(
            "resources.vcpus {} is outside 1-255",
            resources.vcpus
        )));
    }
    if resources.memory_mb < 1 {
        return Err(ApiError::validation(
            "resources.memory_mb must be at least 1",
        ));
    }
    // `AppResources.disk_gb` has `minimum: 1`, matching `CHECK (disk_gb >= 1)`
    // on `vms`, because every app instance is a VM.
    if resources.disk_gb < 1 {
        return Err(ApiError::validation("resources.disk_gb must be at least 1"));
    }
    Ok(())
}

fn validate_placement(placement: &AppPlacement) -> Result<(), ApiError> {
    for node_id in &placement.node_ids {
        if !is_id(Prefix::Node, node_id) {
            return Err(ApiError::validation(
                "placement.node_ids entries must be node ids",
            ));
        }
    }
    Ok(())
}

async fn check_placement_nodes(
    conn: &mut sqlx::SqliteConnection,
    placement: &AppPlacement,
) -> Result<(), ApiError> {
    for node_id in &placement.node_ids {
        let node: Option<(String,)> = sqlx::query_as("SELECT status FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await?;
        match node.as_ref().map(|(status,)| status.as_str()) {
            None => return Err(ApiError::validation("placement node_id does not exist")),
            Some("removed") => {
                return Err(ApiError::validation("placement node_id is removed"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The static validation of a `createApp` body.
fn validate_create(body: &CreateAppRequest) -> Result<(), ApiError> {
    if !is_id(Prefix::Project, &body.project_id) {
        return Err(ApiError::validation("project_id is not a valid project id"));
    }
    if !valid_name(&body.name) {
        return Err(ApiError::validation("name must match ^[a-z0-9-]{1,40}$"));
    }
    // Exactly one of `image` and `source` (openapi.yaml `CreateAppRequest`).
    match (&body.image, &body.source) {
        (Some(_), Some(_)) => {
            return Err(ApiError::validation(
                "give either image or source, not both",
            ));
        }
        (None, None) => return Err(ApiError::validation("image or source is required")),
        (None, Some(source)) => {
            crate::build::validate_source(source)?;
        }
        (Some(image), None) => {
            if image.trim().is_empty() {
                return Err(ApiError::validation("image must not be empty"));
            }
        }
    }
    if !(1..=65535).contains(&body.port) {
        return Err(ApiError::validation("port is outside 1-65535"));
    }
    // An app becomes stateful when a volume is created for it (ADR 0145).
    if body.stateful {
        return Err(ApiError::validation(
            "stateful is derived: create a volume for the app instead",
        ));
    }
    // `cold` needs a volume, which is created after the app (ADR 0145).
    if body.ha_policy != HaPolicy::None {
        return Err(ApiError::validation(
            "ha_policy must be none when the app is created",
        ));
    }
    if let Some(schedule) = &body.backup_schedule {
        validate_backup_schedule(schedule)?;
    }
    validate_replicas(body.replicas)?;
    if let Some(health) = &body.health_check {
        validate_health(health)?;
    }
    if let Some(resources) = &body.resources {
        validate_resources(resources)?;
    }
    if let Some(placement) = &body.placement {
        validate_placement(placement)?;
    }
    Ok(())
}

/// The static validation of an `updateApp` body.
fn validate_update(body: &UpdateAppRequest) -> Result<(), ApiError> {
    if let Some(name) = &body.name
        && !valid_name(name)
    {
        return Err(ApiError::validation("name must match ^[a-z0-9-]{1,40}$"));
    }
    if let Some(port) = body.port
        && !(1..=65535).contains(&port)
    {
        return Err(ApiError::validation("port is outside 1-65535"));
    }
    match body.ha_policy {
        None | Some(HaPolicy::None) => {}
        // `cold` needs a stateful app and a backup repository (ADR 0147); no
        // app can hold a volume until the storage epic ships.
        Some(HaPolicy::Cold) => {
            return Err(ApiError::conflict(
                "ha_policy cold needs a stateful app and a backup repository",
            ));
        }
        Some(HaPolicy::Warm | HaPolicy::Hot) => {
            return Err(ApiError::validation(
                "ha_policy warm and hot are not supported before M5b and M6",
            ));
        }
    }
    if let Some(schedule) = &body.backup_schedule {
        validate_backup_schedule(schedule)?;
    }
    if let Some(replicas) = body.replicas {
        validate_replicas(replicas)?;
    }
    if let Some(health) = &body.health_check {
        validate_health(health)?;
    }
    if let Some(resources) = &body.resources {
        validate_resources(resources)?;
    }
    if let Some(placement) = &body.placement {
        validate_placement(placement)?;
    }
    Ok(())
}

/// The app's VM rows, oldest first.
pub(crate) async fn app_vms(
    conn: &mut sqlx::SqliteConnection,
    app_id: &str,
) -> Result<Vec<Vm>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {VM_COLUMNS} FROM vms WHERE app_id = ? ORDER BY id"
    ))
    .bind(app_id)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(vm_from_row).collect()
}

pub(crate) async fn app_vm_ids(
    conn: &mut sqlx::SqliteConnection,
    app_id: &str,
) -> Result<Vec<String>, ApiError> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM vms WHERE app_id = ? ORDER BY id")
        .bind(app_id)
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Lowercase hex, for the ciphertext the change log may carry.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

/// Apps, optionally filtered by project. Reads are not audited for humans.
///
/// # Errors
///
/// `forbidden` without `app:read`, `validation_failed` for a malformed filter
/// or page bound, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps",
    tag = "apps",
    operation_id = "listApps",
    extensions(
        ("x-quoro-scope" = json!("app:read")),
        ("x-quoro-mcp-tool" = json!("list_apps")),
    ),
    params(ListAppsQuery),
    responses(
        (status = 200, description = "One page of apps.", body = AppList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_apps(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Query(query): Query<ListAppsQuery>,
) -> Result<Json<AppList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, "GET /api/v1/apps", &headers);
    let list = crate::service::apps::list_apps(&state, &ctx, &query).await?;
    Ok(Json(list))
}

/// Creates an app record. The image is rolled out by a separate deployment.
///
/// # Errors
///
/// `forbidden` without `app:create`, `conflict` on a duplicate name or an
/// idempotency clash, `validation_failed` for a bad body or an unknown
/// project/node, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps",
    tag = "apps",
    operation_id = "createApp",
    extensions(("x-quoro-scope" = json!("app:create"))),
    params(
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second resource (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = CreateAppRequest, content_type = "application/json",
        description = "Project, name, image, port and optional settings."),
    responses(
        (status = 201, description = "The app was created.", body = App,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    ApiJson(body): ApiJson<CreateAppRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, CREATE_APP, &headers);
    let params = body_value(&body);
    let audit_project = known_project(&state, Some(&body.project_id)).await;
    ctx.authorize(
        &state,
        Scope::AppCreate,
        None,
        audit_project.as_deref(),
        &params,
    )
    .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_APP, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, body) => {
                let id = body["id"].as_str().unwrap_or_default();
                ctx.audit_replay(
                    &state,
                    Scope::AppCreate,
                    &app_target(id),
                    body["project_id"].as_str(),
                    &params,
                )
                .await?;
                return Ok((status, Json(body)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    &state,
                    Some(Scope::AppCreate),
                    None,
                    audit_project.as_deref(),
                    &params,
                    &error,
                )
                .await?;
                return Err(error);
            }
        }
    }

    match insert_app(&state, &ctx, &body, &params).await {
        Ok(app) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_APP,
                    key,
                    StatusCode::CREATED,
                    serde_json::to_value(&app).unwrap_or(Value::Null),
                );
            }
            Ok((StatusCode::CREATED, Json(app)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_APP, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::AppCreate),
                None,
                audit_project.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Inserts the app, its change-log entry, event and audit record in one
/// transaction.
async fn insert_app(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateAppRequest,
    params: &Value,
) -> Result<App, ApiError> {
    validate_create(body)?;
    let now = now_rfc3339();
    // A Git app holds the built image of its last successful build, or the
    // empty string before the first one; the column is NOT NULL.
    let source = match &body.source {
        Some(source) => Some(crate::build::validate_source(source)?),
        None => None,
    };
    let app = App {
        id: deploy::new_id_with_prefix("app"),
        project_id: body.project_id.clone(),
        name: body.name.clone(),
        stateful: false,
        ha_policy: HaPolicy::None,
        backup_schedule: body.backup_schedule.unwrap_or_default(),
        replicas: body.replicas,
        ha_cost: None,
        image: body.image.clone().unwrap_or_default(),
        source,
        port: body.port,
        health_check: body.health_check.clone().unwrap_or_default(),
        resources: body.resources.clone().unwrap_or_default(),
        placement: body.placement.clone().unwrap_or_default(),
        // No deployment exists yet, so there is nothing to deploy.
        status: AppStatus::Stopped,
        current_deployment_id: None,
        restart_generation: 0,
        created_at: now.clone(),
        updated_at: now,
    };

    let mut w = state.store.begin_write().await?;
    let project: Option<(String,)> = sqlx::query_as("SELECT id FROM projects WHERE id = ?")
        .bind(&app.project_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if project.is_none() {
        return Err(ApiError::validation("project_id does not exist"));
    }
    let taken: Option<(String,)> =
        sqlx::query_as("SELECT id FROM apps WHERE project_id = ? AND name = ?")
            .bind(&app.project_id)
            .bind(&app.name)
            .fetch_optional(&mut *w.tx)
            .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(format!(
            "name {} is taken in this project",
            app.name
        )));
    }
    check_placement_nodes(&mut w.tx, &app.placement).await?;

    let health_check = serde_json::to_string(&app.health_check)
        .map_err(|error| ApiError::internal(&format!("health_check: {error}")))?;
    let resources = serde_json::to_string(&app.resources)
        .map_err(|error| ApiError::internal(&format!("resources: {error}")))?;
    let placement = serde_json::to_string(&app.placement)
        .map_err(|error| ApiError::internal(&format!("placement: {error}")))?;
    let backup_schedule = serde_json::to_string(&app.backup_schedule)
        .map_err(|error| ApiError::internal(&format!("backup_schedule: {error}")))?;
    let source = match &app.source {
        Some(source) => Some(crate::build::source_json(source)?),
        None => None,
    };
    sqlx::query(
        "INSERT INTO apps (id, project_id, name, stateful, ha_policy, backup_schedule, replicas, \
         image, source, port, health_check, resources, placement, status, current_deployment_id, \
         restart_generation, created_at, updated_at) \
         VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?)",
    )
    .bind(&app.id)
    .bind(&app.project_id)
    .bind(&app.name)
    .bind(app.ha_policy.as_str())
    .bind(&backup_schedule)
    .bind(app.replicas)
    .bind(&app.image)
    .bind(&source)
    .bind(app.port)
    .bind(&health_check)
    .bind(&resources)
    .bind(&placement)
    .bind(app.status.as_str())
    .bind(&app.created_at)
    .bind(&app.updated_at)
    .execute(&mut *w.tx)
    .await?;

    let actor = ctx.identity.actor();
    let payload = serde_json::to_value(&app)
        .map_err(|error| ApiError::internal(&format!("app payload: {error}")))?;
    let change_seq =
        changelog::append(&mut w.tx, &actor, "app", &app.id, Op::Create, payload).await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "app.created",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: &app.id,
            project_id: Some(&app.project_id),
            request_id: Some(&ctx.request_id),
            actor: Some(&actor),
            data: json!({
                "app_id": app.id,
                "project_id": app.project_id,
                "name": app.name,
                "image": app.image,
            }),
        },
    )
    .await?;
    let target = app_target(&app.id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppCreate),
        Some(&target),
        Some(&app.project_id),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok(app)
}

/// One app with its instances (its VMs) and its domains.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}",
    tag = "apps",
    operation_id = "getApp",
    extensions(
        ("x-quoro-scope" = json!("app:read")),
        ("x-quoro-mcp-tool" = json!("get_app")),
    ),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The app with its instances and domains.", body = AppDetail,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Json<AppDetail>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}",
        &headers,
    );
    let app = crate::service::apps::get_app(&state, &ctx, &app_id).await?;
    Ok(Json(app))
}

/// Changes app settings. The image is not changed here: a new image is rolled
/// out as a deployment.
///
/// # Errors
///
/// `forbidden` without `app:update`, `not_found` for an unknown or malformed
/// id, `conflict` on a duplicate name, `validation_failed` for a bad body, or a
/// database error.
#[utoipa::path(
    patch,
    path = "/api/v1/apps/{app_id}",
    tag = "apps",
    operation_id = "updateApp",
    extensions(("x-quoro-scope" = json!("app:update"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = UpdateAppRequest, content_type = "application/json",
        description = "Fields to change; absent fields stay unchanged."),
    responses(
        (status = 200, description = "The updated app.", body = App,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn update_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    ApiJson(body): ApiJson<UpdateAppRequest>,
) -> Result<Json<App>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "PATCH /api/v1/apps/{app_id}",
        &headers,
    );
    let params = body_value(&body);
    let target = app_target(&app_id);
    ctx.authorize(&state, Scope::AppUpdate, Some(&target), None, &params)
        .await?;

    match update_app_tx(&state, &ctx, &app_id, &body, &params).await {
        Ok((app, vm_ids)) => {
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok(Json(app))
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::AppUpdate),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

async fn update_app_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    body: &UpdateAppRequest,
    params: &Value,
) -> Result<(App, Vec<String>), ApiError> {
    validate_update(body)?;
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let mut app = app_from_row(&row)?;
    let mut changed = Vec::new();

    // A Git source replaces the whole source of an app that has one; it can
    // neither be added to an image app nor removed in M4 (openapi.yaml
    // `UpdateAppRequest`).
    if let Some(source) = &body.source {
        if app.source.is_none() {
            return Err(ApiError::conflict(
                "a Git source cannot be added to an app created from an image",
            ));
        }
        let source = crate::build::validate_source(source)?;
        if app.source.as_ref() != Some(&source) {
            app.source = Some(source);
            changed.push("source");
        }
    }

    if let Some(name) = &body.name
        && *name != app.name
    {
        let taken: Option<(String,)> =
            sqlx::query_as("SELECT id FROM apps WHERE project_id = ? AND name = ? AND id != ?")
                .bind(&app.project_id)
                .bind(name)
                .bind(app_id)
                .fetch_optional(&mut *w.tx)
                .await?;
        if taken.is_some() {
            return Err(ApiError::conflict(format!(
                "name {name} is taken in this project"
            )));
        }
        app.name = name.clone();
        changed.push("name");
    }
    if let Some(port) = body.port
        && port != app.port
    {
        app.port = port;
        changed.push("port");
    }
    if let Some(policy) = body.ha_policy
        && policy != app.ha_policy
    {
        app.ha_policy = policy;
        changed.push("ha_policy");
    }
    if let Some(schedule) = body.backup_schedule
        && schedule != app.backup_schedule
    {
        app.backup_schedule = schedule;
        changed.push("backup_schedule");
    }
    if let Some(replicas) = body.replicas
        && replicas != app.replicas
    {
        app.replicas = replicas;
        changed.push("replicas");
    }
    if let Some(health) = &body.health_check
        && *health != app.health_check
    {
        app.health_check = health.clone();
        changed.push("health_check");
    }
    if let Some(resources) = &body.resources
        && *resources != app.resources
    {
        app.resources = resources.clone();
        changed.push("resources");
    }
    if let Some(placement) = &body.placement
        && *placement != app.placement
    {
        check_placement_nodes(&mut w.tx, placement).await?;
        app.placement = placement.clone();
        changed.push("placement");
    }

    let vm_ids = app_vm_ids(&mut w.tx, app_id).await?;
    let target = app_target(app_id);
    if !changed.is_empty() {
        app.updated_at = now_rfc3339();
        let health_check = serde_json::to_string(&app.health_check)
            .map_err(|error| ApiError::internal(&format!("health_check: {error}")))?;
        let resources = serde_json::to_string(&app.resources)
            .map_err(|error| ApiError::internal(&format!("resources: {error}")))?;
        let placement = serde_json::to_string(&app.placement)
            .map_err(|error| ApiError::internal(&format!("placement: {error}")))?;
        let backup_schedule = serde_json::to_string(&app.backup_schedule)
            .map_err(|error| ApiError::internal(&format!("backup_schedule: {error}")))?;
        let source = match &app.source {
            Some(source) => Some(crate::build::source_json(source)?),
            None => None,
        };
        sqlx::query(
            "UPDATE apps SET name = ?, port = ?, ha_policy = ?, backup_schedule = ?, replicas = ?, \
             health_check = ?, resources = ?, placement = ?, source = ?, updated_at = ? \
             WHERE id = ?",
        )
        .bind(&app.name)
        .bind(app.port)
        .bind(app.ha_policy.as_str())
        .bind(&backup_schedule)
        .bind(app.replicas)
        .bind(&health_check)
        .bind(&resources)
        .bind(&placement)
        .bind(&source)
        .bind(&app.updated_at)
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;
        let actor = ctx.identity.actor();
        let payload = serde_json::to_value(&app)
            .map_err(|error| ApiError::internal(&format!("app payload: {error}")))?;
        let change_seq =
            changelog::append(&mut w.tx, &actor, "app", app_id, Op::Update, payload).await?;
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "app.updated",
                source: Source::ControlPlane,
                subject_type: "app",
                subject_id: app_id,
                project_id: Some(&app.project_id),
                request_id: Some(&ctx.request_id),
                actor: Some(&actor),
                data: json!({ "app_id": app_id, "changed": changed }),
            },
        )
        .await?;
        ctx.append_ok(
            &mut w.tx,
            Some(Scope::AppUpdate),
            Some(&target),
            Some(&app.project_id),
            params,
            Some(change_seq),
        )
        .await?;
    } else {
        ctx.append_ok(
            &mut w.tx,
            Some(Scope::AppUpdate),
            Some(&target),
            Some(&app.project_id),
            params,
            None,
        )
        .await?;
    }
    w.commit().await?;
    Ok((app, vm_ids))
}

/// Deletes an app and its data. A human with `app:delete` calls it directly;
/// an agent reaches it through an approval grant (permission-model.md
/// section 9), otherwise the call is refused with `approval_required`.
///
/// # Errors
///
/// `forbidden` without `app:delete` or with `approval_required` for an agent,
/// `not_found` for an unknown or malformed id, or a database error.
#[utoipa::path(
    delete,
    path = "/api/v1/apps/{app_id}",
    tag = "apps",
    operation_id = "deleteApp",
    extensions(
        ("x-quoro-scope" = json!("app:delete")),
        ("x-quoro-mcp-tool" = json!("delete_app")),
    ),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Deletion accepted; the instances stop and are removed.", body = App,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::ApprovalRequired),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn delete_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "DELETE /api/v1/apps/{app_id}",
        &headers,
    );
    let app = crate::service::apps::delete_app(&state, &ctx, &app_id).await?;
    Ok((StatusCode::ACCEPTED, Json(app)).into_response())
}

pub(crate) async fn delete_app_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    params: &Value,
) -> Result<(App, Vec<String>, Vec<String>), ApiError> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let app = app_from_row(&row)?;
    let actor = ctx.identity.actor();
    let target = app_target(app_id);

    // `vms.app_id` is ON DELETE SET NULL: a cascade would orphan running VMs,
    // so every VM row is deleted explicitly, before the app.
    let vm_ids = app_vm_ids(&mut w.tx, app_id).await?;
    for vm_id in &vm_ids {
        sqlx::query("DELETE FROM vms WHERE id = ?")
            .bind(vm_id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(&mut w.tx, &actor, "vm", vm_id, Op::Delete, json!({})).await?;
    }

    let domains = sqlx::query(&format!(
        "SELECT {DOMAIN_COLUMNS} FROM domains WHERE app_id = ?"
    ))
    .bind(app_id)
    .fetch_all(&mut *w.tx)
    .await?;
    for row in &domains {
        let domain = domain_from_row(row)?;
        sqlx::query("DELETE FROM domains WHERE id = ?")
            .bind(&domain.id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "domain",
            &domain.id,
            Op::Delete,
            serde_json::to_value(&domain)
                .map_err(|error| ApiError::internal(&format!("domain payload: {error}")))?,
        )
        .await?;
    }

    let env: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT name, value_enc FROM app_env WHERE app_id = ? ORDER BY name")
            .bind(app_id)
            .fetch_all(&mut *w.tx)
            .await?;
    for (name, value_enc) in &env {
        sqlx::query("DELETE FROM app_env WHERE app_id = ? AND name = ?")
            .bind(app_id)
            .bind(name)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "app_env",
            &format!("{app_id}/{name}"),
            Op::Delete,
            json!({ "app_id": app_id, "name": name, "value_enc": hex(value_enc) }),
        )
        .await?;
    }
    sqlx::query("DELETE FROM app_env WHERE app_id = ?")
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;

    // `builds.deployment_id` references the deployment a success started, so
    // the builds of the app go first; a build that is still running leaves the
    // node's `spec.builds` once its row is gone.
    let builds = sqlx::query(&format!(
        "SELECT {} FROM builds WHERE app_id = ?",
        crate::build::BUILD_COLUMNS
    ))
    .bind(app_id)
    .fetch_all(&mut *w.tx)
    .await?;
    let mut build_nodes = Vec::new();
    for row in &builds {
        let build = crate::build::build_from_row(row)?;
        build_nodes.push(build.node_id.clone());
        sqlx::query("DELETE FROM builds WHERE id = ?")
            .bind(&build.id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "build",
            &build.id,
            Op::Delete,
            serde_json::to_value(&build)
                .map_err(|error| ApiError::internal(&format!("build payload: {error}")))?,
        )
        .await?;
    }

    // `apps.current_deployment_id` references the deployment, so it goes first.
    sqlx::query("UPDATE apps SET current_deployment_id = NULL WHERE id = ?")
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;
    let deployments = sqlx::query(&format!(
        "SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE app_id = ?"
    ))
    .bind(app_id)
    .fetch_all(&mut *w.tx)
    .await?;
    for row in &deployments {
        let deployment = deployment_from_row(row)?;
        sqlx::query("DELETE FROM deployments WHERE id = ?")
            .bind(&deployment.id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "deployment",
            &deployment.id,
            Op::Delete,
            serde_json::to_value(&deployment)
                .map_err(|error| ApiError::internal(&format!("deployment payload: {error}")))?,
        )
        .await?;
    }

    sqlx::query("DELETE FROM apps WHERE id = ?")
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;
    let payload = serde_json::to_value(&app)
        .map_err(|error| ApiError::internal(&format!("app payload: {error}")))?;
    let change_seq =
        changelog::append(&mut w.tx, &actor, "app", app_id, Op::Delete, payload).await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "app.deleted",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: app_id,
            project_id: Some(&app.project_id),
            request_id: Some(&ctx.request_id),
            actor: Some(&actor),
            data: json!({
                "app_id": app_id,
                "project_id": app.project_id,
                "name": app.name,
            }),
        },
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppDelete),
        Some(&target),
        Some(&app.project_id),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok((app, vm_ids, build_nodes))
}

/// Bumps `restart_generation`; the node restarts the instances that carry the
/// new generation, and `app.restarted` is emitted.
///
/// # Errors
///
/// `forbidden` without `app:restart`, `not_found` for an unknown or malformed
/// id, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/restart",
    tag = "apps",
    operation_id = "restartApp",
    extensions(
        ("x-quoro-scope" = json!("app:restart")),
        ("x-quoro-mcp-tool" = json!("restart_app")),
    ),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Restart accepted; `restart_generation` was bumped.", body = App,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn restart_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/restart",
        &headers,
    );
    let app = crate::service::apps::restart_app(&state, &ctx, &app_id).await?;
    Ok((StatusCode::ACCEPTED, Json(app)).into_response())
}

pub(crate) async fn restart_app_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    params: &Value,
) -> Result<(App, Vec<String>), ApiError> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let mut app = app_from_row(&row)?;
    let actor = ctx.identity.actor();
    app.restart_generation += 1;
    app.updated_at = now_rfc3339();
    sqlx::query("UPDATE apps SET restart_generation = ?, updated_at = ? WHERE id = ?")
        .bind(app.restart_generation)
        .bind(&app.updated_at)
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;
    let change_seq = changelog::append(
        &mut w.tx,
        &actor,
        "app",
        app_id,
        Op::Update,
        json!({
            "id": app_id,
            "restart_generation": app.restart_generation,
            "updated_at": app.updated_at,
        }),
    )
    .await?;

    let vms = app_vms(&mut w.tx, app_id).await?;
    for mut vm in vms {
        vm.restart_generation = app.restart_generation;
        vm.updated_at = now_rfc3339();
        sqlx::query("UPDATE vms SET restart_generation = ?, updated_at = ? WHERE id = ?")
            .bind(vm.restart_generation)
            .bind(&vm.updated_at)
            .bind(&vm.id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "vm",
            &vm.id,
            Op::Update,
            serde_json::to_value(&vm)
                .map_err(|error| ApiError::internal(&format!("vm payload: {error}")))?,
        )
        .await?;
    }

    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "app.restarted",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: app_id,
            project_id: Some(&app.project_id),
            request_id: Some(&ctx.request_id),
            actor: Some(&actor),
            data: json!({
                "app_id": app_id,
                "restart_generation": app.restart_generation,
            }),
        },
    )
    .await?;
    let vm_ids = app_vm_ids(&mut w.tx, app_id).await?;
    let target = app_target(app_id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppRestart),
        Some(&target),
        Some(&app.project_id),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok((app, vm_ids))
}

/// Rolls out the app's current image again with `trigger: redeploy`.
///
/// # Errors
///
/// `forbidden` without `app:redeploy`, `not_found` for an unknown or malformed
/// id, `validation_failed` when no node is available, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/redeploy",
    tag = "apps",
    operation_id = "redeployApp",
    extensions(
        ("x-quoro-scope" = json!("app:redeploy")),
        ("x-quoro-mcp-tool" = json!("redeploy_app")),
    ),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "The redeployment was accepted and is now `pending`.", body = Deployment,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn redeploy_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/redeploy",
        &headers,
    );
    let deployment = crate::service::apps::redeploy_app(&state, &ctx, &app_id).await?;
    Ok((StatusCode::ACCEPTED, Json(deployment)).into_response())
}

pub(crate) async fn redeploy_app_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    params: &Value,
) -> Result<(Deployment, deploy::CreatedDeployment), ApiError> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let app: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT image, source FROM apps WHERE id = ?")
            .bind(app_id)
            .fetch_optional(&mut *w.tx)
            .await?;
    let Some((image, source)) = app else {
        return Err(ApiError::not_found("app"));
    };
    // A Git app re-rolls the built image of its last successful build, and
    // has none before the first one.
    if source.is_some() && image.is_empty() {
        return Err(ApiError::conflict(
            "the app has no built image yet; start a build first",
        ));
    }
    let created = deploy::create_deployment(
        &mut w.tx,
        app_id,
        &image,
        DeploymentTrigger::Redeploy,
        &ctx.identity.actor(),
    )
    .await?;
    let deployment = created.deployment.clone();
    let target = crate::error::Target {
        kind: "deployment".to_string(),
        id: deployment.id.clone(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppRedeploy),
        Some(&target),
        Some(&created.project_id),
        params,
        Some(created.change_seq),
    )
    .await?;
    w.commit().await?;
    Ok((deployment, created))
}

/// Starts the instances of a stopped app.
///
/// # Errors
///
/// `forbidden` without `app:start`, `not_found` for an unknown or malformed
/// id, `conflict` when the app has no deployment or is already running, or a
/// database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/start",
    tag = "apps",
    operation_id = "startApp",
    extensions(("x-quoro-scope" = json!("app:start"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Start accepted; the app status follows to `running`.", body = App,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn start_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Response, ApiError> {
    set_desired_status(
        state,
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/start",
        Scope::AppStart,
        app_id,
        headers,
        true,
    )
    .await
}

/// Stops the instances of an app. Its domains stay configured and its
/// deployment stays current.
///
/// # Errors
///
/// `forbidden` without `app:stop`, `not_found` for an unknown or malformed id,
/// `conflict` when the app has no deployment, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/stop",
    tag = "apps",
    operation_id = "stopApp",
    extensions(("x-quoro-scope" = json!("app:stop"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Stop accepted; the app status follows to `stopped`.", body = App,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn stop_app(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Response, ApiError> {
    set_desired_status(
        state,
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/stop",
        Scope::AppStop,
        app_id,
        headers,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn set_desired_status(
    state: AppState,
    identity: crate::auth::Identity,
    request_id: String,
    operation: &'static str,
    scope: Scope,
    app_id: String,
    headers: HeaderMap,
    start: bool,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id, operation, &headers);
    let params = json!({ "app_id": app_id });
    let target = app_target(&app_id);
    ctx.authorize(&state, scope, Some(&target), None, &params)
        .await?;

    match set_desired_tx(&state, &ctx, &app_id, start, scope, &params).await {
        Ok((app, vm_ids)) => {
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok((StatusCode::ACCEPTED, Json(app)).into_response())
        }
        Err(error) => {
            ctx.audit_error(&state, Some(scope), Some(&target), None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn set_desired_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    start: bool,
    scope: Scope,
    params: &Value,
) -> Result<(App, Vec<String>), ApiError> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let mut app = app_from_row(&row)?;
    let vms = app_vms(&mut w.tx, app_id).await?;
    if app.current_deployment_id.is_none() || vms.is_empty() {
        return Err(ApiError::conflict("the app has no deployment"));
    }
    let desired = if start {
        crate::api::vms::DesiredStatus::Running
    } else {
        crate::api::vms::DesiredStatus::Stopped
    };
    if start
        && vms
            .iter()
            .all(|vm| vm.desired_status == crate::api::vms::DesiredStatus::Running)
    {
        return Err(ApiError::conflict("the app is already running"));
    }
    let actor = ctx.identity.actor();
    for mut vm in vms {
        if vm.desired_status == desired {
            continue;
        }
        vm.desired_status = desired;
        vm.updated_at = now_rfc3339();
        sqlx::query("UPDATE vms SET desired_status = ?, updated_at = ? WHERE id = ?")
            .bind(if start { "running" } else { "stopped" })
            .bind(&vm.updated_at)
            .bind(&vm.id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "vm",
            &vm.id,
            Op::Update,
            serde_json::to_value(&vm)
                .map_err(|error| ApiError::internal(&format!("vm payload: {error}")))?,
        )
        .await?;
    }

    let status = if start {
        AppStatus::Deploying
    } else {
        AppStatus::Stopped
    };
    if app.status != status {
        app.status = status;
        app.updated_at = now_rfc3339();
        sqlx::query("UPDATE apps SET status = ?, updated_at = ? WHERE id = ?")
            .bind(app.status.as_str())
            .bind(&app.updated_at)
            .bind(app_id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "app",
            app_id,
            Op::Update,
            json!({ "id": app_id, "status": app.status.as_str(), "updated_at": app.updated_at }),
        )
        .await?;
    }
    let vm_ids = app_vm_ids(&mut w.tx, app_id).await?;
    let target = app_target(app_id);
    ctx.append_ok(
        &mut w.tx,
        Some(scope),
        Some(&target),
        Some(&app.project_id),
        params,
        None,
    )
    .await?;
    w.commit().await?;
    Ok((app, vm_ids))
}

/// Names and change times of an app's environment variables; values are never
/// returned.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/env",
    tag = "apps",
    operation_id = "getAppEnv",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "Names and change times of the app's variables.", body = AppEnvList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_app_env(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Json<AppEnvList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/env",
        &headers,
    );
    let target = app_target(&app_id);
    let params = json!({ "app_id": app_id });
    let mut access = ctx
        .authorize_read(&state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<_, ApiError> = async {
        if !deploy::is_id_with_prefix("app", &app_id) {
            return Err(ApiError::not_found("app"));
        }
        let project: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
            .bind(&app_id)
            .fetch_optional(state.store.pool())
            .await?;
        let Some((project_id,)) = project else {
            return Err(ApiError::not_found("app"));
        };
        access.set_project(Some(&project_id));
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT name, updated_at FROM app_env WHERE app_id = ? ORDER BY name")
                .bind(&app_id)
                .fetch_all(state.store.pool())
                .await?;
        let items = rows
            .into_iter()
            .map(|(name, updated_at)| AppEnvVar {
                app_id: app_id.clone(),
                name,
                updated_at,
            })
            .collect();
        Ok(Json(AppEnvList { items }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Full replacement of an app's environment; variables not listed are removed.
///
/// A running VM keeps its create-time environment until the app is deployed
/// again: the new values reach the node in its next desired-state snapshot,
/// and the VM spec applies them when it is created.
///
/// # Errors
///
/// `forbidden` without `app:update`, `not_found` for an unknown or malformed
/// id, `validation_failed` for a bad name, duplicate names, more than 200
/// variables or a value over 32 KiB, or a database error.
#[utoipa::path(
    put,
    path = "/api/v1/apps/{app_id}/env",
    tag = "apps",
    operation_id = "replaceAppEnv",
    extensions(("x-quoro-scope" = json!("app:update"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = AppEnvReplaceRequest, content_type = "application/json",
        description = "The complete list of variables."),
    responses(
        (status = 200, description = "The new set of names and change times.", body = AppEnvList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn replace_app_env(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    ApiJson(body): ApiJson<AppEnvReplaceRequest>,
) -> Result<Json<AppEnvList>, ApiError> {
    // Audit params carry names only: a value never enters the audit log.
    let names: Vec<&str> = body.items.iter().map(|item| item.name.as_str()).collect();
    let params = json!({ "names": names });
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "PUT /api/v1/apps/{app_id}/env",
        &headers,
    );
    let target = app_target(&app_id);
    ctx.authorize(&state, Scope::AppUpdate, Some(&target), None, &params)
        .await?;

    match replace_app_env_tx(&state, &ctx, &app_id, &body, &params).await {
        Ok((items, vm_ids)) => {
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok(Json(AppEnvList { items }))
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::AppUpdate),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Whether `name` matches `^[A-Za-z_][A-Za-z0-9_]*$`.
fn valid_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return false,
    }
    bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Variables the node sets on every app instance (node-agent-protocol.md
/// section 6.10, ADR 0142); an app's environment may not hold them.
const RESERVED_ENV_NAMES: [&str; 2] = ["QUORO_INSTANCE_ID", "QUORO_NODE_ID"];

fn validate_env(items: &[AppEnvVarInput]) -> Result<(), ApiError> {
    if items.len() > 200 {
        return Err(ApiError::validation(
            "an app may have at most 200 environment variables",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for item in items {
        if !valid_env_name(&item.name) {
            return Err(ApiError::validation(format!(
                "environment variable name {:?} is not valid",
                item.name
            )));
        }
        if RESERVED_ENV_NAMES.contains(&item.name.as_str()) {
            return Err(ApiError::validation(format!(
                "environment variable {:?} is reserved: the node sets it",
                item.name
            )));
        }
        if !seen.insert(item.name.as_str()) {
            return Err(ApiError::validation(format!(
                "environment variable {:?} is listed twice",
                item.name
            )));
        }
        if item.value.len() > 32 * 1024 {
            return Err(ApiError::validation(format!(
                "environment variable {:?} is over 32 KiB",
                item.name
            )));
        }
    }
    Ok(())
}

async fn replace_app_env_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    body: &AppEnvReplaceRequest,
    params: &Value,
) -> Result<(Vec<AppEnvVar>, Vec<String>), ApiError> {
    validate_env(&body.items)?;
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if exists.is_none() {
        return Err(ApiError::not_found("app"));
    }
    let existing: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM app_env WHERE app_id = ? ORDER BY name")
            .bind(app_id)
            .fetch_all(&mut *w.tx)
            .await?;
    let existing: std::collections::BTreeSet<String> =
        existing.into_iter().map(|(name,)| name).collect();

    let actor = ctx.identity.actor();
    for name in &existing {
        if !body.items.iter().any(|item| &item.name == name) {
            sqlx::query("DELETE FROM app_env WHERE app_id = ? AND name = ?")
                .bind(app_id)
                .bind(name)
                .execute(&mut *w.tx)
                .await?;
            changelog::append(
                &mut w.tx,
                &actor,
                "app_env",
                &format!("{app_id}/{name}"),
                Op::Delete,
                json!({ "app_id": app_id, "name": name }),
            )
            .await?;
        }
    }
    sqlx::query("DELETE FROM app_env WHERE app_id = ?")
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;

    let now = now_rfc3339();
    let mut items = Vec::with_capacity(body.items.len());
    // `body.items` order is kept for insertion; the response is sorted by name.
    for item in &body.items {
        let value_enc =
            crate::app_env::encrypt_value(&state.data_key, app_id, &item.name, &item.value);
        sqlx::query(
            "INSERT INTO app_env (app_id, name, value_enc, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(app_id)
        .bind(&item.name)
        .bind(&value_enc)
        .bind(&now)
        .execute(&mut *w.tx)
        .await?;
        // The change log only ever carries the ciphertext, never the value.
        changelog::append(
            &mut w.tx,
            &actor,
            "app_env",
            &format!("{app_id}/{}", item.name),
            if existing.contains(&item.name) {
                Op::Update
            } else {
                Op::Create
            },
            json!({
                "app_id": app_id,
                "name": item.name,
                "value_enc": hex(&value_enc),
                "updated_at": now,
            }),
        )
        .await?;
        items.push(AppEnvVar {
            app_id: app_id.to_string(),
            name: item.name.clone(),
            updated_at: now.clone(),
        });
    }
    items.sort_by(|a, b| a.name.cmp(&b.name));

    let vm_ids = app_vm_ids(&mut w.tx, app_id).await?;
    let target = app_target(app_id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppUpdate),
        Some(&target),
        None,
        params,
        None,
    )
    .await?;
    w.commit().await?;
    Ok((items, vm_ids))
}

/// Routes of apps, deployments and domains.
pub fn routes() -> Router<AppState> {
    use axum::routing::{get, post};

    Router::new()
        .route("/api/v1/apps", get(list_apps).post(create_app))
        .route(
            "/api/v1/apps/{app_id}",
            get(get_app).patch(update_app).delete(delete_app),
        )
        .route("/api/v1/apps/{app_id}/restart", post(restart_app))
        .route("/api/v1/apps/{app_id}/redeploy", post(redeploy_app))
        .route("/api/v1/apps/{app_id}/start", post(start_app))
        .route("/api/v1/apps/{app_id}/stop", post(stop_app))
        .route(
            "/api/v1/apps/{app_id}/env",
            get(get_app_env).put(replace_app_env),
        )
        .route(
            "/api/v1/apps/{app_id}/deployments",
            get(super::deployments::list_app_deployments)
                .post(super::deployments::create_deployment),
        )
        .route(
            "/api/v1/deployments/{deployment_id}",
            get(super::deployments::get_deployment),
        )
        .route(
            "/api/v1/apps/{app_id}/domains",
            get(super::domains::list_app_domains).post(super::domains::create_app_domain),
        )
        .route(
            "/api/v1/domains/{domain_id}",
            axum::routing::delete(super::domains::delete_domain),
        )
}

/// OpenAPI paths and schemas of apps, deployments and domains.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        list_apps,
        create_app,
        get_app,
        update_app,
        delete_app,
        restart_app,
        redeploy_app,
        start_app,
        stop_app,
        get_app_env,
        replace_app_env,
        super::deployments::list_app_deployments,
        super::deployments::create_deployment,
        super::deployments::get_deployment,
        super::domains::list_app_domains,
        super::domains::create_app_domain,
        super::domains::delete_domain,
    ),
    components(
        schemas(
            App,
            AppDetail,
            EdgeBackend,
            AppList,
            CreateAppRequest,
            UpdateAppRequest,
            AppSource,
            BuildArg,
            HealthCheck,
            AppResources,
            AppPlacement,
            AppEnvVar,
            AppEnvVarInput,
            AppEnvList,
            AppEnvReplaceRequest,
            Deployment,
            super::deployments::DeploymentList,
            super::deployments::CreateDeploymentRequest,
            Domain,
            super::domains::DomainList,
            super::domains::CreateDomainRequest,
            Vm,
            crate::error::Problem,
            crate::error::Target,
        ),
        responses(
            crate::openapi::Unauthenticated,
            crate::openapi::Forbidden,
            crate::openapi::ApprovalRequired,
            crate::openapi::NotFound,
            crate::openapi::Conflict,
            crate::openapi::ValidationFailed,
            crate::openapi::NotPrimary,
            crate::openapi::RateLimited,
        )
    )
)]
pub struct AppsDoc;

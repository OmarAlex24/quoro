// SPDX-License-Identifier: AGPL-3.0-only
//! Builds of an app's Git source (ADR 0117, ADR 0118, ADR 0119).
//!
//! # Model
//!
//! - A build is one row of `builds`, replicated through the change log like a
//!   deployment, holding the snapshot of the source it started from. The log
//!   stays on the node that ran the build and is read on demand (ADR 0014).
//! - `start_build` queues a build on the node that runs the app, or on the
//!   node the scheduler picks ([`crate::deploy::pick_node`]).
//! - `apply_build_status` moves the row from what a node reports. A success
//!   also sets the app's image, starts the `trigger: build` deployment of the
//!   built image and emits both events in one transaction, so the node's next
//!   snapshot carries the VM in the same revision that drops the build.
//! - `cancel_build` cancels a `queued` build at once and otherwise records the
//!   durable marker `error = 'cancel requested'`, which the desired state
//!   turns into `spec.builds[].cancelled` (the contract has no column for it).

use crate::api::apps::{AppPlacement, AppSource};
use crate::api::builds::{Build, BuildFailedStep, BuildPhase, BuildStatus, BuildTrigger};
use crate::changelog::{self, Actor, ActorType, Op};
use crate::deploy;
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::store::{Store, now_rfc3339};
use quoro_proto::wire;
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};
use std::collections::BTreeSet;

/// Columns of [`Build`], in the order [`build_from_row`] expects them.
pub(crate) const BUILD_COLUMNS: &str = "id, app_id, node_id, source, status, \"commit\", image, \
     image_digest, failed_phase, error, failed_step, deployment_id, \"trigger\", refresh_base, \
     created_by_type, created_by_id, created_at, started_at, finished_at";

/// The durable marker of a cancel that arrived while the build was running.
pub const CANCEL_REQUESTED: &str = "cancel requested";

/// The prefix of the reference a build produces (`README.md` "Built images").
pub const BUILT_IMAGE_PREFIX: &str = "build.quoro.invalid/";

/// The subsystem recorded as the actor of observation-driven build changes.
fn build_actor() -> Actor {
    Actor::system("build")
}

/// The reference of the image a build produces (`README.md` "Built images").
#[must_use]
pub fn built_image_reference(app_id: &str, build_id: &str) -> String {
    format!("{BUILT_IMAGE_PREFIX}{app_id}:{build_id}")
}

/// The build id of a built image's reference, or `None` for any other image.
#[must_use]
pub fn build_id_of_image(image: &str) -> Option<&str> {
    let rest = image.strip_prefix(BUILT_IMAGE_PREFIX)?;
    let (_, build_id) = rest.rsplit_once(':')?;
    deploy::is_id_with_prefix("bld", build_id).then_some(build_id)
}

/// The creator kind of a build, from the change-log actor.
fn creator_type(actor: &Actor) -> crate::api::deployments::CreatedByType {
    match actor.kind {
        ActorType::User => crate::api::deployments::CreatedByType::User,
        ActorType::Agent => crate::api::deployments::CreatedByType::Agent,
        ActorType::Node | ActorType::System => crate::api::deployments::CreatedByType::System,
    }
}

/// Whether `name` matches `^[A-Za-z_][A-Za-z0-9_]*$`.
fn valid_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return false,
    }
    bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// A path of a Git source that stays inside the checkout: relative, without
/// a `..` segment and without a NUL byte.
fn validate_relative_path(what: &str, path: &str) -> Result<(), ApiError> {
    if path.is_empty() {
        return Err(ApiError::validation(format!("{what} must not be empty")));
    }
    if path.chars().count() > 500 {
        return Err(ApiError::validation(format!(
            "{what} is longer than 500 characters"
        )));
    }
    if path.starts_with('/') {
        return Err(ApiError::validation(format!("{what} must be relative")));
    }
    if path.contains('\0') {
        return Err(ApiError::validation(format!("{what} must not contain NUL")));
    }
    if path.split('/').any(|segment| segment == "..") {
        return Err(ApiError::validation(format!(
            "{what} must stay inside the build context"
        )));
    }
    Ok(())
}

/// The static validation of an `AppSource`, with its defaults filled in.
///
/// # Errors
///
/// `validation_failed` for a value the contract rejects.
pub(crate) fn validate_source(source: &AppSource) -> Result<AppSource, ApiError> {
    if !source.repo_url.starts_with("https://") {
        return Err(ApiError::validation(
            "source.repo_url must be an https:// URL",
        ));
    }
    if source.repo_url.chars().count() > 2000 {
        return Err(ApiError::validation(
            "source.repo_url is longer than 2000 characters",
        ));
    }
    if source.repo_url.chars().any(char::is_whitespace) {
        return Err(ApiError::validation(
            "source.repo_url must not contain whitespace",
        ));
    }
    let ref_len = source.git_ref.chars().count();
    if ref_len == 0 || ref_len > 255 {
        return Err(ApiError::validation(
            "source.ref must be 1 to 255 characters",
        ));
    }
    let dockerfile_path = source
        .dockerfile_path
        .clone()
        .unwrap_or_else(|| "Dockerfile".to_string());
    validate_relative_path("source.dockerfile_path", &dockerfile_path)?;
    let context_dir = source
        .context_dir
        .clone()
        .unwrap_or_else(|| ".".to_string());
    validate_relative_path("source.context_dir", &context_dir)?;
    if let Some(target) = &source.target
        && target.chars().count() > 128
    {
        return Err(ApiError::validation(
            "source.target is longer than 128 characters",
        ));
    }
    if source.build_args.len() > 100 {
        return Err(ApiError::validation(
            "source.build_args has more than 100 entries",
        ));
    }
    let mut names = BTreeSet::new();
    for arg in &source.build_args {
        if !valid_name(&arg.name) {
            return Err(ApiError::validation(format!(
                "source.build_args name {:?} is not valid",
                arg.name
            )));
        }
        if !names.insert(arg.name.as_str()) {
            return Err(ApiError::validation(format!(
                "source.build_args name {:?} is listed twice",
                arg.name
            )));
        }
        if arg.value.chars().count() > 4096 {
            return Err(ApiError::validation(format!(
                "source.build_args value of {:?} is longer than 4096 characters",
                arg.name
            )));
        }
    }
    if source.secrets.len() > 50 {
        return Err(ApiError::validation(
            "source.secrets has more than 50 entries",
        ));
    }
    let mut secrets = BTreeSet::new();
    for name in &source.secrets {
        if !valid_name(name) {
            return Err(ApiError::validation(format!(
                "source.secrets name {name:?} is not valid"
            )));
        }
        if !secrets.insert(name.as_str()) {
            return Err(ApiError::validation(format!(
                "source.secrets name {name:?} is listed twice"
            )));
        }
    }
    let mut normalized = source.clone();
    normalized.dockerfile_path = Some(dockerfile_path);
    normalized.context_dir = Some(context_dir);
    normalized.target = source.target.clone().filter(|t| !t.is_empty());
    Ok(normalized)
}

/// The stored JSON of an `AppSource`.
///
/// # Errors
///
/// An internal error when the source cannot be serialized.
pub(crate) fn source_json(source: &AppSource) -> Result<String, ApiError> {
    serde_json::to_string(source)
        .map_err(|error| ApiError::internal(&format!("source snapshot: {error}")))
}

/// Builds a [`Build`] from a `builds` row. `error` is reported only for a
/// failure: the cancel marker of a running build is internal.
///
/// # Errors
///
/// An internal error for a value outside the contract.
pub(crate) fn build_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Build, ApiError> {
    let status: String = row.try_get("status")?;
    let status = BuildStatus::parse(&status)
        .ok_or_else(|| ApiError::internal(&format!("unknown build status {status:?}")))?;
    let failed_phase: Option<String> = row.try_get("failed_phase")?;
    let failed_phase = match failed_phase {
        Some(phase) => Some(
            BuildPhase::parse(&phase)
                .ok_or_else(|| ApiError::internal(&format!("unknown build phase {phase:?}")))?,
        ),
        None => None,
    };
    let failed_step: Option<String> = row.try_get("failed_step")?;
    let failed_step = match failed_step {
        Some(step) => Some(
            serde_json::from_str(&step)
                .map_err(|error| ApiError::internal(&format!("stored failed_step: {error}")))?,
        ),
        None => None,
    };
    let created_by: String = row.try_get("created_by_type")?;
    let created_by_type = crate::api::deployments::CreatedByType::parse(&created_by)
        .ok_or_else(|| ApiError::internal(&format!("unknown creator type {created_by:?}")))?;
    let trigger: String = row.try_get("trigger")?;
    let trigger = BuildTrigger::parse(&trigger)
        .ok_or_else(|| ApiError::internal(&format!("unknown build trigger {trigger:?}")))?;
    let source: String = row.try_get("source")?;
    let source: AppSource = serde_json::from_str(&source)
        .map_err(|error| ApiError::internal(&format!("stored source: {error}")))?;
    let error: Option<String> = row.try_get("error")?;
    Ok(Build {
        id: row.try_get("id")?,
        app_id: row.try_get("app_id")?,
        node_id: row.try_get("node_id")?,
        source,
        status,
        commit: row.try_get("commit")?,
        image: row.try_get("image")?,
        image_digest: row.try_get("image_digest")?,
        failed_phase,
        error: (status == BuildStatus::Failed).then_some(error).flatten(),
        failed_step,
        deployment_id: row.try_get("deployment_id")?,
        trigger,
        refresh_base: row.try_get("refresh_base")?,
        created_by_type,
        created_by_id: row.try_get("created_by_id")?,
        created_at: row.try_get("created_at")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

/// Appends the change-log entry of a build change, with the row's stored
/// `error` (the cancel marker is not part of the API shape).
async fn log_build(
    conn: &mut SqliteConnection,
    build: &Build,
    error: Option<&str>,
    op: Op,
) -> Result<i64, ApiError> {
    let mut payload = serde_json::to_value(build)
        .map_err(|error| ApiError::internal(&format!("build payload: {error}")))?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "error".to_string(),
            error.map_or(Value::Null, |error| Value::String(error.to_string())),
        );
    }
    Ok(changelog::append(conn, &build_actor(), "build", &build.id, op, payload).await?)
}

/// The fields of an app a build needs.
struct AppBuildInfo {
    project_id: String,
    source: Option<AppSource>,
    placement: AppPlacement,
}

/// Loads the fields of `app_id` a build needs.
async fn load_app(conn: &mut SqliteConnection, app_id: &str) -> Result<AppBuildInfo, ApiError> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let row = sqlx::query("SELECT project_id, source, placement FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let source: Option<String> = row.try_get("source")?;
    let source = match source {
        Some(source) => Some(
            serde_json::from_str(&source)
                .map_err(|error| ApiError::internal(&format!("stored source: {error}")))?,
        ),
        None => None,
    };
    let placement: String = row.try_get("placement")?;
    let placement = serde_json::from_str(&placement)
        .map_err(|error| ApiError::internal(&format!("stored placement: {error}")))?;
    Ok(AppBuildInfo {
        project_id: row.try_get("project_id")?,
        source,
        placement,
    })
}

/// The node a build runs on: the node that runs the app's current VM, or the
/// node the deployment scheduler picks.
async fn build_node(
    conn: &mut SqliteConnection,
    app_id: &str,
    placement: &AppPlacement,
) -> Result<String, ApiError> {
    let current: Option<(String,)> = sqlx::query_as(
        "SELECT v.node_id FROM vms v JOIN nodes n ON n.id = v.node_id \
         WHERE v.app_id = ? AND n.status != 'removed' ORDER BY v.id DESC LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    if let Some((node_id,)) = current {
        return Ok(node_id);
    }
    deploy::pick_node(conn, placement).await
}

/// The build [`start_build`] wrote.
pub(crate) struct StartedBuild {
    /// The new build, `queued` or, when a secret is missing, `failed`.
    pub build: Build,
    /// Project of the app, for the audit record.
    pub project_id: String,
    /// Change-log `seq` of the build's create entry.
    pub change_seq: i64,
}

/// Queues a build of `app_id`'s Git source in the caller's write transaction,
/// with its change-log entry and its events.
///
/// # Errors
///
/// `not_found` for an unknown app, `conflict` when the app has no Git source
/// or a build that is not terminal, `validation_failed` for a bad `ref` or when
/// no node is available, or a database error.
pub(crate) async fn start_build(
    conn: &mut SqliteConnection,
    app_id: &str,
    git_ref: Option<&str>,
    refresh_base: bool,
    actor: &Actor,
) -> Result<StartedBuild, ApiError> {
    let app = load_app(conn, app_id).await?;
    let Some(mut source) = app.source else {
        return Err(ApiError::conflict("the app has no Git source"));
    };
    if let Some(git_ref) = git_ref {
        let len = git_ref.chars().count();
        if len == 0 || len > 255 {
            return Err(ApiError::validation("ref must be 1 to 255 characters"));
        }
        source.git_ref = git_ref.to_string();
    }

    let running: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM builds WHERE app_id = ? \
         AND status NOT IN ('succeeded', 'failed', 'cancelled') LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    if running.is_some() {
        return Err(ApiError::conflict(
            "the app already has a build that is not finished",
        ));
    }

    let node_id = build_node(conn, app_id, &app.placement).await?;

    let names: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM app_env WHERE app_id = ? ORDER BY name")
            .bind(app_id)
            .fetch_all(&mut *conn)
            .await?;
    let names: BTreeSet<String> = names.into_iter().map(|(name,)| name).collect();
    let missing = source
        .secrets
        .iter()
        .find(|name| !names.contains(*name))
        .cloned();

    let now = now_rfc3339();
    let failure = missing
        .as_ref()
        .map(|name| format!("secret {name} is not an environment variable of the app"));
    let build = Build {
        id: deploy::new_id_with_prefix("bld"),
        app_id: app_id.to_string(),
        node_id,
        source: source.clone(),
        status: if failure.is_some() {
            BuildStatus::Failed
        } else {
            BuildStatus::Queued
        },
        commit: None,
        image: None,
        image_digest: None,
        failed_phase: failure.as_ref().map(|_| BuildPhase::Preparing),
        error: failure.clone(),
        failed_step: None,
        deployment_id: None,
        trigger: BuildTrigger::Api,
        refresh_base,
        created_by_type: creator_type(actor),
        created_by_id: actor.id.clone(),
        created_at: now.clone(),
        started_at: None,
        finished_at: failure.as_ref().map(|_| now.clone()),
    };

    sqlx::query(
        "INSERT INTO builds (id, app_id, node_id, source, status, \"commit\", image, image_digest, \
         failed_phase, error, failed_step, deployment_id, \"trigger\", refresh_base, \
         created_by_type, created_by_id, created_at, started_at, finished_at) \
         VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, NULL, ?)",
    )
    .bind(&build.id)
    .bind(&build.app_id)
    .bind(&build.node_id)
    .bind(source_json(&build.source)?)
    .bind(build.status.as_str())
    .bind(build.failed_phase.map(BuildPhase::as_str))
    .bind(&build.error)
    .bind(build.trigger.as_str())
    .bind(build.refresh_base)
    .bind(build.created_by_type.as_str())
    .bind(&build.created_by_id)
    .bind(&build.created_at)
    .bind(&build.finished_at)
    .execute(&mut *conn)
    .await?;
    let change_seq = log_build(conn, &build, build.error.as_deref(), Op::Create).await?;

    events::emit(
        conn,
        NewEvent {
            kind: "build.started",
            source: Source::ControlPlane,
            subject_type: "build",
            subject_id: &build.id,
            project_id: Some(&app.project_id),
            request_id: None,
            actor: Some(actor),
            data: json!({
                "build_id": build.id,
                "app_id": build.app_id,
                "node_id": build.node_id,
                "repo_url": build.source.repo_url,
                "ref": build.source.git_ref,
            }),
        },
    )
    .await?;
    if let Some(error) = &build.error {
        events::emit(
            conn,
            NewEvent {
                kind: "build.failed",
                source: Source::ControlPlane,
                subject_type: "build",
                subject_id: &build.id,
                project_id: Some(&app.project_id),
                request_id: None,
                actor: Some(actor),
                data: json!({
                    "build_id": build.id,
                    "app_id": build.app_id,
                    "phase": BuildPhase::Preparing.as_str(),
                    "error": error,
                }),
            },
        )
        .await?;
    }

    Ok(StartedBuild {
        build,
        project_id: app.project_id,
        change_seq,
    })
}

/// The build [`cancel_build`] recorded.
pub(crate) struct CancelledBuild {
    /// The build as recorded, which may still be running.
    pub build: Build,
    /// Project of the app, for the audit record.
    pub project_id: String,
    /// Change-log `seq` of the update, when anything changed.
    pub change_seq: Option<i64>,
    /// Whether the node's desired state has to be rebuilt.
    pub notify: bool,
}

/// Asks for a build to stop, in the caller's write transaction.
///
/// A `queued` build becomes `cancelled` at once and emits `build.cancelled`;
/// a running one gets the durable marker [`CANCEL_REQUESTED`], which
/// `spec.builds[].cancelled` carries. A build that is already cancelled is
/// left alone, so a repeated cancel is harmless.
///
/// # Errors
///
/// `not_found` for an unknown build, `conflict` when it already finished, or a
/// database error.
pub(crate) async fn cancel_build(
    conn: &mut SqliteConnection,
    build_id: &str,
    actor: &Actor,
) -> Result<CancelledBuild, ApiError> {
    if !deploy::is_id_with_prefix("bld", build_id) {
        return Err(ApiError::not_found("build"));
    }
    let row = sqlx::query(&format!("SELECT {BUILD_COLUMNS} FROM builds WHERE id = ?"))
        .bind(build_id)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| ApiError::not_found("build"))?;
    let build = build_from_row(&row)?;
    let raw_error: Option<String> = row.try_get("error")?;
    let project_id: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
        .bind(&build.app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let project_id = project_id
        .map(|(project_id,)| project_id)
        .ok_or_else(|| ApiError::not_found("app"))?;

    match build.status {
        BuildStatus::Succeeded | BuildStatus::Failed => {
            Err(ApiError::conflict("the build already finished"))
        }
        BuildStatus::Cancelled => Ok(CancelledBuild {
            build,
            project_id,
            change_seq: None,
            notify: false,
        }),
        BuildStatus::Queued => {
            let now = now_rfc3339();
            let mut cancelled = build.clone();
            cancelled.status = BuildStatus::Cancelled;
            cancelled.finished_at = Some(now.clone());
            sqlx::query("UPDATE builds SET status = ?, finished_at = ? WHERE id = ?")
                .bind(cancelled.status.as_str())
                .bind(&cancelled.finished_at)
                .bind(&cancelled.id)
                .execute(&mut *conn)
                .await?;
            let change_seq = log_build(conn, &cancelled, None, Op::Update).await?;
            events::emit(
                conn,
                NewEvent {
                    kind: "build.cancelled",
                    source: Source::ControlPlane,
                    subject_type: "build",
                    subject_id: &cancelled.id,
                    project_id: Some(&project_id),
                    request_id: None,
                    actor: Some(actor),
                    data: json!({
                        "build_id": cancelled.id,
                        "app_id": cancelled.app_id,
                    }),
                },
            )
            .await?;
            Ok(CancelledBuild {
                build: cancelled,
                project_id,
                change_seq: Some(change_seq),
                notify: true,
            })
        }
        _ => {
            if raw_error.as_deref() == Some(CANCEL_REQUESTED) {
                return Ok(CancelledBuild {
                    build,
                    project_id,
                    change_seq: None,
                    notify: false,
                });
            }
            sqlx::query("UPDATE builds SET error = ? WHERE id = ?")
                .bind(CANCEL_REQUESTED)
                .bind(&build.id)
                .execute(&mut *conn)
                .await?;
            let change_seq = changelog::append(
                conn,
                &build_actor(),
                "build",
                &build.id,
                Op::Update,
                json!({ "id": build.id, "error": CANCEL_REQUESTED }),
            )
            .await?;
            Ok(CancelledBuild {
                build,
                project_id,
                change_seq: Some(change_seq),
                notify: true,
            })
        }
    }
}

/// Whether `digest` is a manifest digest, `sha256:` and 64 lowercase hex
/// characters.
fn valid_digest(digest: &str) -> bool {
    match digest.strip_prefix("sha256:") {
        Some(hex) => {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }
        None => false,
    }
}

/// Whether `commit` is a full lowercase hex commit SHA.
fn valid_commit(commit: &str) -> bool {
    commit.len() == 40
        && commit
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The stored `failed_step` JSON of a reported step.
fn failed_step_json(step: &wire::FailedStep) -> Result<String, ApiError> {
    let step = BuildFailedStep {
        vertex: i64::from(step.vertex),
        name: step.name.chars().take(500).collect(),
        error: step.error.chars().take(2000).collect(),
        dockerfile_line: step.dockerfile_line.map(i64::from),
        excerpt: step
            .excerpt
            .iter()
            .take(20)
            .map(|line| line.chars().take(500).collect())
            .collect(),
    };
    serde_json::to_string(&step)
        .map_err(|error| ApiError::internal(&format!("failed_step: {error}")))
}

/// Applies one `builds[]` entry of a `status_report` from `node_id` to the
/// build's row, emitting the terminal event. Returns whether the node's
/// desired state has to be rebuilt: when the build ends, and when it first
/// starts, because a newer build that started replaces the failed build the
/// desired state kept for its log (`session::desired`).
///
/// Unknown builds, builds of another node and builds that are already terminal
/// are ignored, so a redelivered report changes nothing.
///
/// # Errors
///
/// A database error.
pub(crate) async fn apply_build_status(
    store: &Store,
    node_id: &str,
    report: &wire::BuildStatus,
) -> Result<bool, ApiError> {
    if !deploy::is_id_with_prefix("bld", &report.build_id) {
        return Ok(false);
    }
    let mut w = store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {BUILD_COLUMNS} FROM builds WHERE id = ?"))
        .bind(&report.build_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    let Some(row) = row else {
        w.commit().await?;
        return Ok(false);
    };
    let before = build_from_row(&row)?;
    let raw_error: Option<String> = row.try_get("error")?;
    if before.node_id != node_id || before.status.is_terminal() {
        w.commit().await?;
        return Ok(false);
    }
    // A cancel that is still waiting for the node survives a status change.
    let cancel_requested = raw_error.as_deref() == Some(CANCEL_REQUESTED);

    let now = now_rfc3339();
    let mut after = before.clone();
    if let Some(commit) = &report.commit
        && valid_commit(commit)
    {
        after.commit = Some(commit.clone());
    }
    // The node's own timestamps win when it gives them.
    if report.started_at.is_some() {
        after.started_at.clone_from(&report.started_at);
    } else if after.started_at.is_none() && report.status != wire::BuildState::Queued {
        after.started_at = Some(now.clone());
    }

    // A success carries the built image, the app's new image and the
    // deployment it starts. The contract requires the manifest digest and the
    // commit in the build row and in `build.succeeded`, so a success without
    // either is a failure to import the layout into a root disk.
    if report.status == wire::BuildState::Succeeded {
        let digest = report
            .image_digest
            .clone()
            .filter(|digest| valid_digest(digest));
        match (digest, after.commit.clone()) {
            (Some(digest), Some(_)) => {
                after.status = BuildStatus::Succeeded;
                after.image = Some(built_image_reference(&after.app_id, &report.build_id));
                after.image_digest = Some(digest);
                after.finished_at = Some(report.finished_at.clone().unwrap_or_else(|| now.clone()));
                succeed_build(&mut w.tx, &before, &after, cancel_requested).await?;
                w.commit().await?;
                return Ok(true);
            }
            (digest, _) => {
                after.status = BuildStatus::Failed;
                after.failed_phase = Some(BuildPhase::Importing);
                after.error = Some(
                    if digest.is_none() {
                        "the node reported a success without an image digest"
                    } else {
                        "the node reported a success without a commit"
                    }
                    .to_string(),
                );
            }
        }
    } else {
        after.status = BuildStatus::parse(report.status.as_str()).ok_or_else(|| {
            ApiError::internal(&format!("unknown build status {:?}", report.status))
        })?;
    }

    match after.status {
        BuildStatus::Failed => {
            // The CHECK on `builds` ties a failure to a phase.
            after.failed_phase = Some(
                report
                    .failed_phase
                    .map(BuildPhase::from)
                    .or_else(|| before.status.phase())
                    .unwrap_or(BuildPhase::Preparing),
            );
            after.error = Some(
                report
                    .error
                    .clone()
                    .unwrap_or_else(|| "the build failed".to_string())
                    .chars()
                    .take(2000)
                    .collect(),
            );
            after.failed_step = match &report.failed_step {
                Some(step) => Some(
                    serde_json::from_str(&failed_step_json(step)?)
                        .map_err(|error| ApiError::internal(&format!("failed_step: {error}")))?,
                ),
                None => None,
            };
            after.finished_at = Some(report.finished_at.clone().unwrap_or_else(|| now.clone()));
        }
        BuildStatus::Cancelled => {
            after.finished_at = Some(report.finished_at.clone().unwrap_or_else(|| now.clone()));
        }
        _ => {}
    }

    save_build(&mut w.tx, &before, &after, cancel_requested).await?;
    match after.status {
        BuildStatus::Failed => {
            let phase = after.failed_phase.unwrap_or(BuildPhase::Preparing);
            let mut data = json!({
                "build_id": after.id,
                "app_id": after.app_id,
                "phase": phase.as_str(),
                "error": after.error,
            });
            if let Some(commit) = &after.commit {
                data["commit"] = json!(commit);
            }
            if let Some(step) = &after.failed_step {
                // The event schema knows `dockerfile_line` as an integer, so
                // an absent line is left out rather than sent as null.
                let mut value = json!({
                    "vertex": step.vertex,
                    "name": step.name,
                    "error": step.error,
                    "excerpt": step.excerpt,
                });
                if let Some(line) = step.dockerfile_line {
                    value["dockerfile_line"] = json!(line);
                }
                data["failed_step"] = value;
            }
            events::emit(
                &mut w.tx,
                NewEvent {
                    kind: "build.failed",
                    source: Source::ControlPlane,
                    subject_type: "build",
                    subject_id: &after.id,
                    project_id: None,
                    request_id: None,
                    actor: None,
                    data,
                },
            )
            .await?;
        }
        BuildStatus::Cancelled => {
            let mut data = json!({
                "build_id": after.id,
                "app_id": after.app_id,
            });
            if let Some(phase) = before.status.phase() {
                data["phase"] = json!(phase.as_str());
            }
            events::emit(
                &mut w.tx,
                NewEvent {
                    kind: "build.cancelled",
                    source: Source::ControlPlane,
                    subject_type: "build",
                    subject_id: &after.id,
                    project_id: None,
                    request_id: None,
                    actor: None,
                    data,
                },
            )
            .await?;
        }
        _ => {}
    }
    w.commit().await?;
    Ok(after.status.is_terminal() || (before.started_at.is_none() && after.started_at.is_some()))
}

/// Writes a build row's mutable fields and its change-log entry. A cancel
/// marker set by [`cancel_build`] is kept while the build is not terminal.
async fn save_build(
    conn: &mut SqliteConnection,
    before: &Build,
    after: &Build,
    cancel_requested: bool,
) -> Result<bool, ApiError> {
    let stored_error = if after.status == BuildStatus::Failed {
        after.error.clone()
    } else if cancel_requested && !after.status.is_terminal() {
        Some(CANCEL_REQUESTED.to_string())
    } else {
        None
    };
    if before == after {
        return Ok(false);
    }
    let failed_step = match &after.failed_step {
        Some(step) => Some(
            serde_json::to_string(step)
                .map_err(|error| ApiError::internal(&format!("failed_step: {error}")))?,
        ),
        None => None,
    };
    sqlx::query(
        "UPDATE builds SET status = ?, \"commit\" = ?, image = ?, image_digest = ?, \
         failed_phase = ?, error = ?, failed_step = ?, started_at = ?, finished_at = ? \
         WHERE id = ?",
    )
    .bind(after.status.as_str())
    .bind(&after.commit)
    .bind(&after.image)
    .bind(&after.image_digest)
    .bind(after.failed_phase.map(BuildPhase::as_str))
    .bind(&stored_error)
    .bind(failed_step)
    .bind(&after.started_at)
    .bind(&after.finished_at)
    .bind(&after.id)
    .execute(&mut *conn)
    .await?;
    log_build(conn, after, stored_error.as_deref(), Op::Update).await?;
    Ok(true)
}

/// Records a successful build: its row, the app's image, the deployment it
/// starts and both events, in one transaction. `after` already carries the
/// status, the image, its digest, the commit and the finish time.
async fn succeed_build(
    conn: &mut SqliteConnection,
    before: &Build,
    after: &Build,
    cancel_requested: bool,
) -> Result<(), ApiError> {
    let image = after
        .image
        .clone()
        .ok_or_else(|| ApiError::internal(&"a success carries an image"))?;
    let digest = after
        .image_digest
        .clone()
        .ok_or_else(|| ApiError::internal(&"a success carries an image digest"))?;
    let commit = after
        .commit
        .clone()
        .ok_or_else(|| ApiError::internal(&"a success carries a commit"))?;
    let now = after.finished_at.clone().unwrap_or_else(now_rfc3339);
    save_build(conn, before, after, cancel_requested).await?;

    let row = sqlx::query("SELECT project_id, image FROM apps WHERE id = ?")
        .bind(&before.app_id)
        .fetch_one(&mut *conn)
        .await?;
    let project_id: String = row.try_get("project_id")?;
    let old_image: String = row.try_get("image")?;
    if old_image != image {
        sqlx::query("UPDATE apps SET image = ?, updated_at = ? WHERE id = ?")
            .bind(&image)
            .bind(&now)
            .bind(&before.app_id)
            .execute(&mut *conn)
            .await?;
        changelog::append(
            conn,
            &build_actor(),
            "app",
            &before.app_id,
            Op::Update,
            json!({ "id": before.app_id, "image": image, "updated_at": now }),
        )
        .await?;
    }

    // The deployment of the built image runs on the build's node, with the
    // digest the build produced: no registry is contacted. Its own events are
    // emitted after `build.succeeded`, as ADR 0117 orders them.
    let created = deploy::create_deployment_with(
        conn,
        &before.app_id,
        &image,
        crate::api::deployments::DeploymentTrigger::Build,
        &build_actor(),
        &deploy::DeploymentTarget {
            node_id: Some(&before.node_id),
            image_digest: Some(&digest),
        },
        false,
    )
    .await?;
    sqlx::query("UPDATE builds SET deployment_id = ? WHERE id = ?")
        .bind(&created.deployment.id)
        .bind(&before.id)
        .execute(&mut *conn)
        .await?;
    changelog::append(
        conn,
        &build_actor(),
        "build",
        &before.id,
        Op::Update,
        json!({ "id": before.id, "deployment_id": created.deployment.id }),
    )
    .await?;

    events::emit(
        conn,
        NewEvent {
            kind: "build.succeeded",
            source: Source::ControlPlane,
            subject_type: "build",
            subject_id: &before.id,
            project_id: Some(&project_id),
            request_id: None,
            actor: None,
            data: json!({
                "build_id": before.id,
                "app_id": before.app_id,
                "commit": commit,
                "image": image,
                "image_digest": digest,
                "deployment_id": created.deployment.id,
            }),
        },
    )
    .await?;
    deploy::emit_created_events(conn, &created, &build_actor()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_built_reference_round_trips() {
        let build_id = "bld_00000000000000000000000001";
        let image = built_image_reference("app_00000000000000000000000001", build_id);
        assert_eq!(
            image,
            "build.quoro.invalid/app_00000000000000000000000001:bld_00000000000000000000000001"
        );
        assert_eq!(build_id_of_image(&image), Some(build_id));
        assert_eq!(build_id_of_image("ghcr.io/example/web:1"), None);
        assert_eq!(
            build_id_of_image("build.quoro.invalid/app_x:not-a-build"),
            None
        );
    }

    #[test]
    fn a_digest_and_a_commit_are_checked() {
        assert!(valid_digest(&format!("sha256:{}", "a".repeat(64))));
        assert!(!valid_digest(&format!("sha256:{}", "A".repeat(64))));
        assert!(!valid_digest("sha256:abc"));
        assert!(valid_commit(&"0".repeat(40)));
        assert!(!valid_commit(&"0".repeat(39)));
    }

    #[test]
    fn a_source_path_cannot_leave_the_context() {
        assert!(validate_relative_path("p", "Dockerfile").is_ok());
        assert!(validate_relative_path("p", "docker/Dockerfile").is_ok());
        assert!(validate_relative_path("p", "../Dockerfile").is_err());
        assert!(validate_relative_path("p", "docker/../Dockerfile").is_err());
        assert!(validate_relative_path("p", "/etc/passwd").is_err());
    }

    fn source() -> AppSource {
        AppSource {
            kind: crate::api::apps::AppSourceKind::Git,
            repo_url: "https://github.com/example/blog.git".to_string(),
            git_ref: "main".to_string(),
            dockerfile_path: None,
            context_dir: None,
            target: None,
            build_args: Vec::new(),
            secrets: vec!["TOKEN".to_string()],
        }
    }

    #[test]
    fn a_source_gets_its_defaults() {
        let source = validate_source(&source()).unwrap();
        assert_eq!(source.dockerfile_path.as_deref(), Some("Dockerfile"));
        assert_eq!(source.context_dir.as_deref(), Some("."));
    }

    #[test]
    fn a_source_with_a_duplicate_secret_is_rejected() {
        let mut source = source();
        source.secrets = vec!["TOKEN".to_string(), "TOKEN".to_string()];
        assert!(validate_source(&source).is_err());
    }
}

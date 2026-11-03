//! `GET /api/v1/projects`, `POST /api/v1/projects` and
//! `GET /api/v1/projects/{project_id}`.

use crate::AppState;

use crate::auth::Identity;
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::store::now_rfc3339;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use quoro_types::{Prefix, Scope, new_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// The operation string recorded in the audit log and used as the
/// idempotency scope.
const CREATE_OPERATION: &str = "POST /api/v1/projects";

/// A group of apps; also the unit a resource selector can name.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "Project")]
pub struct Project {
    /// Project id.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Unique slug used in selectors and the CLI.
    #[schema(pattern = "^[a-z0-9-]{1,40}$")]
    pub slug: String,
    /// Display name.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// When the project was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the project was last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

/// One page of projects.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "ProjectList")]
pub struct ProjectList {
    pub items: Vec<Project>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Create a project.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "CreateProjectRequest")]
pub struct CreateProjectRequest {
    /// Unique slug.
    #[schema(pattern = "^[a-z0-9-]{1,40}$")]
    pub slug: String,
    /// Display name.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
}

/// `limit` and `cursor` (`openapi.yaml` `Limit` and `Cursor`).
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PageParams {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl PageParams {
    /// The validated limit.
    pub(crate) fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    /// The cursor, or the empty string, which sorts before every id.
    pub(crate) fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

/// Builds a [`Project`] from a `projects` row.
pub(crate) fn project_from_row(row: &SqliteRow) -> Result<Project, ApiError> {
    Ok(Project {
        id: row.try_get("id")?,
        slug: row.try_get("slug")?,
        name: row.try_get("name")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Whether `slug` matches `^[a-z0-9-]{1,40}$`.
fn valid_slug(slug: &str) -> bool {
    (1..=40).contains(&slug.len())
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Validates the body before it is authorized.
fn validate(body: &CreateProjectRequest) -> Result<(), ApiError> {
    if !valid_slug(&body.slug) {
        return Err(ApiError::validation("slug must match ^[a-z0-9-]{1,40}$"));
    }
    let len = body.name.chars().count();
    if !(1..=200).contains(&len) {
        return Err(ApiError::validation(
            "name must be between 1 and 200 characters",
        ));
    }
    Ok(())
}

/// Projects group apps and are the unit a grant's selector can name by slug
/// (D-0030).
#[utoipa::path(
    get,
    path = "/api/v1/projects",
    tag = "projects",
    operation_id = "listProjects",
    extensions(
        ("x-quoro-scope" = json!("project:read")),
        ("x-quoro-mcp-tool" = json!("list_projects")),
    ),
    params(PageParams),
    responses(
        (status = 200, description = "One page of projects.", body = ProjectList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `project:read`, or a database error.
pub async fn list_projects(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<PageParams>,
) -> Result<Json<ProjectList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/projects", &headers);
    let list = crate::service::projects::list_projects(&state, &ctx, &page).await?;
    Ok(Json(list))
}

/// Creates a project with a unique slug. `Quoro-Justification` is required
/// when the caller is an agent (D-0031).
#[utoipa::path(
    post,
    path = "/api/v1/projects",
    tag = "projects",
    operation_id = "createProject",
    extensions(("x-quoro-scope" = json!("project:write"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident (`inc_...`) the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "The project was created.", body = Project,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `validation_failed` for a bad slug or name, `forbidden` without
/// `project:write`, `conflict` for a duplicate slug, or a database error.
pub async fn create_project(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>), ApiError> {
    validate(&body)?;
    let identity_id = identity.id().to_string();
    let params = json!({ "slug": &body.slug, "name": &body.name });
    // Authorize before claiming the idempotency slot: a refused call must not
    // leave the key in flight.
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    ctx.authorize(&state, Scope::ProjectWrite, None, None, &params)
        .await?;
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(&identity_id, CREATE_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let project: Project = serde_json::from_value(stored)
                    .map_err(|e| ApiError::internal(&format!("replayed project: {e}")))?;
                let target = Target {
                    kind: "project".to_string(),
                    id: project.id.clone(),
                };
                ctx.audit_replay(
                    &state,
                    Scope::ProjectWrite,
                    &target,
                    Some(&project.id),
                    &params,
                )
                .await?;
                return Ok((status, Json(project)));
            }
            Begin::Conflict => return Err(ApiError::conflict("Idempotency-Key reused")),
        }
    }

    match insert_project(&state, &ctx, &body.slug, &body.name, &params).await {
        Ok(project) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&project)
                    .map_err(|e| ApiError::internal(&format!("project serialization: {e}")))?;
                state.idempotency.finish(
                    &identity_id,
                    CREATE_OPERATION,
                    key,
                    StatusCode::CREATED,
                    stored,
                );
            }
            Ok((StatusCode::CREATED, Json(project)))
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(&identity_id, CREATE_OPERATION, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::ProjectWrite),
                None,
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Inserts the project, its change-log entry and the audit record in one
/// transaction.
async fn insert_project(
    state: &AppState,
    ctx: &CallCtx,
    slug: &str,
    name: &str,
    params: &Value,
) -> Result<Project, ApiError> {
    let mut w = state.store.begin_write().await?;
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM projects WHERE slug = ?")
        .bind(slug)
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(format!("slug {slug} is taken")));
    }
    let id = new_id(Prefix::Project);
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(slug)
    .bind(name)
    .bind(&now)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let project = Project {
        id: id.clone(),
        slug: slug.to_string(),
        name: name.to_string(),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let payload = json!({
        "id": project.id,
        "slug": project.slug,
        "name": project.name,
        "created_at": project.created_at,
        "updated_at": project.updated_at,
    });
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "project",
        &id,
        Op::Create,
        payload,
    )
    .await?;
    let target = Target {
        kind: "project".to_string(),
        id: id.clone(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::ProjectWrite),
        Some(&target),
        Some(&id),
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(project)
}

/// One project by id.
#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}",
    tag = "projects",
    operation_id = "getProject",
    extensions(("x-quoro-scope" = json!("project:read"))),
    params(
        ("project_id" = String, Path, description = "Project id (`prj_...`).", pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The project.", body = Project,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `project:read`, `not_found` for an unknown id, or a
/// database error.
pub async fn get_project(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(project_id): Path<String>,
) -> Result<Json<Project>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/projects/{project_id}",
        &headers,
    );
    let project = crate::service::projects::get_project(&state, &ctx, &project_id).await?;
    Ok(Json(project))
}

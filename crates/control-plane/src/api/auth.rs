//! First-run setup and human sessions: `POST /api/v1/setup`,
//! `POST /api/v1/auth/login`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me`.
//!
//! D-0054: setup needs the one-time `Quoro-Setup-Token` and works only while
//! no user exists. permission-model.md section 2: sessions have a 24 h
//! absolute expiry; only the SHA-256 of the cookie secret is stored, and the
//! `sessions` table is local, so login and logout write no change-log entry.

use crate::AppState;
use crate::audit::{self, Channel, Outcome, Record};
use crate::auth::{
    Credential, Identity, Principal, SESSION_COOKIE, SESSION_TTL_SECONDS, password, setup_token,
};
use crate::changelog::{self, Actor, ActorType, Op};
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, request_id_of};
use crate::store::{now_rfc3339, random_secret, rfc3339_in, sha256_hex};
use axum::Json;
use axum::extract::State;
use axum::http::{Extensions, HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use quoro_types::{Prefix, Role, Scope, new_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::borrow::Cow;
use utoipa::ToSchema;
use utoipa::openapi::RefOr;
use utoipa::openapi::schema::{ObjectBuilder, Schema, Type};

// ---------------------------------------------------------------------------
// Schemas (components of the frozen openapi.yaml)
// ---------------------------------------------------------------------------

/// First-run setup. Creates the first user as `owner`.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "SetupRequest")]
pub struct SetupRequest {
    /// Email of the first owner.
    #[schema(format = "email")]
    pub email: String,
    /// Display name of the first owner.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Password of the first owner. Stored as an Argon2id hash; never returned.
    #[schema(format = "password", min_length = 12, write_only)]
    pub password: String,
}

/// Human login with email and password; answers with a session cookie.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "LoginRequest")]
pub struct LoginRequest {
    /// Email of the user.
    #[schema(format = "email")]
    pub email: String,
    /// Password of the user.
    #[schema(format = "password", min_length = 1, write_only)]
    pub password: String,
}

/// Fixed scope bundle of a human.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum UserRole {
    Owner,
    Operator,
    Viewer,
}

impl From<Role> for UserRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Owner => Self::Owner,
            Role::Operator => Self::Operator,
            Role::Viewer => Self::Viewer,
        }
    }
}

/// A human identity. The password hash is never part of it.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "User")]
pub struct User {
    /// User id.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Email, unique in the cluster.
    #[schema(format = "email")]
    pub email: String,
    /// Display name.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Fixed scope bundle.
    #[schema(inline)]
    pub role: UserRole,
    /// A disabled user cannot log in or use tokens.
    pub disabled: bool,
    /// When the user was created.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// Kind of identity.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum IdentityType {
    User,
    Agent,
}

/// The identity behind the credential.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "MeResponse")]
pub struct MeResponse {
    /// Kind of identity.
    #[serde(rename = "type")]
    #[schema(inline)]
    pub kind: IdentityType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub user: Option<User>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub agent: Option<Agent>,
    /// Active grants of the agent; empty for a human.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub grants: Option<Vec<Grant>>,
}

/// An AI identity (D-0029). Defined with the agent operations in
/// [`crate::api::agents`] and re-exported here for [`MeResponse`].
pub use crate::api::agents::Agent;

/// The `Scope` component: one scope string from the README table.
pub struct ScopeSchema;

impl utoipa::PartialSchema for ScopeSchema {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::String)
            .title(Some("Scope"))
            .description(Some("One scope, `<resource>:<verb>` (D-0030)."))
            .enum_values(Some(Scope::ALL.iter().map(|s| s.as_str())))
            .into()
    }
}

impl ToSchema for ScopeSchema {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("Scope")
    }
}

/// The resources a grant's scopes apply to.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Selector")]
pub struct Selector {
    /// Match every resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub all: Option<bool>,
    /// App ids.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false, inline, value_type = Option<Vec<AppIdItem>>)]
    pub apps: Option<Vec<String>>,
    /// Node ids.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false, inline, value_type = Option<Vec<NodeIdItem>>)]
    pub nodes: Option<Vec<String>>,
    /// Project slugs.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false, inline, value_type = Option<Vec<SlugItem>>)]
    pub projects: Option<Vec<String>>,
}

macro_rules! pattern_item {
    ($name:ident, $pattern:literal) => {
        /// A string item with a fixed pattern, inlined into its array.
        pub struct $name;
        impl utoipa::PartialSchema for $name {
            fn schema() -> RefOr<Schema> {
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .pattern(Some($pattern))
                    .into()
            }
        }
        impl ToSchema for $name {}
    };
}

pattern_item!(AppIdItem, "^app_[0-9A-HJKMNP-TV-Z]{26}$");
pattern_item!(NodeIdItem, "^nod_[0-9A-HJKMNP-TV-Z]{26}$");
pattern_item!(SlugItem, "^[a-z0-9-]{1,40}$");

/// What the grant is for.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum GrantKind {
    Standard,
    Incident,
    Approval,
}

/// Grant status.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum GrantStatus {
    Active,
    Expired,
    Revoked,
    Exhausted,
}

/// Kind of identity that created the grant.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum GrantCreator {
    User,
    System,
}

/// Scope plus resource selector plus a mandatory expiry (D-0030). Defined
/// with the grant operations in [`crate::api::grants`] and re-exported here
/// for [`MeResponse`].
pub use crate::api::grants::Grant;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) type UserRow = (String, String, String, String, i64, String);

pub(crate) fn user_from(row: UserRow) -> Result<User, ApiError> {
    let (id, email, name, role, disabled, created_at) = row;
    let role = Role::parse(&role)
        .ok_or_else(|| ApiError::internal(&format!("user {id} has unknown role {role}")))?;
    Ok(User {
        id,
        email,
        name,
        role: role.into(),
        disabled: disabled != 0,
        created_at,
    })
}

fn me(user: User) -> MeResponse {
    MeResponse {
        kind: IdentityType::User,
        user: Some(user),
        agent: None,
        grants: Some(Vec::new()),
    }
}

/// `true` for a plausible email: one `@`, non-empty sides, no whitespace,
/// at most 254 characters. Deliverability is not Quoro's concern.
pub(crate) fn valid_email(email: &str) -> bool {
    let mut parts = email.split('@');
    let (Some(local), Some(domain), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    email.len() <= 254
        && !local.is_empty()
        && !domain.is_empty()
        && !email.chars().any(char::is_whitespace)
}

fn session_cookie(state: &AppState, value: &str, max_age: i64) -> HeaderValue {
    let secure = if state.config.secure_cookies {
        "; Secure"
    } else {
        ""
    };
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}{secure}"
    ))
    .unwrap_or_else(|_| HeaderValue::from_static("quoro_session=; Max-Age=0"))
}

async fn users_exist(state: &AppState) -> Result<bool, ApiError> {
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM users")
        .fetch_one(state.store.pool())
        .await?;
    Ok(count > 0)
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|v| !v.is_empty())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

const SETUP: &str = "POST /api/v1/setup";

/// Create the first owner while no user exists.
#[utoipa::path(
    post,
    path = "/api/v1/setup",
    tag = "health",
    operation_id = "setup",
    security(()),
    extensions(("x-quoro-scope" = json!("public"))),
    params(
        ("Quoro-Setup-Token" = String, Header, description = "One-time token printed by `quoro setup-token`.", min_length = 1),
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", nullable = false, min_length = 1, max_length = 128),
    ),
    request_body(content = SetupRequest, description = "Email, name and password of the first owner."),
    responses(
        (status = 201, description = "The first owner was created and the setup token was consumed.", body = User,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 500, response = crate::openapi::Internal),
    )
)]
pub async fn setup(
    State(state): State<AppState>,
    extensions: Extensions,
    headers: HeaderMap,
    ApiJson(body): ApiJson<SetupRequest>,
) -> Result<Response, ApiError> {
    let params = serde_json::to_value(&body).map_err(|e| ApiError::internal(&e))?;
    // A repeated call with the same key and body returns the original
    // result (`IdempotencyKey`), so the replay check comes first.
    let key = header_value(&headers, "idempotency-key");
    // The fingerprint also covers the presented token, so a replay needs the
    // same token as the original call, not just its key and body.
    let fingerprint = json!({
        "body": params,
        "token": crate::store::sha256_hex(
            header_value(&headers, "quoro-setup-token").unwrap_or_default().as_bytes()
        ),
    });
    if let Some(key) = &key {
        match state.idempotency.begin("public", SETUP, key, &fingerprint) {
            Begin::Proceed => {}
            Begin::Replay(status, value) => {
                audit_setup_replay(&state, &extensions, &params, &value).await?;
                return Ok((status, Json(value)).into_response());
            }
            Begin::Conflict => return Err(ApiError::conflict("Idempotency-Key reused")),
        }
    }
    match run_setup(&state, &extensions, &headers, &body, &params).await {
        Ok(user) => {
            let value = serde_json::to_value(&user).map_err(|e| ApiError::internal(&e))?;
            if let Some(key) = &key {
                state
                    .idempotency
                    .finish("public", SETUP, key, StatusCode::CREATED, value.clone());
            }
            Ok((StatusCode::CREATED, Json(value)).into_response())
        }
        Err(e) => {
            if let Some(key) = &key {
                state.idempotency.abandon("public", SETUP, key);
            }
            Err(e)
        }
    }
}

/// A replayed setup is recorded too (`IdempotencyKey`: D-0031 records the
/// call either way): `ok`, as the owner it created, with no change-log link
/// because nothing changed.
async fn audit_setup_replay(
    state: &AppState,
    extensions: &Extensions,
    params: &Value,
    user: &Value,
) -> Result<(), ApiError> {
    let id = user["id"]
        .as_str()
        .ok_or_else(|| ApiError::internal(&"replayed setup has no user id"))?
        .to_string();
    let mut w = state.store.begin_write().await?;
    audit::append(
        &mut w.tx,
        &Record {
            actor: Actor {
                kind: ActorType::User,
                id: id.clone(),
            },
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::Api,
            operation: SETUP.to_string(),
            scope: None,
            target_type: Some("user".into()),
            target_id: Some(id),
            project_id: None,
            params: params.clone(),
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: request_id_of(extensions),
            result: Outcome::Ok,
            result_detail: None,
            change_seq: None,
        },
    )
    .await?;
    w.commit().await?;
    Ok(())
}

async fn run_setup(
    state: &AppState,
    extensions: &Extensions,
    headers: &HeaderMap,
    body: &SetupRequest,
    params: &Value,
) -> Result<User, ApiError> {
    // Order fixed by the spec: once an owner exists every call is 409,
    // whatever the token; before that a bad token is 401.
    if users_exist(state).await? {
        return Err(ApiError::conflict("setup is already done"));
    }
    let token = header_value(headers, "quoro-setup-token").ok_or_else(ApiError::unauthenticated)?;
    if !setup_token::is_valid(&state.config.data_dir, &token) {
        return Err(ApiError::unauthenticated());
    }
    let name = body.name.trim().to_string();
    if !valid_email(&body.email) {
        return Err(ApiError::validation("email is not a valid address"));
    }
    if name.is_empty() || name.chars().count() > 200 {
        return Err(ApiError::validation("name must be 1-200 characters"));
    }
    if body.password.chars().count() < 12 {
        return Err(ApiError::validation(
            "password must be at least 12 characters",
        ));
    }
    let user = create_owner(state, extensions, body, &name, params).await?;
    // Single use: the token is gone once the owner exists. A failure here
    // leaves a token that can no longer be used anyway, because the users
    // table is no longer empty.
    if let Err(e) = setup_token::consume(&state.config.data_dir) {
        tracing::warn!(error = %e, "could not remove the consumed setup token");
    }
    Ok(user)
}

async fn create_owner(
    state: &AppState,
    extensions: &Extensions,
    body: &SetupRequest,
    name: &str,
    params: &Value,
) -> Result<User, ApiError> {
    let hash = password::hash(&body.password).map_err(|e| ApiError::internal(&e))?;
    let id = new_id(Prefix::User);
    let created_at = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    // Re-check inside the writer gate: two concurrent setups with the same
    // valid token must not create two owners.
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM users")
        .fetch_one(&mut *w.tx)
        .await?;
    if count > 0 {
        return Err(ApiError::conflict("setup is already done"));
    }
    sqlx::query("INSERT INTO users (id, email, name, role, password_hash, disabled, created_at) VALUES (?, ?, ?, 'owner', ?, 0, ?)")
        .bind(&id)
        .bind(&body.email)
        .bind(name)
        .bind(&hash)
        .bind(&created_at)
        .execute(&mut *w.tx)
        .await?;
    let actor = Actor {
        kind: ActorType::User,
        id: id.clone(),
    };
    // users is replicated: the payload is the full row, password hash
    // included, so replaying the log rebuilds the table.
    let change_seq = changelog::append(
        &mut w.tx,
        &actor,
        "user",
        &id,
        Op::Create,
        json!({
            "id": id, "email": body.email, "name": name, "role": "owner",
            "password_hash": hash, "disabled": 0, "created_at": created_at,
        }),
    )
    .await?;
    audit::append(
        &mut w.tx,
        &Record {
            actor,
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::Api,
            operation: SETUP.to_string(),
            scope: None,
            target_type: Some("user".into()),
            target_id: Some(id.clone()),
            project_id: None,
            params: params.clone(),
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: request_id_of(extensions),
            result: Outcome::Ok,
            result_detail: None,
            change_seq: Some(change_seq),
        },
    )
    .await?;
    w.commit().await?;
    Ok(User {
        id,
        email: body.email.clone(),
        name: name.to_string(),
        role: UserRole::Owner,
        disabled: false,
        created_at,
    })
}

/// Log a human in and set the session cookie.
#[utoipa::path(
    post,
    path = "/api/v1/auth/login",
    tag = "auth",
    operation_id = "login",
    security(()),
    extensions(("x-quoro-scope" = json!("public"))),
    request_body(content = LoginRequest, description = "Email and password."),
    responses(
        (status = 200, description = "Login succeeded; the session cookie is set.", body = MeResponse,
            headers(
                ("Quoro-Request-Id" = String, description = "Correlation id of this call."),
                ("Set-Cookie" = String, description = "The `quoro_session` session cookie."),
            )),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn login(
    State(state): State<AppState>,
    extensions: Extensions,
    ApiJson(body): ApiJson<LoginRequest>,
) -> Result<Response, ApiError> {
    if body.password.is_empty() || body.email.is_empty() {
        return Err(ApiError::validation("email and password are required"));
    }
    let row: Option<(String, String, String, String, i64, String, String)> = sqlx::query_as(
        "SELECT id, email, name, role, disabled, created_at, password_hash FROM users WHERE email = ?",
    )
    .bind(&body.email)
    .fetch_optional(state.store.pool())
    .await?;
    let Some((id, email, name, role, disabled, created_at, hash)) = row else {
        // Same work as a known email, so timing does not reveal accounts.
        let _ = password::verify(&body.password, password::dummy_hash());
        tracing::info!("login refused: unknown email");
        return Err(ApiError::unauthenticated());
    };
    if !password::verify(&body.password, &hash) || disabled != 0 {
        tracing::info!(user_id = %id, "login refused: wrong password or disabled user");
        return Err(ApiError::unauthenticated());
    }
    let user = user_from((id.clone(), email.clone(), name, role, disabled, created_at))?;

    let secret = random_secret();
    let mut w = state.store.begin_write().await?;
    // sessions is LOCAL: no change-log entry.
    sqlx::query("INSERT INTO sessions (id, user_id, token_hash, expires_at, created_at) VALUES (?, ?, ?, ?, ?)")
        .bind(new_id(Prefix::Session))
        .bind(&id)
        .bind(sha256_hex(secret.as_bytes()))
        .bind(rfc3339_in(SESSION_TTL_SECONDS))
        .bind(now_rfc3339())
        .execute(&mut *w.tx)
        .await?;
    audit::append(
        &mut w.tx,
        &Record {
            actor: Actor {
                kind: ActorType::User,
                id: id.clone(),
            },
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::Panel,
            operation: "POST /api/v1/auth/login".to_string(),
            scope: None,
            target_type: Some("user".into()),
            target_id: Some(id),
            project_id: None,
            params: json!({ "email": email }),
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: request_id_of(&extensions),
            result: Outcome::Ok,
            result_detail: None,
            change_seq: None,
        },
    )
    .await?;
    w.commit().await?;

    let mut response = Json(me(user)).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        session_cookie(&state, &secret, SESSION_TTL_SECONDS),
    );
    Ok(response)
}

/// End the current session.
#[utoipa::path(
    post,
    path = "/api/v1/auth/logout",
    tag = "auth",
    operation_id = "logout",
    extensions(("x-quoro-scope" = json!("authenticated"))),
    params(
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", nullable = false, min_length = 1, max_length = 2000),
    ),
    responses(
        (status = 204, description = "The session was deleted.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
    )
)]
pub async fn logout(
    State(state): State<AppState>,
    extensions: Extensions,
    headers: HeaderMap,
    identity: Identity,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id_of(&extensions),
        "POST /api/v1/auth/logout",
        &headers,
    );
    let target = Target {
        kind: "user".into(),
        id: ctx.identity.id().to_string(),
    };
    let mut w = state.store.begin_write().await?;
    if let Credential::Session { session_id } = &ctx.identity.credential {
        // sessions is LOCAL: no change-log entry.
        sqlx::query("DELETE FROM sessions WHERE id = ?")
            .bind(session_id)
            .execute(&mut *w.tx)
            .await?;
    }
    audit::append(
        &mut w.tx,
        &ctx.record(
            None,
            Some(&target),
            None,
            &json!({}),
            Outcome::Ok,
            None,
            None,
        ),
    )
    .await?;
    w.commit().await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, session_cookie(&state, "", 0));
    Ok(response)
}

/// Read the current identity.
#[utoipa::path(
    get,
    path = "/api/v1/auth/me",
    tag = "auth",
    operation_id = "getMe",
    extensions(("x-quoro-scope" = json!("authenticated"))),
    responses(
        (status = 200, description = "The current identity.", body = MeResponse,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_me(
    State(state): State<AppState>,
    identity: Identity,
) -> Result<Json<MeResponse>, ApiError> {
    // A bearer access token resolves to an agent; a session cookie only ever
    // resolves to a human (auth/mod.rs).
    if let Principal::Agent(agent) = &identity.principal {
        let row: Option<crate::api::agents::AgentRow> = sqlx::query_as(
            "SELECT id, name, model, owner_user_id, description, disabled, created_at, updated_at \
             FROM agents WHERE id = ?",
        )
        .bind(&agent.agent_id)
        .fetch_optional(state.store.pool())
        .await?;
        let row = row.ok_or_else(ApiError::unauthenticated)?;
        // The grants the agent can use right now (permission-model.md
        // section 6.2): active, not past the TTL and with uses left.
        let grants =
            crate::service::grants::active_grants_for_agent(&state, &agent.agent_id).await?;
        return Ok(Json(MeResponse {
            kind: IdentityType::Agent,
            user: None,
            agent: Some(crate::api::agents::agent_from_tuple(row)),
            grants: Some(grants),
        }));
    }
    let row: Option<UserRow> = sqlx::query_as(
        "SELECT id, email, name, role, disabled, created_at FROM users WHERE id = ?",
    )
    .bind(identity.id())
    .fetch_optional(state.store.pool())
    .await?;
    let row = row.ok_or_else(ApiError::unauthenticated)?;
    Ok(Json(me(user_from(row)?)))
}

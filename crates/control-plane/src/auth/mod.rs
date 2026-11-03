//! Authentication: resolving the credential of a request to an identity
//! (permission-model.md section 2).
//!
//! Humans authenticate with the `quoro_session` cookie (the panel) or with
//! `Authorization: Bearer tok_...` (a personal API token). Agents authenticate
//! with `Authorization: Bearer atk_...`, a 5-minute access token minted from an
//! agent key at `POST /api/v1/auth/agent-token`. An agent key (`akey_...`) is
//! accepted by that endpoint only ([`authenticate_agent_key`]); everywhere else
//! it is an unknown credential. A session cookie only ever resolves to a human,
//! so it can never authenticate an agent. Only SHA-256 hashes of every secret
//! are stored.

pub mod password;
pub mod setup_token;

use crate::AppState;
use crate::audit::Channel;
use crate::changelog::{Actor, ActorType};
use crate::error::ApiError;
use crate::store::{now_rfc3339, sha256_hex};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, header};
use quoro_types::Role;

/// Name of the panel session cookie (`openapi.yaml` `sessionCookie`).
pub const SESSION_COOKIE: &str = "quoro_session";

/// Absolute session lifetime: 24 h, no sliding renewal (permission-model.md
/// section 2).
pub const SESSION_TTL_SECONDS: i64 = 24 * 60 * 60;

/// Lifetime of an agent access token: 5 minutes (permission-model.md
/// section 2).
pub const ACCESS_TOKEN_TTL_SECONDS: i64 = 5 * 60;

/// Which credential authenticated the request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Credential {
    /// A panel session; the id names the `sessions` row.
    Session { session_id: String },
    /// A personal API token; the id names the `api_tokens` row.
    ApiToken { token_id: String },
    /// An agent access token; the id names the `access_tokens` row.
    AccessToken { token_id: String },
}

/// An authenticated human.
#[derive(Clone, Debug)]
pub struct UserIdentity {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub role: Role,
}

/// An authenticated agent (D-0029). `disabled` is read with the identity on
/// every request, so authorization can refuse a disabled agent before any
/// grant is consulted (permission-model.md section 7.2, step b).
#[derive(Clone, Debug)]
pub struct AgentIdentity {
    pub agent_id: String,
    pub name: String,
    pub model: String,
    pub owner_user_id: String,
    pub disabled: bool,
}

/// Who is calling.
#[derive(Clone, Debug)]
pub enum Principal {
    User(UserIdentity),
    Agent(AgentIdentity),
}

/// An authenticated caller: a human or an agent, and the credential used.
#[derive(Clone, Debug)]
pub struct Identity {
    pub principal: Principal,
    pub credential: Credential,
}

impl Identity {
    /// The channel a call arrives on when the adapter does not say otherwise:
    /// the panel uses the API with a session (README "Channels"); a bearer
    /// credential is the plain API channel. The MCP adapter overrides it.
    #[must_use]
    pub fn default_channel(&self) -> Channel {
        match self.credential {
            Credential::Session { .. } => Channel::Panel,
            Credential::ApiToken { .. } | Credential::AccessToken { .. } => Channel::Api,
        }
    }

    #[must_use]
    pub fn actor(&self) -> Actor {
        match &self.principal {
            Principal::User(user) => Actor {
                kind: ActorType::User,
                id: user.user_id.clone(),
            },
            Principal::Agent(agent) => Actor {
                kind: ActorType::Agent,
                id: agent.agent_id.clone(),
            },
        }
    }

    /// The `usr_` or `agt_` id of the caller. Per-identity state (idempotency
    /// keys, rate limits) is keyed by it.
    #[must_use]
    pub fn id(&self) -> &str {
        match &self.principal {
            Principal::User(user) => &user.user_id,
            Principal::Agent(agent) => &agent.agent_id,
        }
    }

    /// Whether this identity is a human.
    #[must_use]
    pub fn is_human(&self) -> bool {
        matches!(self.principal, Principal::User(_))
    }

    /// The human behind the call, if it is one.
    #[must_use]
    pub fn user(&self) -> Option<&UserIdentity> {
        match &self.principal {
            Principal::User(user) => Some(user),
            Principal::Agent(_) => None,
        }
    }

    /// The agent behind the call, if it is one.
    #[must_use]
    pub fn agent(&self) -> Option<&AgentIdentity> {
        match &self.principal {
            Principal::Agent(agent) => Some(agent),
            Principal::User(_) => None,
        }
    }

    /// The human's role; `None` for an agent, which never holds a role
    /// (permission-model.md section 3).
    #[must_use]
    pub fn role(&self) -> Option<Role> {
        self.user().map(|user| user.role)
    }
}

/// The value of the session cookie in `headers`, if any.
#[must_use]
pub fn session_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == SESSION_COOKIE)
        .map(|(_, value)| value.to_string())
        .filter(|v| !v.is_empty())
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

type UserRow = (String, String, String, String, String);

fn user_identity(row: UserRow, credential: Credential) -> Result<Identity, ApiError> {
    let (_, user_id, email, name, role) = row;
    let role = Role::parse(&role)
        .ok_or_else(|| ApiError::internal(&format!("user {user_id} has unknown role {role}")))?;
    Ok(Identity {
        principal: Principal::User(UserIdentity {
            user_id,
            email,
            name,
            role,
        }),
        credential,
    })
}

type AgentRow = (String, String, String, String, String, i64);

fn agent_identity(row: AgentRow) -> Identity {
    let (token_id, agent_id, name, model, owner_user_id, disabled) = row;
    Identity {
        principal: Principal::Agent(AgentIdentity {
            agent_id,
            name,
            model,
            owner_user_id,
            disabled: disabled != 0,
        }),
        credential: Credential::AccessToken { token_id },
    }
}

/// Resolves a bearer secret: a personal API token (`tok_`) or an agent access
/// token (`atk_`). Anything else, an agent key included, is `unauthenticated`.
async fn resolve_bearer(state: &AppState, token: &str, now: &str) -> Result<Identity, ApiError> {
    let hash = sha256_hex(token.as_bytes());
    if token.starts_with("tok_") {
        let row: Option<UserRow> = sqlx::query_as(
            "SELECT t.id, u.id, u.email, u.name, u.role FROM api_tokens t JOIN users u ON u.id = t.user_id \
             WHERE t.token_hash = ? AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at > ?) \
             AND u.disabled = 0",
        )
        .bind(&hash)
        .bind(now)
        .fetch_optional(state.store.pool())
        .await?;
        let row = row.ok_or_else(ApiError::unauthenticated)?;
        let token_id = row.0.clone();
        return user_identity(row, Credential::ApiToken { token_id });
    }
    if token.starts_with("atk_") {
        // `agents.disabled` is part of the identity, not a reason to fail
        // authentication: a disabled agent is a known identity that
        // authorization refuses with `agent_disabled` (section 7.2, step b).
        let row: Option<AgentRow> = sqlx::query_as(
            "SELECT t.id, a.id, a.name, a.model, a.owner_user_id, a.disabled FROM access_tokens t \
             JOIN agents a ON a.id = t.agent_id \
             WHERE t.token_hash = ? AND t.expires_at > ?",
        )
        .bind(&hash)
        .bind(now)
        .fetch_optional(state.store.pool())
        .await?;
        return row
            .map(agent_identity)
            .ok_or_else(ApiError::unauthenticated);
    }
    Err(ApiError::unauthenticated())
}

/// Resolves the request's credential. A disabled user, an expired or unknown
/// session and an unknown, revoked or expired token are all
/// `unauthenticated`. A bearer header wins over a cookie.
///
/// # Errors
///
/// `unauthenticated`, or an internal error on a database failure.
pub async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<Identity, ApiError> {
    let now = now_rfc3339();
    if let Some(token) = bearer(headers) {
        return resolve_bearer(state, token, &now).await;
    }
    if let Some(secret) = session_cookie(headers) {
        let row: Option<UserRow> = sqlx::query_as(
            "SELECT s.id, u.id, u.email, u.name, u.role FROM sessions s JOIN users u ON u.id = s.user_id \
             WHERE s.token_hash = ? AND s.expires_at > ? AND u.disabled = 0",
        )
        .bind(sha256_hex(secret.as_bytes()))
        .bind(&now)
        .fetch_optional(state.store.pool())
        .await?;
        let row = row.ok_or_else(ApiError::unauthenticated)?;
        let session_id = row.0.clone();
        return user_identity(row, Credential::Session { session_id });
    }
    Err(ApiError::unauthenticated())
}

/// Resolves a bearer credential only and ignores cookies: the entry point for
/// channels that must never accept a browser session, such as the MCP
/// endpoint (permission-model.md sections 2 and 10).
///
/// # Errors
///
/// `unauthenticated`, or an internal error on a database failure.
pub async fn authenticate_bearer(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Identity, ApiError> {
    let token = bearer(headers).ok_or_else(ApiError::unauthenticated)?;
    resolve_bearer(state, token, &now_rfc3339()).await
}

/// The agent an agent key belongs to, for `POST /api/v1/auth/agent-token`.
#[derive(Clone, Debug)]
pub struct AgentKeyHolder {
    /// The `akey_` id of the key that was presented.
    pub key_id: String,
    pub agent: AgentIdentity,
}

/// Resolves `Authorization: Bearer akey_...` for the token-minting endpoint.
/// An unknown or revoked key is `unauthenticated`. A disabled agent still
/// resolves, with `disabled` set, so the endpoint can refuse it with
/// `forbidden` / `agent_disabled` and audit the refusal (openapi
/// `createAgentToken`).
///
/// # Errors
///
/// `unauthenticated`, or an internal error.
pub async fn authenticate_agent_key(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AgentKeyHolder, ApiError> {
    let key = bearer(headers)
        .filter(|key| key.starts_with("akey_"))
        .ok_or_else(ApiError::unauthenticated)?;
    let row: Option<AgentRow> = sqlx::query_as(
        "SELECT k.id, a.id, a.name, a.model, a.owner_user_id, a.disabled FROM agent_keys k \
         JOIN agents a ON a.id = k.agent_id \
         WHERE k.key_hash = ? AND k.revoked_at IS NULL",
    )
    .bind(sha256_hex(key.as_bytes()))
    .fetch_optional(state.store.pool())
    .await?;
    let (key_id, agent_id, name, model, owner_user_id, disabled) =
        row.ok_or_else(ApiError::unauthenticated)?;
    Ok(AgentKeyHolder {
        key_id,
        agent: AgentIdentity {
            agent_id,
            name,
            model,
            owner_user_id,
            disabled: disabled != 0,
        },
    })
}

impl FromRequestParts<AppState> for Identity {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authenticate(state, &parts.headers).await
    }
}

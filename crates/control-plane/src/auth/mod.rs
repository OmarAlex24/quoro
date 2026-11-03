//! Authentication: resolving the credential of a request to an identity
//! (permission-model.md section 2).
//!
//! M1 knows humans only. A request is authenticated by the `quoro_session`
//! cookie (the panel) or by `Authorization: Bearer tok_...` (a personal API
//! token). Only SHA-256 hashes of either secret are stored. Agent access
//! tokens (`atk_`) arrive with the agent endpoints in M3 and are refused as
//! unknown credentials until then.

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

/// Which credential authenticated the request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Credential {
    /// A panel session; the id names the `sessions` row.
    Session { session_id: String },
    /// A personal API token; the id names the `api_tokens` row.
    ApiToken { token_id: String },
}

/// An authenticated human.
#[derive(Clone, Debug)]
pub struct Identity {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub role: Role,
    pub credential: Credential,
}

impl Identity {
    /// The panel uses the API with a session (README "Channels"); a bearer
    /// token is the plain API channel.
    #[must_use]
    pub fn channel(&self) -> Channel {
        match self.credential {
            Credential::Session { .. } => Channel::Panel,
            Credential::ApiToken { .. } => Channel::Api,
        }
    }

    #[must_use]
    pub fn actor(&self) -> Actor {
        Actor {
            kind: ActorType::User,
            id: self.user_id.clone(),
        }
    }

    /// Whether this identity is a human. M1 authenticates humans only: an
    /// agent access token (`atk_`) is refused as an unknown credential until
    /// the agent endpoints arrive in M3, so both credential kinds here are
    /// human ones.
    #[must_use]
    pub fn is_human(&self) -> bool {
        matches!(
            self.credential,
            Credential::Session { .. } | Credential::ApiToken { .. }
        )
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

fn identity_from(row: UserRow) -> Result<Identity, ApiError> {
    let (credential_id, user_id, email, name, role) = row;
    let role = Role::parse(&role)
        .ok_or_else(|| ApiError::internal(&format!("user {user_id} has unknown role {role}")))?;
    Ok(Identity {
        user_id,
        email,
        name,
        role,
        credential: Credential::Session {
            session_id: credential_id,
        },
    })
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
        if !token.starts_with("tok_") {
            return Err(ApiError::unauthenticated());
        }
        let row: Option<UserRow> = sqlx::query_as(
            "SELECT t.id, u.id, u.email, u.name, u.role FROM api_tokens t JOIN users u ON u.id = t.user_id \
             WHERE t.token_hash = ? AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at > ?) \
             AND u.disabled = 0",
        )
        .bind(sha256_hex(token.as_bytes()))
        .bind(&now)
        .fetch_optional(state.store.pool())
        .await?;
        let row = row.ok_or_else(ApiError::unauthenticated)?;
        let mut identity = identity_from(row)?;
        if let Credential::Session { session_id } = identity.credential {
            identity.credential = Credential::ApiToken {
                token_id: session_id,
            };
        }
        return Ok(identity);
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
        return identity_from(row.ok_or_else(ApiError::unauthenticated)?);
    }
    Err(ApiError::unauthenticated())
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

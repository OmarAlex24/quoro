//! `application/problem+json` errors (RFC 9457) with Quoro's `code`, `reason`,
//! `scope` and `target` fields (README "Errors", `openapi.yaml` `Problem`).

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

/// Machine-readable error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    Unauthenticated,
    Forbidden,
    ApprovalRequired,
    NotFound,
    Conflict,
    /// A request body above the API's limit (README "Request body limit",
    /// ADR 0306).
    PayloadTooLarge,
    ValidationFailed,
    RateLimited,
    NotPrimary,
    NodeUnreachable,
    Internal,
}

impl Code {
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden | Self::ApprovalRequired => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ValidationFailed => StatusCode::UNPROCESSABLE_ENTITY,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::NotPrimary => StatusCode::SERVICE_UNAVAILABLE,
            Self::NodeUnreachable => StatusCode::GATEWAY_TIMEOUT,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::Forbidden => "forbidden",
            Self::ApprovalRequired => "approval_required",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::PayloadTooLarge => "payload_too_large",
            Self::ValidationFailed => "validation_failed",
            Self::RateLimited => "rate_limited",
            Self::NotPrimary => "not_primary",
            Self::NodeUnreachable => "node_unreachable",
            Self::Internal => "internal",
        }
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Unauthenticated => "Unauthenticated",
            Self::Forbidden => "Forbidden",
            Self::ApprovalRequired => "Approval required",
            Self::NotFound => "Not found",
            Self::Conflict => "Conflict",
            Self::PayloadTooLarge => "Payload too large",
            Self::ValidationFailed => "Validation failed",
            Self::RateLimited => "Rate limited",
            Self::NotPrimary => "Not primary",
            Self::NodeUnreachable => "Node unreachable",
            Self::Internal => "Internal error",
        }
    }
}

/// Why a `forbidden` response was returned, and from M5b why a failover, a
/// switchover or a delete was refused with `conflict` (ADR 0175 to ADR 0177).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    ScopeMissing,
    SelectorMismatch,
    GrantExpired,
    GrantRevoked,
    GrantExhausted,
    HumanOnly,
    AgentDisabled,
    NotWarm,
    NoCopy,
    NotInSync,
    NodeDisconnected,
    FenceFailed,
    NoSupport,
    LeaseHeld,
    InProgress,
    HasQuarantines,
    TokenExhausted,
}

impl Reason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeMissing => "scope_missing",
            Self::SelectorMismatch => "selector_mismatch",
            Self::GrantExpired => "grant_expired",
            Self::GrantRevoked => "grant_revoked",
            Self::GrantExhausted => "grant_exhausted",
            Self::HumanOnly => "human_only",
            Self::AgentDisabled => "agent_disabled",
            Self::NotWarm => "not_warm",
            Self::NoCopy => "no_copy",
            Self::NotInSync => "not_in_sync",
            Self::NodeDisconnected => "node_disconnected",
            Self::FenceFailed => "fence_failed",
            Self::NoSupport => "no_support",
            Self::LeaseHeld => "lease_held",
            Self::InProgress => "in_progress",
            Self::HasQuarantines => "has_quarantines",
            Self::TokenExhausted => "token_exhausted",
        }
    }
}

/// One addressed resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[schema(title = "Target", description = "One addressed resource.")]
pub struct Target {
    /// Resource kind, for example `app`, `vm`, `project` or `node`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Id of the resource.
    pub id: String,
}

/// The problem body.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Problem")]
pub struct Problem {
    /// URI identifying the problem type; `about:blank` when none.
    #[serde(rename = "type")]
    #[schema(format = "uri-reference", default = "about:blank")]
    pub kind: String,
    /// Short human-readable summary of the problem type.
    pub title: String,
    /// HTTP status code of this response.
    #[schema(minimum = 400, maximum = 599)]
    pub status: u16,
    /// Human-readable explanation of this occurrence.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub detail: Option<String>,
    /// URI of the request that produced the problem.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(format = "uri-reference", nullable = false)]
    pub instance: Option<String>,
    /// Machine-readable error code.
    #[schema(inline)]
    pub code: Code,
    /// Why a `forbidden` response was returned, or a failover, switchover or
    /// delete refused with `conflict`. Absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(inline, nullable = false)]
    pub reason: Option<Reason>,
    /// The scope that was missing, or that an approval must grant.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub target: Option<Target>,
}

/// An API error, rendered as a problem response.
#[derive(Clone, Debug)]
pub struct ApiError {
    pub code: Code,
    pub reason: Option<Reason>,
    pub detail: Option<String>,
    pub scope: Option<String>,
    pub target: Option<Target>,
}

impl ApiError {
    #[must_use]
    pub fn new(code: Code, detail: impl Into<String>) -> Self {
        Self {
            code,
            reason: None,
            detail: Some(detail.into()),
            scope: None,
            target: None,
        }
    }

    #[must_use]
    pub fn unauthenticated() -> Self {
        Self::new(Code::Unauthenticated, "no valid credential was presented")
    }

    #[must_use]
    pub fn forbidden(reason: Reason, scope: Option<&str>) -> Self {
        Self {
            code: Code::Forbidden,
            reason: Some(reason),
            detail: Some(match scope {
                Some(scope) => format!("{} for {scope}", reason.as_str()),
                None => reason.as_str().to_string(),
            }),
            scope: scope.map(str::to_string),
            target: None,
        }
    }

    #[must_use]
    pub fn not_found(what: &str) -> Self {
        Self::new(Code::NotFound, format!("{what} not found"))
    }

    #[must_use]
    pub fn conflict(detail: impl Into<String>) -> Self {
        Self::new(Code::Conflict, detail)
    }

    #[must_use]
    pub fn validation(detail: impl Into<String>) -> Self {
        Self::new(Code::ValidationFailed, detail)
    }

    /// An internal error. The cause goes to the server log, never to the body.
    #[must_use]
    pub fn internal(cause: &dyn std::fmt::Display) -> Self {
        tracing::error!(%cause, "internal error");
        Self::new(Code::Internal, "an unexpected error occurred")
    }

    /// `result_detail` for the audit record: the reason for a refusal, the
    /// code otherwise.
    #[must_use]
    pub fn audit_detail(&self) -> String {
        self.reason.map_or_else(
            || self.code.as_str().to_string(),
            |r| r.as_str().to_string(),
        )
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        Self::internal(&e)
    }
}

impl From<crate::changelog::AppendError> for ApiError {
    fn from(e: crate::changelog::AppendError) -> Self {
        Self::internal(&e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.code.status();
        let body = Problem {
            kind: "about:blank".to_string(),
            title: self.code.title().to_string(),
            status: status.as_u16(),
            detail: self.detail,
            instance: None,
            code: self.code,
            reason: self.reason,
            scope: self.scope,
            target: self.target,
        };
        let mut response = (status, axum::Json(body)).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}

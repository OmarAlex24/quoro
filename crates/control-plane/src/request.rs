//! Per-request plumbing: the `Quoro-Request-Id`, the JSON body extractor that
//! answers `validation_failed`, and [`CallCtx`], the one place where a call is
//! authorized and audited (D-0031: one authorization path, one audit path).

use crate::AppState;
use crate::audit::{self, Channel, Outcome, Record};
use crate::auth::Identity;
use crate::error::{ApiError, Reason, Target};
use axum::extract::{FromRequest, Request};
use axum::http::{HeaderMap, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use quoro_types::{Prefix, Scope, new_id};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Correlation id of the call, `req_...`, echoed as `Quoro-Request-Id`.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// Middleware: assigns the request id and sets the response header on every
/// response, errors included.
pub async fn request_id(mut request: Request, next: Next) -> Response {
    let id = new_id(Prefix::Request);
    request.extensions_mut().insert(RequestId(id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("quoro-request-id", value);
    }
    response
}

/// Pulls the request id the middleware stored.
#[must_use]
pub fn request_id_of(extensions: &axum::http::Extensions) -> String {
    extensions
        .get::<RequestId>()
        .map_or_else(|| new_id(Prefix::Request), |r| r.0.clone())
}

/// `axum::Json` whose rejection is a `validation_failed` problem.
pub struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(value)) => Ok(Self(value)),
            Err(rejection) => Err(ApiError::validation(rejection.body_text())),
        }
    }
}

/// Context of one authenticated call: who, through which channel, and which
/// operation. Every mutating service call goes through [`CallCtx::authorize`]
/// and ends in exactly one audit record.
#[derive(Clone, Debug)]
pub struct CallCtx {
    pub identity: Identity,
    pub request_id: String,
    /// Operation as recorded in the audit log: `POST /api/v1/vms/{vm_id}/start`.
    pub operation: &'static str,
    pub justification: Option<String>,
    pub incident_id: Option<String>,
}

impl CallCtx {
    #[must_use]
    pub fn new(
        identity: Identity,
        request_id: String,
        operation: &'static str,
        headers: &HeaderMap,
    ) -> Self {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .filter(|v| !v.is_empty())
        };
        Self {
            identity,
            request_id,
            operation,
            justification: header("quoro-justification"),
            incident_id: header("quoro-incident"),
        }
    }

    #[must_use]
    pub fn channel(&self) -> Channel {
        self.identity.channel()
    }

    /// Authorizes a read. Human reads are not audited (audit-log-format.md
    /// section 3), so a refusal returns without a record.
    ///
    /// # Errors
    ///
    /// `forbidden` with `reason: scope_missing` when the role lacks `scope`.
    pub fn authorize_read(&self, scope: Scope) -> Result<(), ApiError> {
        if self.identity.role.holds(scope) {
            Ok(())
        } else {
            Err(ApiError::forbidden(
                Reason::ScopeMissing,
                Some(scope.as_str()),
            ))
        }
    }

    /// Authorizes a mutating call (permission-model.md section 7.2, human
    /// branch: the role bundle decides). A refusal is audited with
    /// `result: refused` before it is returned.
    ///
    /// # Errors
    ///
    /// `forbidden` with `reason: scope_missing`, or an internal error if the
    /// refusal could not be recorded.
    pub async fn authorize(
        &self,
        state: &AppState,
        scope: Scope,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
    ) -> Result<(), ApiError> {
        if self.identity.role.holds(scope) {
            return Ok(());
        }
        let error = ApiError::forbidden(Reason::ScopeMissing, Some(scope.as_str()));
        let record = self.record(
            Some(scope),
            target,
            project_id,
            params,
            Outcome::Refused,
            Some(error.audit_detail()),
            None,
        );
        let mut w = state.store.begin_write().await?;
        audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        Err(error)
    }

    /// The audit record for this call.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn record(
        &self,
        scope: Option<Scope>,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
        result: Outcome,
        result_detail: Option<String>,
        change_seq: Option<i64>,
    ) -> Record {
        Record {
            actor: self.identity.actor(),
            agent_model: None,
            on_behalf_of: None,
            channel: self.channel(),
            operation: self.operation.to_string(),
            scope: scope.map(|s| s.as_str().to_string()),
            target_type: target.map(|t| t.kind.clone()),
            target_id: target.map(|t| t.id.clone()),
            project_id: project_id.map(str::to_string),
            params: params.clone(),
            // Humans may omit the justification; it is recorded when given.
            justification: self.justification.clone(),
            trigger_event_id: None,
            incident_id: self.incident_id.clone(),
            grant_id: None,
            approval_id: None,
            request_id: self.request_id.clone(),
            result,
            result_detail,
            change_seq,
        }
    }

    /// Records an authorized call that then failed (`result: error`), in its
    /// own transaction, after the failed one rolled back.
    ///
    /// # Errors
    ///
    /// An internal error if the record could not be written.
    pub async fn audit_error(
        &self,
        state: &AppState,
        scope: Option<Scope>,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
        error: &ApiError,
    ) -> Result<(), ApiError> {
        let record = self.record(
            scope,
            target,
            project_id,
            params,
            Outcome::Error,
            Some(error.audit_detail()),
            None,
        );
        let mut w = state.store.begin_write().await?;
        audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        Ok(())
    }
}

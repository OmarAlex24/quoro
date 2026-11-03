//! Per-request plumbing: the `Quoro-Request-Id`, the JSON body extractor that
//! answers `validation_failed`, and [`CallCtx`], the one place where a call is
//! authorized and audited (D-0031: one authorization path, one audit path).

use crate::AppState;
use crate::audit::{self, Channel, Outcome, Record};
use crate::auth::{AgentIdentity, Identity, Principal};
use crate::authz;
use crate::changelog::{self, Actor, Op};
use crate::error::{ApiError, Code, Reason, Target};
use crate::events::{self, NewEvent, Source};
use crate::store::now_rfc3339;
use axum::extract::{FromRequest, OptionalFromRequest, Request};
use axum::http::{HeaderMap, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use quoro_types::{Prefix, Scope, new_id};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::SqliteConnection;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

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
        match <axum::Json<T> as FromRequest<S>>::from_request(req, state).await {
            Ok(axum::Json(value)) => Ok(Self(value)),
            Err(rejection) => Err(ApiError::validation(rejection.body_text())),
        }
    }
}

impl<T, S> OptionalFromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        match <axum::Json<T> as OptionalFromRequest<S>>::from_request(req, state).await {
            Ok(Some(axum::Json(value))) => Ok(Some(Self(value))),
            Ok(None) => Ok(None),
            Err(rejection) => Err(ApiError::validation(rejection.body_text())),
        }
    }
}

/// The authority an allowed agent call ran under: the grant that matched and
/// the context it carries into the audit record (permission-model.md
/// section 11). Humans act under their role and have none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Authority {
    /// `grt_` id of the matched grant.
    pub grant_id: Option<String>,
    /// The human whose authority the grant lends.
    pub on_behalf_of: Option<String>,
    /// The approval request behind an approval grant.
    pub approval_id: Option<String>,
    /// The incident the call belongs to: the `Quoro-Incident` header, or the
    /// incident an incident grant was issued for.
    pub incident_id: Option<String>,
    /// The event that opened that incident.
    pub trigger_event_id: Option<String>,
}

/// What an authorized read may return. Humans read everything their role
/// covers; an agent's list results are filtered by its grants' selectors,
/// never refused (permission-model.md section 5.1).
#[derive(Clone, Debug)]
pub enum ReadFilter {
    /// Every row.
    All,
    /// Rows matched by any of these selectors (the agent's usable grants
    /// with the scope).
    Selectors(Vec<authz::Selector>),
}

/// What a row is, for selector matching (permission-model.md section 5): its
/// own kind and id plus the app, project and node it belongs to, when it has
/// them. List operations describe every row with one, and
/// [`ReadAccess::filter`] keeps the rows the caller may see.
#[derive(Clone, Copy, Debug)]
pub struct Resource<'a> {
    /// `app`, `vm`, `deployment`, `domain`, `project`, `node`, `incident`,
    /// `approval`, `event`, `audit`, ...
    pub kind: &'a str,
    pub id: &'a str,
    pub app_id: Option<&'a str>,
    /// `prj_` id; selectors name projects by slug, and the filter resolves it.
    pub project_id: Option<&'a str>,
    pub node_id: Option<&'a str>,
}

/// Proof that a read was authorized, consumed by [`CallCtx::finish_read`] so
/// an agent's read is audited with its real outcome (audit-log-format.md
/// section 3: every agent call is recorded, reads included).
#[derive(Debug)]
#[must_use = "pass the access to CallCtx::finish_read so agent reads are audited"]
pub struct ReadAccess {
    pub filter: ReadFilter,
    scope: Scope,
    target: Option<Target>,
    project_id: Option<String>,
    params: Value,
}

impl ReadAccess {
    /// Keeps the rows of one page that the caller may see. Call it on the
    /// items of a page *after* the cursor was computed from the unfiltered
    /// rows, so a filtered page never ends the listing early. Humans and
    /// agents with an `{"all": true}` selector see every row.
    ///
    /// # Errors
    ///
    /// A database error while resolving project slugs.
    pub async fn filter<T>(
        &self,
        state: &AppState,
        items: Vec<T>,
        describe: impl Fn(&T) -> Resource<'_>,
    ) -> Result<Vec<T>, ApiError> {
        let selectors = match &self.filter {
            ReadFilter::All => return Ok(items),
            ReadFilter::Selectors(selectors) => selectors,
        };
        let mut slugs: HashMap<String, Option<String>> = HashMap::new();
        let mut kept = Vec::with_capacity(items.len());
        for item in items {
            let resource = describe(&item);
            let mut resolved = authz::ResolvedResource::default();
            let app_id = resource
                .app_id
                .or((resource.kind == "app").then_some(resource.id));
            let node_id = resource
                .node_id
                .or((resource.kind == "node").then_some(resource.id));
            let project_id = resource
                .project_id
                .or((resource.kind == "project").then_some(resource.id));
            if let Some(app_id) = app_id {
                resolved.apps.insert(app_id.to_string());
            }
            if let Some(node_id) = node_id {
                resolved.nodes.insert(node_id.to_string());
            }
            if let Some(project_id) = project_id {
                if !slugs.contains_key(project_id) {
                    let slug: Option<(String,)> =
                        sqlx::query_as("SELECT slug FROM projects WHERE id = ?")
                            .bind(project_id)
                            .fetch_optional(state.store.pool())
                            .await?;
                    slugs.insert(project_id.to_string(), slug.map(|s| s.0));
                }
                if let Some(Some(slug)) = slugs.get(project_id) {
                    resolved.project_slugs.insert(slug.clone());
                }
            }
            if selectors.iter().any(|s| s.matches(&resolved)) {
                kept.push(item);
            }
        }
        Ok(kept)
    }

    /// Whether the caller sees every row (a human, or an agent whose grant
    /// selector is `{"all": true}`). Cluster-level reads that cannot describe
    /// their rows as a [`Resource`] use it to decide what to return.
    #[must_use]
    pub fn sees_all(&self) -> bool {
        match &self.filter {
            ReadFilter::All => true,
            ReadFilter::Selectors(selectors) => selectors.iter().any(authz::Selector::is_all),
        }
    }

    /// Whether the caller's grants carry only `{}` selectors: a list then
    /// answers an empty page and a cluster-level read is refused with
    /// `selector_mismatch` (permission-model.md sections 5 and 5.1).
    #[must_use]
    pub fn reads_nothing(&self) -> bool {
        matches!(&self.filter, ReadFilter::Selectors(selectors) if selectors.is_empty())
    }

    /// Sets the project recorded on the agent's read audit record, once the
    /// read resolved the target.
    pub fn set_project(&mut self, project_id: Option<&str>) {
        self.project_id = project_id.map(str::to_string);
    }
}

/// Context of one authenticated call: who, through which channel, and which
/// operation. It is built by the adapter (HTTP here, MCP later) and handed to
/// the service layer, which authorizes through [`CallCtx::authorize`] or
/// [`CallCtx::authorize_read`] and ends every call in exactly one audit record
/// where the audit contract asks for one (D-0031: one authorization path, one
/// audit path).
#[derive(Clone, Debug)]
pub struct CallCtx {
    pub identity: Identity,
    /// How the call arrived (`audit_log.channel`).
    pub channel: Channel,
    pub request_id: String,
    /// Operation as recorded in the audit log: `POST /api/v1/vms/{vm_id}/start`
    /// over HTTP, the tool name (`restart_app`) over MCP.
    pub operation: String,
    pub justification: Option<String>,
    pub incident_id: Option<String>,
    /// Set once by a successful agent authorization.
    authority: Arc<OnceLock<Authority>>,
}

/// Section 7.2 step i: inside the action's transaction, re-reads the grant,
/// refuses if it stopped being usable since the decision (revoked, expired
/// or out of uses), and counts the use. The last use exhausts the grant and
/// emits `grant.expired` with `reason: max_uses`.
async fn consume_grant(
    conn: &mut SqliteConnection,
    agent: &AgentIdentity,
    grant_id: &str,
    scope: Option<Scope>,
    request_id: &str,
) -> Result<(), ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {} FROM grants WHERE id = ?",
        authz::GRANT_COLUMNS
    ))
    .bind(grant_id)
    .fetch_optional(&mut *conn)
    .await?;
    let scope_name = scope.map(Scope::as_str);
    let Some(row) = row else {
        return Err(ApiError::forbidden(Reason::GrantRevoked, scope_name));
    };
    let mut grant = authz::grant_from_row(&row)?;
    if let Some(reason) = grant.unusable(&now_rfc3339()) {
        return Err(ApiError::forbidden(reason, scope_name));
    }
    grant.uses += 1;
    let exhausted = grant.max_uses.is_some_and(|max| grant.uses >= max);
    if exhausted {
        grant.status = "exhausted".to_string();
    }
    sqlx::query("UPDATE grants SET uses = ?, status = ? WHERE id = ?")
        .bind(grant.uses)
        .bind(&grant.status)
        .bind(grant_id)
        .execute(&mut *conn)
        .await?;
    let actor = Actor::system("grants");
    changelog::append(
        conn,
        &actor,
        "grant",
        grant_id,
        Op::Update,
        json!({ "id": grant_id, "uses": grant.uses, "status": grant.status }),
    )
    .await?;
    if exhausted {
        events::emit(
            conn,
            NewEvent {
                kind: "grant.expired",
                source: Source::ControlPlane,
                subject_type: "grant",
                subject_id: grant_id,
                project_id: None,
                request_id: Some(request_id),
                actor: None,
                data: json!({
                    "grant_id": grant_id,
                    "agent_id": agent.agent_id,
                    "reason": "max_uses",
                }),
            },
        )
        .await?;
    }
    Ok(())
}

impl CallCtx {
    /// The context of an HTTP call: the channel follows the credential, and
    /// the justification and incident come from the `Quoro-Justification` and
    /// `Quoro-Incident` headers.
    #[must_use]
    pub fn new(
        identity: Identity,
        request_id: String,
        operation: &str,
        headers: &HeaderMap,
    ) -> Self {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .filter(|v| !v.is_empty())
        };
        let channel = identity.default_channel();
        Self::with_channel(
            identity,
            channel,
            request_id,
            operation,
            header("quoro-justification"),
            header("quoro-incident"),
        )
    }

    /// The context of a call from any adapter. The MCP adapter passes
    /// [`Channel::Mcp`], the tool name as `operation`, and the tool's
    /// `justification` and `incident_id` arguments.
    #[must_use]
    pub fn with_channel(
        identity: Identity,
        channel: Channel,
        request_id: String,
        operation: impl Into<String>,
        justification: Option<String>,
        incident_id: Option<String>,
    ) -> Self {
        Self {
            identity,
            channel,
            request_id,
            operation: operation.into(),
            justification,
            incident_id,
            authority: Arc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub fn channel(&self) -> Channel {
        self.channel
    }

    /// The authority an allowed agent call runs under, once authorized.
    #[must_use]
    pub fn authority(&self) -> Option<&Authority> {
        self.authority.get()
    }

    /// Authorizes a read (permission-model.md section 7.2). A human's read is
    /// decided by the role and never audited; an agent's read is refused and
    /// audited like any other refusal, and when allowed it must be finished
    /// with [`CallCtx::finish_read`], which audits the outcome.
    ///
    /// # Errors
    ///
    /// `forbidden` with the reason of section 7.4, or an internal error if the
    /// refusal could not be recorded.
    pub async fn authorize_read(
        &self,
        state: &AppState,
        scope: Scope,
        target: Option<&Target>,
        params: &Value,
    ) -> Result<ReadAccess, ApiError> {
        let access = ReadAccess {
            filter: ReadFilter::All,
            scope,
            target: target.cloned(),
            project_id: None,
            params: params.clone(),
        };
        match &self.identity.principal {
            Principal::User(user) => {
                if user.role.holds(scope) {
                    Ok(access)
                } else {
                    Err(ApiError::forbidden(
                        Reason::ScopeMissing,
                        Some(scope.as_str()),
                    ))
                }
            }
            Principal::Agent(agent) => {
                let allowed = match self.decide_agent(state, agent, scope, target, params).await {
                    Ok(allowed) => allowed,
                    Err(error) => {
                        self.audit_refusal(state, scope, target, None, params, &error)
                            .await?;
                        return Err(error);
                    }
                };
                let mut access = access;
                access.project_id = allowed.resolved.as_ref().and_then(|r| r.project_id.clone());
                if target.is_none() && !allowed.selectors.iter().any(authz::Selector::is_all) {
                    access.filter = ReadFilter::Selectors(allowed.selectors.clone());
                }
                if let Err(error) = self.adopt(state, &allowed).await {
                    let detail = error.audit_detail();
                    let record = self.record(
                        Some(scope),
                        target,
                        access.project_id.as_deref(),
                        params,
                        Outcome::Error,
                        Some(detail),
                        None,
                    );
                    let mut w = state.store.begin_write().await?;
                    audit::append(&mut w.tx, &record).await?;
                    w.commit().await?;
                    return Err(error);
                }
                Ok(access)
            }
        }
    }

    /// Runs the agent half of the decision procedure on its own connection.
    async fn decide_agent(
        &self,
        state: &AppState,
        agent: &AgentIdentity,
        scope: Scope,
        target: Option<&Target>,
        params: &Value,
    ) -> Result<authz::Allowed, ApiError> {
        let mut conn = state.store.pool().acquire().await?;
        authz::decide(&mut conn, agent, scope, target, params).await
    }

    /// Records the authority an allowed agent call runs under: the matched
    /// grant, its human, and the incident context (the `Quoro-Incident`
    /// header or the incident an incident grant was issued for, and the
    /// event that opened it).
    ///
    /// # Errors
    ///
    /// `validation_failed` for an incident id that names no incident.
    async fn adopt(&self, state: &AppState, allowed: &authz::Allowed) -> Result<(), ApiError> {
        let grant = allowed.grant.as_ref();
        let incident_id = self
            .incident_id
            .clone()
            .or_else(|| grant.and_then(|g| g.incident_id.clone()));
        let mut trigger_event_id = None;
        if let Some(incident_id) = &incident_id {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT trigger_event_id FROM incidents WHERE id = ?")
                    .bind(incident_id)
                    .fetch_optional(state.store.pool())
                    .await?;
            match row {
                Some((event_id,)) => trigger_event_id = Some(event_id),
                None => {
                    return Err(ApiError::validation(format!(
                        "incident {incident_id} does not exist"
                    )));
                }
            }
        }
        let authority = Authority {
            grant_id: grant.map(|g| g.id.clone()),
            on_behalf_of: grant.map(|g| g.on_behalf_of.clone()),
            approval_id: grant.and_then(|g| g.approval_id.clone()),
            incident_id,
            trigger_event_id,
        };
        // A context authorizes one call; a second authorization of the same
        // context keeps the first authority.
        let _ = self.authority.set(authority);
        Ok(())
    }

    /// Ends an authorized read: an agent's read is audited with `ok` or with
    /// the error code; a human's is not recorded. Returns `result` unchanged.
    ///
    /// # Errors
    ///
    /// The error in `result`, or an internal error if the record could not be
    /// written.
    pub async fn finish_read<T>(
        &self,
        state: &AppState,
        access: ReadAccess,
        result: Result<T, ApiError>,
    ) -> Result<T, ApiError> {
        if self.identity.is_human() {
            return result;
        }
        let (outcome, detail) = match &result {
            Ok(_) => (Outcome::Ok, None),
            Err(error) => (Outcome::Error, Some(error.audit_detail())),
        };
        let record = self.record(
            Some(access.scope),
            access.target.as_ref(),
            access.project_id.as_deref(),
            &access.params,
            outcome,
            detail,
            None,
        );
        let mut w = state.store.begin_write().await?;
        audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        result
    }

    /// Authorizes a mutating call (permission-model.md section 7.2). A human
    /// is decided by the role bundle; an agent by its grants, after the
    /// human-only and disabled checks. A refusal is audited with
    /// `result: refused` before it is returned. An allowed agent call must
    /// carry a justification, and its state change must be committed through
    /// [`CallCtx::append_ok`], which records the authority it ran under.
    ///
    /// # Errors
    ///
    /// `forbidden` with the reason of section 7.4, `approval_required` for a
    /// destructive scope, `validation_failed` for an agent call without a
    /// justification, or an internal error if the record could not be written.
    pub async fn authorize(
        &self,
        state: &AppState,
        scope: Scope,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
    ) -> Result<(), ApiError> {
        let error = match &self.identity.principal {
            Principal::User(user) => {
                if user.role.holds(scope) {
                    return Ok(());
                }
                ApiError::forbidden(Reason::ScopeMissing, Some(scope.as_str()))
            }
            Principal::Agent(agent) => {
                match self.decide_agent(state, agent, scope, target, params).await {
                    Ok(allowed) => {
                        let project_id = project_id.map(str::to_string).or_else(|| {
                            allowed.resolved.as_ref().and_then(|r| r.project_id.clone())
                        });
                        // Section 10 and the `Quoro-Justification` header: a
                        // mutating agent call states why, or it does not run.
                        let check = match self.adopt(state, &allowed).await {
                            Ok(()) if self.justification.is_none() => Err(ApiError::validation(
                                "a justification is required for an agent's mutating call",
                            )),
                            other => other,
                        };
                        if let Err(error) = check {
                            self.audit_error(
                                state,
                                Some(scope),
                                target,
                                project_id.as_deref(),
                                params,
                                &error,
                            )
                            .await?;
                            return Err(error);
                        }
                        return Ok(());
                    }
                    Err(error) => error,
                }
            }
        };
        self.audit_refusal(state, scope, target, project_id, params, &error)
            .await?;
        Err(error)
    }

    /// Authorizes a human-only mutating call (`README.md` special scope value
    /// `human`, for example `node:enroll`). An agent identity is refused with
    /// `reason: human_only`; the refusal is audited like any other.
    ///
    /// # Errors
    ///
    /// `forbidden` with `reason: human_only` or `scope_missing`, or an internal
    /// error if the refusal could not be recorded.
    pub async fn authorize_human(
        &self,
        state: &AppState,
        scope: Scope,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
    ) -> Result<(), ApiError> {
        if !self.identity.is_human() {
            let error = ApiError::forbidden(Reason::HumanOnly, Some(scope.as_str()));
            self.audit_refusal(state, scope, target, project_id, params, &error)
                .await?;
            return Err(error);
        }
        self.authorize(state, scope, target, project_id, params)
            .await
    }

    /// Writes the `ok` audit record of an authorized mutating call inside the
    /// call's own write transaction (permission-model.md section 7.3,
    /// audit-log-format.md section 3.1), and returns its audit `seq`. For an
    /// agent this is also where the grant's use is consumed, so the state
    /// change, the use and the record commit together or not at all.
    ///
    /// # Errors
    ///
    /// A database error.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_ok(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<Scope>,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
        change_seq: Option<i64>,
    ) -> Result<i64, ApiError> {
        if let (Some(agent), Some(grant_id)) = (
            self.identity.agent(),
            self.authority().and_then(|a| a.grant_id.as_deref()),
        ) {
            consume_grant(conn, agent, grant_id, scope, &self.request_id).await?;
        }
        let record = self.record(
            scope,
            target,
            project_id,
            params,
            Outcome::Ok,
            None,
            change_seq,
        );
        Ok(audit::append(conn, &record).await?)
    }

    async fn audit_refusal(
        &self,
        state: &AppState,
        scope: Scope,
        target: Option<&Target>,
        project_id: Option<&str>,
        params: &Value,
        error: &ApiError,
    ) -> Result<(), ApiError> {
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
        Ok(())
    }

    /// The audit record for this call, with the agent fields of
    /// permission-model.md section 11 filled from the identity and the
    /// authority the call ran under.
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
        let authority = self.authority.get();
        let from_authority =
            |field: fn(&Authority) -> &Option<String>| authority.and_then(|a| field(a).clone());
        Record {
            actor: self.identity.actor(),
            agent_model: self.identity.agent().map(|agent| agent.model.clone()),
            on_behalf_of: from_authority(|a| &a.on_behalf_of),
            channel: self.channel,
            operation: self.operation.clone(),
            scope: scope.map(|s| s.as_str().to_string()),
            target_type: target.map(|t| t.kind.clone()),
            target_id: target.map(|t| t.id.clone()),
            project_id: project_id.map(str::to_string),
            params: params.clone(),
            // Humans may omit the justification; it is recorded when given.
            justification: self.justification.clone(),
            trigger_event_id: from_authority(|a| &a.trigger_event_id),
            incident_id: from_authority(|a| &a.incident_id).or_else(|| self.incident_id.clone()),
            grant_id: from_authority(|a| &a.grant_id),
            approval_id: from_authority(|a| &a.approval_id),
            request_id: self.request_id.clone(),
            result,
            result_detail,
            change_seq,
        }
    }

    /// Records an idempotent replay (`IdempotencyKey`: D-0031 records the call
    /// either way): `ok`, with no change-log link because nothing changed.
    ///
    /// # Errors
    ///
    /// An internal error if the record could not be written.
    pub async fn audit_replay(
        &self,
        state: &AppState,
        scope: Scope,
        target: &Target,
        project_id: Option<&str>,
        params: &Value,
    ) -> Result<(), ApiError> {
        let record = self.record(
            Some(scope),
            Some(target),
            project_id,
            params,
            Outcome::Ok,
            None,
            None,
        );
        let mut w = state.store.begin_write().await?;
        audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        Ok(())
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
        // A refusal found inside the transaction (the grant was revoked or
        // ran out between authorization and commit, section 7.3) is a
        // refusal, not an error.
        let outcome = match error.code {
            Code::Forbidden | Code::ApprovalRequired => Outcome::Refused,
            _ => Outcome::Error,
        };
        let record = self.record(
            scope,
            target,
            project_id,
            params,
            outcome,
            Some(error.audit_detail()),
            None,
        );
        let mut w = state.store.begin_write().await?;
        audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        Ok(())
    }
}

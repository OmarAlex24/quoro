// SPDX-License-Identifier: AGPL-3.0-only
//! Node enrollment (`node-agent-protocol.md` section 2).
//!
//! A human with `node:enroll` creates a one-time join token; the node proves
//! possession of it and presents its Ed25519 public key, and receives a node
//! credential (32 random bytes, base64) plus the cluster public key. The
//! primary stores only the SHA-256 of the credential, in the local
//! `node_credentials` table (migration 0002), so the hash never enters the
//! change log.
//!
//! The primary's own node agent enrolls through the same endpoint with the
//! token in `<data_dir>/local-node-join.json` ([`ensure_local_join`]): at first
//! start there is no user to create a join token.

use crate::AppState;
use crate::api::nodes::{KvmInfo, NodeCapacity, NodeRole};
use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, ActorType, Op};
use crate::error::{ApiError, Target};
use crate::events::{self, NewEvent, Source};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::store::{now_rfc3339, rfc3339_in, sha256_hex};
use axum::Json;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use quoro_types::{Prefix, Scope, new_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use utoipa::ToSchema;

/// Name of the local node's one-time join file.
pub const LOCAL_JOIN_FILE: &str = "local-node-join.json";

/// Default lifetime of a join token (`node-agent-protocol.md` section 2.1).
const JOIN_TOKEN_TTL_SECONDS: i64 = 60 * 60;

/// Longest lifetime a caller may ask for.
const JOIN_TOKEN_MAX_SECONDS: i64 = 7 * 24 * 60 * 60;

/// Audit operation of the join token endpoint.
const CREATE_JOIN_TOKEN: &str = "POST /api/v1/nodes/join-tokens";

// ---------------------------------------------------------------------------
// Schemas (`openapi.yaml`)
// ---------------------------------------------------------------------------

/// Body of `createNodeJoinToken`; the whole body is optional.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(title = "NodeJoinTokenRequest")]
pub struct NodeJoinTokenRequest {
    /// Expiry of the token. Defaults to 1 hour after creation.
    #[serde(default)]
    #[schema(format = "date-time", nullable = false)]
    pub expires_at: Option<String>,
}

/// The new join token; `token` is shown only in this response.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "NodeJoinTokenCreated")]
pub struct NodeJoinTokenCreated {
    /// Join token id.
    #[schema(pattern = "^njt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// The token secret. Shown only in this response.
    #[schema(read_only)]
    pub token: String,
    /// Expiry of the token.
    #[schema(format = "date-time")]
    pub expires_at: String,
    /// One-line command that enrolls the node and starts the node agent.
    pub join_command: String,
}

/// Body of `enrollNode`.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "EnrollNodeRequest")]
pub struct EnrollNodeRequest {
    /// The one-time join token secret.
    #[schema(write_only)]
    pub join_token: String,
    /// Node name, unique in the cluster.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Ed25519 public key of the node, base64.
    pub public_key: String,
    /// Roles the node will run.
    #[schema(inline)]
    pub roles: Vec<NodeRole>,
    /// Capacity of the node. Omitted means zeros.
    #[serde(default)]
    #[schema(nullable = false)]
    pub capacity: Option<NodeCapacity>,
    /// Result of the KVM probe. Omitted means unavailable, empty kernel.
    #[serde(default)]
    #[schema(nullable = false)]
    pub kvm: Option<KvmInfo>,
}

/// Enrollment result; `credential` is shown only in this response.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "EnrollNodeResponse")]
pub struct EnrollNodeResponse {
    /// Id of the enrolled node.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Id of the cluster the node joined.
    #[schema(pattern = "^cls_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub cluster_id: String,
    /// Base URL the node uses to dial the primary.
    #[schema(format = "uri")]
    pub primary_url: String,
    /// Node session credential. Shown only in this response.
    #[schema(read_only)]
    pub credential: String,
    /// Base64 Ed25519 public key of the cluster.
    #[schema(read_only)]
    pub cluster_public_key: String,
}

/// The local node's one-time join file.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalJoin {
    url: String,
    token: String,
    fingerprint: String,
}

// ---------------------------------------------------------------------------
// Join tokens
// ---------------------------------------------------------------------------

/// `POST /api/v1/nodes/join-tokens`.
///
/// # Errors
///
/// `forbidden` without `node:enroll` (an agent is `human_only`),
/// `validation_failed` for a bad expiry or body, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/nodes/join-tokens",
    tag = "nodes",
    operation_id = "createNodeJoinToken",
    extensions(("x-quoro-scope" = json!("node:enroll"))),
    params(
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second resource (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
    ),
    request_body(content = NodeJoinTokenRequest, content_type = "application/json",
        description = "Optional expiry of the token."),
    responses(
        (status = 201, description = "The token and the join command; the secret is in this response only.",
            body = NodeJoinTokenCreated,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_join_token(
    State(state): State<AppState>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    body: Option<ApiJson<NodeJoinTokenRequest>>,
) -> Result<Response, ApiError> {
    let body = body.map(|ApiJson(body)| body).unwrap_or_default();
    let params = serde_json::to_value(&body).unwrap_or(Value::Null);
    let ctx = CallCtx::new(identity, request_id, CREATE_JOIN_TOKEN, &headers);
    ctx.authorize_human(&state, Scope::NodeEnroll, None, None, &params)
        .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_JOIN_TOKEN, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, body) => {
                let id = body["id"].as_str().unwrap_or_default();
                ctx.audit_replay(&state, Scope::NodeEnroll, &token_target(id), None, &params)
                    .await?;
                return Ok((status, Json(body)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(&state, Some(Scope::NodeEnroll), None, None, &params, &error)
                    .await?;
                return Err(error);
            }
        }
    }

    let result = issue_token(&state, &ctx, &body, &params, &headers).await;
    match result {
        Ok(created) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_JOIN_TOKEN,
                    key,
                    StatusCode::CREATED,
                    serde_json::to_value(&created).unwrap_or(Value::Null),
                );
            }
            Ok((StatusCode::CREATED, Json(created)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_JOIN_TOKEN, key);
            }
            ctx.audit_error(&state, Some(Scope::NodeEnroll), None, None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

fn token_target(id: &str) -> Target {
    Target {
        kind: "node_join_token".to_string(),
        id: id.to_string(),
    }
}

/// The `Idempotency-Key` of the call, when present and well-formed.
fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
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

/// Validates and stores one join token.
async fn issue_token(
    state: &AppState,
    ctx: &CallCtx,
    body: &NodeJoinTokenRequest,
    params: &Value,
    headers: &HeaderMap,
) -> Result<NodeJoinTokenCreated, ApiError> {
    let expires_at = expiry(body)?;
    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(|e| ApiError::internal(&e))?;
    let secret = format!("njt_{}", URL_SAFE_NO_PAD.encode(random));
    let id = new_id(Prefix::NodeJoinToken);
    let token_hash = sha256_hex(secret.as_bytes());

    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "INSERT INTO node_join_tokens (id, token_hash, created_by, expires_at, used_at, node_id) \
         VALUES (?, ?, ?, ?, NULL, NULL)",
    )
    .bind(&id)
    .bind(&token_hash)
    .bind(ctx.identity.id())
    .bind(&expires_at)
    .execute(&mut *w.tx)
    .await?;

    // The change log carries the token row without `token_hash`: the log
    // reaches every following node, and a hash of a usable secret has no
    // business there.
    let change_seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "node_join_token",
        &id,
        Op::Create,
        json!({
            "id": id,
            "created_by": ctx.identity.id(),
            "expires_at": expires_at,
            "used_at": null,
            "node_id": null,
        }),
    )
    .await?;

    let target = token_target(&id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::NodeEnroll),
        Some(&target),
        None,
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;

    let primary_url = state.config.primary_url(headers);
    Ok(NodeJoinTokenCreated {
        id,
        token: secret.clone(),
        expires_at,
        join_command: format!(
            "quoro join {primary_url} --token {secret} --fingerprint {}",
            state.config.tls_fingerprint
        ),
    })
}

/// Normalizes the requested expiry to the store's fixed-width RFC 3339 form.
fn expiry(body: &NodeJoinTokenRequest) -> Result<String, ApiError> {
    let Some(text) = &body.expires_at else {
        return Ok(rfc3339_in(JOIN_TOKEN_TTL_SECONDS));
    };
    let parsed = OffsetDateTime::parse(text, &Rfc3339)
        .map_err(|_| ApiError::validation("expires_at is not an RFC 3339 date-time"))?;
    let delta = parsed - OffsetDateTime::now_utc();
    if delta <= time::Duration::ZERO {
        return Err(ApiError::validation("expires_at is in the past"));
    }
    if delta.whole_seconds() > JOIN_TOKEN_MAX_SECONDS {
        return Err(ApiError::validation(
            "expires_at is more than 7 days in the future",
        ));
    }
    Ok(rfc3339_in(delta.whole_seconds().max(1)))
}

// ---------------------------------------------------------------------------
// Enrollment
// ---------------------------------------------------------------------------

/// `POST /api/v1/nodes/enroll`.
///
/// # Errors
///
/// `validation_failed` for a bad body, an unknown or expired token or a bad
/// public key; `conflict` for a used token or a duplicate node name; or a
/// database error.
#[utoipa::path(
    post,
    path = "/api/v1/nodes/enroll",
    tag = "nodes",
    operation_id = "enrollNode",
    security(()),
    extensions(("x-quoro-scope" = json!("public"))),
    params(
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second resource (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
    ),
    request_body(content = EnrollNodeRequest, content_type = "application/json",
        description = "Join token, node facts and public key."),
    responses(
        (status = 201, description = "The node is enrolled; the credential is in this response only.",
            body = EnrollNodeResponse,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn enroll_node(
    State(state): State<AppState>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<EnrollNodeRequest>,
) -> Result<Response, ApiError> {
    validate(&body)?;
    let primary_url = state.config.primary_url(&headers);
    let token = resolve_token(&state, &body.join_token).await?;
    let outcome = enroll(&state, &body, token, &request_id).await?;

    if outcome.local {
        match std::fs::remove_file(local_join_path(&state.config.data_dir)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(%e, "cannot remove the local node join file"),
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(EnrollNodeResponse {
            node_id: outcome.node_id,
            cluster_id: state.cluster_id.clone(),
            primary_url,
            credential: outcome.credential,
            cluster_public_key: state.keys.public_key_base64(),
        }),
    )
        .into_response())
}

fn validate(body: &EnrollNodeRequest) -> Result<(), ApiError> {
    let name_len = body.name.chars().count();
    if !(1..=200).contains(&name_len) {
        return Err(ApiError::validation(format!(
            "name has {name_len} characters, expected 1-200"
        )));
    }
    quoro_proto::sign::public_key_from_base64(&body.public_key)
        .map_err(|_| ApiError::validation("public_key is not a base64 Ed25519 key"))?;
    if body.roles.is_empty() {
        return Err(ApiError::validation("roles must not be empty"));
    }
    let mut seen: Vec<NodeRole> = Vec::new();
    for role in &body.roles {
        if seen.contains(role) {
            return Err(ApiError::validation("roles contains a duplicate"));
        }
        seen.push(*role);
    }
    Ok(())
}

/// Where the presented join token came from.
enum JoinToken {
    /// A row of `node_join_tokens`.
    Row {
        id: String,
        created_by: String,
        expires_at: String,
    },
    /// The primary's own token from `local-node-join.json`.
    Local,
}

/// Looks the presented secret up by hash. The local bootstrap token is not a
/// database row: there is no user at first start to create one.
async fn resolve_token(state: &AppState, presented: &str) -> Result<JoinToken, ApiError> {
    let row: Option<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT id, created_by, expires_at, used_at FROM node_join_tokens WHERE token_hash = ?",
    )
    .bind(sha256_hex(presented.as_bytes()))
    .fetch_optional(state.store.pool())
    .await?;
    if let Some((id, created_by, expires_at, used_at)) = row {
        if used_at.is_some() {
            return Err(ApiError::conflict("the join token was already used"));
        }
        if expires_at <= now_rfc3339() {
            return Err(ApiError::validation("the join token has expired"));
        }
        return Ok(JoinToken::Row {
            id,
            created_by,
            expires_at,
        });
    }

    if local_token_matches(&state.config.data_dir, presented) {
        // The file is deleted after a successful local enrollment; a stale
        // copy must not mint a second credential for an enrolled primary.
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT c.node_id FROM node_credentials c JOIN nodes n ON n.id = c.node_id \
             WHERE n.cp_status = 'primary' AND c.revoked_at IS NULL LIMIT 1",
        )
        .fetch_optional(state.store.pool())
        .await?;
        if existing.is_some() {
            return Err(ApiError::validation(
                "the primary already holds a node credential",
            ));
        }
        return Ok(JoinToken::Local);
    }
    Err(ApiError::validation("the join token is unknown"))
}

/// Whether `presented` is the local node token. The comparison is over the
/// SHA-256 digests, never the strings.
fn local_token_matches(data_dir: &Path, presented: &str) -> bool {
    let Ok(bytes) = std::fs::read(local_join_path(data_dir)) else {
        return false;
    };
    let Ok(stored) = serde_json::from_slice::<LocalJoin>(&bytes) else {
        return false;
    };
    sha256_hex(presented.as_bytes()) == sha256_hex(stored.token.as_bytes())
}

struct Enrolled {
    node_id: String,
    credential: String,
    local: bool,
}

/// Inserts or adopts the node row, consumes the token, stores the credential
/// hash, appends the change-log entries and the audit record, and emits
/// `node.enrolled`, all in one transaction.
async fn enroll(
    state: &AppState,
    body: &EnrollNodeRequest,
    token: JoinToken,
    request_id: &str,
) -> Result<Enrolled, ApiError> {
    let local = matches!(token, JoinToken::Local);
    let capacity = body.capacity.unwrap_or(NodeCapacity {
        cpus: 0,
        memory_mb: 0,
        disk_gb: 0,
    });
    let kvm = body.kvm.clone().unwrap_or(KvmInfo {
        available: false,
        kernel: String::new(),
        nested: None,
    });
    let capacity_json = serde_json::to_string(&capacity).map_err(|e| ApiError::internal(&e))?;
    let kvm_json = serde_json::to_string(&kvm).map_err(|e| ApiError::internal(&e))?;
    let roles_json = serde_json::to_string(&body.roles).map_err(|e| ApiError::internal(&e))?;
    let allocated = json!({ "cpus": 0, "memory_mb": 0, "disk_gb": 0 }).to_string();

    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(|e| ApiError::internal(&e))?;
    let credential = STANDARD.encode(random);
    let credential_hash = sha256_hex(credential.as_bytes());
    let now = now_rfc3339();

    let mut w = state.store.begin_write().await?;

    // The primary's own agent adopts the existing primary row (a v0.0.1 node
    // upgraded in place) instead of becoming a second primary.
    let primary: Option<(String,)> =
        sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary' LIMIT 1")
            .fetch_optional(&mut *w.tx)
            .await?;
    let adopted_id = if local { primary.map(|(id,)| id) } else { None };

    let name_owner: Option<(String,)> =
        sqlx::query_as("SELECT id FROM nodes WHERE name = ? AND status <> 'removed' LIMIT 1")
            .bind(&body.name)
            .fetch_optional(&mut *w.tx)
            .await?;
    if let Some((id,)) = &name_owner
        && Some(id) != adopted_id.as_ref()
    {
        return Err(ApiError::conflict("a node with this name already exists"));
    }

    let (node_id, op) = if let Some(id) = adopted_id {
        sqlx::query(
            "UPDATE nodes SET status = 'pending', public_key = ?, roles = ?, behind_nat = 1, \
             labels = '{}', capacity = ?, allocated = ?, kvm = ?, desired_rev = 0 WHERE id = ?",
        )
        .bind(&body.public_key)
        .bind(&roles_json)
        .bind(&capacity_json)
        .bind(&allocated)
        .bind(&kvm_json)
        .bind(&id)
        .execute(&mut *w.tx)
        .await?;
        (id, Op::Update)
    } else {
        let id = new_id(Prefix::Node);
        let cp_status = if local { Some("primary") } else { None };
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at) \
             VALUES (?, ?, ?, ?, 'pending', ?, NULL, 1, '{}', ?, ?, ?, NULL, NULL, 0, 0, 0, NULL, ?)",
        )
        .bind(&id)
        .bind(&body.name)
        .bind(&roles_json)
        .bind(cp_status)
        .bind(&body.public_key)
        .bind(&capacity_json)
        .bind(&allocated)
        .bind(&kvm_json)
        .bind(&now)
        .execute(&mut *w.tx)
        .await?;
        (id, Op::Create)
    };

    // Every machine with the `node` role owns a guest subnet (ADR 0144). An
    // adopted row that already has one keeps it.
    if body.roles.contains(&NodeRole::Node) {
        let current: Option<String> = sqlx::query_scalar("SELECT subnet FROM nodes WHERE id = ?")
            .bind(&node_id)
            .fetch_one(&mut *w.tx)
            .await?;
        if current.is_none() {
            let taken: Vec<(String,)> =
                sqlx::query_as("SELECT subnet FROM nodes WHERE subnet IS NOT NULL")
                    .fetch_all(&mut *w.tx)
                    .await?;
            let taken: Vec<String> = taken.into_iter().map(|(subnet,)| subnet).collect();
            let subnet = crate::subnet::allocate(&state.config.node_network.subnet, &taken)
                .map_err(ApiError::conflict)?;
            sqlx::query("UPDATE nodes SET subnet = ? WHERE id = ?")
                .bind(&subnet)
                .bind(&node_id)
                .execute(&mut *w.tx)
                .await?;
            changelog::append(
                &mut w.tx,
                &Actor {
                    kind: ActorType::Node,
                    id: node_id.clone(),
                },
                "node",
                &node_id,
                Op::Update,
                json!({ "id": node_id, "subnet": subnet }),
            )
            .await?;
        }
    }

    if let JoinToken::Row {
        id: token_id,
        created_by,
        expires_at,
    } = token
    {
        // The token was checked before this transaction; a concurrent
        // enrollment with the same token may have used it since, so the update
        // itself is the single-use gate.
        let used = sqlx::query(
            "UPDATE node_join_tokens SET used_at = ?, node_id = ? WHERE id = ? AND used_at IS NULL",
        )
        .bind(&now)
        .bind(&node_id)
        .bind(&token_id)
        .execute(&mut *w.tx)
        .await?;
        if used.rows_affected() != 1 {
            return Err(ApiError::conflict("the join token was already used"));
        }
        // Same rule as the insert: the token row travels without its hash.
        changelog::append(
            &mut w.tx,
            &Actor {
                kind: ActorType::Node,
                id: node_id.clone(),
            },
            "node_join_token",
            &token_id,
            Op::Update,
            json!({
                "id": token_id,
                "created_by": created_by,
                "expires_at": expires_at,
                "used_at": now,
                "node_id": node_id,
            }),
        )
        .await?;
    }

    // LOCAL table: the credential hash is never a change-log payload.
    sqlx::query(
        "INSERT INTO node_credentials (node_id, credential_hash, created_at, revoked_at) \
         VALUES (?, ?, ?, NULL) \
         ON CONFLICT (node_id) DO UPDATE SET credential_hash = excluded.credential_hash, \
         created_at = excluded.created_at, revoked_at = NULL",
    )
    .bind(&node_id)
    .bind(&credential_hash)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;

    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.clone(),
    };
    let payload = crate::driver::node_payload(&mut w.tx, &node_id)
        .await
        .map_err(|e| ApiError::internal(&format!("{e:?}")))?;
    changelog::append(&mut w.tx, &actor, "node", &node_id, op, payload).await?;

    let name: String = sqlx::query_scalar("SELECT name FROM nodes WHERE id = ?")
        .bind(&node_id)
        .fetch_one(&mut *w.tx)
        .await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "node.enrolled",
            source: Source::ControlPlane,
            subject_type: "node",
            subject_id: &node_id,
            project_id: None,
            request_id: Some(request_id),
            actor: Some(&actor),
            data: json!({
                "node_id": node_id,
                "name": name,
                "roles": body.roles,
                "address": null,
            }),
        },
    )
    .await?;

    // Enrollment carries the join token secret, so the audit params name the
    // node facts only.
    let record = Record {
        actor: actor.clone(),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::Api,
        operation: "enroll_node".to_string(),
        scope: None,
        target_type: Some("node".to_string()),
        target_id: Some(node_id.clone()),
        project_id: None,
        params: json!({
            "name": body.name,
            "roles": body.roles,
            "capacity": capacity_json,
            "kvm": kvm_json,
        }),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: request_id.to_string(),
        result: Outcome::Ok,
        result_detail: None,
        change_seq: None,
    };
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;

    Ok(Enrolled {
        node_id,
        credential,
        local,
    })
}

// ---------------------------------------------------------------------------
// The primary's own join file
// ---------------------------------------------------------------------------

fn local_join_path(data_dir: &Path) -> PathBuf {
    data_dir.join(LOCAL_JOIN_FILE)
}

/// Writes `<data_dir>/local-node-join.json` (mode `0600`, atomic) when it does
/// not exist, so the primary's own agent can enroll at first start.
///
/// A successful enrollment deletes the file, so a node that already holds a
/// credential never gets a new one; this is the "no primary credential yet"
/// gate in practice.
///
/// # Errors
///
/// An I/O error writing the file.
pub fn ensure_local_join(data_dir: &Path, url: &str, fingerprint: &str) -> std::io::Result<()> {
    let final_path = local_join_path(data_dir);
    if final_path.exists() {
        return Ok(());
    }
    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(std::io::Error::other)?;
    let stored = LocalJoin {
        url: url.to_string(),
        token: format!("njt_{}", URL_SAFE_NO_PAD.encode(random)),
        fingerprint: fingerprint.to_string(),
    };
    let body = serde_json::to_vec(&stored).map_err(std::io::Error::other)?;
    let tmp = data_dir.join(format!(
        "{LOCAL_JOIN_FILE}.tmp.{}",
        &crate::store::random_secret()[..16]
    ));
    let _ = std::fs::remove_file(&tmp);
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(&body)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)
}

/// The local join file's token, for the binary's own agent to enroll with.
#[must_use]
pub fn local_join_token(data_dir: &Path) -> Option<String> {
    let bytes = std::fs::read(local_join_path(data_dir)).ok()?;
    let stored: LocalJoin = serde_json::from_slice(&bytes).ok()?;
    Some(stored.token)
}

//! The agent half of the decision procedure (permission-model.md section 7.2,
//! step 3). [`crate::request::CallCtx`] is the only caller: it handles the
//! human branch, audits every refusal and records the authority an allowed
//! call ran under.

use crate::auth::AgentIdentity;
use crate::error::{ApiError, Code, Reason, Target};
use crate::jcs;
use crate::store::{now_rfc3339, sha256_hex};
use quoro_types::Scope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, SqliteConnection};
use std::collections::BTreeSet;

/// The `approval_required` problem: it carries the destructive scope and the
/// target so the agent can file an approval request without guessing
/// (permission-model.md section 7.4).
#[must_use]
pub fn approval_required(scope: Scope, target: Option<&Target>) -> ApiError {
    let mut error = ApiError::new(
        Code::ApprovalRequired,
        format!(
            "{} is destructive: file an approval request and repeat the call once a human approves it",
            scope.as_str()
        ),
    );
    error.scope = Some(scope.as_str().to_string());
    error.target = target.cloned();
    error
}

/// A grant's resource selector (permission-model.md section 5). Unknown keys
/// are rejected when a grant is created.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projects: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apps: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all: Option<bool>,
}

impl Selector {
    /// `{"all": true}`.
    #[must_use]
    pub fn is_all(&self) -> bool {
        self.all == Some(true)
    }

    /// Whether the selector lists nothing: `{}` matches nothing (section 5).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.is_all()
            && self.projects.as_ref().is_none_or(Vec::is_empty)
            && self.apps.as_ref().is_none_or(Vec::is_empty)
            && self.nodes.as_ref().is_none_or(Vec::is_empty)
    }

    /// Whether `resource` matches any entry (entries are OR-ed).
    #[must_use]
    pub fn matches(&self, resource: &ResolvedResource) -> bool {
        if self.is_all() {
            return true;
        }
        let hit = |list: &Option<Vec<String>>, set: &BTreeSet<String>| {
            list.as_ref()
                .is_some_and(|l| l.iter().any(|v| set.contains(v)))
        };
        hit(&self.apps, &resource.apps)
            || hit(&self.projects, &resource.project_slugs)
            || hit(&self.nodes, &resource.nodes)
    }
}

/// A resource with the apps, project slugs and nodes a selector can name it by
/// (section 5, "Matching per target type").
#[derive(Clone, Debug, Default)]
pub struct ResolvedResource {
    pub apps: BTreeSet<String>,
    pub project_slugs: BTreeSet<String>,
    pub nodes: BTreeSet<String>,
    /// `prj_` id, recorded on the audit record when known.
    pub project_id: Option<String>,
}

fn one(value: Option<String>) -> BTreeSet<String> {
    value.into_iter().collect()
}

/// Resolves what a selector can match `target` by. A target whose row does
/// not exist, or whose kind has no selector form (agent, grant, webhook,
/// user, cluster, ...), resolves to nothing, so only `{"all": true}` covers it.
///
/// # Errors
///
/// A database error.
pub fn resolve<'c>(
    conn: &'c mut SqliteConnection,
    kind: &'c str,
    id: &'c str,
    depth: u8,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ResolvedResource, ApiError>> + Send + 'c>,
> {
    Box::pin(async move {
        let mut out = ResolvedResource::default();
        match kind {
            "app" => {
                let row: Option<(String, String)> = sqlx::query_as(
                    "SELECT a.project_id, p.slug FROM apps a JOIN projects p ON p.id = a.project_id WHERE a.id = ?",
                )
                .bind(id)
                .fetch_optional(&mut *conn)
                .await?;
                if let Some((project_id, slug)) = row {
                    out.apps = one(Some(id.to_string()));
                    out.project_slugs = one(Some(slug));
                    out.project_id = Some(project_id);
                }
            }
            "vm" => {
                let row: Option<(Option<String>, String, String, String)> = sqlx::query_as(
                    "SELECT v.app_id, v.project_id, p.slug, v.node_id FROM vms v JOIN projects p ON p.id = v.project_id WHERE v.id = ?",
                )
                .bind(id)
                .fetch_optional(&mut *conn)
                .await?;
                if let Some((app_id, project_id, slug, node_id)) = row {
                    out.apps = one(app_id);
                    out.project_slugs = one(Some(slug));
                    out.nodes = one(Some(node_id));
                    out.project_id = Some(project_id);
                }
            }
            "deployment" | "domain" | "build" => {
                let table = match kind {
                    "deployment" => "deployments",
                    "build" => "builds",
                    _ => "domains",
                };
                let app: Option<(String,)> =
                    sqlx::query_as(&format!("SELECT app_id FROM {table} WHERE id = ?"))
                        .bind(id)
                        .fetch_optional(&mut *conn)
                        .await?;
                if let Some((app_id,)) = app {
                    return resolve(conn, "app", &app_id, depth).await;
                }
            }
            "project" => {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT slug FROM projects WHERE id = ?")
                        .bind(id)
                        .fetch_optional(&mut *conn)
                        .await?;
                if let Some((slug,)) = row {
                    out.project_slugs = one(Some(slug));
                    out.project_id = Some(id.to_string());
                }
            }
            "node" => {
                let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM nodes WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&mut *conn)
                    .await?;
                out.nodes = one(exists.map(|r| r.0));
            }
            "incident" | "approval" if depth == 0 => {
                let sql = if kind == "incident" {
                    "SELECT subject FROM incidents WHERE id = ?"
                } else {
                    "SELECT target FROM approvals WHERE id = ?"
                };
                let row: Option<(String,)> = sqlx::query_as(sql)
                    .bind(id)
                    .fetch_optional(&mut *conn)
                    .await?;
                if let Some((json,)) = row {
                    let inner: Value = serde_json::from_str(&json).unwrap_or(Value::Null);
                    if let (Some(k), Some(i)) = (inner["type"].as_str(), inner["id"].as_str()) {
                        let (k, i) = (k.to_string(), i.to_string());
                        return resolve(conn, &k, &i, depth + 1).await;
                    }
                }
            }
            _ => {}
        }
        Ok(out)
    })
}

/// One `grants` row, as the decision procedure reads it.
#[derive(Clone, Debug)]
pub struct GrantRow {
    pub id: String,
    pub kind: String,
    pub scopes: Vec<String>,
    pub selector: Selector,
    pub expires_at: String,
    pub max_uses: Option<i64>,
    pub uses: i64,
    pub status: String,
    pub incident_id: Option<String>,
    pub approval_id: Option<String>,
    pub on_behalf_of: String,
    pub created_at: String,
}

pub const GRANT_COLUMNS: &str = "id, kind, scopes, selector, expires_at, max_uses, uses, status, \
     incident_id, approval_id, on_behalf_of, created_at";

/// Maps a row selected with [`GRANT_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON columns do not parse.
pub fn grant_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<GrantRow, ApiError> {
    let scopes: String = row.try_get("scopes")?;
    let selector: String = row.try_get("selector")?;
    Ok(GrantRow {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        scopes: serde_json::from_str(&scopes)
            .map_err(|e| ApiError::internal(&format!("grant scopes: {e}")))?,
        // A stored selector the type cannot read matches nothing.
        selector: serde_json::from_str(&selector).unwrap_or_default(),
        expires_at: row.try_get("expires_at")?,
        max_uses: row.try_get("max_uses")?,
        uses: row.try_get("uses")?,
        status: row.try_get("status")?,
        incident_id: row.try_get("incident_id")?,
        approval_id: row.try_get("approval_id")?,
        on_behalf_of: row.try_get("on_behalf_of")?,
        created_at: row.try_get("created_at")?,
    })
}

impl GrantRow {
    fn has(&self, scope: Scope) -> bool {
        self.scopes.iter().any(|s| s == scope.as_str())
    }

    /// Why the grant cannot authorize at `now`, or `None` when it is usable
    /// (section 7.2 step c). Expiry is checked first, so a grant past its TTL
    /// answers `grant_expired` whatever its stored status.
    #[must_use]
    pub fn unusable(&self, now: &str) -> Option<Reason> {
        if self.status == "expired" || self.expires_at.as_str() <= now {
            Some(Reason::GrantExpired)
        } else if self.status == "revoked" {
            Some(Reason::GrantRevoked)
        } else if self.status == "exhausted" || self.max_uses.is_some_and(|m| self.uses >= m) {
            Some(Reason::GrantExhausted)
        } else if self.status != "active" {
            Some(Reason::GrantRevoked)
        } else {
            None
        }
    }
}

/// The reason an inactive grant gives, in the order of section 7.4.
fn inactive_reason<'a>(grants: impl Iterator<Item = &'a GrantRow>, now: &str) -> Option<Reason> {
    let rank = |r: Reason| match r {
        Reason::GrantExpired => 0,
        Reason::GrantRevoked => 1,
        _ => 2,
    };
    grants
        .filter_map(|g| g.unusable(now))
        .min_by_key(|r| rank(*r))
}

/// The outcome of an allowed agent decision.
#[derive(Clone, Debug)]
pub struct Allowed {
    /// The grant that authorizes the call (step h).
    pub grant: Option<GrantRow>,
    /// For list calls (no target): the selectors of every usable grant with
    /// the scope; the read path filters rows by them.
    pub selectors: Vec<Selector>,
    pub resolved: Option<ResolvedResource>,
}

/// Steps a-h of section 7.2 for an agent and one scope. `target` is `None`
/// for list and cluster-level calls.
///
/// # Errors
///
/// The refusal of section 7.4 (never audited here; the caller audits it), or
/// a database error.
pub async fn decide(
    conn: &mut SqliteConnection,
    agent: &AgentIdentity,
    scope: Scope,
    target: Option<&Target>,
    params: &Value,
) -> Result<Allowed, ApiError> {
    if scope.is_human_only() {
        return Err(ApiError::forbidden(Reason::HumanOnly, Some(scope.as_str())));
    }
    if agent.disabled {
        return Err(ApiError::forbidden(
            Reason::AgentDisabled,
            Some(scope.as_str()),
        ));
    }
    let now = now_rfc3339();
    let rows = sqlx::query(&format!(
        "SELECT {GRANT_COLUMNS} FROM grants WHERE agent_id = ?"
    ))
    .bind(&agent.agent_id)
    .fetch_all(&mut *conn)
    .await?;
    let with_scope: Vec<GrantRow> = rows
        .iter()
        .map(grant_from_row)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|g| g.has(scope))
        .collect();
    let usable: Vec<&GrantRow> = with_scope
        .iter()
        .filter(|g| g.unusable(&now).is_none())
        .collect();
    let refuse = |reason: Reason| ApiError::forbidden(reason, Some(scope.as_str()));

    // Step d.
    if usable.is_empty() {
        if scope.is_destructive() {
            return Err(approval_required(scope, target));
        }
        return Err(refuse(
            inactive_reason(with_scope.iter(), &now).unwrap_or(Reason::ScopeMissing),
        ));
    }

    let Some(target) = target else {
        // A list or cluster-level call: filtered, never refused (section 5.1).
        let selectors: Vec<Selector> = usable
            .iter()
            .filter(|g| !g.selector.is_empty())
            .map(|g| g.selector.clone())
            .collect();
        // Only `{}` selectors: the call is allowed and reads nothing.
        let grant = pick(usable.iter().copied());
        return Ok(Allowed {
            grant,
            selectors,
            resolved: None,
        });
    };

    // Step e.
    let resolved = resolve(conn, &target.kind, &target.id, 0).await?;
    let matching: Vec<&GrantRow> = usable
        .iter()
        .copied()
        .filter(|g| g.selector.matches(&resolved))
        .collect();
    // Step f.
    if matching.is_empty() {
        let covering = with_scope.iter().filter(|g| g.selector.matches(&resolved));
        return Err(refuse(
            inactive_reason(covering, &now).unwrap_or(Reason::SelectorMismatch),
        ));
    }
    // Step g.
    let candidates: Vec<&GrantRow> = if scope.is_destructive() {
        let params_hash = params_hash(params)?;
        let mut kept = Vec::new();
        for grant in matching.into_iter().filter(|g| g.kind == "approval") {
            let Some(approval_id) = &grant.approval_id else {
                continue;
            };
            let row: Option<(String, String)> = sqlx::query_as(
                "SELECT target, params_hash FROM approvals WHERE id = ? AND status = 'approved'",
            )
            .bind(approval_id)
            .fetch_optional(&mut *conn)
            .await?;
            if let Some((approved_target, approved_hash)) = row {
                let approved: Value = serde_json::from_str(&approved_target).unwrap_or(Value::Null);
                if approved["type"] == target.kind.as_str()
                    && approved["id"] == target.id.as_str()
                    && approved_hash == params_hash
                {
                    kept.push(grant);
                }
            }
        }
        if kept.is_empty() {
            return Err(approval_required(scope, Some(target)));
        }
        kept
    } else {
        matching
    };
    // Step h.
    Ok(Allowed {
        grant: pick(candidates.into_iter()),
        selectors: Vec::new(),
        resolved: Some(resolved),
    })
}

/// Earliest `expires_at`, then earliest `created_at`, then lowest id (step h).
fn pick<'a>(grants: impl Iterator<Item = &'a GrantRow>) -> Option<GrantRow> {
    grants
        .min_by(|a, b| {
            (a.expires_at.as_str(), a.created_at.as_str(), a.id.as_str()).cmp(&(
                b.expires_at.as_str(),
                b.created_at.as_str(),
                b.id.as_str(),
            ))
        })
        .cloned()
}

/// `params_hash`: SHA-256 hex of the RFC 8785 serialization (section 9).
///
/// # Errors
///
/// An internal error when `params` cannot be canonicalized.
pub fn params_hash(params: &Value) -> Result<String, ApiError> {
    let canonical = jcs::canonicalize(params)
        .map_err(|e| ApiError::internal(&format!("params do not canonicalize: {e:?}")))?;
    Ok(sha256_hex(canonical.as_bytes()))
}

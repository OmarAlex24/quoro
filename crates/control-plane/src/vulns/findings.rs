// SPDX-License-Identifier: AGPL-3.0-only
//! Vulnerability findings and the loop that keeps them current.
//!
//! [`reconcile_app`] matches one app's current image against the cache and
//! writes what changed into `vulnerability_findings`, all in one transaction:
//! a new row, a changed row, or a resolved one. A row that does not change is
//! not written, so a daily data refresh does not grow the change log. When a
//! new qualifying finding appears (KEV or CVSS 9.0 or more) it emits
//! `app.vulnerable` and opens the vulnerability incident; once none is open
//! any more the system resolves it.
//!
//! [`spawn`] runs [`run_once`] every minute or as soon as an inventory or a
//! data refresh wakes it. A pass only reconciles apps whose fingerprint (image
//! digest, inventory hash, cache generation) changed since the previous one.

use crate::AppState;
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::events::{self, StoredEvent};
use crate::service::incidents;
use crate::store::now_rfc3339;
use crate::vulns::components;
use crate::vulns::ecosystem;
use crate::vulns::matcher::{self, Candidate};
use crate::vulns::subjects;
use quoro_proto::inventory::InventoryObject;
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;

/// How often the loop runs when nothing wakes it.
const INTERVAL: Duration = Duration::from_secs(60);

/// A CVSS score at or above this opens the vulnerability incident.
const CRITICAL_SCORE: f64 = 9.0;

/// Most findings one `app.vulnerable` event carries.
const MAX_EVENT_FINDINGS: usize = 20;

/// What one reconciliation changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    pub inserted: usize,
    pub updated: usize,
    pub resolved: usize,
}

/// One finding a non-app subject should have, as [`apply_subject`] writes it.
pub(crate) struct DesiredFinding {
    /// The row's `component`: the component's name, for a `quoro` finding.
    pub component: Option<String>,
    /// The row's `node_id`, for a `host_kernel` finding.
    pub node_id: Option<String>,
    /// The match, with `installed_version` already the value the row records.
    pub candidate: Candidate,
}

/// What makes two open findings of one subject the same row, as the unique
/// index on open findings does.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SubjectKey {
    component: String,
    node_id: String,
    vuln_id: String,
    package: String,
}

impl SubjectKey {
    /// The key of a desired finding.
    fn desired(finding: &DesiredFinding) -> Self {
        Self {
            component: finding.component.clone().unwrap_or_default(),
            node_id: finding.node_id.clone().unwrap_or_default(),
            vuln_id: finding.candidate.vuln_id.clone(),
            package: finding.candidate.package.clone(),
        }
    }

    /// The key of a stored row.
    fn row(row: &FindingRow) -> Self {
        Self {
            component: row.component.clone().unwrap_or_default(),
            node_id: row.node_id.clone().unwrap_or_default(),
            vuln_id: row.vuln_id.clone(),
            package: row.package.clone(),
        }
    }
}

/// The columns of one finding row, in the order [`FindingRow`] reads them.
const FINDING_COLUMNS: &str = "id, subject_kind, app_id, project_id, image_digest, node_id, \
     component, vuln_id, aliases, summary, ecosystem, package, source_package, \
     installed_version, fixed_version, severity, cvss_score, cvss_vector, kev, epss, \
     first_seen_at, updated_at, resolved_at, resolved_reason";

/// One row of `vulnerability_findings`, as the matcher reads and writes it.
#[derive(Clone, Debug)]
pub(crate) struct FindingRow {
    id: String,
    subject_kind: String,
    app_id: Option<String>,
    project_id: Option<String>,
    image_digest: Option<String>,
    node_id: Option<String>,
    component: Option<String>,
    vuln_id: String,
    aliases: Vec<String>,
    summary: Option<String>,
    ecosystem: String,
    package: String,
    source_package: Option<String>,
    installed_version: String,
    fixed_version: Option<String>,
    severity: String,
    cvss_score: Option<f64>,
    cvss_vector: Option<String>,
    kev: bool,
    epss: Option<f64>,
    first_seen_at: String,
    updated_at: String,
    resolved_at: Option<String>,
    resolved_reason: Option<String>,
}

impl FindingRow {
    fn from_row(row: &SqliteRow) -> Result<Self, ApiError> {
        let aliases: String = row.try_get("aliases")?;
        Ok(Self {
            id: row.try_get("id")?,
            subject_kind: row.try_get("subject_kind")?,
            app_id: row.try_get("app_id")?,
            project_id: row.try_get("project_id")?,
            image_digest: row.try_get("image_digest")?,
            node_id: row.try_get("node_id")?,
            component: row.try_get("component")?,
            vuln_id: row.try_get("vuln_id")?,
            aliases: serde_json::from_str(&aliases).unwrap_or_default(),
            summary: row.try_get("summary")?,
            ecosystem: row.try_get("ecosystem")?,
            package: row.try_get("package")?,
            source_package: row.try_get("source_package")?,
            installed_version: row.try_get("installed_version")?,
            fixed_version: row.try_get("fixed_version")?,
            severity: row.try_get("severity")?,
            cvss_score: row.try_get("cvss_score")?,
            cvss_vector: row.try_get("cvss_vector")?,
            kev: row.try_get::<i64, _>("kev")? != 0,
            epss: row.try_get("epss")?,
            first_seen_at: row.try_get("first_seen_at")?,
            updated_at: row.try_get("updated_at")?,
            resolved_at: row.try_get("resolved_at")?,
            resolved_reason: row.try_get("resolved_reason")?,
        })
    }

    /// A new open finding for one app and candidate.
    fn new(app_id: &str, project_id: &str, digest: &str, candidate: &Candidate, now: &str) -> Self {
        let mut row = Self::new_subject("app", None, None, candidate, now);
        row.app_id = Some(app_id.to_owned());
        row.project_id = Some(project_id.to_owned());
        row.image_digest = Some(digest.to_owned());
        row
    }

    /// A new open finding for one non-app subject: a component of the release,
    /// or a node's host kernel.
    pub(crate) fn new_subject(
        subject_kind: &str,
        component: Option<&str>,
        node_id: Option<&str>,
        candidate: &Candidate,
        now: &str,
    ) -> Self {
        Self {
            id: crate::deploy::new_id_with_prefix("vln"),
            subject_kind: subject_kind.to_owned(),
            app_id: None,
            project_id: None,
            image_digest: None,
            node_id: node_id.map(str::to_owned),
            component: component.map(str::to_owned),
            vuln_id: candidate.vuln_id.clone(),
            aliases: candidate.aliases.clone(),
            summary: candidate.summary.clone(),
            ecosystem: candidate.ecosystem.clone(),
            package: candidate.package.clone(),
            source_package: candidate.source_package.clone(),
            installed_version: candidate.installed_version.clone(),
            fixed_version: candidate.fixed_version.clone(),
            severity: candidate.severity.to_owned(),
            cvss_score: candidate.cvss_score,
            cvss_vector: candidate.cvss_vector.clone(),
            kev: candidate.kev,
            epss: None,
            first_seen_at: now.to_owned(),
            updated_at: now.to_owned(),
            resolved_at: None,
            resolved_reason: None,
        }
    }

    /// The row's `component`.
    pub(crate) fn component(&self) -> Option<&str> {
        self.component.as_deref()
    }

    /// The row's `node_id`.
    pub(crate) fn node_id(&self) -> Option<&str> {
        self.node_id.as_deref()
    }

    /// The row's `installed_version`.
    pub(crate) fn installed_version(&self) -> &str {
        &self.installed_version
    }

    /// Whether the row holds a vulnerability that opens the incident.
    fn qualifying(&self) -> bool {
        self.kev || self.cvss_score.is_some_and(|score| score >= CRITICAL_SCORE)
    }

    /// Whether the stored values differ from `candidate` for the current image.
    fn differs(&self, candidate: &Candidate, digest: &str) -> bool {
        self.image_digest.as_deref() != Some(digest) || self.differs_subject(candidate)
    }

    /// Whether the stored values differ from `candidate` for a subject whose
    /// row carries no image digest.
    fn differs_subject(&self, candidate: &Candidate) -> bool {
        self.installed_version != candidate.installed_version
            || self.fixed_version != candidate.fixed_version
            || self.severity != candidate.severity
            || self.cvss_score != candidate.cvss_score
            || self.cvss_vector != candidate.cvss_vector
            || self.kev != candidate.kev
            || self.summary != candidate.summary
            || self.aliases != candidate.aliases
            || self.source_package != candidate.source_package
            || self.ecosystem != candidate.ecosystem
    }

    /// Copies the candidate's values into this row.
    fn apply(&mut self, candidate: &Candidate, digest: &str, now: &str) {
        self.image_digest = Some(digest.to_owned());
        self.apply_candidate(candidate, now);
    }

    /// Copies the candidate's values into this row, leaving the subject's own
    /// columns (`image_digest`, `component`, `node_id`) alone.
    fn apply_candidate(&mut self, candidate: &Candidate, now: &str) {
        self.aliases = candidate.aliases.clone();
        self.summary = candidate.summary.clone();
        self.ecosystem = candidate.ecosystem.clone();
        self.source_package = candidate.source_package.clone();
        self.installed_version = candidate.installed_version.clone();
        self.fixed_version = candidate.fixed_version.clone();
        self.severity = candidate.severity.to_owned();
        self.cvss_score = candidate.cvss_score;
        self.cvss_vector = candidate.cvss_vector.clone();
        self.kev = candidate.kev;
        self.updated_at = now.to_owned();
    }

    /// The whole row as a change-log payload, so a replica rebuilds the table.
    ///
    /// The change log's canonical JSON refuses a float, so the two REAL
    /// columns are written as their decimal text (`"9.8"`), which the replica
    /// parses back into the REAL column.
    fn payload(&self) -> Value {
        json!({
            "id": self.id,
            "subject_kind": self.subject_kind,
            "app_id": self.app_id,
            "project_id": self.project_id,
            "image_digest": self.image_digest,
            "node_id": self.node_id,
            "component": self.component,
            "vuln_id": self.vuln_id,
            "aliases": self.aliases,
            "summary": self.summary,
            "ecosystem": self.ecosystem,
            "package": self.package,
            "source_package": self.source_package,
            "installed_version": self.installed_version,
            "fixed_version": self.fixed_version,
            "severity": self.severity,
            "cvss_score": score_payload(self.cvss_score),
            "cvss_vector": self.cvss_vector,
            "kev": self.kev,
            "epss": score_payload(self.epss),
            "first_seen_at": self.first_seen_at,
            "updated_at": self.updated_at,
            "resolved_at": self.resolved_at,
            "resolved_reason": self.resolved_reason,
        })
    }

    /// What an `app.vulnerable` event says about this finding.
    fn event_entry(&self) -> Value {
        json!({
            "finding_id": self.id,
            "vuln_id": self.vuln_id,
            "package": self.package,
            "installed_version": self.installed_version,
            "fixed_version": self.fixed_version,
            "severity": self.severity,
            "cvss_score": score_payload(self.cvss_score),
            "kev": self.kev,
            "reason": if self.kev { "kev" } else { "cvss_critical" },
        })
    }
}

/// A REAL column as a JSON number, rounded to the one decimal a CVSS score
/// has, so its canonical form is the short decimal (`9.8`).
fn score_payload(score: Option<f64>) -> Value {
    score
        .map(|score| (score * 10.0).round() / 10.0)
        .and_then(serde_json::Number::from_f64)
        .map_or(Value::Null, Value::Number)
}

/// Orders qualifying findings for the event: KEV first, then the highest
/// score, then by id.
fn event_order(left: &FindingRow, right: &FindingRow) -> std::cmp::Ordering {
    right
        .kev
        .cmp(&left.kev)
        .then_with(|| {
            right
                .cvss_score
                .unwrap_or(0.0)
                .total_cmp(&left.cvss_score.unwrap_or(0.0))
        })
        .then_with(|| left.id.cmp(&right.id))
}

const INSERT_FINDING: &str = "INSERT INTO vulnerability_findings \
     (id, subject_kind, app_id, project_id, image_digest, node_id, component, vuln_id, aliases, \
      summary, ecosystem, package, source_package, installed_version, fixed_version, severity, \
      cvss_score, cvss_vector, kev, epss, first_seen_at, updated_at, resolved_at, resolved_reason) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// Reconciles one app's findings against its current image.
///
/// # Errors
///
/// A database error, or an error writing the change log or the incident.
pub async fn reconcile_app(state: &AppState, app_id: &str) -> Result<Reconciled, ApiError> {
    let app = sqlx::query("SELECT project_id, current_deployment_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?;
    let Some(app) = app else {
        return resolve_all(state, app_id, "app_deleted").await;
    };
    let project_id: String = app.try_get("project_id")?;
    let current_deployment_id: Option<String> = app.try_get("current_deployment_id")?;
    let digest = match current_deployment_id {
        Some(deployment_id) => sqlx::query_as::<_, (Option<String>,)>(
            "SELECT image_digest FROM deployments WHERE id = ?",
        )
        .bind(&deployment_id)
        .fetch_optional(state.store.pool())
        .await?
        .and_then(|(digest,)| digest),
        None => None,
    };
    let Some(digest) = digest else {
        return resolve_all(state, app_id, "image_changed").await;
    };

    let Some((_sha256, json)) = state.vulns.cache().inventory(&digest).await? else {
        // The inventory has not arrived yet: the old findings stay open.
        return Ok(Reconciled::default());
    };
    let Ok(inventory) = serde_json::from_str::<InventoryObject>(&json) else {
        tracing::debug!(app_id, %digest, "the stored image inventory is not an object");
        return Ok(Reconciled::default());
    };
    // Never resolve a finding because the data it needs is still missing.
    for name in inventory_ecosystems(&inventory) {
        if let Some(bucket) = ecosystem::bucket_for(&name)
            && state.vulns.cache().bucket(bucket).await?.is_none()
        {
            tracing::debug!(app_id, %digest, ecosystem = %name, "vulnerability data is still pending");
            return Ok(Reconciled::default());
        }
    }

    let candidates = matcher::match_inventory(state.vulns.cache(), &inventory).await?;
    apply(state, app_id, &project_id, &digest, candidates).await
}

/// Every ecosystem an inventory names, its own list plus the packages'.
fn inventory_ecosystems(inventory: &InventoryObject) -> Vec<String> {
    let mut names: BTreeSet<&str> = inventory.ecosystems.iter().map(String::as_str).collect();
    for package in &inventory.packages {
        names.insert(package.ecosystem.as_str());
    }
    names.into_iter().map(str::to_owned).collect()
}

/// Resolves every open finding of `app_id` with `reason`.
async fn resolve_all(state: &AppState, app_id: &str, reason: &str) -> Result<Reconciled, ApiError> {
    let mut w = state.store.begin_write().await?;
    let rows = open_findings(&mut w.tx, app_id).await?;
    let now = now_rfc3339();
    let actor = Actor::system("vulnerabilities");
    let mut reconciled = Reconciled::default();
    for mut row in rows {
        resolve_row(&mut w.tx, &mut row, reason, &now, &actor).await?;
        reconciled.resolved += 1;
    }
    incidents::resolve_for_app_vulnerable(&mut w.tx, app_id, &new_id(Prefix::Request)).await?;
    w.commit().await?;
    Ok(reconciled)
}

/// Writes the new, changed and resolved findings of one app in one
/// transaction, then opens or resolves the vulnerability incident.
async fn apply(
    state: &AppState,
    app_id: &str,
    project_id: &str,
    digest: &str,
    candidates: Vec<Candidate>,
) -> Result<Reconciled, ApiError> {
    let mut w = state.store.begin_write().await?;
    let rows = open_findings(&mut w.tx, app_id).await?;
    let mut existing: HashMap<(String, String), FindingRow> = rows
        .into_iter()
        .map(|row| ((row.vuln_id.clone(), row.package.clone()), row))
        .collect();
    let now = now_rfc3339();
    let actor = Actor::system("vulnerabilities");
    let mut reconciled = Reconciled::default();
    let mut new_qualifying: Vec<FindingRow> = Vec::new();

    for candidate in candidates {
        let key = (candidate.vuln_id.clone(), candidate.package.clone());
        if let Some(mut row) = existing.remove(&key) {
            let was_qualifying = row.qualifying();
            if !row.differs(&candidate, digest) {
                continue;
            }
            row.apply(&candidate, digest, &now);
            update_row(&mut w.tx, &row).await?;
            changelog::append(
                &mut w.tx,
                &actor,
                "vulnerability_finding",
                &row.id,
                Op::Update,
                row.payload(),
            )
            .await?;
            reconciled.updated += 1;
            if row.qualifying() && !was_qualifying {
                new_qualifying.push(row);
            }
        } else {
            let row = FindingRow::new(app_id, project_id, digest, &candidate, &now);
            insert_row(&mut w.tx, &row).await?;
            changelog::append(
                &mut w.tx,
                &actor,
                "vulnerability_finding",
                &row.id,
                Op::Create,
                row.payload(),
            )
            .await?;
            reconciled.inserted += 1;
            if row.qualifying() {
                new_qualifying.push(row);
            }
        }
    }

    // Whatever the matcher no longer found is resolved.
    let mut remaining: Vec<FindingRow> = existing.into_values().collect();
    remaining.sort_by(|left, right| left.id.cmp(&right.id));
    for mut row in remaining {
        let reason = if row.image_digest.as_deref() != Some(digest) {
            "image_changed"
        } else {
            "data_changed"
        };
        resolve_row(&mut w.tx, &mut row, reason, &now, &actor).await?;
        reconciled.resolved += 1;
    }

    if !new_qualifying.is_empty() {
        open_vulnerability_incident(&mut w.tx, app_id, project_id, digest, &new_qualifying).await?;
    }
    if !has_open_qualifying(&mut w.tx, app_id).await? {
        incidents::resolve_for_app_vulnerable(&mut w.tx, app_id, &new_id(Prefix::Request)).await?;
    }
    w.commit().await?;
    Ok(reconciled)
}

/// The open app findings of `app_id`.
async fn open_findings(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<Vec<FindingRow>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {FINDING_COLUMNS} FROM vulnerability_findings \
         WHERE subject_kind = 'app' AND app_id = ? AND resolved_at IS NULL ORDER BY id"
    ))
    .bind(app_id)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(FindingRow::from_row).collect()
}

/// Writes one non-app subject's findings in one transaction: a new row, a
/// changed row, or a resolved one.
///
/// `reason` says why a finding of this subject that the match no longer names
/// is resolved. Nothing else happens: these subjects raise no event and open no
/// incident (events.schema.json `data_app_vulnerable`).
///
/// # Errors
///
/// A database error, or an error writing the change log.
pub(crate) async fn apply_subject(
    state: &AppState,
    subject_kind: &str,
    desired: Vec<DesiredFinding>,
    reason: impl Fn(&FindingRow) -> &'static str,
) -> Result<Reconciled, ApiError> {
    let mut w = state.store.begin_write().await?;
    let rows = open_subject_findings(&mut w.tx, subject_kind).await?;
    let mut existing: HashMap<SubjectKey, FindingRow> = rows
        .into_iter()
        .map(|row| (SubjectKey::row(&row), row))
        .collect();
    let now = now_rfc3339();
    let actor = Actor::system("vulnerabilities");
    let mut reconciled = Reconciled::default();
    // One record can be published twice for the same package (two ranges); the
    // first match is the finding.
    let mut written: HashSet<SubjectKey> = HashSet::new();

    for desired in desired {
        let key = SubjectKey::desired(&desired);
        if !written.insert(key.clone()) {
            continue;
        }
        if let Some(mut row) = existing.remove(&key) {
            if !row.differs_subject(&desired.candidate) {
                continue;
            }
            row.apply_candidate(&desired.candidate, &now);
            update_row(&mut w.tx, &row).await?;
            changelog::append(
                &mut w.tx,
                &actor,
                "vulnerability_finding",
                &row.id,
                Op::Update,
                row.payload(),
            )
            .await?;
            reconciled.updated += 1;
        } else {
            let row = FindingRow::new_subject(
                subject_kind,
                desired.component.as_deref(),
                desired.node_id.as_deref(),
                &desired.candidate,
                &now,
            );
            insert_row(&mut w.tx, &row).await?;
            changelog::append(
                &mut w.tx,
                &actor,
                "vulnerability_finding",
                &row.id,
                Op::Create,
                row.payload(),
            )
            .await?;
            reconciled.inserted += 1;
        }
    }

    // Whatever the matcher no longer found is resolved.
    let mut remaining: Vec<FindingRow> = existing.into_values().collect();
    remaining.sort_by(|left, right| left.id.cmp(&right.id));
    for mut row in remaining {
        let resolved_reason = reason(&row);
        resolve_row(&mut w.tx, &mut row, resolved_reason, &now, &actor).await?;
        reconciled.resolved += 1;
    }

    w.commit().await?;
    Ok(reconciled)
}

/// The open findings of one non-app subject.
async fn open_subject_findings(
    conn: &mut SqliteConnection,
    subject_kind: &str,
) -> Result<Vec<FindingRow>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {FINDING_COLUMNS} FROM vulnerability_findings \
         WHERE subject_kind = ? AND resolved_at IS NULL ORDER BY id"
    ))
    .bind(subject_kind)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(FindingRow::from_row).collect()
}

async fn insert_row(conn: &mut SqliteConnection, row: &FindingRow) -> Result<(), ApiError> {
    sqlx::query(INSERT_FINDING)
        .bind(&row.id)
        .bind(&row.subject_kind)
        .bind(&row.app_id)
        .bind(&row.project_id)
        .bind(&row.image_digest)
        .bind(&row.node_id)
        .bind(&row.component)
        .bind(&row.vuln_id)
        .bind(aliases_json(row))
        .bind(&row.summary)
        .bind(&row.ecosystem)
        .bind(&row.package)
        .bind(&row.source_package)
        .bind(&row.installed_version)
        .bind(&row.fixed_version)
        .bind(&row.severity)
        .bind(row.cvss_score)
        .bind(&row.cvss_vector)
        .bind(i64::from(row.kev))
        .bind(row.epss)
        .bind(&row.first_seen_at)
        .bind(&row.updated_at)
        .bind(&row.resolved_at)
        .bind(&row.resolved_reason)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn update_row(conn: &mut SqliteConnection, row: &FindingRow) -> Result<(), ApiError> {
    sqlx::query(
        "UPDATE vulnerability_findings SET image_digest = ?, aliases = ?, summary = ?, \
         ecosystem = ?, source_package = ?, installed_version = ?, fixed_version = ?, \
         severity = ?, cvss_score = ?, cvss_vector = ?, kev = ?, updated_at = ? WHERE id = ?",
    )
    .bind(&row.image_digest)
    .bind(aliases_json(row))
    .bind(&row.summary)
    .bind(&row.ecosystem)
    .bind(&row.source_package)
    .bind(&row.installed_version)
    .bind(&row.fixed_version)
    .bind(&row.severity)
    .bind(row.cvss_score)
    .bind(&row.cvss_vector)
    .bind(i64::from(row.kev))
    .bind(&row.updated_at)
    .bind(&row.id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Marks one row resolved and writes its change-log entry.
async fn resolve_row(
    conn: &mut SqliteConnection,
    row: &mut FindingRow,
    reason: &str,
    now: &str,
    actor: &Actor,
) -> Result<(), ApiError> {
    row.resolved_at = Some(now.to_owned());
    row.resolved_reason = Some(reason.to_owned());
    row.updated_at = now.to_owned();
    sqlx::query(
        "UPDATE vulnerability_findings SET resolved_at = ?, resolved_reason = ?, \
         updated_at = ? WHERE id = ?",
    )
    .bind(&row.resolved_at)
    .bind(&row.resolved_reason)
    .bind(&row.updated_at)
    .bind(&row.id)
    .execute(&mut *conn)
    .await?;
    changelog::append(
        conn,
        actor,
        "vulnerability_finding",
        &row.id,
        Op::Update,
        row.payload(),
    )
    .await?;
    Ok(())
}

fn aliases_json(row: &FindingRow) -> String {
    serde_json::to_string(&row.aliases).unwrap_or_else(|_| "[]".to_owned())
}

/// Emits `app.vulnerable` and opens its incident.
async fn open_vulnerability_incident(
    conn: &mut SqliteConnection,
    app_id: &str,
    project_id: &str,
    digest: &str,
    findings: &[FindingRow],
) -> Result<(), ApiError> {
    let event_id = new_id(Prefix::Event);
    let request_id = new_id(Prefix::Request);
    let time = now_rfc3339();
    let rebuildable: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM apps WHERE id = ? AND source IS NOT NULL")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    let mut ordered: Vec<&FindingRow> = findings.iter().collect();
    ordered.sort_by(|left, right| event_order(left, right));
    let entries: Vec<Value> = ordered
        .into_iter()
        .take(MAX_EVENT_FINDINGS)
        .map(FindingRow::event_entry)
        .collect();
    events::insert(
        conn,
        StoredEvent {
            id: &event_id,
            time: &time,
            kind: "app.vulnerable",
            source: json!({ "kind": "control_plane" }),
            subject: json!({ "type": "app", "id": app_id }),
            project_id: Some(project_id),
            incident_id: None,
            request_id: Some(&request_id),
            actor: Some(json!({ "type": "system", "id": "vulnerabilities" })),
            data: json!({
                "app_id": app_id,
                "image_digest": digest,
                "finding_count": findings.len(),
                "findings": entries,
                "rebuildable": rebuildable.is_some(),
            }),
            change_actor: Actor::system("vulnerabilities"),
        },
    )
    .await?;
    incidents::open_for_app_vulnerable(conn, app_id, Some(project_id), &event_id, &request_id).await
}

/// Whether the app still has a finding that keeps the incident open.
async fn has_open_qualifying(conn: &mut SqliteConnection, app_id: &str) -> Result<bool, ApiError> {
    let found: Option<(i64,)> = sqlx::query_as(
        "SELECT 1 FROM vulnerability_findings WHERE subject_kind = 'app' AND app_id = ? \
         AND resolved_at IS NULL AND (kev = 1 OR cvss_score >= ?) LIMIT 1",
    )
    .bind(app_id)
    .bind(CRITICAL_SCORE)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(found.is_some())
}

/// The fingerprint a pass compares: the app's image, its inventory hash and
/// the cache generation. A change in any of them means the app is reconciled.
type Fingerprint = (Option<String>, Option<String>, i64);

fn fingerprints() -> &'static Mutex<HashMap<String, Fingerprint>> {
    static FINGERPRINTS: OnceLock<Mutex<HashMap<String, Fingerprint>>> = OnceLock::new();
    FINGERPRINTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn wake() -> &'static Notify {
    static WAKE: OnceLock<Notify> = OnceLock::new();
    WAKE.get_or_init(Notify::new)
}

/// Wakes the matcher now: a new inventory or new data arrived.
pub fn wake_matcher() {
    wake().notify_one();
}

/// One pass over every app whose fingerprint changed, then over the two
/// non-app subjects.
pub async fn run_once(state: &AppState) {
    let generation = match state.vulns.cache().generation().await {
        Ok(generation) => generation,
        Err(error) => {
            tracing::warn!(%error, "cannot read the vulnerability cache generation");
            return;
        }
    };
    // The release's own components need their ecosystems' data; `Linux` is
    // always fetched, so only the package ecosystems are asked for.
    subjects::want_ecosystems(state);
    let mut apps: BTreeSet<String> = BTreeSet::new();
    match sqlx::query_as::<_, (String,)>("SELECT id FROM apps ORDER BY id")
        .fetch_all(state.store.pool())
        .await
    {
        Ok(rows) => apps.extend(rows.into_iter().map(|(id,)| id)),
        Err(error) => tracing::warn!(%error, "cannot list apps for vulnerability matching"),
    }
    match sqlx::query_as::<_, (String,)>(
        "SELECT DISTINCT app_id FROM vulnerability_findings \
         WHERE resolved_at IS NULL AND app_id IS NOT NULL \
         AND app_id NOT IN (SELECT id FROM apps)",
    )
    .fetch_all(state.store.pool())
    .await
    {
        Ok(rows) => apps.extend(rows.into_iter().map(|(id,)| id)),
        Err(error) => tracing::warn!(%error, "cannot list findings of deleted apps"),
    }

    for app_id in apps {
        let digest = match app_digest(state, &app_id).await {
            Ok(digest) => digest,
            Err(error) => {
                tracing::warn!(app_id = %app_id, %error, "cannot read the app's image");
                continue;
            }
        };
        let sha256 = match &digest {
            Some(digest) => match state.vulns.cache().inventory_sha(digest).await {
                Ok(sha256) => sha256,
                Err(error) => {
                    tracing::warn!(app_id = %app_id, %error, "cannot read the inventory");
                    continue;
                }
            },
            None => None,
        };
        let fingerprint = (digest, sha256, generation);
        if !fingerprint_changed(&app_id, &fingerprint) {
            continue;
        }
        match reconcile_app(state, &app_id).await {
            Ok(_) => remember_fingerprint(&app_id, fingerprint),
            Err(error) => {
                tracing::warn!(app_id = %app_id, error = ?error, "cannot reconcile the app's findings");
            }
        }
    }

    reconcile_quoro_subject(state, generation).await;
    reconcile_host_kernel_subject(state, generation).await;
}

/// The fingerprint of one non-app subject: the cache generation plus what it
/// reconciles.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SubjectFingerprint {
    /// The release's component list, named by [`components::embedded_hash`].
    Quoro(i64, String),
    /// Every node with the `node` role: id, kernel release and status.
    HostKernels(i64, Vec<(String, String, String)>),
}

fn subject_fingerprints() -> &'static Mutex<HashMap<String, SubjectFingerprint>> {
    static FINGERPRINTS: OnceLock<Mutex<HashMap<String, SubjectFingerprint>>> = OnceLock::new();
    FINGERPRINTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The key one subject's fingerprint is held under. The cluster id keeps two
/// primaries in one process (as the tests run them) apart.
fn subject_key(state: &AppState, subject_kind: &str) -> String {
    format!("{}:{subject_kind}", state.cluster_id)
}

/// Reconciles the release's own components when the cache or the component
/// list changed since the last pass.
async fn reconcile_quoro_subject(state: &AppState, generation: i64) {
    let key = subject_key(state, "quoro");
    let fingerprint = SubjectFingerprint::Quoro(generation, components::embedded_hash().to_owned());
    {
        let fingerprints = subject_fingerprints()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if fingerprints.get(&key) == Some(&fingerprint) {
            return;
        }
    }
    match subjects::reconcile_quoro(state).await {
        Ok(_) => remember_subject_fingerprint(&key, fingerprint),
        Err(error) => {
            tracing::warn!(error = ?error, "cannot reconcile Quoro's own findings");
        }
    }
}

/// Reconciles every node's host kernel when the cache or the nodelist changed
/// since the last pass.
async fn reconcile_host_kernel_subject(state: &AppState, generation: i64) {
    let nodes = match subjects::node_kernels(state).await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::warn!(error = ?error, "cannot read the nodes' host kernels");
            return;
        }
    };
    let key = subject_key(state, "host_kernel");
    let fingerprint = SubjectFingerprint::HostKernels(
        generation,
        nodes
            .iter()
            .map(|node| {
                (
                    node.node_id.clone(),
                    node.kernel.clone(),
                    node.status.clone(),
                )
            })
            .collect(),
    );
    {
        let fingerprints = subject_fingerprints()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if fingerprints.get(&key) == Some(&fingerprint) {
            return;
        }
    }
    match subjects::reconcile_host_kernels(state, &nodes).await {
        Ok(_) => remember_subject_fingerprint(&key, fingerprint),
        Err(error) => {
            tracing::warn!(error = ?error, "cannot reconcile the host kernels' findings");
        }
    }
}

fn remember_subject_fingerprint(key: &str, fingerprint: SubjectFingerprint) {
    subject_fingerprints()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key.to_owned(), fingerprint);
}

/// Starts the matcher loop: one pass at start, then on a wake-up or every
/// minute, whichever comes first. Errors never stop the loop.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        loop {
            // A demoted primary writes nothing more (`node-agent-protocol.md`
            // section 7.4).
            if state.store.stops_for_demotion("the vulnerability matcher") {
                break;
            }
            run_once(&state).await;
            tokio::select! {
                () = wake().notified() => {}
                () = tokio::time::sleep(INTERVAL) => {}
            }
        }
    });
}

/// The digest of the image the app currently runs, if it has one.
async fn app_digest(state: &AppState, app_id: &str) -> Result<Option<String>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT d.image_digest FROM apps a \
         LEFT JOIN deployments d ON d.id = a.current_deployment_id WHERE a.id = ?",
    )
    .bind(app_id)
    .fetch_optional(state.store.pool())
    .await?;
    Ok(row.and_then(|row| {
        row.try_get::<Option<String>, _>("image_digest")
            .ok()
            .flatten()
    }))
}

fn fingerprint_changed(app_id: &str, fingerprint: &Fingerprint) -> bool {
    let fingerprints = fingerprints()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    fingerprints.get(app_id) != Some(fingerprint)
}

fn remember_fingerprint(app_id: &str, fingerprint: Fingerprint) {
    fingerprints()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(app_id.to_owned(), fingerprint);
}

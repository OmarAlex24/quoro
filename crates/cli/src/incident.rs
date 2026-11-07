//! `quoro incident list [--status open|resolved] | show INCIDENT | resolve
//! INCIDENT [--note TEXT]`.
//!
//! An incident is opened by a detection and scopes the incident grants an
//! agent gets when an event wakes it (permission-model.md section 8). It is
//! named by its id (`inc_...`); it has no name.
//!
//! - `list` shows the newest incidents (`listIncidents`), one page; the line
//!   under the table says how `quoro api` pages further.
//! - `show` prints the incident, the event that opened it and the incident
//!   grants issued for it (`getIncident`).
//! - `resolve` closes it and records who did it and `--note` (`resolveIncident`,
//!   `incident:resolve`); its incident grants stop working. It is not a
//!   destructive scope, so nothing asks for a confirmation; an agent passes
//!   `--reason`.

use crate::Environment;
use crate::cli::{Global, IncidentCommand};
use crate::error::Failure;
use crate::operations::{GET_INCIDENT, LIST_INCIDENTS, RESOLVE_INCIDENT};
use crate::scope::{self, Scope, is_id, text};
use reqwest::Method;
use serde_json::{Value, json};
use std::io::Write;

/// The incidents `list` shows; more are paged with `quoro api`.
const PAGE: usize = 50;
/// `ResolveIncidentRequest.resolution_note`'s limit.
const NOTE: usize = 2000;

pub async fn run(
    global: &Global,
    command: IncidentCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        IncidentCommand::List { status } => {
            list(global, environment, status.as_deref(), stdout).await
        }
        IncidentCommand::Show { incident } => {
            check(&incident)?;
            let mut scope = Scope::open(global, environment, "incident show", false)?;
            let path = GET_INCIDENT.path.replace("{incident_id}", &incident);
            let answer = scope.answer(&path).await?;
            if global.json {
                return scope::report(&answer, true, stdout, |_| String::new());
            }
            crate::api::write(stdout, describe(&answer.json()?).as_bytes())
        }
        IncidentCommand::Resolve { incident, note } => {
            check(&incident)?;
            if note
                .as_ref()
                .is_some_and(|note| note.chars().count() > NOTE)
            {
                return Err(Failure::usage(format!(
                    "--note has at most {NOTE} characters"
                )));
            }
            let mut scope = Scope::open(global, environment, "incident resolve", true)?;
            let body = match &note {
                Some(note) => json!({ "resolution_note": note }),
                None => json!({}),
            };
            let call = scope.change(
                Method::POST,
                RESOLVE_INCIDENT.path.replace("{incident_id}", &incident),
                Some(&body),
            )?;
            let answer = scope.send(&call).await?;
            scope::report(&answer, global.json, stdout, |resolved| {
                format!(
                    "Resolved the incident {incident} ({}); its incident grants no longer work.\n",
                    text(resolved, "title")
                )
            })
        }
    }
}

fn check(incident: &str) -> Result<(), Failure> {
    if is_id("inc_", incident) {
        return Ok(());
    }
    Err(Failure::usage(format!(
        "'{incident}' is not an incident id (inc_...); quoro incident list shows them"
    )))
}

async fn list(
    global: &Global,
    environment: &Environment,
    status: Option<&str>,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "incident list", false)?;
    let mut path = format!("{}?limit={PAGE}", LIST_INCIDENTS.path);
    if let Some(status) = status {
        path.push_str(&format!("&status={status}"));
    }
    let answer = scope.answer(&path).await?;
    if global.json {
        return scope::report(&answer, true, stdout, |_| String::new());
    }
    let page = answer.json()?;
    let incidents = page["items"].as_array().cloned().unwrap_or_default();
    if incidents.is_empty() {
        let line = match status {
            Some(status) => format!("No {status} incidents.\n"),
            None => "No incidents.\n".to_string(),
        };
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        ["ID", "STATUS", "OPENED", "SUBJECT", "TITLE"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    ];
    for incident in &incidents {
        rows.push(vec![
            text(incident, "id"),
            text(incident, "status"),
            text(incident, "opened_at"),
            subject(incident),
            text(incident, "title"),
        ]);
    }
    let mut out = scope::table(&rows);
    if page["next_cursor"].as_str().is_some_and(|c| !c.is_empty()) {
        out.push_str(&format!(
            "The {PAGE} newest incidents; quoro api GET '{path}&cursor=...' pages further.\n"
        ));
    }
    crate::api::write(stdout, out.as_bytes())
}

/// `app app_...`: the incident's subject.
fn subject(incident: &Value) -> String {
    format!(
        "{} {}",
        text(&incident["subject"], "type"),
        text(&incident["subject"], "id")
    )
}

/// `incident show`: the incident, its trigger and its grants.
fn describe(incident: &Value) -> String {
    let mut out = String::new();
    let mut line = |label: &str, value: String| {
        out.push_str(&format!("{:<10}{value}\n", format!("{label}:")));
    };
    line("ID", text(incident, "id"));
    line("Title", text(incident, "title"));
    line("Status", text(incident, "status"));
    line("Subject", subject(incident));
    line("Project", text(incident, "project_id"));
    line("Opened", text(incident, "opened_at"));
    let event = &incident["trigger_event"];
    line(
        "Trigger",
        format!(
            "{} {} at {}",
            text(event, "type"),
            text(event, "id"),
            text(event, "time")
        ),
    );
    if incident["status"] == "resolved" {
        line(
            "Resolved",
            format!(
                "{} by {} {}",
                text(incident, "resolved_at"),
                text(incident, "resolved_by_type"),
                text(incident, "resolved_by_id")
            ),
        );
        line("Note", text(incident, "resolution_note"));
    }
    let grants = incident["grants"].as_array().cloned().unwrap_or_default();
    if grants.is_empty() {
        out.push_str("Grants:   none\n");
    } else {
        out.push_str("Grants:\n");
        for grant in &grants {
            let scopes = grant["scopes"]
                .as_array()
                .map(|scopes| {
                    scopes
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            out.push_str(&format!(
                "  {}  agent {}  expires {}  {scopes}\n",
                text(grant, "id"),
                text(grant, "agent_id"),
                text(grant, "expires_at")
            ));
        }
    }
    out
}

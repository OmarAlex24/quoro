//! What a resource command acts on: the context, the linked folder, and the
//! project and app named by name or id.
//!
//! The context is `--context`, else `QUORO_CONTEXT`, else the ad-hoc one of
//! `QUORO_URL`, else the context of the folder's link (`links.rs`), else
//! `current`. The link's project and app apply only when the context is the
//! link's own.
//!
//! A project is named by its id (`prj_...`), its slug or its name; an app by
//! its id (`app_...`) or its name, looked up in the project when one is known
//! (`--project`, else the link's). A name that matches more than one resource
//! is refused with the candidates, never resolved by a guess. `--project` and
//! `--app` override the link.
//!
//! A command that changes something opens its scope with `changes`: an
//! agent's change then needs `--reason` (or `QUORO_JUSTIFICATION`), refused
//! here before anything is sent, not even a mint.

use crate::Environment;
use crate::agent::default_justification;
use crate::approval;
use crate::cli::{ApprovalArgs, Global};
use crate::client::{Answer, Call, Session};
use crate::context;
use crate::error::{Failure, exit};
use crate::links::{self, Link, Links};
use reqwest::Method;
use serde_json::Value;
use std::io::Write;
use std::path::PathBuf;

/// The page size of a full listing (the contract's maximum).
const PAGE: usize = 500;

pub struct Scope {
    pub session: Session,
    /// The context's name; `None` for an ad-hoc context.
    pub context_name: Option<String>,
    /// The folder's link, when it belongs to this context.
    pub link: Option<(PathBuf, Link)>,
    pub justification: Option<String>,
    pub incident: Option<String>,
    pub is_agent: bool,
}

impl Scope {
    /// Opens the session for `command` (`project create`, ...). Nothing is
    /// sent yet.
    pub fn open(
        global: &Global,
        environment: &Environment,
        command: &str,
        changes: bool,
    ) -> Result<Self, Failure> {
        let nearest = match links::current_dir(environment) {
            Ok(cwd) => Links::read(environment)?
                .nearest(&cwd)
                .map(|(dir, link)| (dir, link.clone())),
            Err(_) => None,
        };
        let chosen = global
            .context
            .clone()
            .or_else(|| environment.var("QUORO_CONTEXT").map(str::to_string));
        let flag = match (&chosen, environment.var("QUORO_URL"), &nearest) {
            (Some(name), _, _) => Some(name.clone()),
            (None, None, Some((_, link))) => Some(link.context.clone()),
            _ => None,
        };
        let context = context::resolve(flag.as_deref(), environment)?;
        let link = nearest.filter(|(_, link)| context.name.as_deref() == Some(&link.context));
        let justification = environment.justification(global);
        let is_agent = context.is_agent();
        if changes && is_agent && justification.is_none() {
            return Err(Failure::usage(format!(
                "an agent must say why it runs quoro {command}: pass --reason TEXT or set \
                 QUORO_JUSTIFICATION; nothing was sent"
            )));
        }
        let mint_reason = justification
            .clone()
            .unwrap_or_else(|| default_justification(command));
        let context_name = context.name.clone();
        Ok(Self {
            session: Session::open(context, environment, &mint_reason)?,
            context_name,
            link,
            justification,
            incident: global.incident.clone(),
            is_agent,
        })
    }

    pub fn url(&self) -> &str {
        &self.session.context.url
    }

    /// A GET answered as JSON.
    pub async fn get(&mut self, path: &str) -> Result<Value, Failure> {
        let url = self.url().to_string();
        Answer::read(self.session.send(&Call::get(path)).await?, &url)
            .await?
            .json()
    }

    /// A GET answered whole, success or not.
    pub async fn answer(&mut self, path: &str) -> Result<Answer, Failure> {
        let url = self.url().to_string();
        Answer::read(self.session.send(&Call::get(path)).await?, &url).await
    }

    /// Every item of a paged listing.
    pub async fn list_all(&mut self, path: &str) -> Result<Vec<Value>, Failure> {
        let separator = if path.contains('?') { '&' } else { '?' };
        let mut items = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut page_path = format!("{path}{separator}limit={PAGE}");
            if let Some(cursor) = &cursor {
                page_path.push_str(&format!("&cursor={}", query_value(cursor)));
            }
            let page = self.get(&page_path).await?;
            items.extend(page["items"].as_array().cloned().unwrap_or_default());
            match page["next_cursor"].as_str() {
                Some(next) if !next.is_empty() => cursor = Some(next.to_string()),
                _ => return Ok(items),
            }
        }
    }

    /// A change: `--reason` and `--incident` as headers, a JSON body.
    pub fn change(
        &self,
        method: Method,
        path: String,
        body: Option<&Value>,
    ) -> Result<Call, Failure> {
        let headers = crate::api::change_headers(
            &method,
            self.justification.as_deref(),
            self.incident.as_deref(),
        )?;
        Ok(Call {
            method,
            path,
            headers,
            body: body.map(|body| body.to_string().into_bytes()),
        })
    }

    /// Sends a change and reads the answer whole.
    pub async fn send(&mut self, call: &Call) -> Result<Answer, Failure> {
        let url = self.url().to_string();
        Answer::read(self.session.send(call).await?, &url).await
    }

    /// Sends a destructive change. Answered `approval_required` with
    /// `--request-approval`, it files the approval and, with `--wait`,
    /// repeats the call once approved (`approval.rs`).
    pub async fn send_destructive(
        &mut self,
        call: &Call,
        approval: &ApprovalArgs,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<Answer, Failure> {
        let answer = self.send(call).await?;
        if answer.is_success()
            || !approval.request_approval
            || answer.failure().code != exit::APPROVAL_REQUIRED
        {
            return Ok(answer);
        }
        let request = approval::Request {
            call,
            justification: self.justification.as_deref(),
            incident: self.incident.as_deref(),
            json,
            wait: approval.wait,
        };
        let response =
            approval::file(&mut self.session, &request, &answer.body, stdout, stderr).await?;
        let url = self.url().to_string();
        Answer::read(response, &url).await
    }

    /// The project `reference` names: an id, a slug or a name.
    pub async fn project(&mut self, reference: &str) -> Result<Value, Failure> {
        if is_id("prj_", reference) {
            return self.get(&format!("/api/v1/projects/{reference}")).await;
        }
        let projects = self.list_all("/api/v1/projects").await?;
        if let Some(project) = projects.iter().find(|p| p["slug"] == reference) {
            return Ok(project.clone());
        }
        let named: Vec<&Value> = projects.iter().filter(|p| p["name"] == reference).collect();
        match named.as_slice() {
            [project] => Ok((*project).clone()),
            [] => Err(Failure::new(
                exit::NOT_FOUND,
                format!(
                    "no project has the id, slug or name '{reference}'; quoro project list shows them"
                ),
            )),
            several => Err(Failure::usage(format!(
                "the name '{reference}' matches {} projects: {}; name one by its slug or id",
                several.len(),
                several
                    .iter()
                    .map(|p| format!("{} ({})", text(p, "slug"), text(p, "id")))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// The project of `--project`, else the link's; `None` when neither.
    pub async fn project_in_scope(&mut self, global: &Global) -> Result<Option<Value>, Failure> {
        if let Some(reference) = &global.project {
            return self.project(reference).await.map(Some);
        }
        match self.link.as_ref().map(|(_, link)| link.project_id.clone()) {
            Some(id) => self
                .get(&format!("/api/v1/projects/{id}"))
                .await
                .map(Some)
                .map_err(|failure| self.stale(failure, "project")),
            None => Ok(None),
        }
    }

    /// The project a command needs: `--project` or the link's.
    pub async fn required_project(&mut self, global: &Global) -> Result<Value, Failure> {
        self.project_in_scope(global).await?.ok_or_else(|| {
            Failure::usage(
                "no project: pass --project NAME, or link this folder with quoro link PROJECT",
            )
        })
    }

    /// The app `reference` names, an id or a name, in `project_id` when
    /// given.
    pub async fn app(
        &mut self,
        reference: &str,
        project_id: Option<&str>,
    ) -> Result<Value, Failure> {
        if is_id("app_", reference) {
            return self.get(&format!("/api/v1/apps/{reference}")).await;
        }
        let path = match project_id {
            Some(id) => format!("/api/v1/apps?project_id={id}"),
            None => "/api/v1/apps".to_string(),
        };
        let apps = self.list_all(&path).await?;
        let named: Vec<&Value> = apps.iter().filter(|a| a["name"] == reference).collect();
        match named.as_slice() {
            [app] => Ok((*app).clone()),
            [] => Err(Failure::new(
                exit::NOT_FOUND,
                match project_id {
                    Some(id) => format!(
                        "no app has the id or name '{reference}' in the project {id}; quoro app list shows them"
                    ),
                    None => format!(
                        "no app has the id or name '{reference}'; quoro app list shows them"
                    ),
                },
            )),
            several => Err(Failure::usage(format!(
                "the name '{reference}' matches {} apps: {}; pass --project or name it by its id",
                several.len(),
                several
                    .iter()
                    .map(|a| format!(
                        "{} ({}, project {})",
                        text(a, "name"),
                        text(a, "id"),
                        text(a, "project_id")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// The app a command acts on: `--app` (in `--project` or the link's
    /// project), else the link's app.
    pub async fn required_app(&mut self, global: &Global) -> Result<Value, Failure> {
        if let Some(reference) = global.app.clone() {
            let project = if is_id("app_", &reference) {
                None
            } else {
                self.project_in_scope(global).await?
            };
            let project_id = project.as_ref().and_then(|p| p["id"].as_str());
            return self.app(&reference, project_id).await;
        }
        if global.project.is_none()
            && let Some(id) = self.link.as_ref().and_then(|(_, link)| link.app_id.clone())
        {
            return self
                .get(&format!("/api/v1/apps/{id}"))
                .await
                .map_err(|failure| self.stale(failure, "app"));
        }
        Err(Failure::usage(
            "no app: pass --app NAME, or link this folder with quoro link PROJECT APP",
        ))
    }

    /// The node `reference` names: its id (`nod_...`, used as it is) or its
    /// name, looked up with `listNodes` (`node:read`).
    pub async fn node_id(&mut self, reference: &str) -> Result<String, Failure> {
        if is_id("nod_", reference) {
            return Ok(reference.to_string());
        }
        let nodes = self
            .list_all(crate::operations::LIST_NODES.path)
            .await
            .map_err(|mut failure| {
                if failure.code == exit::FORBIDDEN {
                    failure.message = format!(
                        "{}; reading nodes by name needs node:read: name the node by its id (nod_...)",
                        failure.message
                    );
                }
                failure
            })?;
        match nodes.iter().find(|node| node["name"] == reference) {
            Some(node) => Ok(text(node, "id")),
            // A grant on projects reads no node, so an agent's empty list
            // says nothing about the name.
            None if self.is_agent => Err(Failure::new(
                exit::NOT_FOUND,
                format!(
                    "no node you may read has the name '{reference}'; reading nodes by name \
                     needs node:read on all resources: name the node by its id (nod_...)"
                ),
            )),
            None => Err(Failure::new(
                exit::NOT_FOUND,
                format!(
                    "no node has the id or name '{reference}'; quoro status --cluster shows them"
                ),
            )),
        }
    }

    /// A linked id the primary no longer knows says which link to fix.
    fn stale(&self, mut failure: Failure, what: &str) -> Failure {
        if failure.code == exit::NOT_FOUND
            && let Some((dir, _)) = &self.link
        {
            failure.message = format!(
                "{}; the {what} linked to {} is gone: run quoro link again or quoro unlink",
                failure.message,
                dir.display()
            );
        }
        failure
    }
}

/// Whether `text` is an id with `prefix` (`prj_`, `app_`): 26 characters of
/// Crockford base32 after it.
pub fn is_id(prefix: &str, text: &str) -> bool {
    text.strip_prefix(prefix).is_some_and(|rest| {
        rest.len() == 26
            && rest
                .bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
    })
}

/// A string field, or `?`.
pub fn text(value: &Value, key: &str) -> String {
    match &value[key] {
        Value::String(text) => text.clone(),
        Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

/// A cursor in a query string; cursors are opaque, so anything but the
/// unreserved characters is escaped.
pub(crate) fn query_value(text: &str) -> String {
    text.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// Rows as aligned columns, two spaces apart; the first row is the header.
pub fn table(rows: &[Vec<String>]) -> String {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|c| {
            rows.iter()
                .filter_map(|row| row.get(c))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    for row in rows {
        let mut line = String::new();
        for (c, cell) in row.iter().enumerate() {
            if c + 1 == row.len() {
                line.push_str(cell);
            } else {
                line.push_str(cell);
                line.push_str(&" ".repeat(widths[c] - cell.chars().count() + 2));
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

/// Whether a destructive command can be confirmed, checked before anything
/// is sent. `--yes` confirms; an agent is not asked, the primary's approval
/// is its check; anyone else types the name on a terminal, so without one
/// and without `--yes` the command is refused.
pub fn can_confirm(
    global: &Global,
    environment: &Environment,
    is_agent: bool,
    what: &str,
) -> Result<(), Failure> {
    can_confirm_change(global, environment, is_agent, &format!("deleting {what}"))
}

/// [`can_confirm`] for any change that cannot be undone; `change` names it
/// (`deleting an app`, `replacing an app's environment`).
pub fn can_confirm_change(
    global: &Global,
    environment: &Environment,
    is_agent: bool,
    change: &str,
) -> Result<(), Failure> {
    if global.yes || is_agent || environment.stdin_is_terminal {
        return Ok(());
    }
    Err(Failure::usage(format!(
        "{change} cannot be undone: pass --yes to confirm without a terminal; \
         nothing was sent"
    )))
}

/// Asks a human to type `name` before a destructive call, once the resource
/// is known; [`can_confirm`] passed.
pub fn confirm_name(
    global: &Global,
    environment: &Environment,
    is_agent: bool,
    what: &str,
    name: &str,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    confirm(
        global,
        environment,
        is_agent,
        &Confirm {
            change: &format!("deleting {what}"),
            warning: &format!("This deletes {what} for good"),
            label: "its name",
            expected: name,
            nothing: "nothing was deleted",
        },
        stderr,
    )
}

/// What a human confirms by typing `expected`.
pub struct Confirm<'a> {
    /// The change, for the refusal without a terminal (`deleting an app`).
    pub change: &'a str,
    /// The sentence before the prompt (`This deletes the app web for good`).
    pub warning: &'a str,
    /// What to type (`its name`, `the app's name`).
    pub label: &'a str,
    pub expected: &'a str,
    /// The end of the refusal when the answer differs (`nothing was deleted`).
    pub nothing: &'a str,
}

/// Asks a human to type `confirm.expected` before a change that cannot be
/// undone; `--yes` and an agent are not asked.
pub fn confirm(
    global: &Global,
    environment: &Environment,
    is_agent: bool,
    confirm: &Confirm<'_>,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    can_confirm_change(global, environment, is_agent, confirm.change)?;
    if global.yes || is_agent {
        return Ok(());
    }
    let _ = write!(
        stderr,
        "{}. Type {} ({}) to confirm: ",
        confirm.warning, confirm.label, confirm.expected
    );
    let _ = stderr.flush();
    let typed = environment
        .read_line()
        .map_err(|e| Failure::usage(format!("cannot read the answer: {e}")))?;
    if typed.trim() == confirm.expected {
        Ok(())
    } else {
        Err(Failure::usage(format!(
            "'{}' is not '{}'; {}",
            typed.trim(),
            confirm.expected,
            confirm.nothing
        )))
    }
}

/// Prints a change's answer: the body with `--json`, else `line`; an error
/// answer is its failure (its body too with `--json`).
pub fn report(
    answer: &Answer,
    json: bool,
    stdout: &mut dyn Write,
    line: impl FnOnce(&Value) -> String,
) -> Result<(), Failure> {
    if json {
        crate::api::write(stdout, &answer.body)?;
        if !answer.body.ends_with(b"\n") && !answer.body.is_empty() {
            crate::api::write(stdout, b"\n")?;
        }
    }
    if !answer.is_success() {
        return Err(answer.failure());
    }
    if !json {
        let body: Value = serde_json::from_slice(&answer.body).unwrap_or(Value::Null);
        crate::api::write(stdout, line(&body).as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_have_their_prefix_and_26_characters() {
        let id = format!("prj_{}", "0123456789ABCDEFGHJKMNPQRS");
        assert!(is_id("prj_", &id));
        assert!(!is_id("app_", &id));
        assert!(!is_id("prj_", "prj_demo"));
        assert!(!is_id("prj_", &id.to_lowercase()));
    }

    #[test]
    fn a_table_aligns_its_columns() {
        let rows = vec![
            vec!["NAME".to_string(), "ID".to_string()],
            vec!["web-long".to_string(), "app_1".to_string()],
        ];
        assert_eq!(table(&rows), "NAME      ID\nweb-long  app_1\n");
    }
}

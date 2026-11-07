//! `quoro audit list [--actor ACTOR] [--since T]`: a page of the audit log
//! (`listAudit`, `audit:read`), printed oldest first so the newest record is
//! at the bottom, as `logs` prints. The offline check of the hash chain,
//! `quoro audit verify --file | --db`, is a host command (`crates/quoro`),
//! which hands every other `audit` command line here.
//!
//! - Without `--since` the page is the newest one: the 100 newest records.
//!   `listAudit` reads forward from `after_seq` only, so the client first
//!   finds the last record's `seq` (see the search below) and reads from 100
//!   before it.
//! - `--actor` takes what `listAudit`'s `actor_id` takes: a user, agent or
//!   node id (`usr_`, `agt_`, `nod_`), used as it is. A name is looked up
//!   among the agents (`listAgents`, `agent:read`) and, for a human only,
//!   among the users by name or email (`listUsers`, `user:read`, which is
//!   human-only); a name that matches more than one is refused with the ids.
//!   A system subsystem is named by its id through `quoro api`. Without
//!   `--since`, `--actor` shows that actor's newest records, at most 100,
//!   among the newest 1000 records of the whole log: an actor that did
//!   nothing lately shows fewer or none, and `--since` reaches further back.
//! - `--since` takes an RFC 3339 time or a duration ago, as `logs --since`
//!   does, and shows the first 100 records from then on. `listAudit` has no
//!   time filter, so the client finds the first record at or after that time.
//!
//! The search reads single records with `getAuditRecord`: it doubles the
//! sequence number until it passes the time (or the end of the log), then
//! halves the gap, so it takes about twice the logarithm of the log's length
//! in reads, a few dozen at most. Records are appended in time order, so a
//! record's time never goes back as `seq` grows; the search relies on that.
//! A human's reads are not audited, but an agent's are (audit-log-format.md
//! section 3): each of an agent's reads appends a record and uses its grant
//! once when the grant limits its uses.
//!
//! The line under the page is the `quoro api` call that reads the next page,
//! or says that no newer record exists.

use crate::Environment;
use crate::cli::{AuditCommand, Global};
use crate::client::unix_seconds;
use crate::error::{Failure, exit};
use crate::operations::{GET_AUDIT_RECORD, LIST_AGENTS, LIST_AUDIT, LIST_USERS};
use crate::scope::{self, Scope, is_id, query_value, text};
use serde_json::{Value, json};
use std::io::Write;

/// The records a page shows.
const PAGE: usize = 100;
/// How far back from the end of the log `--actor` looks without `--since`.
const ACTOR_WINDOW: u64 = 1000;
/// The pages read within that window, at the contract's maximum size.
const WINDOW_PAGE: usize = 500;
const WINDOW_PAGES: usize = 4;
/// The id prefixes `actor_id` takes as they are.
const ACTOR_PREFIXES: [&str; 3] = ["usr_", "agt_", "nod_"];

pub async fn run(
    global: &Global,
    command: AuditCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let AuditCommand::List { actor, since } = command;
    let since = since
        .as_deref()
        .map(crate::logs::since)
        .transpose()?
        .map(|time| (unix_seconds(&time).unwrap_or(0), time));
    let mut scope = Scope::open(global, environment, "audit list", false)?;
    let actor_id = match &actor {
        Some(actor) => Some(actor_id(&mut scope, actor).await?),
        None => None,
    };
    // From `--since` on: one page forward. Else the newest page: from a
    // window before the last record, every page of it, the last 100 kept.
    let (after, limit, pages) = match &since {
        Some((seconds, _)) => (after_seq(&mut scope, *seconds).await?, PAGE, 1),
        None => {
            let last = after_seq(&mut scope, i64::MAX).await?;
            match actor_id {
                Some(_) => (last.saturating_sub(ACTOR_WINDOW), WINDOW_PAGE, WINDOW_PAGES),
                None => (last.saturating_sub(PAGE as u64), PAGE, 1),
            }
        }
    };
    let mut query = vec![format!("limit={limit}")];
    if let Some(id) = &actor_id {
        query.push(format!("actor_id={}", query_value(id)));
    }
    query.push(format!("after_seq={after}"));
    let path = format!("{}?{}", LIST_AUDIT.path, query.join("&"));
    let mut records: Vec<Value> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..pages {
        let page_path = match &cursor {
            Some(cursor) => format!("{path}&cursor={}", query_value(cursor)),
            None => path.clone(),
        };
        let page = scope.answer(&page_path).await?.json()?;
        records.extend(page["items"].as_array().cloned().unwrap_or_default());
        cursor = page["next_cursor"]
            .as_str()
            .filter(|c| !c.is_empty())
            .map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    if since.is_none() && records.len() > PAGE {
        records.drain(..records.len() - PAGE);
    }
    if global.json {
        let body = json!({ "items": records, "next_cursor": cursor });
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    if records.is_empty() {
        let mut line = "No audit records".to_string();
        if let Some(id) = &actor_id {
            line.push_str(&format!(" of {id}"));
        }
        match &since {
            Some((_, time)) => line.push_str(&format!(" since {time}")),
            None if actor_id.is_some() => {
                line.push_str(&format!(
                    " among the newest {ACTOR_WINDOW}; --since T looks further back"
                ));
            }
            None => {}
        }
        line.push_str(".\n");
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        ["SEQ", "TIME", "ACTOR", "OPERATION", "TARGET", "RESULT"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    ];
    for record in &records {
        let target = match record["target_id"].as_str() {
            Some(id) => format!("{} {id}", text(record, "target_type")),
            None => "-".to_string(),
        };
        let result = match record["result_detail"].as_str() {
            Some(detail) => format!("{} ({detail})", text(record, "result")),
            None => text(record, "result"),
        };
        rows.push(vec![
            text(record, "seq"),
            text(record, "time"),
            format!(
                "{} {}",
                text(record, "actor_type"),
                text(record, "actor_id")
            ),
            text(record, "operation"),
            target,
            result,
        ]);
    }
    let mut out = scope::table(&rows);
    match cursor {
        Some(cursor) => out.push_str(&format!(
            "More records follow; quoro api GET '{path}&cursor={}' reads the next page.\n",
            query_value(&cursor)
        )),
        None => out.push_str("No newer records.\n"),
    }
    crate::api::write(stdout, out.as_bytes())
}

/// The `actor_id` that `actor` names.
async fn actor_id(scope: &mut Scope, actor: &str) -> Result<String, Failure> {
    if ACTOR_PREFIXES.iter().any(|prefix| is_id(prefix, actor)) {
        return Ok(actor.to_string());
    }
    let by_email = actor.contains('@');
    if by_email && scope.is_agent {
        return Err(Failure::usage(format!(
            "reading users needs user:read, which only a human holds: name the user by its id \
             (usr_...), not '{actor}'; nothing was sent"
        )));
    }
    let mut found: Vec<Value> = Vec::new();
    if !by_email {
        let agents = scope
            .list_all(LIST_AGENTS.path)
            .await
            .map_err(|mut failure| {
                if failure.code == exit::FORBIDDEN {
                    failure.message = format!(
                        "{}; reading agents by name needs agent:read: name the actor by its id",
                        failure.message
                    );
                }
                failure
            })?;
        found.extend(agents.into_iter().filter(|agent| agent["name"] == actor));
    }
    if found.is_empty() && !scope.is_agent {
        let users = scope.list_all(LIST_USERS.path).await?;
        let key = if by_email { "email" } else { "name" };
        found.extend(users.into_iter().filter(|user| user[key] == actor));
    }
    match found.as_slice() {
        [one] => Ok(text(one, "id")),
        [] => Err(Failure::new(
            exit::NOT_FOUND,
            format!(
                "no {} has the name '{actor}'; name an actor by its id (usr_, agt_ or nod_), \
                 or a system subsystem with quoro api GET '{}?actor_id=NAME'",
                if scope.is_agent {
                    "agent"
                } else {
                    "agent or user"
                },
                LIST_AUDIT.path
            ),
        )),
        several => Err(Failure::usage(format!(
            "'{actor}' names {} actors: {}; name one by its id",
            several.len(),
            several
                .iter()
                .map(|actor| text(actor, "id"))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// The `after_seq` that starts the log at `since` (Unix seconds): the last
/// record before it, 0 when the log starts later, the last record when none
/// is that recent (with `i64::MAX`: the last record's `seq`).
async fn after_seq(scope: &mut Scope, since: i64) -> Result<u64, Failure> {
    // `before` is a record older than `since` (0: none); `from` is at or
    // after `since`, or past the end of the log.
    let mut before = 0u64;
    let mut from = 1u64;
    while recorded_before(scope, from, since).await? {
        before = from;
        from = from.saturating_mul(2);
    }
    while from - before > 1 {
        let middle = before + (from - before) / 2;
        if recorded_before(scope, middle, since).await? {
            before = middle;
        } else {
            from = middle;
        }
    }
    Ok(before)
}

/// Whether the record `seq` exists and was recorded before `since`.
async fn recorded_before(scope: &mut Scope, seq: u64, since: i64) -> Result<bool, Failure> {
    let path = GET_AUDIT_RECORD.path.replace("{seq}", &seq.to_string());
    let answer = scope.answer(&path).await?;
    if answer.status == 404 {
        return Ok(false);
    }
    let record = answer.json()?;
    let time = record["time"]
        .as_str()
        .and_then(unix_seconds)
        .ok_or_else(|| Failure::other(format!("the audit record {seq} has no readable time")))?;
    Ok(time < since)
}

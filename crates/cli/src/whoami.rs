//! `quoro whoami`: the context and the identity behind its credential.
//!
//! A human shows its name, email and role; an agent its name, model and
//! owner, and its active grants, which is what it checks before it plans a
//! call (`getMe`). `--json` prints `getMe` unchanged.

use crate::Environment;
use crate::agent::default_justification;
use crate::cli::Global;
use crate::client::{Answer, Call, Session};
use crate::context;
use crate::error::Failure;
use crate::operations::GET_ME;
use serde_json::Value;
use std::io::Write;

pub async fn run(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let context = context::resolve(global.context.as_deref(), environment)?;
    let name = context
        .name
        .clone()
        .unwrap_or_else(|| "(ad hoc, from QUORO_URL)".to_string());
    let url = context.url.clone();
    let reason = environment
        .justification(global)
        .unwrap_or_else(|| default_justification("whoami"));
    let mut session = Session::open(context, environment, &reason)?;
    let answer = Answer::read(session.send(&Call::get(GET_ME.path)).await?, &url).await?;
    if global.json {
        crate::api::write(stdout, &answer.body)?;
        return if answer.is_success() {
            Ok(())
        } else {
            Err(answer.failure())
        };
    }
    let me = answer.json()?;
    crate::api::write(stdout, describe(&name, &url, &me).as_bytes())
}

/// The text `whoami` prints for a `getMe` answer.
fn describe(name: &str, url: &str, me: &Value) -> String {
    let text = |value: &Value, key: &str| value[key].as_str().unwrap_or("?").to_string();
    let mut out = format!("context  {name} ({url})\n");
    match me["type"].as_str() {
        Some("agent") => {
            let agent = &me["agent"];
            out.push_str(&format!(
                "agent    {} ({}), model {}, owner {}\n",
                text(agent, "name"),
                text(agent, "id"),
                text(agent, "model"),
                text(agent, "owner_user_id")
            ));
            let grants = me["grants"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default();
            if grants.is_empty() {
                out.push_str("grants   none\n");
            }
            for (index, grant) in grants.iter().enumerate() {
                let scopes: Vec<&str> = grant["scopes"]
                    .as_array()
                    .map(|scopes| scopes.iter().filter_map(Value::as_str).collect())
                    .unwrap_or_default();
                let mut line = format!(
                    "{}{} {}  on {}  until {}",
                    if index == 0 { "grants   " } else { "         " },
                    text(grant, "id"),
                    scopes.join(","),
                    selector(&grant["selector"]),
                    text(grant, "expires_at")
                );
                if let Some(max) = grant["max_uses"].as_u64() {
                    line.push_str(&format!(
                        "  uses {}/{max}",
                        grant["uses"].as_u64().unwrap_or(0)
                    ));
                }
                out.push_str(&line);
                out.push('\n');
            }
        }
        _ => {
            let user = &me["user"];
            out.push_str(&format!(
                "user     {} <{}> ({}), role {}\n",
                text(user, "name"),
                text(user, "email"),
                text(user, "id"),
                text(user, "role")
            ));
        }
    }
    out
}

/// `apps=app_1,app_2 projects=demo`; `{}` matches no resource.
fn selector(selector: &Value) -> String {
    let Some(fields) = selector.as_object().filter(|fields| !fields.is_empty()) else {
        return "{}".to_string();
    };
    fields
        .iter()
        .map(|(key, values)| match values.as_array() {
            Some(values) => format!(
                "{key}={}",
                values
                    .iter()
                    .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            None => format!("{key}={values}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_agent_shows_its_grants() {
        let me = json!({
            "type": "agent",
            "agent": { "id": "agt_1", "name": "ops", "model": "m", "owner_user_id": "usr_1" },
            "grants": [
                { "id": "grt_1", "scopes": ["app:read", "app:restart"],
                  "selector": { "projects": ["demo"] }, "expires_at": "2026-11-06T11:00:00.000Z" },
                { "id": "grt_2", "scopes": ["app:delete"], "selector": { "apps": ["app_1"] },
                  "expires_at": "2026-11-06T10:10:00.000Z", "max_uses": 1, "uses": 0 },
            ],
        });
        assert_eq!(
            describe("lab", "https://p", &me),
            "context  lab (https://p)\n\
             agent    ops (agt_1), model m, owner usr_1\n\
             grants   grt_1 app:read,app:restart  on projects=demo  until 2026-11-06T11:00:00.000Z\n         \
             grt_2 app:delete  on apps=app_1  until 2026-11-06T10:10:00.000Z  uses 0/1\n"
        );
    }

    #[test]
    fn a_user_shows_its_role() {
        let me = json!({
            "type": "user",
            "user": { "id": "usr_1", "name": "Owner", "email": "o@example.com", "role": "owner" },
            "grants": [],
        });
        assert_eq!(
            describe("lab", "https://p", &me),
            "context  lab (https://p)\nuser     Owner <o@example.com> (usr_1), role owner\n"
        );
    }
}

//! Just-in-time approval of a destructive call (permission-model.md
//! section 9).
//!
//! An agent's destructive call without a matching approval grant is answered
//! `approval_required` with the `scope` and `target` to ask for. With
//! `--request-approval` the client files `POST /api/v1/approvals` with that
//! scope and target, the operation's endpoint template and the params the
//! primary hashes for that call; with `--wait` it then waits for a human's
//! decision and, once approved, sends the exact same request again, once.
//! Deciding stays in the panel: there is no client command for it.
//!
//! The approval grant matches the repeated call by target and `params_hash`,
//! the SHA-256 of the canonical params. The problem does not carry the params,
//! so the client reproduces them per operation ([`reproduce`]): the
//! operations it knows are listed there, and any other is refused with a
//! pointer to filing the request by hand rather than with params that would
//! never match.
//!
//! The wait follows `streamEvents` for the approval's decision when the agent
//! may read events, and reads `getApproval` every few seconds as well, so a
//! decision is seen even when the stream filters it out or was made before
//! the stream opened. It ends at the approval's `expires_at`.

use crate::client::{Answer, Call, Session, now_unix, unix_seconds};
use crate::error::{Failure, exit};
use crate::operations::{
    CREATE_APPROVAL, DELETE_APP, DELETE_VM, DELETE_VOLUME, GET_APPROVAL, Operation, RESTORE_BACKUP,
    STREAM_EVENTS,
};
use reqwest::header::HeaderValue;
use reqwest::{Method, Response};
use serde_json::{Value, json};
use std::io::Write;
use std::time::Duration;

/// How often the approval is read while waiting.
const POLL: Duration = Duration::from_secs(2);
/// How long past `expires_at` the wait keeps reading, for the primary's
/// expiry sweep.
const EXPIRY_GRACE_S: i64 = 5;

/// The call that needs approval and how to file it.
pub struct Request<'a> {
    pub call: &'a Call,
    /// `--reason` or `QUORO_JUSTIFICATION`; the request's justification.
    pub justification: Option<&'a str>,
    /// `--incident`.
    pub incident: Option<&'a str>,
    /// `--json`: print the filed approval as JSON.
    pub json: bool,
    /// `--wait`.
    pub wait: bool,
}

/// How a request ended.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Approved,
    Denied(Option<String>),
    Expired,
}

/// Files the approval `problem` (an `approval_required` body) asks for.
/// Without `wait`, prints the approval id and fails with exit 3. With it,
/// waits for the decision and returns the response of the repeated call.
pub async fn file(
    session: &mut Session,
    request: &Request<'_>,
    problem: &[u8],
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Response, Failure> {
    let refused = Failure::from_response(403, problem, None);
    let problem: Value = serde_json::from_slice(problem).unwrap_or(Value::Null);
    let call = request.call;
    let (Some(scope), Some(target)) = (
        problem["scope"].as_str(),
        problem.get("target").filter(|t| t.is_object()),
    ) else {
        return Err(Failure::new(
            exit::APPROVAL_REQUIRED,
            format!(
                "{}; the answer names no scope or target to ask for, so no approval was filed",
                refused.message
            ),
        ));
    };
    let Some((operation, params)) = reproduce(call) else {
        return Err(Failure::new(
            exit::APPROVAL_REQUIRED,
            format!(
                "{}; the client cannot reproduce the params of {} {}, so no approval was filed: \
                 file one with 'quoro api POST {}'",
                refused.message, call.method, call.path, CREATE_APPROVAL.path
            ),
        ));
    };
    let target_text = format!(
        "{} {}",
        target["type"].as_str().unwrap_or("?"),
        target["id"].as_str().unwrap_or("?")
    );
    let justification = request.justification.map_or_else(
        || format!("quoro api {} {}", call.method, call.path),
        str::to_string,
    );
    let mut body = json!({
        "scope": scope,
        "target": target,
        "operation": operation,
        "params": params,
        "justification": justification,
    });
    if let Some(incident) = request.incident {
        body["incident_id"] = json!(incident.trim());
    }
    let mut filing = Call {
        method: Method::POST,
        path: CREATE_APPROVAL.path.to_string(),
        headers: reqwest::header::HeaderMap::new(),
        body: Some(body.to_string().into_bytes()),
    };
    for (name, value) in [
        ("quoro-justification", Some(justification.as_str())),
        ("quoro-incident", request.incident),
    ] {
        if let Some(value) = value {
            let value = HeaderValue::from_str(value.trim())
                .map_err(|_| Failure::usage("--reason must be printable ASCII on one line"))?;
            filing.headers.insert(name, value);
        }
    }
    let answer = Answer::read(session.send(&filing).await?, &session.context.url).await?;
    let approval = answer.json().map_err(|mut failure| {
        failure.message = format!("cannot file the approval request: {}", failure.message);
        failure
    })?;
    let id = approval["id"]
        .as_str()
        .filter(|id| {
            id.starts_with("apr_") && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
        .ok_or_else(|| Failure::other("the approval answer has no id"))?
        .to_string();
    let expires_at = approval["expires_at"].as_str().unwrap_or("?").to_string();

    if !request.wait {
        let printed = if request.json {
            format!("{approval}\n")
        } else {
            format!("{id}\n")
        };
        crate::api::write(stdout, printed.as_bytes())?;
        return Err(Failure::new(
            exit::APPROVAL_REQUIRED,
            format!(
                "approval {id} requested for {scope} on {target_text}; a human decides in the panel \
                 until {expires_at}. Repeat the same call once it is approved, or pass --wait"
            ),
        ));
    }

    let _ = writeln!(
        stderr,
        "quoro: approval {id} requested for {scope} on {target_text}; waiting for a human to decide in the panel (until {expires_at})"
    );
    let deadline = unix_seconds(&expires_at).unwrap_or_else(|| now_unix() + 3600);
    match wait(session, &id, deadline).await? {
        Outcome::Approved => {
            let _ = writeln!(stderr, "quoro: approval {id} approved; repeating the call");
            session.send(call).await
        }
        Outcome::Denied(note) => Err(Failure::new(
            exit::APPROVAL_REQUIRED,
            match note {
                Some(note) => format!("approval {id} was denied: {note}"),
                None => format!("approval {id} was denied"),
            },
        )),
        Outcome::Expired => Err(Failure::new(
            exit::APPROVAL_REQUIRED,
            format!("approval {id} expired before a human decided"),
        )),
    }
}

/// The endpoint template and the params the primary hashes for `call`, for
/// the destructive operations whose params the client can rebuild. Each
/// matches the `params` its handler authorizes with
/// (`crates/control-plane/src/api` and `service`).
fn reproduce(call: &Call) -> Option<(String, Value)> {
    let path = call.path.split(['?', '#']).next().unwrap_or_default();
    let operation = |op: &Operation| format!("{} {}", op.method, op.path);
    for (op, name) in [
        (DELETE_APP, "app_id"),
        (DELETE_VM, "vm_id"),
        (DELETE_VOLUME, "volume_id"),
    ] {
        if call.method.as_str() == op.method
            && let Some(id) = path_parameter(op.path, path)
        {
            let mut params = serde_json::Map::new();
            params.insert(name.to_string(), json!(id));
            return Some((operation(&op), Value::Object(params)));
        }
    }
    if call.method.as_str() == RESTORE_BACKUP.method
        && path_parameter(RESTORE_BACKUP.path, path).is_some()
    {
        // The restore hashes the body's node, not the path's backup.
        let body: Value = serde_json::from_slice(call.body.as_deref()?).ok()?;
        let node_id = body["node_id"].as_str()?;
        return Some((operation(&RESTORE_BACKUP), json!({ "node_id": node_id })));
    }
    None
}

/// The value of a template's one `{parameter}` in `path`, when the path has
/// the template's shape.
fn path_parameter<'a>(template: &str, path: &'a str) -> Option<&'a str> {
    let template: Vec<&str> = template.split('/').collect();
    let path: Vec<&str> = path.split('/').collect();
    if template.len() != path.len() {
        return None;
    }
    let mut value = None;
    for (expected, actual) in template.iter().zip(&path) {
        if expected.starts_with('{') && expected.ends_with('}') {
            if actual.is_empty() {
                return None;
            }
            value = Some(*actual);
        } else if expected != actual {
            return None;
        }
    }
    value
}

/// Waits for the decision on `id` until `deadline` (Unix seconds).
async fn wait(session: &mut Session, id: &str, deadline: i64) -> Result<Outcome, Failure> {
    let stream_call = Call::get(format!(
        "{}?type=approval.approved&type=approval.denied&type=approval.expired&subject_id={id}",
        STREAM_EVENTS.path
    ));
    let mut stream = match session.send(&stream_call).await {
        Ok(response) if response.status().as_u16() == 200 => Some(response),
        _ => None,
    };
    let mut buffer: Vec<u8> = Vec::new();
    let mut can_read = true;
    loop {
        if can_read {
            match read(session, id).await {
                Ok(Some(outcome)) => return Ok(outcome),
                Ok(None) => {}
                Err(failure) if failure.code == exit::FORBIDDEN && stream.is_some() => {
                    can_read = false;
                }
                Err(mut failure) => {
                    failure.message = format!(
                        "cannot wait for approval {id}: {}; the agent needs approval:read or event:read",
                        failure.message
                    );
                    return Err(failure);
                }
            }
        }
        if now_unix() > deadline + EXPIRY_GRACE_S {
            return Ok(Outcome::Expired);
        }
        let Some(response) = stream.as_mut() else {
            tokio::time::sleep(POLL).await;
            continue;
        };
        match tokio::time::timeout(POLL, response.chunk()).await {
            Err(_) => {}
            Ok(Ok(Some(chunk))) => {
                buffer.extend_from_slice(&chunk);
                if let Some(outcome) = decision(&mut buffer, id) {
                    return Ok(outcome);
                }
            }
            Ok(Ok(None) | Err(_)) => {
                stream = None;
                if !can_read {
                    return Err(Failure::new(
                        exit::UNREACHABLE,
                        format!("the event stream ended before approval {id} was decided"),
                    ));
                }
            }
        }
    }
}

/// `getApproval`: the outcome, or `None` while pending or rate limited.
async fn read(session: &mut Session, id: &str) -> Result<Option<Outcome>, Failure> {
    let path = GET_APPROVAL.path.replace("{approval_id}", id);
    let answer = Answer::read(session.send(&Call::get(path)).await?, &session.context.url).await?;
    // A long wait may meet the rate limit; the next read tries again.
    if answer.status == 429 {
        return Ok(None);
    }
    let approval = answer.json()?;
    Ok(match approval["status"].as_str() {
        Some("approved") => Some(Outcome::Approved),
        Some("denied") => Some(Outcome::Denied(
            approval["decision_note"].as_str().map(str::to_string),
        )),
        Some("expired") => Some(Outcome::Expired),
        _ => None,
    })
}

/// The decision in the complete SSE frames of `buffer`, which keeps only the
/// incomplete tail.
fn decision(buffer: &mut Vec<u8>, id: &str) -> Option<Outcome> {
    loop {
        let end = buffer.windows(2).position(|pair| pair == b"\n\n")?;
        let frame: Vec<u8> = buffer.drain(..end + 2).collect();
        let frame = String::from_utf8_lossy(&frame).replace('\r', "");
        if !frame.contains(id) {
            continue;
        }
        let kind = frame
            .lines()
            .find_map(|line| line.strip_prefix("event:"))
            .map(str::trim);
        match kind {
            Some("approval.approved") => return Some(Outcome::Approved),
            Some("approval.denied") => return Some(Outcome::Denied(None)),
            Some("approval.expired") => return Some(Outcome::Expired),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderMap;

    fn call(method: Method, path: &str, body: Option<&str>) -> Call {
        Call {
            method,
            path: path.to_string(),
            headers: HeaderMap::new(),
            body: body.map(|b| b.as_bytes().to_vec()),
        }
    }

    #[test]
    fn the_params_match_what_each_handler_hashes() {
        let app = format!("app_{}", "1".repeat(26));
        assert_eq!(
            reproduce(&call(Method::DELETE, &format!("/api/v1/apps/{app}"), None)),
            Some((
                "DELETE /api/v1/apps/{app_id}".to_string(),
                json!({ "app_id": app })
            ))
        );
        assert_eq!(
            reproduce(&call(Method::DELETE, "/api/v1/vms/vm_1", None))
                .unwrap()
                .1,
            json!({ "vm_id": "vm_1" })
        );
        assert_eq!(
            reproduce(&call(Method::DELETE, "/api/v1/volumes/vol_1", None))
                .unwrap()
                .1,
            json!({ "volume_id": "vol_1" })
        );
        assert_eq!(
            reproduce(&call(
                Method::POST,
                "/api/v1/backups/bkp_1/restore",
                Some(r#"{"node_id":"nod_1"}"#)
            )),
            Some((
                "POST /api/v1/backups/{backup_id}/restore".to_string(),
                json!({ "node_id": "nod_1" })
            ))
        );
        for unknown in [
            call(Method::GET, "/api/v1/apps/app_1", None),
            call(Method::DELETE, "/api/v1/apps/app_1/domains/d", None),
            call(Method::DELETE, "/api/v1/apps/", None),
            call(Method::POST, "/api/v1/backups/bkp_1/restore", None),
            call(Method::DELETE, "/api/v1/projects/prj_1", None),
        ] {
            assert_eq!(
                reproduce(&unknown),
                None,
                "{} {}",
                unknown.method,
                unknown.path
            );
        }
    }

    #[test]
    fn a_decision_is_read_from_complete_frames_only() {
        let id = "apr_1";
        let mut buffer = b": keep-alive\n\nid: 7\nevent: approval.appro".to_vec();
        assert_eq!(decision(&mut buffer, id), None);
        assert_eq!(buffer, b"id: 7\nevent: approval.appro");
        buffer.extend_from_slice(b"ved\ndata: {\"subject_id\":\"apr_1\"}\n\n");
        assert_eq!(decision(&mut buffer, id), Some(Outcome::Approved));

        let mut other = b"event: approval.denied\ndata: {\"subject_id\":\"apr_2\"}\n\n".to_vec();
        assert_eq!(decision(&mut other, id), None, "another approval");
        let mut denied =
            b"event: approval.denied\r\ndata: {\"subject_id\":\"apr_1\"}\r\n\n".to_vec();
        assert_eq!(decision(&mut denied, id), Some(Outcome::Denied(None)));
    }
}

//! `quoro api METHOD PATH [--data @FILE|-]`: any API operation, unchanged.
//!
//! The way to reach everything the command tree leaves out (human-only
//! operations, streams, new endpoints). The response body goes to stdout as
//! the primary sent it, pretty-printed only for a terminal and without
//! `--json` (keys then come out sorted); an error is mapped to its exit code.
//! A request body is read from a file or stdin, never from the command line,
//! where any local user could read it in the process list.
//!
//! An agent's call that changes something needs `--reason`. A call answered
//! `approval_required` can file the approval request (`--request-approval`)
//! and wait for the decision (`--wait`), then runs again unchanged
//! (`approval.rs`).

use crate::Environment;
use crate::agent;
use crate::approval;
use crate::cli::{ApiArgs, Global};
use crate::client::{self, Answer, Call, Session};
use crate::context;
use crate::error::{Failure, exit};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, Response};
use std::io::{Read as _, Write};

pub async fn run(
    global: &Global,
    args: ApiArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    // Everything local is checked before the context, so a typo never
    // reaches the network.
    let method = method(&args.method)?;
    if !args.path.starts_with('/') {
        return Err(Failure::usage(format!(
            "the path {:?} must start with /, such as /api/v1/health",
            args.path
        )));
    }
    let body = args.data.as_deref().map(read_body).transpose()?;
    let justification = environment.justification(global);
    let headers = change_headers(
        &method,
        justification.as_deref(),
        global.incident.as_deref(),
    )?;
    let context = context::resolve(global.context.as_deref(), environment)?;
    // An agent says why it changes something (permission-model.md section
    // 10, D-0031); without a reason nothing is sent, not even a mint.
    if method != Method::GET && context.is_agent() && justification.is_none() {
        return Err(Failure::usage(format!(
            "an agent must say why it calls {} {}: pass --reason TEXT or set QUORO_JUSTIFICATION; nothing was sent",
            method, args.path
        )));
    }
    let mint_reason = justification
        .clone()
        .unwrap_or_else(|| agent::default_justification("api"));
    let mut session = Session::open(context, environment, &mint_reason)?;
    let call = Call {
        method,
        path: args.path,
        headers,
        body,
    };

    let response = session.send(&call).await?;
    let response = if !response.status().is_success() && args.approval.request_approval {
        let answer = Answer::read(response, &session.context.url).await?;
        let failure = answer.failure();
        if failure.code != exit::APPROVAL_REQUIRED {
            return fail(answer, global, stdout);
        }
        let request = approval::Request {
            call: &call,
            justification: justification.as_deref(),
            incident: global.incident.as_deref(),
            json: global.json,
            wait: args.approval.wait,
        };
        approval::file(&mut session, &request, &answer.body, stdout, stderr).await?
    } else {
        response
    };
    output(response, &session.context.url, environment, global, stdout).await
}

/// Prints a response: the body of a success, or the failure of an error.
async fn output(
    response: Response,
    url: &str,
    environment: &Environment,
    global: &Global,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let request_id = client::request_id(&response);
    if !response.status().is_success() {
        return fail(Answer::read(response, url).await?, global, stdout);
    }
    let pretty = environment.stdout_is_terminal && !global.json && is_json(&response);
    if pretty {
        let body = client::read_all(response, url, request_id).await?;
        match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(value) => {
                let text = serde_json::to_string_pretty(&value).unwrap_or_default();
                write(stdout, format!("{text}\n").as_bytes())
            }
            Err(_) => write(stdout, &body),
        }
    } else {
        stream(response, url, request_id, stdout).await
    }
}

/// An error answer: its body on stdout with `--json`, and its failure.
fn fail(answer: Answer, global: &Global, stdout: &mut dyn Write) -> Result<(), Failure> {
    if global.json {
        write(stdout, &answer.body)?;
    }
    Err(answer.failure())
}

/// The method, in any case; only those the API uses.
fn method(text: &str) -> Result<Method, Failure> {
    match text.to_ascii_uppercase().as_str() {
        "GET" => Ok(Method::GET),
        "POST" => Ok(Method::POST),
        "PUT" => Ok(Method::PUT),
        "PATCH" => Ok(Method::PATCH),
        "DELETE" => Ok(Method::DELETE),
        _ => Err(Failure::usage(format!(
            "unknown method {text:?}; expected GET, POST, PUT, PATCH or DELETE"
        ))),
    }
}

/// `@FILE` or `-` (stdin); the body must be JSON.
fn read_body(data: &str) -> Result<Vec<u8>, Failure> {
    let (body, source) = if data == "-" {
        let mut body = Vec::new();
        std::io::stdin()
            .read_to_end(&mut body)
            .map_err(|e| Failure::usage(format!("cannot read the body from stdin: {e}")))?;
        (body, "stdin".to_string())
    } else if let Some(path) = data.strip_prefix('@') {
        let body = std::fs::read(path)
            .map_err(|e| Failure::usage(format!("cannot read the body from {path}: {e}")))?;
        (body, path.to_string())
    } else {
        return Err(Failure::usage(
            "--data takes @FILE or - (stdin), so a body never sits in the process list",
        ));
    };
    serde_json::from_slice::<serde::de::IgnoredAny>(&body)
        .map_err(|e| Failure::usage(format!("the body from {source} is not JSON: {e}")))?;
    Ok(body)
}

/// `--reason` (or `QUORO_JUSTIFICATION`) and `--incident` on a call that
/// changes something (`docs/contracts/README.md`, "Request headers").
pub(crate) fn change_headers(
    method: &Method,
    justification: Option<&str>,
    incident: Option<&str>,
) -> Result<HeaderMap, Failure> {
    let mut headers = HeaderMap::new();
    if *method == Method::GET {
        return Ok(headers);
    }
    let fields = [
        ("quoro-justification", "--reason", justification),
        ("quoro-incident", "--incident", incident),
    ];
    for (name, flag, value) in fields {
        let Some(value) = value else { continue };
        let value = HeaderValue::from_str(value.trim())
            .map_err(|_| Failure::usage(format!("{flag} must be printable ASCII on one line")))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn is_json(response: &Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"))
}

/// Copies the body as it arrives, so a stream through `quoro api` prints
/// each event when it comes.
async fn stream(
    mut response: Response,
    url: &str,
    request_id: Option<String>,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => write(stdout, &chunk)?,
            Ok(None) => return Ok(()),
            Err(e) => {
                return Err(Failure {
                    request_id,
                    ..client::unreachable(url, &e)
                });
            }
        }
    }
}

pub(crate) fn write(stdout: &mut dyn Write, bytes: &[u8]) -> Result<(), Failure> {
    stdout
        .write_all(bytes)
        .and_then(|()| stdout.flush())
        .map_err(|e| Failure::other(format!("cannot write to stdout: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_are_read_in_any_case_and_limited_to_the_api() {
        assert_eq!(method("get").unwrap(), Method::GET);
        assert_eq!(method("Delete").unwrap(), Method::DELETE);
        assert_eq!(method("TRACE").unwrap_err().code, exit::USAGE);
    }

    #[test]
    fn a_body_comes_from_a_file_and_must_be_json() {
        assert!(
            read_body("{\"a\":1}")
                .unwrap_err()
                .message
                .contains("@FILE"),
            "an inline body is refused"
        );
        let path = std::env::temp_dir().join(format!("quoro-cli-body-{}", std::process::id()));
        std::fs::write(&path, b"{\"name\": \"demo\"}").unwrap();
        let data = format!("@{}", path.display());
        assert_eq!(read_body(&data).unwrap(), b"{\"name\": \"demo\"}");
        std::fs::write(&path, b"name=demo").unwrap();
        assert_eq!(read_body(&data).unwrap_err().code, exit::USAGE);
        let _ = std::fs::remove_file(path);
    }
}

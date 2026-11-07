//! Failures and their exit codes.
//!
//! The API answers errors as `application/problem+json` with a `code`
//! (`docs/contracts/README.md`, "Errors"). The client maps that code to a
//! fixed exit code, prints one line naming the problem and the
//! `Quoro-Request-Id` the primary logged it under, and nothing else: no body,
//! no header, no credential.

use serde_json::Value;
use std::io::Write;

/// The client's exit codes. A script or an agent branches on these, so they
/// never change meaning.
///
/// - `unauthenticated` is [`OTHER`]: a missing or wrong credential is fixed
///   locally, not by a grant, so it is kept apart from [`FORBIDDEN`].
/// - `validation_failed`, `rate_limited`, `not_primary` and `internal` are
///   [`OTHER`]; [`USAGE`] stays for command lines the client itself refuses
///   before it sends anything.
/// - `forbidden` is [`FORBIDDEN`] whatever its `reason`, `human_only`
///   included: retrying the same call cannot succeed.
pub mod exit {
    /// Success.
    pub const OK: u8 = 0;
    /// Any other failure: unauthenticated, invalid input, a local file.
    pub const OTHER: u8 = 1;
    /// A command line, a context or a request body the client refused.
    pub const USAGE: u8 = 2;
    /// `approval_required`: a human must approve the call first.
    pub const APPROVAL_REQUIRED: u8 = 3;
    /// `forbidden`, including the `human_only` reason.
    pub const FORBIDDEN: u8 = 4;
    /// `not_found`.
    pub const NOT_FOUND: u8 = 5;
    /// `conflict`.
    pub const CONFLICT: u8 = 6;
    /// The primary could not be reached (connection, TLS pin, timeout), or it
    /// could not reach the node (`node_unreachable`).
    pub const UNREACHABLE: u8 = 7;
}

/// A command that did not succeed.
#[derive(Debug)]
pub struct Failure {
    pub code: u8,
    pub message: String,
    /// The primary's `Quoro-Request-Id`, when a response arrived.
    pub request_id: Option<String>,
}

impl Failure {
    pub fn new(code: u8, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            request_id: None,
        }
    }

    pub fn usage(message: impl Into<String>) -> Self {
        Self::new(exit::USAGE, message)
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::new(exit::OTHER, message)
    }

    /// The failure an error response stands for. `body` is read as a problem
    /// when it is one; any other body falls back to the HTTP status.
    pub fn from_response(status: u16, body: &[u8], request_id: Option<String>) -> Self {
        let problem: Option<Value> = serde_json::from_slice(body)
            .ok()
            .filter(|value: &Value| value.get("code").is_some_and(Value::is_string));
        let (code, message) = match &problem {
            Some(problem) => (exit_code(problem_code(problem), status), describe(problem)),
            None => (
                exit_code("", status),
                format!("the primary answered HTTP {status}"),
            ),
        };
        Self {
            code,
            message,
            request_id,
        }
    }

    /// Prints the failure on `stderr` and returns its exit code.
    pub fn report(&self, stderr: &mut dyn Write) -> u8 {
        let _ = writeln!(stderr, "quoro: {}", one_line(&self.message));
        if let Some(request_id) = &self.request_id {
            let _ = writeln!(stderr, "request id: {request_id}");
        }
        self.code
    }
}

fn problem_code(problem: &Value) -> &str {
    problem.get("code").and_then(Value::as_str).unwrap_or("")
}

/// The exit code of a problem `code`, or of the HTTP status when the body
/// carried none.
fn exit_code(code: &str, status: u16) -> u8 {
    match code {
        "approval_required" => exit::APPROVAL_REQUIRED,
        "forbidden" => exit::FORBIDDEN,
        "not_found" => exit::NOT_FOUND,
        "conflict" => exit::CONFLICT,
        "node_unreachable" => exit::UNREACHABLE,
        "" => match status {
            403 => exit::FORBIDDEN,
            404 => exit::NOT_FOUND,
            409 => exit::CONFLICT,
            504 => exit::UNREACHABLE,
            _ => exit::OTHER,
        },
        _ => exit::OTHER,
    }
}

/// `<detail or title> (<code>[, <reason>])`, plus the scope and target an
/// approval needs, so the line alone says what to ask for.
fn describe(problem: &Value) -> String {
    let text = |key: &str| problem.get(key).and_then(Value::as_str);
    let summary = text("detail").or(text("title")).unwrap_or("request failed");
    let code = problem_code(problem);
    let mut message = match text("reason") {
        Some(reason) => format!("{summary} ({code}, {reason})"),
        None => format!("{summary} ({code})"),
    };
    if let Some(scope) = text("scope") {
        message.push_str(&format!("; scope {scope}"));
    }
    let target = problem.get("target");
    let target_text = |key: &str| target.and_then(|t| t.get(key)).and_then(Value::as_str);
    if let (Some(kind), Some(id)) = (target_text("type"), target_text("id")) {
        message.push_str(&format!(" on {kind} {id}"));
    }
    message
}

/// Server text is printed on one line, so the request id is always the next.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn problem(body: Value) -> Failure {
        Failure::from_response(
            body["status"].as_u64().unwrap_or(400) as u16,
            body.to_string().as_bytes(),
            Some("req_1".to_string()),
        )
    }

    #[test]
    fn every_contract_code_has_its_exit_code() {
        let cases = [
            ("unauthenticated", 401, exit::OTHER),
            ("forbidden", 403, exit::FORBIDDEN),
            ("approval_required", 403, exit::APPROVAL_REQUIRED),
            ("not_found", 404, exit::NOT_FOUND),
            ("conflict", 409, exit::CONFLICT),
            ("validation_failed", 422, exit::OTHER),
            ("rate_limited", 429, exit::OTHER),
            ("not_primary", 503, exit::OTHER),
            ("node_unreachable", 504, exit::UNREACHABLE),
            ("internal", 500, exit::OTHER),
        ];
        for (code, status, expected) in cases {
            let failure = problem(json!({ "code": code, "status": status, "title": "t" }));
            assert_eq!(failure.code, expected, "{code}");
        }
    }

    #[test]
    fn human_only_is_forbidden() {
        let failure = problem(json!({
            "code": "forbidden", "status": 403, "title": "Forbidden", "reason": "human_only"
        }));
        assert_eq!(failure.code, exit::FORBIDDEN);
        assert_eq!(failure.message, "Forbidden (forbidden, human_only)");
    }

    #[test]
    fn an_approval_names_its_scope_and_target() {
        let failure = problem(json!({
            "code": "approval_required", "status": 403, "title": "Approval required",
            "scope": "app:delete", "target": { "type": "app", "id": "app_1" }
        }));
        assert_eq!(
            failure.message,
            "Approval required (approval_required); scope app:delete on app app_1"
        );
    }

    #[test]
    fn a_body_without_a_code_falls_back_to_the_status() {
        assert_eq!(Failure::from_response(404, b"", None).code, exit::NOT_FOUND);
        assert_eq!(
            Failure::from_response(409, b"{}", None).code,
            exit::CONFLICT
        );
        assert_eq!(
            Failure::from_response(502, b"<html>", None).code,
            exit::OTHER
        );
    }

    #[test]
    fn the_report_is_one_line_and_the_request_id() {
        let failure = problem(json!({
            "code": "conflict", "status": 409, "title": "Conflict", "detail": "two\nlines"
        }));
        let mut stderr = Vec::new();
        assert_eq!(failure.report(&mut stderr), exit::CONFLICT);
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "quoro: two lines (conflict)\nrequest id: req_1\n"
        );
    }
}

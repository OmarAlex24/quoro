//! `quoro logs [-n N] [--since T] [-f] [--build [BUILD]] [--deployment DEPLOYMENT]`.
//!
//! Three logs, all read from the node that holds them (ADR 0014), so an
//! unreachable node exits 7:
//!
//! - the app's output (the default): `getAppLogs` with `-n` as `tail` (200
//!   when not given) and `--since` as `since`; `-f` follows `streamAppLogs`
//!   (replaying `-n` lines first) until Ctrl-C, which exits 0.
//! - a build's log (`--build`, the app's newest build without an id):
//!   `getBuildLogs` page by page, the last `-n` lines kept; `-f` follows
//!   `streamBuildLogs` until the build is terminal, and prints each status
//!   change on stderr.
//! - a deployment's progress lines (`--deployment`): `getDeploymentLogs`.
//!
//! `--since` takes an RFC 3339 time or a duration ago (`30s`, `10m`, `2h`,
//! `1d`), sent in UTC. It reads the app's output only and does not combine
//! with `-f`: the stream has no `since`, and replaying a window before live
//! lines could drop or repeat lines at the seam.
//!
//! With `--json` a read prints the API's answer (its `items` cut to the last
//! `-n` lines where the client cut them), and a follow prints one JSON object
//! per line: each log line as the stream carries it and, for a build, the
//! `Build` each time its status changes.
//!
//! The SSE reader ([`frames`]) and the build follow ([`follow_build`]) are
//! shared with `quoro up` (`deploy.rs`).

use crate::Environment;
use crate::cli::{Global, LogsArgs};
use crate::client::{Answer, Call, now_unix, rfc3339, unix_seconds};
use crate::error::{Failure, exit};
use crate::operations::{
    GET_APP_LOGS, GET_BUILD, GET_BUILD_LOGS, GET_DEPLOYMENT_LOGS, LIST_APP_BUILDS, STREAM_APP_LOGS,
    STREAM_BUILD_LOGS,
};
use crate::scope::{Scope, is_id, text};
use reqwest::Response;
use reqwest::header::HeaderValue;
use serde_json::{Value, json};
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::time::Duration;

/// How often a build's status is read while its log cannot be streamed.
const POLL: Duration = Duration::from_secs(2);
/// The page size of a build log read (the contract's maximum).
const BUILD_LOG_PAGE: u32 = 1000;

pub async fn run(
    global: &Global,
    args: LogsArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let since = args.since.as_deref().map(since).transpose()?;
    if let Some(Some(build)) = &args.build {
        check_id("bld_", build, "build")?;
    }
    if let Some(deployment) = &args.deployment {
        check_id("dep_", deployment, "deployment")?;
    }
    let mut scope = Scope::open(global, environment, "logs", false)?;
    if let Some(deployment) = &args.deployment {
        return deployment_logs(&mut scope, deployment, args.lines, global.json, stdout).await;
    }
    if let Some(build) = args.build {
        let build = match build {
            Some(id) => id,
            None => newest_build(&mut scope, global).await?,
        };
        if args.follow {
            let follow = Follow {
                tail: args.lines,
                wait_without_log: false,
                status: Status::default(),
                json: global.json,
            };
            return match follow_build(&mut scope, &build, follow, stdout, stderr).await? {
                Followed::Done(_) | Followed::Interrupted => Ok(()),
            };
        }
        return build_logs(&mut scope, &build, args.lines, global.json, stdout).await;
    }
    let app = scope.required_app(global).await?;
    let id = text(&app, "id");
    if args.follow {
        return follow_app(&mut scope, &id, args.lines, global.json, stdout).await;
    }
    let mut path = format!(
        "{}?tail={}",
        GET_APP_LOGS.path.replace("{app_id}", &id),
        args.lines.unwrap_or(200)
    );
    if let Some(since) = &since {
        path.push_str(&format!("&since={since}"));
    }
    let answer = scope.answer(&path).await?;
    if global.json {
        return print_json_answer(answer, stdout);
    }
    let body = answer.json()?;
    let items = body["items"].as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        let name = text(&app, "name");
        return crate::api::write(
            stdout,
            format!("No log lines from the app {name}; its instances may not run yet.\n")
                .as_bytes(),
        );
    }
    let mut out = String::new();
    for entry in &items {
        out.push_str(&app_line(entry));
    }
    crate::api::write(stdout, out.as_bytes())
}

/// A deployment's progress lines, the last `lines` of them when given.
async fn deployment_logs(
    scope: &mut Scope,
    id: &str,
    lines: Option<u32>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let answer = scope
        .answer(&GET_DEPLOYMENT_LOGS.path.replace("{deployment_id}", id))
        .await?;
    if json && lines.is_none() {
        return print_json_answer(answer, stdout);
    }
    let body = answer.json()?;
    let items = last(body["items"].as_array().cloned().unwrap_or_default(), lines);
    if json {
        return crate::api::write(
            stdout,
            format!("{}\n", json!({ "items": items })).as_bytes(),
        );
    }
    if items.is_empty() {
        return crate::api::write(
            stdout,
            format!("No progress lines for the deployment {id} yet.\n").as_bytes(),
        );
    }
    let mut out = String::new();
    for entry in &items {
        out.push_str(&format!(
            "{} {:<7}{}\n",
            text(entry, "time"),
            text(entry, "phase"),
            text(entry, "message")
        ));
    }
    crate::api::write(stdout, out.as_bytes())
}

/// A build's whole log, page by page, the last `lines` of it when given.
async fn build_logs(
    scope: &mut Scope,
    id: &str,
    lines: Option<u32>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let base = GET_BUILD_LOGS.path.replace("{build_id}", id);
    let mut items = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut path = format!("{base}?limit={BUILD_LOG_PAGE}");
        if let Some(cursor) = &cursor {
            path.push_str(&format!("&cursor={}", crate::scope::query_value(cursor)));
        }
        let page = scope.get(&path).await?;
        let got = page["items"].as_array().cloned().unwrap_or_default();
        let empty = got.is_empty();
        items.extend(got);
        match page["next_cursor"].as_str() {
            // On a running build the last page grows; an empty page is the end
            // for now.
            Some(next) if !next.is_empty() && !empty => cursor = Some(next.to_string()),
            _ => break,
        }
    }
    let items = last(items, lines);
    if json {
        let body = json!({ "items": items, "next_cursor": null });
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    if items.is_empty() {
        return crate::api::write(
            stdout,
            format!("No log lines for the build {id} yet.\n").as_bytes(),
        );
    }
    let mut out = String::new();
    for line in &items {
        out.push_str(&text(line, "line"));
        out.push('\n');
    }
    crate::api::write(stdout, out.as_bytes())
}

/// The app's newest build, for `--build` without an id.
async fn newest_build(scope: &mut Scope, global: &Global) -> Result<String, Failure> {
    let app = scope.required_app(global).await?;
    let id = text(&app, "id");
    let page = scope
        .get(&format!(
            "{}?limit=1",
            LIST_APP_BUILDS.path.replace("{app_id}", &id)
        ))
        .await?;
    match page["items"].get(0).and_then(|build| build["id"].as_str()) {
        Some(build) => Ok(build.to_string()),
        None => Err(Failure::new(
            exit::NOT_FOUND,
            format!(
                "the app {} has no builds; start one with quoro up",
                text(&app, "name")
            ),
        )),
    }
}

/// Follows the app's live output until Ctrl-C (exit 0), an `error` event
/// (its problem's exit code) or the primary closing the stream.
async fn follow_app(
    scope: &mut Scope,
    id: &str,
    lines: Option<u32>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut path = STREAM_APP_LOGS.path.replace("{app_id}", id);
    if let Some(lines) = lines {
        path.push_str(&format!("?tail={lines}"));
    }
    let url = scope.url().to_string();
    let response = scope.session.send(&Call::get(path)).await?;
    if response.status().as_u16() != 200 {
        return Err(Answer::read(response, &url).await?.failure());
    }
    let mut interrupt = interrupt();
    let mut stream = Stream::new(response, url);
    loop {
        let frame = match stream.next(&mut interrupt).await {
            Next::Frame(frame) => frame,
            Next::Interrupted => return Ok(()),
            Next::Closed(failure) => return Err(failure),
        };
        match frame.event.as_str() {
            "log" => {
                let out = if json {
                    format!("{}\n", frame.data)
                } else {
                    let entry: Value = serde_json::from_str(&frame.data).unwrap_or(Value::Null);
                    app_line(&entry)
                };
                crate::api::write(stdout, out.as_bytes())?;
            }
            "error" => return Err(stream.problem(&frame.data)),
            "end" => return Ok(()),
            _ => {}
        }
    }
}

/// How a build follow ended.
pub enum Followed {
    /// The build is terminal: the `Build` as last read.
    Done(Value),
    /// Ctrl-C; the build goes on.
    Interrupted,
}

/// How to follow a build.
pub struct Follow {
    /// Start with the last `tail` lines instead of the whole log.
    pub tail: Option<u32>,
    /// `quoro up`: a log that cannot be streamed (the node unreachable, the
    /// stream cut short, a rate limit) does not end the follow; the build's
    /// status is read every two seconds and the stream is opened again,
    /// resuming after the last line seen. Without it (`quoro logs -f
    /// --build`), that is the command's failure.
    pub wait_without_log: bool,
    /// The status already shown.
    pub status: Status,
    /// One JSON object per line.
    pub json: bool,
}

/// Follows a build's log and status until the build is terminal.
pub async fn follow_build(
    scope: &mut Scope,
    id: &str,
    follow: Follow,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Followed, Failure> {
    let Follow {
        tail,
        wait_without_log,
        mut status,
        json,
    } = follow;
    let build_path = GET_BUILD.path.replace("{build_id}", id);
    let stream_path = STREAM_BUILD_LOGS.path.replace("{build_id}", id);
    let url = scope.url().to_string();
    let mut interrupt = interrupt();
    let mut last_id: Option<String> = None;
    let mut warned = false;
    loop {
        let mut call = Call::get(match (&last_id, tail) {
            (None, Some(tail)) => format!("{stream_path}?tail={tail}"),
            _ => stream_path.clone(),
        });
        if let Some(last) = &last_id
            && let Ok(value) = HeaderValue::from_str(last)
        {
            call.headers.insert("last-event-id", value);
        }
        let response = scope.session.send(&call).await?;
        let lost = if response.status().as_u16() == 200 {
            let mut stream = Stream::new(response, url.clone());
            match read_build(
                &mut stream,
                &mut interrupt,
                &mut status,
                &mut last_id,
                json,
                stdout,
                stderr,
            )
            .await?
            {
                Read::End => {
                    let build = scope.get(&build_path).await?;
                    status.show(&build, json, stdout, stderr)?;
                    return Ok(Followed::Done(build));
                }
                Read::Interrupted => return Ok(Followed::Interrupted),
                Read::Lost(failure) => failure,
            }
        } else {
            let answer = Answer::read(response, &url).await?;
            let failure = answer.failure();
            if answer.status != 429 && failure.code != exit::UNREACHABLE {
                return Err(failure);
            }
            failure
        };
        if !wait_without_log {
            return Err(lost);
        }
        if !warned {
            warned = true;
            let _ = writeln!(
                stderr,
                "quoro: the build log cannot be streamed now ({}); following the build's status",
                lost.message
            );
        }
        let answer = scope.answer(&build_path).await?;
        if answer.status != 429 {
            let build = answer.json()?;
            status.show(&build, json, stdout, stderr)?;
            if terminal(&text(&build, "status")) {
                return Ok(Followed::Done(build));
            }
        }
        tokio::select! {
            () = tokio::time::sleep(POLL) => {}
            () = &mut interrupt => return Ok(Followed::Interrupted),
        }
    }
}

/// How one build stream ended.
enum Read {
    End,
    Interrupted,
    Lost(Failure),
}

async fn read_build(
    stream: &mut Stream,
    interrupt: &mut Interrupt,
    status: &mut Status,
    last_id: &mut Option<String>,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Read, Failure> {
    loop {
        let frame = match stream.next(interrupt).await {
            Next::Frame(frame) => frame,
            Next::Interrupted => return Ok(Read::Interrupted),
            Next::Closed(failure) => return Ok(Read::Lost(failure)),
        };
        match frame.event.as_str() {
            "log" => {
                if frame.id.is_some() {
                    last_id.clone_from(&frame.id);
                }
                let out = if json {
                    format!("{}\n", frame.data)
                } else {
                    let line: Value = serde_json::from_str(&frame.data).unwrap_or(Value::Null);
                    format!("{}\n", text(&line, "line"))
                };
                crate::api::write(stdout, out.as_bytes())?;
            }
            "status" => {
                let build: Value = serde_json::from_str(&frame.data).unwrap_or(Value::Null);
                status.show(&build, json, stdout, stderr)?;
            }
            "error" => return Ok(Read::Lost(stream.problem(&frame.data))),
            "end" => return Ok(Read::End),
            _ => {}
        }
    }
}

/// The build status last shown, so each change is printed once.
#[derive(Default)]
pub struct Status(String);

impl Status {
    /// Starts from a status already printed (the answer of `startBuild`).
    pub fn seen(build: &Value) -> Self {
        Self(text(build, "status"))
    }

    fn show(
        &mut self,
        build: &Value,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<(), Failure> {
        let status = text(build, "status");
        if build.is_null() || status == self.0 {
            return Ok(());
        }
        self.0.clone_from(&status);
        if json {
            return crate::api::write(stdout, format!("{build}\n").as_bytes());
        }
        let _ = writeln!(stderr, "quoro: build {} is {status}", text(build, "id"));
        Ok(())
    }
}

/// Whether a build status is terminal.
pub fn terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "cancelled")
}

/// `2026-11-06T10:00:00.000Z vm_... line`.
fn app_line(entry: &Value) -> String {
    format!(
        "{} {} {}\n",
        text(entry, "time"),
        text(entry, "vm_id"),
        text(entry, "line")
    )
}

/// The last `lines` items, or all of them.
fn last(mut items: Vec<Value>, lines: Option<u32>) -> Vec<Value> {
    if let Some(lines) = lines.and_then(|lines| usize::try_from(lines).ok())
        && items.len() > lines
    {
        items.drain(..items.len() - lines);
    }
    items
}

/// An answer printed unchanged with `--json`, or its failure.
fn print_json_answer(answer: Answer, stdout: &mut dyn Write) -> Result<(), Failure> {
    if !answer.is_success() {
        crate::api::write(stdout, &answer.body)?;
        return Err(answer.failure());
    }
    crate::api::write(stdout, &answer.body)?;
    if !answer.body.ends_with(b"\n") {
        crate::api::write(stdout, b"\n")?;
    }
    Ok(())
}

fn check_id(prefix: &str, id: &str, what: &str) -> Result<(), Failure> {
    if is_id(prefix, id) {
        return Ok(());
    }
    Err(Failure::usage(format!(
        "'{id}' is not a {what} id ({prefix}...); quoro {} shows them",
        if what == "build" {
            "build list"
        } else {
            "status"
        }
    )))
}

/// `--since`: an RFC 3339 time, or a duration ago such as `10m`, as UTC.
pub(crate) fn since(text: &str) -> Result<String, Failure> {
    let text = text.trim();
    let unit = match text.chars().last() {
        Some('s') => Some(1),
        Some('m') => Some(60),
        Some('h') => Some(3600),
        Some('d') => Some(86_400),
        _ => None,
    };
    if let Some(unit) = unit
        && let Ok(count) = text[..text.len() - 1].parse::<i64>()
        && count >= 0
    {
        return Ok(rfc3339(now_unix() - count.saturating_mul(unit)));
    }
    unix_seconds(text).map(rfc3339).ok_or_else(|| {
        Failure::usage(format!(
            "--since takes a duration such as 30s, 10m, 2h or 1d, or an RFC 3339 time such as \
             2026-11-06T10:00:00Z; not '{text}'"
        ))
    })
}

/// Ctrl-C, as a future that completes once; never, when signals cannot be
/// read.
pub type Interrupt = Pin<Box<dyn Future<Output = ()>>>;

pub fn interrupt() -> Interrupt {
    Box::pin(async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    })
}

/// One server-sent event.
#[derive(Debug, PartialEq, Eq)]
pub struct Frame {
    pub id: Option<String>,
    /// `message` when the frame names none.
    pub event: String,
    /// The `data:` lines, joined with newlines.
    pub data: String,
}

/// The complete frames at the start of `buffer`, which keeps only the
/// incomplete tail. Comments (`: keep-alive`) are skipped, and so are frames
/// with no field.
pub fn frames(buffer: &mut Vec<u8>) -> Vec<Frame> {
    let mut found = Vec::new();
    while let Some(end) = buffer.windows(2).position(|pair| pair == b"\n\n") {
        let raw: Vec<u8> = buffer.drain(..end + 2).collect();
        let raw = String::from_utf8_lossy(&raw);
        let mut frame = Frame {
            id: None,
            event: String::new(),
            data: String::new(),
        };
        let mut data: Vec<&str> = Vec::new();
        for line in raw.lines() {
            if line.is_empty() || line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "id" => frame.id = Some(value.to_string()),
                "event" => frame.event = value.to_string(),
                "data" => data.push(value),
                _ => {}
            }
        }
        if frame.id.is_none() && frame.event.is_empty() && data.is_empty() {
            continue;
        }
        if frame.event.is_empty() {
            frame.event = "message".to_string();
        }
        frame.data = data.join("\n");
        found.push(frame);
    }
    found
}

/// What reading a stream gave.
enum Next {
    Frame(Frame),
    Interrupted,
    /// The stream ended or broke without an `end` event.
    Closed(Failure),
}

/// An open event stream, read frame by frame.
struct Stream {
    response: Response,
    url: String,
    request_id: Option<String>,
    buffer: Vec<u8>,
    ready: std::collections::VecDeque<Frame>,
}

impl Stream {
    fn new(response: Response, url: String) -> Self {
        let request_id = crate::client::request_id(&response);
        Self {
            response,
            url,
            request_id,
            buffer: Vec::new(),
            ready: std::collections::VecDeque::new(),
        }
    }

    async fn next(&mut self, interrupt: &mut Interrupt) -> Next {
        loop {
            if let Some(frame) = self.ready.pop_front() {
                return Next::Frame(frame);
            }
            let chunk = tokio::select! {
                chunk = self.response.chunk() => chunk,
                () = &mut *interrupt => return Next::Interrupted,
            };
            match chunk {
                Ok(Some(chunk)) => {
                    // SSE lines may end with CRLF; JSON data never holds a raw CR.
                    self.buffer
                        .extend(chunk.iter().copied().filter(|&b| b != b'\r'));
                    self.ready.extend(frames(&mut self.buffer));
                }
                Ok(None) => {
                    return Next::Closed(Failure {
                        request_id: self.request_id.clone(),
                        ..Failure::new(
                            exit::UNREACHABLE,
                            "the primary closed the log stream before it ended",
                        )
                    });
                }
                Err(e) => {
                    return Next::Closed(Failure {
                        request_id: self.request_id.clone(),
                        ..crate::client::unreachable(&self.url, &e)
                    });
                }
            }
        }
    }

    /// The failure an `error` event's `Problem` stands for (a node problem).
    fn problem(&self, data: &str) -> Failure {
        Failure::from_response(504, data.as_bytes(), self.request_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_read_whole_and_comments_skipped() {
        let mut buffer =
            b": keep-alive\n\nid: 3\nevent: log\ndata: {\"line\":\"a\"}\n\nevent: e".to_vec();
        let found = frames(&mut buffer);
        assert_eq!(
            found,
            vec![Frame {
                id: Some("3".to_string()),
                event: "log".to_string(),
                data: "{\"line\":\"a\"}".to_string(),
            }]
        );
        assert_eq!(buffer, b"event: e");
        buffer.extend_from_slice(b"nd\ndata: {}\n\ndata: x\ndata: y\n\n");
        let found = frames(&mut buffer);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].event, "end");
        assert_eq!(found[1].event, "message");
        assert_eq!(found[1].data, "x\ny");
        assert!(buffer.is_empty());
    }

    #[test]
    fn since_takes_a_duration_or_a_time() {
        let now = now_unix();
        let ten_minutes = unix_seconds(&since("10m").unwrap()).unwrap();
        assert!((now - 600 - ten_minutes).abs() <= 2);
        assert_eq!(
            since("2026-11-06T12:00:00+02:00").unwrap(),
            "2026-11-06T10:00:00Z"
        );
        for bad in ["", "m", "10x", "-5m", "yesterday"] {
            assert_eq!(since(bad).unwrap_err().code, exit::USAGE, "{bad}");
        }
    }

    #[test]
    fn last_keeps_the_newest_lines() {
        let items = vec![json!(1), json!(2), json!(3)];
        assert_eq!(last(items.clone(), Some(2)), vec![json!(2), json!(3)]);
        assert_eq!(last(items.clone(), None), items);
        assert_eq!(last(items.clone(), Some(5)), items);
    }
}

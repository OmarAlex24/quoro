//! `quoro mcp stdio` (ADR 0054): MCP over stdio for a local MCP client,
//! relayed to a Quoro primary's `/mcp` endpoint.
//!
//! The process is a relay and nothing else (ADR 0031, permission-model.md
//! section 10). Every JSON-RPC message the client writes on stdin is posted
//! unchanged to the primary's `/mcp`; every message the primary answers is
//! written unchanged on stdout. The tool list, the argument checks, the
//! scope and grant checks, approvals and the audit record all happen on the
//! primary, in the same service layer the HTTP API and `/mcp` use; this
//! process knows no tool and builds no record.
//!
//! Authentication: the operator gives the agent key (`akey_...`) as a file
//! readable by its owner only (`--key-file`), never on the command line,
//! where any local user could read it in the process list. The key
//! authorizes nothing by itself: the relay exchanges it at
//! `POST /api/v1/auth/agent-token` for a five-minute access token (`atk_`)
//! and mints the next one before it expires, or once more when the primary
//! answers 401.
//!
//! Transport: TLS pinned to the primary's certificate fingerprint exactly as
//! `quoro join` pins it (node-agent-protocol.md section 2.2). No root store
//! is loaded and nothing is sent before the pin matched.
//!
//! stdout carries MCP messages only, one JSON object per line; diagnostics go
//! to stderr.

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use quoro_node_agent::tls;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;

/// The endpoint that exchanges an agent key for an access token.
const AGENT_TOKEN_PATH: &str = "/api/v1/auth/agent-token";
/// The primary's MCP endpoint (`quoro_mcp::MCP_PATH`).
const MCP_PATH: &str = quoro_mcp::MCP_PATH;
/// The prefix of an agent key; anything else in the key file is refused.
const AGENT_KEY_PREFIX: &str = "akey_";
/// The prefix of the access token the primary mints.
const ACCESS_TOKEN_PREFIX: &str = "atk_";
/// An access token lives five minutes (openapi.yaml, `AccessTokenResponse`);
/// this is assumed when `expires_at` cannot be read.
const TOKEN_LIFETIME: Duration = Duration::from_secs(300);
/// A token is replaced this long before it expires, so a slow call never
/// reaches the primary with a token that has just lapsed.
const REFRESH_MARGIN: Duration = Duration::from_secs(60);
/// One request to the primary, connection and TLS handshake included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// The default `Quoro-Justification` of a mint, which the contract requires
/// because the caller is an agent (openapi.yaml, `createAgentToken`).
pub const DEFAULT_JUSTIFICATION: &str =
    "quoro mcp stdio: access token for a local MCP client session";

/// JSON-RPC error codes this relay answers with itself.
const PARSE_ERROR: i64 = -32700;
const INTERNAL_ERROR: i64 = -32603;

/// What `quoro mcp stdio` was started with.
pub struct Options {
    /// `https://host[:port]` of the primary.
    pub url: String,
    /// `sha256:<hex>` of the primary's certificate.
    pub fingerprint: String,
    /// The file that holds the agent key.
    pub key_file: PathBuf,
    /// The reason recorded with every mint.
    pub justification: String,
}

/// Runs the relay until stdin closes. Exits 1, before reading stdin, when the
/// key file, the URL, the pin or the first mint fails.
pub async fn run(options: Options) -> ExitCode {
    match start(options).await {
        Ok(relay) => {
            relay.serve().await;
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("quoro mcp stdio: {message}");
            ExitCode::from(1)
        }
    }
}

/// Checks everything that can be checked before the session and mints the
/// first token, so a bad key, a wrong pin or an unreachable primary fails at
/// start rather than on the client's first request.
async fn start(options: Options) -> Result<Relay, String> {
    let key = read_agent_key(&options.key_file)?;
    let primary = Primary::new(&options.url, &options.fingerprint)?;
    let justification = options.justification.trim().to_string();
    if justification.is_empty() {
        return Err("--justification must not be empty".to_string());
    }
    let mut tokens = Tokens {
        key,
        justification,
        current: None,
    };
    tokens.mint(&primary).await?;
    tracing::info!(primary = %primary.url, "access token minted; relaying MCP between stdio and the primary");
    Ok(Relay {
        primary,
        tokens: Mutex::new(tokens),
        protocol_version: Mutex::new(None),
    })
}

/// Reads the agent key from `path`.
///
/// The file must be a regular file that only its owner can read or write
/// (mode `0600` or stricter), the way SSH treats a private key, and must hold
/// one `akey_...` value. Errors name the path, never the content.
pub fn read_agent_key(path: &Path) -> Result<String, String> {
    let shown = path.display();
    let metadata = std::fs::metadata(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => format!("the agent key file {shown} does not exist"),
        _ => format!("cannot read the agent key file {shown}: {e}"),
    })?;
    if !metadata.is_file() {
        return Err(format!("the agent key file {shown} is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(format!(
                "the agent key file {shown} is accessible by other users (mode {mode:o}); \
                 restrict it with 'chmod 600 {shown}'"
            ));
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read the agent key file {shown}: {e}"))?;
    let key = text.trim();
    let well_formed = key.len() > AGENT_KEY_PREFIX.len()
        && key.starts_with(AGENT_KEY_PREFIX)
        && key.bytes().all(|b| b.is_ascii_graphic());
    if !well_formed {
        return Err(format!(
            "the agent key file {shown} does not hold an agent key (one line starting with {AGENT_KEY_PREFIX})"
        ));
    }
    Ok(key.to_string())
}

/// The primary: where it is and the certificate it must present.
struct Primary {
    /// The base URL, without a trailing slash.
    url: String,
    fingerprint: String,
    host: String,
}

/// One HTTP response from the primary.
struct Reply {
    status: StatusCode,
    content_type: String,
    body: Bytes,
}

impl Primary {
    fn new(url: &str, fingerprint: &str) -> Result<Self, String> {
        let url = url.trim().trim_end_matches('/').to_string();
        let endpoint = tls::Endpoint::parse(&url).map_err(|e| e.to_string())?;
        let fingerprint = tls::normalize_fingerprint(fingerprint).map_err(|e| e.to_string())?;
        Ok(Self {
            url,
            fingerprint,
            host: host_header(&endpoint),
        })
    }

    /// POSTs `body` to `path` over a fresh pinned connection.
    async fn post(
        &self,
        path: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Result<Reply, String> {
        tokio::time::timeout(REQUEST_TIMEOUT, self.post_inner(path, headers, body))
            .await
            .map_err(|_| {
                format!(
                    "the primary did not answer within {}s",
                    REQUEST_TIMEOUT.as_secs()
                )
            })?
    }

    async fn post_inner(
        &self,
        path: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Result<Reply, String> {
        let stream = tls::connect(&self.url, &self.fingerprint)
            .await
            .map_err(|e| format!("cannot reach the primary at {}: {e}", self.url))?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| format!("HTTP handshake with the primary failed: {e}"))?;
        // The connection future must be polled for the request to progress.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "connection to the primary ended");
            }
        });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("host", &self.host);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let request = request
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| format!("cannot build the request to the primary: {e}"))?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| format!("the request to the primary failed: {e}"))?;
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("cannot read the primary's answer: {e}"))?
            .to_bytes();
        Ok(Reply {
            status,
            content_type,
            body,
        })
    }
}

/// The `Host` header for an endpoint: the port only when it is not 443, and
/// an IPv6 literal in brackets.
fn host_header(endpoint: &tls::Endpoint) -> String {
    let host = if endpoint.host.contains(':') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    if endpoint.port == 443 {
        host
    } else {
        format!("{host}:{}", endpoint.port)
    }
}

/// The agent key and the access token minted from it.
struct Tokens {
    key: String,
    justification: String,
    /// The token and the instant from which it is replaced.
    current: Option<(String, Instant)>,
}

impl Tokens {
    /// The current token, minting a new one when there is none or it is due.
    async fn get(&mut self, primary: &Primary) -> Result<String, String> {
        match &self.current {
            Some((token, refresh_at)) if Instant::now() < *refresh_at => Ok(token.clone()),
            _ => self.mint(primary).await,
        }
    }

    /// Exchanges the agent key for a fresh access token.
    async fn mint(&mut self, primary: &Primary) -> Result<String, String> {
        self.current = None;
        let authorization = format!("Bearer {}", self.key);
        let reply = primary
            .post(
                AGENT_TOKEN_PATH,
                &[
                    ("authorization", &authorization),
                    ("quoro-justification", &self.justification),
                    ("accept", "application/json"),
                ],
                Vec::new(),
            )
            .await?;
        if reply.status != StatusCode::OK {
            return Err(format!(
                "the primary refused to mint an access token from the agent key: {}",
                describe_problem(reply.status, &reply.body)
            ));
        }
        let body: Value = serde_json::from_slice(&reply.body)
            .map_err(|_| "the agent-token answer is not JSON".to_string())?;
        let token = body["access_token"]
            .as_str()
            .filter(|t| t.starts_with(ACCESS_TOKEN_PREFIX))
            .ok_or_else(|| "the agent-token answer has no access_token".to_string())?
            .to_string();
        let lifetime = body["expires_at"]
            .as_str()
            .and_then(remaining_lifetime)
            .unwrap_or(TOKEN_LIFETIME);
        let refresh_at = Instant::now() + lifetime.saturating_sub(REFRESH_MARGIN);
        self.current = Some((token.clone(), refresh_at));
        tracing::debug!(
            refresh_in_s = lifetime.saturating_sub(REFRESH_MARGIN).as_secs(),
            "minted an access token"
        );
        Ok(token)
    }

    /// Forgets the current token, after the primary refused it.
    fn discard(&mut self) {
        self.current = None;
    }
}

/// How long until an RFC 3339 `expires_at`, or `None` when it cannot be read.
fn remaining_lifetime(expires_at: &str) -> Option<Duration> {
    let expires =
        time::OffsetDateTime::parse(expires_at, &time::format_description::well_known::Rfc3339)
            .ok()?;
    let remaining = expires - time::OffsetDateTime::now_utc();
    Some(Duration::try_from(remaining).unwrap_or(Duration::ZERO))
}

/// `status code: reason/detail` of a problem body, for a diagnostic. The body
/// never carries a secret: a refused credential is not echoed.
fn describe_problem(status: StatusCode, body: &[u8]) -> String {
    let mut text = format!("HTTP {}", status.as_u16());
    if let Ok(problem) = serde_json::from_slice::<Value>(body) {
        for key in ["code", "reason", "detail"] {
            if let Some(value) = problem.get(key).and_then(Value::as_str) {
                text.push_str(", ");
                text.push_str(key);
                text.push(' ');
                text.push_str(value);
            }
        }
    }
    text
}

/// The running relay, shared by the tasks that forward one message each.
struct Relay {
    primary: Primary,
    tokens: Mutex<Tokens>,
    /// The protocol version the primary chose in its answer to `initialize`;
    /// `None` until that answer arrives.
    protocol_version: Mutex<Option<String>>,
}

impl Relay {
    /// Reads messages from stdin until it closes, forwards each one in its
    /// own task, and writes every answer as one line on stdout.
    async fn serve(self) {
        let relay = Arc::new(self);
        let (out, mut lines_out) = mpsc::unbounded_channel::<String>();
        let writer = tokio::spawn(async move {
            let mut stdout = tokio::io::stdout();
            while let Some(line) = lines_out.recv().await {
                let written = async {
                    stdout.write_all(line.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await
                }
                .await;
                if let Err(e) = written {
                    tracing::error!(error = %e, "cannot write to stdout");
                    return;
                }
            }
        });

        let mut tasks = JoinSet::new();
        let mut stdin = BufReader::new(tokio::io::stdin()).lines();
        loop {
            match stdin.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let relay = Arc::clone(&relay);
                    let out = out.clone();
                    tasks.spawn(async move {
                        for answer in relay.forward(&line).await {
                            let _ = out.send(answer);
                        }
                    });
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!(error = %e, "cannot read stdin");
                    break;
                }
            }
        }
        // Answer everything already sent before exiting.
        while tasks.join_next().await.is_some() {}
        drop(out);
        let _ = writer.await;
    }

    /// Forwards one stdin line and returns the lines to write on stdout.
    async fn forward(&self, line: &str) -> Vec<String> {
        let message: Value = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(e) => {
                return vec![rpc_error(
                    &Value::Null,
                    PARSE_ERROR,
                    &format!("parse error: {e}"),
                )];
            }
        };
        let id = message.get("id").cloned();
        let is_initialize = message.get("method").and_then(Value::as_str) == Some("initialize");
        let body = message.to_string().into_bytes();

        let reply = match self.post_mcp(body, is_initialize).await {
            Ok(reply) => reply,
            Err(e) => {
                tracing::error!(error = %e, "cannot forward a message to the primary");
                return id.map_or_else(Vec::new, |id| vec![rpc_error(&id, INTERNAL_ERROR, &e)]);
            }
        };

        if !reply.status.is_success() {
            let detail = describe_problem(reply.status, &reply.body);
            tracing::warn!(%detail, "the primary refused a message");
            return id.map_or_else(Vec::new, |id| {
                vec![rpc_error(
                    &id,
                    INTERNAL_ERROR,
                    &format!("the Quoro primary refused the message: {detail}"),
                )]
            });
        }

        let messages = decode_messages(&reply.content_type, &reply.body);
        if is_initialize {
            self.remember_protocol_version(&messages).await;
        }
        if messages.is_empty()
            && let Some(id) = id
            && reply.status != StatusCode::ACCEPTED
        {
            return vec![rpc_error(
                &id,
                INTERNAL_ERROR,
                "the Quoro primary answered without a JSON-RPC message",
            )];
        }
        messages.iter().map(Value::to_string).collect()
    }

    /// POSTs one message to `/mcp`, minting a new token once when the primary
    /// answers 401 (the token lapsed or was refused).
    async fn post_mcp(&self, body: Vec<u8>, is_initialize: bool) -> Result<Reply, String> {
        let mut retried = false;
        loop {
            let token = self.tokens.lock().await.get(&self.primary).await?;
            let negotiated = self.protocol_version.lock().await.clone();
            let headers = mcp_headers(&token, negotiated.as_deref(), is_initialize);
            let headers: Vec<(&str, &str)> = headers
                .iter()
                .map(|(name, value)| (*name, value.as_str()))
                .collect();
            let reply = self.primary.post(MCP_PATH, &headers, body.clone()).await?;
            if reply.status == StatusCode::UNAUTHORIZED && !retried {
                retried = true;
                let mut tokens = self.tokens.lock().await;
                // Another task may already have replaced the refused token.
                if tokens.current.as_ref().is_some_and(|(t, _)| *t == token) {
                    tokens.discard();
                }
                continue;
            }
            return Ok(reply);
        }
    }

    /// Keeps the protocol version the primary chose in `initialize`, which
    /// every later request names in `MCP-Protocol-Version`.
    async fn remember_protocol_version(&self, messages: &[Value]) {
        if let Some(version) = negotiated_version(messages) {
            *self.protocol_version.lock().await = Some(version.to_string());
        }
    }
}

/// The headers of one POST to `/mcp`.
///
/// `initialize` carries no `MCP-Protocol-Version`: the version is still being
/// negotiated, it is named in the body, and a header that differs from the
/// body is refused. Every later message carries the version the primary
/// chose; before that answer there is none to send, and the primary then
/// assumes its default (MCP, "Protocol Version Header").
fn mcp_headers(
    token: &str,
    negotiated: Option<&str>,
    is_initialize: bool,
) -> Vec<(&'static str, String)> {
    let mut headers = vec![
        ("authorization", format!("Bearer {token}")),
        ("content-type", "application/json".to_string()),
        ("accept", "application/json, text/event-stream".to_string()),
    ];
    if let Some(version) = negotiated.filter(|_| !is_initialize) {
        headers.push(("mcp-protocol-version", version.to_string()));
    }
    headers
}

/// The `protocolVersion` of the first `initialize` result among `messages`.
fn negotiated_version(messages: &[Value]) -> Option<&str> {
    messages
        .iter()
        .find_map(|m| m["result"]["protocolVersion"].as_str())
}

/// The JSON-RPC messages in a successful `/mcp` answer: a JSON body (one
/// message or a batch) or a `text/event-stream` of `data:` events. An empty
/// body (202 for a notification) holds none.
fn decode_messages(content_type: &str, body: &[u8]) -> Vec<Value> {
    if content_type.contains("text/event-stream") {
        return sse_messages(&String::from_utf8_lossy(body));
    }
    if body.iter().all(u8::is_ascii_whitespace) {
        return Vec::new();
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Array(batch)) => batch,
        Ok(message) => vec![message],
        Err(e) => {
            tracing::warn!(error = %e, "the primary answered a body that is not JSON");
            Vec::new()
        }
    }
}

/// The JSON payloads of the events in a server-sent event stream; events
/// whose data is not JSON (keep-alives, priming events) are skipped.
fn sse_messages(text: &str) -> Vec<Value> {
    let mut messages = Vec::new();
    let mut data: Vec<&str> = Vec::new();
    for line in text.lines().chain(std::iter::once("")) {
        if line.is_empty() {
            if !data.is_empty() {
                if let Ok(message) = serde_json::from_str::<Value>(&data.join("\n")) {
                    messages.push(message);
                }
                data.clear();
            }
        } else if let Some(rest) = line.strip_prefix("data:") {
            data.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    messages
}

/// A JSON-RPC error response, as one line.
fn rpc_error(id: &Value, code: i64, message: &str) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("quoro-mcp-key-{}-{unique}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn key_file(dir: &TempDir, content: &str, mode: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.0.join(format!("key-{mode:o}"));
        std::fs::write(&path, content).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[test]
    fn a_private_key_file_is_read_and_trimmed() {
        let dir = TempDir::new();
        let path = key_file(&dir, "akey_0123456789abcdef\n", 0o600);
        assert_eq!(read_agent_key(&path).unwrap(), "akey_0123456789abcdef");
        let path = key_file(&dir, "akey_0123456789abcdef", 0o400);
        assert_eq!(read_agent_key(&path).unwrap(), "akey_0123456789abcdef");
    }

    #[test]
    fn a_missing_shared_or_malformed_key_file_is_refused() {
        let dir = TempDir::new();
        let missing = read_agent_key(&dir.0.join("absent")).unwrap_err();
        assert!(missing.contains("does not exist"), "{missing}");

        let shared = key_file(&dir, "akey_0123456789abcdef\n", 0o644);
        let shared = read_agent_key(&shared).unwrap_err();
        assert!(shared.contains("chmod 600"), "{shared}");

        let wrong = key_file(&dir, "atk_0123456789abcdef\n", 0o600);
        let wrong = read_agent_key(&wrong).unwrap_err();
        assert!(wrong.contains("does not hold an agent key"), "{wrong}");
        assert!(
            !wrong.contains("atk_0123"),
            "the content is never echoed: {wrong}"
        );

        let directory = read_agent_key(&dir.0).unwrap_err();
        assert!(directory.contains("not a regular file"), "{directory}");
    }

    #[test]
    fn event_streams_and_json_bodies_decode_to_messages() {
        let stream = "id: 1\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n\
                      data:\n\n: keep-alive\n\ndata: {\"jsonrpc\":\"2.0\",\n\
                      data: \"id\":2,\"result\":{}}\n";
        let messages = decode_messages("text/event-stream", stream.as_bytes());
        assert_eq!(messages.len(), 2, "{messages:?}");
        assert_eq!(messages[1]["id"], 2);

        let single = decode_messages("application/json", br#"{"jsonrpc":"2.0","id":7}"#);
        assert_eq!(single, vec![json!({"jsonrpc": "2.0", "id": 7})]);
        assert!(decode_messages("application/json", b"").is_empty());
        assert!(decode_messages("", b"  ").is_empty());
    }

    #[test]
    fn expiry_is_read_from_rfc3339() {
        let remaining = remaining_lifetime("2999-01-01T00:05:00Z").unwrap();
        assert!(remaining > Duration::from_secs(300), "{remaining:?}");
        assert_eq!(
            remaining_lifetime("2000-01-01T00:00:00Z"),
            Some(Duration::ZERO)
        );
        assert_eq!(remaining_lifetime("soon"), None);
    }

    #[test]
    fn the_host_header_omits_443_and_brackets_ipv6() {
        let endpoint = |url: &str| tls::Endpoint::parse(url).unwrap();
        assert_eq!(
            host_header(&endpoint("https://quoro.example.com")),
            "quoro.example.com"
        );
        assert_eq!(
            host_header(&endpoint("https://192.0.2.1:8443")),
            "192.0.2.1:8443"
        );
        assert_eq!(host_header(&endpoint("https://[::1]:8443")), "[::1]:8443");
    }

    #[test]
    fn initialize_carries_no_version_and_later_messages_the_negotiated_one() {
        let version_of = |headers: &[(&str, String)]| {
            headers
                .iter()
                .find(|(name, _)| *name == "mcp-protocol-version")
                .map(|(_, value)| value.clone())
        };
        // The first initialize, and a repeated one, never send the header.
        assert_eq!(version_of(&mcp_headers("atk_x", None, true)), None);
        assert_eq!(
            version_of(&mcp_headers("atk_x", Some("2025-06-18"), true)),
            None
        );
        // Nothing negotiated yet: no header, so the primary uses its default.
        assert_eq!(version_of(&mcp_headers("atk_x", None, false)), None);

        let answer = [json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "protocolVersion": "2025-06-18", "capabilities": {} },
        })];
        let negotiated = negotiated_version(&answer);
        assert_eq!(negotiated, Some("2025-06-18"));
        let headers = mcp_headers("atk_x", negotiated, false);
        assert_eq!(version_of(&headers).as_deref(), Some("2025-06-18"));
        assert!(headers.contains(&("authorization", "Bearer atk_x".to_string())));

        assert_eq!(
            negotiated_version(&[json!({"jsonrpc": "2.0", "id": 1, "error": {}})]),
            None
        );
    }

    #[test]
    fn problems_are_described_without_their_bodies() {
        let body =
            br#"{"code":"forbidden","reason":"agent_disabled","detail":"the agent is disabled"}"#;
        assert_eq!(
            describe_problem(StatusCode::FORBIDDEN, body),
            "HTTP 403, code forbidden, reason agent_disabled, detail the agent is disabled"
        );
        assert_eq!(
            describe_problem(StatusCode::BAD_GATEWAY, b"<html>"),
            "HTTP 502"
        );
    }
}

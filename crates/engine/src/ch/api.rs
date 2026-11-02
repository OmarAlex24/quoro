// SPDX-License-Identifier: AGPL-3.0-only
//! A minimal HTTP/1.1 client for Cloud Hypervisor's JSON API over its UNIX
//! socket (engine-trait.md §5).
//!
//! Cloud Hypervisor exposes one HTTP/JSON endpoint per operation on the socket
//! given with `--api-socket`. The protocol is small enough that this module
//! speaks it directly on a [`tokio::net::UnixStream`]: one request per
//! connection, `Content-Length` framing, no chunked bodies, a header cap of
//! 64 KiB and a body cap of 8 MiB. The response parser is a pure function, so
//! framing is unit-tested on byte strings.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;

use crate::EngineError;

/// Timeout of one request/response exchange unless [`ApiClient::with_timeout`]
/// sets another.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest response head (status line and headers) accepted.
const MAX_HEADER_BYTES: usize = 64 * 1024;
/// Largest response body accepted.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
/// Size of one read from the socket.
const READ_CHUNK: usize = 8 * 1024;

/// What went wrong talking to a VMM.
#[derive(Debug)]
pub enum ApiError {
    /// The socket is missing or refuses connections: the VMM is gone or exiting.
    Unavailable(std::io::Error),
    /// No complete response within the timeout.
    Timeout,
    /// Cloud Hypervisor answered with a non-2xx status.
    Status { status: u16, body: String },
    /// Malformed response, or the connection closed mid-response.
    Protocol(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(err) => write!(f, "vmm api unavailable: {err}"),
            Self::Timeout => f.write_str("vmm api timed out"),
            Self::Status { status, body } if body.is_empty() => {
                write!(f, "vmm api returned HTTP {status}")
            }
            Self::Status { status, body } => write!(f, "vmm api returned HTTP {status}: {body}"),
            Self::Protocol(message) => write!(f, "vmm api protocol error: {message}"),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable(err) => Some(err),
            Self::Timeout | Self::Status { .. } | Self::Protocol(_) => None,
        }
    }
}

impl From<ApiError> for EngineError {
    fn from(err: ApiError) -> Self {
        match err {
            ApiError::Timeout => Self::Timeout,
            ApiError::Unavailable(cause) => {
                Self::Hypervisor(format!("cloud-hypervisor API: {cause}"))
            }
            ApiError::Status { status, body } if body.is_empty() => {
                Self::Hypervisor(format!("cloud-hypervisor API: HTTP {status}"))
            }
            ApiError::Status { status, body } => {
                Self::Hypervisor(format!("cloud-hypervisor API: HTTP {status}: {body}"))
            }
            ApiError::Protocol(message) => {
                Self::Hypervisor(format!("cloud-hypervisor API: {message}"))
            }
        }
    }
}

/// A client for one VMM's API socket. Cheap to build, not `Clone`; build a new
/// one wherever it is needed.
pub struct ApiClient {
    socket: PathBuf,
    timeout: Duration,
}

impl ApiClient {
    /// A client for `socket` with the default 5 s timeout.
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// The whole exchange (connect, write, read) is bounded by `timeout`.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The socket this client talks to.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// `GET /api/v1/vmm.ping`; the parsed JSON body.
    pub async fn ping(&self) -> Result<Value, ApiError> {
        self.get_json("/api/v1/vmm.ping").await
    }

    /// `PUT /api/v1/vm.create` with the VM configuration as the body.
    pub async fn create(&self, config: &Value) -> Result<(), ApiError> {
        let (status, body) = self.put_json("/api/v1/vm.create", Some(config)).await?;
        check_status(status, &body)
    }

    /// `PUT /api/v1/vm.boot`.
    pub async fn boot(&self) -> Result<(), ApiError> {
        let (status, body) = self.exchange("PUT", "/api/v1/vm.boot", None, false).await?;
        check_status(status, &body)
    }

    /// `PUT /api/v1/vm.power-button`.
    pub async fn power_button(&self) -> Result<(), ApiError> {
        let (status, body) = self
            .exchange("PUT", "/api/v1/vm.power-button", None, false)
            .await?;
        check_status(status, &body)
    }

    /// `PUT /api/v1/vmm.shutdown`.
    ///
    /// The VMM may exit before or while answering, so a connection that closes
    /// after the request was fully written counts as success.
    pub async fn shutdown_vmm(&self) -> Result<(), ApiError> {
        let (status, body) = self
            .exchange("PUT", "/api/v1/vmm.shutdown", None, true)
            .await?;
        check_status(status, &body)
    }

    /// `GET /api/v1/vm.info`; the parsed JSON body.
    pub async fn info(&self) -> Result<Value, ApiError> {
        self.get_json("/api/v1/vm.info").await
    }

    /// `GET /api/v1/vm.counters`; the parsed JSON body.
    pub async fn counters(&self) -> Result<Value, ApiError> {
        self.get_json("/api/v1/vm.counters").await
    }

    /// Sends one request and returns the status and body of a 2xx answer.
    async fn get_json(&self, path: &str) -> Result<Value, ApiError> {
        let (status, body) = self.exchange("GET", path, None, false).await?;
        check_status(status, &body)?;
        parse_json(&body)
    }

    /// `PUT` with an optional JSON body; returns status and body unchecked.
    async fn put_json(&self, path: &str, body: Option<&Value>) -> Result<(u16, Vec<u8>), ApiError> {
        self.exchange("PUT", path, body, false).await
    }

    /// Writes one request and reads one response, whole exchange under
    /// `self.timeout`.
    async fn exchange(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        eof_ok: bool,
    ) -> Result<(u16, Vec<u8>), ApiError> {
        let request = encode_request(method, path, body)?;
        let exchange = async move {
            let stream = match UnixStream::connect(&self.socket).await {
                Ok(stream) => stream,
                Err(err) => return Err(connect_error(err)),
            };
            let (mut reader, mut writer) = stream.into_split();
            if let Err(err) = writer.write_all(&request).await {
                return Err(write_error(err));
            }
            if let Err(err) = writer.flush().await {
                return Err(write_error(err));
            }
            read_response(&mut reader, eof_ok).await
        };
        match tokio::time::timeout(self.timeout, exchange).await {
            Ok(result) => result,
            Err(_) => Err(ApiError::Timeout),
        }
    }
}

/// A connect error of the kinds that mean "no VMM behind this socket".
fn connect_error(err: std::io::Error) -> ApiError {
    match err.kind() {
        std::io::ErrorKind::NotFound
        | std::io::ErrorKind::ConnectionRefused
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionAborted => ApiError::Unavailable(err),
        _ => ApiError::Protocol(format!("connect: {err}")),
    }
}

/// A write error; a peer that reset the connection is gone.
fn write_error(err: std::io::Error) -> ApiError {
    match err.kind() {
        std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::ConnectionAborted => ApiError::Unavailable(err),
        _ => ApiError::Protocol(format!("write: {err}")),
    }
}

/// Serialises one HTTP/1.1 request.
fn encode_request(method: &str, path: &str, body: Option<&Value>) -> Result<Vec<u8>, ApiError> {
    let mut head =
        format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n");
    let payload = match body {
        Some(value) => serde_json::to_vec(value)
            .map_err(|err| ApiError::Protocol(format!("cannot encode request body: {err}")))?,
        None => Vec::new(),
    };
    if body.is_some() {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            payload.len()
        ));
    } else if method == "PUT" {
        head.push_str("Content-Length: 0\r\n");
    }
    head.push_str("\r\n");
    let mut request = head.into_bytes();
    request.extend_from_slice(&payload);
    Ok(request)
}

/// One parsed response: status code and body bytes.
type Response = (u16, Vec<u8>);

/// Reads from `reader` until a complete response is framed.
async fn read_response<R>(reader: &mut R, eof_ok: bool) -> Result<Response, ApiError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    let mut chunk = [0u8; READ_CHUNK];
    loop {
        let read = match reader.read(&mut chunk).await {
            Ok(read) => read,
            Err(_) if eof_ok => return Ok((204, Vec::new())),
            Err(err) => return Err(ApiError::Protocol(format!("read: {err}"))),
        };
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(response) = parse_response(&buffer, false)? {
            return Ok(response);
        }
    }
    if let Some(response) = parse_response(&buffer, true)? {
        return Ok(response);
    }
    if eof_ok {
        return Ok((204, Vec::new()));
    }
    Err(ApiError::Protocol(
        "connection closed before a complete response".to_owned(),
    ))
}

/// Frames one HTTP/1.1 response out of `buffer`.
///
/// `Ok(None)` means the bytes so far are a valid prefix and more are needed;
/// `eof` says the peer closed the connection, so a body without
/// `Content-Length` is complete at the current end.
fn parse_response(buffer: &[u8], eof: bool) -> Result<Option<Response>, ApiError> {
    let Some(head_end) = find_head_end(buffer) else {
        if buffer.len() > MAX_HEADER_BYTES {
            return Err(ApiError::Protocol(format!(
                "response head exceeds {MAX_HEADER_BYTES} bytes"
            )));
        }
        return Ok(None);
    };
    if head_end > MAX_HEADER_BYTES {
        return Err(ApiError::Protocol(format!(
            "response head exceeds {MAX_HEADER_BYTES} bytes"
        )));
    }
    let head = std::str::from_utf8(&buffer[..head_end])
        .map_err(|_| ApiError::Protocol("response head is not UTF-8".to_owned()))?;
    let body_start = head_end + 4;
    let rest = &buffer[body_start..];

    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| ApiError::Protocol("empty response head".to_owned()))?;
    let status = parse_status_line(status_line)?;

    let mut content_length: Option<u64> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(ApiError::Protocol(format!("malformed header line: {line}")));
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "content-length" => {
                let length = value.parse::<u64>().map_err(|_| {
                    ApiError::Protocol(format!("malformed content-length: {value}"))
                })?;
                content_length = Some(length);
            }
            "transfer-encoding" if !value.eq_ignore_ascii_case("identity") => {
                return Err(ApiError::Protocol(format!(
                    "unsupported transfer-encoding: {value}"
                )));
            }
            _ => {}
        }
    }

    if let Some(length) = content_length {
        if length > MAX_BODY_BYTES as u64 {
            return Err(ApiError::Protocol(format!(
                "response body exceeds {MAX_BODY_BYTES} bytes"
            )));
        }
        let length = length as usize;
        if rest.len() < length {
            if eof {
                return Err(ApiError::Protocol(format!(
                    "truncated response body: {} of {length} bytes",
                    rest.len()
                )));
            }
            return Ok(None);
        }
        return Ok(Some((status, rest[..length].to_vec())));
    }

    if (100..200).contains(&status) || status == 204 || status == 304 {
        return Ok(Some((status, Vec::new())));
    }
    if rest.len() > MAX_BODY_BYTES {
        return Err(ApiError::Protocol(format!(
            "response body exceeds {MAX_BODY_BYTES} bytes"
        )));
    }
    if eof {
        return Ok(Some((status, rest.to_vec())));
    }
    Ok(None)
}

/// Offset of the empty line that ends the head, i.e. of `\r\n\r\n`.
fn find_head_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Parses `HTTP/1.x <code> [reason]`, tolerating an empty reason phrase.
fn parse_status_line(line: &str) -> Result<u16, ApiError> {
    let mut parts = line.split(' ');
    let version = parts
        .next()
        .ok_or_else(|| ApiError::Protocol("missing status line".to_owned()))?;
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(ApiError::Protocol(format!("malformed status line: {line}")));
    }
    let code = parts
        .next()
        .ok_or_else(|| ApiError::Protocol(format!("malformed status line: {line}")))?;
    let status = code
        .parse::<u16>()
        .map_err(|_| ApiError::Protocol(format!("malformed status line: {line}")))?;
    if !(100..600).contains(&status) {
        return Err(ApiError::Protocol(format!("malformed status line: {line}")));
    }
    Ok(status)
}

/// Returns `Ok(())` for a 2xx status, `Status` otherwise.
fn check_status(status: u16, body: &[u8]) -> Result<(), ApiError> {
    if (200..300).contains(&status) {
        return Ok(());
    }
    Err(ApiError::Status {
        status,
        body: String::from_utf8_lossy(body).trim().to_owned(),
    })
}

/// Parses a JSON body; an empty body is a protocol error, as Cloud Hypervisor
/// always sends one for the endpoints this module reads.
fn parse_json(body: &[u8]) -> Result<Value, ApiError> {
    if body.is_empty() {
        return Err(ApiError::Protocol("empty JSON response body".to_owned()));
    }
    serde_json::from_slice(body)
        .map_err(|err| ApiError::Protocol(format!("malformed JSON response: {err}")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::io::AsyncWriteExt as _;

    use super::{ApiClient, ApiError};
    use crate::EngineError;

    fn response(status: u16, headers: &str, body: &[u8]) -> Vec<u8> {
        let mut out = format!("HTTP/1.1 {status} \r\n").into_bytes();
        out.extend_from_slice(headers.as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn parses_204_without_body() {
        let bytes = response(204, "", b"");
        let parsed = super::parse_response(&bytes, true).expect("parse");
        assert_eq!(parsed, Some((204, Vec::new())));
    }

    #[test]
    fn parses_200_with_content_length() {
        let body = br#"{"state":"Running"}"#;
        let bytes = response(
            200,
            "Content-Type: application/json\r\nContent-Length: 19\r\n",
            body,
        );
        let parsed = super::parse_response(&bytes, true).expect("parse");
        assert_eq!(parsed, Some((200, body.to_vec())));
    }

    #[test]
    fn parses_500_with_body() {
        let bytes = response(500, "Content-Length: 6\r\n", b"oops!\n");
        let parsed = super::parse_response(&bytes, true).expect("parse");
        assert_eq!(parsed, Some((500, b"oops!\n".to_vec())));
    }

    #[test]
    fn header_names_are_case_insensitive() {
        let bytes = response(200, "cOnTeNt-LeNgTh: 2\r\n", b"{}");
        let parsed = super::parse_response(&bytes, true).expect("parse");
        assert_eq!(parsed, Some((200, b"{}".to_vec())));
    }

    #[test]
    fn incomplete_head_needs_more_bytes() {
        let bytes = b"HTTP/1.1 200 \r\nContent-Len";
        assert_eq!(super::parse_response(bytes, false).expect("parse"), None);
    }

    #[test]
    fn split_body_is_completed_by_the_next_read() {
        let full = response(200, "Content-Length: 5\r\n", b"hello");
        let split = 40;
        assert_eq!(
            super::parse_response(&full[..split], false).expect("parse"),
            None
        );
        assert_eq!(
            super::parse_response(&full, false).expect("parse"),
            Some((200, b"hello".to_vec()))
        );
    }

    #[test]
    fn truncated_body_at_eof_is_a_protocol_error() {
        let bytes = response(200, "Content-Length: 10\r\n", b"hello");
        let err = super::parse_response(&bytes, true).expect_err("truncated");
        assert!(matches!(err, ApiError::Protocol(_)));
    }

    #[test]
    fn body_without_content_length_reads_to_eof() {
        let bytes = response(200, "Connection: close\r\n", b"{\"a\":1}");
        assert_eq!(super::parse_response(&bytes, false).expect("parse"), None);
        assert_eq!(
            super::parse_response(&bytes, true).expect("parse"),
            Some((200, b"{\"a\":1}".to_vec()))
        );
    }

    #[test]
    fn oversized_head_is_rejected() {
        let mut bytes = b"HTTP/1.1 200 \r\nX-Pad: ".to_vec();
        bytes.extend(std::iter::repeat_n(b'a', super::MAX_HEADER_BYTES + 1));
        let err = super::parse_response(&bytes, false).expect_err("oversize");
        assert!(matches!(err, ApiError::Protocol(_)));
    }

    #[test]
    fn oversized_body_is_rejected() {
        let head = format!(
            "HTTP/1.1 200 \r\nContent-Length: {}\r\n\r\n",
            super::MAX_BODY_BYTES + 1
        );
        let err = super::parse_response(head.as_bytes(), false).expect_err("oversize");
        assert!(matches!(err, ApiError::Protocol(_)));
    }

    #[test]
    fn chunked_encoding_is_rejected() {
        let bytes = response(200, "Transfer-Encoding: chunked\r\n", b"0\r\n\r\n");
        let err = super::parse_response(&bytes, true).expect_err("chunked");
        assert!(matches!(err, ApiError::Protocol(_)));
    }

    #[test]
    fn malformed_status_line_is_rejected() {
        let bytes = b"HELLO 200\r\n\r\n";
        let err = super::parse_response(bytes, true).expect_err("status");
        assert!(matches!(err, ApiError::Protocol(_)));
    }

    #[test]
    fn malformed_content_length_is_rejected() {
        let bytes = response(200, "Content-Length: many\r\n", b"");
        let err = super::parse_response(&bytes, true).expect_err("length");
        assert!(matches!(err, ApiError::Protocol(_)));
    }

    #[test]
    fn encodes_get_without_body() {
        let request = super::encode_request("GET", "/api/v1/vm.info", None).expect("encode");
        assert_eq!(
            request,
            b"GET /api/v1/vm.info HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\r\n"
        );
    }

    #[test]
    fn encodes_put_without_body() {
        let request = super::encode_request("PUT", "/api/v1/vm.boot", None).expect("encode");
        assert_eq!(
            request,
            b"PUT /api/v1/vm.boot HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Length: 0\r\n\r\n"
        );
    }

    #[test]
    fn encodes_put_with_json_body() {
        let value = json!({"cpus": {"boot_vcpus": 1}});
        let request =
            super::encode_request("PUT", "/api/v1/vm.create", Some(&value)).expect("encode");
        let text = String::from_utf8(request).expect("utf8");
        let (head, body) = text.split_once("\r\n\r\n").expect("split");
        assert!(head.contains("Content-Type: application/json"));
        assert!(head.contains(&format!("Content-Length: {}", body.len())));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).expect("json"),
            value
        );
    }

    #[test]
    fn maps_api_errors_to_engine_errors() {
        assert!(matches!(
            EngineError::from(ApiError::Timeout),
            EngineError::Timeout
        ));
        let unavailable =
            ApiError::Unavailable(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        let mapped = EngineError::from(unavailable);
        assert!(
            matches!(&mapped, EngineError::Hypervisor(m) if m.starts_with("cloud-hypervisor API:"))
        );
        let status = ApiError::Status {
            status: 500,
            body: "boom".to_owned(),
        };
        let mapped = EngineError::from(status);
        assert!(matches!(&mapped, EngineError::Hypervisor(m) if m.contains("HTTP 500: boom")));
    }

    #[tokio::test]
    async fn missing_socket_is_unavailable() {
        let dir = std::env::temp_dir().join(format!("quoro-api-test-{}", std::process::id()));
        let socket = dir.join("absent.sock");
        let client = ApiClient::new(&socket);
        let err = client.ping().await.expect_err("no socket");
        assert!(matches!(err, ApiError::Unavailable(_)));
    }

    #[tokio::test]
    async fn eof_after_the_request_counts_as_success_for_shutdown() {
        let (mut client, mut server) = tokio::io::duplex(256);
        let peer = tokio::spawn(async move {
            server
                .write_all(b"HTTP/1.1 200 \r\nContent-Len")
                .await
                .expect("write");
            drop(server);
        });
        let result = super::read_response(&mut client, true).await;
        assert_eq!(result.expect("eof is fine"), (204, Vec::new()));
        peer.await.expect("peer");
    }

    #[tokio::test]
    async fn eof_before_a_complete_response_is_a_protocol_error() {
        let (mut client, mut server) = tokio::io::duplex(256);
        let peer = tokio::spawn(async move {
            server
                .write_all(b"HTTP/1.1 200 \r\nContent-Len")
                .await
                .expect("write");
            drop(server);
        });
        let err = super::read_response(&mut client, false)
            .await
            .expect_err("truncated");
        assert!(matches!(err, ApiError::Protocol(_)), "{err:?}");
        peer.await.expect("peer");
    }

    #[tokio::test]
    async fn reads_a_json_answer_over_a_socket() {
        let dir = std::env::temp_dir().join(format!("quoro-api-json-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("json.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            stream
                .write_all(b"HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length: 19\r\n\r\n{\"state\":\"Running\"}")
                .await
                .expect("write");
        });
        let client = ApiClient::new(&socket).with_timeout(std::time::Duration::from_secs(2));
        let info = client.info().await.expect("info");
        assert_eq!(info["state"], "Running");
        peer.await.expect("peer");
        let _ = std::fs::remove_file(&socket);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_non_2xx_answer_is_a_status_error() {
        let dir = std::env::temp_dir().join(format!("quoro-api-500-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("error.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            stream
                .write_all(b"HTTP/1.1 500 \r\nContent-Length: 13\r\n\r\ninvalid state")
                .await
                .expect("write");
        });
        let client = ApiClient::new(&socket).with_timeout(std::time::Duration::from_secs(2));
        let err = client.boot().await.expect_err("500");
        match err {
            ApiError::Status { status, body } => {
                assert_eq!(status, 500);
                assert_eq!(body, "invalid state");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        peer.await.expect("peer");
        let _ = std::fs::remove_file(&socket);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn garbage_answer_is_a_protocol_error() {
        let dir = std::env::temp_dir().join(format!("quoro-api-garbage-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("bad.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            stream
                .write_all(b"not http at all\r\n\r\n")
                .await
                .expect("write");
        });
        let client = ApiClient::new(&socket).with_timeout(std::time::Duration::from_secs(2));
        let err = client.info().await.expect_err("garbage");
        assert!(matches!(err, ApiError::Protocol(_)), "{err:?}");
        peer.await.expect("peer");
        let _ = std::fs::remove_file(&socket);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_json_body_is_a_protocol_error() {
        let dir = std::env::temp_dir().join(format!("quoro-api-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("empty.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            stream
                .write_all(b"HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("write");
        });
        let client = ApiClient::new(&socket).with_timeout(std::time::Duration::from_secs(2));
        let err = client.ping().await.expect_err("empty");
        assert!(matches!(err, ApiError::Protocol(_)), "{err:?}");
        peer.await.expect("peer");
        let _ = std::fs::remove_file(&socket);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

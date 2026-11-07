//! `quoro api METHOD PATH [--data @FILE|-]`: any API operation, unchanged.
//!
//! The way to reach everything the command tree leaves out (human-only
//! operations, streams, new endpoints). The response body goes to stdout as
//! the primary sent it, pretty-printed only for a terminal and without
//! `--json` (keys then come out sorted); an error is mapped to its exit code.
//! A request body is read from a file or stdin, never from the command line,
//! where any local user could read it in the process list.

use crate::cli::{ApiArgs, Global};
use crate::context::{self, Context, Trust};
use crate::error::{Failure, exit};
use crate::{Environment, tls};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, Method, Response};
use std::io::{Read as _, Write};
use std::time::Duration;

/// The TCP connection and the TLS handshake together, so an address that
/// never answers fails in bounded time; the node agent uses the same bound.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(
    global: &Global,
    args: ApiArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
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
    let mut headers = change_headers(&method, global)?;
    let context = context::resolve(global.context.as_deref(), environment)?;
    if let Some(token) = &context.token {
        let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
            .map_err(|_| Failure::other("the token is not a valid header value"))?;
        value.set_sensitive(true);
        headers.insert(AUTHORIZATION, value);
    }
    let mut request = client(&context)?
        .request(method, format!("{}{}", context.url, args.path))
        .headers(headers);
    if let Some(body) = body {
        request = request.header(CONTENT_TYPE, "application/json").body(body);
    }

    let response = request
        .send()
        .await
        .map_err(|e| unreachable(&context.url, &e))?;
    let request_id = response
        .headers()
        .get("quoro-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let status = response.status();
    if !status.is_success() {
        let body = read_all(response, &context.url, request_id.clone()).await?;
        if global.json {
            write(stdout, &body)?;
        }
        return Err(Failure::from_response(status.as_u16(), &body, request_id));
    }
    let pretty = environment.stdout_is_terminal && !global.json && is_json(&response);
    if pretty {
        let body = read_all(response, &context.url, request_id).await?;
        match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(value) => {
                let text = serde_json::to_string_pretty(&value).unwrap_or_default();
                write(stdout, format!("{text}\n").as_bytes())
            }
            Err(_) => write(stdout, &body),
        }
    } else {
        stream(response, &context.url, request_id, stdout).await
    }
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

/// `--reason` and `--incident` on a call that changes something
/// (`docs/contracts/README.md`, "Request headers").
fn change_headers(method: &Method, global: &Global) -> Result<HeaderMap, Failure> {
    let mut headers = HeaderMap::new();
    if *method == Method::GET {
        return Ok(headers);
    }
    let fields = [
        ("quoro-justification", "--reason", &global.reason),
        ("quoro-incident", "--incident", &global.incident),
    ];
    for (name, flag, value) in fields {
        let Some(value) = value else { continue };
        let value = HeaderValue::from_str(value.trim())
            .map_err(|_| Failure::usage(format!("{flag} must be printable ASCII on one line")))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn client(context: &Context) -> Result<Client, Failure> {
    let config = match context.trust {
        Trust::Pin(pin) => tls::pinned(pin),
        Trust::System => tls::system(),
    }
    .map_err(Failure::other)?;
    Client::builder()
        .use_preconfigured_tls(config)
        .connect_timeout(CONNECT_TIMEOUT)
        // The API never redirects; a redirect is not followed with the token.
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("quoro/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| Failure::other(format!("cannot build the HTTP client: {e}")))
}

fn is_json(response: &Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"))
}

/// No response, or one cut short. The innermost cause is the useful one: a
/// pin mismatch is several layers below "error sending request".
fn unreachable(url: &str, error: &reqwest::Error) -> Failure {
    let mut cause: &dyn std::error::Error = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    Failure::new(
        exit::UNREACHABLE,
        format!("cannot reach the primary at {url}: {cause}"),
    )
}

async fn read_all(
    response: Response,
    url: &str,
    request_id: Option<String>,
) -> Result<Vec<u8>, Failure> {
    match response.bytes().await {
        Ok(body) => Ok(body.to_vec()),
        Err(e) => Err(Failure {
            request_id,
            ..unreachable(url, &e)
        }),
    }
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
                    ..unreachable(url, &e)
                });
            }
        }
    }
}

fn write(stdout: &mut dyn Write, bytes: &[u8]) -> Result<(), Failure> {
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

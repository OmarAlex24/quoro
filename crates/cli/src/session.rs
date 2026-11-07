//! An interactive session over its WebSocket: the shell of `quoro ssh` and
//! the read-only serial console of `quoro vm console` (ADR 0057, ADR 0114,
//! ADR 0116).
//!
//! The open (`createTerminalSession`, `createConsoleSession`) is an ordinary
//! audited call and answers a single-use ticket that lives 30 seconds, so the
//! WebSocket is attached right after it, with nothing in between. The upgrade
//! (`connectInteractiveSession`) carries the ticket in its query and no other
//! credential, and no `Origin` header: the contract asks for one only from a
//! ticket minted for a panel session cookie, and these tickets are minted for
//! a token. TLS is the HTTP client's: the same `ClientConfig`, pinned to the
//! context's certificate or on the host's CA store (`tls.rs`).
//!
//! Framing (`openapi.yaml`, `connectInteractiveSession`): binary frames carry
//! terminal bytes both ways, text frames one JSON control object each. The
//! client sends `resize` when its window changes (terminal only); the server
//! sends `exit`, `closed` and `error`, then closes.
//!
//! The guest is untrusted and its bytes reach the terminal as they are, the
//! way `ssh` and a serial console client pass them; nothing here interprets
//! them.
//!
//! A terminal runs in raw mode, so every key, Ctrl-C included, goes to the
//! remote program; Ctrl-] closes the session from this side. A console sends
//! nothing at all: the echo is off, Ctrl-C leaves, and what was typed is
//! thrown away. The terminal settings are restored on every way out (a
//! [`terminal::Guard`] dropped on return, error and panic; SIGTERM, SIGHUP
//! and SIGINT end the session first).

use crate::Environment;
use crate::client::{CONNECT_TIMEOUT, tls_config};
use crate::context::Context;
use crate::error::{Failure, exit};
use crate::operations::CONNECT_INTERACTIVE_SESSION;
use crate::terminal::{self, Mode};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use std::io::{Read as _, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

/// The key that closes a terminal session from this side: Ctrl-], as telnet
/// uses it.
pub const DETACH: u8 = 0x1d;
/// How long a close waits for the server's answer.
const CLOSE_WAIT: Duration = Duration::from_secs(2);
/// The size a session starts with when the terminal's cannot be read.
const DEFAULT_SIZE: (u16, u16) = (24, 80);

type Socket = WebSocketStream<TlsStream<TcpStream>>;

/// Which session the ticket opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Terminal,
    Console,
}

/// The size of the terminal a session starts with, as rows and columns,
/// within the contract's 1 to 1000.
pub fn size(environment: &Environment) -> (u16, u16) {
    let (rows, cols) = if environment.input.is_some() {
        DEFAULT_SIZE
    } else {
        terminal::size().unwrap_or(DEFAULT_SIZE)
    };
    (rows.clamp(1, 1000), cols.clamp(1, 1000))
}

/// How the session ended, as this side saw it.
#[derive(Default)]
struct Ending {
    /// The `exit` control: the terminal's program exited with this code.
    exit_code: Option<i64>,
    /// The `closed` control's reason.
    reason: Option<String>,
    /// The user left: Ctrl-], Ctrl-C or a signal.
    left: bool,
}

/// Attaches the WebSocket of the session `ticket` (an
/// `InteractiveSessionTicket`) opened on `context`'s primary and relays it
/// until it closes.
pub async fn attach(
    context: &Context,
    ticket: &Value,
    kind: Kind,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let (Some(id), Some(secret)) = (ticket["id"].as_str(), ticket["ticket"].as_str()) else {
        return Err(Failure::other(
            "the primary's answer carries no session ticket",
        ));
    };
    let path = CONNECT_INTERACTIVE_SESSION.path.replace("{session_id}", id);
    let mut socket = connect(context, &format!("{path}?ticket={secret}")).await?;
    // The terminal is changed only once the session is attached, and it is
    // restored before anything below is printed.
    let guard = if environment.input.is_none() && environment.stdin_is_terminal {
        let mode = match kind {
            Kind::Terminal => Mode::Raw,
            Kind::Console => Mode::Quiet,
        };
        Some(
            terminal::Guard::enter(mode)
                .map_err(|e| Failure::other(format!("cannot set the terminal up: {e}")))?,
        )
    } else {
        None
    };
    let relayed = relay(&mut socket, kind, environment, stdout, stderr).await;
    drop(guard);
    let ending = relayed?;
    finish(kind, &ending, stderr)
}

/// What the session's end means for the exit code.
fn finish(kind: Kind, ending: &Ending, stderr: &mut dyn Write) -> Result<(), Failure> {
    if ending.left {
        let _ = writeln!(stderr, "quoro: closed the session");
        return Ok(());
    }
    match (kind, ending.exit_code, ending.reason.as_deref()) {
        (Kind::Terminal, Some(0), _) => Ok(()),
        (Kind::Terminal, Some(code), _) => Err(Failure::other(format!(
            "the program exited with code {code}"
        ))),
        (_, None, Some(reason)) => Err(Failure::other(format!(
            "the primary closed the session: {reason}"
        ))),
        (_, None, None) => Err(Failure::new(
            exit::UNREACHABLE,
            "the connection to the primary closed before the session ended",
        )),
        (Kind::Console, Some(_), reason) => Err(Failure::other(format!(
            "the primary closed the session: {}",
            reason.unwrap_or("exited")
        ))),
    }
}

/// TCP, TLS and the WebSocket handshake to `path_and_query` on the
/// context's primary, within the client's connect bound.
async fn connect(context: &Context, path_and_query: &str) -> Result<Socket, Failure> {
    let url = reqwest::Url::parse(&context.url).map_err(|e| {
        Failure::usage(format!("the context's URL {} is invalid: {e}", context.url))
    })?;
    let unreachable = |cause: &dyn std::fmt::Display| {
        Failure::new(
            exit::UNREACHABLE,
            format!("cannot reach the primary at {}: {cause}", context.url),
        )
    };
    let port = url.port_or_known_default().unwrap_or(443);
    let Some(host) = url.host_str() else {
        return Err(Failure::usage(format!(
            "the context's URL {url} has no host"
        )));
    };
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    let server_name = match bare.parse::<std::net::IpAddr>() {
        Ok(ip) => rustls::pki_types::ServerName::IpAddress(ip.into()),
        Err(_) => rustls::pki_types::ServerName::try_from(bare.to_string())
            .map_err(|e| Failure::usage(format!("the host {host} is invalid: {e}")))?,
    };
    let address = bare.to_string();
    let authority = format!("{host}:{port}");
    // The configuration offers no ALPN protocol, so the server speaks
    // HTTP/1.1, which the upgrade needs.
    let connector = TlsConnector::from(Arc::new(tls_config(context.trust)?));
    let handshake = async {
        let tcp = TcpStream::connect((address.as_str(), port))
            .await
            .map_err(|e| unreachable(&e))?;
        let tls = connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| unreachable(&e))?;
        let mut request = format!("wss://{authority}{path_and_query}")
            .into_client_request()
            .map_err(|e| Failure::other(format!("cannot build the WebSocket request: {e}")))?;
        request.headers_mut().insert(
            "user-agent",
            reqwest::header::HeaderValue::from_static(concat!("quoro/", env!("CARGO_PKG_VERSION"))),
        );
        match tokio_tungstenite::client_async(request, tls).await {
            Ok((socket, _)) => Ok(socket),
            Err(WsError::Http(response)) => {
                let request_id = response
                    .headers()
                    .get("quoro-request-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let body = response.body().clone().unwrap_or_default();
                Err(Failure::from_response(
                    response.status().as_u16(),
                    &body,
                    request_id,
                ))
            }
            Err(e) => Err(unreachable(&e)),
        }
    };
    tokio::time::timeout(CONNECT_TIMEOUT, handshake)
        .await
        .map_err(|_| unreachable(&"the WebSocket upgrade timed out"))?
}

/// What the loop waits on besides the socket.
enum Input {
    Bytes(Vec<u8>),
    End,
}

/// Relays the socket until it closes or the user leaves.
async fn relay(
    socket: &mut Socket,
    kind: Kind,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Ending, Failure> {
    let mut ending = Ending::default();
    let (tx, mut input) = tokio::sync::mpsc::unbounded_channel();
    let mut reading = kind == Kind::Terminal;
    if reading {
        read_stdin(environment, tx);
    }
    let mut resize = Resize::new();
    let mut leave = Leave::new();
    let mut size = self::size(environment);
    loop {
        tokio::select! {
            message = socket.next() => match message {
                Some(Ok(Message::Binary(bytes))) => {
                    stdout
                        .write_all(&bytes)
                        .and_then(|()| stdout.flush())
                        .map_err(|e| Failure::other(format!("cannot write to stdout: {e}")))?;
                }
                Some(Ok(Message::Text(text))) => control(text.as_str(), &mut ending, stderr),
                Some(Ok(Message::Close(_))) | None => return Ok(ending),
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    return Err(Failure::new(
                        exit::UNREACHABLE,
                        format!("the session's connection failed: {e}"),
                    ));
                }
            },
            next = input.recv(), if reading => match next {
                Some(Input::Bytes(bytes)) => {
                    let (keys, detach) = match bytes.iter().position(|b| *b == DETACH) {
                        Some(at) => (&bytes[..at], true),
                        None => (&bytes[..], false),
                    };
                    if !keys.is_empty() {
                        send(socket, Message::Binary(keys.to_vec().into())).await?;
                    }
                    if detach {
                        ending.left = true;
                        close(socket).await;
                        return Ok(ending);
                    }
                }
                // stdin ended: the session goes on until the program exits.
                Some(Input::End) | None => reading = false,
            },
            () = resize.changed(), if kind == Kind::Terminal => {
                let now = self::size(environment);
                if now != size {
                    size = now;
                    let control = json!({ "type": "resize", "rows": now.0, "cols": now.1 });
                    send(socket, Message::Text(control.to_string().into())).await?;
                }
            }
            () = leave.arrived() => {
                ending.left = true;
                close(socket).await;
                return Ok(ending);
            }
        }
    }
}

/// One control message from the server.
fn control(text: &str, ending: &mut Ending, stderr: &mut dyn Write) {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return;
    };
    match value["type"].as_str() {
        Some("exit") => ending.exit_code = value["code"].as_i64(),
        Some("closed") => ending.reason = value["reason"].as_str().map(str::to_string),
        Some("error") => {
            let message = value["message"].as_str().unwrap_or("session error");
            let code = value["code"].as_str().unwrap_or("error");
            // In raw mode a line feed alone does not return the cursor.
            let _ = write!(stderr, "\r\nquoro: {message} ({code})\r\n");
        }
        _ => {}
    }
}

async fn send(socket: &mut Socket, message: Message) -> Result<(), Failure> {
    socket.send(message).await.map_err(|e| {
        Failure::new(
            exit::UNREACHABLE,
            format!("the session's connection failed: {e}"),
        )
    })
}

/// Closes the WebSocket, which ends the session (`ended_by_client`), and
/// waits a moment for the server's close.
async fn close(socket: &mut Socket) {
    let _ = tokio::time::timeout(CLOSE_WAIT, async {
        let _ = socket.close(None).await;
        while let Some(Ok(_)) = socket.next().await {}
    })
    .await;
}

/// Reads stdin on a thread of its own: a blocking read must not keep the
/// runtime alive once the session is over.
fn read_stdin(environment: &Environment, tx: tokio::sync::mpsc::UnboundedSender<Input>) {
    let test_input = environment.input.clone();
    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        loop {
            let read = match &test_input {
                Some(crate::Input(cursor)) => match cursor.lock() {
                    Ok(mut cursor) => cursor.read(&mut buffer),
                    Err(_) => Ok(0),
                },
                None => std::io::stdin().lock().read(&mut buffer),
            };
            match read {
                Ok(0) | Err(_) => {
                    let _ = tx.send(Input::End);
                    return;
                }
                Ok(n) => {
                    if tx.send(Input::Bytes(buffer[..n].to_vec())).is_err() {
                        return;
                    }
                }
            }
        }
    });
}

/// The window-change signal; never, when it cannot be watched.
struct Resize(#[cfg(unix)] Option<tokio::signal::unix::Signal>);

impl Resize {
    fn new() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Self(signal(SignalKind::window_change()).ok())
        }
        #[cfg(not(unix))]
        Self()
    }

    async fn changed(&mut self) {
        #[cfg(unix)]
        if let Some(signal) = &mut self.0
            && signal.recv().await.is_some()
        {
            return;
        }
        std::future::pending::<()>().await;
    }
}

/// The signals that end a session: SIGINT, SIGTERM and SIGHUP.
struct Leave {
    #[cfg(unix)]
    others: Vec<tokio::signal::unix::Signal>,
    interrupt: crate::logs::Interrupt,
}

impl Leave {
    fn new() -> Self {
        Self {
            #[cfg(unix)]
            others: {
                use tokio::signal::unix::{SignalKind, signal};
                [SignalKind::terminate(), SignalKind::hangup()]
                    .into_iter()
                    .filter_map(|kind| signal(kind).ok())
                    .collect()
            },
            interrupt: crate::logs::interrupt(),
        }
    }

    async fn arrived(&mut self) {
        #[cfg(unix)]
        if !self.others.is_empty() {
            let others = futures_util::future::select_all(
                self.others.iter_mut().map(|signal| Box::pin(signal.recv())),
            );
            tokio::select! {
                () = &mut self.interrupt => {}
                _ = others => {}
            }
            return;
        }
        (&mut self.interrupt).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finished(kind: Kind, ending: Ending) -> Result<(), u8> {
        finish(kind, &ending, &mut Vec::new()).map_err(|failure| failure.code)
    }

    #[test]
    fn the_end_of_a_session_is_its_exit_code() {
        let exited = |code| Ending {
            exit_code: Some(code),
            reason: Some("exited".to_string()),
            left: false,
        };
        assert_eq!(finished(Kind::Terminal, exited(0)), Ok(()));
        assert_eq!(finished(Kind::Terminal, exited(3)), Err(exit::OTHER));
        let left = Ending {
            left: true,
            ..Ending::default()
        };
        assert_eq!(finished(Kind::Console, left), Ok(()));
        let idle = Ending {
            reason: Some("idle_timeout".to_string()),
            ..Ending::default()
        };
        assert_eq!(finished(Kind::Console, idle), Err(exit::OTHER));
        assert_eq!(
            finished(Kind::Terminal, Ending::default()),
            Err(exit::UNREACHABLE)
        );
    }

    #[test]
    fn controls_are_read_and_unknown_ones_ignored() {
        let mut ending = Ending::default();
        let mut stderr = Vec::new();
        control(r#"{"type":"exit","code":2}"#, &mut ending, &mut stderr);
        control(
            r#"{"type":"closed","reason":"exited"}"#,
            &mut ending,
            &mut stderr,
        );
        control(r#"{"type":"later"}"#, &mut ending, &mut stderr);
        control("not json", &mut ending, &mut stderr);
        control(
            r#"{"type":"error","code":"validation_failed","message":"no input"}"#,
            &mut ending,
            &mut stderr,
        );
        assert_eq!(ending.exit_code, Some(2));
        assert_eq!(ending.reason.as_deref(), Some("exited"));
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains("no input (validation_failed)")
        );
    }
}

//! The running primary's local control socket and `quoro cluster
//! maintenance` (`node-agent-protocol.md` section 8.11, ADR 0171).
//!
//! `quoro serve` opens `/run/quoro/control.sock` once the control plane is
//! up. The socket file is the service user's, mode `0600`, so only that user
//! and root may open it, and the primary also checks the peer's uid on each
//! connection. Nothing about it is on the network. `quoro.service` gets the
//! directory from systemd (`RuntimeDirectory=quoro`); a primary that cannot
//! open the socket logs it and serves on, and the command then fails, which
//! the installer reports and goes past.
//!
//! One request per connection, one JSON line each way, both written by this
//! binary only:
//!
//! - `{"op": "maintenance", "duration_s": 180}` opens a window, and
//!   `{"op": "maintenance", "end": true}` ends it;
//! - the answer is `{"ok": true|false, "message": "..."}`, where `message`
//!   is what the command prints.
//!
//! `QUORO_CONTROL_SOCKET` names another path, for tests.

use quoro_control_plane::session::NodeHub;
use quoro_control_plane::session::maintenance::{
    DEFAULT_WINDOW_S, Ending, MAX_WINDOW_S, MIN_WINDOW_S, Opened,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

/// Where the primary listens (section 8.11).
pub const DEFAULT_SOCKET: &str = "/run/quoro/control.sock";

/// Names another socket path, for tests.
pub const SOCKET_VARIABLE: &str = "QUORO_CONTROL_SOCKET";

/// The longest request line the primary reads.
const MAX_REQUEST: u64 = 4_096;

/// How long the primary waits for a request line.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the command waits to connect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the command waits for the answer: the opening's three steps
/// take at most about 20 s with the defaults.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(120);

/// The socket path: `QUORO_CONTROL_SOCKET` when set, else
/// [`DEFAULT_SOCKET`].
#[must_use]
pub fn socket_path() -> PathBuf {
    std::env::var_os(SOCKET_VARIABLE)
        .filter(|path| !path.is_empty())
        .map_or_else(|| PathBuf::from(DEFAULT_SOCKET), PathBuf::from)
}

/// The control socket a running primary holds open. Dropping it stops
/// accepting and removes the socket file.
pub struct Control {
    path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Control {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Opens the control socket at `path` for `hub`'s primary: creates its
/// directory when missing, replaces a stale socket left by an earlier run,
/// binds, sets mode `0600`, and serves each connection on its own task.
///
/// # Errors
///
/// A message when the socket cannot be opened: the primary serves on
/// without it.
pub fn listen(path: &Path, hub: NodeHub) -> Result<Control, String> {
    use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};

    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
        && !dir.exists()
    {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
    match std::fs::symlink_metadata(path) {
        // A socket another process still answers on is not stale.
        Ok(meta)
            if meta.file_type().is_socket()
                && std::os::unix::net::UnixStream::connect(path).is_ok() =>
        {
            return Err(format!(
                "another process answers on {}; it is left as it is",
                path.display()
            ));
        }
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)
            .map_err(|e| format!("cannot remove the stale {}: {e}", path.display()))?,
        Ok(_) => {
            return Err(format!(
                "{} exists and is not a socket; it is left as it is",
                path.display()
            ));
        }
        Err(_) => {}
    }
    let listener =
        UnixListener::bind(path).map_err(|e| format!("cannot bind {}: {e}", path.display()))?;
    // The service's `UMask=0077` already leaves no access to others; this
    // makes it so whatever the umask.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("cannot set the mode of {}: {e}", path.display()))?;
    info!(path = %path.display(), "control socket open (quoro cluster maintenance)");
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let hub = hub.clone();
                    tokio::spawn(async move { serve(stream, &hub).await });
                }
                Err(e) => {
                    warn!(%e, "the control socket cannot accept");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(Control {
        path: path.to_path_buf(),
        task,
    })
}

/// The effective uid of this process: `/proc/self` belongs to it.
#[cfg(target_os = "linux")]
fn own_uid() -> Option<u32> {
    use std::os::unix::fs::MetadataExt as _;
    std::fs::metadata("/proc/self").ok().map(|meta| meta.uid())
}

/// Whether the peer may use the socket: root or the service user, the same
/// users the file mode lets in (section 8.11).
#[cfg(target_os = "linux")]
fn peer_allowed(stream: &UnixStream) -> bool {
    match (stream.peer_cred(), own_uid()) {
        (Ok(peer), Some(own)) => peer.uid() == 0 || peer.uid() == own,
        _ => false,
    }
}

/// Elsewhere the file mode alone decides.
#[cfg(not(target_os = "linux"))]
fn peer_allowed(_stream: &UnixStream) -> bool {
    true
}

/// Serves one connection: one request line, one answer line.
async fn serve(mut stream: UnixStream, hub: &NodeHub) {
    let answer = if peer_allowed(&stream) {
        match read_request(&mut stream).await {
            Ok(request) => answer(hub, &request).await,
            Err(message) => refusal(&message),
        }
    } else {
        warn!("a control-socket peer that is neither root nor the service user was refused");
        refusal("only root and the service user may use the control socket")
    };
    let mut line = answer.to_string();
    line.push('\n');
    let _ = stream.write_all(line.as_bytes()).await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut UnixStream) -> Result<Value, String> {
    let mut line = String::new();
    let mut reader = BufReader::new(stream).take(MAX_REQUEST);
    match tokio::time::timeout(READ_TIMEOUT, reader.read_line(&mut line)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(format!("cannot read the request: {e}")),
        Err(_) => return Err("no request arrived in time".to_string()),
    }
    serde_json::from_str(line.trim()).map_err(|e| format!("the request is not JSON: {e}"))
}

fn refusal(message: &str) -> Value {
    json!({ "ok": false, "message": message })
}

/// Runs one request on the primary.
async fn answer(hub: &NodeHub, request: &Value) -> Value {
    if request["op"] != "maintenance" {
        return refusal("unknown request");
    }
    if request["end"] == true {
        return match hub.end_maintenance().await {
            Ok(Ending::Ended) => json!({
                "ok": true,
                "message": "the maintenance window is closed: renewals are answered with D again",
            }),
            Ok(Ending::NothingOpen) => json!({
                "ok": true,
                "message": "no maintenance window is open; nothing to end",
            }),
            Err(e) => refusal(&e.to_string()),
        };
    }
    let Some(duration_s) = request["duration_s"].as_u64() else {
        return refusal("the request names no duration_s");
    };
    info!(duration_s, "quoro cluster maintenance");
    match hub.open_maintenance(duration_s).await {
        Ok(opened) => json!({ "ok": true, "message": opened_message(&opened) }),
        Err(e) => refusal(&e.to_string()),
    }
}

fn opened_message(opened: &Opened) -> String {
    let covered = match &opened.covered_until {
        Some(until) => format!(
            "{} expiring lease(s) covered until about {until}",
            opened.leases
        ),
        None => "no expiring lease to cover".to_string(),
    };
    format!(
        "maintenance window of {} s open until {}; {covered}",
        opened.duration_s, opened.closes_at
    )
}

/// What `quoro cluster maintenance` asks for.
#[derive(Debug, PartialEq, Eq)]
enum Request {
    Open { duration_s: u64 },
    End,
}

/// Reads `[--duration <seconds>] [--end]`.
fn parse(args: &[String]) -> Result<Request, String> {
    let mut duration: Option<u64> = None;
    let mut end = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--end" => end = true,
            "--duration" => {
                let value = iter.next().ok_or("--duration needs a number of seconds")?;
                let seconds = value
                    .parse::<u64>()
                    .map_err(|_| format!("--duration takes whole seconds, not '{value}'"))?;
                duration = Some(seconds);
            }
            other => return Err(format!("unknown argument '{other}'")),
        }
    }
    match (end, duration) {
        (true, Some(_)) => Err("--end takes no --duration".to_string()),
        (true, None) => Ok(Request::End),
        (false, duration) => {
            let duration_s = duration.unwrap_or(DEFAULT_WINDOW_S);
            if (MIN_WINDOW_S..=MAX_WINDOW_S).contains(&duration_s) {
                Ok(Request::Open { duration_s })
            } else {
                Err(format!(
                    "--duration must be {MIN_WINDOW_S} to {MAX_WINDOW_S} seconds, not {duration_s}"
                ))
            }
        }
    }
}

/// `quoro cluster maintenance [--duration <seconds>]` and `--end`, as root
/// on the primary's host.
pub async fn maintenance_command(args: &[String]) -> ExitCode {
    let request = match parse(args) {
        Ok(request) => request,
        Err(message) => return crate::usage_error(&format!("cluster maintenance: {message}")),
    };
    let body = match request {
        Request::Open { duration_s } => json!({ "op": "maintenance", "duration_s": duration_s }),
        Request::End => json!({ "op": "maintenance", "end": true }),
    };
    let path = socket_path();
    match call(&path, &body).await {
        Ok(answer) if answer["ok"] == true => {
            println!("{}", answer["message"].as_str().unwrap_or("done"));
            ExitCode::SUCCESS
        }
        Ok(answer) => {
            eprintln!(
                "quoro cluster maintenance: {}",
                answer["message"].as_str().unwrap_or("the primary refused")
            );
            ExitCode::from(1)
        }
        Err(message) => {
            eprintln!("quoro cluster maintenance: {message}");
            ExitCode::from(1)
        }
    }
}

/// Sends `body` to the primary at `path` and returns its answer.
async fn call(path: &Path, body: &Value) -> Result<Value, String> {
    let shown = path.display();
    let mut stream = match tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(path)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            return Err(format!(
                "permission denied on {shown}: run it as root on the primary's host"
            ));
        }
        Ok(Err(e)) => {
            return Err(format!(
                "no running primary answers on {shown} ({e}): run it on the primary's host while \
                 quoro.service runs"
            ));
        }
        Err(_) => return Err(format!("no running primary answered on {shown} in time")),
    };
    let mut line = body.to_string();
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .await
        .map_err(|e| format!("cannot send the request on {shown}: {e}"))?;
    let mut answer = String::new();
    let mut reader = BufReader::new(stream);
    match tokio::time::timeout(ANSWER_TIMEOUT, reader.read_line(&mut answer)).await {
        Ok(Ok(0)) => Err("the primary closed the socket without an answer".to_string()),
        Ok(Ok(_)) => serde_json::from_str(answer.trim())
            .map_err(|e| format!("the primary's answer is not JSON: {e}")),
        Ok(Err(e)) => Err(format!("cannot read the primary's answer: {e}")),
        Err(_) => Err("the primary did not answer in time".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|arg| (*arg).to_string()).collect()
    }

    /// Section 8.11: `W` defaults to 180 s and is `D` to 600 s; `--end`
    /// stands alone.
    #[test]
    fn the_command_line_takes_a_duration_in_range_or_end() {
        assert_eq!(parse(&args(&[])), Ok(Request::Open { duration_s: 180 }));
        assert_eq!(
            parse(&args(&["--duration", "30"])),
            Ok(Request::Open { duration_s: 30 })
        );
        assert_eq!(
            parse(&args(&["--duration", "15"])),
            Ok(Request::Open { duration_s: 15 })
        );
        assert_eq!(
            parse(&args(&["--duration", "600"])),
            Ok(Request::Open { duration_s: 600 })
        );
        assert_eq!(parse(&args(&["--end"])), Ok(Request::End));
        for bad in [
            &["--duration", "14"][..],
            &["--duration", "601"],
            &["--duration", "1.5"],
            &["--duration"],
            &["--end", "--duration", "30"],
            &["--force"],
        ] {
            assert!(parse(&args(bad)).is_err(), "{bad:?}");
        }
    }
}

//! Supervision of the `cloudflared` connector process (ADR 0024, 0055).
//!
//! One connector per node dials out to Cloudflare and forwards every tunnel
//! hostname to the embedded proxy on a loopback port. The tunnel token reaches
//! the process only through the `TUNNEL_TOKEN` environment variable: never in
//! argv, never in a log line, and the child's environment holds nothing else
//! (no `HOME`, no proxy settings, nothing inherited from the node agent).
//!
//! [`Cloudflared::spawn`] starts a supervisor task; it restarts the child with
//! a 1 s backoff doubling to 60 s when it exits, restarts it immediately when
//! the token is replaced after a rotation, and kills and reaps it on shutdown.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quoro_providers::Token;
use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWriteExt as _, BufReader,
};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

/// How the connector is started.
#[derive(Clone, Debug)]
pub struct CloudflaredConfig {
    /// The connector binary, for example `/usr/local/lib/quoro/cloudflared`.
    pub binary: PathBuf,
    /// Loopback address for `--metrics`, for example `127.0.0.1:20241`.
    pub metrics: SocketAddr,
    /// Extra arguments appended to the command line (tests only).
    pub extra_args: Vec<String>,
}

struct Inner {
    config: CloudflaredConfig,
    token: Mutex<Token>,
    /// Wakes the supervisor when [`Cloudflared::set_token`] replaced the token.
    token_changed: Notify,
}

/// A supervised connector process.
pub struct Cloudflared {
    inner: Arc<Inner>,
    task: Option<JoinHandle<()>>,
}

impl Cloudflared {
    /// Spawns the supervisor task. The child runs
    /// `<binary> tunnel --no-autoupdate --metrics <addr> run` with the token
    /// only in `TUNNEL_TOKEN`; it is restarted when it exits and killed when
    /// `shutdown` turns true.
    #[must_use]
    pub fn spawn(config: CloudflaredConfig, token: Token, shutdown: watch::Receiver<bool>) -> Self {
        let inner = Arc::new(Inner {
            config,
            token: Mutex::new(token),
            token_changed: Notify::new(),
        });
        let task = tokio::spawn(supervise(Arc::clone(&inner), shutdown));
        Self {
            inner,
            task: Some(task),
        }
    }

    /// Replaces the token (after a rotation) and restarts the child.
    pub fn set_token(&self, token: Token) {
        {
            let mut current = self
                .inner
                .token
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *current = token;
        }
        self.inner.token_changed.notify_one();
    }

    /// Whether the connector's metrics endpoint answers `GET /ready` with 200.
    #[must_use]
    pub async fn ready(&self) -> bool {
        let addr = self.inner.config.metrics;
        let Ok(mut stream) = TcpStream::connect(addr).await else {
            return false;
        };
        let request = format!("GET /ready HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        let answered = async {
            stream.write_all(request.as_bytes()).await.ok()?;
            let mut buf = Vec::with_capacity(128);
            let mut chunk = [0u8; 128];
            while !buf.windows(2).any(|w| w == b"\r\n") && buf.len() < 1024 {
                let n = stream.read(&mut chunk).await.ok()?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Some(String::from_utf8_lossy(&buf).into_owned())
        };
        let Some(head) = tokio::time::timeout(Duration::from_secs(2), answered)
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let status = head.lines().next().unwrap_or_default();
        status.contains(" 200 ")
    }

    /// Waits for the supervisor task to end (after shutdown).
    pub async fn join(self) {
        if let Some(task) = self.task {
            let _ = task.await;
        }
    }
}

/// Why the supervisor woke up while the child was running or while waiting.
enum Wake {
    Timer,
    Shutdown,
    Restart,
}

async fn supervise(inner: Arc<Inner>, mut shutdown: watch::Receiver<bool>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            return;
        }
        let token = {
            let token = inner
                .token
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            token.clone()
        };
        let mut child = match spawn_child(&inner.config, &token) {
            Ok(child) => child,
            Err(e) => {
                tracing::warn!(target: "cloudflared", binary = %inner.config.binary.display(), error = %e, "could not start the connector");
                match wait(&inner, &mut shutdown, backoff).await {
                    Wake::Shutdown => return,
                    Wake::Restart => backoff = Duration::from_secs(1),
                    Wake::Timer => backoff = next_backoff(backoff),
                }
                continue;
            }
        };
        tracing::info!(target: "cloudflared", pid = ?child.id(), "connector started");
        let started = Instant::now();
        let stdout = log_output(child.stdout.take(), "stdout");
        let stderr = log_output(child.stderr.take(), "stderr");

        let wake = tokio::select! {
            () = shutdown.changed_signal() => Wake::Shutdown,
            () = inner.token_changed.notified() => Wake::Restart,
            _ = child.wait() => Wake::Timer,
        };
        // Tear the log readers down before anything reuses the pipes; a
        // grandchild may still hold them open after the connector is gone.
        for logger in [stdout, stderr] {
            logger.abort();
        }
        match wake {
            Wake::Shutdown => {
                terminate(&mut child).await;
                tracing::info!(target: "cloudflared", "connector stopped");
                return;
            }
            Wake::Restart => {
                terminate(&mut child).await;
                tracing::info!(target: "cloudflared", "connector restarting with a new token");
                backoff = Duration::from_secs(1);
            }
            Wake::Timer => {
                if started.elapsed() >= Duration::from_secs(60) {
                    backoff = Duration::from_secs(1);
                }
                tracing::warn!(target: "cloudflared", "connector exited; restarting in {backoff:?}");
                match wait(&inner, &mut shutdown, backoff).await {
                    Wake::Shutdown => return,
                    Wake::Restart => {
                        backoff = Duration::from_secs(1);
                        continue;
                    }
                    Wake::Timer => backoff = next_backoff(backoff),
                }
            }
        }
    }
}

fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(Duration::from_secs(60))
}

async fn wait(inner: &Inner, shutdown: &mut watch::Receiver<bool>, delay: Duration) -> Wake {
    tokio::select! {
        () = tokio::time::sleep(delay) => Wake::Timer,
        () = shutdown.changed_signal() => Wake::Shutdown,
        () = inner.token_changed.notified() => Wake::Restart,
    }
}

/// Small extension so both waits read the same: shutdown is a watch channel
/// whose sender may disappear when the node agent exits.
trait ChangedSignal {
    async fn changed_signal(&mut self);
}

impl ChangedSignal for watch::Receiver<bool> {
    async fn changed_signal(&mut self) {
        // A dropped sender means the owner is gone: treat it as shutdown.
        let _ = self.changed().await;
    }
}

fn spawn_child(config: &CloudflaredConfig, token: &Token) -> io::Result<Child> {
    let mut command = Command::new(&config.binary);
    command
        .arg("tunnel")
        .arg("--no-autoupdate")
        .arg("--metrics")
        .arg(config.metrics.to_string())
        .arg("run")
        .args(&config.extra_args)
        // Nothing from the node agent's environment leaks into the connector;
        // the token is the only secret and it never reaches argv.
        .env_clear()
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
        .env("TUNNEL_TOKEN", token.expose())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.spawn()
}

fn log_output<R>(reader: Option<R>, stream: &'static str) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let Some(reader) = reader else {
            return;
        };
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => tracing::info!(target: "cloudflared", stream, "{line}"),
                Ok(None) => return,
                Err(e) => {
                    tracing::debug!(target: "cloudflared", stream, error = %e, "connector output closed");
                    return;
                }
            }
        }
    })
}

async fn terminate(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

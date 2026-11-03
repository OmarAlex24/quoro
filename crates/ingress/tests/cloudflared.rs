//! The connector supervisor, against a fake `cloudflared` shell script.
//!
//! The script records its argv and its environment, then either sleeps or
//! exits, so the tests can see exactly what the child was given: the token
//! only in `TUNNEL_TOKEN`, never in argv, and no inherited environment.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use quoro_ingress::cloudflared::{Cloudflared, CloudflaredConfig};
use quoro_providers::Token;
use tokio::sync::watch;

const TOKEN: &str = "tunnel-token-placeholder-0123456789";

fn tmp_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes an executable fake connector that records every run to `log`.
fn fake_cloudflared(dir: &Path, body: &str) -> (PathBuf, PathBuf) {
    let binary = dir.join("cloudflared");
    let log = dir.join("runs.log");
    let script = format!(
        "#!/bin/sh\n{{ printf '%s\\n' '--- run'; printf 'argv:%s\\n' \"$*\"; printf 'pid:%s\\n' \"$$\"; \
         env; printf '%s\\n' '--- end'; }} >> '{log}'\n{body}\n",
        log = log.display()
    );
    std::fs::write(&binary, script).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    (binary, log)
}

fn config(binary: PathBuf, metrics: SocketAddr) -> CloudflaredConfig {
    CloudflaredConfig {
        binary,
        metrics,
        extra_args: Vec::new(),
    }
}

/// One recorded run of the fake connector.
#[derive(Debug, Default)]
struct Run {
    argv: String,
    pid: u32,
    env: BTreeMap<String, String>,
}

fn read_runs(log: &Path) -> Vec<Run> {
    let Ok(text) = std::fs::read_to_string(log) else {
        return Vec::new();
    };
    let mut runs = Vec::new();
    let mut current: Option<Run> = None;
    for line in text.lines() {
        if line == "--- run" {
            current = Some(Run::default());
            continue;
        }
        if line == "--- end" {
            if let Some(run) = current.take() {
                runs.push(run);
            }
            continue;
        }
        let Some(run) = current.as_mut() else {
            continue;
        };
        if let Some(argv) = line.strip_prefix("argv:") {
            run.argv = argv.to_string();
        } else if let Some(pid) = line.strip_prefix("pid:") {
            run.pid = pid.parse().unwrap_or_default();
        } else if let Some((key, value)) = line.split_once('=') {
            run.env.insert(key.to_string(), value.to_string());
        }
    }
    runs
}

async fn wait_for_runs(log: &Path, count: usize) -> Vec<Run> {
    let started = Instant::now();
    loop {
        let runs = read_runs(log);
        if runs.len() >= count {
            return runs;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "expected {count} runs, saw {}",
            runs.len()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn metrics() -> SocketAddr {
    // Documentation range, only used as an argument here.
    "127.0.0.1:20241".parse().unwrap()
}

fn process_is_gone(pid: u32) -> bool {
    !std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -0 {pid}"))
        .stderr(std::process::Stdio::null())
        .status()
        .expect("kill -0 runs")
        .success()
}

#[tokio::test]
async fn argv_carries_flags_and_env_carries_the_token() {
    let dir = tmp_dir("cloudflared-argv");
    let (binary, log) = fake_cloudflared(&dir, "sleep 30");
    let (tx, rx) = watch::channel(false);
    let metrics = metrics();
    let supervisor = Cloudflared::spawn(config(binary, metrics), Token::new(TOKEN.to_string()), rx);

    let runs = wait_for_runs(&log, 1).await;
    let run = &runs[0];
    assert!(
        run.argv
            .contains(&format!("tunnel --no-autoupdate --metrics {metrics} run")),
        "argv was {:?}",
        run.argv
    );
    assert!(
        !run.argv.contains(TOKEN),
        "the token reached argv: {:?}",
        run.argv
    );

    assert_eq!(run.env.get("TUNNEL_TOKEN").map(String::as_str), Some(TOKEN));
    assert_eq!(
        run.env.get("PATH").map(String::as_str),
        Some("/usr/sbin:/usr/bin:/sbin:/bin")
    );
    // Only PATH, TUNNEL_TOKEN and what /bin/sh sets for itself: nothing from
    // the test process (no HOME, no USER, no `CARGO_*`, no canary values).
    let allowed = [
        "PATH",
        "TUNNEL_TOKEN",
        "PWD",
        "OLDPWD",
        "SHLVL",
        "_",
        "LC_CTYPE",
    ];
    for key in run.env.keys() {
        assert!(
            allowed.contains(&key.as_str()),
            "unexpected variable {key:?} in the child's environment"
        );
    }
    assert!(!run.env.contains_key("HOME"));

    tx.send(true).unwrap();
    supervisor.join().await;
    assert!(process_is_gone(run.pid), "the connector outlived shutdown");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn an_exiting_child_is_restarted() {
    let dir = tmp_dir("cloudflared-restart");
    let (binary, log) = fake_cloudflared(&dir, "exit 1");
    let (tx, rx) = watch::channel(false);
    let supervisor =
        Cloudflared::spawn(config(binary, metrics()), Token::new(TOKEN.to_string()), rx);

    let runs = wait_for_runs(&log, 2).await;
    assert!(runs.len() >= 2);
    assert_ne!(runs[0].pid, runs[1].pid);

    tx.send(true).unwrap();
    supervisor.join().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_new_token_restarts_the_child() {
    let dir = tmp_dir("cloudflared-token");
    let (binary, log) = fake_cloudflared(&dir, "sleep 30");
    let (tx, rx) = watch::channel(false);
    let supervisor =
        Cloudflared::spawn(config(binary, metrics()), Token::new(TOKEN.to_string()), rx);

    let runs = wait_for_runs(&log, 1).await;
    assert_eq!(
        runs[0].env.get("TUNNEL_TOKEN").map(String::as_str),
        Some(TOKEN)
    );

    let rotated = "rotated-token-9876543210";
    supervisor.set_token(Token::new(rotated.to_string()));
    let runs = wait_for_runs(&log, 2).await;
    assert_eq!(
        runs[1].env.get("TUNNEL_TOKEN").map(String::as_str),
        Some(rotated)
    );
    assert!(!runs[1].argv.contains(rotated));
    assert!(process_is_gone(runs[0].pid));
    assert!(!process_is_gone(runs[1].pid));

    tx.send(true).unwrap();
    supervisor.join().await;
    assert!(process_is_gone(runs[1].pid));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn ready_follows_the_metrics_endpoint() {
    let dir = tmp_dir("cloudflared-ready");
    let (binary, log) = fake_cloudflared(&dir, "sleep 30");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let answering = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut request = [0u8; 256];
        let _ = socket.read(&mut request).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let (tx, rx) = watch::channel(false);
    let supervisor = Cloudflared::spawn(config(binary, addr), Token::new(TOKEN.to_string()), rx);
    wait_for_runs(&log, 1).await;
    assert!(supervisor.ready().await);
    answering.await.unwrap();
    tx.send(true).unwrap();
    supervisor.join().await;

    // Nothing listens on a port that was bound and released: not ready.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap();
    drop(dead);
    let (tx, rx) = watch::channel(false);
    let idle = Cloudflared::spawn(
        config(dir.join("cloudflared"), dead_addr),
        Token::new(TOKEN.to_string()),
        rx,
    );
    assert!(!idle.ready().await);
    tx.send(true).unwrap();
    idle.join().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

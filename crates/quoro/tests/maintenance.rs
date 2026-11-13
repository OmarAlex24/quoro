//! `quoro cluster maintenance` against the real binary
//! (`node-agent-protocol.md` section 8.11, issue #764): a running `quoro
//! serve` opens its control socket, mode `0600`, at the path
//! `QUORO_CONTROL_SOCKET` names, and the command reaches it there. The
//! primary here has no voter, so a window cannot get a majority: the command
//! fails after `D / 3` plus one margin with the reason, and the primary
//! records the `error`. The window's own steps are the control plane's
//! tests (`session::maintenance`).

use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::Command;
use tokio::time::{Instant, sleep, timeout};

/// A directory that is removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("quoro-{label}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create the temporary directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `quoro cluster maintenance <args>` against the socket at `socket`.
async fn maintenance(socket: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args(["cluster", "maintenance"])
        .args(args)
        .env("QUORO_CONTROL_SOCKET", socket)
        .output()
        .await
        .expect("run quoro cluster maintenance")
}

/// Sends one raw request line to the socket and returns the answer line.
async fn raw_request(socket: &Path, request: &str) -> serde_json::Value {
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .expect("connect to the control socket");
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("send the request");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .await
        .expect("read the answer");
    serde_json::from_str(line.trim()).expect("the answer is JSON")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[tokio::test]
async fn the_command_reaches_the_primary_over_its_control_socket() {
    let dir = TempDir::new("maintenance");
    let config = dir.0.join("quoro.toml");
    std::fs::write(
        &config,
        format!(
            "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\nnode_name = \
             \"primary\"\n\n[node]\nlocal = false\n",
            dir.0.join("cp").display()
        ),
    )
    .expect("write quoro.toml");
    let socket = dir.0.join("run").join("control.sock");

    // The usage is checked before any primary is reached.
    let out_of_range = maintenance(&socket, &["--duration", "5"]).await;
    assert_eq!(out_of_range.status.code(), Some(2));
    assert!(
        text(&out_of_range.stderr).contains("15 to 600 seconds"),
        "{}",
        text(&out_of_range.stderr)
    );
    // No socket file, as on the first upgrade from a release before the
    // maintenance window: the command says no window can open, and why.
    let absent = maintenance(&socket, &["--end"]).await;
    assert_eq!(absent.status.code(), Some(1));
    let reason = text(&absent.stderr);
    assert!(
        reason.contains("no maintenance window can open")
            && reason.contains("the running primary has no control socket")
            && reason.contains("a release from before the maintenance window"),
        "{reason}"
    );

    let mut server = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .env("QUORO_TEST_NO_VULN_DOWNLOAD", "1")
        .env("QUORO_CONTROL_SOCKET", &socket)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");
    let stderr = server.stderr.take().expect("stderr is piped");
    let log = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let mut log = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            log.push(line);
        }
        log
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while !socket.exists() {
        if Instant::now() > deadline || server.try_wait().expect("poll the server").is_some() {
            server.kill().await.ok();
            panic!(
                "the control socket did not open:\n{}",
                log.await.unwrap().join("\n")
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&socket)
            .expect("the socket's metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "only its owner and root may open it");
    }

    // The socket's lines are versioned: a request of another version, or of
    // none, is refused with an error that says so, and changes nothing.
    for (request, expected) in [
        (
            r#"{"v":2,"op":"maintenance","end":true}"#,
            "serves version 1 of the control socket, and the request is version 2",
        ),
        (
            r#"{"op":"maintenance","duration_s":30}"#,
            "the request carries no version",
        ),
    ] {
        let answer = raw_request(&socket, request).await;
        assert_eq!(answer["v"], 1, "{answer}");
        assert_eq!(answer["ok"], false, "{answer}");
        let message = answer["message"].as_str().unwrap_or_default();
        assert!(message.contains(expected), "{message}");
    }

    let nothing = maintenance(&socket, &["--end"]).await;
    assert_eq!(nothing.status.code(), Some(0), "{}", text(&nothing.stderr));
    assert!(
        text(&nothing.stdout).contains("no maintenance window is open"),
        "{}",
        text(&nothing.stdout)
    );

    let started = Instant::now();
    let refused = timeout(
        Duration::from_secs(60),
        maintenance(&socket, &["--duration", "30"]),
    )
    .await
    .expect("the command answers");
    assert_eq!(refused.status.code(), Some(1), "{}", text(&refused.stdout));
    let reason = text(&refused.stderr);
    assert!(
        reason.contains("no majority of voters supported a 30 s lease"),
        "{reason}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(6_500),
        "it waited D / 3 plus one margin"
    );

    server.kill().await.expect("stop the server");
    let _ = server.wait().await;
    // A killed primary leaves its socket file behind, and nothing answers on
    // it.
    let stale = maintenance(&socket, &["--end"]).await;
    assert_eq!(stale.status.code(), Some(1));
    assert!(
        text(&stale.stderr).contains("no running primary answers"),
        "{}",
        text(&stale.stderr)
    );
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}?mode=ro",
        dir.0.join("cp").join("quoro.db").display()
    ))
    .await
    .expect("open the database");
    // The refused requests wrote nothing: one record, the failed opening's.
    let records: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT actor_id, channel, params, result FROM audit_log \
         WHERE operation = 'cluster.maintenance' ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("read the audit log");
    assert_eq!(
        records,
        vec![(
            "cluster".to_string(),
            "cli".to_string(),
            r#"{"duration_s":30}"#.to_string(),
            "error".to_string()
        )]
    );
    pool.close().await;
}

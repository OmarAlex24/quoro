//! `quoro serve --skip-upgrade-backup` (`backup-format.md` section 7.2,
//! "Before an upgrade").
//!
//! The flag is the operator's way past a pre-upgrade backup that cannot run.
//! A start with nothing pending skips nothing: the binary serves as usual and
//! writes no `backup.upgrade_skipped` record.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

/// A data directory that is removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("quoro-{label}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create the temporary data directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The configuration the server reads: loopback on an ephemeral port and the
/// fake engine, so the local node agent needs no hypervisor.
fn write_config(dir: &Path) -> PathBuf {
    let path = dir.join("quoro.toml");
    let body = format!(
        "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\nnode_name = \"test-node\"\n\n[node]\ndata_dir = \"{}\"\n",
        dir.join("cp").display(),
        dir.join("node").display(),
    );
    std::fs::write(&path, body).expect("write quoro.toml");
    path
}

#[tokio::test(flavor = "multi_thread")]
async fn the_skip_flag_starts_a_primary_with_nothing_pending_and_writes_no_record() {
    let dir = TempDir::new("skip-upgrade");
    let config = write_config(dir.path());

    let mut server = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--skip-upgrade-backup")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");
    let stderr = server.stderr.take().expect("stderr is piped");
    let mut lines = BufReader::new(stderr).lines();
    let listened = timeout(Duration::from_secs(30), async {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains("listening on https://") {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(
        listened,
        Ok(true),
        "the primary with --skip-upgrade-backup did not listen"
    );
    server.kill().await.expect("stop the server");
    let _ = server.wait().await;

    // A new database has nothing pending, so the flag skipped nothing.
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(dir.path().join("cp").join("quoro.db"))
        .read_only(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("read the database the primary wrote");
    let skipped: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE operation = 'backup.upgrade_skipped'",
    )
    .fetch_one(&pool)
    .await
    .expect("read the audit log");
    assert_eq!(skipped, 0, "the flag with nothing pending writes no record");
    pool.close().await;
}

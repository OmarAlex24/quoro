//! `quoro node rejoin` through the built binary: its usage errors and the
//! refusal of a run that is not root's (`node-agent-protocol.md` section
//! 7.4). The moves themselves are unit tests of `src/rejoin.rs`, with a fake
//! systemd.

use std::process::{Command, Output};

fn rejoin(args: &[&str], as_other_user: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_quoro"));
    command.args(["node", "rejoin"]).args(args);
    #[cfg(unix)]
    if as_other_user && is_root() {
        use std::os::unix::process::CommandExt as _;
        command.uid(65_534).gid(65_534);
    }
    #[cfg(not(unix))]
    let _ = as_other_user;
    command.output().expect("run quoro")
}

fn is_root() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::metadata("/proc/self").is_ok_and(|metadata| metadata.uid() == 0)
    }
    #[cfg(not(unix))]
    false
}

#[test]
fn usage_errors_exit_2_with_the_usage() {
    for (args, expected) in [
        (&["--force"][..], "--force needs --statement"),
        (&["--statement", "why"][..], "--statement needs --force"),
        (&["--force", "--statement", ""][..], "non-empty statement"),
        (&["--force", "--statement"][..], "needs a value"),
        (&["--yes"][..], "unknown option '--yes'"),
        (&["now"][..], "unexpected argument 'now'"),
    ] {
        let output = rejoin(args, false);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(stderr.contains(expected), "{args:?}: {stderr}");
        assert!(stderr.contains("node rejoin [--config PATH]"), "{stderr}");
    }
}

#[test]
fn a_run_that_is_not_roots_is_refused_before_anything_is_read() {
    // Run as root, the test drops the child to uid 65534. The configuration
    // does not exist: the refusal comes before it is read.
    let output = rejoin(&["--config", "/nonexistent/quoro.toml"], true);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("run this as root"), "{stderr}");
    assert!(!stderr.contains("cannot read"), "{stderr}");
}

/// A control-plane database at `path` whose `meta` holds `rows`.
async fn database(path: &std::path::Path, rows: &[(&str, &str)]) {
    use sqlx::Connection as _;
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&mut connection)
        .await
        .unwrap();
    for (key, value) in rows {
        sqlx::query("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&mut connection)
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
}

/// The whole command as root, with a real configuration file, on a host
/// without systemd: the production paths, the real `systemctl` seam (absent
/// here) and the real journal socket (absent here). Ignored by default: run
/// as root on a host with `quoro.service` it would stop the real unit. The
/// container gate runs it with `--ignored`, and it skips itself on a host
/// with systemd.
#[tokio::test]
#[ignore = "runs the real command as root; for a container without systemd"]
async fn as_root_without_systemd_the_binary_moves_a_demoted_control_plane_aside() {
    use std::path::Path;
    if !is_root()
        || Path::new("/run/systemd/system").exists()
        || Path::new("/etc/systemd/system/quoro.service").exists()
    {
        eprintln!("skipped: not root, or this host runs systemd");
        return;
    }
    let root = std::path::PathBuf::from(format!("/tmp/qrb-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let (data_dir, node_dir) = (root.join("lib/quoro"), root.join("lib/quoro-node"));
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&node_dir).unwrap();
    let config = root.join("quoro.toml");
    std::fs::write(
        &config,
        format!(
            "data_dir = \"{}\"\n\n[node]\ndata_dir = \"{}\"\nlocal = false\n",
            data_dir.display(),
            node_dir.display()
        ),
    )
    .unwrap();
    let config = config.display().to_string();
    let stdout = |output: &Output| String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = |output: &Output| String::from_utf8_lossy(&output.stderr).into_owned();

    // No database, no unit, no copy.
    let output = rejoin(&["--config", &config], false);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(stdout(&output).contains("no control plane to move aside"));

    // A database that records no demotion: refused, nothing changed.
    database(&data_dir.join("quoro.db"), &[("term", "5")]).await;
    let names = |dir: &Path| {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        names
    };
    let before = (names(&root.join("lib")), names(&data_dir));
    let output = rejoin(&["--config", &config], false);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(stderr(&output).contains("does not count as demoted"));
    assert_eq!((names(&root.join("lib")), names(&data_dir)), before);

    // A forced run with no journal to write to: refused.
    if !Path::new("/run/systemd/journal/socket").exists() {
        let output = rejoin(
            &["--config", &config, "--force", "--statement", "why"],
            false,
        );
        assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
        assert!(
            stderr(&output).contains("system journal"),
            "{}",
            stderr(&output)
        );
        assert_eq!((names(&root.join("lib")), names(&data_dir)), before);
    }

    // A recorded demotion: the data directory moves; without systemctl the
    // reload only warns.
    database(&data_dir.join("quoro.db"), &[("demoted_by_term", "7")]).await;
    let output = rejoin(&["--config", &config], false);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(
        stdout(&output).contains("daemon-reload failed"),
        "{}",
        stdout(&output)
    );
    assert!(!data_dir.exists());
    let copies: Vec<_> = names(&root.join("lib"))
        .into_iter()
        .filter(|name| name.to_string_lossy().starts_with("quoro.demoted-t7-"))
        .collect();
    assert_eq!(copies.len(), 1);
    let copy = root.join("lib").join(&copies[0]);
    assert!(copy.join("rejoin-done").is_file());
    assert!(copy.join("quoro/quoro.db").is_file());
    assert!(node_dir.exists());

    // And again: already rejoined.
    let output = rejoin(&["--config", &config], false);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(stdout(&output).contains("already rejoined"));
    let _ = std::fs::remove_dir_all(&root);
}

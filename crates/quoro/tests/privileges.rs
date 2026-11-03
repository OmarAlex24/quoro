//! The privilege split of ADR 0089: the control plane holds no capabilities
//! and only the node agent keeps the privileges the engine needs.
//!
//! Two layers are checked. The systemd units are read as shipped, because they
//! are what takes the capabilities away on a real host. And on Linux the
//! running `quoro serve` process is inspected through `/proc/<pid>/status`, so
//! a binary that gained a capability some other way (a file capability, a
//! setuid bit) fails here too.

use std::path::Path;

/// `packaging/systemd/<name>` in the repository.
fn unit(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packaging/systemd")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Every value of `key=` in the unit, in order. Comments are skipped.
fn values<'a>(unit: &'a str, key: &str) -> Vec<&'a str> {
    let prefix = format!("{key}=");
    unit.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .map(str::trim)
        .collect()
}

#[test]
fn the_control_plane_unit_runs_unprivileged_with_no_capabilities() {
    let unit = unit("quoro.service");
    assert_eq!(values(&unit, "ExecStart"), ["/usr/local/bin/quoro serve"]);
    assert_eq!(
        values(&unit, "User"),
        ["quoro-api"],
        "a dedicated system user"
    );
    assert_eq!(values(&unit, "Group"), ["quoro-api"]);
    // An empty assignment empties the set; it must be the only assignment so
    // nothing later adds a capability back.
    assert_eq!(values(&unit, "CapabilityBoundingSet"), [""]);
    assert_eq!(values(&unit, "AmbientCapabilities"), [""]);
    assert_eq!(values(&unit, "NoNewPrivileges"), ["yes"]);
    assert_eq!(values(&unit, "ProtectSystem"), ["strict"]);
    assert_eq!(values(&unit, "ReadWritePaths"), ["/var/lib/quoro"]);
    assert_eq!(
        values(&unit, "PrivateDevices"),
        ["yes"],
        "no /dev/kvm, no TAP"
    );
}

#[test]
fn the_node_agent_unit_is_the_privileged_one_and_spares_the_vms() {
    let unit = unit("quoro-node.service");
    assert_eq!(values(&unit, "ExecStart"), ["/usr/local/bin/quoro node"]);
    assert!(values(&unit, "User").is_empty(), "the engine needs root");
    // Restarting the agent must not stop the VMMs it spawned: the next agent
    // adopts them from the engine state.
    assert_eq!(values(&unit, "KillMode"), ["process"]);
    assert_eq!(values(&unit, "RestartPreventExitStatus"), ["78"]);
    // Ordered after the control plane on a primary, but never bound to it: a
    // node keeps running while the control plane is down (ADR 0011).
    assert!(
        values(&unit, "After")
            .iter()
            .any(|after| after.split_whitespace().any(|u| u == "quoro.service"))
    );
    assert!(values(&unit, "Requires").is_empty());
    assert!(values(&unit, "BindsTo").is_empty());
    assert!(values(&unit, "PartOf").is_empty());
}

/// The capability sets of a process, from `/proc/<pid>/status`.
#[cfg(target_os = "linux")]
fn capabilities(pid: u32) -> Vec<(String, u64)> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("read status");
    status
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.starts_with("Cap").then(|| {
                let value = u64::from_str_radix(value.trim(), 16).expect("a hex capability set");
                (key.to_string(), value)
            })
        })
        .collect()
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_running_control_plane_holds_no_capabilities() {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt as _, BufReader};

    // Root holds every capability by definition; the unit is what drops them
    // there, and the unit test above covers it. Elsewhere the process must not
    // gain any.
    let own = capabilities(std::process::id());
    if own
        .iter()
        .any(|(key, value)| key == "CapEff" && *value != 0)
    {
        eprintln!("skipped: the test runner itself holds capabilities: {own:?}");
        return;
    }

    let dir = TempDir::new();
    let config = dir.0.join("quoro.toml");
    std::fs::write(
        &config,
        format!(
            "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\n\n[node]\ndata_dir = \"{}\"\nlocal = false\n",
            dir.0.join("cp").display(),
            dir.0.join("node").display(),
        ),
    )
    .expect("write quoro.toml");

    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");
    let pid = server.id().expect("a pid");
    let mut lines = BufReader::new(server.stderr.take().expect("stderr")).lines();
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains("listening on https://") {
                return;
            }
        }
        panic!("quoro serve exited before it listened");
    })
    .await
    .expect("quoro serve listens within 30s");

    for (key, value) in capabilities(pid) {
        if matches!(key.as_str(), "CapInh" | "CapPrm" | "CapEff" | "CapAmb") {
            assert_eq!(value, 0, "{key} of quoro serve is {value:#x}");
        }
    }

    server.kill().await.expect("stop the server");
}

/// A data directory that is removed when the test ends.
#[cfg(target_os = "linux")]
struct TempDir(std::path::PathBuf);

#[cfg(target_os = "linux")]
impl TempDir {
    fn new() -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("quoro-privileges-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create the temporary directory");
        Self(path)
    }
}

#[cfg(target_os = "linux")]
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

//! The node role: `quoro join`, `quoro node` and the primary's own local node.
//!
//! Every machine that runs VMs runs the node agent (`quoro-node-agent`). It
//! dials the primary over the pinned TLS session and reconciles the signed
//! desired state it stores locally, so its VMs keep running and restart while
//! the control plane is unreachable (ADR 0011). The primary runs one too and
//! reaches its own listener through the same enrollment and session as any
//! other node, so there is one code path for every node.
//!
//! The engine is chosen here, with a `match` over [`EngineKind`], because the
//! `Engine` trait is not object safe. The Cloud Hypervisor arm refuses to run
//! on a host that cannot run guests and exits with [`EX_CONFIG`], so systemd
//! stops restarting a node that will never boot.

use crate::config::{Config, EngineKind};
use quoro_engine::{Engine, FakeEngine};
use quoro_node_agent::{AgentConfig, JoinTarget, NodeFacts};
use quoro_proto::wire::{Capacity, Kvm};
use std::future::Future;
use std::path::Path;
use tracing::{info, warn};

/// The exit status of a configuration the process cannot recover from
/// (`EX_CONFIG`, sysexits.h).
///
/// The systemd unit turns this into `RestartPreventExitStatus=78`, so a node
/// without KVM or with missing artifacts stops instead of looping.
pub const EX_CONFIG: i32 = 78;

/// The file the control plane writes for the primary's own node agent: the
/// loopback URL, a one-time token and the certificate fingerprint.
pub const LOCAL_JOIN_FILE: &str = "local-node-join.json";

/// The node agent's configuration from `quoro.toml`.
fn agent_config(config: &Config) -> AgentConfig {
    AgentConfig {
        data_dir: config.node.data_dir.clone(),
        node_name: config.node_name.clone(),
        bridge: config.cloud_hypervisor.bridge.clone(),
        dns: config.cloud_hypervisor.dns(),
        roles: vec!["node".to_string()],
        cloudflared_binary: config
            .ingress
            .cloudflared_binary(&config.cloud_hypervisor.artifacts_dir),
        cloudflared_metrics: config.ingress.cloudflared_metrics,
        acme_directory_url: config.ingress.acme_directory_url.clone(),
        acme_contact: config.ingress.acme_contact.clone(),
    }
}

/// `quoro join <url> --token --fingerprint`: enrolls this machine and writes
/// its identity, without starting anything. `quoro node` (or its systemd unit)
/// then runs the agent. Returns the process exit code.
pub async fn join(config: &Config, target: &JoinTarget) -> i32 {
    let agent = agent_config(config);
    if quoro_node_agent::is_enrolled(&agent.data_dir) {
        eprintln!(
            "this machine is already enrolled ({}); remove the node and its data directory to join again",
            agent.data_dir.display()
        );
        return 1;
    }
    let facts = match facts_for(config).await {
        Ok(facts) => facts,
        Err(code) => return code,
    };
    match quoro_node_agent::join(&agent, target, &facts).await {
        Ok(identity) => {
            println!("enrolled as {}", identity.node_id);
            0
        }
        Err(e) => {
            eprintln!("quoro join: {e}");
            1
        }
    }
}

/// `quoro node`: runs the node agent until `shutdown` resolves.
///
/// A primary's own agent that is not enrolled yet enrolls first with the
/// control plane's local join file, which lives in the control plane's data
/// directory; any other machine must have run `quoro join`.
///
/// # Errors
///
/// A message for the operator; refusals to run on this host are returned as
/// `Ok(EX_CONFIG)`.
pub async fn run(
    config: &Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    match config.engine {
        EngineKind::Fake => run_with(config, FakeEngine::new(), shutdown).await,
        EngineKind::CloudHypervisor => run_cloud_hypervisor(config, shutdown).await,
    }
}

#[cfg(target_os = "linux")]
async fn run_cloud_hypervisor(
    config: &Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    match cloud_hypervisor_engine(config).await {
        Ok(engine) => run_with(config, engine, shutdown).await,
        Err(code) => Ok(code),
    }
}

#[cfg(not(target_os = "linux"))]
async fn run_cloud_hypervisor(
    _config: &Config,
    _shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    eprintln!("the cloud-hypervisor engine runs on Linux only");
    Ok(EX_CONFIG)
}

/// Builds the Cloud Hypervisor backend and checks the host, or returns the
/// exit code of the refusal.
///
/// The host check is the engine's own `probe_host`, not a second copy of it:
/// the backend already knows where KVM, the CPU flags and the kernel release
/// live, and a duplicated check would drift from it.
#[cfg(target_os = "linux")]
async fn cloud_hypervisor_engine(config: &Config) -> Result<quoro_engine::ch::ChEngine, i32> {
    use quoro_engine::ch::{ChConfig, ChEngine};

    let ch = &config.cloud_hypervisor;
    let ch_config = ChConfig::new(
        ch.state_dir(&config.node.data_dir),
        ch.artifacts_dir.clone(),
        ch.init_binary.clone(),
    );
    // A pin mismatch, a missing `nft` or a broken artifact lands here; the
    // engine's message already names the cause, so it is passed through.
    let engine = match ChEngine::new(ch_config).await {
        Ok(engine) => engine,
        Err(e) => {
            eprintln!("cannot start the cloud-hypervisor engine: {e}");
            return Err(EX_CONFIG);
        }
    };
    let capabilities = engine.capabilities().await;
    info!(version = %capabilities.version, "cloud-hypervisor engine ready");

    let probe = match engine.probe_host().await {
        Ok(probe) => probe,
        Err(e) => {
            eprintln!("cannot probe this host for the cloud-hypervisor engine: {e}");
            return Err(EX_CONFIG);
        }
    };
    if !probe.kvm_available {
        eprintln!(
            "/dev/kvm is missing or not accessible: the cloud-hypervisor engine needs KVM (ADR 0009)"
        );
        return Err(EX_CONFIG);
    }
    if !(probe.cpu_vmx || probe.cpu_svm) {
        eprintln!("the CPU exposes neither vmx nor svm: enable hardware or nested virtualization");
        return Err(EX_CONFIG);
    }
    if !probe.kernel_supported {
        eprintln!(
            "kernel {} is older than the engine's supported minimum (6.8)",
            probe.kernel_release
        );
        return Err(EX_CONFIG);
    }
    Ok(engine)
}

/// Enrolls through the local join file when needed, then runs the agent.
async fn run_with<E: Engine + 'static>(
    config: &Config,
    engine: E,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    let agent = agent_config(config);
    let facts = facts_of(&engine).await;
    let dns = &agent.dns;
    if dns.is_empty() {
        // The node still runs; guests just cannot resolve names.
        warn!("no DNS server found: guests get an empty resolv.conf and cannot resolve names");
    } else {
        let servers = dns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        info!(servers = %servers, "guest DNS servers");
    }

    if !quoro_node_agent::is_enrolled(&agent.data_dir) {
        let local = config.data_dir.join(LOCAL_JOIN_FILE);
        let target = wait_for_local_join(&local).await?;
        let identity = quoro_node_agent::join(&agent, &target, &facts)
            .await
            .map_err(|e| format!("cannot enroll the local node: {e}"))?;
        info!(node_id = %identity.node_id, "local node enrolled");
    }

    match quoro_node_agent::run(agent, engine, facts, shutdown).await {
        Ok(()) => Ok(0),
        // A rejected credential or a removed node never heals by redialing:
        // exit like a configuration refusal so systemd stops restarting.
        Err(e) if e.is_fatal() => {
            eprintln!("the node agent stopped: {e}; re-enroll this machine to rejoin");
            Ok(EX_CONFIG)
        }
        Err(e) => Err(format!("the node agent stopped: {e}")),
    }
}

/// Reads the control plane's local join file, waiting for it to appear while
/// the control plane starts. A machine that is not a primary never gets one,
/// so the wait is bounded and ends with the instruction to run `quoro join`.
async fn wait_for_local_join(path: &Path) -> Result<JoinTarget, String> {
    for _ in 0..60 {
        match std::fs::read_to_string(path) {
            Ok(text) => return parse_local_join(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
        }
    }
    Err(format!(
        "this node is not enrolled: run 'quoro join <url> --token <token> --fingerprint <fingerprint>' (no local join file at {})",
        path.display()
    ))
}

/// `{"url", "token", "fingerprint"}`.
fn parse_local_join(text: &str) -> Result<JoinTarget, String> {
    #[derive(serde::Deserialize)]
    struct LocalJoin {
        url: String,
        token: String,
        fingerprint: String,
    }
    let local: LocalJoin =
        serde_json::from_str(text).map_err(|e| format!("the local join file is malformed: {e}"))?;
    Ok(JoinTarget {
        url: local.url,
        token: local.token,
        fingerprint: local.fingerprint,
    })
}

/// The facts `quoro join` reports, from the engine the node will run.
async fn facts_for(config: &Config) -> Result<NodeFacts, i32> {
    match config.engine {
        EngineKind::Fake => Ok(facts_of(&FakeEngine::new()).await),
        #[cfg(target_os = "linux")]
        EngineKind::CloudHypervisor => {
            let engine = cloud_hypervisor_engine(config).await?;
            Ok(facts_of(&engine).await)
        }
        #[cfg(not(target_os = "linux"))]
        EngineKind::CloudHypervisor => {
            eprintln!("the cloud-hypervisor engine runs on Linux only");
            Err(EX_CONFIG)
        }
    }
}

/// Capacity, KVM probe and versions of this machine.
async fn facts_of<E: Engine>(engine: &E) -> NodeFacts {
    let capabilities = engine.capabilities().await;
    let probe = match engine.probe_host().await {
        Ok(probe) => Some(probe),
        Err(e) => {
            warn!(error = ?e, "host probe failed; reporting KVM as unavailable");
            None
        }
    };
    NodeFacts {
        capacity: Capacity {
            cpus: std::thread::available_parallelism()
                .map(|n| u32::try_from(n.get()).unwrap_or(u32::MAX))
                .unwrap_or(1),
            memory_mb: memory_mb(),
            // The free-space probe needs `statvfs`, which the standard library
            // does not expose; a later milestone reports the real figure.
            disk_gb: 0,
        },
        kvm: Kvm {
            available: probe.as_ref().is_some_and(|probe| probe.kvm_available),
            kernel: probe
                .as_ref()
                .map(|probe| probe.kernel_release.clone())
                .unwrap_or_default(),
            nested: probe.map(|probe| probe.nested),
        },
        engine_version: capabilities.version,
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

/// MemTotal from `/proc/meminfo` in mebibytes, or 0 when it is not readable.
fn memory_mb() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .unwrap_or(0)
                / 1024;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_local_join_file_is_read() {
        let target = parse_local_join(
            r#"{"url":"https://127.0.0.1:8443","token":"njt_x","fingerprint":"sha256:ab"}"#,
        )
        .unwrap();
        assert_eq!(target.url, "https://127.0.0.1:8443");
        assert_eq!(target.token, "njt_x");
        assert_eq!(target.fingerprint, "sha256:ab");
        assert!(parse_local_join("{}").is_err());
    }
}

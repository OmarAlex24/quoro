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
//! stops restarting a node that will never boot. An edge-only machine (the
//! role persisted by `quoro join --role edge`) runs [`NullEngine`] instead,
//! which starts no VM and needs no KVM (ADR 0143).

use crate::config::{Config, EngineKind};
use crate::null_engine::NullEngine;
use quoro_engine::{Engine, FakeEngine};
use quoro_node_agent::{AgentConfig, JoinTarget, NodeFacts};
use quoro_proto::wire::{Capacity, Kvm};
use std::fmt;
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

/// The file under the node data directory that records the role this machine
/// enrolled with. It holds one word, `node` or `edge`, is written `0600` like
/// the other identity files, and is absent on installs from before edge
/// existed, which means [`Role::Node`].
pub const ROLE_FILE: &str = "role";

/// The role a machine took when it enrolled (ADR 0143).
///
/// A `node` runs microVMs and needs KVM; an `edge` runs the proxy and the mesh
/// only, so it runs the agent without an engine. A machine may hold both roles
/// in the protocol, but `quoro join` records one, and a machine that runs VMs
/// enrolls as a node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Role {
    /// Runs microVMs for the primary; needs KVM and Cloud Hypervisor.
    #[default]
    Node,
    /// Runs the edge proxy and the mesh only; holds no engine.
    Edge,
}

impl Role {
    /// Reads `node` or `edge`; anything else is a usage error.
    ///
    /// # Errors
    ///
    /// A message naming the accepted values when `text` is neither.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "node" => Ok(Self::Node),
            "edge" => Ok(Self::Edge),
            other => Err(format!(
                "unknown role '{other}'; expected node or edge (a primary runs 'quoro serve' instead)"
            )),
        }
    }

    /// The word the role file and the enrollment request carry.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }

    /// The roles sent in the enrollment request (section 2.3).
    #[must_use]
    pub fn enrollment_roles(self) -> Vec<String> {
        vec![self.as_str().to_string()]
    }

    /// Whether this machine runs guests. Only a node does.
    #[must_use]
    pub const fn runs_vms(self) -> bool {
        matches!(self, Self::Node)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The role this machine enrolled with; a missing or unreadable file means
/// [`Role::Node`], so installs from before edge existed keep working.
#[must_use]
pub fn read_role(data_dir: &Path) -> Role {
    let path = data_dir.join(ROLE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => match Role::parse(text.trim()) {
            Ok(role) => role,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "unknown role; running as node");
                Role::Node
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Role::Node,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot read the role; running as node");
            Role::Node
        }
    }
}

/// Records the role `quoro join` enrolled with, `0600` next to the identity.
///
/// # Errors
///
/// A message for the operator when the file cannot be written.
pub fn write_role(data_dir: &Path, role: Role) -> Result<(), String> {
    quoro_node_agent::identity::write_private(data_dir, ROLE_FILE, role.as_str().as_bytes())
        .map_err(|e| format!("cannot write the role in {}: {e}", data_dir.display()))
}

/// The node agent's configuration from `quoro.toml`, with the roles this
/// machine enrolled with.
fn agent_config(config: &Config, role: Role) -> AgentConfig {
    AgentConfig {
        data_dir: config.node.data_dir.clone(),
        node_name: config.node_name.clone(),
        bridge: config.cloud_hypervisor.bridge.clone(),
        dns: config.cloud_hypervisor.dns(),
        roles: role.enrollment_roles(),
        cloudflared_binary: config
            .ingress
            .cloudflared_binary(&config.cloud_hypervisor.artifacts_dir),
        cloudflared_metrics: config.ingress.cloudflared_metrics,
        acme_directory_url: config.ingress.acme_directory_url.clone(),
        acme_contact: config.ingress.acme_contact.clone(),
    }
}

/// `quoro join <url> --token --fingerprint [--role node|edge]`: enrolls this
/// machine and writes its identity and role, without starting anything.
/// `quoro node` (or its systemd unit) then runs the agent. Returns the process
/// exit code.
pub async fn join(config: &Config, target: &JoinTarget, role: Role) -> i32 {
    let agent = agent_config(config, role);
    if quoro_node_agent::is_enrolled(&agent.data_dir) {
        eprintln!(
            "this machine is already enrolled ({}); remove the node and its data directory to join again",
            agent.data_dir.display()
        );
        return 1;
    }
    let facts = match join_facts(config, role).await {
        Ok(facts) => facts,
        Err(code) => return code,
    };
    match quoro_node_agent::join(&agent, target, &facts).await {
        Ok(identity) => {
            // The role is written after the enrollment, so a refused join
            // leaves no role behind for `quoro node` to act on.
            if let Err(e) = write_role(&agent.data_dir, role) {
                eprintln!("quoro join: {e}");
                return 1;
            }
            println!("enrolled as {} ({role})", identity.node_id);
            0
        }
        Err(e) => {
            eprintln!("quoro join: {e}");
            1
        }
    }
}

/// `quoro node set-primary <url>`: points an enrolled node at its primary's
/// new address (ADR 0139, layer 1). Returns the process exit code.
///
/// The order matters. The new address is proved first, with the pin stored at
/// enrollment; only then is a running agent stopped, the identity rewritten,
/// and the agent started again. The running agent holds the identity in
/// memory and writes it back on a key rotation or when it learns new
/// addresses, which would overwrite the new `primary_url`. `KillMode=process`
/// on the unit keeps the microVMs running across the stop.
pub async fn set_primary(config: &Config, url: &str, restart: bool) -> i32 {
    let data_dir = config.node.data_dir.clone();
    if let Err(e) = std::fs::read_dir(&data_dir) {
        eprintln!(
            "quoro node set-primary: cannot read {}: {e}; run this as root",
            data_dir.display()
        );
        return 1;
    }

    let (identity, primary_url) =
        match quoro_node_agent::set_primary::verify_primary(&data_dir, url).await {
            Ok(result) => result,
            Err(e) => {
                let text = e.to_string();
                if text.contains("ermission denied") {
                    eprintln!("quoro node set-primary: {text}; run this as root");
                } else {
                    eprintln!("quoro node set-primary: {text}");
                }
                return 1;
            }
        };
    let previous = identity.primary_url;
    if previous == primary_url {
        println!("primary address is already {primary_url}; nothing was changed");
        return 0;
    }

    let mut stopped = false;
    if restart && unit_active() {
        if let Err(e) = systemctl("stop") {
            eprintln!(
                "quoro node set-primary: cannot stop quoro-node.service: {e}; nothing was changed"
            );
            return 1;
        }
        stopped = true;
    }

    let written = quoro_node_agent::set_primary::write_primary(&data_dir, &primary_url);
    match &written {
        Ok(_) => println!("primary address {previous} -> {primary_url}"),
        Err(e) => eprintln!(
            "quoro node set-primary: {e}; {} must be writable by root",
            data_dir.display()
        ),
    }

    // Start the agent again whenever it was stopped, including after a failed
    // write: leaving it down would take the node offline for no gain.
    if stopped {
        match systemctl("start") {
            Ok(()) => println!("restarted quoro-node.service"),
            Err(e) => {
                eprintln!("quoro node set-primary: cannot start quoro-node.service: {e}");
                return 1;
            }
        }
    } else if written.is_ok() {
        if restart {
            println!("quoro-node.service was not running; start it to use the new address");
        } else {
            println!("restart quoro-node.service to use the new address");
        }
    }

    if written.is_ok() { 0 } else { 1 }
}

/// Whether `quoro-node.service` is running, so a stop would actually do
/// something.
fn unit_active() -> bool {
    std::process::Command::new("systemctl")
        .args(["is-active", "--quiet", "quoro-node.service"])
        .status()
        .is_ok_and(|status| status.success())
}

/// Runs `systemctl <action> quoro-node.service`.
fn systemctl(action: &str) -> Result<(), String> {
    let status = std::process::Command::new("systemctl")
        .arg(action)
        .arg("quoro-node.service")
        .status()
        .map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("systemctl {action} quoro-node.service: {status}"))
    }
}

/// `quoro node`: runs the node agent until `shutdown` resolves.
///
/// A primary's own agent that is not enrolled yet enrolls first with the
/// control plane's local join file, which lives in the control plane's data
/// directory; any other machine must have run `quoro join`.
///
/// The role persisted at enrollment picks the engine. An edge-only machine
/// holds no engine at all, so [`NullEngine`] lets the agent run on a host
/// without KVM (ADR 0143); a node needs Cloud Hypervisor as before.
///
/// # Errors
///
/// A message for the operator; refusals to run on this host are returned as
/// `Ok(EX_CONFIG)`.
pub async fn run(
    config: &Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    let role = read_role(&config.node.data_dir);
    if !role.runs_vms() {
        info!("running as {role} without an engine: this machine starts no VMs");
        return run_with(config, role, NullEngine::new(), shutdown).await;
    }
    match config.engine {
        EngineKind::Fake => run_with(config, role, FakeEngine::new(), shutdown).await,
        EngineKind::CloudHypervisor => run_cloud_hypervisor(config, role, shutdown).await,
    }
}

#[cfg(target_os = "linux")]
async fn run_cloud_hypervisor(
    config: &Config,
    role: Role,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    match cloud_hypervisor_engine(config).await {
        Ok(engine) => run_with(config, role, engine, shutdown).await,
        Err(code) => Ok(code),
    }
}

#[cfg(not(target_os = "linux"))]
async fn run_cloud_hypervisor(
    _config: &Config,
    _role: Role,
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
    let mut ch_config = ChConfig::new(
        ch.state_dir(&config.node.data_dir),
        ch.artifacts_dir.clone(),
        ch.init_binary.clone(),
    );
    // The engine removes the snapshots left in its volume directory when it
    // starts, except those the node agent's store records: a warm base or a
    // quarantined copy outlives the agent (engine-trait.md section 3.16).
    ch_config.snapshot_sweep = quoro_node_agent::kept::sweep_rule(&config.node.data_dir).await;
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
    role: Role,
    engine: E,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<i32, String> {
    let agent = agent_config(config, role);
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

/// The facts `quoro join` reports, from the engine the machine will run.
async fn join_facts(config: &Config, role: Role) -> Result<NodeFacts, i32> {
    if !role.runs_vms() {
        // An edge enrolls without KVM and without an engine; the probe still
        // reports /dev/kvm as it finds it (section 2.3).
        return Ok(facts_of(&NullEngine::new()).await);
    }
    facts_for(config).await
}

/// The facts `quoro join` reports for a node, from the engine it will run.
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

    #[test]
    fn roles_parse_and_refuse_anything_else() {
        assert_eq!(Role::default(), Role::Node, "an unasked machine is a node");
        assert_eq!(Role::parse("node"), Ok(Role::Node));
        assert_eq!(Role::parse("edge"), Ok(Role::Edge));
        for bad in ["primary", "witness", "Edge", ""] {
            let err = Role::parse(bad).expect_err("only node and edge are join roles");
            assert!(err.contains("expected node or edge"), "{err}");
        }
        assert_eq!(Role::Edge.enrollment_roles(), vec!["edge".to_string()]);
        assert_eq!(Role::Node.enrollment_roles(), vec!["node".to_string()]);
        assert!(!Role::Edge.runs_vms());
        assert!(Role::Node.runs_vms());
    }

    #[test]
    fn the_persisted_role_round_trips_and_missing_means_node() {
        let dir = test_dir("role-round-trip");
        assert_eq!(
            read_role(&dir),
            Role::Node,
            "a data directory without a role file is a node"
        );

        write_role(&dir, Role::Edge).unwrap();
        assert_eq!(read_role(&dir), Role::Edge);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.join(ROLE_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "the role is private like the identity");
        }

        write_role(&dir, Role::Node).unwrap();
        assert_eq!(read_role(&dir), Role::Node);

        std::fs::write(dir.join(ROLE_FILE), "nonsense").unwrap();
        assert_eq!(
            read_role(&dir),
            Role::Node,
            "an unreadable value falls back to the safe role"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A fresh directory for one test, named after it.
    fn test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("quoro-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}

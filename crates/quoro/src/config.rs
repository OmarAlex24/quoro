//! The binary's configuration file (D-0054).
//!
//! `/etc/quoro/quoro.toml` is TOML with four optional keys and one optional
//! `[cloud_hypervisor]` table; an absent file (or an absent key) means the
//! defaults below, so a fresh host boots with `quoro serve` alone. Unknown
//! keys are refused instead of silently ignored, so a typo in a key name
//! cannot quietly fall back to a default that writes to the wrong place. The
//! same holds inside the table: the backend reads paths from there and a
//! misspelled key must not leave a guest kernel behind.
//!
//! Two defaults cannot be filled in while the file is parsed: the engine's
//! state directory sits below `data_dir`, and the guest DNS servers come from
//! the host's resolver configuration. Both are resolved on demand through
//! [`CloudHypervisorConfig::state_dir`] and [`CloudHypervisorConfig::dns`].
//!
//! The Cloud Hypervisor arm of `serve` is Linux-only while this table is
//! parsed on every platform (so `quoro setup-token` and the tests agree on the
//! file's shape); on another platform only the network part is read, so the
//! items the arm alone uses are allowed to go unread there rather than being
//! compiled out of the parser.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use serde::Deserialize;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

/// Where the installer puts the configuration (D-0054).
pub const DEFAULT_PATH: &str = "/etc/quoro/quoro.toml";

/// Where the installer puts the database, the TLS material and the token.
pub const DEFAULT_DATA_DIR: &str = "/var/lib/quoro";

/// The node agent's own data directory: its identity, local store and the
/// engine state. A sibling of the control plane's, never inside it, so the
/// two can belong to different users (ADR 0089).
pub const DEFAULT_NODE_DATA_DIR: &str = "/var/lib/quoro-node";

/// The one HTTPS port of D-0054.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:8443";

/// Where the pinned Cloud Hypervisor artifacts live (`cloud-hypervisor-static`
/// and `vmlinux-x86_64` under their pinned names).
pub const DEFAULT_ARTIFACTS_DIR: &str = "/usr/local/lib/quoro";

/// The static `quoro-init` binary that goes into the guest initramfs.
pub const DEFAULT_INIT_BINARY: &str = "/usr/local/lib/quoro/quoro-init";

/// The bridge every VM is attached to.
pub const DEFAULT_BRIDGE: &str = "quoro0";

/// The node subnet: `.1` is the bridge address and VMs start at `.2`.
pub const DEFAULT_SUBNET: &str = "10.42.0.0/24";

/// The file glibc reads for the host's DNS servers.
const RESOLV_CONF: &str = "/etc/resolv.conf";

/// systemd-resolved's file that lists the real upstream servers instead of the
/// loopback stub `/etc/resolv.conf` usually points at.
const SYSTEMD_RESOLV_CONF: &str = "/run/systemd/resolve/resolv.conf";

/// Which engine the local node driver runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineKind {
    /// The in-memory engine: no hypervisor, used for development and tests.
    #[default]
    Fake,
    /// Cloud Hypervisor over KVM, the backend of a Linux node (ADR 0007).
    CloudHypervisor,
}

impl EngineKind {
    /// The name used in logs and in the configuration file.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::CloudHypervisor => "cloud-hypervisor",
        }
    }
}

/// An IPv4 subnet: a network address and a prefix length.
///
/// Only the shape the node network actually uses is modelled: one IPv4 CIDR
/// with host addresses below the prefix. Parsing refuses a host address such
/// as `10.42.0.5/24`, because a mistyped network would otherwise make the
/// driver hand out addresses outside the bridge's subnet and every VM would
/// fail to reach it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Subnet {
    network: Ipv4Addr,
    prefix: u8,
}

impl Subnet {
    /// Reads `text` as `network/prefix`.
    ///
    /// # Errors
    ///
    /// A message for the operator when `text` is not an IPv4 CIDR, its prefix
    /// is outside `/8`..`/30`, or its host bits are not zero.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (address, prefix) = text.split_once('/').ok_or_else(|| {
            format!("subnet {text:?} is not a CIDR; expected an address and a prefix, for example {DEFAULT_SUBNET}")
        })?;
        let address: Ipv4Addr = address
            .parse()
            .map_err(|e| format!("subnet {text:?} does not start with an IPv4 address: {e}"))?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|e| format!("subnet {text:?} has no numeric prefix after '/': {e}"))?;
        if !(8..=30).contains(&prefix) {
            return Err(format!(
                "subnet {text:?} has a /{prefix} prefix; the node subnet must be between /8 and /30 so it has room for guests and the bridge"
            ));
        }
        let mask = network_mask(prefix);
        let network = u32::from(address) & mask;
        if network != u32::from(address) {
            return Err(format!(
                "subnet {text:?} is not a network address; the host bits below /{prefix} must be zero (did you mean {}/{prefix}?)",
                Ipv4Addr::from(network)
            ));
        }
        Ok(Self {
            network: address,
            prefix,
        })
    }

    /// The node's own address on the bridge: the first host address.
    #[must_use]
    pub fn gateway(self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.network).saturating_add(1))
    }
}

impl Default for Subnet {
    fn default() -> Self {
        Self::parse(DEFAULT_SUBNET).expect("DEFAULT_SUBNET is a valid subnet")
    }
}

impl fmt::Display for Subnet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

impl<'de> Deserialize<'de> for Subnet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// The mask of a prefix length, with the prefix clamped to a shift that cannot
/// overflow.
fn network_mask(prefix: u8) -> u32 {
    let host_bits = 32 - u32::from(prefix.min(32));
    u32::MAX.checked_shl(host_bits).unwrap_or(0)
}

/// The node's network and the pinned artifacts the Cloud Hypervisor engine
/// needs.
///
/// The table is optional and every key inside it is optional, so a host that
/// only sets `engine = "cloud-hypervisor"` runs with the documented defaults.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CloudHypervisorConfig {
    /// Directory holding the pinned `cloud-hypervisor-static` and
    /// `vmlinux-x86_64` under their pinned file names.
    #[serde(default = "default_artifacts_dir")]
    pub artifacts_dir: PathBuf,
    /// The static `quoro-init` binary for the guest initramfs.
    #[serde(default = "default_init_binary")]
    pub init_binary: PathBuf,
    /// The engine's state root (`vms/`, `images/`, `boot/`). Absent means
    /// `<node data_dir>/engine`, which only the caller knows at this point.
    pub state_dir: Option<PathBuf>,
    /// The bridge every VM is attached to.
    #[serde(default = "default_bridge")]
    pub bridge: String,
    /// The node subnet; the gateway is its first host address.
    #[serde(default)]
    pub subnet: Subnet,
    /// DNS servers written into every guest's `resolv.conf`. Absent means the
    /// host's own upstream servers (see [`Self::dns`]); an empty list means
    /// guests get no resolver at all.
    pub dns: Option<Vec<Ipv4Addr>>,
}

impl Default for CloudHypervisorConfig {
    fn default() -> Self {
        Self {
            artifacts_dir: default_artifacts_dir(),
            init_binary: default_init_binary(),
            state_dir: None,
            bridge: default_bridge(),
            subnet: Subnet::default(),
            dns: None,
        }
    }
}

impl CloudHypervisorConfig {
    /// The engine's state root: `state_dir` when set, else
    /// `<node data_dir>/engine`.
    #[must_use]
    pub fn state_dir(&self, node_data_dir: &Path) -> PathBuf {
        self.state_dir
            .clone()
            .unwrap_or_else(|| node_data_dir.join("engine"))
    }

    /// The guest DNS servers: `dns` when set, else the host's upstream
    /// servers, else an empty list.
    ///
    /// This reads the host's resolver files on demand rather than during
    /// parsing so a unit test of the defaults never depends on the machine it
    /// runs on.
    #[must_use]
    pub fn dns(&self) -> Vec<Ipv4Addr> {
        self.dns.clone().unwrap_or_else(host_nameservers)
    }
}

/// The parsed `quoro.toml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address of the single HTTPS listener.
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    /// Database, TLS material and the setup token.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Engine the in-process node driver runs.
    #[serde(default)]
    pub engine: EngineKind,
    /// Name this node reports to the control plane.
    #[serde(default = "default_node_name")]
    pub node_name: String,
    /// The Cloud Hypervisor engine's node network and artifact paths.
    #[serde(default)]
    pub cloud_hypervisor: CloudHypervisorConfig,
    /// Base URL nodes dial and join commands carry, for example
    /// `https://primary.example.com:8443`. Unset: the `Host` of the request
    /// that created the join token.
    #[serde(default)]
    pub public_url: Option<String>,
    /// The node agent on this machine.
    #[serde(default)]
    pub node: NodeConfig,
}

/// The `[node]` table: the node agent on this machine.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Identity, local store and engine state of the node agent.
    pub data_dir: PathBuf,
    /// Whether `quoro serve` also runs this machine's node agent in its own
    /// process, dialing its own listener through the same session as any
    /// other node. `false` once the agent runs as its own service.
    pub local: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_NODE_DATA_DIR),
            local: true,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            engine: EngineKind::default(),
            node_name: default_node_name(),
            cloud_hypervisor: CloudHypervisorConfig::default(),
            public_url: None,
            node: NodeConfig::default(),
        }
    }
}

impl Config {
    /// Reads `path`; a missing file is the defaults (D-0054).
    ///
    /// # Errors
    ///
    /// An I/O error other than "not found", or a file that is not valid TOML
    /// for this shape.
    pub fn load(path: &Path) -> io::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{path:?}: {e}"))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }
}

/// The hostname from `/etc/hostname`, or `quoro` when it cannot be read.
fn default_node_name() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "quoro".to_string())
}

fn default_listen() -> SocketAddr {
    DEFAULT_LISTEN
        .parse()
        .expect("DEFAULT_LISTEN is a socket address")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DATA_DIR)
}

fn default_artifacts_dir() -> PathBuf {
    PathBuf::from(DEFAULT_ARTIFACTS_DIR)
}

fn default_init_binary() -> PathBuf {
    PathBuf::from(DEFAULT_INIT_BINARY)
}

fn default_bridge() -> String {
    DEFAULT_BRIDGE.to_string()
}

/// The nameservers in `text`, skipping loopback entries and IPv6.
///
/// A guest reaches the host's stub resolver at `127.0.0.53` only by accident;
/// handing that address to a VM gives it a resolver it cannot reach, so
/// loopback servers are dropped and the caller falls back to
/// systemd-resolved's upstream list.
#[must_use]
pub fn parse_nameservers(text: &str) -> Vec<Ipv4Addr> {
    let mut servers: Vec<Ipv4Addr> = Vec::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("nameserver") {
            continue;
        }
        let Some(server) = fields.next().and_then(|text| text.parse::<Ipv4Addr>().ok()) else {
            // An IPv6 server, a comment or a malformed line: not usable here.
            continue;
        };
        if server.is_loopback() || servers.contains(&server) {
            continue;
        }
        servers.push(server);
    }
    servers
}

/// The host's upstream nameservers, or an empty list when none can be read.
///
/// `/etc/resolv.conf` usually lists `127.0.0.53`, systemd-resolved's stub,
/// which [`parse_nameservers`] drops; the second file then holds the real
/// upstream servers.
fn host_nameservers() -> Vec<Ipv4Addr> {
    for path in [RESOLV_CONF, SYSTEMD_RESOLV_CONF] {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let servers = parse_nameservers(&text);
        if !servers.is_empty() {
            return servers;
        }
    }
    Vec::new()
}

/// Creates `path` (and parents) if needed and restricts it to the owner, as
/// `/var/lib/quoro` holds the database, the TLS key and the setup token.
///
/// # Errors
///
/// An I/O error from creating the directory or setting its mode.
pub fn ensure_data_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_overrides() {
        let config = Config::default();
        assert_eq!(config.listen.to_string(), DEFAULT_LISTEN);
        assert_eq!(config.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(config.engine, EngineKind::Fake);

        let parsed: Config = toml::from_str(
            "listen = \"127.0.0.1:9000\"\ndata_dir = \"/var/lib/quoro-test\"\nengine = \"cloud-hypervisor\"\nnode_name = \"n1\"\n",
        )
        .unwrap();
        assert_eq!(parsed.listen.to_string(), "127.0.0.1:9000");
        assert_eq!(parsed.data_dir, PathBuf::from("/var/lib/quoro-test"));
        assert_eq!(parsed.engine, EngineKind::CloudHypervisor);
        assert_eq!(parsed.node_name, "n1");
    }

    #[test]
    fn the_node_table_defaults_to_a_local_agent_beside_the_control_plane() {
        let config = Config::default();
        assert_eq!(config.node.data_dir, PathBuf::from(DEFAULT_NODE_DATA_DIR));
        assert!(config.node.local, "a single host runs its own node agent");
        assert!(config.public_url.is_none());

        let parsed: Config = toml::from_str(
            "public_url = \"https://primary.example:8443\"\n[node]\ndata_dir = \"/srv/node\"\nlocal = false\n",
        )
        .unwrap();
        assert_eq!(
            parsed.public_url.as_deref(),
            Some("https://primary.example:8443")
        );
        assert_eq!(parsed.node.data_dir, PathBuf::from("/srv/node"));
        assert!(!parsed.node.local);
        assert!(toml::from_str::<Config>("[node]\nbogus = 1\n").is_err());
    }

    #[test]
    fn cloud_hypervisor_defaults_are_the_documented_ones() {
        let config = Config::default();
        let ch = &config.cloud_hypervisor;
        assert_eq!(ch.artifacts_dir, PathBuf::from(DEFAULT_ARTIFACTS_DIR));
        assert_eq!(ch.init_binary, PathBuf::from(DEFAULT_INIT_BINARY));
        assert_eq!(ch.bridge, DEFAULT_BRIDGE);
        assert_eq!(ch.subnet.to_string(), DEFAULT_SUBNET);
        assert_eq!(ch.subnet.network, Ipv4Addr::new(10, 42, 0, 0));
        assert_eq!(ch.subnet.prefix, 24);
        assert_eq!(ch.subnet.gateway(), Ipv4Addr::new(10, 42, 0, 1));
        // Absent `state_dir` follows `data_dir`, which a table cannot know.
        assert_eq!(
            ch.state_dir(Path::new("/var/lib/quoro")),
            PathBuf::from("/var/lib/quoro/engine")
        );

        // An absent table is the same as an empty one.
        let parsed: Config = toml::from_str("").unwrap();
        assert_eq!(
            parsed.cloud_hypervisor.artifacts_dir, ch.artifacts_dir,
            "an absent table falls back to the defaults"
        );
    }

    #[test]
    fn a_full_cloud_hypervisor_table_is_read() {
        let parsed: Config = toml::from_str(
            r#"
engine = "cloud-hypervisor"

[cloud_hypervisor]
artifacts_dir = "/opt/quoro/artifacts"
init_binary = "/opt/quoro/bin/quoro-init"
state_dir = "/srv/quoro/engine"
bridge = "qbr9"
subnet = "10.9.8.0/29"
dns = ["9.9.9.9", "1.1.1.1"]
"#,
        )
        .unwrap();
        let ch = &parsed.cloud_hypervisor;
        assert_eq!(ch.artifacts_dir, PathBuf::from("/opt/quoro/artifacts"));
        assert_eq!(ch.init_binary, PathBuf::from("/opt/quoro/bin/quoro-init"));
        assert_eq!(
            ch.state_dir(Path::new("/var/lib/quoro")),
            PathBuf::from("/srv/quoro/engine")
        );
        assert_eq!(ch.bridge, "qbr9");
        assert_eq!(ch.subnet.to_string(), "10.9.8.0/29");
        assert_eq!(ch.subnet.gateway(), Ipv4Addr::new(10, 9, 8, 1));
        assert_eq!(
            ch.dns,
            Some(vec![Ipv4Addr::new(9, 9, 9, 9), Ipv4Addr::new(1, 1, 1, 1)])
        );
        assert_eq!(ch.dns(), ch.dns.clone().unwrap());
    }

    #[test]
    fn an_unknown_key_inside_the_table_is_refused() {
        let err = toml::from_str::<Config>("[cloud_hypervisor]\nbridg = \"quoro0\"\n")
            .expect_err("a misspelled key must not fall back to the default");
        assert!(err.to_string().contains("bridg"), "{err}");
    }

    #[test]
    fn a_bad_subnet_is_refused_at_load() {
        // A host address instead of a network address.
        let err = toml::from_str::<Config>("[cloud_hypervisor]\nsubnet = \"10.42.0.5/24\"\n")
            .expect_err("host bits must be zero");
        let message = err.to_string();
        assert!(message.contains("network address"), "{message}");
        assert!(message.contains("10.42.0.0/24"), "{message}");

        for bad in ["10.42.0.0/7", "10.42.0.0/31", "10.42.0.0/32", "nonsense"] {
            assert!(
                toml::from_str::<Config>(&format!("[cloud_hypervisor]\nsubnet = \"{bad}\"\n"))
                    .is_err(),
                "{bad} should be refused"
            );
        }
        assert_eq!(
            Subnet::parse("10.0.0.0/8").unwrap().to_string(),
            "10.0.0.0/8",
            "the prefixes at the edge of the range are accepted"
        );
        assert_eq!(
            Subnet::parse("10.0.0.0/30").unwrap().to_string(),
            "10.0.0.0/30"
        );
    }

    #[test]
    fn nameservers_skip_loopback_comments_and_ipv6() {
        let text = "\
# Generated by resolvconf
nameserver 127.0.0.53
nameserver 127.0.0.1
nameserver ::1
nameserver 9.9.9.9
nameserver 1.1.1.1 # upstream
search example.test
nameserver 9.9.9.9
";
        assert_eq!(
            parse_nameservers(text),
            vec![Ipv4Addr::new(9, 9, 9, 9), Ipv4Addr::new(1, 1, 1, 1)]
        );
        assert!(parse_nameservers("").is_empty());
        assert!(parse_nameservers("nameserver\n").is_empty());
        assert!(parse_nameservers("nameserver 127.0.0.53\n").is_empty());
    }

    #[test]
    fn unknown_keys_are_refused() {
        assert!(toml::from_str::<Config>("liste = \"127.0.0.1:9000\"\n").is_err());
        assert!(toml::from_str::<Config>("engine = \"firecracker\"\n").is_err());
    }

    #[test]
    fn missing_file_is_default() {
        let path = std::env::temp_dir().join("quoro-config-that-does-not-exist.toml");
        let config = Config::load(&path).unwrap();
        assert_eq!(config.listen.to_string(), DEFAULT_LISTEN);
    }
}

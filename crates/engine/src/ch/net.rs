// SPDX-License-Identifier: AGPL-3.0-only
//! Node networking: one Linux bridge per node network, one TAP per VM, and the
//! nftables rules that isolate the VMs from each other (architecture.md
//! section 9).
//!
//! Every VM on the bridge is isolated from every other VM on it: bridge port
//! isolation drops the L2 hairpin, and the `forward` chain drops bridge to
//! bridge for the L3 path. A VM still reaches the gateway (ICMP and replies)
//! and the internet through masquerade, and the host can still open
//! connections to a VM, because the ingress runs on the host.
//!
//! The bridge is never reconfigured silently: an existing bridge that does not
//! carry exactly the requested gateway address is `Conflict`
//! (engine-trait.md section 3.3).

use std::fmt::{self, Write as _};
use std::net::Ipv4Addr;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::EngineError;
use crate::ch::tools::{self, HostTools};
use crate::{NodeNetwork, VmId};

/// Where Linux exposes one directory per network interface.
const SYS_CLASS_NET: &str = "/sys/class/net";

/// The forwarding switch the bridge needs to route between its own ports and
/// the node's uplink.
const IP_FORWARD: &str = "/proc/sys/net/ipv4/ip_forward";

/// An IPv4 subnet, stored in its canonical form (host bits zero).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Subnet {
    /// The first address of the subnet, host bits zero.
    pub network: Ipv4Addr,
    /// The number of leading one bits, 8 to 30.
    pub prefix_len: u8,
}

impl Subnet {
    /// Parses `"10.42.1.0/24"`.
    ///
    /// Host bits must be zero and the prefix must be 8 to 30: a `/31` has no
    /// usable host address and Quoro's node subnets are never smaller than a
    /// `/30`.
    ///
    /// # Errors
    ///
    /// `Conflict("invalid subnet …")` for anything else.
    pub fn parse(cidr: &str) -> Result<Subnet, EngineError> {
        let invalid =
            |reason: &str| EngineError::Conflict(format!("invalid subnet {cidr}: {reason}"));
        let (address, prefix) = cidr
            .split_once('/')
            .ok_or_else(|| invalid("expected <address>/<prefix>"))?;
        let address: Ipv4Addr = address
            .parse()
            .map_err(|_| invalid("the address is not an IPv4 address"))?;
        let prefix_len: u8 = prefix
            .parse()
            .map_err(|_| invalid("the prefix is not a number"))?;
        if !(8..=30).contains(&prefix_len) {
            return Err(invalid("the prefix must be between 8 and 30"));
        }
        let network = u32::from(address) & mask(prefix_len);
        if network != u32::from(address) {
            return Err(invalid("the address has host bits set"));
        }
        Ok(Subnet {
            network: Ipv4Addr::from(network),
            prefix_len,
        })
    }

    /// True when `ip` is inside the subnet, network and broadcast included.
    #[must_use]
    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        u32::from(ip) & mask(self.prefix_len) == u32::from(self.network)
    }

    /// True for an address a host can use: inside the subnet, and neither the
    /// network nor the broadcast address.
    #[must_use]
    pub fn is_host(&self, ip: Ipv4Addr) -> bool {
        self.contains(ip) && ip != self.network && ip != self.broadcast()
    }

    /// The last address of the subnet.
    #[must_use]
    pub fn broadcast(&self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.network) | !mask(self.prefix_len))
    }
}

impl fmt::Display for Subnet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix_len)
    }
}

/// The network mask of a prefix, as a host-order word.
fn mask(prefix_len: u8) -> u32 {
    u32::MAX << (32 - prefix_len)
}

/// Validates and brings the node network to `net` (idempotent).
///
/// The bridge exists, carries `<gateway>/<prefix>`, is up, and has the node's
/// ruleset in place; `ip_forward` is on.
///
/// # Errors
///
/// `Conflict` when the bridge name or the subnet is malformed, when the
/// gateway is not a host address of the subnet, when an interface with the
/// bridge's name exists and is not a bridge, and when a bridge with that name
/// already carries a different address. `Io` when a host tool fails, `Timeout`
/// when one hangs.
pub async fn ensure_network(tools: &HostTools, net: &NodeNetwork) -> Result<Subnet, EngineError> {
    validate_bridge_name(&net.bridge)?;
    let subnet = Subnet::parse(&net.subnet)?;
    if !subnet.is_host(net.gateway) {
        return Err(EngineError::Conflict(format!(
            "gateway {} is not a host address of {subnet}",
            net.gateway
        )));
    }

    let ip = tools.ip.as_path();
    let bridge = net.bridge.as_str();
    let gateway_cidr = format!("{}/{}", net.gateway, subnet.prefix_len);

    if interface_exists(bridge) {
        if !is_bridge(bridge) {
            return Err(EngineError::Conflict(format!(
                "interface {bridge} exists and is not a bridge"
            )));
        }
        let output = tools::run(ip, &["-j", "-4", "addr", "show", "dev", bridge]).await?;
        let addresses = parse_addrs(&output)?;
        if addresses.is_empty() {
            tools::run(ip, &["addr", "add", &gateway_cidr, "dev", bridge]).await?;
        } else if addresses != [(net.gateway, subnet.prefix_len)] {
            return Err(EngineError::Conflict(format!(
                "bridge {bridge} already exists with {}; refusing to reconfigure",
                join_addrs(&addresses)
            )));
        }
    } else {
        tools::run(ip, &["link", "add", "name", bridge, "type", "bridge"]).await?;
        tools::run(ip, &["addr", "add", &gateway_cidr, "dev", bridge]).await?;
    }

    tools::run(ip, &["link", "set", "dev", bridge, "up"]).await?;
    tokio::fs::write(IP_FORWARD, "1")
        .await
        .map_err(|err| EngineError::Io(format!("{IP_FORWARD}: {err}")))?;
    tools::run_with_stdin(&tools.nft, &["-f", "-"], &ruleset(bridge, &subnet)).await?;
    Ok(subnet)
}

/// The nftables ruleset for one bridge, as `nft -f -` reads it.
///
/// The first two lines declare and then delete the table, so applying the file
/// replaces Quoro's rules for this bridge in one transaction instead of
/// stacking duplicates. A bridge added later starts from an empty table.
#[must_use]
pub fn ruleset(bridge: &str, subnet: &Subnet) -> String {
    let table = table_name(bridge);
    let mut rules = String::with_capacity(1024);
    let _ = writeln!(rules, "table ip {table} {{}}");
    let _ = writeln!(rules, "delete table ip {table}");
    let _ = writeln!(rules, "table ip {table} {{");
    let _ = writeln!(rules, "    chain forward {{");
    let _ = writeln!(
        rules,
        "        type filter hook forward priority filter - 1; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" oifname \"{bridge}\" drop"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" ip saddr != {subnet} drop"
    );
    let _ = writeln!(rules, "        iifname \"{bridge}\" accept");
    let _ = writeln!(
        rules,
        "        oifname \"{bridge}\" ct state established,related accept"
    );
    let _ = writeln!(rules, "        oifname \"{bridge}\" drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain input {{");
    let _ = writeln!(
        rules,
        "        type filter hook input priority filter - 1; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" ct state established,related accept"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" ip protocol icmp accept"
    );
    let _ = writeln!(rules, "        iifname \"{bridge}\" drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain postrouting {{");
    let _ = writeln!(
        rules,
        "        type nat hook postrouting priority srcnat; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        ip saddr {subnet} oifname != \"{bridge}\" masquerade"
    );
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "}}");
    rules
}

/// The nftables table that holds one bridge's rules.
fn table_name(bridge: &str) -> String {
    format!("quoro_{}", bridge.replace('-', "_"))
}

/// `"qt"` followed by the first twelve hex digits of the VM id's sha256.
///
/// Always fourteen characters of `[a-z0-9]`, which is a valid and collision-
/// resistant interface name (the kernel limit is fifteen).
#[must_use]
pub fn tap_name(id: &VmId) -> String {
    let mut hasher = Sha256::new();
    hasher.update(id.0.as_bytes());
    let digest = hasher.finalize();
    let mut name = String::with_capacity(14);
    name.push_str("qt");
    for byte in &digest[..6] {
        let _ = write!(name, "{byte:02x}");
    }
    name
}

/// Creates the VM's TAP device on the bridge, or repairs an existing one.
///
/// Idempotent: the four `ip` calls below are all "bring it to this state"
/// operations.
///
/// # Errors
///
/// `Conflict` when an interface with the TAP's name exists but is not a TAP
/// device; `Io`/`Timeout` from the host tools.
pub async fn create_tap(tools: &HostTools, tap: &str, bridge: &str) -> Result<(), EngineError> {
    let ip = tools.ip.as_path();
    if interface_exists(tap) {
        if !is_tap(tap) {
            return Err(EngineError::Conflict(format!(
                "interface {tap} exists and is not a TAP device"
            )));
        }
    } else {
        tools::run(ip, &["tuntap", "add", "dev", tap, "mode", "tap"]).await?;
    }

    // Port isolation is what keeps one VM's frames from reaching another VM on
    // the same bridge; the ruleset closes the routed path.
    tools::run(ip, &["link", "set", "dev", tap, "master", bridge]).await?;
    tools::run(
        ip,
        &[
            "link",
            "set",
            "dev",
            tap,
            "type",
            "bridge_slave",
            "isolated",
            "on",
        ],
    )
    .await?;
    tools::run(ip, &["link", "set", "dev", tap, "up"]).await?;
    Ok(())
}

/// Deletes the VM's TAP device. A TAP that is already gone is `Ok`.
///
/// # Errors
///
/// `Io`/`Timeout` when the host tool fails for another reason.
pub async fn delete_tap(tools: &HostTools, tap: &str) -> Result<(), EngineError> {
    if !interface_exists(tap) {
        return Ok(());
    }
    match tools::run(&tools.ip, &["link", "del", "dev", tap]).await {
        Ok(_) => Ok(()),
        // The TAP can disappear between the check above and the call.
        Err(EngineError::Io(message)) if message.contains("Cannot find device") => Ok(()),
        Err(err) => Err(err),
    }
}

/// True when `/sys/class/net/<name>` exists.
#[must_use]
pub fn interface_exists(name: &str) -> bool {
    sys_class_net(name).exists()
}

/// True when the interface is a bridge (it has a `bridge` sysfs directory).
fn is_bridge(name: &str) -> bool {
    sys_class_net(name).join("bridge").exists()
}

/// True when the interface is a TUN or TAP device (it has `tun_flags`).
fn is_tap(name: &str) -> bool {
    sys_class_net(name).join("tun_flags").exists()
}

/// The sysfs directory of one interface.
fn sys_class_net(name: &str) -> std::path::PathBuf {
    Path::new(SYS_CLASS_NET).join(name)
}

/// Test and teardown helper: removes the bridge's nftables table and then the
/// bridge itself. Anything already gone is `Ok`.
///
/// The engine never calls this; only tests and an explicit node teardown do.
///
/// # Errors
///
/// `Io`/`Timeout` when a host tool fails for another reason.
pub async fn remove_network(tools: &HostTools, bridge: &str) -> Result<(), EngineError> {
    let table = table_name(bridge);
    let table = table.as_str();
    let existing = tools::run(&tools.nft, &["list", "table", "ip", table]).await;
    if existing.is_ok() {
        tools::run(&tools.nft, &["delete", "table", "ip", table]).await?;
    }
    if interface_exists(bridge) {
        tools::run(&tools.ip, &["link", "del", "dev", bridge]).await?;
    }
    Ok(())
}

/// Validates a bridge name: one to fifteen characters of `[A-Za-z0-9_-]`, the
/// kernel's limit for an interface name and the character set that is safe in
/// both sysfs paths and an nftables table name.
fn validate_bridge_name(bridge: &str) -> Result<(), EngineError> {
    let valid = !bridge.is_empty()
        && bridge.len() <= 15
        && bridge
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(EngineError::Conflict(format!(
            "invalid bridge name {bridge:?}: expected 1 to 15 characters of [A-Za-z0-9_-]"
        )))
    }
}

/// Reads the IPv4 addresses out of `ip -j -4 addr show` output.
///
/// Kept separate from [`ensure_network`] so the JSON shape is unit-tested
/// without a network namespace.
fn parse_addrs(json: &str) -> Result<Vec<(Ipv4Addr, u8)>, EngineError> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|err| EngineError::Io(format!("ip -j addr show: not JSON: {err}")))?;
    let interfaces = value
        .as_array()
        .ok_or_else(|| EngineError::Io("ip -j addr show: expected a JSON array".to_owned()))?;

    let mut addresses = Vec::new();
    for interface in interfaces {
        let infos = interface
            .get("addr_info")
            .and_then(serde_json::Value::as_array);
        let Some(infos) = infos else {
            continue;
        };
        for info in infos {
            if info.get("family").and_then(serde_json::Value::as_str) != Some("inet") {
                continue;
            }
            let local = info
                .get("local")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    EngineError::Io("ip -j addr show: an inet entry without local".to_owned())
                })?;
            let ip: Ipv4Addr = local.parse().map_err(|_| {
                EngineError::Io(format!("ip -j addr show: invalid IPv4 address {local}"))
            })?;
            let prefix_len = info
                .get("prefixlen")
                .and_then(serde_json::Value::as_u64)
                .and_then(|len| u8::try_from(len).ok())
                .filter(|len| *len <= 32)
                .ok_or_else(|| {
                    EngineError::Io("ip -j addr show: an inet entry without a prefix".to_owned())
                })?;
            addresses.push((ip, prefix_len));
        }
    }
    Ok(addresses)
}

/// The addresses as `nft`-style CIDRs, for an error message.
fn join_addrs(addresses: &[(Ipv4Addr, u8)]) -> String {
    addresses
        .iter()
        .map(|(ip, prefix)| format!("{ip}/{prefix}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subnet(cidr: &str) -> Subnet {
        Subnet::parse(cidr).expect("valid subnet")
    }

    #[test]
    fn parse_accepts_a_canonical_cidr() {
        assert_eq!(
            subnet("10.42.1.0/24"),
            Subnet {
                network: Ipv4Addr::new(10, 42, 1, 0),
                prefix_len: 24,
            }
        );
        assert_eq!(subnet("10.42.1.0/24").to_string(), "10.42.1.0/24");
        assert_eq!(subnet("10.0.0.0/8").to_string(), "10.0.0.0/8");
        assert_eq!(subnet("192.168.4.0/30").to_string(), "192.168.4.0/30");
    }

    #[test]
    fn parse_rejects_host_bits_and_out_of_range_prefixes() {
        for cidr in [
            "10.42.1.5/24",
            "10.42.1.0/7",
            "10.42.1.0/0",
            "10.42.1.0/31",
            "10.42.1.0/32",
            "10.42.1.0",
            "10.42.1.0/",
            "10.42.1.0/255",
            "10.42.1.0/24/8",
            "not-an-address/24",
            "",
        ] {
            match Subnet::parse(cidr) {
                Err(EngineError::Conflict(message)) => {
                    assert!(message.starts_with("invalid subnet "), "{message}");
                }
                other => panic!("{cidr:?} parsed as {other:?}"),
            }
        }
    }

    #[test]
    fn contains_covers_the_whole_subnet() {
        let net = subnet("10.99.5.0/24");
        for ip in ["10.99.5.0", "10.99.5.1", "10.99.5.254", "10.99.5.255"] {
            assert!(net.contains(ip.parse().expect("ip")), "{ip}");
        }
        for ip in ["10.99.4.255", "10.99.6.0", "192.168.0.1"] {
            assert!(!net.contains(ip.parse().expect("ip")), "{ip}");
        }
    }

    #[test]
    fn is_host_excludes_network_and_broadcast() {
        let net = subnet("192.168.4.0/30");
        assert_eq!(net.broadcast(), Ipv4Addr::new(192, 168, 4, 3));
        assert!(!net.is_host(Ipv4Addr::new(192, 168, 4, 0)));
        assert!(!net.is_host(Ipv4Addr::new(192, 168, 4, 3)));
        assert!(net.is_host(Ipv4Addr::new(192, 168, 4, 1)));
        assert!(net.is_host(Ipv4Addr::new(192, 168, 4, 2)));
        assert!(!net.is_host(Ipv4Addr::new(192, 168, 5, 1)));

        let net = subnet("10.42.0.0/16");
        assert_eq!(net.broadcast(), Ipv4Addr::new(10, 42, 255, 255));
        assert!(!net.is_host(Ipv4Addr::new(10, 42, 0, 0)));
        assert!(!net.is_host(Ipv4Addr::new(10, 42, 255, 255)));
        assert!(net.is_host(Ipv4Addr::new(10, 42, 255, 254)));
    }

    #[test]
    fn tap_name_is_a_stable_fourteen_character_interface_name() {
        let id = VmId("vm_01".to_owned());
        // sha256("vm_01") = 1a70a4124929…
        assert_eq!(tap_name(&id), "qt1a70a4124929");
        assert_eq!(tap_name(&id).len(), 14);
        assert_ne!(tap_name(&VmId("vm_02".to_owned())), tap_name(&id));
        assert_ne!(
            tap_name(&VmId("vm_1".to_owned())),
            tap_name(&VmId("vm_1 ".to_owned()))
        );
        for name in [
            tap_name(&VmId("vm_a".to_owned())),
            tap_name(&VmId(String::new())),
        ] {
            assert_eq!(name.len(), 14);
            assert!(name.starts_with("qt"));
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            );
        }
    }

    #[test]
    fn ruleset_is_the_documented_text() {
        let expected = "\
table ip quoro_qw5br0 {}
delete table ip quoro_qw5br0
table ip quoro_qw5br0 {
    chain forward {
        type filter hook forward priority filter - 1; policy accept;
        iifname \"qw5br0\" oifname \"qw5br0\" drop
        iifname \"qw5br0\" ip saddr != 10.99.5.0/24 drop
        iifname \"qw5br0\" accept
        oifname \"qw5br0\" ct state established,related accept
        oifname \"qw5br0\" drop
    }
    chain input {
        type filter hook input priority filter - 1; policy accept;
        iifname \"qw5br0\" ct state established,related accept
        iifname \"qw5br0\" ip protocol icmp accept
        iifname \"qw5br0\" drop
    }
    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip saddr 10.99.5.0/24 oifname != \"qw5br0\" masquerade
    }
}
";
        assert_eq!(ruleset("qw5br0", &subnet("10.99.5.0/24")), expected);
    }

    #[test]
    fn ruleset_table_name_flattens_dashes() {
        let rules = ruleset("quoro-br-1", &subnet("10.99.5.0/24"));
        assert!(
            rules.starts_with("table ip quoro_quoro_br_1 {}\n"),
            "{rules}"
        );
        assert!(rules.contains("iifname \"quoro-br-1\" oifname \"quoro-br-1\" drop"));
        assert_eq!(rules.matches("delete table ip quoro_quoro_br_1").count(), 1);
    }

    #[test]
    fn parse_addrs_reads_the_inet_entries() {
        // `ip -j -4 addr show dev qw5br0`, with an IPv6 entry left in on
        // purpose: only `inet` addresses belong to the bridge check.
        let json = r#"[{"ifindex":7,"ifname":"qw5br0","flags":["BROADCAST","MULTICAST","UP"],
            "mtu":1500,"qdisc":"noqueue","operstate":"UP","linkmode":"DEFAULT","group":"default",
            "txqlen":1000,"link_type":"ether","address":"0a:1b:2c:3d:4e:5f","broadcast":"ff:ff:ff:ff:ff:ff",
            "addr_info":[
              {"family":"inet","local":"10.99.5.1","prefixlen":24,"broadcast":"10.99.5.255",
               "scope":"global","label":"qw5br0","valid_life_time":4294967295,"preferred_life_time":4294967295},
              {"family":"inet6","local":"fe80::81b:2cff:fe3d:4e5f","prefixlen":64,"scope":"link",
               "valid_life_time":4294967295,"preferred_life_time":4294967295}
            ]}]"#;
        assert_eq!(
            parse_addrs(json).expect("parses"),
            [(Ipv4Addr::new(10, 99, 5, 1), 24)]
        );
    }

    #[test]
    fn parse_addrs_handles_an_interface_without_addresses() {
        let json = r#"[{"ifindex":3,"ifname":"qw5br1","addr_info":[]}]"#;
        assert_eq!(parse_addrs(json).expect("parses"), []);
    }

    #[test]
    fn parse_addrs_rejects_malformed_json() {
        assert!(matches!(parse_addrs("{"), Err(EngineError::Io(_))));
        assert!(matches!(parse_addrs("{}"), Err(EngineError::Io(_))));
        let no_local = r#"[{"addr_info":[{"family":"inet","prefixlen":24}]}]"#;
        assert!(matches!(parse_addrs(no_local), Err(EngineError::Io(_))));
        let no_prefix = r#"[{"addr_info":[{"family":"inet","local":"10.0.0.1"}]}]"#;
        assert!(matches!(parse_addrs(no_prefix), Err(EngineError::Io(_))));
    }

    #[test]
    fn validate_bridge_name_enforces_the_kernel_rules() {
        for valid in ["qw5br0", "br0", "a", "A-Z_09", "quoro_net-1"] {
            assert!(validate_bridge_name(valid).is_ok(), "{valid}");
        }
        for invalid in ["", "sixteen-chars-xx", "br/0", "br 0", "br.0", "br:0"] {
            assert!(
                matches!(validate_bridge_name(invalid), Err(EngineError::Conflict(_))),
                "{invalid:?}"
            );
        }
    }

    fn fake_tools() -> HostTools {
        // Validation-only tests never reach a host tool.
        let path = std::path::PathBuf::from("/nonexistent/ip");
        HostTools {
            ip: path.clone(),
            nft: std::path::PathBuf::from("/nonexistent/nft"),
            mkfs_ext4: std::path::PathBuf::from("/nonexistent/mkfs.ext4"),
        }
    }

    fn network(bridge: &str, subnet: &str, gateway: Ipv4Addr) -> NodeNetwork {
        NodeNetwork {
            bridge: bridge.to_owned(),
            subnet: subnet.to_owned(),
            gateway,
            dns: Vec::new(),
        }
    }

    #[tokio::test]
    async fn ensure_network_rejects_bad_input_before_touching_the_host() {
        let tools = fake_tools();
        let cases = [
            network("bad/name", "10.99.5.0/24", Ipv4Addr::new(10, 99, 5, 1)),
            network("qw5br0", "10.99.5.0/48", Ipv4Addr::new(10, 99, 5, 1)),
            network("qw5br0", "10.99.5.0/24", Ipv4Addr::new(10, 99, 5, 0)),
            network("qw5br0", "10.99.5.0/24", Ipv4Addr::new(10, 99, 5, 255)),
            network("qw5br0", "10.99.5.0/24", Ipv4Addr::new(10, 99, 6, 1)),
        ];
        for case in cases {
            match ensure_network(&tools, &case).await {
                Err(EngineError::Conflict(message)) => assert!(!message.is_empty()),
                other => panic!("{:?} gave {other:?}", case),
            }
        }
    }

    // --- lab tests: need root and a real network namespace -------------------
    //
    // Run them with `crates/engine/scripts/lab-suite.sh --only net`; it builds
    // the test binaries as the normal user, sets `QUORO_ENGINE_LAB=1` and runs
    // the suite as root.

    fn lab_enabled() -> bool {
        std::env::var("QUORO_ENGINE_LAB").is_ok_and(|value| value == "1")
    }

    const LAB_BRIDGE: &str = "qw5br0";

    fn lab_network() -> NodeNetwork {
        network(LAB_BRIDGE, "10.99.5.0/24", Ipv4Addr::new(10, 99, 5, 1))
    }

    /// Every address the interface carries, as `(ip, prefix)`, via `ip -j`.
    async fn addrs_of(tools: &HostTools, interface: &str) -> Vec<(Ipv4Addr, u8)> {
        let out = tools::run(&tools.ip, &["-j", "-4", "addr", "show", "dev", interface])
            .await
            .expect("ip addr show");
        parse_addrs(&out).expect("parses")
    }

    #[tokio::test]
    #[ignore = "needs root: QUORO_ENGINE_LAB=1"]
    async fn lab_ensure_network_is_idempotent() {
        if !lab_enabled() {
            return;
        }
        let tools = HostTools::discover().expect("host tools");
        remove_network(&tools, LAB_BRIDGE)
            .await
            .expect("clean start");

        let subnet = ensure_network(&tools, &lab_network())
            .await
            .expect("first ensure");
        assert_eq!(subnet, self::subnet("10.99.5.0/24"));
        assert!(is_bridge(LAB_BRIDGE), "bridge was not created");

        ensure_network(&tools, &lab_network())
            .await
            .expect("second ensure");
        assert_eq!(
            addrs_of(&tools, LAB_BRIDGE).await,
            [(Ipv4Addr::new(10, 99, 5, 1), 24)]
        );

        let forward = std::fs::read_to_string(IP_FORWARD).expect("ip_forward");
        assert_eq!(forward.trim(), "1");

        // A different gateway on the same bridge name is a conflict, and the
        // address is left alone.
        let other = network(LAB_BRIDGE, "10.99.5.0/24", Ipv4Addr::new(10, 99, 5, 2));
        match ensure_network(&tools, &other).await {
            Err(EngineError::Conflict(message)) => {
                assert!(
                    message.contains("already exists with 10.99.5.1/24"),
                    "{message}"
                );
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
        // A different subnet is a conflict too.
        let other = network(LAB_BRIDGE, "10.99.6.0/24", Ipv4Addr::new(10, 99, 6, 1));
        assert!(matches!(
            ensure_network(&tools, &other).await,
            Err(EngineError::Conflict(_))
        ));
        assert_eq!(
            addrs_of(&tools, LAB_BRIDGE).await,
            [(Ipv4Addr::new(10, 99, 5, 1), 24)]
        );

        // An existing non-bridge interface is never moved out of the way.
        let lo = network("lo", "10.99.5.0/24", Ipv4Addr::new(10, 99, 5, 1));
        match ensure_network(&tools, &lo).await {
            Err(EngineError::Conflict(message)) => {
                assert!(message.contains("is not a bridge"), "{message}");
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        remove_network(&tools, LAB_BRIDGE).await.expect("teardown");
        assert!(!interface_exists(LAB_BRIDGE));
    }

    #[tokio::test]
    #[ignore = "needs root: QUORO_ENGINE_LAB=1"]
    async fn lab_tap_lifecycle() {
        if !lab_enabled() {
            return;
        }
        let tools = HostTools::discover().expect("host tools");
        remove_network(&tools, LAB_BRIDGE)
            .await
            .expect("clean start");
        ensure_network(&tools, &lab_network())
            .await
            .expect("ensure");
        let tap = tap_name(&VmId("vm_lab_tap".to_owned()));

        create_tap(&tools, &tap, LAB_BRIDGE)
            .await
            .expect("first create");
        assert!(is_tap(&tap), "{tap} is not a TAP device");
        create_tap(&tools, &tap, LAB_BRIDGE)
            .await
            .expect("second create");

        let details = tools::run(&tools.ip, &["-d", "link", "show", &tap])
            .await
            .expect("ip -d link show");
        assert!(
            details.contains(&format!("master {LAB_BRIDGE}")),
            "{details}"
        );
        assert!(details.contains("isolated on"), "{details}");
        assert!(details.contains("tap"), "{details}");

        // An existing interface that is not a TAP is a conflict.
        assert!(matches!(
            create_tap(&tools, "lo", LAB_BRIDGE).await,
            Err(EngineError::Conflict(_))
        ));

        delete_tap(&tools, &tap).await.expect("first delete");
        assert!(!interface_exists(&tap));
        delete_tap(&tools, &tap).await.expect("second delete");

        remove_network(&tools, LAB_BRIDGE).await.expect("teardown");
        assert!(!interface_exists(LAB_BRIDGE));
    }
}

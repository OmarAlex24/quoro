// SPDX-License-Identifier: AGPL-3.0-only
//! Node networking: one Linux bridge per node network, one TAP per VM, and the
//! nftables rules that isolate the VMs from each other (architecture.md
//! section 9).
//!
//! Every VM on the bridge is isolated from every other VM on it, except from
//! the VMs of its own isolation group (engine-trait.md section 3.3, ADR 0084).
//! Three layers enforce it:
//!
//! - a bridge-family `forward` chain accepts a frame between two TAP ports only
//!   when the `(in, out)` pair is in the bridge's `peers` set, which holds
//!   every ordered pair of TAPs that share a group, and drops every other
//!   port-to-port frame;
//! - bridge port isolation stays on for a VM without a group, so the kernel
//!   itself drops its frames to every other isolated port;
//! - the `ip` `forward` chain drops bridge to bridge for the routed path, except
//!   frames the bridge chain marked as same-group (that only matters when
//!   `br_netfilter` sends bridged traffic through the `ip` hooks).
//!
//! A VM still reaches the gateway (ICMP and replies) and the internet through
//! masquerade, and the host can still open connections to a VM, because the
//! ingress runs on the host.
//!
//! An egress-only VM (§3.3, ADR 0118) is stricter. Its TAP joins the bridge's
//! `egress` set, and the tuple of that TAP, its MAC address and the address the
//! spec assigned joins `egress_addrs`; a bridge-family `prerouting` chain drops
//! every frame from the TAP that is neither IPv4 nor ARP, drops a frame whose
//! Ethernet source, ARP sender hardware address, ARP sender address or IPv4
//! source address is not the VM's own, and marks the rest. The `ip` chains key
//! on that mark: input accepts DNS to the network's servers and drops
//! everything else from the VM, and forward drops link-local addresses and the
//! bridge's own subnet. The mark is set from the bridge port, never from the
//! source address, because the guest can spoof one inside the subnet; the
//! address and MAC checks stop it claiming a neighbour's address or MAC. Both
//! would send host-originated traffic for that neighbour to it: a spoofed
//! address redirects the neighbour table, and a spoofed Ethernet source moves
//! the neighbour's MAC to this port in the bridge's forwarding database, which
//! the frame reaches only after the bridge `prerouting` hook, so dropping the
//! frame there stops the bridge from learning it.
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
use crate::{MacAddr, NodeNetwork, VmId};

/// Where Linux exposes one directory per network interface.
const SYS_CLASS_NET: &str = "/sys/class/net";

/// The packet mark the bridge chain sets on a same-group frame, so the `ip`
/// `forward` chain lets it through when `br_netfilter` is loaded.
pub const PEER_MARK: u32 = 0x5155_0084;

/// The packet mark the bridge chain sets on a frame from an egress-only VM
/// (engine-trait.md section 3.3). The `ip` chains key on it, never on the
/// source address: the guest can spoof an address inside the subnet.
pub const EGRESS_MARK: u32 = 0x5155_0088;

/// The link-local range no egress-only VM may reach.
const LINK_LOCAL: &str = "169.254.0.0/16";

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
    tools::run_with_stdin(
        &tools.nft,
        &["-f", "-"],
        &ruleset(bridge, &subnet, &net.dns),
    )
    .await?;
    Ok(subnet)
}

/// The nftables ruleset for one bridge, as `nft -f -` reads it.
///
/// The first two lines declare and then delete the table, so applying the file
/// replaces Quoro's rules for this bridge in one transaction instead of
/// stacking duplicates. A bridge added later starts from an empty table.
///
/// `dns` is the `NodeNetwork::dns` list: the only host addresses an egress-only
/// VM reaches, on port 53.
#[must_use]
pub fn ruleset(bridge: &str, subnet: &Subnet, dns: &[Ipv4Addr]) -> String {
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
        "        iifname \"{bridge}\" oifname \"{bridge}\" meta mark {PEER_MARK:#010x} accept"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" oifname \"{bridge}\" drop"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" ip saddr != {subnet} drop"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" meta mark {EGRESS_MARK:#010x} ip daddr {LINK_LOCAL} drop"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" meta mark {EGRESS_MARK:#010x} ip daddr {subnet} drop"
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
    if !dns.is_empty() {
        let servers = dns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        for protocol in ["udp", "tcp"] {
            let _ = writeln!(
                rules,
                "        iifname \"{bridge}\" meta mark {EGRESS_MARK:#010x} ip daddr {{ {servers} }} {protocol} dport 53 accept"
            );
        }
    }
    // A marked packet that got this far is not DNS to a configured server; it
    // may not reach the host, ICMP included.
    let _ = writeln!(
        rules,
        "        iifname \"{bridge}\" meta mark {EGRESS_MARK:#010x} drop"
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
    let _ = writeln!(rules, "table bridge {table} {{}}");
    let _ = writeln!(rules, "delete table bridge {table}");
    let _ = writeln!(rules, "table bridge {table} {{");
    let _ = writeln!(rules, "    set peers {{");
    let _ = writeln!(rules, "        type ifname . ifname");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    set egress {{");
    let _ = writeln!(rules, "        type ifname");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    set egress_addrs {{");
    let _ = writeln!(rules, "        type ifname . ether_addr . ipv4_addr");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain prerouting {{");
    let _ = writeln!(
        rules,
        "        type filter hook prerouting priority filter; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        meta ibrname \"{bridge}\" iifname @egress ether type != ip ether type != arp drop"
    );
    // An egress-only VM may only speak for the address and the MAC its spec
    // assigned: a spoofed ARP, Ethernet source or source address would make
    // the host send a neighbour's traffic to it.
    let _ = writeln!(
        rules,
        "        meta ibrname \"{bridge}\" iifname @egress ether type arp iifname . ether saddr . arp saddr ip != @egress_addrs drop"
    );
    let _ = writeln!(
        rules,
        "        meta ibrname \"{bridge}\" iifname @egress ether type arp iifname . arp saddr ether . arp saddr ip != @egress_addrs drop"
    );
    let _ = writeln!(
        rules,
        "        meta ibrname \"{bridge}\" iifname @egress ether type ip iifname . ether saddr . ip saddr != @egress_addrs drop"
    );
    let _ = writeln!(
        rules,
        "        meta ibrname \"{bridge}\" iifname @egress meta mark set {EGRESS_MARK:#010x}"
    );
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain forward {{");
    let _ = writeln!(
        rules,
        "        type filter hook forward priority filter; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        meta ibrname \"{bridge}\" iifname . oifname @peers meta mark set {PEER_MARK:#010x} accept"
    );
    let _ = writeln!(rules, "        meta ibrname \"{bridge}\" drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "}}");
    rules
}

/// Every ordered pair of TAPs that share an isolation group, sorted.
///
/// `members` is `(tap, group)` for every VM on one bridge; a VM without a
/// group pairs with nobody.
#[must_use]
pub fn peer_pairs(members: &[(String, Option<String>)]) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for (tap, group) in members {
        let Some(group) = group else { continue };
        for (other, other_group) in members {
            if other != tap && other_group.as_ref() == Some(group) {
                pairs.push((tap.clone(), other.clone()));
            }
        }
    }
    pairs.sort();
    pairs.dedup();
    pairs
}

/// The `nft -f -` script that replaces the bridge's `peers` set with `pairs`,
/// in one transaction.
#[must_use]
pub fn peers_script(bridge: &str, pairs: &[(String, String)]) -> String {
    let table = table_name(bridge);
    let mut script = format!("flush set bridge {table} peers\n");
    if !pairs.is_empty() {
        let elements = pairs
            .iter()
            .map(|(a, b)| format!("\"{a}\" . \"{b}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(script, "add element bridge {table} peers {{ {elements} }}");
    }
    script
}

/// Replaces the bridge's `peers` set (see [`peers_script`]).
///
/// # Errors
///
/// `Conflict` for a malformed bridge or TAP name (they are written into the
/// script), `Io`/`Timeout` when `nft` fails.
pub async fn sync_peers(
    tools: &HostTools,
    bridge: &str,
    pairs: &[(String, String)],
) -> Result<(), EngineError> {
    validate_bridge_name(bridge)?;
    for (a, b) in pairs {
        validate_bridge_name(a)?;
        validate_bridge_name(b)?;
    }
    tools::run_with_stdin(&tools.nft, &["-f", "-"], &peers_script(bridge, pairs)).await?;
    Ok(())
}

/// The `nft -f -` script that replaces the bridge's `egress` and
/// `egress_addrs` sets with `entries`, in one transaction.
///
/// `entries` is `(tap, mac, ip)` for every egress-only VM on the bridge: the
/// TAP joins `egress`, which the mark rule reads, and the tuple joins
/// `egress_addrs`, which the address and MAC checks read.
#[must_use]
pub fn egress_script(bridge: &str, entries: &[(String, MacAddr, Ipv4Addr)]) -> String {
    let table = table_name(bridge);
    let mut script =
        format!("flush set bridge {table} egress\nflush set bridge {table} egress_addrs\n");
    if !entries.is_empty() {
        let taps = entries
            .iter()
            .map(|(tap, _, _)| format!("\"{tap}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(script, "add element bridge {table} egress {{ {taps} }}");
        let tuples = entries
            .iter()
            .map(|(tap, mac, ip)| format!("\"{tap}\" . {} . {ip}", mac_text(*mac)))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(
            script,
            "add element bridge {table} egress_addrs {{ {tuples} }}"
        );
    }
    script
}

/// Replaces the bridge's `egress` and `egress_addrs` sets: the TAPs of
/// egress-only VMs, with their assigned addresses and MAC addresses (§3.3).
///
/// The MAC comes from [`MacAddr`], so every element is written from bytes and
/// nothing unvalidated reaches `nft`.
///
/// # Errors
///
/// `Conflict` for a malformed bridge or TAP name (they are written into the
/// script), `Io`/`Timeout` when `nft` fails.
pub async fn sync_egress(
    tools: &HostTools,
    bridge: &str,
    entries: &[(String, MacAddr, Ipv4Addr)],
) -> Result<(), EngineError> {
    validate_bridge_name(bridge)?;
    for (tap, _, _) in entries {
        validate_bridge_name(tap)?;
    }
    tools::run_with_stdin(&tools.nft, &["-f", "-"], &egress_script(bridge, entries)).await?;
    Ok(())
}

/// A MAC address the way `nft` writes an `ether_addr` element,
/// `02:00:00:00:00:01`.
fn mac_text(mac: MacAddr) -> String {
    let [a, b, c, d, e, f] = mac.0;
    format!("{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}")
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
/// `isolated` is bridge port isolation: on for a VM without an isolation
/// group, off for a grouped VM, whose frames the bridge chain filters by the
/// `peers` set instead (engine-trait.md section 3.3).
///
/// Idempotent: the four `ip` calls below are all "bring it to this state"
/// operations.
///
/// # Errors
///
/// `Conflict` when an interface with the TAP's name exists but is not a TAP
/// device; `Io`/`Timeout` from the host tools.
pub async fn create_tap(
    tools: &HostTools,
    tap: &str,
    bridge: &str,
    isolated: bool,
) -> Result<(), EngineError> {
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

    // Port isolation keeps an ungrouped VM's frames from reaching another
    // isolated port even without the bridge chain; the ruleset closes the
    // rest.
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
            if isolated { "on" } else { "off" },
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
        iifname \"qw5br0\" oifname \"qw5br0\" meta mark 0x51550084 accept
        iifname \"qw5br0\" oifname \"qw5br0\" drop
        iifname \"qw5br0\" ip saddr != 10.99.5.0/24 drop
        iifname \"qw5br0\" meta mark 0x51550088 ip daddr 169.254.0.0/16 drop
        iifname \"qw5br0\" meta mark 0x51550088 ip daddr 10.99.5.0/24 drop
        iifname \"qw5br0\" accept
        oifname \"qw5br0\" ct state established,related accept
        oifname \"qw5br0\" drop
    }
    chain input {
        type filter hook input priority filter - 1; policy accept;
        iifname \"qw5br0\" ct state established,related accept
        iifname \"qw5br0\" meta mark 0x51550088 drop
        iifname \"qw5br0\" ip protocol icmp accept
        iifname \"qw5br0\" drop
    }
    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip saddr 10.99.5.0/24 oifname != \"qw5br0\" masquerade
    }
}
table bridge quoro_qw5br0 {}
delete table bridge quoro_qw5br0
table bridge quoro_qw5br0 {
    set peers {
        type ifname . ifname
    }
    set egress {
        type ifname
    }
    set egress_addrs {
        type ifname . ether_addr . ipv4_addr
    }
    chain prerouting {
        type filter hook prerouting priority filter; policy accept;
        meta ibrname \"qw5br0\" iifname @egress ether type != ip ether type != arp drop
        meta ibrname \"qw5br0\" iifname @egress ether type arp iifname . ether saddr . arp saddr ip != @egress_addrs drop
        meta ibrname \"qw5br0\" iifname @egress ether type arp iifname . arp saddr ether . arp saddr ip != @egress_addrs drop
        meta ibrname \"qw5br0\" iifname @egress ether type ip iifname . ether saddr . ip saddr != @egress_addrs drop
        meta ibrname \"qw5br0\" iifname @egress meta mark set 0x51550088
    }
    chain forward {
        type filter hook forward priority filter; policy accept;
        meta ibrname \"qw5br0\" iifname . oifname @peers meta mark set 0x51550084 accept
        meta ibrname \"qw5br0\" drop
    }
}
";
        assert_eq!(ruleset("qw5br0", &subnet("10.99.5.0/24"), &[]), expected);
    }

    #[test]
    fn ruleset_accepts_dns_to_each_configured_server_only() {
        let dns = [Ipv4Addr::new(10, 99, 5, 1), Ipv4Addr::new(1, 1, 1, 1)];
        let rules = ruleset("qw5br0", &subnet("10.99.5.0/24"), &dns);
        let udp = "        iifname \"qw5br0\" meta mark 0x51550088 ip daddr { 10.99.5.1, 1.1.1.1 } udp dport 53 accept";
        let tcp = "        iifname \"qw5br0\" meta mark 0x51550088 ip daddr { 10.99.5.1, 1.1.1.1 } tcp dport 53 accept";
        assert!(rules.contains(udp), "{rules}");
        assert!(rules.contains(tcp), "{rules}");
        let drop = "        iifname \"qw5br0\" meta mark 0x51550088 drop";
        assert!(rules.find(udp) < rules.find(drop), "{rules}");
        assert!(rules.find(tcp) < rules.find(drop), "{rules}");
    }

    #[test]
    fn peer_pairs_join_only_members_of_one_group() {
        let members = vec![
            ("qta".to_string(), Some("prj_a".to_string())),
            ("qtb".to_string(), Some("prj_a".to_string())),
            ("qtc".to_string(), Some("prj_b".to_string())),
            ("qtd".to_string(), None),
            ("qte".to_string(), None),
        ];
        assert_eq!(
            peer_pairs(&members),
            vec![
                ("qta".to_string(), "qtb".to_string()),
                ("qtb".to_string(), "qta".to_string()),
            ]
        );
        assert!(peer_pairs(&members[2..]).is_empty());
    }

    #[test]
    fn peers_script_flushes_then_adds() {
        assert_eq!(
            peers_script("qw5br0", &[]),
            "flush set bridge quoro_qw5br0 peers\n"
        );
        assert_eq!(
            peers_script(
                "qw5br0",
                &[
                    ("qta".to_string(), "qtb".to_string()),
                    ("qtb".to_string(), "qta".to_string())
                ]
            ),
            "flush set bridge quoro_qw5br0 peers\n\
             add element bridge quoro_qw5br0 peers { \"qta\" . \"qtb\", \"qtb\" . \"qta\" }\n"
        );
    }

    #[test]
    fn egress_script_flushes_then_adds_the_taps_addresses_and_macs() {
        assert_eq!(
            egress_script("qw5br0", &[]),
            "flush set bridge quoro_qw5br0 egress\n\
             flush set bridge quoro_qw5br0 egress_addrs\n"
        );
        assert_eq!(
            egress_script(
                "qw5br0",
                &[
                    (
                        "qta".to_string(),
                        MacAddr([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
                        Ipv4Addr::new(10, 99, 5, 7),
                    ),
                    (
                        "qtb".to_string(),
                        MacAddr([0x02, 0x1a, 0x2b, 0x3c, 0x4d, 0xef]),
                        Ipv4Addr::new(10, 99, 5, 8),
                    ),
                ]
            ),
            "flush set bridge quoro_qw5br0 egress\n\
             flush set bridge quoro_qw5br0 egress_addrs\n\
             add element bridge quoro_qw5br0 egress { \"qta\", \"qtb\" }\n\
             add element bridge quoro_qw5br0 egress_addrs { \"qta\" . 02:00:00:00:00:01 . 10.99.5.7, \"qtb\" . 02:1a:2b:3c:4d:ef . 10.99.5.8 }\n"
        );
    }

    #[test]
    fn ruleset_table_name_flattens_dashes() {
        let rules = ruleset("quoro-br-1", &subnet("10.99.5.0/24"), &[]);
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

        create_tap(&tools, &tap, LAB_BRIDGE, true)
            .await
            .expect("first create");
        assert!(is_tap(&tap), "{tap} is not a TAP device");
        create_tap(&tools, &tap, LAB_BRIDGE, true)
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
            create_tap(&tools, "lo", LAB_BRIDGE, true).await,
            Err(EngineError::Conflict(_))
        ));

        delete_tap(&tools, &tap).await.expect("first delete");
        assert!(!interface_exists(&tap));
        delete_tap(&tools, &tap).await.expect("second delete");

        remove_network(&tools, LAB_BRIDGE).await.expect("teardown");
        assert!(!interface_exists(LAB_BRIDGE));
    }
}

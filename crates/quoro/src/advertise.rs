// SPDX-License-Identifier: AGPL-3.0-only
//! The base URLs the primary advertises in `welcome.primary_urls`
//! (`node-agent-protocol.md` section 5.10).
//!
//! A node that cannot dial the primary's public address gets to try the
//! addresses listed here with the same certificate pin and credential. The
//! list is built on every call, because an address can appear or disappear
//! while the primary runs: the configured `public_url` first, then the host's
//! non-loopback addresses, then `<host name>.local` when that name resolves.
//!
//! The addresses come from `/proc/net/fib_trie` (IPv4) and `/proc/net/if_inet6`
//! (IPv6), not from `getifaddrs`: `quoro.service` may open only `AF_INET`,
//! `AF_INET6` and `AF_UNIX` sockets (ADR 0096), and `getifaddrs` needs a
//! netlink socket. Both files are readable inside that sandbox.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use quoro_proto::wire::MAX_PRIMARY_URLS;
use tracing::warn;

use crate::config::Config;

/// How long `<host name>.local` gets to resolve at start-up.
const LOCAL_NAME_TIMEOUT: Duration = Duration::from_secs(2);

/// The addresses this primary advertises, in order of preference.
#[derive(Clone, Debug)]
pub struct AdvertisedUrls {
    /// The configured `public_url`, trailing slash removed.
    public_url: Option<String>,
    /// The bound listener address (port 0 already resolved).
    listen: SocketAddr,
    /// The interface carrying the VM bridge, left out of the list.
    bridge: String,
    /// The node subnet, whose addresses are left out.
    subnet: Option<(Ipv4Addr, u8)>,
    /// `<host name>.local`, present only when it resolved at start-up.
    local_name: Option<String>,
}

impl AdvertisedUrls {
    /// Reads the host name once (at start-up) and resolves `<name>.local`, so
    /// a later provider call does no DNS work.
    pub async fn discover(config: &Config, listen: SocketAddr) -> Self {
        let subnet = subnet_network(&config.cloud_hypervisor.subnet.to_string());
        let host_name = std::fs::read_to_string("/proc/sys/kernel/hostname")
            .ok()
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty());
        let local_name = match host_name {
            Some(name) if resolves(&name, listen.port()).await => Some(name),
            _ => None,
        };
        Self {
            public_url: config
                .public_url
                .as_deref()
                .map(|url| url.trim_end_matches('/').to_owned())
                .filter(|url| !url.is_empty()),
            listen,
            bridge: config.cloud_hypervisor.bridge.clone(),
            subnet,
            local_name,
        }
    }

    /// The `welcome.primary_urls` list: `https://` base URLs, best first,
    /// deduplicated and capped at [`MAX_PRIMARY_URLS`].
    ///
    /// Interface enumeration runs on every call, so an address that changes
    /// while the primary runs is picked up by the next `welcome`.
    #[must_use]
    pub fn urls(&self) -> Vec<String> {
        let mut interfaces = Vec::new();
        match std::fs::read_to_string("/proc/net/fib_trie") {
            Ok(text) => interfaces.extend(
                local_ipv4(&text)
                    .into_iter()
                    .map(|ip| (String::new(), IpAddr::V4(ip))),
            ),
            Err(e) => warn!(error = %e, "cannot read /proc/net/fib_trie for primary_urls"),
        }
        // A host without IPv6 has no if_inet6; that is not worth a warning.
        if let Ok(text) = std::fs::read_to_string("/proc/net/if_inet6") {
            interfaces.extend(
                inet6_addresses(&text)
                    .into_iter()
                    .map(|(name, ip)| (name, IpAddr::V6(ip))),
            );
        }
        compose(
            self.public_url.as_deref(),
            self.listen,
            &interfaces,
            &self.bridge,
            self.subnet,
            self.local_name.as_deref(),
        )
    }
}

/// Builds the list from the fixed inputs: the configured `public_url`, the
/// listener, the host's interface addresses, the bridge to leave out, the node
/// subnet and the resolved `<host name>.local`.
///
/// When `listen` names a specific address rather than `0.0.0.0`/`::`, that
/// address is advertised and the interfaces are not enumerated: the operator
/// already said where the listener answers.
fn compose(
    public_url: Option<&str>,
    listen: SocketAddr,
    interfaces: &[(String, IpAddr)],
    bridge: &str,
    subnet: Option<(Ipv4Addr, u8)>,
    local_name: Option<&str>,
) -> Vec<String> {
    let mut urls: Vec<String> = Vec::new();
    let mut push = |url: String| {
        if !urls.contains(&url) && urls.len() < MAX_PRIMARY_URLS {
            urls.push(url);
        }
    };

    if let Some(public_url) = public_url {
        push(public_url.trim_end_matches('/').to_owned());
    }

    let port = listen.port();
    if listen.ip().is_unspecified() {
        // `0.0.0.0` accepts IPv4 only; `::` accepts both families on Linux.
        let ipv6 = listen.is_ipv6();
        for (interface, ip) in interfaces {
            if (ipv6 || ip.is_ipv4()) && is_advertised(*ip, interface, bridge, subnet) {
                push(base_url(*ip, port));
            }
        }
    } else {
        push(base_url(listen.ip(), port));
    }

    if let Some(name) = local_name {
        push(format!("https://{name}.local:{port}"));
    }

    urls
}

/// Whether an address of `interface` may be advertised.
fn is_advertised(
    ip: IpAddr,
    interface: &str,
    bridge: &str,
    subnet: Option<(Ipv4Addr, u8)>,
) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || interface == bridge {
        return false;
    }
    match ip {
        IpAddr::V4(ip) => {
            let [first, second, ..] = ip.octets();
            // Link-local, which no other host can reach.
            if first == 169 && second == 254 {
                return false;
            }
            !in_subnet(ip, subnet)
        }
        IpAddr::V6(ip) => {
            // IPv4-mapped addresses are already covered by their IPv4 form,
            // and `fe80::/10` is link-local.
            ip.to_ipv4_mapped().is_none() && ip.segments()[0] & 0xffc0 != 0xfe80
        }
    }
}

/// Whether an IPv4 address falls inside the node subnet.
fn in_subnet(ip: Ipv4Addr, subnet: Option<(Ipv4Addr, u8)>) -> bool {
    let Some((network, prefix)) = subnet else {
        return false;
    };
    let mask = u32::MAX
        .checked_shl(32 - u32::from(prefix.min(32)))
        .unwrap_or(0);
    u32::from(ip) & mask == u32::from(network)
}

/// `https://<ip>:<port>`, with brackets around an IPv6 literal.
fn base_url(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("https://{ip}:{port}"),
        IpAddr::V6(ip) => format!("https://[{ip}]:{port}"),
    }
}

/// The network and prefix of the configured subnet, read from its display
/// form.
fn subnet_network(text: &str) -> Option<(Ipv4Addr, u8)> {
    let (address, prefix) = text.split_once('/')?;
    Some((address.parse().ok()?, prefix.parse().ok()?))
}

/// The host's own IPv4 addresses, from `/proc/net/fib_trie`: every address
/// line followed by a `/32 host LOCAL` entry, in order, without duplicates
/// (the file lists the main and the local table).
///
/// An interface name is not in the file. The node bridge's address is left out
/// later because it lies inside the node subnet.
fn local_ipv4(fib_trie: &str) -> Vec<Ipv4Addr> {
    let mut out = Vec::new();
    let mut last = None;
    for line in fib_trie.lines() {
        let line = line.trim();
        if let Some(address) = line.strip_prefix("|-- ") {
            last = address.trim().parse::<Ipv4Addr>().ok();
        } else if line.starts_with("/32 host LOCAL")
            && let Some(ip) = last
            && !out.contains(&ip)
        {
            out.push(ip);
        }
    }
    out
}

/// The host's IPv6 addresses and their interfaces, from `/proc/net/if_inet6`:
/// `<32 hex address> <ifindex> <prefix> <scope> <flags> <name>` per line.
/// Addresses still in duplicate address detection (`tentative`, flag `0x40`)
/// or `dadfailed` (`0x08`) are left out, since nothing can reach them yet.
fn inet6_addresses(if_inet6: &str) -> Vec<(String, Ipv6Addr)> {
    let mut out = Vec::new();
    for line in if_inet6.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [hex, _index, _prefix, _scope, flags, name] = fields.as_slice() else {
            continue;
        };
        let Ok(bits) = u128::from_str_radix(hex, 16) else {
            continue;
        };
        if hex.len() != 32 {
            continue;
        }
        let flags = u32::from_str_radix(flags, 16).unwrap_or(0);
        if flags & (0x40 | 0x08) != 0 {
            continue;
        }
        out.push(((*name).to_owned(), Ipv6Addr::from(bits)));
    }
    out
}

/// Whether `<name>.local:<port>` resolves on this host, waiting at most
/// [`LOCAL_NAME_TIMEOUT`].
async fn resolves(name: &str, port: u16) -> bool {
    let target = format!("{name}.local:{port}");
    let lookup = tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs as _;
        target
            .to_socket_addrs()
            .is_ok_and(|mut addresses| addresses.next().is_some())
    });
    matches!(
        tokio::time::timeout(LOCAL_NAME_TIMEOUT, lookup).await,
        Ok(Ok(true))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interface(name: &str, ip: &str) -> (String, IpAddr) {
        (name.to_owned(), ip.parse().unwrap())
    }

    #[test]
    fn the_list_is_public_url_then_host_addresses_then_the_local_name() {
        let interfaces = vec![
            interface("lo0", "127.0.0.1"),
            interface("en0", "192.168.1.20"),
            interface("en0", "169.254.10.1"),
            interface("en0", "fe80::1"),
            interface("en1", "2001:db8::7"),
            interface("quoro0", "10.42.0.1"),
            interface("en2", "10.42.0.9"),
        ];
        let urls = compose(
            Some("https://primary.example:8443/"),
            "[::]:8443".parse().unwrap(),
            &interfaces,
            "quoro0",
            Some((Ipv4Addr::new(10, 42, 0, 0), 24)),
            Some("box"),
        );
        assert_eq!(
            urls,
            vec![
                "https://primary.example:8443",
                "https://192.168.1.20:8443",
                "https://[2001:db8::7]:8443",
                "https://box.local:8443",
            ]
        );
    }

    #[test]
    fn local_ipv4_addresses_are_read_from_fib_trie() {
        let fib_trie = "\
Main:
  +-- 0.0.0.0/0 3 0 5
     |-- 0.0.0.0
        /0 universe UNICAST
     +-- 10.42.0.0/24 2 0 2
        +-- 10.42.0.0/31 1 0 0
           |-- 10.42.0.0
              /24 link UNICAST
           |-- 10.42.0.1
              /32 host LOCAL
        |-- 10.42.0.255
           /32 link BROADCAST
     +-- 192.168.1.0/24 2 0 2
        |-- 192.168.1.69
           /32 host LOCAL
Local:
  +-- 0.0.0.0/0 3 0 5
     |-- 127.0.0.1
        /32 host LOCAL
     |-- 192.168.1.69
        /32 host LOCAL
";
        assert_eq!(
            local_ipv4(fib_trie),
            vec![
                Ipv4Addr::new(10, 42, 0, 1),
                Ipv4Addr::new(192, 168, 1, 69),
                Ipv4Addr::new(127, 0, 0, 1),
            ]
        );
    }

    #[test]
    fn ipv6_addresses_are_read_from_if_inet6() {
        let if_inet6 = "\
fe80000000000000be2411fffe51ab01 02 40 20 80     eth0
20010db8000000000000000000000007 02 40 00 00     eth0
20010db8000000000000000000000008 02 40 00 40     eth0
00000000000000000000000000000001 01 80 10 80       lo
";
        let addresses = inet6_addresses(if_inet6);
        assert_eq!(addresses.len(), 3, "the tentative address is left out");
        assert_eq!(
            addresses[1],
            ("eth0".to_owned(), "2001:db8::7".parse().unwrap())
        );
    }

    #[test]
    fn an_ipv4_listener_advertises_no_ipv6_address() {
        let interfaces = vec![
            interface("en0", "192.168.1.20"),
            interface("en1", "2001:db8::7"),
        ];
        let urls = compose(
            None,
            "0.0.0.0:8443".parse().unwrap(),
            &interfaces,
            "quoro0",
            None,
            None,
        );
        assert_eq!(urls, vec!["https://192.168.1.20:8443"]);
    }

    #[test]
    fn a_named_listen_address_replaces_the_interfaces() {
        let interfaces = vec![interface("en0", "192.168.1.20")];
        let urls = compose(
            None,
            "127.0.0.1:9443".parse().unwrap(),
            &interfaces,
            "quoro0",
            None,
            None,
        );
        assert_eq!(urls, vec!["https://127.0.0.1:9443"]);
    }

    #[test]
    fn loopback_duplicates_and_the_cap_are_applied() {
        let interfaces = vec![
            interface("en0", "192.168.1.20"),
            interface("en0", "192.168.1.20"),
            interface("en1", "192.168.1.21"),
        ];
        let urls = compose(
            Some("https://192.168.1.20:8443"),
            "0.0.0.0:8443".parse().unwrap(),
            &interfaces,
            "quoro0",
            None,
            None,
        );
        assert_eq!(
            urls,
            vec!["https://192.168.1.20:8443", "https://192.168.1.21:8443"]
        );

        let many: Vec<(String, IpAddr)> = (1..=12)
            .map(|last| interface("en0", &format!("192.168.2.{last}")))
            .collect();
        assert_eq!(
            compose(None, "0.0.0.0:8443".parse().unwrap(), &many, "", None, None).len(),
            MAX_PRIMARY_URLS
        );
    }

    #[test]
    fn an_unspecified_or_mapped_address_is_never_advertised() {
        assert!(!is_advertised("0.0.0.0".parse().unwrap(), "en0", "q", None));
        assert!(!is_advertised("::".parse().unwrap(), "en0", "q", None));
        assert!(!is_advertised(
            "::ffff:192.168.1.20".parse().unwrap(),
            "en0",
            "q",
            None
        ));
        assert!(!is_advertised(
            "10.42.0.7".parse().unwrap(),
            "en9",
            "q",
            Some((Ipv4Addr::new(10, 42, 0, 0), 24))
        ));
        assert!(is_advertised(
            "192.168.1.20".parse().unwrap(),
            "en0",
            "q",
            None
        ));
    }
}

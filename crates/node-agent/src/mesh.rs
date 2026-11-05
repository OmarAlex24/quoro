// SPDX-License-Identifier: AGPL-3.0-only
//! The kernel WireGuard mesh (`node-agent-protocol.md` sections 5.1, 5.11,
//! 6.2 step 1 and 6.9; ADR 0063, ADR 0144).
//!
//! The machine owns one X25519 key pair. The private key is written `0600` next
//! to the credential and never sent anywhere; `hello` carries only the public
//! key. `spec.mesh` is applied to the `quoro-wg0` interface with `ip` and
//! generic netlink, and the interface's traffic is filtered by the
//! `inet quoro_mesh` table so only the `allow` entries reach a guest.
//!
//! [`MeshManager`] keeps the interface equal to the newest verified snapshot:
//! every changed snapshot is evaluated, and a failed apply or an unchanged one
//! is retried at least every [`REAPPLY`].

use std::fmt::{self, Write as _};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use curve25519_dalek::MontgomeryPoint;
use quoro_engine::MESH_MARK;
use quoro_proto::wire::SpecMesh;
use tokio::sync::watch;
use tracing::warn;

use crate::AgentError;
use crate::identity;
use crate::reconcile::Snapshot;

/// File name of the mesh private key, next to the credential.
pub const KEY_FILE: &str = "mesh.key";

/// Name of the WireGuard interface on every machine.
pub const INTERFACE: &str = "quoro-wg0";

/// Name of the nftables table that filters the mesh interface.
const TABLE: &str = "quoro_mesh";

/// How long an applied mesh may stand without being re-applied.
pub const REAPPLY: Duration = Duration::from_secs(60);

/// The machine's X25519 private key.
///
/// The key is 32 raw bytes; [`MeshKey::load_or_create`] clamps it the way
/// X25519 requires. The [`fmt::Debug`] implementation never prints the private
/// bytes.
#[derive(Clone)]
pub struct MeshKey {
    private: [u8; 32],
}

impl MeshKey {
    /// Loads `<data_dir>/mesh.key`, or generates and writes it `0600` on first
    /// start.
    ///
    /// A file that exists but is not exactly 32 bytes is an error; it is never
    /// silently replaced, because replacing it would change the machine's
    /// public key behind the primary's back.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when the file cannot be read, is the wrong
    /// length, or the operating system provides no random bytes;
    /// [`AgentError::Io`] when a new key cannot be written.
    pub fn load_or_create(data_dir: &Path) -> Result<Self, AgentError> {
        let path = data_dir.join(KEY_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let private: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    AgentError::Identity(format!("{} is not 32 bytes", path.display()))
                })?;
                Ok(Self { private })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut private = [0u8; 32];
                getrandom::fill(&mut private).map_err(|e| {
                    AgentError::Identity(format!("could not generate the mesh key: {e}"))
                })?;
                clamp(&mut private);
                identity::write_private(data_dir, KEY_FILE, &private)?;
                Ok(Self { private })
            }
            Err(error) => Err(AgentError::Identity(format!(
                "could not read {}: {error}",
                path.display()
            ))),
        }
    }

    /// The standard-base64 X25519 public key (44 characters), the form
    /// `hello.mesh_public_key` and `spec.mesh.peers[].public_key` use.
    #[must_use]
    pub fn public_key_base64(&self) -> String {
        STANDARD.encode(self.public_key())
    }

    /// The raw X25519 public key.
    fn public_key(&self) -> [u8; 32] {
        MontgomeryPoint::mul_base_clamped(self.private).to_bytes()
    }
}

impl fmt::Debug for MeshKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MeshKey(<redacted>)")
    }
}

/// X25519 clamping: clear the low three bits, clear the top bit and set the
/// second-highest bit.
fn clamp(key: &mut [u8; 32]) {
    key[0] &= 248;
    key[31] &= 127;
    key[31] |= 64;
}

/// The routes the interface needs beyond the one its own address installs.
///
/// These are every peer's `allowed_ips` CIDR that is not inside this
/// interface's own network (the `/16` of `mesh.address`), deduplicated and
/// sorted. The own-network route comes with the address itself.
#[must_use]
pub fn desired_routes(mesh: &SpecMesh) -> Vec<String> {
    let own = parse_cidr(&mesh.address);
    let mut routes: Vec<String> = Vec::new();
    for peer in &mesh.peers {
        for cidr in &peer.allowed_ips {
            let Some(parsed) = parse_cidr(cidr) else {
                continue;
            };
            if own.is_some_and(|own| cidr_contains(own, parsed)) {
                continue;
            }
            if !routes.contains(cidr) {
                routes.push(cidr.clone());
            }
        }
    }
    routes.sort();
    routes
}

/// The full `nft -f -` ruleset for the mesh table, or the two lines that remove
/// it when there is no mesh.
///
/// The first two lines declare and then delete `inet quoro_mesh`, so applying
/// the file replaces the table in one transaction instead of stacking
/// duplicates. `allow` entries become prerouting rules that set
/// [`MESH_MARK`]; the forward and input chains then admit only marked packets
/// from the mesh interface and drop everything else. Any entry whose `source`
/// or `address` is not an IPv4 address is skipped with a warning: unparsed
/// text is never written into nftables syntax.
#[must_use]
pub fn nft_ruleset(mesh: Option<&SpecMesh>) -> String {
    let mut rules = String::with_capacity(1024);
    let _ = writeln!(rules, "table inet {TABLE} {{}}");
    let _ = writeln!(rules, "delete table inet {TABLE}");
    let Some(mesh) = mesh else {
        return rules;
    };
    let _ = writeln!(rules, "table inet {TABLE} {{");
    let _ = writeln!(rules, "    chain prerouting {{");
    let _ = writeln!(
        rules,
        "        type filter hook prerouting priority mangle; policy accept;"
    );
    for allow in &mesh.allow {
        let Ok(source) = allow.source.parse::<Ipv4Addr>() else {
            warn!(
                source = %allow.source,
                "skipping a mesh allow entry whose source is not an IPv4 address"
            );
            continue;
        };
        let Ok(address) = allow.address.parse::<Ipv4Addr>() else {
            warn!(
                address = %allow.address,
                "skipping a mesh allow entry whose address is not an IPv4 address"
            );
            continue;
        };
        let _ = writeln!(
            rules,
            "        iifname \"{INTERFACE}\" ip saddr {source} ip daddr {address} tcp dport {} meta mark set {MESH_MARK:#010x}",
            allow.port
        );
    }
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain forward {{");
    let _ = writeln!(
        rules,
        "        type filter hook forward priority filter - 2; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{INTERFACE}\" meta mark {MESH_MARK:#010x} accept"
    );
    let _ = writeln!(rules, "        iifname \"{INTERFACE}\" drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain input {{");
    let _ = writeln!(
        rules,
        "        type filter hook input priority filter - 2; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        iifname \"{INTERFACE}\" ct state established,related accept"
    );
    let _ = writeln!(rules, "        iifname \"{INTERFACE}\" drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "}}");
    rules
}

/// Parses an endpoint as `IPv4:port` or `[IPv6]:port`.
///
/// A host name is not resolved here: WireGuard's endpoint must be an address,
/// so a name yields `None` and the caller omits the endpoint with a warning.
#[must_use]
pub fn parse_endpoint(text: &str) -> Option<SocketAddr> {
    text.parse().ok()
}

/// A CIDR as an address and a prefix length.
fn parse_cidr(text: &str) -> Option<(IpAddr, u8)> {
    let (address, prefix) = text.trim().split_once('/')?;
    let address: IpAddr = address.parse().ok()?;
    let prefix: u8 = prefix.parse().ok()?;
    let bits = if address.is_ipv4() { 32 } else { 128 };
    if prefix > bits {
        return None;
    }
    Some((address, prefix))
}

/// The network address of `(address, prefix)`, with every host bit zeroed.
fn network((address, prefix): (IpAddr, u8)) -> IpAddr {
    match address {
        IpAddr::V4(address) => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - u32::from(prefix))
            };
            IpAddr::V4(Ipv4Addr::from(u32::from(address) & mask))
        }
        IpAddr::V6(address) => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - u32::from(prefix))
            };
            IpAddr::V6((u128::from(address) & mask).into())
        }
    }
}

/// Whether the CIDR `inner` is entirely inside the CIDR `outer`.
fn cidr_contains(outer: (IpAddr, u8), inner: (IpAddr, u8)) -> bool {
    if outer.0.is_ipv4() != inner.0.is_ipv4() {
        return false;
    }
    inner.1 >= outer.1 && network((inner.0, outer.1)) == network(outer)
}

/// Applies and removes the mesh interface.
///
/// Implementations return a readable error string rather than an error type:
/// the manager logs it and retries, and the driver never needs to be inspected
/// by the rest of the agent.
pub trait MeshDriver: Send + Sync {
    /// Makes the interface equal to `mesh`, using `key` as its private key.
    fn apply(
        &self,
        key: &MeshKey,
        mesh: &SpecMesh,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Removes the interface and its firewall table.
    fn remove(&self) -> impl Future<Output = Result<(), String>> + Send;
}

/// A driver for tests: it records every call and changes nothing.
#[derive(Clone, Default)]
pub struct RecordingMeshDriver {
    calls: Arc<Mutex<Vec<MeshCall>>>,
}

/// One call a [`RecordingMeshDriver`] saw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MeshCall {
    /// [`MeshDriver::apply`] with this mesh.
    Apply(SpecMesh),
    /// [`MeshDriver::remove`].
    Remove,
}

impl RecordingMeshDriver {
    /// A driver with no recorded calls.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every call seen so far, in order.
    #[must_use]
    pub fn calls(&self) -> Vec<MeshCall> {
        self.lock().clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<MeshCall>> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl MeshDriver for RecordingMeshDriver {
    fn apply(
        &self,
        _key: &MeshKey,
        mesh: &SpecMesh,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let mesh = mesh.clone();
        let calls = Arc::clone(&self.calls);
        async move {
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(MeshCall::Apply(mesh));
            Ok(())
        }
    }

    fn remove(&self) -> impl Future<Output = Result<(), String>> + Send {
        let calls = Arc::clone(&self.calls);
        async move {
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(MeshCall::Remove);
            Ok(())
        }
    }
}

/// The driver for targets without a Linux kernel WireGuard implementation.
///
/// It logs once that the mesh needs Linux and then does nothing, so the rest of
/// the agent still starts on a development machine.
#[derive(Debug)]
pub struct UnsupportedMeshDriver {
    warned: std::sync::Once,
}

impl UnsupportedMeshDriver {
    /// A no-op driver.
    #[must_use]
    pub fn new() -> Self {
        Self {
            warned: std::sync::Once::new(),
        }
    }
}

impl Default for UnsupportedMeshDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl MeshDriver for UnsupportedMeshDriver {
    fn apply(
        &self,
        _key: &MeshKey,
        _mesh: &SpecMesh,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let warned = &self.warned;
        async move {
            warned.call_once(|| {
                warn!("the WireGuard mesh needs Linux; ignoring spec.mesh on this machine");
            });
            Ok(())
        }
    }

    async fn remove(&self) -> Result<(), String> {
        Ok(())
    }
}

/// The driver the current target runs.
#[cfg(target_os = "linux")]
pub type PlatformMeshDriver = LinuxMeshDriver;

/// The driver the current target runs.
#[cfg(not(target_os = "linux"))]
pub type PlatformMeshDriver = UnsupportedMeshDriver;

/// Keeps the mesh interface equal to the newest verified snapshot.
pub struct MeshManager<D: MeshDriver> {
    driver: D,
    key: MeshKey,
}

impl<D: MeshDriver> MeshManager<D> {
    /// A manager that applies `desired` with `driver` and `key`.
    #[must_use]
    pub fn new(driver: D, key: MeshKey) -> Self {
        Self { driver, key }
    }

    /// Applies the mesh of every snapshot until `stop` resolves.
    ///
    /// A snapshot whose `spec.mesh` differs from the last applied one, a
    /// snapshot after a failed apply, and every [`REAPPLY`] period all cause an
    /// apply. A snapshot without a mesh removes the interface; the first start
    /// also removes it once, so an interface left by a previous run cannot
    /// survive with stale peers.
    pub async fn run(
        self,
        mut desired: watch::Receiver<Option<Snapshot>>,
        stop: impl Future<Output = ()> + Send,
    ) {
        let mut stop = std::pin::pin!(stop);
        let mut ticker = tokio::time::interval(REAPPLY);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick of an interval is immediate; the first evaluation
        // below already looks at the current snapshot.
        ticker.tick().await;

        let mut state = ManagerState::default();
        let current = desired.borrow_and_update().clone();
        self.sync(current.as_ref(), &mut state, false).await;

        loop {
            tokio::select! {
                () = &mut stop => break,
                changed = desired.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let current = desired.borrow_and_update().clone();
                    self.sync(current.as_ref(), &mut state, false).await;
                }
                _ = ticker.tick() => {
                    let current = desired.borrow_and_update().clone();
                    self.sync(current.as_ref(), &mut state, true).await;
                }
            }
        }
    }

    /// One evaluation of the newest snapshot.
    async fn sync(&self, snapshot: Option<&Snapshot>, state: &mut ManagerState, force: bool) {
        let mesh = snapshot.and_then(|snapshot| snapshot.state.spec.mesh.as_ref());
        match mesh {
            Some(mesh) => {
                if state.should_apply(mesh, force) {
                    match self.driver.apply(&self.key, mesh).await {
                        Ok(()) => {
                            state.applied = Some(mesh.clone());
                            state.failed = false;
                            state.removed = false;
                            state.last_apply = Some(tokio::time::Instant::now());
                        }
                        Err(error) => {
                            warn!(error = %error, "could not apply the mesh");
                            state.failed = true;
                        }
                    }
                }
            }
            None => {
                if state.applied.is_some() || !state.removed {
                    match self.driver.remove().await {
                        Ok(()) => {
                            state.applied = None;
                            state.failed = false;
                            state.removed = true;
                            state.last_apply = None;
                        }
                        Err(error) => {
                            warn!(error = %error, "could not remove the mesh");
                        }
                    }
                }
            }
        }
    }
}

/// What the manager last did, so it can skip an unchanged mesh.
#[derive(Default)]
struct ManagerState {
    applied: Option<SpecMesh>,
    failed: bool,
    last_apply: Option<tokio::time::Instant>,
    removed: bool,
}

impl ManagerState {
    /// Whether this mesh must be applied now.
    fn should_apply(&self, mesh: &SpecMesh, force: bool) -> bool {
        force
            || self.failed
            || self.applied.as_ref() != Some(mesh)
            || self.last_apply.is_none_or(|last| last.elapsed() >= REAPPLY)
    }
}

/// The Linux driver: `ip` for the link, address and routes, generic netlink for
/// WireGuard, and `nft` for the firewall.
#[cfg(target_os = "linux")]
mod linux {
    use std::net::IpAddr;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::time::Duration;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use futures_util::StreamExt as _;
    use genetlink::new_connection;
    use netlink_packet_core::{NLM_F_ACK, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload};
    use netlink_packet_generic::GenlMessage;
    use netlink_packet_wireguard::{
        WireguardAddressFamily, WireguardAllowedIp, WireguardAllowedIpAttr, WireguardAttribute,
        WireguardCmd, WireguardDeviceFlags, WireguardMessage, WireguardPeer,
        WireguardPeerAttribute,
    };
    use quoro_proto::wire::{MeshPeer, SpecMesh};
    use tokio::io::AsyncWriteExt as _;
    use tokio::process::Command;
    use tracing::warn;

    use super::{
        INTERFACE, MeshDriver, MeshKey, desired_routes, nft_ruleset, parse_cidr, parse_endpoint,
    };

    /// Where `ip` and `nft` are looked for, in order.
    const SEARCH_DIRS: [&str; 4] = ["/usr/sbin", "/sbin", "/usr/bin", "/bin"];

    /// How long one host tool call may take before it is killed.
    const TIMEOUT: Duration = Duration::from_secs(30);

    /// The Linux mesh driver. Host tools are resolved on every apply so a
    /// missing package surfaces as a logged, retried error rather than a
    /// startup failure.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct LinuxMeshDriver;

    impl LinuxMeshDriver {
        /// A driver that runs the host's `ip` and `nft`.
        #[must_use]
        pub fn new() -> Self {
            Self
        }
    }

    impl MeshDriver for LinuxMeshDriver {
        async fn apply(&self, key: &MeshKey, mesh: &SpecMesh) -> Result<(), String> {
            let key = key.clone();
            let mesh = mesh.clone();
            apply(&key, &mesh).await
        }

        async fn remove(&self) -> Result<(), String> {
            remove().await
        }
    }

    /// Applies one mesh specification.
    async fn apply(key: &MeshKey, mesh: &SpecMesh) -> Result<(), String> {
        let ip = find_tool("ip", "iproute2")?;
        let nft = find_tool("nft", "nftables")?;
        let own = parse_cidr(&mesh.address)
            .ok_or_else(|| format!("mesh address {:?} is not an IPv4 CIDR", mesh.address))?;
        let IpAddr::V4(own_address) = own.0 else {
            return Err(format!("mesh address {:?} is not IPv4", mesh.address));
        };

        // Creating the link loads the WireGuard module, so it must happen
        // before the generic netlink family is resolved below.
        if !Path::new("/sys/class/net").join(INTERFACE).exists() {
            run(&ip, &["link", "add", "dev", INTERFACE, "type", "wireguard"]).await?;
        }

        let address = format!("{own_address}/{}", own.1);
        run(&ip, &["address", "replace", &address, "dev", INTERFACE]).await?;
        remove_other_addresses(&ip, own_address, own.1).await?;
        run(&ip, &["link", "set", "dev", INTERFACE, "up"]).await?;

        set_wireguard(key, mesh).await?;

        let desired = desired_routes(mesh);
        for cidr in &desired {
            run(&ip, &["route", "replace", cidr, "dev", INTERFACE]).await?;
        }
        remove_other_routes(&ip, &desired, own).await?;

        run_with_stdin(&nft, &["-f", "-"], &nft_ruleset(Some(mesh))).await?;
        Ok(())
    }

    /// Removes the firewall table and the interface.
    async fn remove() -> Result<(), String> {
        let nft = find_tool("nft", "nftables")?;
        if let Err(error) = run_with_stdin(&nft, &["-f", "-"], &nft_ruleset(None)).await {
            // The table may never have been created on this machine.
            if !error.contains("No such file or directory") {
                return Err(error);
            }
        }
        let ip = find_tool("ip", "iproute2")?;
        if Path::new("/sys/class/net").join(INTERFACE).exists() {
            run(&ip, &["link", "del", "dev", INTERFACE]).await?;
        }
        Ok(())
    }

    /// Sets the private key, listen port and peers in one `SetDevice` request.
    ///
    /// `ReplacePeers` makes a peer that left `spec.mesh.peers` disappear. A peer
    /// whose public key is not 32 base64 bytes is skipped with a warning rather
    /// than failing the whole apply.
    async fn set_wireguard(key: &MeshKey, mesh: &SpecMesh) -> Result<(), String> {
        let peers: Vec<WireguardPeer> = mesh.peers.iter().filter_map(peer).collect();
        let attributes = vec![
            WireguardAttribute::IfName(INTERFACE.to_owned()),
            WireguardAttribute::PrivateKey(key.private),
            WireguardAttribute::ListenPort(mesh.listen_port),
            WireguardAttribute::Flags(WireguardDeviceFlags::ReplacePeers),
            WireguardAttribute::Peers(peers),
        ];
        let genlmsg: GenlMessage<WireguardMessage> = GenlMessage::from_payload(WireguardMessage {
            cmd: WireguardCmd::SetDevice,
            attributes,
        });
        let mut nlmsg = NetlinkMessage::from(genlmsg);
        nlmsg.header.flags = NLM_F_REQUEST | NLM_F_ACK;

        let (connection, mut handle, _) =
            new_connection().map_err(|e| format!("could not open generic netlink: {e}"))?;
        tokio::spawn(connection);
        let mut responses = handle
            .request(nlmsg)
            .await
            .map_err(|e| format!("could not send the WireGuard request: {e}"))?;
        while let Some(item) = responses.next().await {
            let packet = item.map_err(|e| format!("could not read the WireGuard response: {e}"))?;
            if let NetlinkPayload::Error(error) = packet.payload {
                if error.code.is_some() {
                    return Err(format!(
                        "the kernel refused the WireGuard configuration: {}",
                        error.to_io()
                    ));
                }
                break;
            }
        }
        Ok(())
    }

    /// Builds one netlink peer, or `None` when its public key is unusable.
    fn peer(peer: &MeshPeer) -> Option<WireguardPeer> {
        let decoded = STANDARD.decode(&peer.public_key).ok();
        let Some(public_key) = decoded
            .as_deref()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        else {
            warn!(
                node_id = %peer.node_id,
                "skipping a mesh peer whose public key is not 32 base64 bytes"
            );
            return None;
        };
        let mut attributes = vec![WireguardPeerAttribute::PublicKey(public_key)];
        if let Some(endpoint) = &peer.endpoint {
            match parse_endpoint(endpoint) {
                Some(address) => attributes.push(WireguardPeerAttribute::Endpoint(address)),
                None => warn!(
                    node_id = %peer.node_id,
                    endpoint = %endpoint,
                    "skipping a mesh peer endpoint that is not an address"
                ),
            }
        }
        if let Some(seconds) = peer.persistent_keepalive_s {
            attributes.push(WireguardPeerAttribute::PersistentKeepalive(seconds));
        }
        let mut allowed_ips = Vec::new();
        for cidr in &peer.allowed_ips {
            let Some((address, prefix)) = parse_cidr(cidr) else {
                warn!(
                    node_id = %peer.node_id,
                    cidr = %cidr,
                    "skipping a mesh peer allowed-ips entry that is not a CIDR"
                );
                continue;
            };
            let family = if address.is_ipv4() {
                WireguardAddressFamily::Ipv4
            } else {
                WireguardAddressFamily::Ipv6
            };
            allowed_ips.push(WireguardAllowedIp(vec![
                WireguardAllowedIpAttr::Family(family),
                WireguardAllowedIpAttr::IpAddr(address),
                WireguardAllowedIpAttr::Cidr(prefix),
            ]));
        }
        if !allowed_ips.is_empty() {
            attributes.push(WireguardPeerAttribute::AllowedIps(allowed_ips));
        }
        Some(WireguardPeer(attributes))
    }

    /// Deletes every IPv4 address on the interface except the mesh address.
    async fn remove_other_addresses(
        ip: &Path,
        own_address: std::net::Ipv4Addr,
        own_prefix: u8,
    ) -> Result<(), String> {
        let text = run(ip, &["-j", "-4", "address", "show", "dev", INTERFACE]).await?;
        let parsed: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| format!("could not parse `ip address`: {e}"))?;
        let Some(links) = parsed.as_array() else {
            return Ok(());
        };
        for link in links {
            let Some(addresses) = link.get("addr_info").and_then(serde_json::Value::as_array)
            else {
                continue;
            };
            for info in addresses {
                if info.get("family").and_then(serde_json::Value::as_str) != Some("inet") {
                    continue;
                }
                let Some(local) = info.get("local").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let Some(prefix) = info.get("prefixlen").and_then(serde_json::Value::as_u64) else {
                    continue;
                };
                if local == own_address.to_string() && prefix == u64::from(own_prefix) {
                    continue;
                }
                let address = format!("{local}/{prefix}");
                run(ip, &["address", "del", &address, "dev", INTERFACE]).await?;
            }
        }
        Ok(())
    }

    /// Deletes routes on the interface that are neither desired nor the
    /// kernel's own.
    async fn remove_other_routes(
        ip: &Path,
        desired: &[String],
        own: (IpAddr, u8),
    ) -> Result<(), String> {
        let text = run(ip, &["-j", "-4", "route", "show", "dev", INTERFACE]).await?;
        let parsed: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("could not parse `ip route`: {e}"))?;
        let Some(routes) = parsed.as_array() else {
            return Ok(());
        };
        let own_network = super::network(own).to_string();
        for route in routes {
            let Some(dst) = route.get("dst").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if desired.iter().any(|cidr| cidr == dst) || dst == own_network {
                continue;
            }
            // The address itself installs the /16 with protocol `kernel`;
            // older iproute2 versions spell the key `proto`.
            let kernel_route = ["protocol", "proto"]
                .iter()
                .any(|key| route.get(*key).and_then(serde_json::Value::as_str) == Some("kernel"));
            if kernel_route {
                continue;
            }
            run(ip, &["route", "del", dst, "dev", INTERFACE]).await?;
        }
        Ok(())
    }

    /// The first executable regular file called `name`, or an error naming the
    /// package that provides it.
    fn find_tool(name: &str, package: &str) -> Result<PathBuf, String> {
        SEARCH_DIRS
            .iter()
            .map(|dir| Path::new(dir).join(name))
            .find(|candidate| is_executable(candidate))
            .ok_or_else(|| format!("{name} not found; install {package}"))
    }

    /// True when `path` is a file (following symlinks) with an executable bit.
    fn is_executable(path: &Path) -> bool {
        std::fs::metadata(path)
            .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
    }

    /// Runs `program args…` without a shell and returns its standard output.
    async fn run(program: &Path, args: &[&str]) -> Result<String, String> {
        run_inner(program, args, None).await
    }

    /// [`run`], with `stdin` written to the child's standard input.
    async fn run_with_stdin(program: &Path, args: &[&str], stdin: &str) -> Result<String, String> {
        run_inner(program, args, Some(stdin)).await
    }

    /// The one implementation of both entry points.
    async fn run_inner(
        program: &Path,
        args: &[&str],
        stdin: Option<&str>,
    ) -> Result<String, String> {
        let label = format!("{} {}", program.display(), args.join(" "))
            .trim_end()
            .to_owned();
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Dropping `wait_with_output` on the timeout below drops the child,
            // which then kills the tool instead of leaving it running.
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|e| format!("{label}: {e}"))?;
        if let Some(text) = stdin
            && let Some(mut pipe) = child.stdin.take()
        {
            // A write error means the tool closed its stdin early; the exit
            // status and stderr below say why, and those are worth returning.
            let _ = pipe.write_all(text.as_bytes()).await;
            let _ = pipe.shutdown().await;
        }
        let output = match tokio::time::timeout(TIMEOUT, child.wait_with_output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => return Err(format!("{label}: {e}")),
            Err(_) => return Err(format!("{label}: timed out")),
        };
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        let code = output
            .status
            .code()
            .map_or_else(|| "killed by signal".to_owned(), |code| code.to_string());
        Err(format!(
            "{label}: exit {code}: {}",
            detail(&output.stdout, &output.stderr)
        ))
    }

    /// The trimmed stderr, or the trimmed stdout when stderr is empty.
    fn detail(stdout: &[u8], stderr: &[u8]) -> String {
        let stderr = String::from_utf8_lossy(stderr);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            String::from_utf8_lossy(stdout).trim().to_owned()
        } else {
            stderr.to_owned()
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::LinuxMeshDriver;

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_proto::wire::{MeshAllow, MeshPeer};

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-mesh-{}",
                crate::util::hex(&crate::util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
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

    fn test_key() -> MeshKey {
        MeshKey { private: [7; 32] }
    }

    fn mesh() -> SpecMesh {
        SpecMesh {
            address: "10.43.0.5/16".to_owned(),
            listen_port: 51820,
            peers: vec![MeshPeer {
                node_id: "nod_edge".to_owned(),
                public_key: "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=".to_owned(),
                endpoint: Some("203.0.113.7:51820".to_owned()),
                allowed_ips: vec![
                    "10.43.0.1/32".to_owned(),
                    "10.99.5.0/24".to_owned(),
                    "10.99.5.0/24".to_owned(),
                ],
                persistent_keepalive_s: Some(25),
            }],
            allow: vec![MeshAllow {
                source: "10.43.0.1".to_owned(),
                address: "10.99.5.10".to_owned(),
                port: 8080,
            }],
        }
    }

    fn snapshot(mesh: SpecMesh) -> Snapshot {
        let state = quoro_proto::wire::DesiredState {
            rev: 1,
            issued_at: String::new(),
            term: 0,
            spec: quoro_proto::wire::Spec {
                mesh: Some(mesh),
                ..quoro_proto::wire::Spec::default()
            },
            signature: String::new(),
        };
        Snapshot { rev: 1, state }
    }

    async fn stop_future(mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            if stop.changed().await.is_err() {
                return;
            }
        }
    }

    #[test]
    fn the_rfc7748_vector_produces_the_documented_public_key() {
        // RFC 7748 section 6.1, Alice's private and public keys.
        let private: [u8; 32] = [
            0x77, 0x07, 0x6d, 0x0a, 0x73, 0x18, 0xa5, 0x7d, 0x3c, 0x16, 0xc1, 0x72, 0x51, 0xb2,
            0x66, 0x45, 0xdf, 0x4c, 0x2f, 0x87, 0xeb, 0xc0, 0x99, 0x2a, 0xb1, 0x77, 0xfb, 0xa5,
            0x1d, 0xb9, 0x2c, 0x2a,
        ];
        let expected: [u8; 32] = [
            0x85, 0x20, 0xf0, 0x09, 0x89, 0x30, 0xa7, 0x54, 0x74, 0x8b, 0x7d, 0xdc, 0xb4, 0x3e,
            0xf7, 0x5a, 0x0d, 0xbf, 0x3a, 0x0d, 0x26, 0x38, 0x1a, 0xf4, 0xeb, 0xa4, 0xa9, 0x8e,
            0xaa, 0x9b, 0x4e, 0x6a,
        ];
        let key = MeshKey { private };
        assert_eq!(key.public_key(), expected);
        assert_eq!(key.public_key_base64(), STANDARD.encode(expected));
        assert_eq!(key.public_key_base64().len(), 44);
    }

    #[test]
    fn a_created_key_is_clamped_and_reused() {
        let dir = TempDir::new();
        let key = MeshKey::load_or_create(dir.path()).unwrap();
        let path = dir.path().join(KEY_FILE);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[0] & 0b0000_0111, 0);
        assert_eq!(bytes[31] & 0b1100_0000, 0b0100_0000);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let again = MeshKey::load_or_create(dir.path()).unwrap();
        assert_eq!(key.public_key_base64(), again.public_key_base64());
    }

    #[test]
    fn a_key_file_of_the_wrong_length_is_an_error() {
        let dir = TempDir::new();
        std::fs::write(dir.path().join(KEY_FILE), [1, 2, 3]).unwrap();
        let error = MeshKey::load_or_create(dir.path()).unwrap_err();
        assert!(error.to_string().contains("not 32 bytes"));
        assert_eq!(std::fs::read(dir.path().join(KEY_FILE)).unwrap(), [1, 2, 3]);
    }

    #[test]
    fn debug_never_prints_the_private_key() {
        let key = MeshKey {
            private: [0xab; 32],
        };
        let printed = format!("{key:?}");
        assert_eq!(printed, "MeshKey(<redacted>)");
        assert!(!printed.contains("ab"));
    }

    #[test]
    fn desired_routes_skip_the_own_network_and_are_sorted_and_deduplicated() {
        let routes = desired_routes(&mesh());
        assert_eq!(routes, vec!["10.99.5.0/24".to_owned()]);
    }

    #[test]
    fn the_ruleset_is_the_documented_text() {
        let expected = "\
table inet quoro_mesh {}
delete table inet quoro_mesh
table inet quoro_mesh {
    chain prerouting {
        type filter hook prerouting priority mangle; policy accept;
        iifname \"quoro-wg0\" ip saddr 10.43.0.1 ip daddr 10.99.5.10 tcp dport 8080 meta mark set 0x5155008c
    }
    chain forward {
        type filter hook forward priority filter - 2; policy accept;
        iifname \"quoro-wg0\" meta mark 0x5155008c accept
        iifname \"quoro-wg0\" drop
    }
    chain input {
        type filter hook input priority filter - 2; policy accept;
        iifname \"quoro-wg0\" ct state established,related accept
        iifname \"quoro-wg0\" drop
    }
}
";
        assert_eq!(nft_ruleset(Some(&mesh())), expected);
    }

    #[test]
    fn no_mesh_is_only_the_two_table_lines() {
        assert_eq!(
            nft_ruleset(None),
            "table inet quoro_mesh {}\ndelete table inet quoro_mesh\n"
        );
    }

    #[test]
    fn a_malformed_allow_entry_is_not_written_into_the_ruleset() {
        let mut mesh = mesh();
        mesh.allow.push(MeshAllow {
            source: "not-an-ip".to_owned(),
            address: "10.99.5.11".to_owned(),
            port: 80,
        });
        let rules = nft_ruleset(Some(&mesh));
        assert!(!rules.contains("not-an-ip"));
        assert_eq!(rules.matches("tcp dport").count(), 1);
    }

    #[test]
    fn endpoints_are_addresses_and_never_names() {
        assert_eq!(
            parse_endpoint("203.0.113.7:51820"),
            Some("203.0.113.7:51820".parse().unwrap())
        );
        assert_eq!(
            parse_endpoint("[2001:db8::1]:51820"),
            Some("[2001:db8::1]:51820".parse().unwrap())
        );
        assert_eq!(parse_endpoint("edge.example.com:51820"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_manager_applies_a_mesh_and_removes_it() {
        let (desired_tx, desired_rx) = watch::channel(None);
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            MeshManager::new(driver, test_key()).run(desired_rx, stop_future(stop_rx)),
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorded.calls(), vec![MeshCall::Remove]);

        desired_tx.send(Some(snapshot(mesh()))).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            recorded.calls(),
            vec![MeshCall::Remove, MeshCall::Apply(mesh())]
        );

        desired_tx.send(Some(snapshot(mesh()))).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            recorded.calls(),
            vec![MeshCall::Remove, MeshCall::Apply(mesh())],
            "an unchanged mesh is not applied again inside the reapply period"
        );

        desired_tx.send(None).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            recorded.calls(),
            vec![MeshCall::Remove, MeshCall::Apply(mesh()), MeshCall::Remove]
        );

        stop_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn the_manager_reapplies_an_unchanged_mesh_after_the_period() {
        let (desired_tx, desired_rx) = watch::channel(Some(snapshot(mesh())));
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        let (_stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            MeshManager::new(driver, test_key()).run(desired_rx, stop_future(stop_rx)),
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorded.calls(), vec![MeshCall::Apply(mesh())]);
        tokio::time::sleep(REAPPLY + Duration::from_secs(1)).await;
        assert_eq!(
            recorded.calls(),
            vec![MeshCall::Apply(mesh()), MeshCall::Apply(mesh())]
        );
        desired_tx.send(None).unwrap();
        task.abort();
    }
}

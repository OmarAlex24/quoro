// SPDX-License-Identifier: AGPL-3.0-only
//! The kernel WireGuard mesh (`node-agent-protocol.md` sections 5.1, 5.11,
//! 6.2 step 1 and 6.9; ADR 0063, ADR 0144).
//!
//! The machine owns one X25519 key pair. The private key is written `0600` next
//! to the credential and never sent anywhere; `hello` carries only the public
//! key. `spec.mesh` is applied to the `quoro-wg0` interface with `ip` and
//! generic netlink, and the interface's traffic is filtered by the
//! `inet quoro_mesh` table so only the `allow` entries get through: to a
//! guest, or, for a warm volume's replication stream, to the host's own mesh
//! address (section 6.13). The table also keeps new connections out of the
//! mesh unless a root process on this machine opens them, and forwards none:
//! the mesh carries no traffic between guests, and a guest opens no
//! connection on it (section 6.9). On a voter it also admits the vote
//! listener from every other voter with the `node` role (sections 6.9 and
//! 8.9), a rule that follows from `spec.votes` and is not in `allow`, and
//! takes TCP to the listener only over the mesh interface or loopback.
//!
//! [`MeshManager`] keeps the interface equal to the newest verified snapshot:
//! every snapshot whose `spec.mesh` or `spec.votes` changed is evaluated, and
//! a failed apply or an unchanged one is retried at least every [`REAPPLY`].
//! It also tells the vote listener where it may listen ([`ListenerLink`]):
//! only while votes are open, and never through an apply that configures the
//! peers of a state in which they are not (section 8.9).
//!
//! These are version [`MESH_RULES`] of the mesh rules, which `hello` reports
//! (section 5.1).

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
use quoro_proto::wire::{MeshPeer, SpecMesh, SpecVotes};
use tokio::sync::watch;
use tracing::{debug, error, warn};

use crate::AgentError;
use crate::identity;
use crate::reconcile::Snapshot;

/// File name of the mesh private key, next to the credential.
pub const KEY_FILE: &str = "mesh.key";

/// Name of the WireGuard interface on every machine.
pub const INTERFACE: &str = "quoro-wg0";

/// Name of the nftables table that filters the mesh interface.
const TABLE: &str = "quoro_mesh";

/// The version of the mesh rules of section 6.9 that this agent applies,
/// which `hello.mesh_rules` reports (section 5.1). Version 2 is every rule
/// of that section: the firewall table loaded before any peer, and the
/// "New connections" rule that only root processes of this machine open
/// connections through the mesh. A change to those rules raises it here.
pub const MESH_RULES: u64 = 2;

/// How long an applied mesh may stand without being re-applied.
pub const REAPPLY: Duration = Duration::from_secs(60);

/// How long the manager waits before it retries a failed apply the first
/// time. Each failure in a row doubles the wait, up to [`REAPPLY`].
pub const RETRY_FIRST: Duration = Duration::from_secs(2);

/// The wait before the next retry of a failed apply, after `previous`, the
/// wait before this failure's attempt, if it was a retry: [`RETRY_FIRST`],
/// then twice the previous wait, never more than [`REAPPLY`].
#[must_use]
pub fn retry_delay(previous: Option<Duration>) -> Duration {
    previous.map_or(RETRY_FIRST, |previous| {
        previous.saturating_mul(2).min(REAPPLY)
    })
}

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
/// [`MESH_MARK`]. The forward chain then admits only marked packets from the
/// mesh interface; the input chain admits replies to what this machine dialed
/// and marked packets, such as the warm replication stream to the target's
/// own mesh address (section 6.13). Both drop everything else. Before its
/// `iifname` rules, the forward chain drops every new connection forwarded out
/// through the mesh interface: a guest opens no connection on the mesh
/// (section 6.9). The output chain accepts root's new connections out through
/// the interface and drops every other new one, a packet with no socket
/// included: every connection Quoro opens on the mesh comes from root, the
/// node agent or `quoro cluster promote` (section 7.3). Replies, the rest of
/// established connections and the kernel's own ICMP errors are not `new`, so
/// both rules let them through.
/// Any entry whose `source` or `address` is not an IPv4 address is skipped
/// with a warning: unparsed text is never written into nftables syntax.
///
/// On a voter, `votes` adds the vote listener's rules (sections 6.9 and 8.9).
/// Prerouting marks TCP to this machine's own mesh address on `votes.port`
/// from the mesh address of every other voter with the `node` role, like an
/// `allow` entry, so the input chain admits it ([`vote_rules`]). A
/// witness-only voter is never a source: only a machine with the `node` role
/// can be promoted. A caller on this machine reaches the listener over `lo`,
/// so it needs no rule. The input chain's first two rules drop TCP to the own
/// mesh address on `votes.port` that arrives on any interface other than the
/// mesh interface and `lo`, and, over `lo`, from any source other than that
/// address ([`vote_input_rules`]).
///
/// Every rule names an interface by `iifname` or `oifname`, never by index,
/// so the ruleset loads before [`INTERFACE`] exists.
#[must_use]
pub fn nft_ruleset(mesh: Option<&SpecMesh>, votes: Option<&SpecVotes>) -> String {
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
    vote_rules(&mut rules, mesh, votes);
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain forward {{");
    let _ = writeln!(
        rules,
        "        type filter hook forward priority filter - 2; policy accept;"
    );
    // A guest opens no connection on the mesh (section 6.9): nothing
    // forwarded opens one through it.
    let _ = writeln!(rules, "        oifname \"{INTERFACE}\" ct state new drop");
    let _ = writeln!(
        rules,
        "        iifname \"{INTERFACE}\" meta mark {MESH_MARK:#010x} accept"
    );
    let _ = writeln!(rules, "        iifname \"{INTERFACE}\" drop");
    let _ = writeln!(rules, "    }}");
    // Only root opens connections on the mesh (section 7.3).
    let _ = writeln!(rules, "    chain output {{");
    let _ = writeln!(
        rules,
        "        type filter hook output priority filter - 2; policy accept;"
    );
    let _ = writeln!(
        rules,
        "        oifname \"{INTERFACE}\" ct state new meta skuid 0 accept"
    );
    let _ = writeln!(rules, "        oifname \"{INTERFACE}\" ct state new drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "    chain input {{");
    let _ = writeln!(
        rules,
        "        type filter hook input priority filter - 2; policy accept;"
    );
    // First, so that no accept below it can let the listener's port through
    // from another interface.
    vote_input_rules(&mut rules, mesh, votes);
    let _ = writeln!(
        rules,
        "        iifname \"{INTERFACE}\" ct state established,related accept"
    );
    // An `allow` entry whose `address` is this machine's own mesh address,
    // the warm target's replication listener (section 6.13), reaches the
    // host instead of a guest.
    let _ = writeln!(
        rules,
        "        iifname \"{INTERFACE}\" meta mark {MESH_MARK:#010x} accept"
    );
    let _ = writeln!(rules, "        iifname \"{INTERFACE}\" drop");
    let _ = writeln!(rules, "    }}");
    let _ = writeln!(rules, "}}");
    rules
}

/// The prerouting rules that admit the vote listener (sections 6.9 and 8.9):
/// one per voter with the `node` role and an IPv4 mesh address other than
/// this machine's own. Each marks the packet, so the input chain accepts it.
fn vote_rules(rules: &mut String, mesh: &SpecMesh, votes: Option<&SpecVotes>) {
    let Some(votes) = votes else {
        return;
    };
    let Some(IpAddr::V4(own)) = crate::vote::mesh_ip(&mesh.address) else {
        warn!(
            address = %mesh.address,
            "this machine's mesh address is not an IPv4 address; the vote listener is not admitted"
        );
        return;
    };
    let mut sources: Vec<Ipv4Addr> = crate::vote::node_voter_addresses(votes)
        .filter_map(|(_, address)| match address {
            IpAddr::V4(address) if address != own => Some(address),
            _ => None,
        })
        .collect();
    sources.sort_unstable();
    sources.dedup();
    for source in sources {
        let _ = writeln!(
            rules,
            "        iifname \"{INTERFACE}\" ip saddr {source} ip daddr {own} tcp dport {} meta mark set {MESH_MARK:#010x}",
            votes.port
        );
    }
}

/// The input rules that keep the vote listener's TCP to the mesh interface
/// and loopback. They are this implementation's rules under section 6.9's
/// mesh firewall, for the listener of section 8.9. TCP to this machine's own
/// mesh address on `votes.port` is dropped when it arrives on an interface
/// other than [`INTERFACE`] and `lo`. Over `lo`, it is dropped unless its
/// source is that same mesh address, so only connections from the machine's
/// own mesh address reach the listener over loopback, and the listener's
/// owner check always applies to them. Nothing without `votes` or without an
/// IPv4 mesh address ([`vote_rules`] warns about the latter).
fn vote_input_rules(rules: &mut String, mesh: &SpecMesh, votes: Option<&SpecVotes>) {
    let Some(listener) = vote_listen_address(mesh, votes) else {
        return;
    };
    let (own, port) = (listener.ip(), listener.port());
    let _ = writeln!(
        rules,
        "        iifname != \"{INTERFACE}\" iifname != \"lo\" ip daddr {own} tcp dport {port} drop"
    );
    let _ = writeln!(
        rules,
        "        iifname \"lo\" ip daddr {own} tcp dport {port} ip saddr != {own} drop"
    );
}

/// The vote listener's address whose rules the ruleset of `mesh` and `votes`
/// loads ([`vote_input_rules`]): this machine's IPv4 mesh address on
/// `votes.port`. `None` without `votes`, or with a mesh address that is not
/// IPv4, whose ruleset has no such rules.
#[must_use]
pub fn vote_listen_address(mesh: &SpecMesh, votes: Option<&SpecVotes>) -> Option<SocketAddr> {
    let votes = votes?;
    let Some(IpAddr::V4(own)) = crate::vote::mesh_ip(&mesh.address) else {
        return None;
    };
    Some(SocketAddr::new(IpAddr::V4(own), votes.port))
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

/// Whether an apply sets `peer`'s endpoint on the device: when the device
/// does not have the peer yet (`on_device` is false), or when `previous`, the
/// mesh the last successful apply set, did not name this endpoint for the
/// peer's key.
///
/// Otherwise the endpoint stays as WireGuard has it, which may be one it
/// learned by roaming. A voter behind NAT gets its NAT's address as its
/// endpoint, and is reached once its own keepalive arrives (section 6.9,
/// ADR 0266). Setting the spec's endpoint again on every periodic apply would
/// undo that, and its peers would lose it until its next keepalive.
#[must_use]
pub fn sets_endpoint(peer: &MeshPeer, on_device: bool, previous: Option<&SpecMesh>) -> bool {
    let unchanged = previous.is_some_and(|previous| {
        previous
            .peers
            .iter()
            .any(|old| old.public_key == peer.public_key && old.endpoint == peer.endpoint)
    });
    !on_device || !unchanged
}

/// Applies and removes the mesh interface.
///
/// Implementations return a readable error string rather than an error type:
/// the manager logs it and retries, and the driver never needs to be inspected
/// by the rest of the agent.
pub trait MeshDriver: Send + Sync {
    /// Makes the interface equal to `mesh`, using `key` as its private key,
    /// and its firewall admit the vote listener of `votes` ([`nft_ruleset`]).
    /// `previous` is the mesh the last successful apply set, if any, less the
    /// peers whose endpoint a failed apply since may have changed: a peer's
    /// endpoint is set only when [`sets_endpoint`] says so.
    fn apply(
        &self,
        key: &MeshKey,
        mesh: &SpecMesh,
        votes: Option<&SpecVotes>,
        previous: Option<&SpecMesh>,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Removes the interface and its firewall table.
    fn remove(&self) -> impl Future<Output = Result<(), String>> + Send;
}

/// A driver for tests: it records every call and changes nothing.
#[derive(Clone, Default)]
pub struct RecordingMeshDriver {
    calls: Arc<Mutex<Vec<MeshCall>>>,
    previous: Arc<Mutex<Vec<Option<SpecMesh>>>>,
    votes: Arc<Mutex<Vec<Option<SpecVotes>>>>,
    failing: Arc<std::sync::atomic::AtomicBool>,
    failing_removes: Arc<std::sync::atomic::AtomicBool>,
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

    /// Makes every later [`MeshDriver::apply`] fail after it is recorded, or
    /// succeed again.
    pub fn fail_applies(&self, failing: bool) {
        self.failing
            .store(failing, std::sync::atomic::Ordering::SeqCst);
    }

    /// Makes every later [`MeshDriver::remove`] fail after it is recorded,
    /// or succeed again.
    pub fn fail_removes(&self, failing: bool) {
        self.failing_removes
            .store(failing, std::sync::atomic::Ordering::SeqCst);
    }

    /// The `previous` mesh of every [`MeshDriver::apply`] call, in order.
    #[must_use]
    pub fn previous(&self) -> Vec<Option<SpecMesh>> {
        self.previous
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The `votes` of every [`MeshDriver::apply`] call, in order.
    #[must_use]
    pub fn votes(&self) -> Vec<Option<SpecVotes>> {
        self.votes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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
        votes: Option<&SpecVotes>,
        previous: Option<&SpecMesh>,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let mesh = mesh.clone();
        let previous = previous.cloned();
        let calls = Arc::clone(&self.calls);
        let previous_meshes = Arc::clone(&self.previous);
        let applied_votes = Arc::clone(&self.votes);
        let votes = votes.cloned();
        let failing = self.failing.load(std::sync::atomic::Ordering::SeqCst);
        async move {
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(MeshCall::Apply(mesh));
            previous_meshes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(previous);
            applied_votes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(votes);
            if failing {
                Err("a failing test driver".to_owned())
            } else {
                Ok(())
            }
        }
    }

    fn remove(&self) -> impl Future<Output = Result<(), String>> + Send {
        let calls = Arc::clone(&self.calls);
        let failing = self
            .failing_removes
            .load(std::sync::atomic::Ordering::SeqCst);
        async move {
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(MeshCall::Remove);
            if failing {
                Err("a failing test driver".to_owned())
            } else {
                Ok(())
            }
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
        _votes: Option<&SpecVotes>,
        _previous: Option<&SpecMesh>,
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
    firewall: watch::Sender<Option<SocketAddr>>,
    /// The address the vote listener holds a socket on, or may be about to
    /// bind, as the listener reports it ([`ListenerLink::bound`]).
    bound: watch::Receiver<Option<SocketAddr>>,
    bound_tx: Arc<watch::Sender<Option<SocketAddr>>>,
}

/// What the vote listener and the [`MeshManager`] share
/// ([`MeshManager::listener_link`]): the address the manager lets the
/// listener listen on, and the address the listener reports it holds, so
/// that the manager can wait for the listener to stop before an apply.
pub struct ListenerLink {
    /// The address the listener may listen on ([`MeshManager::firewall`]).
    pub(crate) firewall: watch::Receiver<Option<SocketAddr>>,
    /// Where the listener reports the address it holds a socket on, or is
    /// about to bind; `None` once its socket is closed.
    pub(crate) bound: Arc<watch::Sender<Option<SocketAddr>>>,
}

impl ListenerLink {
    /// A link that follows `firewall` with no manager behind it, for tests
    /// that drive the listener alone. The receiver reads what the listener
    /// reports. Only the vote listener's tests use it, and they run on Linux.
    #[cfg(all(test, target_os = "linux"))]
    #[must_use]
    pub(crate) fn detached(
        firewall: watch::Receiver<Option<SocketAddr>>,
    ) -> (Self, watch::Receiver<Option<SocketAddr>>) {
        let (bound, bound_rx) = watch::channel(None);
        (
            Self {
                firewall,
                bound: Arc::new(bound),
            },
            bound_rx,
        )
    }
}

/// The address the vote listener may listen on for `mesh` and `votes`: the
/// one whose firewall rules the ruleset loads ([`vote_listen_address`]),
/// and only while votes are open (`spec.votes.listen`, section 8.9).
#[must_use]
pub fn open_listen_address(mesh: &SpecMesh, votes: Option<&SpecVotes>) -> Option<SocketAddr> {
    vote_listen_address(mesh, votes.filter(|votes| votes.open()))
}

impl<D: MeshDriver> MeshManager<D> {
    /// A manager that applies `desired` with `driver` and `key`.
    #[must_use]
    pub fn new(driver: D, key: MeshKey) -> Self {
        let (bound_tx, bound) = watch::channel(None);
        Self {
            driver,
            key,
            firewall: watch::Sender::new(None),
            bound,
            bound_tx: Arc::new(bound_tx),
        }
    }

    /// The address the vote listener may listen on
    /// ([`open_listen_address`]): set by a successful apply of a mesh whose
    /// `spec.votes` has `listen: true`, and `None` from the moment an apply
    /// fails, the mesh is removed, or an apply is about to change it.
    #[must_use]
    pub fn firewall(&self) -> watch::Receiver<Option<SocketAddr>> {
        self.firewall.subscribe()
    }

    /// The link the vote listener needs ([`ListenerLink`]): the listener
    /// listens only where [`MeshManager::firewall`] says, and reports its
    /// socket back, so that an apply that closes the listener waits for its
    /// socket to close before it configures any peer.
    #[must_use]
    pub fn listener_link(&self) -> ListenerLink {
        ListenerLink {
            firewall: self.firewall.subscribe(),
            bound: Arc::clone(&self.bound_tx),
        }
    }

    /// Before an apply whose listener address is `listen`: takes the
    /// listener's address away when it differs, and then, when the listener
    /// reports a socket other than on `listen`, waits until it reports none.
    /// Returns at once when the listener holds no socket, including when no
    /// listener runs.
    ///
    /// The listener reports an address before it binds and checks this
    /// manager's address after that ([`ListenerLink`]), so a listener that
    /// reads no socket here can no longer bind one.
    async fn close_listener_unless(&self, listen: Option<SocketAddr>) {
        if *self.firewall.borrow() != listen {
            self.publish_firewall(None);
        }
        let held = *self.bound.borrow();
        if held.is_none() || held == listen {
            return;
        }
        self.publish_firewall(None);
        let mut bound = self.bound.clone();
        // The manager holds a sender, so the channel stays open.
        let _ = bound.wait_for(Option::is_none).await;
    }

    /// Publishes the address the loaded firewall covers.
    fn publish_firewall(&self, address: Option<SocketAddr>) {
        self.firewall.send_if_modified(|current| {
            let changed = *current != address;
            *current = address;
            changed
        });
    }

    /// Applies the mesh of every snapshot until `stop` resolves.
    ///
    /// A snapshot whose `spec.mesh` or `spec.votes` differs from the last
    /// applied one, a snapshot after a failed apply, and every [`REAPPLY`]
    /// period all cause an apply. A snapshot without a mesh removes the interface; the first start
    /// also removes it once, so an interface left by a previous run cannot
    /// survive with stale peers.
    ///
    /// A failed apply is retried without waiting for a snapshot or the
    /// period: after [`RETRY_FIRST`], then after twice the last wait, up to
    /// [`REAPPLY`] ([`retry_delay`]). A successful apply resets the wait. An
    /// apply whose firewall does not load leaves WireGuard with no peers, so
    /// the retry is what brings the peers back within seconds. A machine
    /// without `nft` installed keeps an empty mesh and retries; the failure is
    /// logged at `error` at most once per [`REAPPLY`], with how many were held
    /// back since, and at `debug` otherwise. A snapshot without a mesh ends
    /// the retries of an apply; a removal that fails is retried with the same
    /// growing wait, and logged at `warn` under the same limit.
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
                () = sleep_until(state.retry_at) => {
                    // Consumed here, so a pass that schedules nothing never
                    // finds it due again.
                    state.retry_at = None;
                    let current = desired.borrow_and_update().clone();
                    self.sync(current.as_ref(), &mut state, false).await;
                }
            }
        }
    }

    /// One evaluation of the newest snapshot.
    async fn sync(&self, snapshot: Option<&Snapshot>, state: &mut ManagerState, force: bool) {
        let mesh = snapshot.and_then(|snapshot| snapshot.state.spec.mesh.as_ref());
        let votes = snapshot.and_then(|snapshot| snapshot.state.spec.votes.as_ref());
        match mesh {
            Some(mesh) => {
                if !state.should_apply(mesh, votes, force) {
                    return;
                }
                if std::mem::take(&mut state.removal_failed) {
                    // The retries of a removal do not set this apply's wait.
                    state.retry_delay = None;
                }
                // Section 8.9: a voter that applies a state without `listen`
                // stops listening before it configures that state's peers.
                // The listener loses its address first, and a socket it
                // holds anywhere this state does not listen is closed before
                // the driver runs.
                let listen = open_listen_address(mesh, votes);
                self.close_listener_unless(listen).await;
                match self
                    .driver
                    .apply(&self.key, mesh, votes, state.applied.as_ref())
                    .await
                {
                    Ok(()) => {
                        state.applied = Some(mesh.clone());
                        state.applied_votes = votes.cloned();
                        state.failed = false;
                        state.removed = false;
                        state.last_apply = Some(tokio::time::Instant::now());
                        state.retry_delay = None;
                        state.retry_at = None;
                        self.publish_firewall(listen);
                    }
                    Err(error) => {
                        self.publish_firewall(None);
                        let now = tokio::time::Instant::now();
                        match state.error_log.admit(now) {
                            Some(suppressed) => error!(
                                error = %error,
                                suppressed,
                                "could not apply the mesh; retrying"
                            ),
                            None => debug!(error = %error, "could not apply the mesh; retrying"),
                        }
                        state.schedule_retry(now);
                        state.failed = true;
                        // The apply may have changed the device before it
                        // failed, so a snapshot without a mesh removes it.
                        state.removed = false;
                        // The apply may have set WireGuard before a later
                        // step failed, so the device may hold this mesh's
                        // endpoints. Forget the peers whose endpoint it
                        // changed: the next apply sets theirs again.
                        if let Some(applied) = &mut state.applied {
                            applied.peers.retain(|old| {
                                !mesh.peers.iter().any(|new| {
                                    new.public_key == old.public_key && new.endpoint != old.endpoint
                                })
                            });
                        }
                    }
                }
            }
            None => {
                self.publish_firewall(None);
                // No mesh: the retries of an apply end here. Only a removal
                // that fails below schedules a retry, of itself.
                let removal_wait = if state.removal_failed {
                    state.retry_delay
                } else {
                    None
                };
                state.retry_delay = None;
                state.retry_at = None;
                if state.applied.is_none() && state.removed {
                    return;
                }
                match self.driver.remove().await {
                    Ok(()) => {
                        state.applied = None;
                        state.applied_votes = None;
                        state.failed = false;
                        state.removed = true;
                        state.removal_failed = false;
                        state.last_apply = None;
                    }
                    Err(error) => {
                        let now = tokio::time::Instant::now();
                        match state.remove_log.admit(now) {
                            Some(suppressed) => warn!(
                                error = %error,
                                suppressed,
                                "could not remove the mesh; retrying"
                            ),
                            None => debug!(error = %error, "could not remove the mesh; retrying"),
                        }
                        state.retry_delay = removal_wait;
                        state.schedule_retry(now);
                        state.removal_failed = true;
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
    /// The `spec.votes` the firewall of the last successful apply admits.
    applied_votes: Option<SpecVotes>,
    failed: bool,
    last_apply: Option<tokio::time::Instant>,
    removed: bool,
    /// The wait before the pending retry of a failed apply or removal.
    retry_delay: Option<Duration>,
    /// When the pending retry of a failed apply or removal runs.
    retry_at: Option<tokio::time::Instant>,
    /// Whether the pending retry is of a removal that failed.
    removal_failed: bool,
    /// The `error` for a failed apply.
    error_log: LogLimit,
    /// The `warn` for a failed removal.
    remove_log: LogLimit,
}

/// Lets a repeated `error` through at most once per [`REAPPLY`], counting
/// the ones it holds back.
#[derive(Default)]
struct LogLimit {
    last: Option<tokio::time::Instant>,
    suppressed: u64,
}

impl LogLimit {
    /// At `now`, `Some` with the number held back since the last one when
    /// this one goes out, or `None` when it is held back.
    fn admit(&mut self, now: tokio::time::Instant) -> Option<u64> {
        match self.last {
            Some(last) if now.saturating_duration_since(last) < REAPPLY => {
                self.suppressed = self.suppressed.saturating_add(1);
                None
            }
            _ => {
                self.last = Some(now);
                Some(std::mem::take(&mut self.suppressed))
            }
        }
    }
}

/// Resolves at `at`, or never without one.
async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

impl ManagerState {
    /// Schedules the next retry after a failure at `now`, with a wait one
    /// step longer than the last ([`retry_delay`]).
    fn schedule_retry(&mut self, now: tokio::time::Instant) {
        let delay = retry_delay(self.retry_delay);
        self.retry_delay = Some(delay);
        self.retry_at = Some(now + delay);
    }

    /// Whether this mesh must be applied now.
    fn should_apply(&self, mesh: &SpecMesh, votes: Option<&SpecVotes>, force: bool) -> bool {
        force
            || self.failed
            || self.applied.as_ref() != Some(mesh)
            || self.applied_votes.as_ref() != votes
            || self.last_apply.is_none_or(|last| last.elapsed() >= REAPPLY)
    }
}

/// The Linux driver: `ip` for the link, address and routes, generic netlink for
/// WireGuard, and `nft` for the firewall.
#[cfg(target_os = "linux")]
mod linux {
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::time::Duration;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use futures_util::StreamExt as _;
    use genetlink::GenetlinkHandle;
    use genetlink::new_connection;
    use netlink_packet_core::{
        NLM_F_ACK, NLM_F_DUMP, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload,
    };
    use netlink_packet_generic::GenlMessage;
    use netlink_packet_wireguard::{
        WireguardAddressFamily, WireguardAllowedIp, WireguardAllowedIpAttr, WireguardAttribute,
        WireguardCmd, WireguardMessage, WireguardPeer, WireguardPeerAttribute, WireguardPeerFlags,
    };
    use quoro_proto::wire::{MeshPeer, SpecMesh, SpecVotes};
    use tokio::io::AsyncWriteExt as _;
    use tokio::process::Command;
    use tracing::warn;

    use super::{
        INTERFACE, MeshDriver, MeshKey, desired_routes, nft_ruleset, parse_cidr, parse_endpoint,
        sets_endpoint,
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
        async fn apply(
            &self,
            key: &MeshKey,
            mesh: &SpecMesh,
            votes: Option<&SpecVotes>,
            previous: Option<&SpecMesh>,
        ) -> Result<(), String> {
            let key = key.clone();
            let mesh = mesh.clone();
            let votes = votes.cloned();
            let previous = previous.cloned();
            apply(&key, &mesh, votes.as_ref(), previous.as_ref()).await
        }

        async fn remove(&self) -> Result<(), String> {
            remove().await
        }
    }

    /// Applies one mesh specification ([`apply_steps`]).
    async fn apply(
        key: &MeshKey,
        mesh: &SpecMesh,
        votes: Option<&SpecVotes>,
        previous: Option<&SpecMesh>,
    ) -> Result<(), String> {
        let ip = find_tool("ip", "iproute2")?;
        let own = parse_cidr(&mesh.address)
            .ok_or_else(|| format!("mesh address {:?} is not an IPv4 CIDR", mesh.address))?;
        let IpAddr::V4(own_address) = own.0 else {
            return Err(format!("mesh address {:?} is not IPv4", mesh.address));
        };
        let mut steps = HostSteps {
            key,
            ip,
            own_address,
            own_prefix: own.1,
        };
        apply_steps(&mut steps, mesh, votes, previous).await
    }

    /// The steps of one apply, in the order [`apply_steps`] runs them.
    trait ApplySteps {
        /// Creates the link if it is missing, sets its address and brings it
        /// up.
        fn link(&mut self) -> impl Future<Output = Result<(), String>> + Send;
        /// Loads `ruleset` with `nft -f -`.
        fn ruleset(&mut self, ruleset: &str) -> impl Future<Output = Result<(), String>> + Send;
        /// Configures WireGuard with the peers of `mesh` ([`set_wireguard`]).
        fn wireguard(
            &mut self,
            mesh: &SpecMesh,
            previous: Option<&SpecMesh>,
        ) -> impl Future<Output = Result<(), String>> + Send;
        /// Sets the routes of `mesh` and removes the others.
        fn routes(&mut self, mesh: &SpecMesh) -> impl Future<Output = Result<(), String>> + Send;
    }

    /// Runs one apply's steps: the link and its address, then the firewall,
    /// then WireGuard, then the routes.
    ///
    /// The firewall comes before WireGuard, so this apply configures no peer
    /// before the ruleset it computed is loaded. The ruleset needs
    /// nothing from the link: every rule names an interface by `iifname` or
    /// `oifname`, which match by name and load before the interface exists
    /// ([`nft_ruleset`]). The link comes first anyway, because creating it
    /// loads the WireGuard module that the generic netlink family needs, and
    /// a failed ruleset still needs the device to remove peers from.
    ///
    /// When the ruleset does not load, WireGuard is configured with no peers,
    /// which also removes every peer an earlier apply set, and the ruleset's
    /// error is returned, with the removal's error after it if that failed
    /// too.
    async fn apply_steps<S: ApplySteps>(
        steps: &mut S,
        mesh: &SpecMesh,
        votes: Option<&SpecVotes>,
        previous: Option<&SpecMesh>,
    ) -> Result<(), String> {
        steps.link().await?;
        if let Err(error) = steps.ruleset(&nft_ruleset(Some(mesh), votes)).await {
            let no_peers = SpecMesh {
                peers: Vec::new(),
                ..mesh.clone()
            };
            return Err(match steps.wireguard(&no_peers, None).await {
                Ok(()) => error,
                Err(cleared) => format!("{error}; could not remove the mesh peers: {cleared}"),
            });
        }
        steps.wireguard(mesh, previous).await?;
        steps.routes(mesh).await
    }

    /// The steps on this host, with its `ip`, `nft` and generic netlink.
    struct HostSteps<'a> {
        key: &'a MeshKey,
        ip: PathBuf,
        own_address: Ipv4Addr,
        own_prefix: u8,
    }

    impl ApplySteps for HostSteps<'_> {
        async fn link(&mut self) -> Result<(), String> {
            let ip = &self.ip;
            // Creating the link loads the WireGuard module, so it must happen
            // before the generic netlink family is resolved.
            if !Path::new("/sys/class/net").join(INTERFACE).exists() {
                run(ip, &["link", "add", "dev", INTERFACE, "type", "wireguard"]).await?;
            }
            let address = format!("{}/{}", self.own_address, self.own_prefix);
            run(ip, &["address", "replace", &address, "dev", INTERFACE]).await?;
            remove_other_addresses(ip, self.own_address, self.own_prefix).await?;
            run(ip, &["link", "set", "dev", INTERFACE, "up"]).await?;
            Ok(())
        }

        async fn ruleset(&mut self, ruleset: &str) -> Result<(), String> {
            // Looked up here, so a missing `nft` is a ruleset that did not
            // load.
            let nft = find_tool("nft", "nftables")?;
            run_with_stdin(&nft, &["-f", "-"], ruleset).await?;
            Ok(())
        }

        async fn wireguard(
            &mut self,
            mesh: &SpecMesh,
            previous: Option<&SpecMesh>,
        ) -> Result<(), String> {
            set_wireguard(self.key, mesh, previous).await
        }

        async fn routes(&mut self, mesh: &SpecMesh) -> Result<(), String> {
            let desired = desired_routes(mesh);
            for cidr in &desired {
                run(&self.ip, &["route", "replace", cidr, "dev", INTERFACE]).await?;
            }
            let own = (IpAddr::V4(self.own_address), self.own_prefix);
            remove_other_routes(&self.ip, &desired, own).await
        }
    }

    /// Removes the interface and then the firewall table ([`remove_steps`]).
    async fn remove() -> Result<(), String> {
        remove_steps(&mut HostRemoval).await
    }

    /// The steps of a removal, in the order [`remove_steps`] runs them.
    trait RemoveSteps {
        /// Deletes the WireGuard link, and its peers with it, if it exists.
        fn delete_link(&mut self) -> impl Future<Output = Result<(), String>> + Send;
        /// Loads `ruleset` with `nft -f -`.
        fn ruleset(&mut self, ruleset: &str) -> impl Future<Output = Result<(), String>> + Send;
    }

    /// Runs a removal: the link first, then the table, so that no peer ever
    /// runs without the table. A link that cannot be deleted keeps the table
    /// and returns its error.
    async fn remove_steps<S: RemoveSteps>(steps: &mut S) -> Result<(), String> {
        steps.delete_link().await?;
        if let Err(error) = steps.ruleset(&nft_ruleset(None, None)).await {
            // The table may never have been created on this machine.
            if !error.contains("No such file or directory") {
                return Err(error);
            }
        }
        Ok(())
    }

    /// The removal on this host, with its `ip` and `nft`.
    struct HostRemoval;

    impl RemoveSteps for HostRemoval {
        async fn delete_link(&mut self) -> Result<(), String> {
            let ip = find_tool("ip", "iproute2")?;
            if Path::new("/sys/class/net").join(INTERFACE).exists() {
                run(&ip, &["link", "del", "dev", INTERFACE]).await?;
            }
            Ok(())
        }

        async fn ruleset(&mut self, ruleset: &str) -> Result<(), String> {
            let nft = find_tool("nft", "nftables")?;
            run_with_stdin(&nft, &["-f", "-"], ruleset).await?;
            Ok(())
        }
    }

    /// Sets the private key, listen port and peers in one `SetDevice` request.
    ///
    /// Peers are updated in place, never replaced as a set: replacing them
    /// would drop the endpoint the hub learned from each node and the session
    /// with it, so the edge could not reach a node until that node's next
    /// keepalive. Every apply, the periodic one included, would cut the edge
    /// off its backends. For the same reason a peer's endpoint is set only
    /// when it is new or changed since `previous` ([`sets_endpoint`]), so an
    /// endpoint learned by roaming survives a re-apply. A peer that left
    /// `spec.mesh.peers` is removed on its own. A peer whose public key is not
    /// 32 base64 bytes is skipped with a warning rather than failing the whole
    /// apply.
    async fn set_wireguard(
        key: &MeshKey,
        mesh: &SpecMesh,
        previous: Option<&SpecMesh>,
    ) -> Result<(), String> {
        let (connection, mut handle, _) =
            new_connection().map_err(|e| format!("could not open generic netlink: {e}"))?;
        tokio::spawn(connection);

        let current = current_peer_keys(&mut handle).await?;
        let attributes = vec![
            WireguardAttribute::IfName(INTERFACE.to_owned()),
            WireguardAttribute::PrivateKey(key.private),
            WireguardAttribute::ListenPort(mesh.listen_port),
            WireguardAttribute::Peers(peer_updates(&mesh.peers, &current, previous)),
        ];
        let genlmsg: GenlMessage<WireguardMessage> = GenlMessage::from_payload(WireguardMessage {
            cmd: WireguardCmd::SetDevice,
            attributes,
        });
        let mut nlmsg = NetlinkMessage::from(genlmsg);
        nlmsg.header.flags = NLM_F_REQUEST | NLM_F_ACK;

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

    /// The public keys of the peers the interface has now, read with a
    /// `GetDevice` dump.
    async fn current_peer_keys(handle: &mut GenetlinkHandle) -> Result<Vec<[u8; 32]>, String> {
        let genlmsg: GenlMessage<WireguardMessage> = GenlMessage::from_payload(WireguardMessage {
            cmd: WireguardCmd::GetDevice,
            attributes: vec![WireguardAttribute::IfName(INTERFACE.to_owned())],
        });
        let mut nlmsg = NetlinkMessage::from(genlmsg);
        nlmsg.header.flags = NLM_F_REQUEST | NLM_F_DUMP;
        let mut responses = handle
            .request(nlmsg)
            .await
            .map_err(|e| format!("could not send the WireGuard query: {e}"))?;
        let mut keys = Vec::new();
        while let Some(item) = responses.next().await {
            let packet = item.map_err(|e| format!("could not read the WireGuard device: {e}"))?;
            match packet.payload {
                NetlinkPayload::InnerMessage(message) => {
                    keys.extend(peer_keys(&message.payload.attributes));
                }
                NetlinkPayload::Error(error) if error.code.is_some() => {
                    return Err(format!(
                        "the kernel refused the WireGuard query: {}",
                        error.to_io()
                    ));
                }
                NetlinkPayload::Done(_) => break,
                _ => {}
            }
        }
        Ok(keys)
    }

    /// The public key of every peer in one `GetDevice` answer.
    fn peer_keys(attributes: &[WireguardAttribute]) -> Vec<[u8; 32]> {
        let mut keys = Vec::new();
        for attribute in attributes {
            if let WireguardAttribute::Peers(peers) = attribute {
                for WireguardPeer(peer) in peers {
                    for nla in peer {
                        if let WireguardPeerAttribute::PublicKey(key) = nla {
                            keys.push(*key);
                        }
                    }
                }
            }
        }
        keys
    }

    /// The peer list of one `SetDevice` request: every desired peer, updated in
    /// place with its allowed IPs replaced, and a removal for every current peer
    /// that is no longer desired. `previous` is the mesh the last successful
    /// apply set ([`sets_endpoint`]).
    pub(super) fn peer_updates(
        desired: &[MeshPeer],
        current: &[[u8; 32]],
        previous: Option<&SpecMesh>,
    ) -> Vec<WireguardPeer> {
        let mut updates: Vec<WireguardPeer> = desired
            .iter()
            .filter_map(|wanted| peer(wanted, current, previous))
            .collect();
        let kept: Vec<[u8; 32]> = updates
            .iter()
            .flat_map(|WireguardPeer(attributes)| attributes.iter())
            .filter_map(|attribute| match attribute {
                WireguardPeerAttribute::PublicKey(key) => Some(*key),
                _ => None,
            })
            .collect();
        for key in current {
            if !kept.contains(key) {
                updates.push(WireguardPeer(vec![
                    WireguardPeerAttribute::PublicKey(*key),
                    WireguardPeerAttribute::Flags(WireguardPeerFlags::RemoveMe),
                ]));
            }
        }
        updates
    }

    /// Builds one netlink peer, or `None` when its public key is unusable.
    ///
    /// It carries `ReplaceAllowedIps`, so an entry that left the peer's
    /// allowed IPs disappears while its endpoint and session stay. It carries
    /// the endpoint only when [`sets_endpoint`] says so for a device whose
    /// peers are `current`.
    fn peer(
        peer: &MeshPeer,
        current: &[[u8; 32]],
        previous: Option<&SpecMesh>,
    ) -> Option<WireguardPeer> {
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
        let mut attributes = vec![
            WireguardPeerAttribute::PublicKey(public_key),
            WireguardPeerAttribute::Flags(WireguardPeerFlags::ReplaceAllowedIps),
        ];
        if let Some(endpoint) = peer
            .endpoint
            .as_ref()
            .filter(|_| sets_endpoint(peer, current.contains(&public_key), previous))
        {
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

    #[cfg(test)]
    mod tests {
        use super::*;

        fn key(byte: u8) -> [u8; 32] {
            [byte; 32]
        }

        fn mesh_peer(byte: u8, endpoint: Option<&str>) -> MeshPeer {
            MeshPeer {
                node_id: format!("nod_{byte}"),
                public_key: STANDARD.encode(key(byte)),
                endpoint: endpoint.map(str::to_owned),
                allowed_ips: vec![format!("10.42.{byte}.0/24")],
                persistent_keepalive_s: None,
            }
        }

        fn flags_of(peer: &WireguardPeer) -> Vec<WireguardPeerFlags> {
            peer.0
                .iter()
                .filter_map(|attribute| match attribute {
                    WireguardPeerAttribute::Flags(flags) => Some(*flags),
                    _ => None,
                })
                .collect()
        }

        fn key_of(peer: &WireguardPeer) -> [u8; 32] {
            peer.0
                .iter()
                .find_map(|attribute| match attribute {
                    WireguardPeerAttribute::PublicKey(key) => Some(*key),
                    _ => None,
                })
                .expect("every update names its peer")
        }

        /// The hub's periodic re-apply: the same peers, no endpoint in the spec.
        /// Each peer is updated in place, so the endpoint the hub learned and
        /// the session survive; nothing is removed.
        #[test]
        fn an_unchanged_peer_set_is_updated_in_place() {
            let desired = [mesh_peer(1, None), mesh_peer(2, None)];
            let updates = peer_updates(&desired, &[key(1), key(2)], None);
            assert_eq!(updates.len(), 2);
            for update in &updates {
                assert_eq!(
                    flags_of(update),
                    vec![WireguardPeerFlags::ReplaceAllowedIps]
                );
                assert!(
                    !update
                        .0
                        .iter()
                        .any(|attribute| matches!(attribute, WireguardPeerAttribute::Endpoint(_))),
                    "a hub peer without an endpoint must not get one set"
                );
            }
        }

        /// An entry between two voters may have no endpoint (the other voter
        /// dials in) and still carries the keepalive (section 6.9, ADR 0266).
        #[test]
        fn a_peer_without_an_endpoint_keeps_its_keepalive() {
            let mut desired = mesh_peer(1, None);
            desired.persistent_keepalive_s = Some(25);
            let update = peer(&desired, &[], None).expect("a valid key");
            assert!(update.0.iter().any(|attribute| matches!(
                attribute,
                WireguardPeerAttribute::PersistentKeepalive(25)
            )));
            assert!(
                !update
                    .0
                    .iter()
                    .any(|attribute| matches!(attribute, WireguardPeerAttribute::Endpoint(_)))
            );
        }

        /// A periodic re-apply of the same mesh leaves the endpoint WireGuard
        /// learned by roaming; a changed one, or a peer new to the device,
        /// gets the spec's (section 6.9, ADR 0266).
        #[test]
        fn an_unchanged_endpoint_is_not_set_again() {
            let has_endpoint = |update: &WireguardPeer| {
                update
                    .0
                    .iter()
                    .any(|attribute| matches!(attribute, WireguardPeerAttribute::Endpoint(_)))
            };
            let desired = [mesh_peer(1, Some("192.0.2.10:51820"))];
            let previous = SpecMesh {
                address: "10.43.0.2/16".to_owned(),
                listen_port: 51820,
                peers: desired.to_vec(),
                allow: Vec::new(),
            };
            let updates = peer_updates(&desired, &[key(1)], Some(&previous));
            assert!(!has_endpoint(&updates[0]), "unchanged and on the device");
            let updates = peer_updates(&desired, &[], Some(&previous));
            assert!(has_endpoint(&updates[0]), "the device lost the peer");
            let moved = [mesh_peer(1, Some("192.0.2.11:51820"))];
            let updates = peer_updates(&moved, &[key(1)], Some(&previous));
            assert!(has_endpoint(&updates[0]), "the endpoint changed");
        }

        #[test]
        fn a_peer_that_left_the_spec_is_removed_alone() {
            let desired = [mesh_peer(1, None)];
            let updates = peer_updates(&desired, &[key(1), key(3)], None);
            assert_eq!(updates.len(), 2);
            assert_eq!(key_of(&updates[0]), key(1));
            assert_eq!(
                flags_of(&updates[0]),
                vec![WireguardPeerFlags::ReplaceAllowedIps]
            );
            assert_eq!(key_of(&updates[1]), key(3));
            assert_eq!(flags_of(&updates[1]), vec![WireguardPeerFlags::RemoveMe]);
        }

        #[test]
        fn a_new_peer_is_added_without_touching_the_others() {
            let desired = [mesh_peer(1, None), mesh_peer(2, Some("192.0.2.10:51820"))];
            let updates = peer_updates(&desired, &[key(1)], None);
            assert_eq!(updates.len(), 2);
            assert!(updates.iter().all(|update| {
                !flags_of(update)
                    .iter()
                    .any(|flags| flags.contains(WireguardPeerFlags::RemoveMe))
            }));
        }

        #[test]
        fn peer_keys_reads_every_peer_of_a_device_answer() {
            let attributes = vec![
                WireguardAttribute::IfName(INTERFACE.to_owned()),
                WireguardAttribute::Peers(vec![
                    WireguardPeer(vec![WireguardPeerAttribute::PublicKey(key(1))]),
                    WireguardPeer(vec![
                        WireguardPeerAttribute::PersistentKeepalive(25),
                        WireguardPeerAttribute::PublicKey(key(2)),
                    ]),
                ]),
            ];
            assert_eq!(peer_keys(&attributes), vec![key(1), key(2)]);
        }

        /// One step a [`FakeSteps`] ran.
        #[derive(Clone, Debug, PartialEq, Eq)]
        enum Step {
            Link,
            DeleteLink,
            /// The ruleset text it was given.
            Ruleset(String),
            /// The number of peers WireGuard was given, and whether a
            /// previous mesh came with them.
            Wireguard(usize, bool),
            Routes,
            /// Whether [`FakeSteps::probe`] accepted a TCP connection when
            /// WireGuard was about to be configured.
            Listening(bool),
        }

        /// Steps that record what ran and fail where they are told to.
        #[derive(Default)]
        struct FakeSteps {
            ran: Vec<Step>,
            fail_link: bool,
            fail_ruleset: bool,
            fail_wireguard: bool,
            fail_delete_link: bool,
            /// The peers the device has after the steps so far.
            peers: usize,
            /// An address to try a TCP connection to just before WireGuard
            /// is configured, recorded as [`Step::Listening`].
            probe: Option<std::net::SocketAddr>,
        }

        fn outcome(failing: bool, what: &str) -> Result<(), String> {
            if failing {
                Err(format!("{what} failed"))
            } else {
                Ok(())
            }
        }

        impl ApplySteps for FakeSteps {
            async fn link(&mut self) -> Result<(), String> {
                self.ran.push(Step::Link);
                outcome(self.fail_link, "the link")
            }

            async fn ruleset(&mut self, ruleset: &str) -> Result<(), String> {
                self.ran.push(Step::Ruleset(ruleset.to_owned()));
                outcome(self.fail_ruleset, "nft")
            }

            async fn wireguard(
                &mut self,
                mesh: &SpecMesh,
                previous: Option<&SpecMesh>,
            ) -> Result<(), String> {
                if let Some(probe) = self.probe {
                    // A blocking connect: it does not yield, so no other
                    // task, the vote listener included, runs between the
                    // steps before it and this probe.
                    let listening = std::net::TcpStream::connect(probe).is_ok();
                    self.ran.push(Step::Listening(listening));
                }
                self.ran
                    .push(Step::Wireguard(mesh.peers.len(), previous.is_some()));
                outcome(self.fail_wireguard, "WireGuard")?;
                self.peers = mesh.peers.len();
                Ok(())
            }

            async fn routes(&mut self, _mesh: &SpecMesh) -> Result<(), String> {
                self.ran.push(Step::Routes);
                Ok(())
            }
        }

        impl RemoveSteps for FakeSteps {
            async fn delete_link(&mut self) -> Result<(), String> {
                self.ran.push(Step::DeleteLink);
                outcome(self.fail_delete_link, "ip link del")?;
                self.peers = 0;
                Ok(())
            }

            async fn ruleset(&mut self, ruleset: &str) -> Result<(), String> {
                self.ran.push(Step::Ruleset(ruleset.to_owned()));
                outcome(self.fail_ruleset, "nft")
            }
        }

        /// A driver that runs [`apply_steps`] and [`remove_steps`] over one
        /// shared [`FakeSteps`].
        #[derive(Clone)]
        struct StepsDriver(std::sync::Arc<tokio::sync::Mutex<FakeSteps>>);

        impl MeshDriver for StepsDriver {
            fn apply(
                &self,
                _key: &MeshKey,
                mesh: &SpecMesh,
                votes: Option<&SpecVotes>,
                previous: Option<&SpecMesh>,
            ) -> impl Future<Output = Result<(), String>> + Send {
                let steps = std::sync::Arc::clone(&self.0);
                let mesh = mesh.clone();
                let votes = votes.cloned();
                let previous = previous.cloned();
                async move {
                    let mut steps = steps.lock().await;
                    apply_steps(&mut *steps, &mesh, votes.as_ref(), previous.as_ref()).await
                }
            }

            fn remove(&self) -> impl Future<Output = Result<(), String>> + Send {
                let steps = std::sync::Arc::clone(&self.0);
                async move { remove_steps(&mut *steps.lock().await).await }
            }
        }

        fn two_peer_mesh() -> SpecMesh {
            SpecMesh {
                address: "10.43.0.2/16".to_owned(),
                listen_port: 51820,
                peers: vec![mesh_peer(1, None), mesh_peer(2, Some("192.0.2.10:51820"))],
                allow: Vec::new(),
            }
        }

        /// The ruleset this apply computed is loaded before WireGuard gets
        /// any peer, and the routes come last.
        #[tokio::test]
        async fn the_ruleset_loads_before_any_peer_is_configured() {
            let mesh = two_peer_mesh();
            let mut steps = FakeSteps::default();
            apply_steps(&mut steps, &mesh, None, Some(&mesh))
                .await
                .unwrap();
            assert_eq!(
                steps.ran,
                vec![
                    Step::Link,
                    Step::Ruleset(nft_ruleset(Some(&mesh), None)),
                    Step::Wireguard(2, true),
                    Step::Routes,
                ]
            );
        }

        /// A ruleset that does not load leaves WireGuard with no peers, which
        /// removes those an earlier apply set, and the apply returns the
        /// ruleset's error. No route is set.
        #[tokio::test]
        async fn a_failed_ruleset_leaves_wireguard_with_no_peers() {
            let mesh = two_peer_mesh();
            let mut steps = FakeSteps {
                fail_ruleset: true,
                ..FakeSteps::default()
            };
            let error = apply_steps(&mut steps, &mesh, None, Some(&mesh))
                .await
                .unwrap_err();
            assert_eq!(error, "nft failed");
            assert_eq!(
                steps.ran,
                vec![
                    Step::Link,
                    Step::Ruleset(nft_ruleset(Some(&mesh), None)),
                    Step::Wireguard(0, false),
                ]
            );

            // When removing the peers fails too, both errors are returned,
            // the ruleset's first.
            let mut steps = FakeSteps {
                fail_ruleset: true,
                fail_wireguard: true,
                ..FakeSteps::default()
            };
            let error = apply_steps(&mut steps, &mesh, None, None)
                .await
                .unwrap_err();
            assert_eq!(
                error,
                "nft failed; could not remove the mesh peers: WireGuard failed"
            );
            assert_eq!(steps.ran.last(), Some(&Step::Wireguard(0, false)));
        }

        /// A ruleset that does not load leaves the device with no peers, and
        /// the manager retries after [`super::super::RETRY_FIRST`] without a
        /// new snapshot: once the ruleset loads, the peers are back, long
        /// before the reapply period.
        #[tokio::test(start_paused = true)]
        async fn a_failed_ruleset_is_retried_within_seconds_and_the_peers_come_back() {
            let mesh = two_peer_mesh();
            let votes = SpecVotes {
                version: 1,
                port: 51821,
                voters: Vec::new(),
                listen: Some(true),
            };
            let state = quoro_proto::wire::DesiredState {
                rev: 1,
                issued_at: String::new(),
                term: 0,
                spec: quoro_proto::wire::Spec {
                    mesh: Some(mesh.clone()),
                    votes: Some(votes.clone()),
                    ..quoro_proto::wire::Spec::default()
                },
                signature: String::new(),
            };
            let snapshot = crate::reconcile::Snapshot { rev: 1, state };
            let (_desired_tx, desired_rx) = tokio::sync::watch::channel(Some(snapshot));
            let steps = std::sync::Arc::new(tokio::sync::Mutex::new(FakeSteps {
                fail_ruleset: true,
                ..FakeSteps::default()
            }));
            let manager = super::super::MeshManager::new(
                StepsDriver(std::sync::Arc::clone(&steps)),
                MeshKey { private: [7; 32] },
            );
            let firewall = manager.firewall();
            let task = tokio::spawn(manager.run(desired_rx, std::future::pending()));

            tokio::time::sleep(Duration::from_millis(1)).await;
            {
                let steps = steps.lock().await;
                assert_eq!(steps.peers, 0, "{:?}", steps.ran);
                assert_eq!(steps.ran.last(), Some(&Step::Wireguard(0, false)));
            }
            assert_eq!(*firewall.borrow(), None, "no firewall, no vote listener");
            steps.lock().await.fail_ruleset = false;

            tokio::time::sleep(super::super::RETRY_FIRST - Duration::from_millis(10)).await;
            assert_eq!(steps.lock().await.peers, 0, "no retry before its time");
            tokio::time::sleep(Duration::from_millis(20)).await;
            {
                let steps = steps.lock().await;
                assert_eq!(steps.peers, 2, "{:?}", steps.ran);
                assert_eq!(
                    steps.ran[steps.ran.len() - 3..],
                    [
                        Step::Ruleset(nft_ruleset(Some(&mesh), Some(&votes))),
                        Step::Wireguard(2, false),
                        Step::Routes,
                    ]
                );
            }
            assert_eq!(
                *firewall.borrow(),
                Some("10.43.0.2:51821".parse().unwrap()),
                "the loaded firewall covers the vote listener"
            );
            task.abort();
        }

        /// Section 8.9: a voter that applies a desired state without
        /// `listen` stops listening before it configures that state's
        /// peers. A real vote listener on 127.0.0.1 is bound while votes are
        /// open; the next state drops `listen`, and the manager runs the
        /// apply only once the listener's socket is closed, so nothing
        /// accepts a connection when WireGuard gets its peers. The
        /// listener here follows a state in which votes stay open, so only
        /// the manager's wait closes it before the peers.
        #[tokio::test]
        async fn a_state_without_listen_closes_the_listener_before_any_peer() {
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let listener_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let mesh = SpecMesh {
                address: "127.0.0.1/8".to_owned(),
                ..two_peer_mesh()
            };
            let open = SpecVotes {
                version: 1,
                port,
                voters: Vec::new(),
                listen: Some(true),
            };
            let closed = SpecVotes {
                listen: None,
                ..open.clone()
            };
            let state = |votes: &SpecVotes| {
                Some(crate::reconcile::Snapshot {
                    rev: 1,
                    state: quoro_proto::wire::DesiredState {
                        rev: 1,
                        issued_at: String::new(),
                        term: 0,
                        spec: quoro_proto::wire::Spec {
                            mesh: Some(mesh.clone()),
                            votes: Some(votes.clone()),
                            ..quoro_proto::wire::Spec::default()
                        },
                        signature: String::new(),
                    },
                })
            };
            let (desired_tx, desired_rx) = tokio::sync::watch::channel(state(&open));
            let steps = std::sync::Arc::new(tokio::sync::Mutex::new(FakeSteps {
                probe: Some(listener_addr),
                ..FakeSteps::default()
            }));
            let manager = super::super::MeshManager::new(
                StepsDriver(std::sync::Arc::clone(&steps)),
                MeshKey { private: [7; 32] },
            );

            let dir = std::env::temp_dir().join(format!(
                "quoro-mesh-listener-{}",
                crate::util::hex(&crate::util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let store = crate::store::NodeStore::open(&dir).await.unwrap();
            let (_listener_desired, listener_rx) = tokio::sync::watch::channel(state(&open));
            let listener = crate::vote_listener::VoteListener::new(
                store,
                "nod_me".to_owned(),
                "cls_1".to_owned(),
                std::sync::Arc::new(crate::lease::ManualHostClock::new(
                    Some("boot-a"),
                    Duration::from_secs(1000),
                )),
                listener_rx,
                manager.listener_link(),
                crate::vote_listener::VoteListenerOptions::quick(),
            );
            let listener_task = tokio::spawn(listener.run(std::future::pending()));
            let task = tokio::spawn(manager.run(desired_rx, std::future::pending()));

            tokio::time::timeout(Duration::from_secs(10), async {
                while tokio::net::TcpStream::connect(listener_addr).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the listener binds once votes are open");
            steps.lock().await.ran.clear();

            desired_tx.send_replace(state(&closed));
            tokio::time::timeout(Duration::from_secs(10), async {
                while !steps.lock().await.ran.contains(&Step::Routes) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the state without listen is applied");
            assert_eq!(
                steps.lock().await.ran,
                vec![
                    Step::Link,
                    Step::Ruleset(nft_ruleset(Some(&mesh), Some(&closed))),
                    Step::Listening(false),
                    Step::Wireguard(2, true),
                    Step::Routes,
                ],
                "the listener is closed before any peer"
            );
            assert!(
                tokio::net::TcpStream::connect(listener_addr).await.is_err(),
                "and stays closed"
            );
            task.abort();
            listener_task.abort();
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// A removal deletes the link, and its peers with it, before the
        /// table.
        #[tokio::test]
        async fn a_removal_deletes_the_link_before_the_table() {
            let mut steps = FakeSteps {
                peers: 2,
                ..FakeSteps::default()
            };
            remove_steps(&mut steps).await.unwrap();
            assert_eq!(
                steps.ran,
                vec![Step::DeleteLink, Step::Ruleset(nft_ruleset(None, None))]
            );
            assert_eq!(steps.peers, 0);
        }

        /// A link that cannot be deleted keeps the table: the peers never run
        /// without it.
        #[tokio::test]
        async fn a_link_that_cannot_be_deleted_keeps_the_table() {
            let mut steps = FakeSteps {
                fail_delete_link: true,
                peers: 2,
                ..FakeSteps::default()
            };
            let error = remove_steps(&mut steps).await.unwrap_err();
            assert_eq!(error, "ip link del failed");
            assert_eq!(steps.ran, vec![Step::DeleteLink]);
        }

        /// A link that cannot be set up stops the apply before the ruleset
        /// and before any peer.
        #[tokio::test]
        async fn a_failed_link_configures_nothing_else() {
            let mut steps = FakeSteps {
                fail_link: true,
                ..FakeSteps::default()
            };
            let error = apply_steps(&mut steps, &two_peer_mesh(), None, None)
                .await
                .unwrap_err();
            assert_eq!(error, "the link failed");
            assert_eq!(steps.ran, vec![Step::Link]);
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::LinuxMeshDriver;

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_proto::wire::{MeshAllow, MeshPeer, SpecVoter};

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
        oifname \"quoro-wg0\" ct state new drop
        iifname \"quoro-wg0\" meta mark 0x5155008c accept
        iifname \"quoro-wg0\" drop
    }
    chain output {
        type filter hook output priority filter - 2; policy accept;
        oifname \"quoro-wg0\" ct state new meta skuid 0 accept
        oifname \"quoro-wg0\" ct state new drop
    }
    chain input {
        type filter hook input priority filter - 2; policy accept;
        iifname \"quoro-wg0\" ct state established,related accept
        iifname \"quoro-wg0\" meta mark 0x5155008c accept
        iifname \"quoro-wg0\" drop
    }
}
";
        assert_eq!(nft_ruleset(Some(&mesh()), None), expected);
    }

    /// The warm target's entry names its own mesh address, so the marked
    /// stream is delivered to the host and the input chain must admit it
    /// before its drop (section 6.13).
    #[test]
    fn the_warm_target_admits_the_replication_stream_to_its_own_address() {
        let mut mesh = mesh();
        mesh.allow = vec![MeshAllow {
            source: "10.43.0.2".to_owned(),
            address: "10.43.0.5".to_owned(),
            port: 51822,
        }];
        let rules = nft_ruleset(Some(&mesh), None);
        assert!(rules.contains(
            "iifname \"quoro-wg0\" ip saddr 10.43.0.2 ip daddr 10.43.0.5 tcp dport 51822 meta mark set 0x5155008c"
        ));
        let input = &rules[rules.find("chain input").expect("an input chain")..];
        let accept = input
            .find("iifname \"quoro-wg0\" meta mark 0x5155008c accept")
            .expect("the input chain admits marked packets");
        let drop = input
            .find("iifname \"quoro-wg0\" drop")
            .expect("the input chain drops the rest");
        assert!(accept < drop, "the accept comes before the drop");
    }

    /// A guest opens no connection on the mesh (section 6.9), so nothing
    /// forwarded opens one through it, and only root opens connections on
    /// it.
    #[test]
    fn new_connections_leave_the_mesh_only_from_root_processes() {
        let rules = nft_ruleset(Some(&mesh()), None);
        let root_accept = "oifname \"quoro-wg0\" ct state new meta skuid 0 accept";
        let new_drop = "oifname \"quoro-wg0\" ct state new drop";

        let forward_start = rules.find("chain forward {").expect("a forward chain");
        let forward = &rules[forward_start..];
        let forward = &forward[..forward.find("    }").expect("the forward chain ends")];
        let first_rule = forward
            .lines()
            .nth(2)
            .expect("the forward chain has rules")
            .trim();
        assert_eq!(first_rule, new_drop, "the oifname rule comes first");
        let oifname = forward.find(new_drop).expect("the oifname rule");
        let iifname = forward.find("iifname").expect("the iifname rules");
        assert!(oifname < iifname, "the oifname rule precedes every iifname");

        let output_start = rules.find("chain output {").expect("an output chain");
        let input_start = rules.find("chain input {").expect("an input chain");
        assert!(
            forward_start < output_start && output_start < input_start,
            "the output chain sits between the forward and input chains"
        );
        let output = &rules[output_start..input_start];
        assert!(output.contains("type filter hook output priority filter - 2; policy accept;"));
        let accept = output.find(root_accept).expect("root's new connections");
        let drop = output.find(new_drop).expect("every other new connection");
        assert!(accept < drop, "the accept comes before the drop");

        let oifname_rules: Vec<&str> = rules.lines().filter(|l| l.contains("oifname")).collect();
        assert_eq!(oifname_rules.len(), 3, "{oifname_rules:?}");
        assert!(
            oifname_rules.iter().all(|l| l.contains("ct state new")),
            "every oifname rule matches only new connections"
        );
    }

    /// Two voters with the `node` role and a witness (section 6.12's
    /// example): A at 10.43.0.2, B at 10.43.0.3, the witness at 10.43.0.1.
    fn votes() -> SpecVotes {
        let voter = |node_id: &str, roles: &[&str], address: Option<&str>| SpecVoter {
            node_id: node_id.to_owned(),
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            mesh_address: address.map(str::to_owned),
        };
        SpecVotes {
            version: 3120,
            port: 51821,
            voters: vec![
                voter("nod_a", &["node"], Some("10.43.0.2")),
                voter("nod_b", &["node"], Some("10.43.0.3")),
                voter("nod_w", &["edge", "witness"], Some("10.43.0.1")),
                voter("nod_new", &["node"], None),
            ],
            listen: Some(true),
        }
    }

    /// Every rule that marks a packet, so admits it.
    fn marking(rules: &str) -> Vec<&str> {
        rules
            .lines()
            .filter(|line| line.contains("meta mark set"))
            .map(str::trim)
            .collect()
    }

    fn vote_rule(source: &str, own: &str) -> String {
        format!(
            "iifname \"quoro-wg0\" ip saddr {source} ip daddr {own} tcp dport 51821 meta mark set 0x5155008c"
        )
    }

    /// Sections 6.9 and 8.9: the vote listener is admitted from the mesh
    /// address of every voter with the `node` role and from nothing else,
    /// to this machine's own address and `spec.votes.port`. On the witness
    /// both nodes are admitted; on node A only node B, never the witness
    /// nor A itself.
    #[test]
    fn the_vote_listener_admits_exactly_the_node_voters() {
        let mut on_witness = mesh();
        on_witness.address = "10.43.0.1/16".to_owned();
        on_witness.allow.clear();
        let rules = nft_ruleset(Some(&on_witness), Some(&votes()));
        assert_eq!(
            marking(&rules),
            vec![
                vote_rule("10.43.0.2", "10.43.0.1"),
                vote_rule("10.43.0.3", "10.43.0.1"),
            ]
        );
        let input = &rules[rules.find("chain input").expect("an input chain")..];
        assert!(input.contains("iifname \"quoro-wg0\" meta mark 0x5155008c accept"));

        let mut on_node = mesh();
        on_node.address = "10.43.0.2/16".to_owned();
        let rules = nft_ruleset(Some(&on_node), Some(&votes()));
        assert_eq!(
            marking(&rules),
            vec![
                "iifname \"quoro-wg0\" ip saddr 10.43.0.1 ip daddr 10.99.5.10 tcp dport 8080 meta mark set 0x5155008c".to_owned(),
                vote_rule("10.43.0.3", "10.43.0.2"),
            ],
            "the allow entry stays, and only the other node is a vote source"
        );
    }

    /// Section 6.9: without `spec.votes` there is no vote rule at all.
    #[test]
    fn no_votes_is_no_vote_rule() {
        let mut on_node = mesh();
        on_node.allow.clear();
        assert!(marking(&nft_ruleset(Some(&on_node), None)).is_empty());
        assert_eq!(nft_ruleset(None, Some(&votes())), nft_ruleset(None, None));
    }

    /// The rules of the chain named `name`, without its `type` line; empty
    /// when the ruleset has no such chain.
    fn chain<'a>(rules: &'a str, name: &str) -> Vec<&'a str> {
        let header = format!("    chain {name} {{");
        rules
            .lines()
            .skip_while(|line| *line != header)
            .skip(1)
            .take_while(|line| *line != "    }")
            .map(str::trim)
            .filter(|line| !line.starts_with("type "))
            .collect()
    }

    /// An unspecified or multicast mesh address publishes no listener
    /// address and loads no vote rule, so the listener never binds an
    /// address other than the one the vote rules name.
    #[test]
    fn an_unspecified_or_multicast_address_gets_no_vote_listener() {
        for address in ["0.0.0.0/16", "0.0.0.0", "224.0.0.1/4"] {
            let mut on_node = mesh();
            on_node.address = address.to_owned();
            assert_eq!(
                vote_listen_address(&on_node, Some(&votes())),
                None,
                "{address}"
            );
            let rules = nft_ruleset(Some(&on_node), Some(&votes()));
            assert!(!rules.contains("dport 51821"), "{address}: {rules}");
        }
    }

    /// With `spec.votes` the general mesh rules stay as they are: the
    /// forward chain drops new connections forwarded out through the mesh
    /// before its `iifname` rules, and the output chain lets only root's new
    /// connections out. The vote rule is a prerouting mark that the input
    /// chain accepts, and the input chain starts with the off-mesh drop.
    #[test]
    fn the_general_rules_hold_with_votes() {
        let mut on_node = mesh();
        on_node.address = "10.43.0.2/16".to_owned();
        let rules = nft_ruleset(Some(&on_node), Some(&votes()));
        assert_eq!(
            chain(&rules, "forward"),
            vec![
                "oifname \"quoro-wg0\" ct state new drop",
                "iifname \"quoro-wg0\" meta mark 0x5155008c accept",
                "iifname \"quoro-wg0\" drop",
            ]
        );
        assert_eq!(
            chain(&rules, "output"),
            vec![
                "oifname \"quoro-wg0\" ct state new meta skuid 0 accept",
                "oifname \"quoro-wg0\" ct state new drop",
            ]
        );
        assert!(
            chain(&rules, "prerouting").contains(&vote_rule("10.43.0.3", "10.43.0.2").as_str()),
            "{rules}"
        );
        assert_eq!(
            chain(&rules, "input"),
            vec![
                off_mesh_rule("10.43.0.2", 51821).as_str(),
                loopback_rule("10.43.0.2", 51821).as_str(),
                "iifname \"quoro-wg0\" ct state established,related accept",
                "iifname \"quoro-wg0\" meta mark 0x5155008c accept",
                "iifname \"quoro-wg0\" drop",
            ]
        );
        assert!(!rules.contains("skuid != 0"), "no vote-only output rule");
        assert_eq!(
            rules.matches("oifname").count(),
            3,
            "only the general oifname rules"
        );
    }

    fn off_mesh_rule(own: &str, port: u16) -> String {
        format!("iifname != \"quoro-wg0\" iifname != \"lo\" ip daddr {own} tcp dport {port} drop")
    }

    fn loopback_rule(own: &str, port: u16) -> String {
        format!("iifname \"lo\" ip daddr {own} tcp dport {port} ip saddr != {own} drop")
    }

    /// Over loopback, only connections from the machine's own mesh address
    /// reach the listener, so its owner check always applies: with
    /// `spec.votes`, the input chain's second rule drops TCP on `lo` to the
    /// own mesh address on `spec.votes.port` from any other source, ahead of
    /// every accept. Without `spec.votes` there is no such rule, and it
    /// follows the port and the own address.
    #[test]
    fn over_loopback_only_the_own_mesh_address_reaches_the_listener() {
        let mut on_node = mesh();
        on_node.address = "10.43.0.2/16".to_owned();
        let rules = nft_ruleset(Some(&on_node), Some(&votes()));
        let input = chain(&rules, "input");
        assert_eq!(
            input.get(1).copied(),
            Some(loopback_rule("10.43.0.2", 51821).as_str())
        );
        let accept = input
            .iter()
            .position(|rule| rule.ends_with("accept"))
            .expect("an accept");
        assert!(
            accept > 1,
            "both vote rules come before every accept: {input:?}"
        );
        assert_eq!(rules.matches("iifname \"lo\"").count(), 1, "{rules}");

        assert!(
            !nft_ruleset(Some(&on_node), None).contains("saddr !="),
            "no loopback rule without spec.votes"
        );

        let mut moved = votes();
        moved.port = 51900;
        let rules = nft_ruleset(Some(&on_node), Some(&moved));
        assert_eq!(
            chain(&rules, "input").get(1).copied(),
            Some(loopback_rule("10.43.0.2", 51900).as_str())
        );
        let mut on_witness = mesh();
        on_witness.address = "10.43.0.1/16".to_owned();
        let rules = nft_ruleset(Some(&on_witness), Some(&votes()));
        assert_eq!(
            chain(&rules, "input").get(1).copied(),
            Some(loopback_rule("10.43.0.1", 51821).as_str())
        );
    }

    /// The listener takes TCP only over the mesh interface or loopback. With
    /// `spec.votes`, the input chain's first rule drops TCP to the own mesh
    /// address on `spec.votes.port` from every interface but the mesh one
    /// and `lo`, ahead of every accept. Without `spec.votes` there is no such
    /// rule, and a new port moves it.
    #[test]
    fn the_vote_listener_takes_tcp_only_over_the_mesh() {
        let mut on_node = mesh();
        on_node.address = "10.43.0.2/16".to_owned();
        let rules = nft_ruleset(Some(&on_node), Some(&votes()));
        let input = chain(&rules, "input");
        assert_eq!(
            input.first().copied(),
            Some(off_mesh_rule("10.43.0.2", 51821).as_str())
        );
        assert_eq!(
            rules.matches("iifname != ").count(),
            2,
            "one rule, with its two negated interfaces: {rules}"
        );

        let without = nft_ruleset(Some(&on_node), None);
        assert!(!without.contains("iifname != "), "{without}");
        assert!(!without.contains("lo\""), "{without}");
        assert_eq!(
            chain(&without, "input").first().copied(),
            Some("iifname \"quoro-wg0\" ct state established,related accept")
        );

        let mut moved = votes();
        moved.port = 51900;
        let rules = nft_ruleset(Some(&on_node), Some(&moved));
        assert_eq!(
            chain(&rules, "input").first().copied(),
            Some(off_mesh_rule("10.43.0.2", 51900).as_str())
        );
        assert!(!rules.contains("dport 51821"), "the old port is gone");

        // On the witness the rule names the witness's own address.
        let mut on_witness = mesh();
        on_witness.address = "10.43.0.1/16".to_owned();
        let rules = nft_ruleset(Some(&on_witness), Some(&votes()));
        assert_eq!(
            chain(&rules, "input").first().copied(),
            Some(off_mesh_rule("10.43.0.1", 51821).as_str())
        );
    }

    /// The vote rule follows `spec.votes`: a snapshot whose mesh is
    /// unchanged but whose voter set changed is applied at once, not at the
    /// next reapply period.
    #[tokio::test(start_paused = true)]
    async fn a_changed_voter_set_is_applied_at_once() {
        let with_votes = |votes: Option<SpecVotes>| {
            let mut snapshot = snapshot(mesh());
            snapshot.state.spec.votes = votes;
            Some(snapshot)
        };
        let (desired_tx, desired_rx) = watch::channel(with_votes(Some(votes())));
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            MeshManager::new(driver, test_key()).run(desired_rx, stop_future(stop_rx)),
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorded.votes(), vec![Some(votes())]);

        let mut changed = votes();
        changed.version += 1;
        changed.voters.pop();
        desired_tx.send(with_votes(Some(changed.clone()))).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorded.votes(), vec![Some(votes()), Some(changed.clone())]);

        desired_tx.send(with_votes(None)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorded.votes(), vec![Some(votes()), Some(changed), None]);
        assert_eq!(recorded.calls().len(), 3, "the mesh itself never changed");

        stop_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[test]
    fn an_endpoint_is_set_only_when_new_or_changed() {
        let mesh = mesh();
        let peer = &mesh.peers[0];
        assert!(sets_endpoint(peer, false, Some(&mesh)), "new to the device");
        assert!(sets_endpoint(peer, true, None), "nothing applied yet");
        assert!(
            !sets_endpoint(peer, true, Some(&mesh)),
            "unchanged: the roamed endpoint stays"
        );
        let mut moved = mesh.clone();
        moved.peers[0].endpoint = Some("203.0.113.8:51820".to_owned());
        assert!(sets_endpoint(&moved.peers[0], true, Some(&mesh)), "changed");
        let mut rekeyed = mesh.clone();
        rekeyed.peers[0].public_key = "HIgo9xNzJMWLKASShiTqIybxZ0U3wGLiUeJ1PKf8ykw=".to_owned();
        assert!(
            sets_endpoint(&rekeyed.peers[0], true, Some(&mesh)),
            "another key is another peer"
        );
    }

    #[test]
    fn no_mesh_is_only_the_two_table_lines() {
        assert_eq!(
            nft_ruleset(None, None),
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
        let rules = nft_ruleset(Some(&mesh), None);
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

        // The removal forgot the applied mesh: the next apply sets every
        // endpoint again.
        desired_tx.send(Some(snapshot(mesh()))).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(recorded.previous(), vec![None, None]);

        stop_tx.send(true).unwrap();
        task.await.unwrap();
    }

    /// A failed apply may have set the new endpoints before a later step
    /// failed. If the spec then goes back, the next apply must set the old
    /// endpoint again, so the failed attempt's peers are not counted as
    /// applied.
    #[tokio::test(start_paused = true)]
    async fn a_failed_apply_does_not_count_its_endpoints_as_applied() {
        let (desired_tx, desired_rx) = watch::channel(Some(snapshot(mesh())));
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            MeshManager::new(driver, test_key()).run(desired_rx, stop_future(stop_rx)),
        );
        tokio::time::sleep(Duration::from_millis(1)).await;

        let mut moved = mesh();
        moved.peers[0].endpoint = Some("203.0.113.8:51820".to_owned());
        recorded.fail_applies(true);
        desired_tx.send(Some(snapshot(moved))).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        recorded.fail_applies(false);
        desired_tx.send(Some(snapshot(mesh()))).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let previous = recorded.previous();
        assert_eq!(previous.len(), 3, "{previous:?}");
        assert_eq!(previous[1], Some(mesh()), "the failed apply knew the first");
        let after_failure = previous[2].clone().expect("the first apply stays");
        assert!(
            after_failure.peers.is_empty(),
            "the peer whose endpoint the failed apply changed is forgotten"
        );
        assert!(sets_endpoint(&mesh().peers[0], true, Some(&after_failure)));

        stop_tx.send(true).unwrap();
        task.await.unwrap();
    }

    /// The wait before a retry starts at [`RETRY_FIRST`], doubles, and stops
    /// at [`REAPPLY`].
    #[test]
    fn the_retry_wait_doubles_up_to_the_reapply_period() {
        let mut waits = Vec::new();
        let mut previous = None;
        for _ in 0..7 {
            let wait = retry_delay(previous);
            waits.push(wait.as_secs());
            previous = Some(wait);
        }
        assert_eq!(waits, vec![2, 4, 8, 16, 32, 60, 60]);
    }

    /// A failed apply is retried with no new snapshot: after 2 s, then 4 s
    /// later; a success stops the retries until the reapply period, and the
    /// next failure waits 2 s again.
    #[tokio::test(start_paused = true)]
    async fn a_failed_apply_is_retried_with_a_growing_wait() {
        let (desired_tx, desired_rx) = watch::channel(Some(snapshot(mesh())));
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        recorded.fail_applies(true);
        let (_stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            MeshManager::new(driver, test_key()).run(desired_rx, stop_future(stop_rx)),
        );
        let applies = || recorded.calls().len();
        let at = |ms: u64| tokio::time::sleep(Duration::from_millis(ms));

        at(1).await;
        assert_eq!(applies(), 1, "the first apply fails");
        at(1_990).await;
        assert_eq!(applies(), 1, "no retry before 2 s");
        at(20).await;
        assert_eq!(applies(), 2, "the retry after 2 s");
        at(3_970).await;
        assert_eq!(applies(), 2, "the next wait is 4 s");
        recorded.fail_applies(false);
        at(20).await;
        assert_eq!(applies(), 3, "the retry after 4 s succeeds");
        at(30_000).await;
        assert_eq!(applies(), 3, "no retry after a success");

        recorded.fail_applies(true);
        let mut moved = mesh();
        moved.listen_port += 1;
        desired_tx.send(Some(snapshot(moved))).unwrap();
        at(1).await;
        assert_eq!(applies(), 4, "a new snapshot is applied at once and fails");
        at(1_990).await;
        assert_eq!(applies(), 4);
        at(20).await;
        assert_eq!(applies(), 5, "the wait started again at 2 s");
        task.abort();
    }

    /// A snapshot without a mesh ends the retries of a failed apply: the
    /// device the failed apply may have changed is removed, and no retry is
    /// left pending, so nothing is due again.
    #[tokio::test(start_paused = true)]
    async fn a_mesh_that_goes_away_ends_the_retries_of_its_apply() {
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        let manager = MeshManager::new(driver, test_key());
        let mut state = ManagerState::default();
        manager.sync(None, &mut state, false).await;
        recorded.fail_applies(true);
        manager
            .sync(Some(&snapshot(mesh())), &mut state, false)
            .await;
        assert!(state.retry_at.is_some(), "the failed apply is retried");

        manager.sync(None, &mut state, false).await;
        assert_eq!(
            recorded.calls(),
            vec![MeshCall::Remove, MeshCall::Apply(mesh()), MeshCall::Remove]
        );
        assert_eq!((state.retry_at, state.retry_delay), (None, None));

        manager.sync(None, &mut state, false).await;
        assert_eq!(recorded.calls().len(), 3, "nothing left to remove");
        assert_eq!((state.retry_at, state.retry_delay), (None, None));
    }

    /// A removal that fails is retried with the same growing wait as an
    /// apply, never at once: after 2 s, then 4 s, then 8 s, and not again
    /// once it succeeds. The failed apply before it does not set its wait.
    #[tokio::test(start_paused = true)]
    async fn a_failed_removal_is_retried_with_a_growing_wait() {
        let (desired_tx, desired_rx) = watch::channel(Some(snapshot(mesh())));
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        recorded.fail_applies(true);
        recorded.fail_removes(true);
        let (_stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            MeshManager::new(driver, test_key()).run(desired_rx, stop_future(stop_rx)),
        );
        let calls = || recorded.calls().len();
        let at = |ms: u64| tokio::time::sleep(Duration::from_millis(ms));

        at(1).await;
        assert_eq!(calls(), 1, "the apply fails");
        desired_tx.send(None).unwrap();
        at(1).await;
        assert_eq!(calls(), 2, "the removal fails");
        at(1_990).await;
        assert_eq!(calls(), 2, "no retry before 2 s");
        at(20).await;
        assert_eq!(calls(), 3, "the retry after 2 s");
        at(3_970).await;
        assert_eq!(calls(), 3, "the next wait is 4 s");
        at(30).await;
        assert_eq!(calls(), 4);
        recorded.fail_removes(false);
        at(8_000).await;
        assert_eq!(calls(), 5, "the retry after 8 s succeeds");
        at(40_000).await;
        assert_eq!(calls(), 5, "no retry after a success");
        assert!(
            recorded.calls()[1..]
                .iter()
                .all(|call| *call == MeshCall::Remove)
        );
        task.abort();
    }

    /// The firewall watch holds the vote listener's address while a
    /// successful apply of a mesh with `spec.votes` and `listen: true`
    /// stands, and `None` once an apply fails, without `spec.votes` or its
    /// `listen` (section 8.9), and once the mesh is removed. Without
    /// `listen` the firewall's rules are still loaded.
    #[tokio::test(start_paused = true)]
    async fn the_firewall_watch_follows_the_loaded_rules() {
        let with_votes = |votes: Option<SpecVotes>| {
            let mut snapshot = snapshot(mesh());
            snapshot.state.spec.votes = votes;
            Some(snapshot)
        };
        let (desired_tx, desired_rx) = watch::channel(with_votes(Some(votes())));
        let driver = RecordingMeshDriver::new();
        let recorded = driver.clone();
        let manager = MeshManager::new(driver, test_key());
        let firewall = manager.firewall();
        let (_stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(manager.run(desired_rx, stop_future(stop_rx)));
        let listener: SocketAddr = "10.43.0.5:51821".parse().unwrap();
        let at = |ms: u64| tokio::time::sleep(Duration::from_millis(ms));

        at(1).await;
        assert_eq!(*firewall.borrow(), Some(listener));

        recorded.fail_applies(true);
        let mut changed = votes();
        changed.version += 1;
        desired_tx.send(with_votes(Some(changed.clone()))).unwrap();
        at(1).await;
        assert_eq!(*firewall.borrow(), None, "a failed apply");
        recorded.fail_applies(false);
        at(2_010).await;
        assert_eq!(*firewall.borrow(), Some(listener), "the retry succeeded");

        for closed in [None, Some(false)] {
            let applies = recorded.calls().len();
            let not_open = SpecVotes {
                listen: closed,
                ..changed.clone()
            };
            desired_tx.send(with_votes(Some(not_open.clone()))).unwrap();
            at(1).await;
            assert_eq!(*firewall.borrow(), None, "listen {closed:?}");
            assert_eq!(recorded.calls().len(), applies + 1, "listen {closed:?}");
            assert_eq!(recorded.votes().last(), Some(&Some(not_open)));
        }
        desired_tx.send(with_votes(Some(changed.clone()))).unwrap();
        at(1).await;
        assert_eq!(*firewall.borrow(), Some(listener), "open again");

        desired_tx.send(with_votes(None)).unwrap();
        at(1).await;
        assert_eq!(*firewall.borrow(), None, "no spec.votes");

        desired_tx.send(with_votes(Some(changed))).unwrap();
        at(1).await;
        assert_eq!(*firewall.borrow(), Some(listener));
        desired_tx.send(None).unwrap();
        at(1).await;
        assert_eq!(*firewall.borrow(), None, "the mesh is removed");
        task.abort();
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
        // The re-apply knows what the first apply set, so it leaves the
        // endpoints WireGuard learned since.
        assert_eq!(recorded.previous(), vec![None, Some(mesh())]);
        desired_tx.send(None).unwrap();
        task.abort();
    }
}

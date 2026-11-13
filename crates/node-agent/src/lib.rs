// SPDX-License-Identifier: AGPL-3.0-only
//! The node agent: local store, reconciliation, the control-plane session and
//! the lease state machine with self-fencing.
//!
//! See `docs/contracts/node-agent-protocol.md`. The lease modules date from M0;
//! [`identity`], [`tls`], [`enroll`], [`store`], [`localkey`], [`logs`],
//! [`health`], [`reconcile`] and [`session`] are the M2 agent the binary starts.
//!
//! The two entry points the binary calls are [`join`] (one-time enrollment,
//! section 2) and [`run`] (reconciler plus dial-out session, sections 3-6).

mod agent;
pub mod backup;
pub mod build;
pub mod edge;
pub mod enroll;
pub mod follow;
pub mod freeze;
pub mod health;
pub mod identity;
pub mod ingress;
pub mod interactive;
pub mod inventory;
pub mod kept;
pub mod lease;
pub mod localkey;
pub mod logs;
pub mod mesh;
pub mod metrics;
pub mod reconcile;
pub mod session;
pub mod set_primary;
pub mod store;
pub mod tls;
pub mod transcript;
mod util;
pub mod vote;
pub mod vote_client;
pub mod vote_listener;
pub mod warm;

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub use agent::{run, run_with_mesh};
pub use backup::S3Repository;
pub use backup::manager::BackupManager;
pub use build::manager::{BuildManager, BuildsHandle};
pub use identity::{Identity, is_enrolled};
pub use ingress::{DnsBridge, IngressConfig, IngressSupervisor};
pub use lease::{
    Action, Attached, FenceRecord, FenceReport, FenceTimers, HostClock, HostNow, Lease,
    LeaseConfig, LeaseLink, LeaseState, LeaseTable, LeaseView, LocalLeases, MIN_DURATION,
    ManualHostClock, Refusal, RevokeOutcome, StartPermit, StoredLease, StoredValidity,
    SystemHostClock, VmFenceOutcome, budget_for, margin, start_local_leases, stop_vms,
    stop_vms_until,
};
pub use set_primary::{SetPrimary, set_primary, verify_primary, write_primary};

/// The ACME directory `acme` routes use unless the configuration names another
/// one (a staging directory for tests and labs).
pub use quoro_ingress::acme::LETS_ENCRYPT_PRODUCTION as DEFAULT_ACME_DIRECTORY;

/// The bound of a stateful start under an `expiring` lease, 3 000 ms
/// (node-agent-protocol.md section 6.4, ADR 0242).
pub const EXPIRING_START_WITHIN: Duration = Duration::from_millis(3000);

/// The bound of every other start the node agent asks the engine for
/// (engine-trait.md section 3.7). The contract leaves it to the node agent.
/// It is longer than any start that works today, a wait for the VM's lock
/// behind another call included, so it changes none of them; it only ends a
/// start that would otherwise hang.
pub const START_WITHIN: Duration = Duration::from_secs(60);

/// The bound of a forced stop outside a fence, and of the forced part of the
/// reconciler's graceful stop (engine-trait.md section 3.8, ADR 0243). A
/// fence uses its own `T_force`. Like [`START_WITHIN`] it is longer than any
/// forced stop that works today: a forced stop queued behind the longest
/// start, which ends by [`START_WITHIN`], still finishes within it.
pub const FORCE_WITHIN: Duration = Duration::from_secs(90);

/// Loopback metrics address of the `cloudflared` connector unless configured
/// otherwise.
pub const DEFAULT_CLOUDFLARED_METRICS: &str = "127.0.0.1:20241";

/// Everything the agent needs from the node's configuration.
#[derive(Clone, Debug)]
pub struct AgentConfig {
    /// Root-owned data directory, for example `/var/lib/quoro-node`. Created
    /// `0700` when it does not exist.
    pub data_dir: PathBuf,
    /// Operator-visible node name, unique in the cluster.
    pub node_name: String,
    /// The node bridge every VM is attached to.
    pub bridge: String,
    /// DNS servers handed to guests, from the node configuration.
    pub dns: Vec<Ipv4Addr>,
    /// Roles sent at enrollment: any of `node`, `witness`, `edge`.
    pub roles: Vec<String>,
    /// The `cloudflared` connector for `tunnel` routes, under the artifacts
    /// directory (`<artifacts dir>/cloudflared` by default).
    pub cloudflared_binary: PathBuf,
    /// Loopback `--metrics` address of that connector.
    pub cloudflared_metrics: SocketAddr,
    /// ACME directory URL for `acme` routes.
    pub acme_directory_url: String,
    /// ACME account contacts (`mailto:`); may be empty.
    pub acme_contact: Vec<String>,
    /// The file names of the snapshots the engine's start-up sweep left in
    /// its volume directory (engine-trait.md section 3.16), or `None` for an
    /// engine without a sweep or one that skipped it. The agent marks its
    /// store as tracking every snapshot once each of them is recorded
    /// ([`kept::start_tracking`]).
    pub snapshots_at_start: Option<std::collections::BTreeSet<String>>,
    /// Where this machine's control-plane listener would answer if it were
    /// promoted, for `hello.control_plane_urls` (node-agent-protocol.md
    /// section 5.1, ADR 0235). Only a machine with the `node` role sends
    /// them; the default source lists nothing.
    pub control_plane_urls: ControlPlaneUrls,
}

/// The source of `hello.control_plane_urls` (node-agent-protocol.md section
/// 5.1, ADR 0235): the base URLs where this machine's control-plane listener
/// would answer if it were promoted, best first. The binary installs one
/// built from the host's own addresses and the listener port of the
/// installed configuration. It is asked again for every `hello`, so an
/// address that changes while the agent runs reaches the primary with the
/// next session. The default source lists nothing.
#[derive(Clone, Default)]
pub struct ControlPlaneUrls(Option<Arc<dyn Fn() -> Vec<String> + Send + Sync>>);

impl ControlPlaneUrls {
    /// A source that asks `urls` for the list.
    #[must_use]
    pub fn new(urls: impl Fn() -> Vec<String> + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(urls)))
    }

    /// What `hello.control_plane_urls` carries: the source's `https://`
    /// entries in order, without duplicates, at most
    /// [`quoro_proto::wire::MAX_CONTROL_PLANE_URLS`].
    #[must_use]
    pub fn urls(&self) -> Vec<String> {
        let Some(source) = &self.0 else {
            return Vec::new();
        };
        let mut urls: Vec<String> = Vec::new();
        for url in source() {
            if !url.starts_with("https://") || urls.contains(&url) {
                continue;
            }
            urls.push(url);
            if urls.len() == quoro_proto::wire::MAX_CONTROL_PLANE_URLS {
                break;
            }
        }
        urls
    }
}

impl fmt::Debug for ControlPlaneUrls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ControlPlaneUrls")
            .field(&if self.0.is_some() { "source" } else { "none" })
            .finish()
    }
}

/// Whether `roles` include `node`. Only such a machine keeps a custody key
/// and reports it (section 7.2); an edge-only or witness-only machine never
/// does.
#[must_use]
pub fn has_node_role(roles: &[String]) -> bool {
    roles.iter().any(|role| role == "node")
}

impl AgentConfig {
    /// Whether this machine has the `node` role ([`has_node_role`]). The
    /// role is chosen at enrollment and recorded there, so `roles` is the
    /// same at `join` and at every start; a roles change adds or removes
    /// only `witness` (section 6.12).
    #[must_use]
    pub fn has_node_role(&self) -> bool {
        has_node_role(&self.roles)
    }

    /// The ingress settings this node's directories imply.
    #[must_use]
    pub fn ingress_config(&self) -> IngressConfig {
        let mut config = IngressConfig::new(
            &self.data_dir,
            quoro_ingress::cloudflared::CloudflaredConfig {
                binary: self.cloudflared_binary.clone(),
                metrics: self.cloudflared_metrics,
                extra_args: Vec::new(),
            },
        );
        config
            .acme_directory_url
            .clone_from(&self.acme_directory_url);
        config.acme_contact.clone_from(&self.acme_contact);
        config
    }
}

/// The three values a join command carries (section 2.2).
#[derive(Clone, Debug)]
pub struct JoinTarget {
    /// Primary URL, for example `https://quoro.example.com`.
    pub url: String,
    /// The one-time join token.
    pub token: String,
    /// SHA-256 of the primary's TLS certificate fingerprint.
    pub fingerprint: String,
}

/// What enrollment and every later `hello` report about this node (sections
/// 2.3 and 5.1).
#[derive(Clone, Debug)]
pub struct NodeFacts {
    /// `cpus`, `memory_mb`, `disk_gb`.
    pub capacity: quoro_proto::wire::Capacity,
    /// Result of the KVM probe.
    pub kvm: quoro_proto::wire::Kvm,
    /// Version of the engine backend.
    pub engine_version: String,
    /// Version of this agent binary.
    pub agent_version: String,
}

/// Why a session must never be dialed again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FatalReason {
    /// The primary rejected the credential (section 5.18).
    Unauthenticated,
    /// The node was removed from the cluster (section 10).
    NodeRemoved,
}

impl fmt::Display for FatalReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthenticated => f.write_str("the primary rejected the node credential"),
            Self::NodeRemoved => f.write_str("the node was removed from the cluster"),
        }
    }
}

/// Everything that can stop the agent.
#[derive(Debug)]
pub enum AgentError {
    /// A filesystem operation failed; `what` names it.
    Io {
        /// The operation that failed.
        what: &'static str,
        /// The underlying error.
        source: std::io::Error,
    },
    /// An identity file is missing, malformed or unusable.
    Identity(String),
    /// [`join`] was called on a node that already has an identity.
    AlreadyEnrolled(PathBuf),
    /// Enrollment was refused; `detail` is the problem document's text.
    Enroll {
        /// HTTP status the primary answered with.
        status: u16,
        /// Problem `detail` or `title`, when the primary sent one.
        detail: String,
    },
    /// The pinned TLS connection could not be established.
    Tls(String),
    /// A frame or a message body was unacceptable.
    Protocol(String),
    /// The local SQLite store failed.
    Store(String),
    /// The node-local key file is missing, malformed or a body cannot be
    /// decrypted with it.
    LocalKey(String),
    /// The engine returned an error where the agent could not continue.
    Engine(String),
    /// The session ended in a way that must stop the agent (section 4.4).
    Fatal(FatalReason),
    /// Another process holds the promotion lock
    /// ([`freeze::PromotionLock`]): a promotion runs on this host (section
    /// 7.3 "`--abort`", ADR 0280).
    PromotionRunning,
}

impl AgentError {
    /// Wraps an I/O error under the operation that failed.
    #[must_use]
    pub fn io(what: &'static str, source: std::io::Error) -> Self {
        Self::Io { what, source }
    }

    /// Whether redialing can never help, so the binary should exit non-zero.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { what, source } => write!(f, "{what}: {source}"),
            Self::Identity(reason) => write!(f, "identity: {reason}"),
            Self::AlreadyEnrolled(dir) => write!(
                f,
                "{} already holds an enrollment; remove the node instead of joining again",
                dir.display()
            ),
            Self::Enroll { status, detail } => {
                write!(f, "enrollment was refused with HTTP {status}").and_then(|()| {
                    if detail.is_empty() {
                        Ok(())
                    } else {
                        write!(f, ": {detail}")
                    }
                })
            }
            Self::Tls(reason) => write!(f, "TLS: {reason}"),
            Self::Protocol(reason) => write!(f, "protocol: {reason}"),
            Self::Store(reason) => write!(f, "local store: {reason}"),
            Self::LocalKey(reason) => write!(f, "node-local key: {reason}"),
            Self::Engine(reason) => write!(f, "engine: {reason}"),
            Self::Fatal(reason) => write!(f, "session ended: {reason}"),
            Self::PromotionRunning => f.write_str("a promotion runs on this host"),
        }
    }
}

impl std::error::Error for AgentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Enrolls the node with a join token (sections 2.2-2.5) and writes the
/// identity files.
///
/// Refuses when the data directory already holds an enrollment: a node never
/// re-enrolls on its own (section 2.5); a lost credential is recovered by
/// removing and re-enrolling the node by a human.
///
/// With the `node` role the machine keeps its custody key before it asks,
/// generating it when the data directory has none, and sends its public half
/// in the request (sections 2.3 and 7.2).
///
/// # Errors
///
/// [`AgentError::AlreadyEnrolled`], [`AgentError::Io`],
/// [`AgentError::Tls`], [`AgentError::Enroll`] or [`AgentError::Identity`].
pub async fn join(
    config: &AgentConfig,
    target: &JoinTarget,
    facts: &NodeFacts,
) -> Result<Identity, AgentError> {
    if identity::is_enrolled(&config.data_dir) {
        return Err(AgentError::AlreadyEnrolled(config.data_dir.clone()));
    }
    identity::ensure_data_dir(&config.data_dir)?;

    let seed: [u8; 32] = util::random_bytes();
    let public_key = quoro_proto::sign::public_key_to_base64(
        &quoro_proto::sign::signing_key_from_seed(&seed).verifying_key(),
    );
    identity::write_seed(&config.data_dir, &seed)?;
    // The primary binds the first custody key it sees, so the key is on disk
    // before its public half is sent.
    let custody_public_key = if config.has_node_role() {
        Some(
            identity::load_or_create_custody_key(&config.data_dir)?
                .public_key()
                .to_base64(),
        )
    } else {
        None
    };

    let enrollment = enroll::enroll(
        config,
        target,
        facts,
        &public_key,
        custody_public_key.as_deref(),
    )
    .await?;

    let key_id = Identity::key_id_for(&enrollment.cluster_public_key)?;
    let fingerprint = format!(
        "sha256:{}",
        tls::normalize_fingerprint(&target.fingerprint)?
    );
    let identity = Identity {
        node_id: enrollment.node_id,
        cluster_id: enrollment.cluster_id,
        primary_url: enrollment.primary_url,
        primary_urls: Vec::new(),
        fingerprint,
        cluster_public_key: enrollment.cluster_public_key,
        cluster_key_id: key_id,
    };

    identity::write_credential(&config.data_dir, &enrollment.credential)?;
    identity::save_identity(&config.data_dir, &identity)?;
    Ok(identity)
}

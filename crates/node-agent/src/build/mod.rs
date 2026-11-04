// SPDX-License-Identifier: AGPL-3.0-only
//! Image builds on this node (`node-agent-protocol.md` section 6.6, ADR 0117,
//! ADR 0118, ADR 0119).
//!
//! A build runs untrusted code: the Dockerfile and everything it fetches. It
//! runs only inside an ephemeral builder microVM created through the
//! [`Engine`](quoro_engine::Engine), with egress-only networking, never on the
//! host. The host never mounts a disk the builder VM wrote:
//!
//! - `quoro-work`: the app's persistent ext4 BuildKit cache, created by the
//!   engine and only ever mounted inside the builder VM;
//! - `quoro-src`: an ephemeral raw disk the node writes before boot, one tar
//!   stream with the source tree and the secret files;
//! - `quoro-out`: an ephemeral raw disk the builder writes the OCI layout to
//!   as one tar stream, which the node reads in user space.
//!
//! The pieces:
//!
//! - [`Builder`] is the interface one frontend implements (ADR 0117):
//!   [`buildkit::DockerfileBuilder`] in M4, Railpack later, and
//!   [`fake::FakeBuilder`] for tests.
//! - [`progress`] turns the builder VM's tagged output (BuildKit `rawjson`
//!   progress plus plain lines) into [`BuildEvent`]s and the failing step.
//! - [`source`] fetches one commit with the host's Git client into a bare
//!   repository and writes the source tar.
//! - [`log`] keeps each build's human log on the node.
//! - [`manager::BuildManager`] runs `spec.builds`, one at a time, reports
//!   their status for `status_report.builds`, imports a success with
//!   `prepare_layout`, and serves `build_logs` streams and
//!   `build_logs_query`.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::Ipv4Addr;

use quoro_engine::OciLayout;
use quoro_proto::wire::{BuildLimits, BuildPhase, FailedStep, LogStream};
use tokio::sync::{mpsc, watch};

pub mod buildkit;
pub mod fake;
pub mod log;
pub mod manager;
pub mod progress;
pub mod source;

/// Where a build's source comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildSource {
    /// A public `https://` Git repository at a branch, tag or full commit SHA.
    Git { url: String, git_ref: String },
}

/// Which frontend turns the source into an image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frontend {
    /// BuildKit's Dockerfile frontend. `path` is relative to the context
    /// directory; `target` names a stage of a multi-stage Dockerfile.
    Dockerfile {
        path: String,
        target: Option<String>,
    },
}

/// Everything one build needs (ADR 0117).
#[derive(Clone, PartialEq, Eq)]
pub struct BuildSpec {
    /// `bld_<ULID>`.
    pub build_id: String,
    /// `app_<ULID>`; also the cache key, so caches never mix between apps.
    pub app_id: String,
    pub source: BuildSource,
    pub frontend: Frontend,
    /// Relative to the repository root; `.` is the root.
    pub context_dir: String,
    pub build_args: BTreeMap<String, String>,
    /// Secret id to plaintext value. Never logged: `Debug` prints the ids.
    pub secrets: BTreeMap<String, String>,
    pub limits: BuildLimits,
    /// The builder VM's address on the node bridge, chosen by the manager so
    /// it never collides with an app VM.
    pub net: BuilderNet,
}

/// The builder VM's place on the node bridge. It is always attached with
/// `egress_only` and no isolation group (ADR 0118); guest DNS comes from the
/// engine's node network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuilderNet {
    pub ip: Ipv4Addr,
    pub gateway: Ipv4Addr,
}

impl std::fmt::Debug for BuildSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secrets: Vec<&String> = self.secrets.keys().collect();
        f.debug_struct("BuildSpec")
            .field("build_id", &self.build_id)
            .field("app_id", &self.app_id)
            .field("source", &self.source)
            .field("frontend", &self.frontend)
            .field("context_dir", &self.context_dir)
            .field("build_args", &self.build_args)
            .field("secrets", &secrets)
            .field("limits", &self.limits)
            .field("net", &self.net)
            .finish()
    }
}

/// One thing that happened during a build, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildEvent {
    /// The build entered this phase (`preparing`, `building`, `exporting`).
    /// `importing` is the manager's, not the builder's.
    Phase(BuildPhase),
    /// The ref resolved to this full commit SHA (40 lowercase hex).
    Commit(String),
    /// One line of the human log.
    Line {
        stream: LogStream,
        /// BuildKit's step number (`#6`), when the line belongs to one.
        vertex: Option<u32>,
        text: String,
    },
}

/// What a successful build leaves on the node: the OCI layout to import.
///
/// The layout's file belongs to the builder until [`Builder::cleanup`] runs;
/// the manager imports it with `prepare_layout` first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildOutput {
    pub layout: OciLayout,
    /// The commit that was built.
    pub commit: String,
}

/// Why a build did not produce an image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildFailure {
    /// The phase the build was in.
    pub phase: BuildPhase,
    /// One-line reason, at most 2 000 characters: the step error, `timeout`,
    /// or what went wrong around the build.
    pub error: String,
    /// The failing BuildKit step, when a step failed.
    pub step: Option<FailedStep>,
    /// True when the build stopped because it was cancelled.
    pub cancelled: bool,
}

/// One frontend's way of running a build (ADR 0117).
///
/// `build` runs the whole build: source checkout, builder VM, export. It sends
/// every [`BuildEvent`] on `events` as it happens (a closed receiver is not an
/// error) and returns once the builder VM is gone. When `cancel` turns `true`
/// it stops the builder VM with a forced stop and returns a
/// [`BuildFailure`] with `cancelled: true`. It enforces `limits.timeout_s`
/// itself: on expiry it stops the VM and fails with the error `timeout` in
/// the phase it was in.
pub trait Builder: Send + Sync {
    fn build(
        &self,
        spec: &BuildSpec,
        events: mpsc::Sender<BuildEvent>,
        cancel: watch::Receiver<bool>,
    ) -> impl Future<Output = Result<BuildOutput, BuildFailure>> + Send;

    /// Removes everything ephemeral the build left on the node (its source
    /// and output disks, its builder VM if one is left). Never touches the
    /// app's cache disk. Idempotent.
    fn cleanup(&self, build_id: &str) -> impl Future<Output = ()> + Send;

    /// Removes the app's persistent cache disk. Called once the app is in
    /// neither `spec.vms` nor `spec.builds`. Idempotent.
    fn remove_cache(&self, app_id: &str) -> impl Future<Output = ()> + Send;
}

/// Cuts `text` to at most `max` characters, on a character boundary.
#[must_use]
pub fn truncate_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((end, _)) => text[..end].to_owned(),
        None => text.to_owned(),
    }
}

/// The one-line error limit of `status_report.builds[].error`.
pub const MAX_ERROR_CHARS: usize = 2000;
/// At most this many excerpt lines in a failed step.
pub const MAX_EXCERPT_LINES: usize = 20;
/// Each excerpt line is cut to this many characters.
pub const MAX_EXCERPT_CHARS: usize = 500;

// SPDX-License-Identifier: AGPL-3.0-only
//! Helpers shared by the M2 agent tests: a temp data directory, snapshot
//! builders, and an engine that records what the reconciler asked it to do.
//!
//! This is `tests/agent_common/mod.rs`, so cargo does not build it as its own
//! test binary.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use quoro_engine::{
    ByteStream, Capabilities, ConsoleStream, Engine, EngineError, EngineEvent, EventStream,
    ExecRequest, ExecSession, FakeEngine, HostProbe, ImageConfig, ImageDigest, ImageRef,
    LineStream, NodeNetwork, PreparedImage, StopMode, VmCounters, VmId, VmInfo, VmSpec, VolumeId,
    VolumeSnapshot,
};
use quoro_node_agent::NodeFacts;
use quoro_node_agent::identity::{self, Identity, IdentityHandle};
use quoro_node_agent::reconcile::{ReconcileConfig, ReconcileOptions, Reconciler, Snapshot};
use quoro_node_agent::store::NodeStore;
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::{
    BuildLimits, Capacity, DesiredState, DesiredStatus, Kvm, Spec, SpecBuild, SpecNetwork, SpecVm,
};

/// A temporary directory under the platform temp dir, removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    /// Creates a unique temporary directory.
    pub fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("quoro-agent-test-{}", quoro_node_agent_test_id()));
        std::fs::create_dir_all(&path).expect("create the test directory");
        Self(path)
    }

    /// The directory path.
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A unique tag for a test directory, so parallel tests never collide.
fn quoro_node_agent_test_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// The bridge every test node uses.
pub const BRIDGE: &str = "quoro0";
/// The subnet every test snapshot uses.
pub const SUBNET: &str = "10.42.1.0/24";
/// The gateway every test snapshot uses.
pub const GATEWAY: &str = "10.42.1.1";

/// The DNS servers the node configuration hands guests.
pub fn dns() -> Vec<Ipv4Addr> {
    vec![Ipv4Addr::new(9, 9, 9, 9), Ipv4Addr::new(1, 1, 1, 1)]
}

/// Short timings so a test never waits for the contract's seconds.
pub fn options() -> ReconcileOptions {
    ReconcileOptions {
        tick: Duration::from_millis(10),
        stop_timeout: Duration::from_millis(20),
        backoff_base: Duration::from_millis(1),
        backoff_max: Duration::from_millis(8),
        healthy_after: Duration::from_secs(60),
    }
}

/// The reconciler configuration for [`BRIDGE`] and [`dns`].
pub fn reconcile_config() -> ReconcileConfig {
    ReconcileConfig {
        bridge: BRIDGE.to_owned(),
        dns: dns(),
    }
}

/// One VM of a snapshot.
pub fn spec_vm(vm_id: &str, project_id: &str, desired_status: DesiredStatus) -> SpecVm {
    SpecVm {
        vm_id: vm_id.to_owned(),
        project_id: project_id.to_owned(),
        app_id: None,
        deployment_id: None,
        name: vm_id.to_owned(),
        image: "docker.io/library/nginx:1.27".to_owned(),
        image_digest: "sha256:aa".to_owned(),
        vcpus: 1,
        memory_mb: 256,
        disk_gb: 1,
        port: Some(80),
        env: BTreeMap::from([("APP_ENV".to_owned(), "test".to_owned())]),
        health_check: None,
        desired_status,
        restart_generation: 0,
        stateful: false,
        lease_id: None,
        fencing_token: None,
        volumes: Vec::new(),
        build_id: None,
    }
}

/// A snapshot with `vms` and no signature check (the reconciler takes an
/// already verified snapshot).
pub fn snapshot(rev: u64, vms: Vec<SpecVm>) -> Snapshot {
    snapshot_with(rev, vms, Vec::new())
}

/// A snapshot with `vms` and `builds`, and no signature check.
pub fn snapshot_with(rev: u64, vms: Vec<SpecVm>, builds: Vec<SpecBuild>) -> Snapshot {
    Snapshot {
        rev,
        state: DesiredState {
            rev,
            issued_at: "2026-11-02T10:15:31Z".to_owned(),
            term: 1,
            spec: Spec {
                vms,
                routes: Vec::new(),
                network: SpecNetwork {
                    subnet: SUBNET.to_owned(),
                    gateway: GATEWAY.to_owned(),
                },
                ingress: None,
                builds,
            },
            signature: String::new(),
        },
    }
}

/// One `spec.builds[]` entry with the contract's default limits.
pub fn spec_build(build_id: &str, app_id: &str) -> SpecBuild {
    SpecBuild {
        build_id: build_id.to_owned(),
        app_id: app_id.to_owned(),
        project_id: "prj_1".to_owned(),
        repo_url: "https://github.com/example/blog.git".to_owned(),
        git_ref: "main".to_owned(),
        dockerfile_path: "Dockerfile".to_owned(),
        context_dir: ".".to_owned(),
        target: None,
        build_args: BTreeMap::from([("NODE_ENV".to_owned(), "production".to_owned())]),
        secrets: BTreeMap::from([("token".to_owned(), "s3cret".to_owned())]),
        limits: BuildLimits::default(),
        cancelled: false,
    }
}

/// What `hello` reports.
pub fn facts() -> NodeFacts {
    NodeFacts {
        capacity: Capacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        kvm: Kvm {
            available: true,
            kernel: "6.11.2".to_owned(),
            nested: Some(true),
        },
        engine_version: "fake 0".to_owned(),
        agent_version: "0.0.1".to_owned(),
    }
}

/// Writes the three identity files with a placeholder primary URL.
pub fn identity_files(dir: &Path, cluster_key: &SigningKey) -> IdentityHandle {
    identity_files_with_url(dir, cluster_key, "https://primary.test")
}

/// Writes the three identity files and returns their handle.
pub fn identity_files_with_url(
    dir: &Path,
    cluster_key: &SigningKey,
    primary_url: &str,
) -> IdentityHandle {
    identity::ensure_data_dir(dir).expect("create the data directory");
    identity::write_seed(dir, &[7; 32]).expect("write the seed");
    identity::write_credential(dir, "credential-for-tests").expect("write the credential");
    identity::save_identity(
        dir,
        &Identity {
            node_id: "nod_test".to_owned(),
            cluster_id: "cls_test".to_owned(),
            primary_url: primary_url.to_owned(),
            primary_urls: Vec::new(),
            fingerprint: format!(
                "sha256:{}",
                "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35"
            ),
            cluster_public_key: sign::public_key_to_base64(&cluster_key.verifying_key()),
            cluster_key_id: Identity::key_id_for(&sign::public_key_to_base64(
                &cluster_key.verifying_key(),
            ))
            .expect("key id"),
        },
    )
    .expect("write the identity");
    IdentityHandle::load(dir).expect("load the identity")
}

/// A `FakeEngine` that records what the reconciler asked it to do, because the
/// fake itself exposes no network or spec getters.
///
/// Cloning shares the engine and the records, so a test can hand one clone to
/// [`quoro_node_agent::run`] and inspect the other.
#[derive(Clone)]
pub struct RecordingEngine {
    inner: Arc<FakeEngine>,
    networks: Arc<Mutex<Vec<NodeNetwork>>>,
    created: Arc<Mutex<Vec<VmSpec>>>,
    stops: Arc<Mutex<Vec<(VmId, StopMode)>>>,
}

impl RecordingEngine {
    /// A fresh recording engine.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FakeEngine::new()),
            networks: Arc::new(Mutex::new(Vec::new())),
            created: Arc::new(Mutex::new(Vec::new())),
            stops: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The underlying fake, for its injection hooks (`crash`, `fail_next`).
    pub fn inner(&self) -> &FakeEngine {
        &self.inner
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Every `ensure_network` call, in order.
    pub fn networks(&self) -> Vec<NodeNetwork> {
        Self::lock(&self.networks).clone()
    }

    /// Every spec handed to `create`.
    pub fn created(&self) -> Vec<VmSpec> {
        Self::lock(&self.created).clone()
    }

    /// How many VMs were created.
    pub fn create_count(&self) -> usize {
        Self::lock(&self.created).len()
    }

    /// Every stop call, in order.
    pub fn stops(&self) -> Vec<(VmId, StopMode)> {
        Self::lock(&self.stops).clone()
    }

    /// Creates and starts a VM directly on the engine, as a leftover from an
    /// earlier agent run would be.
    pub async fn raw_running_vm(&self, vm_id: &str) -> VmId {
        let id = VmId(vm_id.to_owned());
        let digest = ImageDigest("sha256:raw".to_owned());
        self.prepare_image(&ImageRef {
            reference: "raw".to_owned(),
            digest: digest.clone(),
        })
        .await
        .expect("prepare the raw image");
        let spec = VmSpec {
            id: id.clone(),
            vcpus: 1,
            memory_mb: 128,
            rootfs: digest,
            disk_gb: 1,
            volumes: Vec::new(),
            net: quoro_engine::NetAttach {
                ip: Ipv4Addr::new(10, 42, 1, 200),
                gateway: Ipv4Addr::new(10, 42, 1, 1),
                mac: quoro_engine::MacAddr([0x52, 0x55, 10, 42, 1, 200]),
                isolation_group: None,
                egress_only: false,
            },
            init: quoro_engine::InitConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
            },
            console: quoro_engine::ConsoleMode::Off,
            serial: quoro_engine::SerialMode::Off,
            vsock: None,
            work_disks: Vec::new(),
        };
        self.create(&spec).await.expect("create the raw VM");
        self.start(&id).await.expect("start the raw VM");
        id
    }
}

impl Default for RecordingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine for RecordingEngine {
    async fn capabilities(&self) -> Capabilities {
        self.inner.capabilities().await
    }

    async fn probe_host(&self) -> Result<HostProbe, EngineError> {
        self.inner.probe_host().await
    }

    async fn ensure_network(&self, net: &NodeNetwork) -> Result<(), EngineError> {
        Self::lock(&self.networks).push(net.clone());
        self.inner.ensure_network(net).await
    }

    async fn prepare_image(&self, image: &ImageRef) -> Result<PreparedImage, EngineError> {
        self.inner.prepare_image(image).await
    }

    async fn remove_image(&self, digest: &ImageDigest) -> Result<(), EngineError> {
        self.inner.remove_image(digest).await
    }

    async fn create(&self, spec: &VmSpec) -> Result<VmInfo, EngineError> {
        Self::lock(&self.created).push(spec.clone());
        self.inner.create(spec).await
    }

    async fn start(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        self.inner.start(id).await
    }

    async fn stop(&self, id: &VmId, mode: StopMode) -> Result<VmInfo, EngineError> {
        Self::lock(&self.stops).push((id.clone(), mode.clone()));
        self.inner.stop(id, mode).await
    }

    async fn delete(&self, id: &VmId) -> Result<(), EngineError> {
        self.inner.delete(id).await
    }

    async fn inspect(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        self.inner.inspect(id).await
    }

    async fn list(&self) -> Result<Vec<VmInfo>, EngineError> {
        self.inner.list().await
    }

    fn events(&self) -> EventStream {
        self.inner.events()
    }

    fn output(&self, id: &VmId) -> Result<LineStream, EngineError> {
        self.inner.output(id)
    }

    fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError> {
        self.inner.console(id)
    }

    async fn exec(&self, id: &VmId, request: &ExecRequest) -> Result<ExecSession, EngineError> {
        self.inner.exec(id, request).await
    }

    fn serial_output(&self, id: &VmId, tail: Option<u64>) -> Result<ByteStream, EngineError> {
        self.inner.serial_output(id, tail)
    }

    async fn counters(&self, id: &VmId) -> Result<VmCounters, EngineError> {
        self.inner.counters(id).await
    }

    async fn snapshot_volume(&self, id: &VolumeId) -> Result<VolumeSnapshot, EngineError> {
        self.inner.snapshot_volume(id).await
    }

    async fn delete_volume_snapshot(&self, snap: &VolumeSnapshot) -> Result<(), EngineError> {
        self.inner.delete_volume_snapshot(snap).await
    }
}

/// Builds a reconciler over a recording engine and opens the store.
pub fn reconciler(
    engine: Arc<RecordingEngine>,
    store: NodeStore,
) -> (
    Reconciler<RecordingEngine>,
    tokio::sync::watch::Receiver<Vec<quoro_proto::wire::VmStatus>>,
) {
    Reconciler::new(engine, store, reconcile_config(), options())
}

/// A pre-built `ImageConfig` for tests that call the engine directly.
pub fn image_config() -> ImageConfig {
    ImageConfig {
        entrypoint: vec!["/app".to_owned()],
        cmd: Vec::new(),
        env: vec![("IMAGE_ENV".to_owned(), "yes".to_owned())],
        user: None,
        workdir: Some("/app".to_owned()),
        exposed_ports: Vec::new(),
    }
}

/// Every engine event type, so a test can name one without the enum import.
pub fn crashed_event(id: &VmId) -> EngineEvent {
    EngineEvent::Crashed {
        id: id.clone(),
        reason: "test crash".to_owned(),
    }
}

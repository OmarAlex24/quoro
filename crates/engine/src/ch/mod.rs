// SPDX-License-Identifier: AGPL-3.0-only
//! The Cloud Hypervisor backend (engine-trait.md section 5).
//!
//! One `cloud-hypervisor` process per VM with its own API socket, driven over
//! HTTP/JSON on that UNIX socket; host networking with a Linux bridge, one TAP
//! per VM and nftables.
//!
//! [`ChEngine`] is the [`Engine`] implementation. It keeps the whole node's
//! state under one directory (`vms/`, `images/`, `boot/`, `networks.json`) so a
//! restarted agent rediscovers its VMMs instead of restarting them (§3.11,
//! D-0011); [`engine::Inner`] holds that state, [`supervise`] watches each VMM
//! process, and [`host`] reads the host facts the trait reports.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::{
    Capabilities, ConsoleStream, Engine, EngineError, EventStream, HostProbe, ImageDigest,
    ImageInventory, ImageRef, LineStream, NodeNetwork, OciLayout, PreparedImage, StopMode,
    VmCounters, VmId, VmInfo, VmSpec, VolumeId, VolumeInfo, VolumeSnapshot,
};

pub mod api;
pub mod events;
pub mod net;
pub mod state;
pub mod tools;
pub mod vmconfig;
pub mod vmm;

mod engine;
mod exec;
mod host;
mod pool;
mod serial;
mod stream;
mod supervise;
mod volume;

use engine::Inner;

use crate::{ByteStream, ExecRequest, ExecSession};

/// Worst case a forced stop takes before it reports failure.
///
/// `Force` sends `vmm.shutdown` with a 2 s timeout, waits 3 s for the exit,
/// SIGKILLs the VMM and waits 5 s for it to be reaped (contract §3.8, D-0074).
/// Ten seconds is therefore the bound: a `stop(Force)` that has not returned by
/// then is not still working, it has already failed.
pub const FORCE_STOP_BOUND: Duration = Duration::from_secs(10);

/// Everything [`ChEngine::new`] needs to run VMs on this node.
#[derive(Clone, Debug)]
pub struct ChConfig {
    /// Root of all engine state: `vms/`, `images/`, `boot/`, `networks.json`.
    pub state_dir: PathBuf,
    /// Directory holding the pinned artifacts under their pinned file names
    /// (pins.txt `file`).
    pub artifacts_dir: PathBuf,
    /// The static (musl) quoro-init binary that goes into the initramfs.
    pub init_binary: PathBuf,
    /// The guest init's SIGTERM→SIGKILL grace after the power button (default
    /// 30 s).
    pub guest_stop_timeout: Duration,
    /// Host memory never handed to VMs when reserving at create (default
    /// 512 MiB).
    pub reserved_memory_mb: u64,
}

impl ChConfig {
    /// A configuration with the documented defaults: a 30 s guest stop grace,
    /// 512 MiB of reserved host memory. Guest DNS servers come from the
    /// node network (`NodeNetwork::dns`, contract §3.3).
    #[must_use]
    pub fn new(state_dir: PathBuf, artifacts_dir: PathBuf, init_binary: PathBuf) -> Self {
        Self {
            state_dir,
            artifacts_dir,
            init_binary,
            guest_stop_timeout: Duration::from_secs(30),
            reserved_memory_mb: 512,
        }
    }
}

/// The Cloud Hypervisor backend (contract §5).
pub struct ChEngine {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ChEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChEngine")
            .field("state_dir", &self.inner.config.state_dir)
            .field("version", &self.inner.ch_version)
            .finish_non_exhaustive()
    }
}

impl ChEngine {
    /// Verifies the pinned artifacts, prepares the host-independent state and
    /// rediscovers the VMs an earlier agent process left behind.
    ///
    /// # Errors
    ///
    /// `Unsupported` when a host tool or a pin is missing, `Hypervisor` or `Io`
    /// when an artifact does not match its pin or `cloud-hypervisor` cannot run,
    /// and whatever preparing the initramfs, the image store or the recorded
    /// networks reports.
    pub async fn new(config: ChConfig) -> Result<ChEngine, EngineError> {
        let inner = Inner::new(config).await?;
        Ok(Self { inner })
    }

    /// How many lifecycle events were dropped because a subscriber's buffer was
    /// full (§3.12).
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.inner.dropped_events()
    }

    /// Detaches the volume pool from `<state_dir>/volumes` (§3.16, ADR 0156).
    ///
    /// `umount` releases the auto-clear loop device, so the pool can be
    /// inspected or the state directory wiped. Only a caller that knows no VM
    /// on this node attaches a volume may call it: tests between cases, and
    /// node removal. The engine never calls it on its own, and it refuses
    /// while the pool still holds a volume file.
    ///
    /// # Errors
    ///
    /// `Conflict` when the pool still holds a volume or the directory is not
    /// the pool, `Unsupported` without `umount`, and `Io` when `umount` fails
    /// or the directory stays mounted.
    pub fn detach_volume_pool(state_dir: &Path) -> Result<(), EngineError> {
        pool::detach(state_dir)
    }
}

impl Engine for ChEngine {
    async fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn probe_host(&self) -> Result<HostProbe, EngineError> {
        self.inner.probe_host()
    }

    async fn ensure_network(&self, net: &NodeNetwork) -> Result<(), EngineError> {
        self.inner.ensure_network(net).await
    }

    async fn prepare_image(&self, image: &ImageRef) -> Result<PreparedImage, EngineError> {
        self.inner.prepare_image(image).await
    }

    async fn prepare_layout(&self, layout: &OciLayout) -> Result<PreparedImage, EngineError> {
        self.inner.prepare_layout(layout).await
    }

    async fn remove_image(&self, digest: &ImageDigest) -> Result<(), EngineError> {
        self.inner.remove_image(digest).await
    }

    async fn image_inventory(&self, digest: &ImageDigest) -> Result<ImageInventory, EngineError> {
        self.inner.images.inventory(digest).await
    }

    async fn create(&self, spec: &VmSpec) -> Result<VmInfo, EngineError> {
        self.inner.create(spec).await
    }

    async fn start(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        self.inner.start(id).await
    }

    async fn stop(&self, id: &VmId, mode: StopMode) -> Result<VmInfo, EngineError> {
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
        self.inner.subscribe()
    }

    fn output(&self, id: &VmId) -> Result<LineStream, EngineError> {
        self.inner.output(id)
    }

    fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError> {
        self.inner.console(id)
    }

    fn serial_output(&self, id: &VmId, tail_bytes: Option<u64>) -> Result<ByteStream, EngineError> {
        self.inner.serial_output(id, tail_bytes)
    }

    async fn exec(&self, id: &VmId, request: &ExecRequest) -> Result<ExecSession, EngineError> {
        self.inner.exec(id, request).await
    }

    async fn counters(&self, id: &VmId) -> Result<VmCounters, EngineError> {
        self.inner.counters(id).await
    }

    async fn ensure_volume(&self, id: &VolumeId, size_gb: u32) -> Result<VolumeInfo, EngineError> {
        self.inner.ensure_volume(id, size_gb).await
    }

    async fn import_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &std::path::Path,
    ) -> Result<VolumeInfo, EngineError> {
        self.inner.import_volume(id, size_gb, from).await
    }

    async fn delete_volume(&self, id: &VolumeId) -> Result<(), EngineError> {
        self.inner.delete_volume(id).await
    }

    async fn snapshot_volume(&self, id: &VolumeId) -> Result<VolumeSnapshot, EngineError> {
        self.inner.snapshot_volume(id).await
    }

    async fn delete_volume_snapshot(&self, snapshot: &VolumeSnapshot) -> Result<(), EngineError> {
        self.inner.delete_volume_snapshot(snapshot).await
    }
}

#[cfg(test)]
mod tests {
    use super::host;

    #[test]
    fn kernel_version_takes_the_leading_numbers() {
        assert_eq!(host::kernel_version("6.8.0-1-amd64"), Some((6, 8)));
        assert_eq!(host::kernel_version("7.2.8"), Some((7, 2)));
        assert_eq!(host::kernel_version("10.11.0"), Some((10, 11)));
        assert_eq!(host::kernel_version("6"), None);
        assert_eq!(host::kernel_version(""), None);
    }

    #[test]
    fn kernel_support_starts_at_6_8() {
        assert!(host::kernel_supported("6.8.0"));
        assert!(host::kernel_supported("7.2.8"));
        assert!(!host::kernel_supported("6.7.12"));
        assert!(!host::kernel_supported("garbage"));
    }

    #[test]
    fn cpu_flag_lookup_matches_whole_words() {
        let flags = "fpu vme sse2 vmx hypervisor";
        assert!(host::cpu_has_flag(flags, "vmx"));
        assert!(host::cpu_has_flag(flags, "hypervisor"));
        assert!(!host::cpu_has_flag(flags, "svm"));
        assert!(!host::cpu_has_flag(flags, "vm"));
    }

    #[test]
    fn meminfo_parses_kilobytes() {
        let text = "MemTotal:       16316384 kB\nMemFree:  100 kB\n";
        assert_eq!(host::parse_mem_total_kb(text), Some(16_316_384));
        assert_eq!(
            host::parse_mem_total_kb("MemFree: 1 kB\n"),
            None,
            "a missing MemTotal is not zero memory"
        );
        assert_eq!(host::parse_mem_total_kb("MemTotal: nope kB\n"), None);
    }

    #[test]
    fn counters_sum_every_block_and_net_device() {
        let doc = serde_json::json!({
            "block_0": {"read_bytes": 10, "write_bytes": 20},
            "block_1": {"read_bytes": 1, "write_bytes": 2},
            "net_0": {"rx_bytes": 100, "tx_bytes": 200},
            "net_1": {"rx_bytes": 5, "tx_bytes": 6},
            "vmm": {"panic_count": 0}
        });
        let counters = host::counters_from_ch(&doc);
        assert_eq!(counters.block_read_bytes, 11);
        assert_eq!(counters.block_write_bytes, 22);
        assert_eq!(counters.net_rx_bytes, 105);
        assert_eq!(counters.net_tx_bytes, 206);
        assert_eq!(counters.cpu_time_ms, 0);
        assert_eq!(counters.memory_rss_bytes, 0);
    }

    #[test]
    fn counters_tolerate_a_document_without_devices() {
        let counters = host::counters_from_ch(&serde_json::json!({}));
        assert_eq!(counters.net_rx_bytes, 0);
        let counters = host::counters_from_ch(&serde_json::json!("not an object"));
        assert_eq!(counters.block_read_bytes, 0);
    }
}

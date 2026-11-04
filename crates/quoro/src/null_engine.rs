//! An [`Engine`] for a machine that runs no virtual machines.
//!
//! An edge-only machine (ADR 0143) runs the node agent for its proxy routes
//! and its mesh peer, but it holds no engine and starts no guest. This backend
//! implements the trait with that fact: nothing is ever created, and every call
//! that would observe or change a VM says so instead of pretending.
//!
//! The host probe is the one call that still reads the machine: an edge
//! reports `kvm.available` as it finds it (`node-agent-protocol.md` section
//! 2.3), even though it never uses it.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use quoro_engine::{
    ByteStream, Capabilities, ConsoleStream, Engine, EngineError, EngineEvent, EventStream,
    HostProbe, ImageDigest, ImageRef, LineStream, NodeNetwork, PreparedImage, StopMode, VmCounters,
    VmId, VmInfo, VmSpec, VolumeId, VolumeInfo, VolumeSnapshot,
};

/// `/dev/kvm`, opened read-write exactly the way an engine needs it.
const KVM: &str = "/dev/kvm";
/// The file holding the running kernel's release.
const OSRELEASE: &str = "/proc/sys/kernel/osrelease";
/// The file holding the CPU flags of the host's processors.
const CPUINFO: &str = "/proc/cpuinfo";
/// A kernel with this version or newer runs Cloud Hypervisor v54.
const MINIMUM_KERNEL: (u32, u32) = (6, 8);

/// The reason every VM call returns: this machine holds no engine.
fn no_vms() -> EngineError {
    EngineError::Unsupported(
        "this machine runs no VMs: an edge-only node has no engine (ADR 0143)".to_string(),
    )
}

/// An [`Engine`] that runs nothing.
///
/// `list` is an empty list and `events` ends immediately; every operation that
/// names a VM returns [`EngineError::Unsupported`] with the reason above.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullEngine;

impl NullEngine {
    /// Creates the backend. It holds no state.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Engine for NullEngine {
    async fn capabilities(&self) -> Capabilities {
        Capabilities {
            engine: "none".to_string(),
            version: "0".to_string(),
            kvm: kvm_available(),
            text_console: false,
            graphical_console: false,
            vm_snapshot: false,
            volume_snapshot: false,
            exec: false,
            volume_snapshot_method: None,
        }
    }

    async fn probe_host(&self) -> Result<HostProbe, EngineError> {
        let kernel_release = std::fs::read_to_string(OSRELEASE)
            .map(|text| text.trim().to_string())
            .unwrap_or_default();
        let flags = std::fs::read_to_string(CPUINFO)
            .ok()
            .and_then(|text| {
                text.lines()
                    .find_map(|line| line.strip_prefix("flags"))
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(_, flags)| flags.trim().to_string())
            })
            .unwrap_or_default();
        let kvm_available = kvm_available();
        Ok(HostProbe {
            kvm_available,
            kernel_supported: matches!(
                kernel_version(&kernel_release),
                Some(version) if version >= MINIMUM_KERNEL
            ),
            cpu_vmx: cpu_has_flag(&flags, "vmx"),
            cpu_svm: cpu_has_flag(&flags, "svm"),
            nested: kvm_available && cpu_has_flag(&flags, "hypervisor"),
            kernel_release,
        })
    }

    async fn ensure_network(&self, _net: &NodeNetwork) -> Result<(), EngineError> {
        Err(no_vms())
    }

    async fn prepare_image(&self, _image: &ImageRef) -> Result<PreparedImage, EngineError> {
        Err(no_vms())
    }

    async fn remove_image(&self, _digest: &ImageDigest) -> Result<(), EngineError> {
        Ok(())
    }

    async fn create(&self, _spec: &VmSpec) -> Result<VmInfo, EngineError> {
        Err(no_vms())
    }

    async fn start(&self, _id: &VmId) -> Result<VmInfo, EngineError> {
        Err(no_vms())
    }

    async fn stop(&self, _id: &VmId, _mode: StopMode) -> Result<VmInfo, EngineError> {
        Err(no_vms())
    }

    async fn delete(&self, _id: &VmId) -> Result<(), EngineError> {
        Err(no_vms())
    }

    async fn inspect(&self, _id: &VmId) -> Result<VmInfo, EngineError> {
        Err(EngineError::NotFound)
    }

    async fn list(&self) -> Result<Vec<VmInfo>, EngineError> {
        Ok(Vec::new())
    }

    fn events(&self) -> EventStream {
        Box::pin(NoEvents)
    }

    fn output(&self, _id: &VmId) -> Result<LineStream, EngineError> {
        Err(no_vms())
    }

    fn console(&self, _id: &VmId) -> Result<ConsoleStream, EngineError> {
        Err(no_vms())
    }

    async fn counters(&self, _id: &VmId) -> Result<VmCounters, EngineError> {
        Err(no_vms())
    }

    fn serial_output(
        &self,
        _id: &VmId,
        _tail_bytes: Option<u64>,
    ) -> Result<ByteStream, EngineError> {
        Err(no_vms())
    }

    async fn ensure_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
    ) -> Result<VolumeInfo, EngineError> {
        Err(no_vms())
    }

    async fn import_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
        _from: &std::path::Path,
    ) -> Result<VolumeInfo, EngineError> {
        Err(no_vms())
    }

    async fn delete_volume(&self, _id: &VolumeId) -> Result<(), EngineError> {
        Err(no_vms())
    }

    async fn snapshot_volume(&self, _id: &VolumeId) -> Result<VolumeSnapshot, EngineError> {
        Err(no_vms())
    }

    async fn delete_volume_snapshot(&self, _snap: &VolumeSnapshot) -> Result<(), EngineError> {
        Err(no_vms())
    }
}

/// A lifecycle stream with no events: an edge has no VM to report on.
struct NoEvents;

impl Stream for NoEvents {
    type Item = EngineEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<EngineEvent>> {
        Poll::Ready(None)
    }
}

/// True when `/dev/kvm` can be opened read-write.
fn kvm_available() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(KVM)
        .is_ok()
}

/// The major and minor version at the start of `release`, if it has both.
fn kernel_version(release: &str) -> Option<(u32, u32)> {
    let mut parts = release.trim().split('.');
    let major = leading_number(parts.next()?)?;
    let minor = leading_number(parts.next()?)?;
    Some((major, minor))
}

/// The decimal number `text` starts with.
fn leading_number(text: &str) -> Option<u32> {
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// True when the space-separated `flags` contain the whole word `flag`.
fn cpu_has_flag(flags: &str, flag: &str) -> bool {
    flags.split_whitespace().any(|word| word == flag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_edge_engine_lists_no_vms_and_refuses_to_create_one() {
        let engine = NullEngine::new();
        assert!(engine.list().await.unwrap().is_empty());

        let spec = VmSpec {
            id: VmId("vm_01".to_string()),
            vcpus: 1,
            memory_mb: 128,
            rootfs: ImageDigest("sha256:00".to_string()),
            disk_gb: 1,
            volumes: Vec::new(),
            net: quoro_engine::NetAttach {
                ip: "10.42.0.2".parse().unwrap(),
                gateway: "10.42.0.1".parse().unwrap(),
                mac: quoro_engine::MacAddr([2, 0, 0, 0, 0, 1]),
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
        let err = engine.create(&spec).await.expect_err("no VM is created");
        assert!(
            err.to_string().contains("runs no VMs"),
            "the refusal names the reason: {err}"
        );

        assert_eq!(
            engine.inspect(&spec.id).await.expect_err("no VM exists"),
            EngineError::NotFound
        );
        assert!(engine.start(&spec.id).await.is_err());
        assert!(engine.stop(&spec.id, StopMode::Force).await.is_err());
        assert!(engine.delete(&spec.id).await.is_err());
        assert!(engine.counters(&spec.id).await.is_err());
        assert!(
            engine
                .ensure_network(&NodeNetwork {
                    bridge: "quoro0".to_string(),
                    subnet: "10.42.0.0/24".to_string(),
                    gateway: "10.42.0.1".parse().unwrap(),
                    dns: Vec::new(),
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_host_probe_reads_the_machine_and_events_end_at_once() {
        let engine = NullEngine::new();
        let probe = engine.probe_host().await.unwrap();
        assert_eq!(
            probe.kvm_available,
            std::path::Path::new(KVM).exists() && kvm_available(),
            "the probe reports /dev/kvm as the machine has it"
        );

        let mut events = engine.events();
        assert!(
            std::future::poll_fn(|cx| events.as_mut().poll_next(cx))
                .await
                .is_none(),
            "an edge has no lifecycle events"
        );
    }

    #[test]
    fn kernel_versions_are_read_from_the_release_prefix() {
        assert_eq!(kernel_version("6.8.0-1-amd64"), Some((6, 8)));
        assert_eq!(kernel_version("5.15"), Some((5, 15)));
        assert_eq!(kernel_version("nonsense"), None);
        assert!(matches!(
            kernel_version("6.8"),
            Some(version) if version >= MINIMUM_KERNEL
        ));
    }
}

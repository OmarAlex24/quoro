// SPDX-License-Identifier: AGPL-3.0-only
//! Helpers for the lease tests.
//!
//! This is `tests/common/mod.rs`, so cargo does not build it as its own test
//! binary; each helper is used by at least one test.

#![allow(dead_code)]

use std::net::Ipv4Addr;
use std::time::Duration;

use quoro_engine::{
    ConsoleMode, Engine, FakeEngine, ImageDigest, ImageRef, InitConfig, MacAddr, NetAttach,
    StopMode, VmId, VmSpec,
};

/// Shorthand for a duration in milliseconds.
#[must_use]
pub fn millis(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

/// Waits (real time, bounded) until `ready` holds.
pub async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A VM the fake engine runs, as an agent would have started it.
pub async fn running_vm(engine: &FakeEngine, id: &str, index: u8) -> VmId {
    let digest = ImageDigest(format!("sha256:{}", "0".repeat(64)));
    let image = ImageRef {
        reference: "busybox".to_string(),
        digest: digest.clone(),
    };
    engine.prepare_image(&image).await.expect("prepare_image");
    let vm = VmId(id.to_string());
    let spec = VmSpec {
        id: vm.clone(),
        vcpus: 1,
        memory_mb: 128,
        rootfs: digest,
        disk_gb: 1,
        volumes: Vec::new(),
        net: NetAttach {
            ip: Ipv4Addr::new(10, 0, 0, 2 + index),
            gateway: Ipv4Addr::new(10, 0, 0, 1),
            mac: MacAddr([0x02, 0, 0, 0, 0, index]),
            isolation_group: None,
            egress_only: false,
        },
        init: InitConfig {
            entrypoint: Vec::new(),
            cmd: Vec::new(),
            env: Vec::new(),
            user: None,
            workdir: None,
        },
        console: ConsoleMode::Off,
        serial: quoro_engine::SerialMode::Off,
        vsock: None,
        work_disks: Vec::new(),
    };
    engine.create(&spec).await.expect("create");
    engine.start(&vm).await.expect("start");
    vm
}

/// The graceful and the forced stops among `stop_calls`.
#[must_use]
pub fn stop_counts(stop_calls: &[(VmId, StopMode)]) -> (usize, usize) {
    let graceful = stop_calls
        .iter()
        .filter(|(_, mode)| matches!(mode, StopMode::Graceful { .. }))
        .count();
    (graceful, stop_calls.len() - graceful)
}

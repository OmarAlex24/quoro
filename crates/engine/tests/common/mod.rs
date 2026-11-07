// SPDX-License-Identifier: AGPL-3.0-only
//! Shared test support: a sample spec and the generic conformance suite.
//!
//! Each integration-test binary compiles this module separately and uses a
//! subset of it, so unused helpers are allowed here.
#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

use quoro_engine::{
    ConsoleMode, Engine, EngineError, EngineEvent, EventStream, ImageDigest, ImageRef, InitConfig,
    LineStream, MacAddr, NetAttach, OutputLine, StopMode, VmId, VmSpec, VmState,
};

/// The bound every start in these shared tests gets: long enough for a real
/// boot on the lab (engine-trait.md section 3.7).
pub const START: Duration = Duration::from_secs(60);

/// A forced stop whose bound no working stop reaches (section 3.8).
pub const FORCE: StopMode = StopMode::Force {
    within: Duration::from_secs(10),
};

/// The image every test prepares.
pub fn image_ref() -> ImageRef {
    ImageRef {
        reference: "example/app:latest".to_string(),
        digest: ImageDigest(format!("sha256:{}", "0".repeat(64))),
    }
}

/// A spec with two env entries in non-sorted order, plus one volume.
pub fn sample_spec(rootfs: &ImageDigest, id: &str) -> VmSpec {
    VmSpec {
        id: VmId(id.to_string()),
        vcpus: 2,
        memory_mb: 512,
        rootfs: rootfs.clone(),
        disk_gb: 1,
        volumes: Vec::new(),
        net: NetAttach {
            ip: "10.42.1.10".parse().expect("sample address"),
            gateway: "10.42.1.1".parse().expect("sample gateway"),
            mac: MacAddr([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
            isolation_group: None,
            egress_only: false,
        },
        init: InitConfig {
            entrypoint: vec!["/app".to_string()],
            cmd: Vec::new(),
            env: vec![
                ("B".to_string(), "2".to_string()),
                ("A".to_string(), "1".to_string()),
            ],
            user: None,
            workdir: Some("/app".to_string()),
        },
        console: ConsoleMode::VirtioConsole {
            socket: PathBuf::from("/fake/console.sock"),
        },
        serial: quoro_engine::SerialMode::Off,
        vsock: None,
        work_disks: Vec::new(),
    }
}

/// Reads the next event from an event stream.
pub async fn next_event(stream: &mut EventStream) -> Option<EngineEvent> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

/// Reads the next line from an output stream.
pub async fn next_line(stream: &mut LineStream) -> Option<OutputLine> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

/// The lifecycle conformance suite from section 3 of the contract.
///
/// Every engine implementation runs this unchanged; it only uses the trait.
pub async fn lifecycle_suite<E: Engine>(engine: &E, image: ImageRef) {
    let prepared = engine
        .prepare_image(&image)
        .await
        .expect("prepare_image succeeds");
    let prepared_again = engine
        .prepare_image(&image)
        .await
        .expect("prepare_image is idempotent");
    assert_eq!(prepared, prepared_again, "prepare_image must be idempotent");
    assert_eq!(prepared.digest, image.digest);

    let spec = sample_spec(&prepared.digest, "vm_suite");
    let created = engine.create(&spec).await.expect("create succeeds");
    assert_eq!(created.state, VmState::Stopped);
    assert_eq!(created.pid, None);

    let mut reordered = spec.clone();
    reordered.init.env.reverse();
    let created_again = engine
        .create(&reordered)
        .await
        .expect("equal canonical spec is idempotent");
    assert_eq!(created_again, created);

    let mut different = spec.clone();
    different.memory_mb += 1;
    assert!(
        matches!(
            engine.create(&different).await,
            Err(EngineError::Conflict(_))
        ),
        "a different spec for the same id is a conflict"
    );

    let running = engine
        .start(&created.id, START)
        .await
        .expect("start succeeds");
    assert_eq!(running.state, VmState::Running);
    assert!(running.pid.is_some());

    let running_again = engine
        .start(&created.id, START)
        .await
        .expect("start is idempotent");
    assert_eq!(running_again.pid, running.pid, "start must not reboot");

    assert_eq!(
        engine.inspect(&created.id).await.expect("inspect succeeds"),
        running
    );
    assert!(
        engine
            .list()
            .await
            .expect("list succeeds")
            .iter()
            .any(|info| info.id == created.id),
        "list contains the VM"
    );

    let stopped = engine
        .stop(
            &created.id,
            StopMode::Graceful {
                timeout: Duration::from_secs(5),
                force_within: Duration::from_secs(10),
            },
        )
        .await
        .expect("graceful stop succeeds");
    assert_eq!(stopped.state, VmState::Stopped);

    let stopped_again = engine
        .stop(&created.id, FORCE)
        .await
        .expect("stopping a stopped VM succeeds");
    assert_eq!(stopped_again.state, VmState::Stopped);

    engine
        .start(&created.id, START)
        .await
        .expect("start after a stop succeeds");
    assert!(
        matches!(
            engine.delete(&created.id).await,
            Err(EngineError::Conflict(_))
        ),
        "delete of a running VM is a conflict"
    );

    engine
        .stop(&created.id, FORCE)
        .await
        .expect("forced stop succeeds");
    engine.delete(&created.id).await.expect("delete succeeds");
    assert!(matches!(
        engine.inspect(&created.id).await,
        Err(EngineError::NotFound)
    ));
    engine
        .delete(&created.id)
        .await
        .expect("delete of a missing VM succeeds");
    assert!(matches!(
        engine.start(&created.id, START).await,
        Err(EngineError::NotFound)
    ));
}

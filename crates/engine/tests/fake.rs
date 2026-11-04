// SPDX-License-Identifier: AGPL-3.0-only
//! Behaviour of the in-memory `FakeEngine` and its injection hooks.

mod common;

use std::time::Duration;

use quoro_engine::{
    Engine, EngineError, EngineEvent, FakeEngine, FakeOp, ImageDigest, NodeNetwork, OciLayout,
    OutputStream, StopMode, VmCounters, VmId, VmState, VolumeId, WorkDisk,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// An engine with one created (stopped) VM and a prepared image.
async fn created() -> (FakeEngine, VmId) {
    let engine = FakeEngine::new();
    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("prepare_image succeeds");
    let spec = common::sample_spec(&prepared.digest, "vm_fake");
    let id = spec.id.clone();
    engine.create(&spec).await.expect("create succeeds");
    (engine, id)
}

#[tokio::test]
async fn capabilities_and_probe_report_the_fake_backend() {
    let engine = FakeEngine::new();
    let caps = engine.capabilities().await;
    assert_eq!(caps.engine, "fake");
    assert_eq!(caps.version, "0");
    assert!(!caps.kvm);
    assert!(caps.text_console);
    assert!(!caps.graphical_console);
    assert!(!caps.vm_snapshot);
    assert!(caps.volume_snapshot);

    let probe = engine.probe_host().await.expect("probe succeeds");
    assert!(!probe.kvm_available);
    assert_eq!(probe.kernel_release, "fake");
    assert!(probe.kernel_supported);
    assert!(!probe.cpu_vmx);
    assert!(!probe.cpu_svm);
    assert!(!probe.nested);
}

#[tokio::test]
async fn counters_are_zero_for_a_known_vm_and_not_found_otherwise() {
    let (engine, id) = created().await;
    assert_eq!(
        engine.counters(&id).await.expect("counters succeed"),
        VmCounters {
            cpu_time_ms: 0,
            memory_rss_bytes: 0,
            net_rx_bytes: 0,
            net_tx_bytes: 0,
            block_read_bytes: 0,
            block_write_bytes: 0,
        }
    );
    assert!(matches!(
        engine.counters(&VmId("vm_missing".to_string())).await,
        Err(EngineError::NotFound)
    ));
}

#[tokio::test]
async fn events_deliver_started_exited_and_crashed() {
    let (engine, id) = created().await;
    let mut events = engine.events();

    engine.start(&id).await.expect("start succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );

    engine
        .stop(&id, StopMode::Force)
        .await
        .expect("stop succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Exited {
            id: id.clone(),
            code: 137
        })
    );

    engine.start(&id).await.expect("start succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );
    engine.crash(&id, "kernel panic");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Crashed {
            id: id.clone(),
            reason: "kernel panic".to_string()
        })
    );
    let info = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(info.state, VmState::Crashed);
    assert_eq!(info.pid, None);
}

#[tokio::test]
async fn an_app_that_exits_on_its_own_is_a_crash_with_its_code() {
    let (engine, id) = created().await;
    let mut events = engine.events();
    engine.start(&id).await.expect("start succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );
    engine.exit_app(&id, 3);
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Crashed {
            id: id.clone(),
            reason: "app exited with code 3".to_string()
        })
    );
    let info = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(info.state, VmState::Crashed);
    assert_eq!(info.exit_code, Some(3));
}

#[tokio::test(start_paused = true)]
async fn an_ignored_power_button_waits_the_timeout_then_forces() {
    let (engine, id) = created().await;
    engine.start(&id).await.expect("start succeeds");
    engine.set_ignore_power_button(&id, true);

    let began = tokio::time::Instant::now();
    let info = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(1),
            },
        )
        .await
        .expect("graceful stop succeeds");
    assert_eq!(began.elapsed(), Duration::from_secs(1));
    assert_eq!(info.state, VmState::Stopped);
    assert_eq!(info.exit_code, Some(137));

    let calls = engine.stop_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, id);
    assert_eq!(
        calls[0].1,
        StopMode::Graceful {
            timeout: Duration::from_secs(1)
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_cooperative_graceful_stop_exits_zero_without_waiting() {
    let (engine, id) = created().await;
    engine.start(&id).await.expect("start succeeds");

    let began = tokio::time::Instant::now();
    let info = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(30),
            },
        )
        .await
        .expect("graceful stop succeeds");
    assert_eq!(began.elapsed(), Duration::ZERO);
    assert_eq!(info.exit_code, Some(0));
}

#[tokio::test(start_paused = true)]
async fn a_stop_delay_is_awaited_before_acting() {
    let (engine, id) = created().await;
    engine.start(&id).await.expect("start succeeds");
    engine.set_stop_delay(&id, Duration::from_secs(2));

    let began = tokio::time::Instant::now();
    let info = engine
        .stop(&id, StopMode::Force)
        .await
        .expect("forced stop succeeds");
    assert_eq!(began.elapsed(), Duration::from_secs(2));
    assert_eq!(info.exit_code, Some(137));
}

#[tokio::test(start_paused = true)]
async fn an_unkillable_vm_keeps_its_state_and_every_stop_fails() {
    let (engine, id) = created().await;
    engine.start(&id).await.expect("start succeeds");
    let running = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(running.state, VmState::Running);
    engine.set_unkillable(&id, true);

    let began = tokio::time::Instant::now();
    let graceful = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(30),
            },
        )
        .await;
    assert_eq!(
        graceful,
        Err(EngineError::Hypervisor(
            "VMM process did not exit after SIGKILL".to_string()
        ))
    );

    let forced = engine.stop(&id, StopMode::Force).await;
    assert_eq!(
        forced,
        Err(EngineError::Hypervisor(
            "VMM process did not exit after SIGKILL".to_string()
        ))
    );
    // The failure is immediate: neither the stop delay nor the timeout is awaited.
    assert_eq!(began.elapsed(), Duration::ZERO);

    let info = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(info.state, VmState::Running);
    assert_eq!(info.pid, running.pid);
    assert_eq!(engine.stop_calls().len(), 2);

    engine.set_unkillable(&id, false);
    let info = engine
        .stop(&id, StopMode::Force)
        .await
        .expect("a forced stop succeeds once the VMM is killable");
    assert_eq!(info.state, VmState::Stopped);
    assert_eq!(info.exit_code, Some(137));
}

#[tokio::test]
async fn output_allows_one_reader_until_it_is_dropped() {
    let (engine, id) = created().await;
    let mut first = engine.output(&id).expect("first reader succeeds");
    assert!(matches!(engine.output(&id), Err(EngineError::Conflict(_))));

    engine.push_output(&id, OutputStream::Stdout, "hello");
    let line = common::next_line(&mut first).await.expect("one line");
    assert_eq!(line.stream, OutputStream::Stdout);
    assert_eq!(line.line, "hello");

    drop(first);
    let mut second = engine
        .output(&id)
        .expect("a new reader after the first is dropped");
    engine.push_output(&id, OutputStream::Stderr, "again");
    let line = common::next_line(&mut second).await.expect("one line");
    assert_eq!(line.stream, OutputStream::Stderr);
    assert_eq!(line.line, "again");
}

#[tokio::test]
async fn console_echo_round_trips_through_pump_console() {
    let (engine, id) = created().await;
    engine.set_console_echo(&id, true);

    let mut client = engine.console(&id).expect("console succeeds");
    client.write_all(b"hi").await.expect("write to console");
    client.flush().await.expect("flush console");

    let pumped = engine.pump_console(&id).await.expect("pump succeeds");
    assert_eq!(pumped, 2);

    let mut buf = [0u8; 2];
    client.read_exact(&mut buf).await.expect("read echo");
    assert_eq!(&buf, b"hi");
}

#[tokio::test]
async fn console_allows_one_client_until_it_is_dropped() {
    let (engine, id) = created().await;
    let first = engine.console(&id).expect("first client succeeds");
    assert!(matches!(engine.console(&id), Err(EngineError::Conflict(_))));
    drop(first);
    assert!(
        engine.console(&id).is_ok(),
        "a new client is accepted after the first is dropped"
    );
}

#[tokio::test]
async fn advance_crashes_a_vm_once_its_deadline_passes() {
    let (engine, id) = created().await;
    engine.start(&id).await.expect("start succeeds");
    let mut events = engine.events();
    // The Started event from before the subscription is not in this stream.
    engine.schedule_crash(&id, Duration::from_secs(5));

    engine.advance(Duration::from_secs(4));
    assert_eq!(
        engine.inspect(&id).await.expect("inspect succeeds").state,
        VmState::Running
    );

    engine.advance(Duration::from_secs(1));
    assert_eq!(
        engine.inspect(&id).await.expect("inspect succeeds").state,
        VmState::Crashed
    );
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Crashed {
            id: id.clone(),
            reason: "scheduled crash".to_string()
        })
    );
}

#[tokio::test]
async fn fail_next_returns_an_error_once_for_every_operation() {
    let engine = FakeEngine::new();
    let image = common::image_ref();

    engine.fail_next(
        FakeOp::PrepareImage,
        EngineError::Image("injected".to_string()),
    );
    assert_eq!(
        engine
            .prepare_image(&image)
            .await
            .expect_err("injected failure"),
        EngineError::Image("injected".to_string())
    );
    let prepared = engine
        .prepare_image(&image)
        .await
        .expect("the next call succeeds");
    let spec = common::sample_spec(&prepared.digest, "vm_fail");

    engine.fail_next(
        FakeOp::Create,
        EngineError::ResourceExhausted("injected".to_string()),
    );
    assert_eq!(
        engine.create(&spec).await.expect_err("injected failure"),
        EngineError::ResourceExhausted("injected".to_string())
    );
    let info = engine.create(&spec).await.expect("the next call succeeds");
    let id = info.id;

    engine.fail_next(FakeOp::Start, EngineError::Timeout);
    assert_eq!(
        engine.start(&id).await.expect_err("injected failure"),
        EngineError::Timeout
    );
    engine.start(&id).await.expect("the next call succeeds");

    engine.fail_next(
        FakeOp::Stop,
        EngineError::Hypervisor("injected".to_string()),
    );
    assert_eq!(
        engine
            .stop(&id, StopMode::Force)
            .await
            .expect_err("injected failure"),
        EngineError::Hypervisor("injected".to_string())
    );
    engine
        .stop(&id, StopMode::Force)
        .await
        .expect("the next call succeeds");

    engine.fail_next(FakeOp::Delete, EngineError::Io("injected".to_string()));
    assert_eq!(
        engine.delete(&id).await.expect_err("injected failure"),
        EngineError::Io("injected".to_string())
    );
    engine.delete(&id).await.expect("the next call succeeds");
}

#[tokio::test]
async fn ensure_network_is_idempotent_and_conflicts_on_a_different_value() {
    let engine = FakeEngine::new();
    let net = NodeNetwork {
        bridge: "br0".to_string(),
        subnet: "10.42.1.0/24".to_string(),
        gateway: "10.42.1.1".parse().expect("sample gateway"),
        dns: Vec::new(),
    };
    engine.ensure_network(&net).await.expect("first call");
    engine.ensure_network(&net).await.expect("same values");

    let mut different_subnet = net.clone();
    different_subnet.subnet = "10.43.0.0/24".to_string();
    assert!(matches!(
        engine.ensure_network(&different_subnet).await,
        Err(EngineError::Conflict(_))
    ));

    let mut different_gateway = net.clone();
    different_gateway.gateway = "10.42.1.2".parse().expect("sample gateway");
    assert!(matches!(
        engine.ensure_network(&different_gateway).await,
        Err(EngineError::Conflict(_))
    ));
}

#[tokio::test]
async fn events_are_dropped_and_counted_when_a_subscriber_is_full() {
    let (engine, id) = created().await;
    let _subscriber = engine.events();
    for _ in 0..600 {
        engine.start(&id).await.expect("start succeeds");
        engine
            .stop(&id, StopMode::Force)
            .await
            .expect("stop succeeds");
    }
    assert!(engine.dropped_events() > 0);
}

#[tokio::test]
async fn volume_snapshots_are_recorded_and_deleted() {
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    let snapshot = engine
        .snapshot_volume(&volume)
        .await
        .expect("snapshot succeeds");
    assert_eq!(snapshot.volume_id, volume);
    assert_eq!(snapshot.snapshot_id, "snap-1");
    assert_eq!(snapshot.size_bytes, 0);
    assert_eq!(snapshot.location.to_string_lossy(), "/fake/snapshots/1");

    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete succeeds");
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("deleting a missing snapshot succeeds");
}

#[tokio::test]
async fn engine_error_displays_a_reason() {
    assert_eq!(EngineError::NotFound.to_string(), "not found");
    assert_eq!(
        EngineError::Conflict("exists".to_string()).to_string(),
        "conflict: exists"
    );
}

/// An engine with one running VM that has a vsock device and a socket serial
/// port, as the node agent creates them from M4.
async fn interactive() -> (FakeEngine, VmId) {
    let engine = FakeEngine::new();
    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("prepare_image succeeds");
    let mut spec = common::sample_spec(&prepared.digest, "vm_interactive");
    spec.serial = quoro_engine::SerialMode::Socket {
        path: "/fake/serial.sock".into(),
    };
    spec.vsock = Some(quoro_engine::VsockAttach {
        cid: 3,
        socket: "/fake/vsock.sock".into(),
    });
    let id = spec.id.clone();
    engine.create(&spec).await.expect("create succeeds");
    (engine, id)
}

fn shell() -> quoro_engine::ExecRequest {
    quoro_engine::ExecRequest {
        argv: vec!["/bin/sh".to_string()],
        user: None,
        rows: 24,
        cols: 80,
    }
}

#[tokio::test]
async fn exec_echoes_input_reports_resizes_and_exits() {
    let (engine, id) = interactive().await;
    assert!(engine.capabilities().await.exec);
    assert!(
        matches!(
            engine.exec(&id, &shell()).await,
            Err(EngineError::Conflict(_))
        ),
        "a stopped VM has no exec"
    );
    engine.start(&id).await.expect("start");

    let mut session = engine.exec(&id, &shell()).await.expect("exec");
    session.io.write_all(b"uname -a\n").await.expect("write");
    let mut echoed = [0_u8; 9];
    session.io.read_exact(&mut echoed).await.expect("echo");
    assert_eq!(&echoed, b"uname -a\n");

    engine.push_exec_output(&id, 0, b"Linux\r\n");
    let mut output = [0_u8; 7];
    session.io.read_exact(&mut output).await.expect("output");
    assert_eq!(&output, b"Linux\r\n");

    session.resize.send((50, 132)).await.expect("resize");
    assert_eq!(engine.exec_resizes(&id, 0), vec![(50, 132)]);
    assert_eq!(engine.exec_requests(&id), vec![shell()]);

    engine.exit_exec(&id, 0, 3);
    assert_eq!(session.exit.await.expect("exit code"), 3);
    let mut rest = Vec::new();
    session.io.read_to_end(&mut rest).await.expect("eof");
    assert!(rest.is_empty());
}

#[tokio::test]
async fn exec_validates_requests_and_ends_with_the_vm() {
    let (engine, id) = interactive().await;
    engine.start(&id).await.expect("start");
    for bad in [
        quoro_engine::ExecRequest {
            argv: Vec::new(),
            ..shell()
        },
        quoro_engine::ExecRequest {
            argv: vec!["a\0b".to_string()],
            ..shell()
        },
        quoro_engine::ExecRequest { rows: 0, ..shell() },
        quoro_engine::ExecRequest {
            cols: 1001,
            ..shell()
        },
    ] {
        assert!(matches!(
            engine.exec(&id, &bad).await,
            Err(EngineError::Conflict(_))
        ));
    }
    assert!(matches!(
        engine.exec(&VmId("vm_missing".to_string()), &shell()).await,
        Err(EngineError::NotFound)
    ));

    let first = engine.exec(&id, &shell()).await.expect("first");
    let second = engine
        .exec(&id, &shell())
        .await
        .expect("second, independent");
    drop(second);
    assert!(engine.exec_closed(&id, 1));
    assert!(!engine.exec_closed(&id, 0));

    engine.stop(&id, StopMode::Force).await.expect("stop");
    assert!(
        first.exit.await.is_err(),
        "no exit code when the VM stopped"
    );
}

#[tokio::test]
async fn serial_output_replays_the_ring_and_follows_live_output() {
    let (engine, id) = interactive().await;
    engine.start(&id).await.expect("start");
    engine.push_serial(&id, b"quoro-init: booting\n");
    engine.push_serial(&id, b"quoro-init: app started\n");

    let mut all = engine.serial_output(&id, None).expect("stream");
    let mut tail = engine.serial_output(&id, Some(8)).expect("stream");
    let replay = next_bytes(&mut all).await;
    assert_eq!(replay, b"quoro-init: booting\nquoro-init: app started\n");
    assert_eq!(next_bytes(&mut tail).await, b"started\n");

    engine.push_serial(&id, b"live\n");
    assert_eq!(next_bytes(&mut all).await, b"live\n");
    assert_eq!(next_bytes(&mut tail).await, b"live\n");

    // A console stream outlives a restart and ends when the VM is deleted.
    engine.stop(&id, StopMode::Force).await.expect("stop");
    engine.start(&id).await.expect("start again");
    engine.push_serial(&id, b"second boot\n");
    assert_eq!(next_bytes(&mut all).await, b"second boot\n");
    engine.stop(&id, StopMode::Force).await.expect("stop");
    engine.delete(&id).await.expect("delete");
    assert!(
        std::future::poll_fn(|cx| all.as_mut().poll_next(cx))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn serial_output_and_console_follow_the_serial_mode() {
    let (engine, id) = created().await;
    assert!(matches!(
        engine.serial_output(&id, None),
        Err(EngineError::Conflict(_))
    ));
    let (engine, id) = interactive().await;
    assert!(matches!(engine.console(&id), Err(EngineError::Conflict(_))));
    assert!(matches!(
        engine.serial_output(&VmId("vm_missing".to_string()), None),
        Err(EngineError::NotFound)
    ));
}

async fn next_bytes(stream: &mut quoro_engine::ByteStream) -> Vec<u8> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx))
        .await
        .expect("a chunk")
}

/// A temporary file that stands in for a builder's OCI layout.
fn layout_file(label: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("quoro-fake-layout-{label}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn layout(path: std::path::PathBuf, digest: &str) -> OciLayout {
    OciLayout {
        path,
        manifest_digest: ImageDigest(digest.to_string()),
    }
}

const LAYOUT_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

#[tokio::test]
async fn prepare_layout_caches_by_manifest_digest() {
    let engine = FakeEngine::new();
    let path = layout_file("cached");
    let request = layout(path.clone(), LAYOUT_DIGEST);

    assert!(
        matches!(
            engine.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ),
        "a missing layout is an image error"
    );

    std::fs::write(&path, b"layout").expect("layout file");
    let prepared = engine
        .prepare_layout(&request)
        .await
        .expect("prepare a layout that exists");
    assert_eq!(prepared.digest, ImageDigest(LAYOUT_DIGEST.to_string()));

    std::fs::remove_file(&path).expect("remove the layout");
    let cached = engine
        .prepare_layout(&request)
        .await
        .expect("a cached digest does not need the layout");
    assert_eq!(cached, prepared);
}

fn work_disk(path: &str, size_gb: u32, serial: &str) -> WorkDisk {
    WorkDisk {
        path: std::path::PathBuf::from(path),
        size_gb,
        serial: serial.to_string(),
    }
}

#[tokio::test]
async fn create_records_work_disks_and_refuses_a_shared_path() {
    let engine = FakeEngine::new();
    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("prepare_image succeeds");

    let mut first = common::sample_spec(&prepared.digest, "vm_work_a");
    first.work_disks = vec![work_disk("/var/lib/quoro/work/a.img", 8, "quoro-work")];
    engine.create(&first).await.expect("create A");

    let mut same = first.clone();
    same.work_disks[0].size_gb = 9;
    assert!(
        matches!(engine.create(&same).await, Err(EngineError::Conflict(_))),
        "work_disks are part of the spec"
    );

    let mut second = common::sample_spec(&prepared.digest, "vm_work_b");
    second.net.mac.0[5] = 2;
    second.work_disks = vec![work_disk("/var/lib/quoro/work/a.img", 1, "quoro-out")];
    assert!(
        matches!(engine.create(&second).await, Err(EngineError::Conflict(_))),
        "two VMs may not name the same work-disk path"
    );

    second.work_disks = vec![work_disk("/var/lib/quoro/work/b.img", 1, "quoro-out")];
    engine
        .create(&second)
        .await
        .expect("a VM with its own work disk is created");
}

#[tokio::test]
async fn create_refuses_egress_only_with_an_isolation_group() {
    let engine = FakeEngine::new();
    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("prepare_image succeeds");

    let mut spec = common::sample_spec(&prepared.digest, "vm_egress");
    spec.net.egress_only = true;
    spec.net.isolation_group = Some("prj_01".to_string());
    match engine.create(&spec).await {
        Err(EngineError::Conflict(reason)) => {
            assert!(reason.contains("egress only"), "{reason}");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }

    spec.net.isolation_group = None;
    engine
        .create(&spec)
        .await
        .expect("egress only without a group is valid");
}

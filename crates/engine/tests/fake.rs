//! Behaviour of the in-memory `FakeEngine` and its injection hooks.

mod common;

use std::time::Duration;

use quoro_engine::{
    Engine, EngineError, EngineEvent, FakeEngine, FakeOp, NodeNetwork, OutputStream, StopMode,
    VmCounters, VmId, VmState, VolumeId,
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

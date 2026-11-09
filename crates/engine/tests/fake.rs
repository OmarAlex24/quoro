// SPDX-License-Identifier: AGPL-3.0-only
//! Behaviour of the in-memory `FakeEngine` and its injection hooks.

mod common;

use std::time::Duration;

use quoro_engine::{
    ByteRange, ChangedRanges, Engine, EngineError, EngineEvent, FakeEngine, FakeOp, FakeRanges,
    GuestFlush, ImageDigest, NodeNetwork, OciLayout, OutputStream, RangesOf, SnapshotMethod,
    SnapshotSweep, StageFrom, StopMode, VmCounters, VmId, VmState, VolumeAttach, VolumeId,
    VolumeSnapshot, WorkDisk,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A start bound no fake boot reaches, unless a test makes it slow.
const START: Duration = Duration::from_secs(60);

/// The force bound of a graceful stop no fake stop reaches.
const FORCE_WITHIN: Duration = Duration::from_secs(60);

/// A forced stop no fake stop overruns, unless a test makes it slow.
const FORCE: StopMode = StopMode::Force {
    within: FORCE_WITHIN,
};

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

    engine.start(&id, START).await.expect("start succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );

    engine.stop(&id, FORCE).await.expect("stop succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Exited {
            id: id.clone(),
            code: 137
        })
    );

    engine.start(&id, START).await.expect("start succeeds");
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
    engine.start(&id, START).await.expect("start succeeds");
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
    engine.start(&id, START).await.expect("start succeeds");
    engine.set_ignore_power_button(&id, true);

    let began = tokio::time::Instant::now();
    let info = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(1),
                force_within: FORCE_WITHIN,
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
            timeout: Duration::from_secs(1),
            force_within: FORCE_WITHIN,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_cooperative_graceful_stop_exits_zero_without_waiting() {
    let (engine, id) = created().await;
    engine.start(&id, START).await.expect("start succeeds");

    let began = tokio::time::Instant::now();
    let info = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(30),
                force_within: FORCE_WITHIN,
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
    engine.start(&id, START).await.expect("start succeeds");
    engine.set_stop_delay(&id, Duration::from_secs(2));

    let began = tokio::time::Instant::now();
    let info = engine.stop(&id, FORCE).await.expect("forced stop succeeds");
    assert_eq!(began.elapsed(), Duration::from_secs(2));
    assert_eq!(info.exit_code, Some(137));
}

/// #434, contract §3.8: a forced stop that has begun finishes even when its
/// caller stops waiting. Meanwhile the VM reads `Stopping` with its pid, and
/// `start` and `delete` are refused.
#[tokio::test(start_paused = true)]
async fn a_forced_stop_finishes_after_its_caller_stops_waiting() {
    let (engine, id) = created().await;
    let started = engine.start(&id, START).await.expect("start succeeds");
    let mut events = engine.events();
    engine.set_stop_delay(&id, Duration::from_millis(500));

    let gave_up = tokio::time::timeout(Duration::from_millis(100), engine.stop(&id, FORCE)).await;
    assert!(gave_up.is_err(), "the caller stopped waiting");
    let stopping = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopping.state, VmState::Stopping);
    assert_eq!(stopping.pid, started.pid);
    assert!(matches!(
        engine.start(&id, START).await,
        Err(EngineError::Conflict(_))
    ));
    assert!(matches!(
        engine.delete(&id).await,
        Err(EngineError::Conflict(_))
    ));

    // Past the 500 ms the slow VMM takes, counted from the stop call.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stopped = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopped.state, VmState::Stopped);
    assert_eq!(stopped.pid, None);
    assert_eq!(stopped.exit_code, Some(137));
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Exited {
            id: id.clone(),
            code: 137
        })
    );
    // Stopped once: a later start boots the VM again and nothing stops it.
    engine.set_stop_delay(&id, Duration::ZERO);
    engine.start(&id, START).await.expect("start succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        engine.inspect(&id).await.expect("inspect succeeds").state,
        VmState::Running
    );
}

/// Contract §3.8 as D-0243 extends it: a stop called while a forced stop of
/// the same VM still runs joins it instead of starting another, and the VM
/// is stopped once.
#[tokio::test(start_paused = true)]
async fn a_later_stop_joins_a_forced_stop_in_progress() {
    let (engine, id) = created().await;
    engine.start(&id, START).await.expect("start succeeds");
    let mut events = engine.events();
    engine.set_stop_delay(&id, Duration::from_millis(500));
    let began = tokio::time::Instant::now();
    let gave_up = tokio::time::timeout(Duration::from_millis(100), engine.stop(&id, FORCE)).await;
    assert!(gave_up.is_err(), "the caller stopped waiting");

    let joined = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(30),
                force_within: FORCE_WITHIN,
            },
        )
        .await
        .expect("the later stop succeeds");
    assert_eq!(began.elapsed(), Duration::from_millis(500));
    assert_eq!(joined.state, VmState::Stopped);
    assert_eq!(joined.exit_code, Some(137), "the forced stop ended the VM");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Exited {
            id: id.clone(),
            code: 137
        })
    );
    assert_eq!(engine.stop_calls().len(), 2);
}

/// No event is waiting on `events` right now.
async fn no_event(events: &mut quoro_engine::EventStream) {
    assert!(
        tokio::time::timeout(Duration::from_millis(1), common::next_event(events))
            .await
            .is_err(),
        "an event was emitted"
    );
}

/// ADR 0243, contract §7: a forced stop that overruns its bound returns
/// `Timeout` at the bound, and the stop goes on until the VM is stopped.
#[tokio::test(start_paused = true)]
async fn a_forced_stop_that_overruns_its_bound_times_out_and_still_stops() {
    let (engine, id) = created().await;
    let started = engine.start(&id, START).await.expect("start succeeds");
    engine.set_stop_delay(&id, Duration::from_millis(500));

    let began = tokio::time::Instant::now();
    let within = Duration::from_millis(100);
    assert_eq!(
        engine.stop(&id, StopMode::Force { within }).await,
        Err(EngineError::Timeout)
    );
    assert_eq!(began.elapsed(), within);
    let stopping = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopping.state, VmState::Stopping);
    assert_eq!(stopping.pid, started.pid, "the VM may still run");

    // A later stop joins it within its own bound.
    assert_eq!(
        engine
            .stop(
                &id,
                StopMode::Force {
                    within: Duration::from_millis(50)
                }
            )
            .await,
        Err(EngineError::Timeout)
    );
    assert_eq!(began.elapsed(), Duration::from_millis(150));

    tokio::time::sleep(Duration::from_millis(400)).await;
    let stopped = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopped.state, VmState::Stopped);
    assert_eq!(stopped.pid, None);
    assert_eq!(stopped.exit_code, Some(137));
}

/// ADR 0243: a graceful stop returns within `timeout + force_within`, and the
/// forced stop it began goes on after its `Timeout`.
#[tokio::test(start_paused = true)]
async fn a_graceful_stop_returns_within_its_two_bounds() {
    let (engine, id) = created().await;
    engine.start(&id, START).await.expect("start succeeds");
    engine.set_ignore_power_button(&id, true);
    engine.set_stop_delay(&id, Duration::from_millis(500));

    let began = tokio::time::Instant::now();
    let (timeout, force_within) = (Duration::from_millis(200), Duration::from_millis(100));
    assert_eq!(
        engine
            .stop(
                &id,
                StopMode::Graceful {
                    timeout,
                    force_within
                }
            )
            .await,
        Err(EngineError::Timeout)
    );
    assert_eq!(began.elapsed(), timeout + force_within);
    assert_eq!(
        engine.inspect(&id).await.expect("inspect succeeds").state,
        VmState::Stopping
    );

    // The forced part began at 200 ms and takes the 500 ms of the slow VMM;
    // wake past its end, not at it.
    tokio::time::sleep(Duration::from_millis(450)).await;
    let stopped = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopped.state, VmState::Stopped);
    assert_eq!(stopped.exit_code, Some(137));
}

/// ADR 0242, contract §7: a slow boot within the start's bound runs.
#[tokio::test(start_paused = true)]
async fn a_slow_start_within_its_bound_runs() {
    let (engine, id) = created().await;
    let mut events = engine.events();
    engine.set_start_delay(&id, Duration::from_millis(200));

    let began = tokio::time::Instant::now();
    let started = engine
        .start(&id, Duration::from_secs(1))
        .await
        .expect("start succeeds");
    assert_eq!(began.elapsed(), Duration::from_millis(200));
    assert_eq!(started.state, VmState::Running);
    assert!(started.pid.is_some());
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );
}

/// ADR 0242, contract §3.7 and §7: a start that overruns its bound returns
/// `Timeout` at the bound. The fake then stops what it started; the VM may
/// still run until the test lets that stop finish, and it never boots.
#[tokio::test(start_paused = true)]
async fn a_start_that_overruns_its_bound_times_out_and_stops_what_it_started() {
    let (engine, id) = created().await;
    let mut events = engine.events();
    engine.set_start_delay(&id, Duration::from_millis(500));
    engine.set_stop_delay(&id, Duration::from_millis(300));

    let began = tokio::time::Instant::now();
    let within = Duration::from_millis(100);
    assert_eq!(engine.start(&id, within).await, Err(EngineError::Timeout));
    assert_eq!(began.elapsed(), within);
    let stopping = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopping.state, VmState::Stopping);
    assert!(stopping.pid.is_some(), "the VM may still run");
    assert!(matches!(
        engine.start(&id, START).await,
        Err(EngineError::Conflict(_))
    ));
    assert!(matches!(
        engine.delete(&id).await,
        Err(EngineError::Conflict(_))
    ));

    // A stop joins the one the start began.
    let joined = engine.stop(&id, FORCE).await.expect("the stop joins");
    assert_eq!(began.elapsed(), Duration::from_millis(400));
    assert_eq!(joined.state, VmState::Stopped);
    assert_eq!(joined.pid, None);
    assert_eq!(joined.exit_code, None, "the VM never booted");

    // The boot the start gave up never lands, and nothing was emitted.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        engine.inspect(&id).await.expect("inspect succeeds").state,
        VmState::Stopped
    );
    no_event(&mut events).await;

    engine.set_start_delay(&id, Duration::ZERO);
    engine.start(&id, START).await.expect("start succeeds");
    assert_eq!(
        common::next_event(&mut events).await,
        Some(EngineEvent::Started { id: id.clone() })
    );
}

/// Contract §4: a start whose caller stops waiting is given up like one that
/// overran its bound, and a fast VMM stops at once.
#[tokio::test(start_paused = true)]
async fn a_start_whose_caller_stops_waiting_is_given_up() {
    let (engine, id) = created().await;
    let mut events = engine.events();
    engine.set_start_delay(&id, Duration::from_millis(500));

    let gave_up = tokio::time::timeout(
        Duration::from_millis(100),
        engine.start(&id, Duration::from_secs(1)),
    )
    .await;
    assert!(gave_up.is_err(), "the caller stopped waiting");
    let stopped = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(stopped.state, VmState::Stopped);
    assert_eq!(stopped.pid, None);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        engine.inspect(&id).await.expect("inspect succeeds").state,
        VmState::Stopped,
        "the given-up boot never lands"
    );
    no_event(&mut events).await;
}

#[tokio::test(start_paused = true)]
async fn an_unkillable_vm_keeps_its_state_and_every_stop_fails() {
    let (engine, id) = created().await;
    engine.start(&id, START).await.expect("start succeeds");
    let running = engine.inspect(&id).await.expect("inspect succeeds");
    assert_eq!(running.state, VmState::Running);
    engine.set_unkillable(&id, true);

    let began = tokio::time::Instant::now();
    let graceful = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(30),
                force_within: FORCE_WITHIN,
            },
        )
        .await;
    assert_eq!(
        graceful,
        Err(EngineError::Hypervisor(
            "VMM process did not exit after SIGKILL".to_string()
        ))
    );

    let forced = engine.stop(&id, FORCE).await;
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
        .stop(&id, FORCE)
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
    engine.start(&id, START).await.expect("start succeeds");
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
        engine
            .start(&id, START)
            .await
            .expect_err("injected failure"),
        EngineError::Timeout
    );
    engine
        .start(&id, START)
        .await
        .expect("the next call succeeds");

    engine.fail_next(
        FakeOp::Stop,
        EngineError::Hypervisor("injected".to_string()),
    );
    assert_eq!(
        engine.stop(&id, FORCE).await.expect_err("injected failure"),
        EngineError::Hypervisor("injected".to_string())
    );
    engine
        .stop(&id, FORCE)
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
        engine.start(&id, START).await.expect("start succeeds");
        engine.stop(&id, FORCE).await.expect("stop succeeds");
    }
    assert!(engine.dropped_events() > 0);
}

#[tokio::test]
async fn volume_snapshots_are_recorded_and_deleted() {
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    assert_eq!(
        engine.snapshot_volume(&volume).await,
        Err(EngineError::NotFound),
        "an unknown volume has no snapshot"
    );
    engine.ensure_volume(&volume, 1).await.expect("volume");
    let snapshot = engine
        .snapshot_volume(&volume)
        .await
        .expect("snapshot succeeds");
    assert_eq!(snapshot.volume_id, volume);
    assert_eq!(snapshot.snapshot_id, "snap-1");
    assert_eq!(snapshot.size_bytes, 1 << 30);
    assert_eq!(snapshot.location.to_string_lossy(), "/fake/snapshots/1");
    assert_eq!(snapshot.method, SnapshotMethod::Reflink);
    assert_eq!(snapshot.pause, Duration::ZERO);
    // From M5b a volume may have several snapshots at once (section 3.16).
    let second = engine
        .snapshot_volume(&volume)
        .await
        .expect("a second snapshot succeeds");
    assert_ne!(second.snapshot_id, snapshot.snapshot_id);
    assert_eq!(engine.volume_snapshots().len(), 2);

    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete succeeds");
    engine
        .delete_volume_snapshot(&second)
        .await
        .expect("delete succeeds");
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("deleting a missing snapshot succeeds");
}

#[tokio::test]
async fn volumes_are_created_attached_and_deleted() {
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    let info = engine.ensure_volume(&volume, 2).await.expect("volume");
    assert_eq!(info.size_gb, 2);
    assert_eq!(
        engine.ensure_volume(&volume, 2).await.expect("idempotent"),
        info
    );
    assert!(matches!(
        engine.ensure_volume(&volume, 3).await,
        Err(EngineError::Conflict(_))
    ));
    assert!(matches!(
        engine
            .import_volume(&volume, 2, std::path::Path::new("/fake/restore"))
            .await,
        Err(EngineError::Conflict(_))
    ));

    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("image");
    let mut spec = common::sample_spec(&prepared.digest, "vm_vol");
    spec.volumes = vec![VolumeAttach {
        volume_id: volume.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    let mut missing = spec.clone();
    missing.id = VmId("vm_missing".to_string());
    missing.volumes[0].volume_id = VolumeId("vol_02".to_string());
    assert_eq!(engine.create(&missing).await, Err(EngineError::NotFound));
    engine.create(&spec).await.expect("create with a volume");
    let mut second = spec.clone();
    second.id = VmId("vm_vol_2".to_string());
    assert!(
        matches!(engine.create(&second).await, Err(EngineError::Conflict(_))),
        "a volume belongs to one VM at a time"
    );
    assert!(matches!(
        engine.delete_volume(&volume).await,
        Err(EngineError::Conflict(_))
    ));

    engine.delete(&spec.id).await.expect("delete the VM");
    assert_eq!(
        engine.volumes(),
        vec![(volume.clone(), 2)],
        "delete keeps volumes"
    );
    engine.delete_volume(&volume).await.expect("delete volume");
    engine
        .delete_volume(&volume)
        .await
        .expect("a missing volume is Ok");
    assert!(engine.volumes().is_empty());
}

#[tokio::test]
async fn a_pause_copy_snapshot_reports_its_pause_only_for_a_running_vm() {
    let engine = FakeEngine::new();
    engine.set_snapshot_method(SnapshotMethod::PauseCopy, Duration::from_millis(250));
    assert_eq!(
        engine.capabilities().await.volume_snapshot_method,
        Some(SnapshotMethod::PauseCopy)
    );
    let volume = VolumeId("vol_01".to_string());
    engine.ensure_volume(&volume, 1).await.expect("volume");
    let idle = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(idle.pause, Duration::ZERO, "no VM runs, so no pause");
    engine.delete_volume_snapshot(&idle).await.expect("delete");

    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("image");
    let mut spec = common::sample_spec(&prepared.digest, "vm_vol");
    spec.volumes = vec![VolumeAttach {
        volume_id: volume.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    engine.create(&spec).await.expect("create");
    engine.start(&spec.id, START).await.expect("start");
    let running = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(running.method, SnapshotMethod::PauseCopy);
    assert_eq!(running.pause, Duration::from_millis(250));
}

/// The guest flush follows the VM that attaches the volume by default, and a
/// test can set any outcome or error (section 7, ADR 0282).
#[tokio::test]
async fn the_guest_flush_follows_the_vm_and_can_be_injected() {
    const WITHIN: Duration = Duration::from_secs(2);
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Err(EngineError::NotFound),
        "an unknown volume"
    );
    engine.ensure_volume(&volume, 1).await.expect("volume");
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Ok(GuestFlush::NoRunningVm)
    );
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(snapshot.guest_flush, GuestFlush::NoRunningVm);

    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("image");
    let mut spec = common::sample_spec(&prepared.digest, "vm_flush");
    spec.volumes = vec![VolumeAttach {
        volume_id: volume.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    engine.create(&spec).await.expect("create");
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Ok(GuestFlush::NoRunningVm),
        "a VM that exists but does not run"
    );
    engine.start(&spec.id, START).await.expect("start");
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Ok(GuestFlush::Flushed)
    );
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(snapshot.guest_flush, GuestFlush::Flushed);
    engine.pause(&spec.id).await.expect("pause");
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Ok(GuestFlush::Paused)
    );
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(snapshot.guest_flush, GuestFlush::Paused);
    engine.resume(&spec.id).await.expect("resume");

    // An injected outcome holds until it is set again.
    engine.set_guest_flush(Some(Ok(GuestFlush::TimedOut)));
    for _ in 0..2 {
        assert_eq!(
            engine.flush_volume(&volume, WITHIN).await,
            Ok(GuestFlush::TimedOut)
        );
    }
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(snapshot.guest_flush, GuestFlush::TimedOut);

    // An injected error: `flush_volume` returns it, a snapshot records
    // `Failed`, and `NotFound` fails the snapshot.
    let hypervisor = EngineError::Hypervisor("vsock gone".to_string());
    engine.set_guest_flush(Some(Err(hypervisor.clone())));
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Err(hypervisor.clone())
    );
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(snapshot.guest_flush, GuestFlush::Failed);
    engine.set_guest_flush(Some(Err(EngineError::NotFound)));
    assert_eq!(
        engine.snapshot_volume(&volume).await,
        Err(EngineError::NotFound)
    );

    // `fail_next` fails one `flush_volume` call and not a snapshot.
    engine.set_guest_flush(None);
    engine.fail_next(FakeOp::FlushVolume, EngineError::Timeout);
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(snapshot.guest_flush, GuestFlush::Flushed);
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Err(EngineError::Timeout)
    );
    assert_eq!(
        engine.flush_volume(&volume, WITHIN).await,
        Ok(GuestFlush::Flushed)
    );

    let calls: Vec<_> = engine
        .flush_calls()
        .into_iter()
        .map(|(id, result)| {
            assert_eq!(id, volume);
            result
        })
        .collect();
    assert_eq!(
        calls,
        [
            Err(EngineError::NotFound),
            Ok(GuestFlush::NoRunningVm),
            Ok(GuestFlush::NoRunningVm),
            Ok(GuestFlush::Flushed),
            Ok(GuestFlush::Paused),
            Ok(GuestFlush::TimedOut),
            Ok(GuestFlush::TimedOut),
            Err(hypervisor),
            Err(EngineError::Timeout),
            Ok(GuestFlush::Flushed),
        ]
    );
}

#[tokio::test]
async fn volume_files_are_real_with_a_volume_dir() {
    let dir = std::env::current_dir()
        .expect("cwd")
        .join("target")
        .join("fake-volume-test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let engine = FakeEngine::new();
    engine.set_volume_dir(&dir);
    let volume = VolumeId("vol_01".to_string());
    let info = engine.ensure_volume(&volume, 1).await.expect("volume");
    assert_eq!(info.path, dir.join("vol_01"));
    std::fs::write(&info.path, b"guest data").expect("write");
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    assert_eq!(
        std::fs::read(&snapshot.location).expect("read"),
        b"guest data"
    );
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete");
    assert!(!snapshot.location.exists());

    let restored = dir.join("restored.raw");
    std::fs::File::create(&restored)
        .and_then(|file| file.set_len(1 << 30))
        .expect("restored file");
    let other = VolumeId("vol_02".to_string());
    let imported = engine
        .import_volume(&other, 1, &restored)
        .await
        .expect("import");
    assert_eq!(imported.path, dir.join("vol_02"));
    assert!(!restored.exists(), "import renames the file into place");
    engine.delete_volume(&other).await.expect("delete");
    engine.delete_volume(&volume).await.expect("delete");
    assert!(!dir.join("vol_01").exists());
    std::fs::remove_dir_all(&dir).expect("cleanup");
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
    engine.start(&id, START).await.expect("start");

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
    engine.start(&id, START).await.expect("start");
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

    engine.stop(&id, FORCE).await.expect("stop");
    assert!(
        first.exit.await.is_err(),
        "no exit code when the VM stopped"
    );
}

#[tokio::test]
async fn serial_output_replays_the_ring_and_follows_live_output() {
    let (engine, id) = interactive().await;
    engine.start(&id, START).await.expect("start");
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
    engine.stop(&id, FORCE).await.expect("stop");
    engine.start(&id, START).await.expect("start again");
    engine.push_serial(&id, b"second boot\n");
    assert_eq!(next_bytes(&mut all).await, b"second boot\n");
    engine.stop(&id, FORCE).await.expect("stop");
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

/// A scratch directory under the crate's `target`, emptied first.
fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target")
        .join("fake-tests")
        .join(format!("warm-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// A VM spec that attaches `volume` on `quoro-vol-0`.
async fn vm_with_volume(engine: &FakeEngine, volume: &VolumeId, name: &str) -> VmId {
    let prepared = engine
        .prepare_image(&common::image_ref())
        .await
        .expect("image");
    let mut spec = common::sample_spec(&prepared.digest, name);
    spec.volumes = vec![VolumeAttach {
        volume_id: volume.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    engine.create(&spec).await.expect("create");
    spec.id
}

#[tokio::test]
async fn changed_ranges_default_to_the_whole_volume() {
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    assert_eq!(
        engine
            .volume_changed_ranges(&RangesOf::Volume(volume.clone()), None)
            .await,
        Err(EngineError::NotFound),
        "an unknown volume"
    );
    engine.ensure_volume(&volume, 2).await.expect("volume");
    let base = engine.snapshot_volume(&volume).await.expect("base");
    let next = engine.snapshot_volume(&volume).await.expect("next");
    let ranges = engine
        .volume_changed_ranges(&RangesOf::Snapshot(next.clone()), Some(&base))
        .await
        .expect("ranges");
    assert_eq!(
        ranges,
        ChangedRanges {
            ranges: vec![ByteRange {
                offset: 0,
                length: 2 << 30,
            }],
            full: true,
            extent_count: 1,
        }
    );

    let pushed = ChangedRanges {
        ranges: vec![ByteRange {
            offset: 4096,
            length: 8192,
        }],
        full: false,
        extent_count: 7,
    };
    engine.push_changed_ranges(pushed.clone());
    assert_eq!(
        engine
            .volume_changed_ranges(&RangesOf::Snapshot(next.clone()), Some(&base))
            .await,
        Ok(pushed),
        "a pushed result is returned once"
    );
    assert!(
        engine
            .volume_changed_ranges(&RangesOf::Snapshot(next.clone()), Some(&base))
            .await
            .expect("ranges")
            .full
    );

    engine.fail_next(FakeOp::VolumeChangedRanges, EngineError::Timeout);
    assert_eq!(
        engine
            .volume_changed_ranges(&RangesOf::Snapshot(next.clone()), None)
            .await,
        Err(EngineError::Timeout)
    );

    engine.delete_volume_snapshot(&base).await.expect("delete");
    assert_eq!(
        engine
            .volume_changed_ranges(&RangesOf::Snapshot(next.clone()), Some(&base))
            .await,
        Err(EngineError::NotFound),
        "a deleted base"
    );
}

#[tokio::test]
async fn changed_ranges_refuse_a_foreign_base_and_an_attached_volume() {
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    let other = VolumeId("vol_02".to_string());
    engine.ensure_volume(&volume, 1).await.expect("volume");
    engine.ensure_volume(&other, 1).await.expect("volume");
    let foreign = engine.snapshot_volume(&other).await.expect("snapshot");
    assert!(matches!(
        engine
            .volume_changed_ranges(&RangesOf::Volume(volume.clone()), Some(&foreign))
            .await,
        Err(EngineError::Conflict(_))
    ));

    vm_with_volume(&engine, &volume, "vm_warm").await;
    assert!(
        matches!(
            engine
                .volume_changed_ranges(&RangesOf::Volume(volume.clone()), None)
                .await,
            Err(EngineError::Conflict(_))
        ),
        "a VM that exists attaches the live file"
    );
}

#[tokio::test]
async fn content_mode_reports_the_blocks_that_differ() {
    let dir = scratch("content");
    let engine = FakeEngine::new();
    engine.set_volume_dir(&dir);
    engine.set_changed_ranges_mode(FakeRanges::Content);
    let volume = VolumeId("vol_01".to_string());
    let info = engine.ensure_volume(&volume, 1).await.expect("volume");
    let base = engine.snapshot_volume(&volume).await.expect("base");

    write_at(&info.path, 10, b"x");
    write_at(&info.path, 3 * 4096 + 5, b"yy");
    write_at(&info.path, 4 * 4096, b"z");
    write_at(&info.path, (1 << 30) - 1, b"!");
    let next = engine.snapshot_volume(&volume).await.expect("next");
    let ranges = engine
        .volume_changed_ranges(&RangesOf::Snapshot(next.clone()), Some(&base))
        .await
        .expect("ranges");
    assert!(!ranges.full);
    assert_eq!(
        ranges.ranges,
        vec![
            ByteRange {
                offset: 0,
                length: 4096,
            },
            ByteRange {
                offset: 3 * 4096,
                length: 2 * 4096,
            },
            ByteRange {
                offset: (1 << 30) - 4096,
                length: 4096,
            },
        ]
    );
    assert!(
        engine
            .volume_changed_ranges(&RangesOf::Snapshot(next), None)
            .await
            .expect("ranges")
            .full,
        "no base: the whole volume"
    );
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

/// Writes `bytes` at `offset` into `path` without truncating it.
fn write_at(path: &std::path::Path, offset: u64, bytes: &[u8]) {
    use std::io::{Seek as _, SeekFrom, Write as _};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(bytes).expect("write");
}

#[tokio::test]
async fn a_staged_copy_replaces_the_volume_only_when_committed() {
    let dir = scratch("stage");
    let engine = FakeEngine::new();
    engine.set_volume_dir(&dir);
    let volume = VolumeId("vol_01".to_string());

    // A first copy on a target: no volume file yet, an empty staging file.
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Empty)
        .await
        .expect("stage");
    assert_eq!(staged.size_bytes, 1 << 30);
    assert_eq!(
        std::fs::metadata(&staged.location)
            .expect("staging file")
            .len(),
        1 << 30
    );
    assert!(
        matches!(
            engine.stage_volume(&volume, 1, &StageFrom::Empty).await,
            Err(EngineError::Conflict(_))
        ),
        "one staging file per volume"
    );
    write_at(&staged.location, 0, b"first copy");
    assert!(
        engine.volumes().is_empty(),
        "nothing is a volume before the commit"
    );
    let info = engine.commit_staged_volume(&staged).await.expect("commit");
    assert_eq!(info.path, dir.join("vol_01"));
    assert!(!staged.location.exists());
    assert_eq!(engine.volumes(), vec![(volume.clone(), 1)]);
    assert!(engine.staged_volumes().is_empty());
    assert_eq!(
        &std::fs::read(&info.path).expect("read")[..10],
        b"first copy"
    );

    // The next copy clones the volume; a discarded one leaves it unchanged.
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Volume)
        .await
        .expect("stage");
    assert_eq!(
        &std::fs::read(&staged.location).expect("read")[..10],
        b"first copy"
    );
    write_at(&staged.location, 0, b"half");
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");
    assert!(!staged.location.exists());
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discarding twice is Ok");
    assert_eq!(
        &std::fs::read(&info.path).expect("read")[..10],
        b"first copy"
    );
    assert_eq!(
        engine.commit_staged_volume(&staged).await,
        Err(EngineError::NotFound),
        "a discarded staging file cannot be committed"
    );

    // A clone of a snapshot of the volume.
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    write_at(&info.path, 0, b"live write");
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Snapshot(snapshot.clone()))
        .await
        .expect("stage");
    assert_eq!(
        &std::fs::read(&staged.location).expect("read")[..10],
        b"first copy"
    );
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

/// Whether the filesystem reports the first hole of `path` before its end.
#[cfg(target_os = "linux")]
fn reports_holes(path: &std::path::Path) -> bool {
    let file = std::fs::File::open(path).expect("open");
    let length = file.metadata().expect("metadata").len();
    matches!(
        rustix::fs::seek(&file, rustix::fs::SeekFrom::Hole(0)),
        Ok(hole) if hole < length
    )
}

/// macOS keeps holes on APFS, the filesystem these tests run on there.
#[cfg(not(target_os = "linux"))]
fn reports_holes(_path: &std::path::Path) -> bool {
    true
}

#[tokio::test]
async fn snapshots_and_staging_clones_keep_the_holes_of_the_volume() {
    use std::os::unix::fs::MetadataExt as _;

    let dir = scratch("holes");
    let engine = FakeEngine::new();
    engine.set_volume_dir(&dir);
    let volume = VolumeId("vol_01".to_string());
    let info = engine.ensure_volume(&volume, 1).await.expect("volume");
    write_at(&info.path, 4096, b"near the start");
    write_at(&info.path, 512 << 20, b"in the middle");
    let allocated =
        |path: &std::path::Path| std::fs::metadata(path).expect("metadata").blocks() * 512;
    // Only a filesystem that reports holes can keep them.
    let reports_holes = reports_holes(&info.path);

    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Volume)
        .await
        .expect("stage");
    for copy in [&snapshot.location, &staged.location] {
        let bytes = std::fs::read(copy).expect("read");
        assert_eq!(bytes.len(), 1 << 30, "the volume's length");
        assert_eq!(&bytes[4096..4096 + 14], b"near the start");
        assert_eq!(&bytes[(512 << 20)..(512 << 20) + 13], b"in the middle");
        assert!(bytes[..4096].iter().all(|byte| *byte == 0));
        if reports_holes {
            assert!(
                allocated(copy) <= allocated(&info.path) + (1 << 20),
                "{} uses {} bytes, the volume {}",
                copy.display(),
                allocated(copy),
                allocated(&info.path)
            );
        }
    }
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Snapshot(snapshot.clone()))
        .await
        .expect("stage");
    if reports_holes {
        assert!(allocated(&staged.location) <= allocated(&info.path) + (1 << 20));
    }
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

#[tokio::test]
async fn staging_checks_its_source_and_the_commit_refuses_an_attached_volume() {
    let engine = FakeEngine::new();
    let volume = VolumeId("vol_01".to_string());
    let other = VolumeId("vol_02".to_string());
    assert_eq!(
        engine.stage_volume(&volume, 1, &StageFrom::Volume).await,
        Err(EngineError::NotFound),
        "no volume file to clone"
    );
    engine.ensure_volume(&volume, 1).await.expect("volume");
    engine.ensure_volume(&other, 1).await.expect("volume");
    assert!(
        matches!(
            engine.stage_volume(&volume, 2, &StageFrom::Volume).await,
            Err(EngineError::Conflict(_))
        ),
        "a clone of a file of another size"
    );
    let foreign = engine.snapshot_volume(&other).await.expect("snapshot");
    assert!(matches!(
        engine
            .stage_volume(&volume, 1, &StageFrom::Snapshot(foreign.clone()))
            .await,
        Err(EngineError::Conflict(_))
    ));
    engine
        .delete_volume_snapshot(&foreign)
        .await
        .expect("delete");
    let mut missing = foreign.clone();
    missing.volume_id = volume.clone();
    assert_eq!(
        engine
            .stage_volume(&volume, 1, &StageFrom::Snapshot(missing))
            .await,
        Err(EngineError::NotFound)
    );

    engine.fail_next(
        FakeOp::StageVolume,
        EngineError::ResourceExhausted("full".into()),
    );
    assert!(matches!(
        engine.stage_volume(&volume, 1, &StageFrom::Empty).await,
        Err(EngineError::ResourceExhausted(_))
    ));

    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Volume)
        .await
        .expect("stage");
    // A handle that names another file or size is refused, not committed.
    let mut foreign_location = staged.clone();
    foreign_location.location = std::path::PathBuf::from("/elsewhere/vol_01.staging");
    for refused in [
        engine.commit_staged_volume(&foreign_location).await.err(),
        engine.discard_staged_volume(&foreign_location).await.err(),
    ] {
        assert!(
            matches!(refused, Some(EngineError::Conflict(_))),
            "{refused:?}"
        );
    }
    let mut wrong_size = staged.clone();
    wrong_size.size_bytes *= 2;
    assert!(matches!(
        engine.commit_staged_volume(&wrong_size).await,
        Err(EngineError::Conflict(_))
    ));
    engine.fail_next(
        FakeOp::CommitStagedVolume,
        EngineError::Io("injected".into()),
    );
    assert_eq!(
        engine.commit_staged_volume(&staged).await,
        Err(EngineError::Io("injected".into()))
    );
    assert_eq!(engine.staged_volumes().len(), 1, "a failed commit keeps it");
    let vm = vm_with_volume(&engine, &volume, "vm_warm").await;
    assert!(
        matches!(
            engine.commit_staged_volume(&staged).await,
            Err(EngineError::Conflict(_))
        ),
        "a VM that exists attaches the volume"
    );
    assert_eq!(engine.staged_volumes().len(), 1, "the staging file is kept");
    engine.delete(&vm).await.expect("delete");
    engine.commit_staged_volume(&staged).await.expect("commit");
}

#[tokio::test]
async fn the_start_up_sweep_keeps_only_the_recorded_snapshots() {
    let dir = scratch("sweep");
    let engine = FakeEngine::new();
    engine.set_volume_dir(&dir);
    let volume = VolumeId("vol_01".to_string());
    engine.ensure_volume(&volume, 1).await.expect("volume");
    let kept = engine.snapshot_volume(&volume).await.expect("snapshot");
    let swept = engine.snapshot_volume(&volume).await.expect("snapshot");
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Empty)
        .await
        .expect("stage");

    let name = |snapshot: &VolumeSnapshot| {
        snapshot
            .location
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a file name")
            .to_owned()
    };
    let rule = SnapshotSweep::Keep([name(&kept)].into_iter().collect());
    let left = engine.start_up_sweep(&rule).expect("sweep");
    assert_eq!(left, [name(&kept)].into_iter().collect());
    assert_eq!(
        engine
            .volume_snapshots()
            .into_iter()
            .map(|snapshot| snapshot.snapshot_id)
            .collect::<Vec<_>>(),
        vec![kept.snapshot_id.clone()]
    );
    assert!(kept.location.exists(), "the recorded snapshot stays");
    assert!(!swept.location.exists(), "the unrecorded one is removed");
    assert!(!staged.location.exists(), "a staging file never survives");
    assert!(engine.staged_volumes().is_empty());

    let left = engine
        .start_up_sweep(&SnapshotSweep::KeepAll)
        .expect("sweep");
    assert_eq!(left, [name(&kept)].into_iter().collect());
    assert!(kept.location.exists());
    let left = engine
        .start_up_sweep(&SnapshotSweep::Keep(Default::default()))
        .expect("sweep");
    assert!(left.is_empty());
    assert!(!kept.location.exists());
    assert!(engine.volume_snapshots().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_failed_snapshot_delete_keeps_the_file() {
    let dir = scratch("delete-fails");
    let engine = FakeEngine::new();
    engine.set_volume_dir(&dir);
    let volume = VolumeId("vol_01".to_string());
    engine.ensure_volume(&volume, 1).await.expect("volume");
    let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
    engine.fail_next(
        FakeOp::DeleteVolumeSnapshot,
        EngineError::Unsupported("the volume pool is broken".into()),
    );
    assert!(matches!(
        engine.delete_volume_snapshot(&snapshot).await,
        Err(EngineError::Unsupported(_))
    ));
    assert!(snapshot.location.exists());
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete");
    assert!(!snapshot.location.exists());
    // A missing snapshot is Ok, as in the real engine (section 3.16).
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete again");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_node_without_reflink_stages_only_empty_copies() {
    let engine = FakeEngine::new();
    engine.set_snapshot_method(SnapshotMethod::PauseCopy, Duration::ZERO);
    let volume = VolumeId("vol_01".to_string());
    engine.ensure_volume(&volume, 1).await.expect("volume");
    assert!(matches!(
        engine.stage_volume(&volume, 1, &StageFrom::Volume).await,
        Err(EngineError::Unsupported(_))
    ));
    let staged = engine
        .stage_volume(&volume, 1, &StageFrom::Empty)
        .await
        .expect("an empty staging file needs no clone");
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");
}

#[tokio::test]
async fn pause_and_resume_flip_a_flag_and_the_vm_still_reads_running() {
    let (engine, id) = created().await;
    assert_eq!(
        engine.pause(&VmId("vm_missing".to_string())).await,
        Err(EngineError::NotFound)
    );
    assert!(
        matches!(engine.pause(&id).await, Err(EngineError::Conflict(_))),
        "a stopped VM cannot be paused"
    );
    engine.start(&id, START).await.expect("start");
    engine.pause(&id).await.expect("pause");
    engine.pause(&id).await.expect("pausing a paused VM is Ok");
    assert!(engine.is_paused(&id));
    assert_eq!(
        engine.inspect(&id).await.expect("inspect").state,
        VmState::Running
    );
    engine.resume(&id).await.expect("resume");
    engine
        .resume(&id)
        .await
        .expect("resuming a running VM is Ok");
    assert!(!engine.is_paused(&id));

    engine.pause(&id).await.expect("pause");
    engine.stop(&id, FORCE).await.expect("a paused VM stops");
    assert!(!engine.is_paused(&id));
    assert!(matches!(
        engine.resume(&id).await,
        Err(EngineError::Conflict(_))
    ));
    engine.start(&id, START).await.expect("start");
    assert!(!engine.is_paused(&id), "a new start is not paused");

    engine.fail_next(FakeOp::Pause, EngineError::Timeout);
    assert_eq!(engine.pause(&id).await, Err(EngineError::Timeout));
    assert!(!engine.is_paused(&id));
    engine.pause(&id).await.expect("pause");
    engine.fail_next(FakeOp::Resume, EngineError::Timeout);
    assert_eq!(engine.resume(&id).await, Err(EngineError::Timeout));
    assert!(engine.is_paused(&id), "a failed resume leaves it paused");
}

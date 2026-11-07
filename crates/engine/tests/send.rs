// SPDX-License-Identifier: AGPL-3.0-only
//! The engine's async methods return `Send` futures (D-0071), so a caller
//! generic over `E: Engine` can spawn an engine call on the tokio runtime.
//!
//! The check is mostly compile-time: `calls_are_send` builds every trait future
//! without awaiting it and passes it to a `Send` bound. The `#[tokio::test]`
//! below proves the same thing at run time with a real `tokio::spawn`.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use quoro_engine::{
    Engine, FakeEngine, ImageDigest, ImageRef, NodeNetwork, RangesOf, SnapshotMethod, StageFrom,
    StagedVolume, StopMode, VmId, VmSpec, VolumeId, VolumeSnapshot,
};

fn assert_send<T: Send>(_: &T) {}

/// The argument values one `calls_are_send` call needs.
struct Sample<'a> {
    net: &'a NodeNetwork,
    image: &'a ImageRef,
    digest: &'a ImageDigest,
    spec: &'a VmSpec,
    id: &'a VmId,
    volume: &'a VolumeId,
    snapshot: &'a VolumeSnapshot,
}

/// Builds, but does not await, the future of every async trait method. Each one
/// must satisfy `Send`, including the futures of the default volume methods.
fn calls_are_send<E: Engine>(engine: &E, sample: &Sample<'_>) {
    assert_send(&engine.capabilities());
    assert_send(&engine.probe_host());
    assert_send(&engine.ensure_network(sample.net));
    assert_send(&engine.prepare_image(sample.image));
    assert_send(&engine.remove_image(sample.digest));
    assert_send(&engine.create(sample.spec));
    assert_send(&engine.start(sample.id, Duration::from_secs(1)));
    assert_send(&engine.stop(
        sample.id,
        StopMode::Graceful {
            timeout: Duration::from_secs(1),
            force_within: Duration::from_secs(1),
        },
    ));
    assert_send(&engine.stop(
        sample.id,
        StopMode::Force {
            within: Duration::from_secs(1),
        },
    ));
    assert_send(&engine.delete(sample.id));
    assert_send(&engine.inspect(sample.id));
    assert_send(&engine.list());
    assert_send(&engine.counters(sample.id));
    assert_send(&engine.ensure_volume(sample.volume, 1));
    assert_send(&engine.import_volume(sample.volume, 1, Path::new("/fake/restore")));
    assert_send(&engine.delete_volume(sample.volume));
    assert_send(&engine.snapshot_volume(sample.volume));
    assert_send(&engine.delete_volume_snapshot(sample.snapshot));
    let newer = RangesOf::Snapshot(sample.snapshot.clone());
    assert_send(&engine.volume_changed_ranges(&newer, Some(sample.snapshot)));
    assert_send(&engine.stage_volume(sample.volume, 1, &StageFrom::Empty));
    let staged = StagedVolume {
        volume_id: sample.volume.clone(),
        size_bytes: 1 << 30,
        location: "/fake/staging/vol_01".into(),
    };
    assert_send(&engine.commit_staged_volume(&staged));
    assert_send(&engine.discard_staged_volume(&staged));
    assert_send(&engine.pause(sample.id));
    assert_send(&engine.resume(sample.id));
}

#[tokio::test]
async fn a_generic_caller_can_spawn_an_engine_call() {
    let engine = Arc::new(FakeEngine::new());
    let image = common::image_ref();
    let prepared = engine
        .prepare_image(&image)
        .await
        .expect("prepare_image succeeds");
    let spec = common::sample_spec(&prepared.digest, "vm_send");
    let net = NodeNetwork {
        bridge: "quoro0".to_string(),
        subnet: "10.42.1.0/24".to_string(),
        gateway: "10.42.1.1".parse().expect("sample gateway"),
        dns: Vec::new(),
    };
    let volume = VolumeId("vol_01".to_string());
    let snapshot = VolumeSnapshot {
        volume_id: volume.clone(),
        snapshot_id: "snap-1".to_string(),
        created_at: SystemTime::now(),
        size_bytes: 0,
        location: "/fake/snapshots/1".into(),
        method: SnapshotMethod::Reflink,
        pause: Duration::ZERO,
    };

    calls_are_send(
        &*engine,
        &Sample {
            net: &net,
            image: &image,
            digest: &prepared.digest,
            spec: &spec,
            id: &spec.id,
            volume: &volume,
            snapshot: &snapshot,
        },
    );

    let spawned = Arc::clone(&engine);
    let info = tokio::spawn(async move { spawned.inspect(&spec.id).await })
        .await
        .expect("the spawned task joins");
    assert_eq!(info, Err(quoro_engine::EngineError::NotFound));
}

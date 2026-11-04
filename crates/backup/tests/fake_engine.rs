// SPDX-License-Identifier: AGPL-3.0-only
//! An end-to-end backup and restore against `quoro_engine::FakeEngine`'s real
//! sparse volume files (backup-format.md sections 5, 6.1 and 8.1).
//!
//! The in-memory object store keys objects exactly like the repository layout,
//! and an upload of a key the store already holds is skipped, which is the
//! node's `If-None-Match: *` behaviour of the contract (a `412` counts as
//! success).

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use quoro_backup::{
    BackupWriter, ChunkId, Chunker, FORMAT, Manifest, RepoKey, Restorer, SourceKeys, SourceKind,
    chunks, layout,
};
use quoro_engine::{Engine, FakeEngine, VolumeId};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Deterministic bytes from BLAKE3's XOF.
fn seeded(len: usize, seed: u64) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&seed.to_be_bytes());
    let mut reader = hasher.finalize_xof();
    let mut data = vec![0u8; len];
    let mut written = 0;
    while written < len {
        let end = (written + 65_536).min(len);
        reader.fill(&mut data[written..end]);
        written = end;
    }
    data
}

/// A throwaway directory under the checkout's `target/` (never `/tmp`).
fn test_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/backup-fake-test")
        .join(format!("run-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("volumes")).unwrap();
    dir
}

fn header(backup_id: &str) -> Manifest {
    Manifest {
        format: FORMAT,
        backup_id: backup_id.to_owned(),
        cluster_id: "cls_one".into(),
        repository_id: "brp_one".into(),
        source_kind: SourceKind::Volume,
        source_id: "app_one".into(),
        volume_id: Some("vol_one".into()),
        fencing_token: Some(3),
        snapshot_method: Some("reflink".into()),
        logical_bytes: 0,
        chunker: Chunker::fixed(),
        chunks: Vec::new(),
        quoro_version: "0.6.0".into(),
        created_at: "2026-11-05T03:00:41Z".into(),
    }
}

/// Backs up one snapshot file, returning its manifest and chunk objects.
fn backup_file(path: &Path, keys: &SourceKeys, backup_id: &str) -> (Manifest, Vec<Vec<u8>>) {
    let mut writer = BackupWriter::new(keys.clone(), header(backup_id));
    let mut objects = Vec::new();
    for chunk in chunks(File::open(path).unwrap()) {
        let (_, object, _) = writer.push(chunk.unwrap());
        objects.push(object);
    }
    let (manifest, _, _) = writer.finish().unwrap();
    (manifest, objects)
}

/// Uploads the chunks the store does not hold yet, like `If-None-Match: *`,
/// and returns how many objects were new.
fn upload(
    store: &mut HashMap<String, Vec<u8>>,
    manifest: &Manifest,
    objects: Vec<Vec<u8>>,
) -> usize {
    let mut added = 0;
    for (entry, object) in manifest.chunks.iter().zip(objects) {
        let id = ChunkId::from_hex(&entry.id).unwrap();
        if let Entry::Vacant(slot) = store.entry(layout::chunk(&id)) {
            slot.insert(object);
            added += 1;
        }
    }
    added
}

/// Whether two files have the same bytes, compared in streams so a 1 GiB
/// volume never lands in memory.
fn same_bytes(left: &Path, right: &Path) -> bool {
    let mut left = File::open(left).unwrap();
    let mut right = File::open(right).unwrap();
    let mut left_buffer = vec![0u8; 1 << 20];
    let mut right_buffer = vec![0u8; 1 << 20];
    loop {
        let read = left.read(&mut left_buffer).unwrap();
        if read == 0 {
            return right.read(&mut right_buffer).unwrap() == 0;
        }
        if right.read(&mut right_buffer).unwrap() != read
            || left_buffer[..read] != right_buffer[..read]
        {
            return false;
        }
    }
}

#[tokio::test]
async fn a_volume_backup_dedups_then_restores_into_a_new_volume() {
    let dir = test_dir();
    let engine = FakeEngine::new();
    engine.set_volume_dir(dir.join("volumes"));

    let volume = VolumeId("vol_one".into());
    let info = engine.ensure_volume(&volume, 1).await.unwrap();
    assert_eq!(info.size_gb, 1);

    // A few MiB at a few offsets; the rest of the 1 GiB stays a hole.
    let mut file = OpenOptions::new().write(true).open(&info.path).unwrap();
    for (offset, seed) in [(0, 1), (100 * MIB, 2), (700 * MIB, 3)] {
        let data = seeded(2 * MIB as usize, seed);
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&data).unwrap();
    }
    drop(file);

    let keys = RepoKey::generate().source_keys("app_one");
    let mut store: HashMap<String, Vec<u8>> = HashMap::new();

    let first_snapshot = engine.snapshot_volume(&volume).await.unwrap();
    let (first, first_objects) = backup_file(&first_snapshot.location, &keys, "bkp_one");
    assert_eq!(first.logical_bytes, GIB);
    let added = upload(&mut store, &first, first_objects);
    assert_eq!(added, store.len());
    assert!(added < first.chunks.len());
    // The zero chunks of the sparse file all hash to one object.
    assert!(
        store.len() <= 10,
        "{} objects for a mostly empty volume",
        store.len()
    );
    assert!(same_bytes(&first_snapshot.location, &info.path));
    engine
        .delete_volume_snapshot(&first_snapshot)
        .await
        .unwrap();

    // Change one small region; the second backup must only add its chunks.
    let mut file = OpenOptions::new().write(true).open(&info.path).unwrap();
    file.seek(SeekFrom::Start(300 * MIB)).unwrap();
    file.write_all(&seeded(4096, 4)).unwrap();
    drop(file);

    let second_snapshot = engine.snapshot_volume(&volume).await.unwrap();
    let (second, second_objects) = backup_file(&second_snapshot.location, &keys, "bkp_two");
    let added = upload(&mut store, &second, second_objects);
    assert!(added >= 1, "the changed region must add a chunk");
    assert!(added <= 5, "one changed region added {added} chunk objects");

    // Restore the second manifest into a sparse file.
    let restored = dir.join("restored.img");
    let mut restorer = Restorer::create(keys.clone(), &restored, second.logical_bytes).unwrap();
    for entry in &second.chunks {
        let key = layout::chunk(&ChunkId::from_hex(&entry.id).unwrap());
        restorer.apply(entry, &store[&key]).unwrap();
    }
    restorer.finish().unwrap();
    assert!(same_bytes(&restored, &info.path));
    assert!(same_bytes(&restored, &second_snapshot.location));

    // Import it as a new volume and check the bytes survived the move.
    let imported = VolumeId("vol_restored".into());
    let imported_info = engine.import_volume(&imported, 1, &restored).await.unwrap();
    assert!(same_bytes(&imported_info.path, &second_snapshot.location));

    std::fs::remove_dir_all(&dir).unwrap();
}

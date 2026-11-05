// SPDX-License-Identifier: AGPL-3.0-only
//! Property tests for the backup repository format: round trips, dedup,
//! tampering and manifest layout (backup-format.md sections 5, 6 and 8.1).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use quoro_backup::{
    BackupWriter, CHUNK_MAX, CHUNK_MIN, ChunkId, Chunker, FORMAT, Manifest, RepoKey, Restorer,
    SourceKeys, SourceKind, chunks, decrypt_manifest, encrypt_manifest, manifest_digest,
    open_chunk, seal_chunk,
};

/// A few cases of up to ~12 MiB are enough to exercise every boundary and keep
/// the suite fast.
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(16)
}

/// Deterministic test data from BLAKE3's XOF, so a case is reproducible and
/// generating 12 MiB costs a hash pass instead of a shrink loop.
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

fn keys() -> SourceKeys {
    RepoKey::generate().source_keys("app_one")
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

/// A throwaway directory under the checkout's `target/` (never `/tmp`).
fn test_dir(seed: u64) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/backup-props")
        .join(format!("case-{seed}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes a backup of `data` and returns the manifest and its chunk objects,
/// as an in-memory store would hold them.
fn write_backup(data: &[u8], keys: &SourceKeys, backup_id: &str) -> (Manifest, Vec<Vec<u8>>) {
    let mut writer = BackupWriter::new(keys.clone(), header(backup_id));
    let mut objects = Vec::new();
    for chunk in chunks(data) {
        let (_, object, _) = writer.push(chunk.unwrap());
        objects.push(object);
    }
    let (manifest, _, _) = writer.finish().unwrap();
    (manifest, objects)
}

fn ids(manifest: &Manifest) -> Vec<ChunkId> {
    manifest
        .chunks
        .iter()
        .map(|entry| ChunkId::from_hex(&entry.id).unwrap())
        .collect()
}

proptest! {
    #![proptest_config(config())]

    /// Chunks → writer → restorer gives the same bytes, and every chunk but
    /// the last is between the fixed minimum and maximum.
    #[test]
    fn a_backup_round_trips(len in 0usize..=12 * 1024 * 1024, seed in any::<u64>()) {
        let data = seeded(len, seed);
        let keys = keys();
        let (manifest, objects) = write_backup(&data, &keys, "bkp_one");

        prop_assert_eq!(manifest.logical_bytes, data.len() as u64);
        prop_assert_eq!(manifest.chunks.len(), objects.len());
        let last = manifest.chunks.len().saturating_sub(1);
        for (index, entry) in manifest.chunks.iter().enumerate() {
            let length = entry.length as usize;
            prop_assert!(length <= CHUNK_MAX as usize);
            if index != last {
                prop_assert!(length >= CHUNK_MIN as usize);
            }
            if index + 1 < manifest.chunks.len() {
                prop_assert_eq!(
                    manifest.chunks[index + 1].offset,
                    entry.offset + u64::from(entry.length)
                );
            }
        }

        let dir = test_dir(seed);
        let path = dir.join("volume.img");
        let mut restorer = Restorer::create(keys.clone(), &path, manifest.logical_bytes).unwrap();
        for (entry, object) in manifest.chunks.iter().zip(&objects) {
            restorer.apply(entry, object).unwrap();
        }
        restorer.finish().unwrap();
        prop_assert_eq!(std::fs::read(&path).unwrap(), data);
        std::fs::remove_file(&path).unwrap();
    }

    /// The same data twice gives the same ids, and changing one byte in the
    /// middle changes only a few of them.
    #[test]
    fn dedup_is_stable_and_local(seed in any::<u64>(), position in 0f64..1f64) {
        let len = 4 * 1024 * 1024;
        let data = seeded(len, seed);
        let keys = keys();
        let (first, _) = write_backup(&data, &keys, "bkp_one");
        let (again, _) = write_backup(&data, &keys, "bkp_one");
        prop_assert_eq!(ids(&first), ids(&again));

        let mut changed = data.clone();
        let index = ((len as f64 - 1.0) * position) as usize;
        changed[index] ^= 0x80;
        let (second, _) = write_backup(&changed, &keys, "bkp_two");
        let old: HashSet<ChunkId> = ids(&first).into_iter().collect();
        let new_ids = ids(&second).into_iter().filter(|id| !old.contains(id)).count();
        prop_assert!(new_ids <= 3, "one changed byte produced {new_ids} new chunk ids");
    }

    /// Flipping any byte of a chunk object makes it refused by `open_chunk`
    /// and by `Restorer::apply`.
    #[test]
    fn a_tampered_chunk_object_is_refused(seed in any::<u64>(), position in 0f64..1f64) {
        let keys = keys();
        let data = seeded(600 * 1024, seed);
        let (id, mut object) = seal_chunk(&keys, &data);
        let index = ((object.len() as f64 - 1.0) * position) as usize;
        object[index] ^= 0x01;
        prop_assert!(open_chunk(&keys, &id, &object).is_err());

        let dir = test_dir(seed);
        let path = dir.join("tampered.img");
        let mut restorer = Restorer::create(keys.clone(), &path, data.len() as u64).unwrap();
        let entry = quoro_backup::ManifestChunk {
            id: id.to_hex(),
            offset: 0,
            length: data.len() as u32,
        };
        prop_assert!(restorer.apply(&entry, &object).is_err());
        prop_assert!(path.exists());
        std::fs::remove_file(&path).unwrap();
    }

    /// Another source's `data_key` is refused, and so is an object presented
    /// under another chunk id.
    #[test]
    fn wrong_keys_and_wrong_ids_are_refused(seed in any::<u64>(), other in any::<u32>()) {
        let keys = keys();
        let data = seeded(300 * 1024, seed);
        let (id, object) = seal_chunk(&keys, &data);

        let other_keys = RepoKey::generate().source_keys("app_two");
        prop_assert!(open_chunk(&other_keys, &id, &object).is_err());

        let mut id_bytes = [0u8; 32];
        id_bytes[..4].copy_from_slice(&other.to_be_bytes());
        let other_id = ChunkId(id_bytes);
        prop_assert!(open_chunk(&keys, &other_id, &object).is_err());
    }

    /// A tampered manifest object is refused, and a manifest with a gap, an
    /// overlap or a wrong `logical_bytes` does not validate.
    #[test]
    fn manifest_tampering_and_bad_layout_are_refused(
        seed in any::<u64>(),
        position in 0f64..1f64,
    ) {
        let keys = keys();
        // More than twice the maximum chunk size always gives at least three
        // chunks; 3 MiB of random data sometimes cut into one.
        let data = seeded(2 * CHUNK_MAX as usize + 1024 * 1024, seed);
        let (manifest, _) = write_backup(&data, &keys, "bkp_one");
        prop_assert!(manifest.chunks.len() >= 2, "the case must produce several chunks");

        let object = encrypt_manifest(&keys, &manifest).unwrap();
        prop_assert_eq!(manifest_digest(&object).len(), 64);
        let mut tampered = object.clone();
        let index = ((tampered.len() as f64 - 1.0) * position) as usize;
        tampered[index] ^= 0x01;
        prop_assert!(decrypt_manifest(&keys, "bkp_one", "app_one", &tampered).is_err());

        let mut gap = manifest.clone();
        gap.chunks[1].offset += 1;
        prop_assert!(encrypt_manifest(&keys, &gap).is_err());

        let mut overlap = manifest.clone();
        let second_offset = overlap.chunks[1].offset.saturating_sub(2);
        overlap.chunks[1].offset = second_offset;
        prop_assert!(overlap.validate().is_err());

        let mut wrong = manifest.clone();
        wrong.logical_bytes += 1;
        prop_assert!(wrong.validate().is_err());
    }
}

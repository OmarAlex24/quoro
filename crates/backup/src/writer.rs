// SPDX-License-Identifier: AGPL-3.0-only
//! Writing a backup (`backup-format.md` sections 5, 6.1 and 7.1 step 4).
//!
//! A node or the primary feeds the snapshot's chunks in file order. Each
//! [`BackupWriter::push`] returns the chunk id, the object to upload and
//! whether the chunk is all zero, which the node reports and uses to skip a
//! second upload of the same id. [`BackupWriter::finish`] returns the manifest,
//! its encrypted object and its digest.

use crate::{
    Chunk, ChunkId, Error, Manifest, ManifestChunk, Result, SourceKeys, encrypt_manifest,
    manifest_digest, seal_chunk,
};

/// Builds one backup's manifest while its chunks are written.
pub struct BackupWriter {
    keys: SourceKeys,
    manifest: Manifest,
    chunks: Vec<ManifestChunk>,
    offset: u64,
    /// The first layout error, reported by [`BackupWriter::finish`] because
    /// `push` returns a tuple.
    error: Option<Error>,
}

impl BackupWriter {
    /// Starts a backup from `manifest_header_without_chunks`, whose `chunks`
    /// are ignored and replaced by the pushed chunks.
    ///
    /// Its `logical_bytes` is written by [`BackupWriter::finish`] as the total
    /// of the pushed chunk lengths, so the header only needs the ids, the
    /// source and the chunker.
    #[must_use]
    pub fn new(keys: SourceKeys, manifest_header_without_chunks: Manifest) -> Self {
        let mut manifest = manifest_header_without_chunks;
        manifest.chunks.clear();
        Self {
            keys,
            manifest,
            chunks: Vec::new(),
            offset: 0,
            error: None,
        }
    }

    /// Adds the next chunk in file order.
    ///
    /// Returns its id, its object to upload and whether it is all zero. A
    /// chunk that is empty, longer than [`crate::CHUNK_MAX`] or not at the
    /// expected offset is still returned, and makes [`BackupWriter::finish`]
    /// fail with [`Error::Format`].
    pub fn push(&mut self, chunk: Chunk) -> (ChunkId, Vec<u8>, bool) {
        if self.error.is_none() {
            if chunk.data.is_empty() {
                self.error = Some(Error::Format("an empty chunk cannot be written".into()));
            } else if chunk.data.len() > crate::CHUNK_MAX as usize {
                self.error = Some(Error::Format(format!(
                    "chunk length {} is outside 1..={}",
                    chunk.data.len(),
                    crate::CHUNK_MAX
                )));
            } else if chunk.offset != self.offset {
                self.error = Some(Error::Format(format!(
                    "chunk at offset {} is expected at {}",
                    chunk.offset, self.offset
                )));
            }
        }
        let all_zero = chunk.data.iter().all(|byte| *byte == 0);
        let (id, object) = seal_chunk(&self.keys, &chunk.data);
        self.chunks.push(ManifestChunk {
            id: id.to_hex(),
            offset: chunk.offset,
            length: u32::try_from(chunk.data.len()).expect("a chunk is bounded by CHUNK_MAX"),
        });
        self.offset += chunk.data.len() as u64;
        (id, object, all_zero)
    }

    /// The manifest, its encrypted object and its digest, after the last chunk.
    ///
    /// # Errors
    ///
    /// The first layout error a [`BackupWriter::push`] saw, or any error
    /// [`Manifest::validate`] or [`encrypt_manifest`] reports.
    pub fn finish(mut self) -> Result<(Manifest, Vec<u8>, String)> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.manifest.chunks = std::mem::take(&mut self.chunks);
        self.manifest.logical_bytes = self.offset;
        let object = encrypt_manifest(&self.keys, &self.manifest)?;
        let digest = manifest_digest(&object);
        Ok((self.manifest, object, digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepoKey;
    use crate::chunking::chunks;
    use crate::{Chunker, FORMAT, SourceKind, decrypt_manifest};

    fn keys() -> SourceKeys {
        RepoKey::generate().source_keys("app_one")
    }

    fn header() -> Manifest {
        Manifest {
            format: FORMAT,
            backup_id: "bkp_one".into(),
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

    fn seeded(len: usize) -> Vec<u8> {
        let mut data = vec![0u8; len];
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"writer");
        hasher.finalize_xof().fill(&mut data);
        data
    }

    #[test]
    fn writes_a_manifest_covering_every_chunk() {
        let keys = keys();
        let data = seeded(5 * 1024 * 1024 + 3);
        let mut writer = BackupWriter::new(keys.clone(), header());
        let mut ids = Vec::new();
        for chunk in chunks(data.as_slice()) {
            let (id, object, all_zero) = writer.push(chunk.unwrap());
            assert!(!all_zero);
            assert!(!object.is_empty());
            ids.push(id);
        }
        let (manifest, object, digest) = writer.finish().unwrap();
        assert_eq!(manifest.logical_bytes, data.len() as u64);
        assert_eq!(manifest.chunks.len(), ids.len());
        assert_eq!(
            manifest
                .chunks
                .iter()
                .map(|c| c.id.clone())
                .collect::<Vec<_>>(),
            ids.iter().map(ChunkId::to_hex).collect::<Vec<_>>()
        );
        assert_eq!(digest, manifest_digest(&object));
        assert_eq!(
            decrypt_manifest(&keys, "bkp_one", "app_one", &object).unwrap(),
            manifest
        );
    }

    #[test]
    fn an_all_zero_chunk_is_reported_and_still_written() {
        let keys = keys();
        let mut writer = BackupWriter::new(keys.clone(), header());
        let (id, object, all_zero) = writer.push(Chunk {
            offset: 0,
            data: vec![0u8; 1024 * 1024],
        });
        assert!(all_zero);
        let (manifest, _, _) = writer.finish().unwrap();
        assert_eq!(manifest.chunks[0].id, id.to_hex());
        assert!(object.len() < 4096);
    }

    #[test]
    fn a_gap_or_an_empty_chunk_fails_finish() {
        let mut writer = BackupWriter::new(keys(), header());
        writer.push(Chunk {
            offset: 1,
            data: vec![1u8; 16],
        });
        assert!(matches!(writer.finish(), Err(Error::Format(_))));

        let mut writer = BackupWriter::new(keys(), header());
        writer.push(Chunk {
            offset: 0,
            data: Vec::new(),
        });
        assert!(matches!(writer.finish(), Err(Error::Format(_))));
    }

    #[test]
    fn an_empty_source_writes_an_empty_manifest() {
        let writer = BackupWriter::new(keys(), header());
        let (manifest, object, _) = writer.finish().unwrap();
        assert!(manifest.chunks.is_empty());
        assert_eq!(manifest.logical_bytes, 0);
        assert!(!object.is_empty());
    }
}

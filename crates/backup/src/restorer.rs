// SPDX-License-Identifier: AGPL-3.0-only
//! Restoring a backup into a sparse file (`backup-format.md` section 8.1).
//!
//! [`Restorer::create`] creates the file at the volume's size. Each
//! [`Restorer::apply`] opens one manifest entry's chunk, checks it against the
//! entry, and writes it at its offset; an all-zero chunk is not written, so it
//! stays a hole in the sparse file. [`Restorer::finish`] checks the chunks
//! covered the file exactly and fsyncs it.
//!
//! On any error the caller deletes the partial file: this crate never does,
//! because the file may be the only copy of a volume being restored.

use std::fs::File;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::Path;

use crate::{CHUNK_MAX, ChunkId, Error, ManifestChunk, Result, SourceKeys, open_chunk};

/// Restores a backup into one sparse file.
pub struct Restorer {
    keys: SourceKeys,
    file: File,
    logical_bytes: u64,
    next_offset: u64,
}

impl Restorer {
    /// Creates `path` at `file_len` bytes, ready to receive the chunks of the
    /// source `keys` belong to.
    ///
    /// An existing file is truncated. A zero-length source is allowed.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the file cannot be created or sized.
    pub fn create(keys: SourceKeys, path: impl AsRef<Path>, file_len: u64) -> Result<Self> {
        let file = File::create(path)?;
        file.set_len(file_len)?;
        Ok(Self {
            keys,
            file,
            logical_bytes: file_len,
            next_offset: 0,
        })
    }

    /// The offset the next entry must have.
    #[must_use]
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Opens one manifest entry's object and writes it at its offset.
    ///
    /// # Errors
    ///
    /// [`Error::Format`] when the entry is empty, larger than
    /// [`CHUNK_MAX`] or not at [`Restorer::next_offset`]; [`Error::Corrupt`]
    /// when the object does not authenticate, does not hash to the entry's id,
    /// or has another length; and [`Error::Io`] when the write fails.
    pub fn apply(&mut self, entry: &ManifestChunk, object: &[u8]) -> Result<()> {
        if entry.offset != self.next_offset {
            return Err(Error::Format(format!(
                "chunk at offset {} is expected at {}",
                entry.offset, self.next_offset
            )));
        }
        if entry.length == 0 || entry.length > CHUNK_MAX {
            return Err(Error::Format(format!(
                "chunk length {} is outside 1..={CHUNK_MAX}",
                entry.length
            )));
        }
        let id = ChunkId::from_hex(&entry.id)?;
        let plaintext = open_chunk(&self.keys, &id, object)?;
        if plaintext.len() != entry.length as usize {
            return Err(Error::Corrupt(format!(
                "the chunk is {} bytes, not {}",
                plaintext.len(),
                entry.length
            )));
        }
        if plaintext.iter().any(|byte| *byte != 0) {
            self.file.seek(SeekFrom::Start(entry.offset))?;
            self.file.write_all(&plaintext)?;
        }
        self.next_offset += u64::from(entry.length);
        Ok(())
    }

    /// Checks the chunks covered the file exactly and fsyncs it.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] when the chunks do not cover `logical_bytes`, and
    /// [`Error::Io`] when the fsync fails.
    pub fn finish(self) -> Result<()> {
        if self.next_offset != self.logical_bytes {
            return Err(Error::Corrupt(format!(
                "the chunks cover {} bytes, not {}",
                self.next_offset, self.logical_bytes
            )));
        }
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunking::chunks;
    use crate::{
        BackupWriter, Chunk, Chunker, FORMAT, Manifest, RepoKey, SourceKind, decrypt_manifest,
        encrypt_manifest,
    };
    use std::path::PathBuf;

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

    fn test_dir() -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/backup-unit-tests")
            .join(format!("restore-{}", blake3::hash(b"restorer").to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn seeded(len: usize) -> Vec<u8> {
        let mut data = vec![0u8; len];
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"restorer");
        hasher.finalize_xof().fill(&mut data);
        data
    }

    #[test]
    fn restores_identical_bytes_and_leaves_zero_chunks_as_holes() {
        let keys = keys();
        let mut data = seeded(3 * 1024 * 1024);
        // A run of zeros that spans several chunks stays sparse.
        data[1_500_000..2_500_000].fill(0);

        let mut writer = BackupWriter::new(keys.clone(), header());
        let mut objects = Vec::new();
        for chunk in chunks(data.as_slice()) {
            let (_, object, _) = writer.push(chunk.unwrap());
            objects.push(object);
        }
        let (manifest, _, _) = writer.finish().unwrap();

        let dir = test_dir();
        let path = dir.join("volume.img");
        let mut restorer = Restorer::create(keys.clone(), &path, manifest.logical_bytes).unwrap();
        for (entry, object) in manifest.chunks.iter().zip(&objects) {
            restorer.apply(entry, object).unwrap();
        }
        restorer.finish().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), data);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn an_out_of_order_or_short_file_fails() {
        let keys = keys();
        let data = seeded(1024 * 1024);
        let mut writer = BackupWriter::new(keys.clone(), header());
        let mut objects = Vec::new();
        for chunk in chunks(data.as_slice()) {
            objects.push(writer.push(chunk.unwrap()).1);
        }
        let (manifest, _, _) = writer.finish().unwrap();

        let dir = test_dir();
        let path = dir.join("short.img");
        // The file is one byte longer than the chunks cover.
        let mut restorer =
            Restorer::create(keys.clone(), &path, manifest.logical_bytes + 1).unwrap();
        let mut swapped = manifest.clone();
        swapped.chunks[0].offset = 1;
        assert!(matches!(
            restorer.apply(&swapped.chunks[0], &objects[0]),
            Err(Error::Format(_))
        ));
        restorer.apply(&manifest.chunks[0], &objects[0]).unwrap();
        assert!(matches!(restorer.finish(), Err(Error::Corrupt(_))));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_tampered_object_fails_and_the_partial_file_stays_for_the_caller() {
        let keys = keys();
        let data = seeded(1024 * 1024);
        let mut writer = BackupWriter::new(keys.clone(), header());
        let (_, mut object, _) = writer.push(Chunk {
            offset: 0,
            data: data.clone(),
        });
        let (manifest, encrypted, _) = writer.finish().unwrap();
        let entry = &manifest.chunks[0];

        object[24] ^= 0x01;
        let dir = test_dir();
        let path = dir.join("tampered.img");
        let mut restorer = Restorer::create(keys.clone(), &path, manifest.logical_bytes).unwrap();
        assert!(restorer.apply(entry, &object).is_err());
        assert!(path.exists());
        std::fs::remove_file(&path).unwrap();

        // A manifest that claims the wrong length is refused too.
        let mut wrong = manifest.clone();
        wrong.logical_bytes += 1;
        assert!(encrypt_manifest(&keys, &wrong).is_err());
        assert!(decrypt_manifest(&keys, "bkp_one", "app_one", &encrypted).is_ok());
    }
}

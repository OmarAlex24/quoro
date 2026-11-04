// SPDX-License-Identifier: AGPL-3.0-only
//! Content-defined chunking of a snapshot file (`backup-format.md` section 5).
//!
//! FastCDC, the 2020 variant, with a minimum of 256 KiB, an average of 1 MiB
//! and a maximum of 4 MiB, over the whole snapshot file. Each backup scans the
//! full snapshot; there is no change tracking between backups. The reader is
//! streamed, so a snapshot is never held in memory as a whole.

use std::io::Read;

use fastcdc::v2020::StreamCDC;

use crate::{CHUNK_AVG, CHUNK_MAX, CHUNK_MIN, Error, Result};

/// One chunk of a snapshot: its offset in the file and its plaintext bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    /// Byte offset of the chunk in the snapshot file.
    pub offset: u64,
    /// The chunk's plaintext bytes.
    pub data: Vec<u8>,
}

/// Chunks `reader` with the fixed parameters of section 5.
///
/// The last chunk may be shorter than [`CHUNK_MIN`]; every other chunk is
/// between [`CHUNK_MIN`] and [`CHUNK_MAX`] bytes.
pub fn chunks<R: Read>(reader: R) -> impl Iterator<Item = Result<Chunk>> {
    StreamCDC::new(
        reader,
        CHUNK_MIN as usize,
        CHUNK_AVG as usize,
        CHUNK_MAX as usize,
    )
    .map(|result| {
        result
            .map(|chunk| Chunk {
                offset: chunk.offset,
                data: chunk.data,
            })
            .map_err(|error| Error::Io(error.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes from BLAKE3's XOF, so the test data is fast and deterministic.
    fn seeded(len: usize, seed: &[u8]) -> Vec<u8> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(seed);
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

    #[test]
    fn chunks_cover_the_file_in_order() {
        let data = seeded(5 * 1024 * 1024 + 17, b"chunking");
        let chunks: Vec<Chunk> = chunks(data.as_slice()).map(Result::unwrap).collect();
        assert!(chunks.len() > 1);
        let mut offset = 0;
        for (index, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.offset, offset);
            assert!(!chunk.data.is_empty());
            assert!(chunk.data.len() <= CHUNK_MAX as usize);
            if index + 1 < chunks.len() {
                assert!(chunk.data.len() >= CHUNK_MIN as usize);
            }
            offset += chunk.data.len() as u64;
        }
        assert_eq!(offset, data.len() as u64);
        let joined: Vec<u8> = chunks.into_iter().flat_map(|chunk| chunk.data).collect();
        assert_eq!(joined, data);
    }

    #[test]
    fn an_empty_file_has_no_chunks() {
        assert_eq!(chunks(&[][..]).count(), 0);
    }

    #[test]
    fn a_short_file_is_one_chunk() {
        let mut chunks = chunks(&b"tiny"[..]).map(Result::unwrap);
        let chunk = chunks.next().unwrap();
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.data, b"tiny");
        assert!(chunks.next().is_none());
    }
}

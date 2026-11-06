// SPDX-License-Identifier: AGPL-3.0-only
//! Cell hashes of a warm volume copy (`node-agent-protocol.md` section 6.13,
//! ADR 0173).
//!
//! A volume is divided into cells of 1 MiB at offsets `k × 1 048 576`; a
//! volume's size is a whole number of GiB, so every cell is full. A cell's
//! hash is the BLAKE3 hash of its 1 MiB, and the **root hash** of a copy is
//! the BLAKE3 hash of the concatenation of every cell's hash in offset order.
//! Each side of a warm volume keeps the cell hashes of its copy and updates
//! only the cells a cycle touched, so neither side reads the whole volume per
//! cycle.
//!
//! The cells are not FastCDC chunks: they apply in place, at fixed offsets.

use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom};
use std::sync::OnceLock;

/// The size of one cell, 1 MiB.
pub const CELL: u64 = 1 << 20;

/// The BLAKE3 hash of one cell.
pub type CellHash = [u8; 32];

/// The hash of a cell that reads as zeros: a hole, or never written.
#[must_use]
pub fn zero_cell_hash() -> CellHash {
    static ZERO: OnceLock<CellHash> = OnceLock::new();
    *ZERO.get_or_init(|| hash_cell(&vec![0_u8; CELL as usize]))
}

/// The BLAKE3 hash of one cell's bytes.
#[must_use]
pub fn hash_cell(bytes: &[u8]) -> CellHash {
    *blake3::hash(bytes).as_bytes()
}

/// The index of every cell that one of `ranges` (`(offset, length)` in
/// bytes) touches.
#[must_use]
pub fn cells_touched(ranges: impl IntoIterator<Item = (u64, u64)>) -> BTreeSet<u64> {
    let mut cells = BTreeSet::new();
    for (offset, length) in ranges {
        if length == 0 {
            continue;
        }
        let first = offset / CELL;
        let last = offset.saturating_add(length - 1) / CELL;
        cells.extend(first..=last);
    }
    cells
}

/// Why a set of cell hashes could not be built or read.
#[derive(Debug, PartialEq, Eq)]
pub enum CellsError {
    /// The size is not a whole number of cells.
    Size(u64),
    /// The stored form does not hold one hash per cell of the size.
    Stored { bytes: usize, cells: u64 },
    /// A cell index beyond the copy.
    OutOfRange(u64),
}

impl std::fmt::Display for CellsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Size(size) => write!(f, "{size} bytes is not a whole number of 1 MiB cells"),
            Self::Stored { bytes, cells } => {
                write!(f, "{bytes} stored bytes do not hold {cells} cell hashes")
            }
            Self::OutOfRange(cell) => write!(f, "cell {cell} is beyond the copy"),
        }
    }
}

impl std::error::Error for CellsError {}

/// The cell hashes of one copy of a volume.
#[derive(Clone, PartialEq, Eq)]
pub struct CellHashes {
    hashes: Vec<CellHash>,
}

impl std::fmt::Debug for CellHashes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellHashes")
            .field("cells", &self.hashes.len())
            .field("root", &self.root_hex())
            .finish()
    }
}

/// The number of cells of a copy of `size_bytes`.
fn cell_count(size_bytes: u64) -> Result<u64, CellsError> {
    if size_bytes.is_multiple_of(CELL) {
        Ok(size_bytes / CELL)
    } else {
        Err(CellsError::Size(size_bytes))
    }
}

impl CellHashes {
    /// The hashes of a copy of `size_bytes` that reads as zeros everywhere:
    /// an empty sparse file, where a full copy starts.
    ///
    /// # Errors
    ///
    /// [`CellsError::Size`] when the size is not a whole number of cells.
    pub fn zeroed(size_bytes: u64) -> Result<Self, CellsError> {
        let cells =
            usize::try_from(cell_count(size_bytes)?).map_err(|_| CellsError::Size(size_bytes))?;
        Ok(Self {
            hashes: vec![zero_cell_hash(); cells],
        })
    }

    /// Reads the stored form back: the hashes concatenated in cell order.
    ///
    /// # Errors
    ///
    /// [`CellsError`] when the size is not a whole number of cells or the
    /// bytes do not hold exactly one hash per cell.
    pub fn from_bytes(bytes: &[u8], size_bytes: u64) -> Result<Self, CellsError> {
        let cells = cell_count(size_bytes)?;
        let (hashes, rest) = bytes.as_chunks::<32>();
        if !rest.is_empty() || hashes.len() as u64 != cells {
            return Err(CellsError::Stored {
                bytes: bytes.len(),
                cells,
            });
        }
        Ok(Self {
            hashes: hashes.to_vec(),
        })
    }

    /// The stored form: the hashes concatenated in cell order.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.hashes.concat()
    }

    /// Hashes every cell of a copy by reading it once.
    ///
    /// # Errors
    ///
    /// An I/O error from the reader, `InvalidInput` when the size is not a
    /// whole number of cells, and `UnexpectedEof` when the copy is shorter.
    pub fn compute<R: Read + Seek>(copy: &mut R, size_bytes: u64) -> std::io::Result<Self> {
        let mut hashes = Self::zeroed(size_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        hashes.rehash(copy, 0..hashes.len())?;
        Ok(hashes)
    }

    /// Hashes `cells` of a copy again by reading each one.
    ///
    /// # Errors
    ///
    /// An I/O error from the reader, `InvalidInput` for a cell beyond the
    /// copy, and `UnexpectedEof` when the copy is shorter than the cell.
    pub fn rehash<R: Read + Seek>(
        &mut self,
        copy: &mut R,
        cells: impl IntoIterator<Item = u64>,
    ) -> std::io::Result<()> {
        let mut buf = vec![0_u8; CELL as usize];
        for cell in cells {
            let index = usize::try_from(cell)
                .ok()
                .filter(|index| *index < self.hashes.len())
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        CellsError::OutOfRange(cell),
                    )
                })?;
            copy.seek(SeekFrom::Start(cell * CELL))?;
            copy.read_exact(&mut buf)?;
            self.hashes[index] = hash_cell(&buf);
        }
        Ok(())
    }

    /// Sets the hash of one cell, for a cell whose bytes the caller hashed
    /// whole: a block that covers it, or a range of zeros.
    ///
    /// # Errors
    ///
    /// [`CellsError::OutOfRange`] for a cell beyond the copy.
    pub fn set(&mut self, cell: u64, hash: CellHash) -> Result<(), CellsError> {
        let slot = usize::try_from(cell)
            .ok()
            .and_then(|index| self.hashes.get_mut(index))
            .ok_or(CellsError::OutOfRange(cell))?;
        *slot = hash;
        Ok(())
    }

    /// The number of cells.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.hashes.len() as u64
    }

    /// Whether the copy has no cells.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// The hash of one cell.
    #[must_use]
    pub fn get(&self, cell: u64) -> Option<&CellHash> {
        usize::try_from(cell).ok().and_then(|i| self.hashes.get(i))
    }

    /// The root hash: BLAKE3 over every cell hash in offset order.
    #[must_use]
    pub fn root(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for hash in &self.hashes {
            hasher.update(hash);
        }
        *hasher.finalize().as_bytes()
    }

    /// The root hash as 64 lowercase hex characters, as the manifest frame
    /// carries it.
    #[must_use]
    pub fn root_hex(&self) -> String {
        blake3::Hash::from(self.root()).to_hex().to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{CELL, CellHashes, CellsError, cells_touched, hash_cell, zero_cell_hash};

    const SIZE: u64 = 8 * CELL;

    fn copy_with(writes: &[(u64, &[u8])]) -> Vec<u8> {
        let mut bytes = vec![0_u8; SIZE as usize];
        for (offset, data) in writes {
            let start = *offset as usize;
            bytes[start..start + data.len()].copy_from_slice(data);
        }
        bytes
    }

    #[test]
    fn an_empty_copy_hashes_like_its_zeroed_hashes() {
        let computed =
            CellHashes::compute(&mut Cursor::new(vec![0_u8; SIZE as usize]), SIZE).expect("hash");
        assert_eq!(computed, CellHashes::zeroed(SIZE).expect("zeroed"));
        assert_eq!(computed.len(), 8);
        assert_eq!(computed.get(3), Some(&zero_cell_hash()));
    }

    #[test]
    fn the_root_is_blake3_over_the_cell_hashes_in_order() {
        let bytes = copy_with(&[(0, b"first"), (5 * CELL + 7, b"sixth")]);
        let hashes = CellHashes::compute(&mut Cursor::new(bytes.clone()), SIZE).expect("hash");
        let mut concatenated = Vec::new();
        for cell in bytes.chunks(CELL as usize) {
            concatenated.extend_from_slice(&hash_cell(cell));
        }
        assert_eq!(hashes.root(), *blake3::hash(&concatenated).as_bytes());
        assert_eq!(hashes.root_hex().len(), 64);
        assert_eq!(hashes.root_hex(), hashes.root_hex().to_lowercase());
    }

    #[test]
    fn rehashing_the_touched_cells_matches_a_full_hash() {
        let before = copy_with(&[(0, b"first")]);
        let mut hashes = CellHashes::compute(&mut Cursor::new(before), SIZE).expect("hash");
        let writes: [(u64, &[u8]); 2] = [(CELL - 2, b"across"), (7 * CELL + 10, b"last")];
        let after = copy_with(&[(0, b"first"), writes[0], writes[1]]);
        let touched = cells_touched(writes.iter().map(|(o, d)| (*o, d.len() as u64)));
        assert_eq!(touched.into_iter().collect::<Vec<_>>(), vec![0, 1, 7]);
        hashes
            .rehash(
                &mut Cursor::new(after.clone()),
                cells_touched(writes.iter().map(|(o, d)| (*o, d.len() as u64))),
            )
            .expect("rehash");
        assert_eq!(
            hashes,
            CellHashes::compute(&mut Cursor::new(after), SIZE).expect("hash")
        );
    }

    #[test]
    fn the_stored_form_round_trips_and_is_checked() {
        let hashes =
            CellHashes::compute(&mut Cursor::new(copy_with(&[(3, b"x")])), SIZE).expect("hash");
        let stored = hashes.to_bytes();
        assert_eq!(stored.len(), 8 * 32);
        assert_eq!(CellHashes::from_bytes(&stored, SIZE), Ok(hashes));
        assert_eq!(
            CellHashes::from_bytes(&stored[..32], SIZE),
            Err(CellsError::Stored {
                bytes: 32,
                cells: 8
            })
        );
        assert_eq!(
            CellHashes::from_bytes(&stored, SIZE + 1),
            Err(CellsError::Size(SIZE + 1))
        );
    }

    #[test]
    fn a_cell_beyond_the_copy_or_a_short_copy_is_an_error() {
        let mut hashes = CellHashes::zeroed(SIZE).expect("zeroed");
        let mut copy = Cursor::new(vec![0_u8; SIZE as usize]);
        assert!(hashes.rehash(&mut copy, [8]).is_err());
        let mut short = Cursor::new(vec![0_u8; (SIZE - 1) as usize]);
        assert!(hashes.rehash(&mut short, [7]).is_err());
        assert!(CellHashes::compute(&mut short, SIZE).is_err());
        assert_eq!(hashes.set(8, [0; 32]), Err(CellsError::OutOfRange(8)));
    }

    #[test]
    fn setting_a_whole_cell_matches_hashing_it() {
        let mut bytes = copy_with(&[]);
        let cell = vec![5_u8; CELL as usize];
        bytes[2 * CELL as usize..3 * CELL as usize].copy_from_slice(&cell);
        let mut hashes = CellHashes::zeroed(SIZE).expect("zeroed");
        hashes.set(2, hash_cell(&cell)).expect("set");
        assert_eq!(
            hashes,
            CellHashes::compute(&mut Cursor::new(bytes), SIZE).expect("hash")
        );
    }

    #[test]
    fn empty_ranges_touch_no_cell() {
        assert!(cells_touched([(5, 0)]).is_empty());
        assert_eq!(
            cells_touched([(CELL, CELL)])
                .into_iter()
                .collect::<Vec<_>>(),
            vec![1]
        );
    }
}

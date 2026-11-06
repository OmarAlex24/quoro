// SPDX-License-Identifier: AGPL-3.0-only
//! The extent-map comparison behind `volume_changed_ranges` (engine-trait.md
//! section 3.22, ADR 0172).
//!
//! Two files of one volume that share a block through a reflink hold the same
//! bytes there, so a range of the newer file is unchanged when it maps to the
//! very same physical block as the same range of the base, or is a hole in
//! both. Everything else may have changed and is reported: a range mapped in
//! only one of the files, a range mapped to another physical block, and any
//! range covered by an extent whose location cannot be compared (the FIEMAP
//! flags `UNKNOWN`, `DELALLOC`, `ENCODED`, `NOT_ALIGNED`, `DATA_INLINE` and
//! `DATA_TAIL`, or a physical address of zero). The result can only be too
//! large, never too small: a range whose bytes are equal may be reported, a
//! range that changed never goes missing.
//!
//! The module reads no file. The Cloud Hypervisor backend feeds it the extent
//! maps it read with `FS_IOC_FIEMAP`, which keeps the comparison testable on
//! any platform.

use crate::ByteRange;

/// The granularity of reported ranges: 4 KiB, the block size the contract
/// rounds every range out to (section 3.22).
pub const BLOCK: u64 = 4096;

/// `FIEMAP_EXTENT_UNKNOWN`: the data location is unknown.
pub const FIEMAP_EXTENT_UNKNOWN: u32 = 0x0000_0002;
/// `FIEMAP_EXTENT_DELALLOC`: the location is still pending.
pub const FIEMAP_EXTENT_DELALLOC: u32 = 0x0000_0004;
/// `FIEMAP_EXTENT_ENCODED`: the data cannot be read while the filesystem is
/// unmounted.
pub const FIEMAP_EXTENT_ENCODED: u32 = 0x0000_0008;
/// `FIEMAP_EXTENT_NOT_ALIGNED`: the extent's offsets may not be block aligned.
pub const FIEMAP_EXTENT_NOT_ALIGNED: u32 = 0x0000_0100;
/// `FIEMAP_EXTENT_DATA_INLINE`: the data is mixed with metadata.
pub const FIEMAP_EXTENT_DATA_INLINE: u32 = 0x0000_0200;
/// `FIEMAP_EXTENT_DATA_TAIL`: several files share the block.
pub const FIEMAP_EXTENT_DATA_TAIL: u32 = 0x0000_0400;
/// `FIEMAP_EXTENT_UNWRITTEN`: the space is allocated and reads as zeros.
pub const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x0000_0800;

/// The flags that make an extent's physical location meaningless for a
/// comparison (section 3.22).
const INCOMPARABLE: u32 = FIEMAP_EXTENT_UNKNOWN
    | FIEMAP_EXTENT_DELALLOC
    | FIEMAP_EXTENT_ENCODED
    | FIEMAP_EXTENT_NOT_ALIGNED
    | FIEMAP_EXTENT_DATA_INLINE
    | FIEMAP_EXTENT_DATA_TAIL;

/// One extent of a file's map.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    /// Offset in the file, in bytes.
    pub logical: u64,
    /// Offset on the device, in bytes.
    pub physical: u64,
    /// Length, in bytes.
    pub length: u64,
    /// The location cannot be compared: the range counts as changed.
    pub incomparable: bool,
    /// Allocated but unwritten: it reads as zeros.
    pub unwritten: bool,
}

impl Extent {
    /// An extent as `FS_IOC_FIEMAP` reports it, classified by its flags.
    ///
    /// A physical address of zero is treated as incomparable too: no
    /// filesystem places file data in its first block, and a filesystem that
    /// reports zero is not telling where the data is.
    #[must_use]
    pub fn from_fiemap(logical: u64, physical: u64, length: u64, flags: u32) -> Self {
        Self {
            logical,
            physical,
            length,
            incomparable: flags & INCOMPARABLE != 0 || physical == 0,
            unwritten: flags & FIEMAP_EXTENT_UNWRITTEN != 0,
        }
    }

    /// The end of the extent in the file, saturating.
    fn end(&self) -> u64 {
        self.logical.saturating_add(self.length)
    }
}

/// The ranges of a file of `size` bytes whose data may differ between the
/// extent map `newer` and the extent map `base` of an earlier reflink clone
/// of the same file.
///
/// The result is sorted, does not overlap, and every range is rounded out to
/// [`BLOCK`] and cut at `size`. Extents past `size` (speculative
/// preallocation) are ignored.
///
/// `None` when either map cannot be read as a map: two extents that overlap
/// once sorted. A filesystem never reports that, and the caller then treats
/// the maps as not comparable and ships a full copy.
#[must_use]
pub fn changed_ranges(newer: &[Extent], base: &[Extent], size: u64) -> Option<Vec<ByteRange>> {
    let newer = normalized(newer, size)?;
    let base = normalized(base, size)?;

    let mut bounds = Vec::with_capacity(2 * (newer.len() + base.len()) + 2);
    bounds.push(0);
    bounds.push(size);
    for extent in newer.iter().chain(base.iter()) {
        bounds.push(extent.logical);
        bounds.push(extent.end());
    }
    bounds.sort_unstable();
    bounds.dedup();

    let mut changed = Vec::new();
    let mut next_newer = 0;
    let mut next_base = 0;
    for window in bounds.windows(2) {
        let (start, end) = (window[0], window[1]);
        if start >= size {
            break;
        }
        let in_newer = covering(&newer, &mut next_newer, start);
        let in_base = covering(&base, &mut next_base, start);
        if differs(in_newer, in_base, start) {
            changed.push(ByteRange {
                offset: start,
                length: end - start,
            });
        }
    }
    Some(round_and_merge(changed, size))
}

/// Rounds every range out to [`BLOCK`], cuts it at `size`, drops empty ones
/// and merges the ones that overlap or touch. The input need not be sorted.
#[must_use]
pub fn round_and_merge(mut ranges: Vec<ByteRange>, size: u64) -> Vec<ByteRange> {
    ranges.sort_unstable_by_key(|range| range.offset);
    let mut merged: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        let start = range.offset - range.offset % BLOCK;
        let end = round_up(range.offset.saturating_add(range.length)).min(size);
        if start >= end {
            continue;
        }
        match merged.last_mut() {
            Some(last) if start <= last.offset + last.length => {
                let last_end = last.offset + last.length;
                if end > last_end {
                    last.length = end - last.offset;
                }
            }
            _ => merged.push(ByteRange {
                offset: start,
                length: end - start,
            }),
        }
    }
    merged
}

/// `value` rounded up to a multiple of [`BLOCK`], saturating at the largest
/// multiple.
fn round_up(value: u64) -> u64 {
    match value % BLOCK {
        0 => value,
        rest => value
            .checked_add(BLOCK - rest)
            .unwrap_or(u64::MAX - u64::MAX % BLOCK),
    }
}

/// The map sorted by offset, cut at `size`, without empty extents; `None`
/// when two extents overlap.
fn normalized(map: &[Extent], size: u64) -> Option<Vec<Extent>> {
    let mut extents: Vec<Extent> = map
        .iter()
        .filter(|extent| extent.length > 0 && extent.logical < size)
        .map(|extent| Extent {
            length: extent.end().min(size) - extent.logical,
            ..*extent
        })
        .collect();
    extents.sort_unstable_by_key(|extent| extent.logical);
    for pair in extents.windows(2) {
        if pair[1].logical < pair[0].end() {
            return None;
        }
    }
    Some(extents)
}

/// The extent of the sorted, non-overlapping `map` that covers `offset`,
/// moving `next` forward past the extents that end at or before it. Offsets
/// must be asked for in increasing order.
fn covering<'a>(map: &'a [Extent], next: &mut usize, offset: u64) -> Option<&'a Extent> {
    while *next < map.len() && map[*next].end() <= offset {
        *next += 1;
    }
    map.get(*next).filter(|extent| extent.logical <= offset)
}

/// Whether the elementary range starting at `offset` may differ. No extent
/// begins or ends inside the range, so what holds at `offset` holds for all
/// of it.
fn differs(newer: Option<&Extent>, base: Option<&Extent>, offset: u64) -> bool {
    match (newer, base) {
        (None, None) => false,
        (Some(_), None) | (None, Some(_)) => true,
        (Some(newer), Some(base)) => {
            if newer.incomparable || base.incomparable || newer.unwritten != base.unwritten {
                return true;
            }
            match (physical_at(newer, offset), physical_at(base, offset)) {
                (Some(left), Some(right)) => left != right,
                _ => true,
            }
        }
    }
}

/// The physical address of `offset` inside `extent`; `None` on overflow.
fn physical_at(extent: &Extent, offset: u64) -> Option<u64> {
    extent.physical.checked_add(offset - extent.logical)
}

#[cfg(test)]
mod tests {
    use super::{
        BLOCK, Extent, FIEMAP_EXTENT_DATA_INLINE, FIEMAP_EXTENT_DATA_TAIL, FIEMAP_EXTENT_DELALLOC,
        FIEMAP_EXTENT_ENCODED, FIEMAP_EXTENT_NOT_ALIGNED, FIEMAP_EXTENT_UNKNOWN,
        FIEMAP_EXTENT_UNWRITTEN, changed_ranges, round_and_merge,
    };
    use crate::ByteRange;

    const MIB: u64 = 1 << 20;

    fn extent(logical: u64, physical: u64, length: u64) -> Extent {
        Extent::from_fiemap(logical, physical, length, 0)
    }

    fn range(offset: u64, length: u64) -> ByteRange {
        ByteRange { offset, length }
    }

    #[test]
    fn identical_maps_have_no_changed_range() {
        let map = [
            extent(0, 100 * MIB, MIB),
            extent(4 * MIB, 300 * MIB, 2 * MIB),
        ];
        assert_eq!(changed_ranges(&map, &map, 8 * MIB), Some(Vec::new()));
    }

    #[test]
    fn two_empty_files_have_no_changed_range() {
        assert_eq!(changed_ranges(&[], &[], 8 * MIB), Some(Vec::new()));
    }

    #[test]
    fn a_range_mapped_in_one_file_only_changed() {
        let newer = [extent(0, 100 * MIB, MIB)];
        assert_eq!(
            changed_ranges(&newer, &[], 8 * MIB),
            Some(vec![range(0, MIB)]),
            "data written into a hole"
        );
        assert_eq!(
            changed_ranges(&[], &newer, 8 * MIB),
            Some(vec![range(0, MIB)]),
            "a hole punched where the base had data reads as zeros now"
        );
    }

    #[test]
    fn a_rewritten_block_inside_a_shared_extent_is_the_only_change() {
        // The base maps 1 MiB contiguously; the newer file shares it except
        // for one 4 KiB block that copy-on-write moved elsewhere.
        let base = [extent(0, 100 * MIB, MIB)];
        let newer = [
            extent(0, 100 * MIB, 64 * 1024),
            extent(64 * 1024, 900 * MIB, BLOCK),
            extent(
                64 * 1024 + BLOCK,
                100 * MIB + 64 * 1024 + BLOCK,
                MIB - 64 * 1024 - BLOCK,
            ),
        ];
        assert_eq!(
            changed_ranges(&newer, &base, MIB),
            Some(vec![range(64 * 1024, BLOCK)])
        );
    }

    #[test]
    fn the_same_physical_block_at_another_offset_changed() {
        // Both files map 8 KiB starting at the same physical address, but the
        // newer file's extent starts one block later in the file.
        let base = [extent(0, 100 * MIB, 2 * BLOCK)];
        let newer = [extent(BLOCK, 100 * MIB, 2 * BLOCK)];
        assert_eq!(
            changed_ranges(&newer, &base, MIB),
            Some(vec![range(0, 3 * BLOCK)])
        );
    }

    #[test]
    fn a_flagged_extent_counts_as_changed_on_either_side() {
        for flag in [
            FIEMAP_EXTENT_UNKNOWN,
            FIEMAP_EXTENT_DELALLOC,
            FIEMAP_EXTENT_ENCODED,
            FIEMAP_EXTENT_NOT_ALIGNED,
            FIEMAP_EXTENT_DATA_INLINE,
            FIEMAP_EXTENT_DATA_TAIL,
        ] {
            let plain = [extent(0, 100 * MIB, MIB)];
            let flagged = [Extent::from_fiemap(0, 100 * MIB, MIB, flag)];
            assert_eq!(
                changed_ranges(&flagged, &plain, MIB),
                Some(vec![range(0, MIB)]),
                "flag {flag:#x} on the newer side"
            );
            assert_eq!(
                changed_ranges(&plain, &flagged, MIB),
                Some(vec![range(0, MIB)]),
                "flag {flag:#x} on the base side"
            );
        }
    }

    #[test]
    fn a_physical_address_of_zero_is_not_compared() {
        let map = [extent(0, 0, MIB)];
        assert_eq!(changed_ranges(&map, &map, MIB), Some(vec![range(0, MIB)]));
    }

    #[test]
    fn an_unwritten_extent_differs_from_a_written_one() {
        let written = [extent(0, 100 * MIB, MIB)];
        let unwritten = [Extent::from_fiemap(
            0,
            100 * MIB,
            MIB,
            FIEMAP_EXTENT_UNWRITTEN,
        )];
        assert_eq!(
            changed_ranges(&unwritten, &written, MIB),
            Some(vec![range(0, MIB)])
        );
        assert_eq!(
            changed_ranges(&unwritten, &unwritten, MIB),
            Some(Vec::new())
        );
    }

    #[test]
    fn extents_past_the_end_of_the_file_are_ignored() {
        // Speculative preallocation past the end of the file in the newer map.
        let base = [extent(0, 100 * MIB, MIB)];
        let newer = [extent(0, 100 * MIB, MIB), extent(MIB, 500 * MIB, 16 * MIB)];
        assert_eq!(changed_ranges(&newer, &base, MIB), Some(Vec::new()));
        let longer = [extent(0, 100 * MIB, 2 * MIB)];
        assert_eq!(changed_ranges(&longer, &base, MIB), Some(Vec::new()));
    }

    #[test]
    fn overlapping_extents_make_a_map_unusable() {
        let broken = [
            extent(0, 100 * MIB, 2 * BLOCK),
            extent(BLOCK, 200 * MIB, BLOCK),
        ];
        assert_eq!(changed_ranges(&broken, &[], MIB), None);
        assert_eq!(changed_ranges(&[], &broken, MIB), None);
    }

    #[test]
    fn unaligned_changes_are_rounded_out_to_whole_blocks() {
        let base = [extent(0, 100 * MIB, MIB)];
        let newer = [
            extent(0, 100 * MIB, 5000),
            extent(5000, 700 * MIB, 100),
            extent(5100, 100 * MIB + 5100, MIB - 5100),
        ];
        assert_eq!(
            changed_ranges(&newer, &base, MIB),
            Some(vec![range(BLOCK, BLOCK)])
        );
    }

    #[test]
    fn rounding_merges_ranges_that_touch_and_cuts_at_the_size() {
        let merged = round_and_merge(
            vec![
                range(3 * BLOCK + 10, 5),
                range(0, 1),
                range(BLOCK, BLOCK),
                range(2 * BLOCK, 1),
                range(10 * BLOCK, 4 * BLOCK),
                range(9 * BLOCK, 0),
            ],
            12 * BLOCK,
        );
        assert_eq!(
            merged,
            vec![range(0, 4 * BLOCK), range(10 * BLOCK, 2 * BLOCK)]
        );
    }

    /// A small deterministic generator (xorshift64*), so the property test
    /// needs no dependency and every failure names a seed that replays it.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.below(100) < percent
        }
    }

    /// What one 4 KiB block of a simulated file holds.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Slot {
        Hole,
        /// A physical block number; its bytes are a function of the number.
        Data(u64),
        /// Allocated but unwritten at this physical block: reads as zeros.
        Unwritten(u64),
    }

    /// A simulated reflink filesystem: files are maps from block to slot,
    /// a clone copies the map (sharing every physical block), and a write
    /// moves the written blocks to fresh physical blocks (copy on write).
    struct Fs {
        next_physical: u64,
    }

    impl Fs {
        fn allocate(&mut self, count: u64) -> u64 {
            let first = self.next_physical;
            self.next_physical += count;
            first
        }
    }

    /// The extent map FIEMAP would report for `file`: runs of blocks with
    /// consecutive physical numbers of one kind, sometimes split in two the
    /// way a filesystem may report adjacent extents separately, and with an
    /// injected flagged extent now and then.
    fn extent_map(file: &[Slot], rng: &mut Rng) -> Vec<Extent> {
        let mut map: Vec<Extent> = Vec::new();
        let mut run: Option<(u64, Slot, u64)> = None;
        let flush = |map: &mut Vec<Extent>, run: Option<(u64, Slot, u64)>, rng: &mut Rng| {
            let Some((start, slot, count)) = run else {
                return;
            };
            let (physical, flags) = match slot {
                Slot::Hole => return,
                Slot::Data(physical) => (physical, 0),
                Slot::Unwritten(physical) => (physical, FIEMAP_EXTENT_UNWRITTEN),
            };
            let flags = if rng.chance(2) {
                flags | FIEMAP_EXTENT_DELALLOC
            } else {
                flags
            };
            // Physical numbers start at 1, so no extent sits at address 0.
            if count > 1 && rng.chance(20) {
                let split = 1 + rng.below(count - 1);
                map.push(Extent::from_fiemap(
                    start * BLOCK,
                    physical * BLOCK,
                    split * BLOCK,
                    flags,
                ));
                map.push(Extent::from_fiemap(
                    (start + split) * BLOCK,
                    (physical + split) * BLOCK,
                    (count - split) * BLOCK,
                    flags,
                ));
            } else {
                map.push(Extent::from_fiemap(
                    start * BLOCK,
                    physical * BLOCK,
                    count * BLOCK,
                    flags,
                ));
            }
        };
        for (index, slot) in file.iter().enumerate() {
            let index = index as u64;
            let continues = match (run, slot) {
                (Some((start, Slot::Data(first), count)), Slot::Data(physical)) => {
                    start + count == index && first + count == *physical
                }
                (Some((start, Slot::Unwritten(first), count)), Slot::Unwritten(physical)) => {
                    start + count == index && first + count == *physical
                }
                _ => false,
            };
            if continues {
                if let Some((_, _, count)) = run.as_mut() {
                    *count += 1;
                }
            } else {
                flush(&mut map, run, rng);
                run = Some((index, *slot, 1));
            }
        }
        flush(&mut map, run, rng);
        // FIEMAP returns extents in order; shuffle a little anyway, since the
        // comparison must not depend on it.
        if map.len() > 1 && rng.chance(30) {
            let a = rng.below(map.len() as u64) as usize;
            let b = rng.below(map.len() as u64) as usize;
            map.swap(a, b);
        }
        map
    }

    /// What a block reads as: zeros for a hole or an unwritten block, the
    /// physical number otherwise.
    fn content(slot: Slot) -> u64 {
        match slot {
            Slot::Hole | Slot::Unwritten(_) => 0,
            Slot::Data(physical) => physical,
        }
    }

    /// One random cycle of writes to the live file: single blocks, runs, a
    /// punched hole, an unwritten preallocation, or nothing at all.
    fn write_cycle(live: &mut [Slot], fs: &mut Fs, rng: &mut Rng) {
        let blocks = live.len() as u64;
        let operations = rng.below(12);
        for _ in 0..operations {
            let start = rng.below(blocks);
            let count = (1 + rng.below(16)).min(blocks - start);
            match rng.below(10) {
                0 => {
                    for slot in &mut live[start as usize..(start + count) as usize] {
                        *slot = Slot::Hole;
                    }
                }
                1 => {
                    let first = fs.allocate(count);
                    for (offset, slot) in live[start as usize..(start + count) as usize]
                        .iter_mut()
                        .enumerate()
                    {
                        *slot = Slot::Unwritten(first + offset as u64);
                    }
                }
                _ => {
                    // Copy on write: the written blocks get new physical
                    // blocks, contiguous for one write.
                    let first = fs.allocate(count);
                    for (offset, slot) in live[start as usize..(start + count) as usize]
                        .iter_mut()
                        .enumerate()
                    {
                        *slot = Slot::Data(first + offset as u64);
                    }
                }
            }
        }
    }

    /// Ships N cycles the way a warm source does: snapshot, compare with the
    /// base, apply the changed ranges of the snapshot to the target's copy.
    /// After every cycle the target must read exactly like the snapshot, and
    /// the comparison must report every block whose mapping changed and no
    /// block whose mapping is the same and unflagged.
    #[test]
    fn applying_the_changed_ranges_of_every_cycle_keeps_the_copy_identical() {
        const BLOCKS: u64 = 512;
        const CYCLES: usize = 40;
        for seed in 1..=150_u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let mut fs = Fs { next_physical: 1 };
            let mut live = vec![Slot::Hole; BLOCKS as usize];
            // Fill part of the file before the first snapshot.
            let filled = rng.below(BLOCKS);
            let first = fs.allocate(filled);
            for (index, slot) in live.iter_mut().take(filled as usize).enumerate() {
                *slot = Slot::Data(first + index as u64);
            }

            // The first copy is full: the target starts from the snapshot's
            // content.
            let mut base = live.clone();
            let mut target: Vec<u64> = base.iter().copied().map(content).collect();
            let size = BLOCKS * BLOCK;

            for cycle in 0..CYCLES {
                write_cycle(&mut live, &mut fs, &mut rng);
                let next = live.clone();
                let newer_map = extent_map(&next, &mut rng);
                let base_map = extent_map(&base, &mut rng);
                let ranges = changed_ranges(&newer_map, &base_map, size)
                    .unwrap_or_else(|| panic!("seed {seed} cycle {cycle}: maps unusable"));

                // Every range is aligned, inside the file, sorted and apart.
                let mut previous_end = 0;
                for range in &ranges {
                    assert_eq!(range.offset % BLOCK, 0, "seed {seed} cycle {cycle}");
                    assert_eq!(range.length % BLOCK, 0, "seed {seed} cycle {cycle}");
                    assert!(range.length > 0, "seed {seed} cycle {cycle}");
                    assert!(
                        range.offset + range.length <= size,
                        "seed {seed} cycle {cycle}"
                    );
                    assert!(
                        ranges.first() == Some(range) || range.offset > previous_end,
                        "seed {seed} cycle {cycle}: ranges overlap or touch"
                    );
                    previous_end = range.offset + range.length;
                }

                let reported = |block: u64| {
                    let offset = block * BLOCK;
                    ranges
                        .iter()
                        .any(|range| range.offset <= offset && offset < range.offset + range.length)
                };
                let flagged = |map: &[Extent], block: u64| {
                    let offset = block * BLOCK;
                    map.iter().any(|extent| {
                        extent.incomparable
                            && extent.logical <= offset
                            && offset < extent.logical + extent.length
                    })
                };
                for block in 0..BLOCKS {
                    let mapping_changed = next[block as usize] != base[block as usize];
                    if mapping_changed {
                        assert!(
                            reported(block),
                            "seed {seed} cycle {cycle}: block {block} changed and was missed"
                        );
                    } else if !flagged(&newer_map, block) && !flagged(&base_map, block) {
                        assert!(
                            !reported(block),
                            "seed {seed} cycle {cycle}: block {block} is shared and was reported"
                        );
                    }
                }

                // Apply the snapshot's bytes in the reported ranges.
                for range in &ranges {
                    for block in range.offset / BLOCK..(range.offset + range.length) / BLOCK {
                        target[block as usize] = content(next[block as usize]);
                    }
                }
                let expected: Vec<u64> = next.iter().copied().map(content).collect();
                assert_eq!(
                    target, expected,
                    "seed {seed} cycle {cycle}: the copy differs from the snapshot"
                );
                base = next;
            }
        }
    }
}

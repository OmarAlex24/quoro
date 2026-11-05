// SPDX-License-Identifier: AGPL-3.0-only
//! A minimal, read-only, bounded SQLite file reader: enough to list the rows
//! of one table of an `rpm` database. It never links SQLite (ADR 0151).
//!
//! The file is untrusted. Every page is read on demand through
//! [`read_range`], never the whole file; page numbers, cell offsets, payload
//! sizes and record lengths are all checked against the file before use. A
//! page visited twice, an overflow chain that loops, a payload larger than
//! the budget or a record that lies about its length ends that row or subtree
//! and nothing else: the reader has no error path and cannot spin.
//!
//! Layout follows <https://www.sqlite.org/fileformat2.html>. Freeblocks,
//! WAL files, indexes and virtual tables are not read.

use std::collections::HashSet;

use super::{TreeFile, read_range};

/// The file header's magic string.
const MAGIC: &[u8; 16] = b"SQLite format 3\0";
/// The header is 100 bytes.
const HEADER_LEN: u64 = 100;
/// Page 1's b-tree header starts after the 100-byte file header.
const PAGE1_BTREE_OFFSET: usize = 100;
const MIN_PAGE_SIZE: u32 = 512;
const MAX_PAGE_SIZE: u32 = 65_536;
/// Smallest usable page size (header, b-tree header and pointer array fit).
const MIN_USABLE: u32 = 480;
/// Most pages read over one traversal, whatever the file claims.
const MAX_PAGES: u32 = 200_000;
/// Deepest b-tree path followed.
const MAX_DEPTH: u32 = 64;
/// Largest payload one row may carry; over it the row is dropped.
const MAX_PAYLOAD: u64 = 64 << 20;
/// Bound on pending children, so a hostile interior page cannot grow the
/// stack without limit.
const MAX_STACK: usize = 1 << 18;
/// Bound on the columns of one record.
const MAX_COLUMNS: usize = 4_096;

/// One SQLite value as stored in a record.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

/// An open SQLite database file.
pub struct Db<'a> {
    file: TreeFile<'a>,
    page_size: u32,
    usable: u32,
    page_count: u32,
    pages_read: u32,
}

impl<'a> Db<'a> {
    /// Reads the file header and checks it. `None` when the file is not a
    /// SQLite 3 database or its header is out of bounds.
    pub fn open(mut file: TreeFile<'a>) -> Option<Db<'a>> {
        let header = read_range(&mut file, 0, HEADER_LEN, usize::try_from(HEADER_LEN).ok()?)?;
        if header.get(..MAGIC.len())? != MAGIC {
            return None;
        }
        let raw_page_size = read_u16(&header, 16)?;
        // 1 is the encoding of 65536, which does not fit in a u16.
        let page_size = if raw_page_size == 1 {
            MAX_PAGE_SIZE
        } else {
            u32::from(raw_page_size)
        };
        if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) || !page_size.is_power_of_two() {
            return None;
        }
        let reserved = u32::from(*header.get(20)?);
        let usable = page_size.checked_sub(reserved)?;
        if usable < MIN_USABLE {
            return None;
        }
        let physical = file.len / u64::from(page_size);
        if physical == 0 {
            return None;
        }
        let declared = u64::from(read_u32(&header, 28)?);
        // A missing or stale in-header size is replaced by the physical size.
        let count = if declared == 0 || declared != physical {
            physical
        } else {
            declared
        };
        let count = count.min(physical).min(u64::from(u32::MAX));
        let page_count = u32::try_from(count).ok()?;
        if page_count == 0 {
            return None;
        }
        Some(Db {
            file,
            page_size,
            usable,
            page_count,
            pages_read: 0,
        })
    }

    /// Finds the root page of the table `name` by reading `sqlite_schema` on
    /// page 1. `None` when there is no such table.
    pub fn table_root(&mut self, name: &str) -> Option<u32> {
        let wanted = name.as_bytes();
        let mut root = None;
        self.for_each_row(1, |values| {
            let (Some(Value::Text(kind)), Some(Value::Text(table))) =
                (values.first(), values.get(1))
            else {
                return true;
            };
            if kind.as_slice() != b"table" || table.as_slice() != wanted {
                return true;
            }
            match values.get(3) {
                Some(Value::Int(page)) => match u32::try_from(*page) {
                    Ok(page) if page != 0 => {
                        root = Some(page);
                        false
                    }
                    _ => true,
                },
                _ => true,
            }
        });
        root
    }

    /// Calls `f` with the values of each row of the table b-tree at `root`,
    /// in key order, until `f` returns false.
    ///
    /// An `INTEGER PRIMARY KEY` column is stored as NULL in the record (the
    /// value lives in the rowid); records are handed over as stored.
    pub fn for_each_row(&mut self, root: u32, mut f: impl FnMut(&[Value]) -> bool) {
        let mut visited: HashSet<u32> = HashSet::new();
        let mut stack: Vec<(u32, u32)> = vec![(root, 0)];
        while let Some((page_no, depth)) = stack.pop() {
            if depth > MAX_DEPTH || !visited.insert(page_no) {
                continue;
            }
            let Some(page) = self.page(page_no) else {
                continue;
            };
            let header = if page_no == 1 { PAGE1_BTREE_OFFSET } else { 0 };
            let Some(page_type) = page.get(header).copied() else {
                continue;
            };
            let Some(cell_count) = read_u16(&page, header.saturating_add(3)) else {
                continue;
            };
            // A leaf page's cell pointer array starts right after the eight
            // header bytes; an interior page's after the right-most pointer.
            let pointer_start = header.saturating_add(if page_type == 0x05 { 12 } else { 8 });
            let pointer_end = usize::from(cell_count)
                .checked_mul(2)
                .and_then(|len| pointer_start.checked_add(len));
            // The pointer array must fit inside the usable part of the page.
            match pointer_end {
                Some(end) if end <= usize::try_from(self.usable).unwrap_or(0) => {}
                _ => continue,
            }
            match page_type {
                // Table leaf.
                0x0d => {
                    for index in 0..usize::from(cell_count) {
                        let Some(offset) = read_u16(&page, pointer_start.saturating_add(index * 2))
                        else {
                            continue;
                        };
                        let offset = usize::from(offset);
                        if offset < pointer_start || offset >= page.len() {
                            continue;
                        }
                        let Some(values) = self.leaf_values(&page, offset) else {
                            continue;
                        };
                        if !f(&values) {
                            return;
                        }
                    }
                }
                // Table interior: every cell holds a left child, the header
                // holds the right-most child.
                0x05 => {
                    let right_most = read_u32(&page, header.saturating_add(8)).unwrap_or(0);
                    let mut children: Vec<u32> = Vec::new();
                    for index in 0..usize::from(cell_count) {
                        let Some(offset) = read_u16(&page, pointer_start.saturating_add(index * 2))
                        else {
                            continue;
                        };
                        match read_u32(&page, usize::from(offset)) {
                            Some(child) if child != 0 => children.push(child),
                            _ => continue,
                        }
                    }
                    if right_most != 0 {
                        children.push(right_most);
                    }
                    // The stack hands children back left to right.
                    for child in children.into_iter().rev() {
                        if stack.len() >= MAX_STACK {
                            break;
                        }
                        match depth.checked_add(1) {
                            Some(next) if next <= MAX_DEPTH => stack.push((child, next)),
                            _ => break,
                        }
                    }
                }
                // Index pages and anything else belong to no table.
                _ => {}
            }
        }
    }

    /// Reads one page, counting against the traversal budget.
    fn page(&mut self, page_no: u32) -> Option<Vec<u8>> {
        if page_no == 0
            || page_no > self.page_count
            || self.pages_read >= MAX_PAGES.min(self.page_count)
        {
            return None;
        }
        self.pages_read = self.pages_read.saturating_add(1);
        let offset = u64::from(page_no.checked_sub(1)?).checked_mul(u64::from(self.page_size))?;
        let len = u64::from(self.page_size);
        read_range(
            &mut self.file,
            offset,
            len,
            usize::try_from(self.page_size).ok()?,
        )
    }

    /// Reads the payload of the table-leaf cell at `offset` and decodes its
    /// record. `None` when the cell, its overflow chain or its record is
    /// malformed; nothing is partially returned.
    fn leaf_values(&mut self, page: &[u8], offset: usize) -> Option<Vec<Value>> {
        let mut at = offset;
        let payload_len = take_varint(page, &mut at)?;
        let _rowid = take_varint(page, &mut at)?;
        if payload_len > MAX_PAYLOAD {
            return None;
        }
        let usable = u64::from(self.usable);
        // Local payload size, per the file format's table b-tree rules.
        let max_local = usable.checked_sub(35)?;
        let local = if payload_len <= max_local {
            payload_len
        } else {
            let min_local = usable
                .checked_sub(12)?
                .checked_mul(32)?
                .checked_div(255)?
                .checked_sub(23)?;
            let candidate = payload_len
                .checked_sub(min_local)?
                .checked_rem(usable.checked_sub(4)?)?
                .checked_add(min_local)?;
            if candidate <= max_local {
                candidate
            } else {
                min_local
            }
        };
        let local_len = usize::try_from(local).ok()?;
        let local_end = at.checked_add(local_len)?;
        let mut payload = page.get(at..local_end)?.to_vec();
        if payload_len > local {
            let mut next = read_u32(page, local_end)?;
            let mut remaining = payload_len.checked_sub(local)?;
            let mut chain: HashSet<u32> = HashSet::new();
            // Each overflow page holds a 4-byte next pointer then page data.
            let chunk = usize::try_from(self.usable.checked_sub(4)?).ok()?;
            while remaining > 0 {
                if next == 0 || !chain.insert(next) {
                    return None;
                }
                let overflow = self.page(next)?;
                let take = usize::try_from(remaining.min(u64::try_from(chunk).ok()?)).ok()?;
                payload.extend_from_slice(overflow.get(4..4usize.checked_add(take)?)?);
                remaining = remaining.checked_sub(u64::try_from(take).ok()?)?;
                next = read_u32(&overflow, 0)?;
            }
        }
        decode_record(&payload)
    }
}

/// Decodes one record (the header of serial types, then the values).
fn decode_record(payload: &[u8]) -> Option<Vec<Value>> {
    let mut at = 0usize;
    let header_len = take_varint(payload, &mut at)?;
    let header_end = usize::try_from(header_len).ok()?;
    if header_len < 1 || header_end > payload.len() || header_end < at {
        return None;
    }
    let mut serials: Vec<u64> = Vec::new();
    while at < header_end {
        if serials.len() >= MAX_COLUMNS {
            return None;
        }
        serials.push(take_varint(payload, &mut at)?);
    }
    if at != header_end {
        return None;
    }
    let mut body = header_end;
    let mut values = Vec::with_capacity(serials.len());
    for serial in serials {
        match serial {
            0 => values.push(Value::Null),
            // Integers of 1, 2, 3, 4, 6 and 8 bytes, big-endian, signed.
            1..=6 => {
                let size = match serial {
                    1 => 1usize,
                    2 => 2,
                    3 => 3,
                    4 => 4,
                    5 => 6,
                    _ => 8,
                };
                let end = body.checked_add(size)?;
                values.push(Value::Int(signed_be(payload.get(body..end)?)?));
                body = end;
            }
            7 => {
                let end = body.checked_add(8)?;
                let bytes: [u8; 8] = payload.get(body..end)?.try_into().ok()?;
                values.push(Value::Real(f64::from_be_bytes(bytes)));
                body = end;
            }
            // Constants 0 and 1, no bytes in the body.
            8 => values.push(Value::Int(0)),
            9 => values.push(Value::Int(1)),
            // Serial types 10 and 11 are reserved.
            10 | 11 => return None,
            _ => {
                let (size, is_blob) = if serial % 2 == 0 {
                    (serial.checked_sub(12)?.checked_div(2)?, true)
                } else {
                    (serial.checked_sub(13)?.checked_div(2)?, false)
                };
                let end = body.checked_add(usize::try_from(size).ok()?)?;
                let bytes = payload.get(body..end)?.to_vec();
                values.push(if is_blob {
                    Value::Blob(bytes)
                } else {
                    Value::Text(bytes)
                });
                body = end;
            }
        }
    }
    Some(values)
}

/// Decodes a big-endian signed integer of 1, 2, 3, 4, 6 or 8 bytes.
fn signed_be(bytes: &[u8]) -> Option<i64> {
    match bytes.len() {
        1 => Some(i64::from(i8::from_be_bytes(bytes.try_into().ok()?))),
        2 => Some(i64::from(i16::from_be_bytes(bytes.try_into().ok()?))),
        3 => {
            let mut wide = [0u8; 4];
            wide.get_mut(1..)?.copy_from_slice(bytes);
            let value = i32::from_be_bytes(wide);
            Some(i64::from(if value > 0x7f_ffff {
                value.checked_sub(0x100_0000)?
            } else {
                value
            }))
        }
        4 => Some(i64::from(i32::from_be_bytes(bytes.try_into().ok()?))),
        6 => {
            let mut wide = [0u8; 8];
            wide.get_mut(2..)?.copy_from_slice(bytes);
            let value = i64::from_be_bytes(wide);
            Some(if value > 0x7fff_ffff_ffff {
                value.checked_sub(0x1_0000_0000_0000)?
            } else {
                value
            })
        }
        8 => Some(i64::from_be_bytes(bytes.try_into().ok()?)),
        _ => None,
    }
}

/// Reads a SQLite varint of at most 9 bytes; the ninth byte contributes all
/// eight of its bits. `None` when the buffer ends first.
fn take_varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for index in 0..9u32 {
        let byte = *bytes.get(*at)?;
        *at = at.checked_add(1)?;
        if index == 8 {
            return Some((value << 8) | u64::from(byte));
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn read_u16(bytes: &[u8], at: usize) -> Option<u16> {
    let end = at.checked_add(2)?;
    let slice = bytes.get(at..end)?;
    Some(u16::from_be_bytes(slice.try_into().ok()?))
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let slice = bytes.get(at..end)?;
    Some(u32::from_be_bytes(slice.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::super::Tree;
    use super::super::testing::MemTree;
    use super::*;

    /// The fixture generated by `testdata/generate.py`: 301 rows, page size
    /// 1024, one blob over 4 KiB (overflow chain).
    const DB_1024: &[u8] = include_bytes!("testdata/rpmdb-1024.sqlite");
    /// 60 rows, page size 4096, blobs over 4 KiB.
    const DB_4096: &[u8] = include_bytes!("testdata/rpmdb-4096.sqlite");

    fn open<'t>(tree: &'t MemTree, path: &str) -> Option<Db<'t>> {
        Db::open(tree.open(path)?)
    }

    fn count_rows(tree: &MemTree, path: &str) -> (usize, usize) {
        let mut db = open(tree, path).expect("fixture opens");
        let root = db.table_root("Packages").expect("Packages table");
        let mut rows = 0usize;
        let mut largest = 0usize;
        db.for_each_row(root, |values| {
            rows += 1;
            for value in values {
                if let Value::Blob(blob) = value {
                    largest = largest.max(blob.len());
                }
            }
            true
        });
        (rows, largest)
    }

    #[test]
    fn reads_a_page_1024_fixture_with_overflow_and_interior_pages() {
        let tree = MemTree::new().file("db", DB_1024.to_vec());
        let (rows, largest) = count_rows(&tree, "db");
        assert_eq!(rows, 301);
        assert!(largest > 4096, "the fixture must use overflow pages");
        assert!(
            DB_1024.len() / 1024 > 8,
            "the fixture must need interior pages"
        );
    }

    #[test]
    fn reads_a_page_4096_fixture() {
        let tree = MemTree::new().file("db", DB_4096.to_vec());
        let (rows, largest) = count_rows(&tree, "db");
        assert_eq!(rows, 60);
        assert!(largest > 4096);
    }

    #[test]
    fn a_missing_table_has_no_root() {
        let tree = MemTree::new().file("db", DB_4096.to_vec());
        let mut db = open(&tree, "db").expect("fixture opens");
        assert_eq!(db.table_root("Nope"), None);
    }

    #[test]
    fn rejects_a_file_without_the_magic() {
        let tree = MemTree::new().file("db", [0u8; 512].to_vec());
        assert!(open(&tree, "db").is_none());
        let tree = MemTree::new().file("db", b"SQLite format 3\0".to_vec());
        assert!(open(&tree, "db").is_none());
        let tree = MemTree::new().file("db", Vec::new());
        assert!(open(&tree, "db").is_none());
    }

    #[test]
    fn rejects_a_page_size_that_is_not_a_power_of_two() {
        let mut file = build_file(512, 1, vec![btree_page(512, true, 0x0d, &[], 0)]);
        file.get_mut(16..18)
            .expect("page size")
            .copy_from_slice(&3000u16.to_be_bytes());
        let tree = MemTree::new().file("db", file);
        assert!(open(&tree, "db").is_none());

        let mut file = build_file(512, 1, vec![btree_page(512, true, 0x0d, &[], 0)]);
        file.get_mut(16..18)
            .expect("page size")
            .copy_from_slice(&256u16.to_be_bytes());
        let tree = MemTree::new().file("db", file);
        assert!(open(&tree, "db").is_none());
    }

    #[test]
    fn an_interior_page_pointing_at_itself_terminates() {
        let cell = interior_cell(1);
        let file = build_file(
            512,
            2,
            vec![
                btree_page(512, true, 0x05, &[cell], 1),
                btree_page(512, false, 0x0d, &[], 0),
            ],
        );
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn two_interior_pages_pointing_at_each_other_terminate() {
        let file = build_file(
            512,
            2,
            vec![
                btree_page(512, true, 0x05, &[interior_cell(2)], 0),
                btree_page(512, false, 0x05, &[interior_cell(1)], 0),
            ],
        );
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn an_overflow_chain_that_loops_terminates() {
        // Page 512, usable 512: a 600-byte payload keeps 92 bytes local
        // (M = 39, K = 92); the rest comes from page 2, which points at
        // itself.
        let mut cell = put_varint(600);
        cell.extend_from_slice(&put_varint(1));
        cell.extend_from_slice(&[0xab; 92]);
        cell.extend_from_slice(&2u32.to_be_bytes());
        let mut overflow = vec![0u8; 512];
        overflow
            .get_mut(0..4)
            .expect("pointer")
            .copy_from_slice(&2u32.to_be_bytes());
        let file = build_file(
            512,
            2,
            vec![btree_page(512, true, 0x0d, &[cell], 0), overflow],
        );
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn a_payload_of_two_to_the_sixty_two_is_dropped() {
        let mut cell = put_varint(1 << 62);
        cell.extend_from_slice(&put_varint(1));
        let file = build_file(512, 1, vec![btree_page(512, true, 0x0d, &[cell], 0)]);
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn a_record_header_lying_about_its_size_is_dropped() {
        let mut payload = put_varint(50);
        payload.extend_from_slice(b"abc");
        let cell = leaf_cell(&payload, 1);
        let file = build_file(512, 1, vec![btree_page(512, true, 0x0d, &[cell], 0)]);
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn a_record_whose_columns_run_past_the_payload_is_dropped() {
        // One TEXT column of 100 bytes, no body.
        let payload = record(&[13 + 200], &[]);
        let cell = leaf_cell(&payload, 1);
        let file = build_file(512, 1, vec![btree_page(512, true, 0x0d, &[cell], 0)]);
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn a_cell_pointer_outside_the_page_is_skipped() {
        let mut page = btree_page(512, true, 0x0d, &[], 0);
        // One cell, pointing past the end of the page.
        page.get_mut(103..105)
            .expect("cell count")
            .copy_from_slice(&1u16.to_be_bytes());
        page.get_mut(108..110)
            .expect("pointer")
            .copy_from_slice(&60_000u16.to_be_bytes());
        let file = build_file(512, 1, vec![page]);
        let tree = MemTree::new().file("db", file);
        let mut db = open(&tree, "db").expect("opens");
        let mut rows = 0usize;
        db.for_each_row(1, |_| {
            rows += 1;
            true
        });
        assert_eq!(rows, 0);
    }

    #[test]
    fn decodes_every_serial_type() {
        let body = [
            0x01, // 1-byte int 1
            0xff, 0xfe, // 2-byte int -2
            0xff, 0xff, 0xfd, // 3-byte int -3
            0xff, 0xff, 0xff, 0xfc, // 4-byte int -4
            0xff, 0xff, 0xff, 0xff, 0xff, 0xfb, // 6-byte int -5
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfa, // 8-byte int -6
            0x3f, 0xf0, 0, 0, 0, 0, 0, 0, // 1.0
            b'h', b'i', // TEXT
            0x00, 0x01, // BLOB
        ];
        let serials = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13 + 4, 12 + 4];
        let payload = record(&serials, &body);
        let values = decode_record(&payload).expect("record decodes");
        assert_eq!(values[0], Value::Null);
        assert_eq!(values[1], Value::Int(1));
        assert_eq!(values[2], Value::Int(-2));
        assert_eq!(values[3], Value::Int(-3));
        assert_eq!(values[4], Value::Int(-4));
        assert_eq!(values[5], Value::Int(-5));
        assert_eq!(values[6], Value::Int(-6));
        assert_eq!(values[7], Value::Real(1.0));
        assert_eq!(values[8], Value::Int(0));
        assert_eq!(values[9], Value::Int(1));
        assert_eq!(values[10], Value::Text(b"hi".to_vec()));
        assert_eq!(values[11], Value::Blob(vec![0x00, 0x01]));
    }

    #[test]
    fn reserved_serial_types_are_rejected() {
        for serial in [10u64, 11] {
            let payload = record(&[serial], &[]);
            assert_eq!(decode_record(&payload), None);
        }
    }

    #[test]
    fn a_nine_byte_varint_keeps_all_sixty_four_bits() {
        let bytes = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        let mut at = 0usize;
        assert_eq!(take_varint(&bytes, &mut at), Some(u64::MAX));
        assert_eq!(at, 9);
        let mut at = 0usize;
        assert_eq!(take_varint(&[0x80], &mut at), None);
        let mut at = 0usize;
        assert_eq!(take_varint(&[0x01], &mut at), Some(1));
    }

    #[test]
    fn the_page_count_is_capped_at_the_file_length() {
        let file = build_file(512, 1, vec![btree_page(512, true, 0x0d, &[], 0)]);
        let mut lying = file.clone();
        lying
            .get_mut(28..32)
            .expect("page count")
            .copy_from_slice(&1_000u32.to_be_bytes());
        let tree = MemTree::new().file("db", lying);
        let db = open(&tree, "db").expect("opens");
        assert_eq!(db.page_count, 1);
    }

    // Helpers: pages are built by hand so the tests can lie where a real
    // database never would.

    fn build_file(page_size: u16, page_count: u32, pages: Vec<Vec<u8>>) -> Vec<u8> {
        let mut file = Vec::new();
        for mut page in pages {
            page.resize(usize::from(page_size), 0);
            file.extend_from_slice(&page);
        }
        file.resize(
            usize::from(page_size) * usize::try_from(page_count).unwrap_or(1),
            0,
        );
        file.get_mut(0..16)
            .expect("magic")
            .copy_from_slice(b"SQLite format 3\0");
        file.get_mut(16..18)
            .expect("page size")
            .copy_from_slice(&page_size.to_be_bytes());
        file.get_mut(18..20)
            .expect("versions")
            .copy_from_slice(&[1, 1]);
        *file.get_mut(20).expect("reserved") = 0;
        *file.get_mut(21).expect("payload fractions") = 64;
        file.get_mut(22..24)
            .expect("payload fractions")
            .copy_from_slice(&[32, 32]);
        file.get_mut(28..32)
            .expect("page count")
            .copy_from_slice(&page_count.to_be_bytes());
        file
    }

    /// One b-tree page: header, cell pointer array, then cells packed from
    /// the end of the page.
    fn btree_page(
        page_size: usize,
        first: bool,
        page_type: u8,
        cells: &[Vec<u8>],
        right_most: u32,
    ) -> Vec<u8> {
        let header = if first { PAGE1_BTREE_OFFSET } else { 0 };
        let mut page = vec![0u8; page_size];
        *page.get_mut(header).expect("type") = page_type;
        page.get_mut(header + 3..header + 5)
            .expect("count")
            .copy_from_slice(&u16::try_from(cells.len()).unwrap_or(u16::MAX).to_be_bytes());
        let mut content_start = page_size;
        let mut pointers = Vec::with_capacity(cells.len());
        for cell in cells {
            content_start -= cell.len();
            page.get_mut(content_start..content_start + cell.len())
                .expect("cell fits")
                .copy_from_slice(cell);
            pointers.push(u16::try_from(content_start).unwrap_or(u16::MAX));
        }
        page.get_mut(header + 5..header + 7)
            .expect("content start")
            .copy_from_slice(&u16::try_from(content_start).unwrap_or(0).to_be_bytes());
        let pointer_start = header + if page_type == 0x05 { 12 } else { 8 };
        for (index, pointer) in pointers.iter().enumerate() {
            let at = pointer_start + index * 2;
            page.get_mut(at..at + 2)
                .expect("pointer")
                .copy_from_slice(&pointer.to_be_bytes());
        }
        if page_type == 0x05 {
            page.get_mut(header + 8..header + 12)
                .expect("right-most")
                .copy_from_slice(&right_most.to_be_bytes());
        }
        page
    }

    fn leaf_cell(payload: &[u8], rowid: u64) -> Vec<u8> {
        let mut cell = put_varint(u64::try_from(payload.len()).unwrap_or(0));
        cell.extend_from_slice(&put_varint(rowid));
        cell.extend_from_slice(payload);
        cell
    }

    fn interior_cell(child: u32) -> Vec<u8> {
        let mut cell = child.to_be_bytes().to_vec();
        cell.extend_from_slice(&put_varint(1));
        cell
    }

    /// A record: header length, serial types, body.
    fn record(serials: &[u64], body: &[u8]) -> Vec<u8> {
        let mut header_tail = Vec::new();
        for serial in serials {
            header_tail.extend_from_slice(&put_varint(*serial));
        }
        let header_len = u64::try_from(header_tail.len() + 1).unwrap_or(1);
        let mut out = put_varint(header_len);
        out.extend_from_slice(&header_tail);
        out.extend_from_slice(body);
        out
    }

    fn put_varint(value: u64) -> Vec<u8> {
        if value <= 0x7f {
            return vec![u8::try_from(value).unwrap_or(0)];
        }
        let mut out = [0u8; 9];
        let mut remaining = value;
        out[8] = u8::try_from(remaining & 0xff).unwrap_or(0);
        remaining >>= 8;
        let mut at = 8usize;
        while remaining > 0 {
            at -= 1;
            out[at] = 0x80 | u8::try_from(remaining & 0x7f).unwrap_or(0);
            remaining >>= 7;
        }
        out.get(at..).unwrap_or(&[]).to_vec()
    }
}

// SPDX-License-Identifier: AGPL-3.0-only
//! The frames of a warm replication stream (`node-agent-protocol.md` section
//! 6.13, ADR 0173).
//!
//! A stream is the body of one `POST /v1/replicate`: one header, then blocks
//! and zero ranges in increasing offset order with no overlap, then one
//! manifest. Each frame is `[u8 type][u32 BE length][payload]`, as on the
//! terminal exec channel (ADR 0126).
//!
//! [`Decoder`] turns body chunks into frames and refuses a length before it
//! buffers the payload, so a peer cannot make the target hold more than one
//! block. [`Sequence`] checks the order: what lies inside the volume, inside
//! one cell, after the previous frame, and how many frames the manifest
//! counts.

use serde::{Deserialize, Serialize};

use quoro_backup::cells::CELL;

/// Frame type of the header.
pub const TYPE_HEADER: u8 = 1;
/// Frame type of a block.
pub const TYPE_BLOCK: u8 = 2;
/// Frame type of a zero range.
pub const TYPE_ZERO: u8 = 3;
/// Frame type of the manifest.
pub const TYPE_MANIFEST: u8 = 4;

/// The largest block, 1 MiB: one whole cell.
pub const BLOCK_MAX: u32 = 1 << 20;
/// The largest header or manifest payload. Both are a few hundred bytes of
/// JSON; the bound only stops a peer from making the target buffer more.
pub const JSON_MAX: u32 = 64 * 1024;
/// A block's payload before its data: `offset`, `length` and `blake3`.
const BLOCK_PREFIX: usize = 8 + 4 + 32;
/// A zero range's payload: `offset` and `length`.
const ZERO_PAYLOAD: usize = 8 + 8;
/// Type and length in front of every payload.
const FRAME_PREFIX: usize = 1 + 4;

/// The header frame's JSON (section 6.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamHeader {
    pub app_id: String,
    pub volume_id: String,
    pub lease_id: String,
    pub fencing_token: u64,
    pub snapshot_seq: u64,
    /// The target's copy the ranges apply to; `None` for a full copy.
    pub base_seq: Option<u64>,
    /// RFC 3339: when the source took the snapshot.
    pub taken_at: String,
    /// The volume's size in bytes, `size_gb` GiB.
    pub size_bytes: u64,
}

/// The manifest frame's JSON (section 6.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Blocks and zero ranges sent.
    pub block_count: u64,
    /// The root hash of the copy, 64 lowercase hex characters.
    pub root_hash: String,
}

/// `status` of the answer to a stream (section 6.13).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerStatus {
    Complete,
    Refused,
}

/// `reason` of a refused stream (section 6.13).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefuseReason {
    StaleToken,
    WrongLease,
    NotTarget,
    NotNewer,
    BaseMismatch,
    RootMismatch,
    BadFrame,
    Busy,
}

impl RefuseReason {
    /// The reason as the answer spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StaleToken => "stale_token",
            Self::WrongLease => "wrong_lease",
            Self::NotTarget => "not_target",
            Self::NotNewer => "not_newer",
            Self::BaseMismatch => "base_mismatch",
            Self::RootMismatch => "root_mismatch",
            Self::BadFrame => "bad_frame",
            Self::Busy => "busy",
        }
    }
}

/// The JSON answer to a stream (section 6.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    pub status: AnswerStatus,
    /// The target's newest complete copy, or null.
    pub snapshot_seq: Option<u64>,
    /// Present on `refused`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<RefuseReason>,
}

/// One decoded frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Header(StreamHeader),
    Block {
        offset: u64,
        /// The BLAKE3 hash the source computed over `data`.
        blake3: [u8; 32],
        data: Vec<u8>,
    },
    Zero {
        offset: u64,
        length: u64,
    },
    Manifest(Manifest),
}

/// Why a frame or a stream is malformed; the target answers `bad_frame`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameError(pub String);

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FrameError {}

fn bad(reason: impl Into<String>) -> FrameError {
    FrameError(reason.into())
}

/// Encodes one frame.
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    let (kind, payload) = match frame {
        Frame::Header(header) => (TYPE_HEADER, serde_json::to_vec(header).unwrap_or_default()),
        Frame::Block {
            offset,
            blake3,
            data,
        } => {
            let mut payload = Vec::with_capacity(BLOCK_PREFIX + data.len());
            payload.extend_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&u32::try_from(data.len()).unwrap_or(u32::MAX).to_be_bytes());
            payload.extend_from_slice(blake3);
            payload.extend_from_slice(data);
            (TYPE_BLOCK, payload)
        }
        Frame::Zero { offset, length } => {
            let mut payload = Vec::with_capacity(ZERO_PAYLOAD);
            payload.extend_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&length.to_be_bytes());
            (TYPE_ZERO, payload)
        }
        Frame::Manifest(manifest) => (
            TYPE_MANIFEST,
            serde_json::to_vec(manifest).unwrap_or_default(),
        ),
    };
    let mut out = Vec::with_capacity(FRAME_PREFIX + payload.len());
    out.push(kind);
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(&payload);
    out
}

/// The largest payload a frame of `kind` may declare, or an error for a type
/// the stream does not have.
fn max_payload(kind: u8) -> Result<usize, FrameError> {
    match kind {
        TYPE_HEADER | TYPE_MANIFEST => Ok(JSON_MAX as usize),
        TYPE_BLOCK => Ok(BLOCK_PREFIX + BLOCK_MAX as usize),
        TYPE_ZERO => Ok(ZERO_PAYLOAD),
        other => Err(bad(format!("unknown frame type {other}"))),
    }
}

/// Turns the bytes of a stream body into frames.
#[derive(Debug, Default)]
pub struct Decoder {
    buf: Vec<u8>,
    /// Bytes of `buf` already consumed.
    start: usize,
}

impl Decoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a chunk of the body.
    pub fn push(&mut self, chunk: &[u8]) {
        if self.start > 0 && self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        }
        self.buf.extend_from_slice(chunk);
    }

    /// Bytes received and not yet decoded into a frame.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.buf.len() - self.start
    }

    /// The next whole frame, `None` while its bytes have not all arrived.
    ///
    /// # Errors
    ///
    /// [`FrameError`] for an unknown type, a length out of bounds for its
    /// type (checked before the payload is buffered), or a payload that does
    /// not parse.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        let rest = &self.buf[self.start..];
        if rest.len() < FRAME_PREFIX {
            return Ok(None);
        }
        let kind = rest[0];
        let length = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
        if length > max_payload(kind)? {
            return Err(bad(format!(
                "a frame of type {kind} declares {length} bytes"
            )));
        }
        if rest.len() < FRAME_PREFIX + length {
            return Ok(None);
        }
        let payload = &rest[FRAME_PREFIX..FRAME_PREFIX + length];
        let frame = parse(kind, payload)?;
        self.start += FRAME_PREFIX + length;
        if self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        } else if self.start > (BLOCK_PREFIX + BLOCK_MAX as usize) {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        Ok(Some(frame))
    }
}

fn parse(kind: u8, payload: &[u8]) -> Result<Frame, FrameError> {
    let u64_at = |at: usize| {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&payload[at..at + 8]);
        u64::from_be_bytes(bytes)
    };
    match kind {
        TYPE_HEADER => serde_json::from_slice(payload)
            .map(Frame::Header)
            .map_err(|e| bad(format!("header: {e}"))),
        TYPE_MANIFEST => {
            let manifest: Manifest =
                serde_json::from_slice(payload).map_err(|e| bad(format!("manifest: {e}")))?;
            let hex = &manifest.root_hash;
            if hex.len() != 64
                || !hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(bad(
                    "manifest: root_hash is not 64 lowercase hex characters",
                ));
            }
            Ok(Frame::Manifest(manifest))
        }
        TYPE_BLOCK => {
            if payload.len() < BLOCK_PREFIX + 1 {
                return Err(bad("a block frame is too short"));
            }
            let offset = u64_at(0);
            let length =
                u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]) as usize;
            if length == 0 || length > BLOCK_MAX as usize {
                return Err(bad(format!("a block of {length} bytes")));
            }
            if payload.len() != BLOCK_PREFIX + length {
                return Err(bad(format!(
                    "a block of {length} bytes in a frame of {} bytes",
                    payload.len()
                )));
            }
            let mut blake3 = [0_u8; 32];
            blake3.copy_from_slice(&payload[12..BLOCK_PREFIX]);
            Ok(Frame::Block {
                offset,
                blake3,
                data: payload[BLOCK_PREFIX..].to_vec(),
            })
        }
        TYPE_ZERO => {
            if payload.len() != ZERO_PAYLOAD {
                return Err(bad("a zero range frame is not 16 bytes"));
            }
            Ok(Frame::Zero {
                offset: u64_at(0),
                length: u64_at(8),
            })
        }
        other => Err(bad(format!("unknown frame type {other}"))),
    }
}

/// The order rules of one stream after its header (section 6.13).
#[derive(Debug)]
pub struct Sequence {
    size_bytes: u64,
    /// The end of the previous block or zero range: the next one starts at
    /// or after it.
    next_offset: u64,
    /// Blocks and zero ranges seen.
    count: u64,
}

impl Sequence {
    /// The rules for a copy of `size_bytes`, which must be a whole number of
    /// cells.
    ///
    /// # Errors
    ///
    /// [`FrameError`] when it is not.
    pub fn new(size_bytes: u64) -> Result<Self, FrameError> {
        if size_bytes == 0 || !size_bytes.is_multiple_of(CELL) {
            return Err(bad(format!(
                "size_bytes {size_bytes} is not a whole number of 1 MiB cells"
            )));
        }
        Ok(Self {
            size_bytes,
            next_offset: 0,
            count: 0,
        })
    }

    /// Checks one range and moves past it.
    fn range(&mut self, what: &str, offset: u64, length: u64) -> Result<u64, FrameError> {
        if length == 0 {
            return Err(bad(format!("an empty {what}")));
        }
        if offset < self.next_offset {
            return Err(bad(format!(
                "a {what} at {offset} overlaps or precedes the previous frame, which ends at {}",
                self.next_offset
            )));
        }
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.size_bytes)
            .ok_or_else(|| {
                bad(format!(
                    "a {what} at {offset} of {length} bytes is beyond the volume"
                ))
            })?;
        self.next_offset = end;
        self.count += 1;
        Ok(end)
    }

    /// A block: inside the volume, inside one cell, after the previous frame.
    ///
    /// # Errors
    ///
    /// [`FrameError`] when it is not.
    pub fn block(&mut self, offset: u64, length: u64) -> Result<(), FrameError> {
        let end = self.range("block", offset, length)?;
        if offset / CELL != (end - 1) / CELL {
            return Err(bad(format!(
                "a block at {offset} of {length} bytes crosses a cell boundary"
            )));
        }
        Ok(())
    }

    /// A zero range: inside the volume, after the previous frame.
    ///
    /// # Errors
    ///
    /// [`FrameError`] when it is not.
    pub fn zero(&mut self, offset: u64, length: u64) -> Result<(), FrameError> {
        self.range("zero range", offset, length).map(|_| ())
    }

    /// The manifest counts every block and zero range sent.
    ///
    /// # Errors
    ///
    /// [`FrameError`] when its count differs.
    pub fn manifest(&self, manifest: &Manifest) -> Result<(), FrameError> {
        if manifest.block_count != self.count {
            return Err(bad(format!(
                "the manifest counts {} frames and {} arrived",
                manifest.block_count, self.count
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BLOCK_MAX, Decoder, Frame, FrameError, JSON_MAX, Manifest, Sequence, StreamHeader,
        TYPE_BLOCK, TYPE_HEADER, encode,
    };
    use quoro_backup::cells::CELL;

    fn header() -> StreamHeader {
        StreamHeader {
            app_id: "app_01".to_owned(),
            volume_id: "vol_01".to_owned(),
            lease_id: "lse_01".to_owned(),
            fencing_token: 7,
            snapshot_seq: 2,
            base_seq: Some(1),
            taken_at: "2026-11-06T20:00:00Z".to_owned(),
            size_bytes: 1 << 30,
        }
    }

    fn frames() -> Vec<Frame> {
        vec![
            Frame::Header(header()),
            Frame::Block {
                offset: 4096,
                blake3: [9; 32],
                data: vec![1, 2, 3],
            },
            Frame::Zero {
                offset: CELL,
                length: 3 * CELL,
            },
            Frame::Manifest(Manifest {
                block_count: 2,
                root_hash: "ab".repeat(32),
            }),
        ]
    }

    #[test]
    fn frames_round_trip_whatever_the_chunking() {
        let bytes: Vec<u8> = frames().iter().flat_map(encode).collect();
        for chunk in [1, 3, 7, 64, bytes.len()] {
            let mut decoder = Decoder::new();
            let mut out = Vec::new();
            for piece in bytes.chunks(chunk) {
                decoder.push(piece);
                while let Some(frame) = decoder.next_frame().expect("decode") {
                    out.push(frame);
                }
            }
            assert_eq!(out, frames(), "chunks of {chunk}");
            assert_eq!(decoder.pending(), 0);
        }
    }

    #[test]
    fn a_length_out_of_bounds_is_refused_before_its_payload_arrives() {
        for (kind, length) in [
            (TYPE_HEADER, JSON_MAX + 1),
            (TYPE_BLOCK, 44 + BLOCK_MAX + 1),
            (3, 17),
            (9, 1),
        ] {
            let mut decoder = Decoder::new();
            decoder.push(&[kind]);
            decoder.push(&length.to_be_bytes());
            assert!(decoder.next_frame().is_err(), "type {kind} length {length}");
        }
    }

    #[test]
    fn malformed_payloads_are_refused() {
        let decode = |bytes: Vec<u8>| {
            let mut decoder = Decoder::new();
            decoder.push(&bytes);
            decoder.next_frame()
        };
        // A block whose length field disagrees with its frame.
        let mut block = encode(&Frame::Block {
            offset: 0,
            blake3: [0; 32],
            data: vec![0; 8],
        });
        block[5 + 11] = 9;
        assert!(decode(block).is_err());
        // An empty block.
        let mut empty = vec![TYPE_BLOCK];
        empty.extend_from_slice(&44_u32.to_be_bytes());
        empty.extend_from_slice(&[0; 44]);
        assert!(decode(empty).is_err());
        // A root hash in upper case, or too short.
        for root in ["AB".repeat(32), "ab".repeat(31)] {
            let manifest = encode(&Frame::Manifest(Manifest {
                block_count: 0,
                root_hash: root,
            }));
            assert!(decode(manifest).is_err());
        }
        // A header that is not the header's JSON.
        let mut junk = vec![TYPE_HEADER];
        junk.extend_from_slice(&2_u32.to_be_bytes());
        junk.extend_from_slice(b"{}");
        assert!(decode(junk).is_err());
    }

    #[test]
    fn the_sequence_holds_frames_in_order_inside_the_volume_and_the_cells() {
        let size = 4 * CELL;
        let mut sequence = Sequence::new(size).expect("size");
        sequence.block(0, 4096).expect("first");
        sequence.block(4096, CELL - 4096).expect("rest of the cell");
        sequence.zero(CELL, 2 * CELL).expect("two cells of zeros");
        sequence.block(3 * CELL, CELL).expect("a whole cell");
        assert!(
            sequence
                .manifest(&Manifest {
                    block_count: 4,
                    root_hash: String::new()
                })
                .is_ok()
        );
        assert!(
            sequence
                .manifest(&Manifest {
                    block_count: 5,
                    root_hash: String::new()
                })
                .is_err()
        );

        let fresh = || Sequence::new(size).expect("size");
        let refused = |result: Result<(), FrameError>| assert!(result.is_err());
        // Overlap and going backwards.
        let mut s = fresh();
        s.block(8192, 4096).expect("block");
        refused(s.block(8192, 1));
        refused(s.zero(0, 10));
        // Across a cell boundary, beyond the volume, empty, overflowing.
        refused(fresh().block(CELL - 1, 2));
        refused(fresh().zero(3 * CELL, CELL + 1));
        refused(fresh().block(0, 0));
        refused(fresh().zero(u64::MAX - 1, 4));
        // Not a whole number of cells.
        assert!(Sequence::new(CELL + 1).is_err());
        assert!(Sequence::new(0).is_err());
    }
}

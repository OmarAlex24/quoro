// SPDX-License-Identifier: AGPL-3.0-only
//! The backup repository format: keys, chunks, manifests and seals.
//!
//! `docs/contracts/backup-format.md` is the binding contract. This crate
//! implements sections 2 to 6 and 8.1: the object layout ([`layout`]), the
//! key derivations of section 3, the config object of section 4, the chunk
//! object of section 5, the manifest and seal of section 6, and the restore
//! checks of section 8.1. Every object this crate writes is byte-for-byte what
//! the contract specifies, so another reader can interoperate with it.
//!
//! Nothing here talks to object storage. The node or primary that took a
//! backup feeds snapshot bytes through [`chunks`] and [`BackupWriter`] and
//! uploads the objects it returns; a restore reads them back and feeds them to
//! [`Restorer`].

use std::fmt;

pub mod layout;

mod aead;
mod chunk;
mod chunking;
mod config;
mod key;
mod manifest;
mod recovery;
mod restorer;
mod seal;
mod util;
mod wrap;
mod writer;

pub use chunk::{ChunkId, chunk_id, open_chunk, seal_chunk};
pub use chunking::{Chunk, chunks};
pub use config::RepoConfig;
pub use key::{Key, RepoKey, SourceKeys};
pub use manifest::{
    Chunker, Manifest, ManifestChunk, SourceKind, decrypt_manifest, encrypt_manifest,
    manifest_digest, open_manifest,
};
pub use recovery::RecoveryKey;
pub use restorer::Restorer;
pub use seal::{Seal, sign_seal, verify_seal};
pub use wrap::{WrappedKey, unwrap_repo_key, wrap_repo_key};
pub use writer::BackupWriter;

/// The format version of the config object, manifests and seals. A reader
/// refuses a version it does not know (section 11).
pub const FORMAT: u32 = 1;

/// FastCDC minimum chunk size, 256 KiB (section 5).
pub const CHUNK_MIN: u32 = 262_144;
/// FastCDC average chunk size, 1 MiB (section 5).
pub const CHUNK_AVG: u32 = 1_048_576;
/// FastCDC maximum chunk size, 4 MiB (section 5).
pub const CHUNK_MAX: u32 = 4_194_304;

/// The result type of this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Why a repository object was refused.
///
/// There is one variant per failure kind and no variant carries key material
/// or plaintext.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// Reading or writing a local file failed.
    Io(String),
    /// An object failed authentication or one of the restore checks of
    /// section 8.1: a truncated or tampered AEAD object, a bad digest, a bad
    /// chunk id or a manifest that does not lay out.
    Corrupt(String),
    /// A structure is not what format version 1 fixes: an unknown format
    /// number, JSON that does not parse, or chunker parameters that differ
    /// from section 5.
    Format(String),
    /// The recovery key text form is not the one section 3 fixes.
    RecoveryKey(String),
    /// A seal was not signed, or does not verify, under the cluster key.
    Signature(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(reason) => write!(f, "i/o error: {reason}"),
            Self::Corrupt(reason) => write!(f, "corrupt backup object: {reason}"),
            Self::Format(reason) => write!(f, "bad format: {reason}"),
            Self::RecoveryKey(reason) => write!(f, "bad recovery key: {reason}"),
            Self::Signature(reason) => write!(f, "bad seal signature: {reason}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

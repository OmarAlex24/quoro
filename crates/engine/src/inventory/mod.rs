// SPDX-License-Identifier: AGPL-3.0-only
//! The package inventory of an unpacked image (engine-trait.md section 3.21,
//! node-agent-protocol.md section 6.11, ADR 0150, ADR 0151).
//!
//! Every byte read here comes from an image the operator did not write, and
//! the engine reads it as root. The rules every parser follows:
//!
//! - Files are reached only through a [`Tree`], which resolves every path
//!   inside the unpacked root. A parser never builds a host path.
//! - Every read is bounded ([`read_bounded`], [`read_range`]); a file over its
//!   bound is skipped and named in `not_inventoried`.
//! - Malformed input yields fewer packages and, where it says something, a
//!   `not_inventoried` kind. It never returns an error and never panics: no
//!   indexing without a bound check, no `unwrap`, no unchecked arithmetic on
//!   values read from the image.
//! - Nothing from the image is executed.
//!
//! [`build`] runs every parser and returns the inventory, cut at
//! [`MAX_PACKAGES`] packages and [`MAX_JSON_BYTES`] of JSON (`truncated`).

use std::collections::{BTreeSet, HashSet};
use std::io::{Read, Seek, SeekFrom};

use crate::{ImageDigest, ImageInventory, InventoryPackage};

pub mod apk;
pub mod dpkg;
pub mod elf;
pub mod npm;
pub mod os_release;
pub mod python;
pub mod rpm;

/// Most packages an inventory holds (section 6.11).
pub const MAX_PACKAGES: usize = 20_000;
/// Most bytes of JSON an inventory serializes to (section 6.11).
pub const MAX_JSON_BYTES: usize = 4 * 1024 * 1024;
/// What the inventory's other fields may take of [`MAX_JSON_BYTES`]; the
/// packages get the rest.
const METADATA_RESERVE: usize = 64 * 1024;
/// Most directory entries one walk visits.
pub const MAX_WALK_ENTRIES: usize = 300_000;
/// Deepest directory a walk descends into, counted from the root.
pub const MAX_WALK_DEPTH: usize = 48;
/// Most executables one walk inspects as ELF files.
pub const MAX_ELF_FILES: usize = 10_000;
/// Largest file inspected as an ELF executable.
pub const MAX_ELF_FILE_BYTES: u64 = 1 << 30;

/// Field length bounds; a package with a longer field is dropped.
const MAX_ECOSYSTEM_LEN: usize = 64;
const MAX_NAME_LEN: usize = 256;
const MAX_VERSION_LEN: usize = 256;
const MAX_PURL_LEN: usize = 2048;
const MAX_PATH_LEN: usize = 1024;

/// `not_inventoried` kinds shared by more than one module.
pub mod kind {
    /// The inventory hit [`super::MAX_PACKAGES`] or [`super::MAX_JSON_BYTES`].
    pub const TRUNCATED: &str = "truncated";
    /// The walk hit [`super::MAX_WALK_ENTRIES`] or [`super::MAX_WALK_DEPTH`].
    pub const WALK_LIMIT: &str = "walk_limit";
    /// More than [`super::MAX_ELF_FILES`] executables.
    pub const ELF_LIMIT: &str = "elf_limit";
    /// A statically linked executable without Go or `cargo auditable` data.
    pub const STATIC_BINARY: &str = "static_binary";
}

/// What a directory entry is, without following a symlink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
    Other,
}

/// One entry of [`Tree::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The entry's name, one path component. Names that are not UTF-8 are
    /// left out by the tree.
    pub name: String,
    pub kind: EntryKind,
    /// Size in bytes, for a regular file.
    pub size: u64,
    /// Any execute bit is set, for a regular file.
    pub executable: bool,
}

/// A file opened through a [`Tree`].
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek + ?Sized> ReadSeek for T {}

/// An open regular file and its length.
pub struct TreeFile<'a> {
    pub len: u64,
    pub reader: Box<dyn ReadSeek + 'a>,
}

/// Read-only access to an unpacked image root.
///
/// Paths are relative to the root, `/`-separated, without a leading `/`;
/// `""` is the root itself. An implementation resolves every path inside the
/// root (an absolute symlink or a `..` never leaves it), opens regular files
/// only, and never blocks on a FIFO or opens a device node.
pub trait Tree {
    /// Opens the regular file at `path`, following symlinks inside the root.
    /// `None` when it is missing, not a regular file or cannot be read.
    fn open(&self, path: &str) -> Option<TreeFile<'_>>;
    /// The entries of the directory at `path`, following symlinks inside the
    /// root to reach it, in no particular order. `None` when it is missing or
    /// not a directory.
    fn list(&self, path: &str) -> Option<Vec<Entry>>;
}

/// The outcome of [`read_bounded`].
#[derive(Debug, PartialEq, Eq)]
pub enum ReadOutcome {
    Missing,
    TooLarge,
    Data(Vec<u8>),
}

/// Reads the whole file at `path` when it is at most `max` bytes.
pub fn read_bounded<T: Tree + ?Sized>(tree: &T, path: &str, max: usize) -> ReadOutcome {
    let Some(file) = tree.open(path) else {
        return ReadOutcome::Missing;
    };
    let limit = u64::try_from(max).unwrap_or(u64::MAX);
    if file.len > limit {
        return ReadOutcome::TooLarge;
    }
    let mut data = Vec::new();
    // The length can change under us only if the tree is being written,
    // which it is not; the `take` keeps the bound regardless.
    match file
        .reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut data)
    {
        Ok(_) if data.len() > max => ReadOutcome::TooLarge,
        Ok(_) => ReadOutcome::Data(data),
        Err(_) => ReadOutcome::Missing,
    }
}

/// Reads `len` bytes at `offset` of an open file; `None` when the range is
/// not inside the file or `len` is over `max`.
pub fn read_range(file: &mut TreeFile<'_>, offset: u64, len: u64, max: usize) -> Option<Vec<u8>> {
    let end = offset.checked_add(len)?;
    if end > file.len || len > u64::try_from(max).ok()? {
        return None;
    }
    file.reader.seek(SeekFrom::Start(offset)).ok()?;
    let mut data = vec![0u8; usize::try_from(len).ok()?];
    file.reader.read_exact(&mut data).ok()?;
    Some(data)
}

/// Joins a directory and an entry name into a tree path.
pub fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

/// The length of `s` as a JSON string literal (RFC 8785: UTF-8 kept as is,
/// `"` and `\` escaped, control characters escaped).
fn json_string_len(s: &str) -> usize {
    let mut len = 2usize;
    for c in s.chars() {
        len = len.saturating_add(match c {
            '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        });
    }
    len
}

/// The serialized length of one package object, plus its separating comma.
fn package_json_len(p: &InventoryPackage) -> usize {
    let mut len = 2usize; // braces
    let mut field = |key: &str, value: &str| {
        // "key":value,
        len = len
            .saturating_add(json_string_len(key))
            .saturating_add(1)
            .saturating_add(json_string_len(value))
            .saturating_add(1);
    };
    field("ecosystem", &p.ecosystem);
    field("name", &p.name);
    if let Some(source) = &p.source_name {
        field("source_name", source);
    }
    field("version", &p.version);
    field("purl", &p.purl);
    if let Some(path) = &p.path {
        field("path", path);
    }
    len
}

fn valid_text(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control)
}

/// Collects packages within the bounds.
#[derive(Debug, Default)]
pub struct Collector {
    packages: Vec<InventoryPackage>,
    seen: HashSet<(String, String, String, Option<String>)>,
    covered: BTreeSet<String>,
    not_inventoried: BTreeSet<String>,
    bytes: usize,
    full: bool,
}

impl Collector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a package. A package with an empty, over-long or control-character
    /// field is dropped; a duplicate (same ecosystem, name, version and
    /// source) is dropped. Returns false once the inventory is full, after
    /// which every push is refused and `truncated` is recorded.
    pub fn push(&mut self, package: InventoryPackage) -> bool {
        if self.full {
            return false;
        }
        let valid = valid_text(&package.ecosystem, MAX_ECOSYSTEM_LEN)
            && valid_text(&package.name, MAX_NAME_LEN)
            && valid_text(&package.version, MAX_VERSION_LEN)
            && valid_text(&package.purl, MAX_PURL_LEN)
            && package
                .source_name
                .as_deref()
                .is_none_or(|s| valid_text(s, MAX_NAME_LEN))
            && package
                .path
                .as_deref()
                .is_none_or(|s| valid_text(s, MAX_PATH_LEN));
        if !valid {
            return true;
        }
        let key = (
            package.ecosystem.clone(),
            package.name.clone(),
            package.version.clone(),
            package.source_name.clone(),
        );
        if self.seen.contains(&key) {
            return true;
        }
        let len = package_json_len(&package);
        if self.packages.len() >= MAX_PACKAGES
            || self.bytes.saturating_add(len) > MAX_JSON_BYTES - METADATA_RESERVE
        {
            self.full = true;
            self.not_inventoried.insert(kind::TRUNCATED.to_owned());
            return false;
        }
        self.bytes += len;
        self.covered.insert(package.ecosystem.clone());
        self.seen.insert(key);
        self.packages.push(package);
        true
    }

    /// Records that the inventory covers `ecosystem` even without a package
    /// in it (an empty package database was read).
    pub fn cover(&mut self, ecosystem: &str) {
        if valid_text(ecosystem, MAX_ECOSYSTEM_LEN) && self.covered.len() < 64 {
            self.covered.insert(ecosystem.to_owned());
        }
    }

    /// Records a kind of content seen and not read.
    pub fn not_inventoried(&mut self, kind: &str) {
        if self.not_inventoried.len() < 32 {
            self.not_inventoried.insert(kind.to_owned());
        }
    }

    pub fn is_full(&self) -> bool {
        self.full
    }

    pub fn len(&self) -> usize {
        self.packages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.packages.is_empty()
    }

    /// The packages collected so far (tests).
    pub fn packages(&self) -> &[InventoryPackage] {
        &self.packages
    }

    /// The kinds recorded so far (tests).
    pub fn kinds(&self) -> Vec<String> {
        self.not_inventoried.iter().cloned().collect()
    }

    fn finish(
        mut self,
        digest: ImageDigest,
        os: Option<(String, String)>,
        inventoried_at: String,
    ) -> ImageInventory {
        self.packages.sort_by(|a, b| {
            (&a.ecosystem, &a.name, &a.version, &a.path).cmp(&(
                &b.ecosystem,
                &b.name,
                &b.version,
                &b.path,
            ))
        });
        ImageInventory {
            digest,
            os,
            ecosystems: self.covered.into_iter().collect(),
            not_inventoried: self.not_inventoried.into_iter().collect(),
            packages: self.packages,
            inventoried_at,
        }
    }
}

/// Percent-encodes one package URL component: everything but ASCII
/// alphanumerics and `.-_~` (purl specification).
pub fn purl_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(b"0123456789ABCDEF"[usize::from(byte >> 4)]));
            out.push(char::from(b"0123456789ABCDEF"[usize::from(byte & 0x0f)]));
        }
    }
    out
}

/// Builds a package URL: `pkg:<kind>/<namespace>/<name>@<version>?<k=v&…>`.
/// Each namespace segment, the name, the version and each qualifier value is
/// percent-encoded; qualifiers are given sorted by key.
pub fn purl(
    kind: &str,
    namespace: &[&str],
    name: &str,
    version: &str,
    qualifiers: &[(&str, &str)],
) -> String {
    let mut out = format!("pkg:{kind}/");
    for segment in namespace {
        out.push_str(&purl_encode(segment));
        out.push('/');
    }
    out.push_str(&purl_encode(name));
    out.push('@');
    out.push_str(&purl_encode(version));
    let mut first = true;
    for (key, value) in qualifiers {
        if value.is_empty() {
            continue;
        }
        out.push(if first { '?' } else { '&' });
        first = false;
        out.push_str(key);
        out.push('=');
        out.push_str(&purl_encode(value));
    }
    out
}

/// What the walk hands to [`visit`] besides the entry itself.
struct Walk {
    entries: usize,
    elf_files: usize,
}

/// Directories at the root that are never part of an image's content.
const SKIP_AT_ROOT: &[&str] = &["proc", "sys", "dev"];

/// Builds the inventory of the tree. Never fails: what cannot be read is
/// named in `not_inventoried`.
pub fn build<T: Tree + ?Sized>(
    tree: &T,
    digest: ImageDigest,
    inventoried_at: String,
) -> ImageInventory {
    let mut out = Collector::new();
    let os = os_release::read(tree);
    dpkg::collect(tree, os.as_ref(), &mut out);
    apk::collect(tree, os.as_ref(), &mut out);
    rpm::collect(tree, os.as_ref(), &mut out);

    let mut walk = Walk {
        entries: 0,
        elf_files: 0,
    };
    // Depth-first with an explicit stack: the image chooses the depth.
    let mut stack: Vec<(String, usize)> = vec![(String::new(), 0)];
    'walk: while let Some((dir, depth)) = stack.pop() {
        if out.is_full() {
            break;
        }
        let Some(mut entries) = tree.list(&dir) else {
            continue;
        };
        // Deterministic order, so a truncated inventory is the same on every
        // node.
        entries.sort_by(|a, b| b.name.cmp(&a.name));
        for entry in entries {
            walk.entries += 1;
            if walk.entries > MAX_WALK_ENTRIES {
                out.not_inventoried(kind::WALK_LIMIT);
                break 'walk;
            }
            let path = join(&dir, &entry.name);
            match entry.kind {
                EntryKind::Dir => {
                    if depth == 0 && SKIP_AT_ROOT.contains(&entry.name.as_str()) {
                        continue;
                    }
                    if python::is_metadata_dir(&entry.name) {
                        python::collect_dir(tree, &path, &mut out);
                        continue;
                    }
                    if depth + 1 >= MAX_WALK_DEPTH {
                        out.not_inventoried(kind::WALK_LIMIT);
                        continue;
                    }
                    stack.push((path, depth + 1));
                }
                EntryKind::File => {
                    if npm::is_lockfile(&path) {
                        npm::collect_file(tree, &path, &mut out);
                    } else if entry.executable
                        && entry.size >= 64
                        && entry.size <= MAX_ELF_FILE_BYTES
                    {
                        walk.elf_files += 1;
                        if walk.elf_files > MAX_ELF_FILES {
                            out.not_inventoried(kind::ELF_LIMIT);
                        } else {
                            elf::inspect(tree, &path, &mut out);
                        }
                    }
                }
                EntryKind::Symlink | EntryKind::Other => {}
            }
        }
    }

    let os_pair = os.map(|os| (os.id, os.version_id.unwrap_or_default()));
    out.finish(digest, os_pair, inventoried_at)
}

/// An in-memory [`Tree`] for tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use super::{Entry, EntryKind, Tree, TreeFile};

    #[derive(Default)]
    pub struct MemTree {
        files: BTreeMap<String, (Vec<u8>, bool)>,
        symlinks: BTreeMap<String, String>,
    }

    #[allow(dead_code, reason = "used by the parser tests as they land")]
    impl MemTree {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn file(mut self, path: &str, data: impl Into<Vec<u8>>) -> Self {
            self.files.insert(path.to_owned(), (data.into(), false));
            self
        }

        pub fn exe(mut self, path: &str, data: impl Into<Vec<u8>>) -> Self {
            self.files.insert(path.to_owned(), (data.into(), true));
            self
        }

        /// A symlink entry; [`Tree::open`] does not follow it in this tree.
        pub fn symlink(mut self, path: &str, target: &str) -> Self {
            self.symlinks.insert(path.to_owned(), target.to_owned());
            self
        }
    }

    impl Tree for MemTree {
        fn open(&self, path: &str) -> Option<TreeFile<'_>> {
            let (data, _) = self.files.get(path)?;
            Some(TreeFile {
                len: data.len() as u64,
                reader: Box::new(Cursor::new(data.as_slice())),
            })
        }

        fn list(&self, path: &str) -> Option<Vec<Entry>> {
            let prefix = if path.is_empty() {
                String::new()
            } else {
                format!("{path}/")
            };
            let mut names: BTreeMap<String, Entry> = BTreeMap::new();
            let mut any = path.is_empty();
            for (file, (data, exe)) in &self.files {
                let Some(rest) = file.strip_prefix(&prefix) else {
                    continue;
                };
                any = true;
                match rest.split_once('/') {
                    Some((dir, _)) => {
                        names.entry(dir.to_owned()).or_insert(Entry {
                            name: dir.to_owned(),
                            kind: EntryKind::Dir,
                            size: 0,
                            executable: false,
                        });
                    }
                    None => {
                        names.insert(
                            rest.to_owned(),
                            Entry {
                                name: rest.to_owned(),
                                kind: EntryKind::File,
                                size: data.len() as u64,
                                executable: *exe,
                            },
                        );
                    }
                }
            }
            for link in self.symlinks.keys() {
                if let Some(rest) = link.strip_prefix(&prefix)
                    && !rest.contains('/')
                {
                    any = true;
                    names.insert(
                        rest.to_owned(),
                        Entry {
                            name: rest.to_owned(),
                            kind: EntryKind::Symlink,
                            size: 0,
                            executable: false,
                        },
                    );
                }
            }
            any.then(|| names.into_values().collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::MemTree;
    use super::*;

    fn pkg(name: &str, version: &str) -> InventoryPackage {
        InventoryPackage {
            ecosystem: "npm".into(),
            name: name.into(),
            source_name: None,
            version: version.into(),
            purl: purl("npm", &[], name, version, &[]),
            path: None,
        }
    }

    #[test]
    fn purl_encodes_components_and_skips_empty_qualifiers() {
        assert_eq!(
            purl(
                "deb",
                &["debian"],
                "libssl3t64",
                "3.5.1-1",
                &[("arch", "amd64"), ("distro", "debian-13"), ("upstream", "")]
            ),
            "pkg:deb/debian/libssl3t64@3.5.1-1?arch=amd64&distro=debian-13"
        );
        assert_eq!(
            purl("npm", &["@types"], "node", "1.0.0+x", &[]),
            "pkg:npm/%40types/node@1.0.0%2Bx"
        );
    }

    #[test]
    fn collector_drops_invalid_and_duplicate_packages() {
        let mut out = Collector::new();
        assert!(out.push(pkg("a", "1")));
        assert!(out.push(pkg("a", "1")));
        assert!(out.push(pkg("", "1")));
        assert!(out.push(pkg("b\n", "1")));
        assert!(out.push(pkg(&"x".repeat(MAX_NAME_LEN + 1), "1")));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn collector_stops_at_the_package_bound() {
        let mut out = Collector::new();
        for i in 0..MAX_PACKAGES {
            assert!(out.push(pkg(&format!("p{i}"), "1")));
        }
        assert!(!out.push(pkg("one-more", "1")));
        assert!(!out.push(pkg("and-another", "1")));
        assert_eq!(out.len(), MAX_PACKAGES);
        assert_eq!(out.kinds(), vec![kind::TRUNCATED.to_owned()]);
    }

    #[test]
    fn collector_stops_at_the_byte_bound() {
        let mut out = Collector::new();
        let long = "v".repeat(MAX_VERSION_LEN);
        let mut pushed = 0usize;
        while out.push(pkg(&format!("{pushed:0>200}"), &long)) {
            pushed += 1;
            assert!(pushed <= MAX_PACKAGES, "the byte bound never applied");
        }
        assert!(out.bytes <= MAX_JSON_BYTES - METADATA_RESERVE);
        assert!(out.kinds().contains(&kind::TRUNCATED.to_owned()));
    }

    #[test]
    fn json_length_counts_escapes() {
        assert_eq!(json_string_len("ab"), 4);
        assert_eq!(json_string_len("a\"b"), 6);
        assert_eq!(json_string_len("é"), 4);
        assert_eq!(json_string_len("\u{1}"), 8);
    }

    #[test]
    fn read_bounded_refuses_an_oversized_file() {
        let tree = MemTree::new().file("a", vec![0u8; 10]);
        assert_eq!(read_bounded(&tree, "a", 9), ReadOutcome::TooLarge);
        assert_eq!(read_bounded(&tree, "a", 10), ReadOutcome::Data(vec![0u8; 10]));
        assert_eq!(read_bounded(&tree, "b", 10), ReadOutcome::Missing);
    }

    #[test]
    fn read_range_checks_the_range() {
        let tree = MemTree::new().file("a", (0u8..10).collect::<Vec<_>>());
        let mut file = tree.open("a").expect("open");
        assert_eq!(read_range(&mut file, 2, 3, 16), Some(vec![2, 3, 4]));
        assert_eq!(read_range(&mut file, 8, 3, 16), None);
        assert_eq!(read_range(&mut file, u64::MAX, 3, 16), None);
        assert_eq!(read_range(&mut file, 0, 10, 4), None);
    }

    #[test]
    fn build_of_an_empty_tree_is_empty() {
        let inventory = build(
            &MemTree::new(),
            ImageDigest("sha256:00".into()),
            "2026-11-05T00:00:00Z".into(),
        );
        assert!(inventory.packages.is_empty());
        assert!(inventory.os.is_none());
        assert!(inventory.ecosystems.is_empty());
    }
}

// SPDX-License-Identifier: AGPL-3.0-only
//! The [`Tree`] the image inventory reads: the staging root of an image
//! being prepared, with every path resolved inside it (engine-trait.md
//! sections 3.4 and 3.21).
//!
//! Every lookup goes through `openat2(…, RESOLVE_IN_ROOT |
//! RESOLVE_NO_MAGICLINKS)` on the root's descriptor, as layer application
//! does: an absolute symlink (`etc/os-release -> /usr/lib/os-release`)
//! resolves inside the root and a `..` is clamped at it, so no read leaves the
//! unpacked image. A file is first opened `O_PATH`, which never opens the
//! object itself, and only a regular file is then opened for reading,
//! `O_NONBLOCK | O_NOCTTY`: a FIFO, a socket or a device node in the image is
//! never opened.

use std::fs::File;
use std::path::Path;

use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, CWD, Dir, FileType, Mode, OFlags, ResolveFlags, fstat, openat, openat2, statat,
};

use crate::EngineError;
use crate::inventory::{Entry, EntryKind, Tree, TreeFile};

/// Most entries one directory listing returns; a directory with more is cut.
const MAX_DIR_ENTRIES: usize = 100_000;

fn resolve_flags() -> ResolveFlags {
    ResolveFlags::IN_ROOT | ResolveFlags::NO_MAGICLINKS
}

/// A path relative to the root; `"."` for the root. `None` for a path with a
/// NUL byte, which no image path can hold.
fn relative(path: &str) -> Option<&str> {
    if path.contains('\0') {
        return None;
    }
    let trimmed = path.trim_start_matches('/');
    Some(if trimmed.is_empty() { "." } else { trimmed })
}

/// An unpacked image root, read-only.
pub(crate) struct RootTree {
    root: OwnedFd,
}

impl RootTree {
    /// Opens the root directory itself (it is the engine's own staging
    /// directory, so its path is trusted).
    pub(crate) fn open(root: &Path) -> Result<Self, EngineError> {
        let root = openat(
            CWD,
            root,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|err| EngineError::Io(format!("{}: {err}", root.display())))?;
        Ok(Self { root })
    }
}

impl Tree for RootTree {
    fn open(&self, path: &str) -> Option<TreeFile<'_>> {
        let rel = relative(path)?;
        let handle = openat2(
            &self.root,
            rel,
            OFlags::PATH | OFlags::CLOEXEC,
            Mode::empty(),
            resolve_flags(),
        )
        .ok()?;
        let first = fstat(&handle).ok()?;
        if FileType::from_raw_mode(first.st_mode) != FileType::RegularFile {
            return None;
        }
        let fd = openat2(
            &self.root,
            rel,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOCTTY,
            Mode::empty(),
            resolve_flags(),
        )
        .ok()?;
        // Same object as the one checked: nothing else writes the staging
        // tree, but the check costs one call.
        let second = fstat(&fd).ok()?;
        if FileType::from_raw_mode(second.st_mode) != FileType::RegularFile
            || second.st_dev != first.st_dev
            || second.st_ino != first.st_ino
        {
            return None;
        }
        let len = u64::try_from(second.st_size).ok()?;
        Some(TreeFile {
            len,
            reader: Box::new(File::from(fd)),
        })
    }

    fn list(&self, path: &str) -> Option<Vec<Entry>> {
        let rel = relative(path)?;
        let dir = openat2(
            &self.root,
            rel,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            resolve_flags(),
        )
        .ok()?;
        let mut entries = Vec::new();
        let mut reader = Dir::read_from(&dir).ok()?;
        while let Some(item) = reader.read() {
            let Ok(item) = item else {
                break;
            };
            let raw = item.file_name();
            if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
                continue;
            }
            let Ok(name) = std::str::from_utf8(raw.to_bytes()) else {
                continue;
            };
            // The final component is never followed: a symlink is listed as
            // one and the walk does not descend into it.
            let Ok(stat) = statat(&dir, raw, AtFlags::SYMLINK_NOFOLLOW) else {
                continue;
            };
            let kind = match FileType::from_raw_mode(stat.st_mode) {
                FileType::RegularFile => EntryKind::File,
                FileType::Directory => EntryKind::Dir,
                FileType::Symlink => EntryKind::Symlink,
                _ => EntryKind::Other,
            };
            entries.push(Entry {
                name: name.to_owned(),
                kind,
                size: u64::try_from(stat.st_size).unwrap_or(0),
                executable: kind == EntryKind::File && stat.st_mode & 0o111 != 0,
            });
            if entries.len() >= MAX_DIR_ENTRIES {
                break;
            }
        }
        Some(entries)
    }
}

/// The current UTC time as RFC 3339 with seconds (`2026-11-05T06:09:41Z`).
pub(crate) fn rfc3339_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    rfc3339(secs)
}

/// Formats seconds since the epoch as RFC 3339 UTC.
fn rfc3339(secs: u64) -> String {
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rem = secs % 86_400;
    // Civil date from days since 1970-01-01 (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::cache::tests_support::temp_dir;
    use std::io::Read;

    #[test]
    fn rfc3339_formats_known_instants() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339(1_793_772_581), "2026-11-04T06:09:41Z");
    }

    #[test]
    fn reads_stay_inside_the_root() {
        let outside = temp_dir("tree-outside");
        std::fs::write(outside.join("secret"), b"host").expect("secret");
        let root = temp_dir("tree-root");
        std::fs::create_dir_all(root.join("etc")).expect("etc");
        std::fs::create_dir_all(root.join("usr/lib")).expect("usr/lib");
        std::fs::write(root.join("usr/lib/os-release"), b"ID=debian\n").expect("os-release");
        std::os::unix::fs::symlink("/usr/lib/os-release", root.join("etc/os-release"))
            .expect("absolute symlink");
        std::os::unix::fs::symlink(outside.join("secret"), root.join("escape"))
            .expect("escaping symlink");
        std::os::unix::fs::symlink("../../../../../../..", root.join("up")).expect("up");
        std::os::unix::fs::symlink(&outside, root.join("dirlink")).expect("dir symlink");

        let tree = RootTree::open(&root).expect("root");
        let mut data = String::new();
        tree.open("etc/os-release")
            .expect("in-root symlink resolves")
            .reader
            .read_to_string(&mut data)
            .expect("read");
        assert_eq!(data, "ID=debian\n");
        // An absolute target resolves inside the root, where it does not exist.
        assert!(tree.open("escape").is_none());
        assert!(tree.open("up/secret").is_none());
        assert!(
            tree.open(&format!("up{}", outside.join("secret").display()))
                .is_none()
        );
        assert!(
            tree.list("dirlink")
                .is_none_or(|entries| entries.is_empty())
        );
        // Directories are not files, and a NUL never reaches the kernel.
        assert!(tree.open("etc").is_none());
        assert!(tree.open("etc\0os-release").is_none());

        let mut names: Vec<(String, EntryKind)> = tree
            .list("")
            .expect("root listing")
            .into_iter()
            .map(|entry| (entry.name, entry.kind))
            .collect();
        names.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            names,
            vec![
                ("dirlink".to_owned(), EntryKind::Symlink),
                ("escape".to_owned(), EntryKind::Symlink),
                ("etc".to_owned(), EntryKind::Dir),
                ("up".to_owned(), EntryKind::Symlink),
                ("usr".to_owned(), EntryKind::Dir),
            ]
        );
        std::fs::remove_dir_all(&root).expect("cleanup");
        std::fs::remove_dir_all(&outside).expect("cleanup");
    }

    #[test]
    fn a_fifo_is_never_opened() {
        let root = temp_dir("tree-fifo");
        rustix::fs::mknodat(
            CWD,
            root.join("pipe"),
            FileType::Fifo,
            Mode::from_raw_mode(0o644),
            0,
        )
        .expect("fifo");
        let tree = RootTree::open(&root).expect("root");
        // Opening a FIFO for reading would block until a writer appears.
        assert!(tree.open("pipe").is_none());
        let entries = tree.list("").expect("listing");
        assert_eq!(entries[0].kind, EntryKind::Other);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

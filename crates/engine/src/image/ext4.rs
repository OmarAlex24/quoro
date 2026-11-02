// SPDX-License-Identifier: AGPL-3.0-only
//! Sizing and building the ext4 root disk.
//!
//! The staging tree is walked with `symlink_metadata` (never following a
//! symlink), the size is derived from its blocks and inodes, and
//! `mkfs.ext4 -d` writes the tree into a fresh file in one go. The guest mounts
//! the result read-only.

use std::fs::File;
use std::path::Path;
use std::process::Command;

use crate::EngineError;

/// One filesystem block.
const BLOCK: u64 = 4096;
/// One mebibyte.
const MIB: u64 = 1024 * 1024;

/// What the staging tree needs on disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TreeSize {
    /// Sum of `round_up(len, 4096)` for files and one block per other inode.
    pub data_bytes: u64,
    /// Number of inodes, the root directory included.
    pub inodes: u64,
}

/// Walks `root` and sums the blocks and inodes it occupies.
pub(crate) fn staging_size(root: &Path) -> Result<TreeSize, EngineError> {
    let mut size = TreeSize {
        data_bytes: BLOCK,
        inodes: 1,
    };
    walk(root, &mut size)?;
    Ok(size)
}

fn walk(dir: &Path, size: &mut TreeSize) -> Result<(), EngineError> {
    let entries = std::fs::read_dir(dir).map_err(|err| io_error(dir, &err))?;
    for entry in entries {
        let entry = entry.map_err(|err| io_error(dir, &err))?;
        let path = entry.path();
        // `DirEntry::metadata` does not follow a symlink.
        let metadata = entry.metadata().map_err(|err| io_error(&path, &err))?;
        size.inodes += 1;
        if metadata.is_dir() {
            size.data_bytes += BLOCK;
            walk(&path, size)?;
        } else if metadata.is_file() {
            size.data_bytes += round_up(metadata.len(), BLOCK);
        }
    }
    Ok(())
}

/// The image size the staging tree needs: data with 15% slack, 256 bytes per
/// inode and 32 MiB of headroom, rounded up to a mebibyte.
pub(crate) fn image_size(data_bytes: u64, inodes: u64) -> u64 {
    let data = data_bytes + data_bytes * 15 / 100;
    let raw = data + inodes * 256 + 32 * MIB;
    round_up(raw, MIB)
}

/// Builds `<out>` from the staging tree with `mkfs.ext4 -d`.
///
/// Returns the image's size in bytes. An out-of-space failure is retried once
/// with 50% more room; `mkfs`'s stderr is part of the error.
pub(crate) fn build(mkfs: &Path, staging: &Path, out: &Path) -> Result<u64, EngineError> {
    let TreeSize { data_bytes, inodes } = staging_size(staging)?;
    let inode_count = inodes + inodes / 4 + 1024;
    let mut size = image_size(data_bytes, inodes);

    match run_mkfs(mkfs, staging, out, size, inode_count) {
        Ok(()) => Ok(size),
        Err(failure) if failure.out_of_space => {
            size = round_up(size * 3 / 2, MIB);
            run_mkfs(mkfs, staging, out, size, inode_count)
                .map(|()| size)
                .map_err(|retry| {
                    EngineError::Io(format!(
                        "mkfs.ext4 failed after retrying with {size} bytes: {}",
                        retry.message
                    ))
                })
        }
        Err(failure) => Err(EngineError::Io(format!(
            "mkfs.ext4 failed: {}",
            failure.message
        ))),
    }
}

/// Why `mkfs.ext4` failed.
struct MkfsFailure {
    message: String,
    out_of_space: bool,
}

fn run_mkfs(
    mkfs: &Path,
    staging: &Path,
    out: &Path,
    size_bytes: u64,
    inode_count: u64,
) -> Result<(), MkfsFailure> {
    let create = File::create(out).and_then(|file| file.set_len(size_bytes));
    if let Err(err) = create {
        return Err(MkfsFailure {
            message: format!("creating {}: {err}", out.display()),
            out_of_space: false,
        });
    }
    let size_kib = size_bytes.div_ceil(1024).max(1);
    let output = Command::new(mkfs)
        .arg("-q")
        .arg("-F")
        .arg("-t")
        .arg("ext4")
        .arg("-b")
        .arg("4096")
        .arg("-m")
        .arg("0")
        .arg("-O")
        .arg("^has_journal")
        .arg("-N")
        .arg(inode_count.to_string())
        .arg("-L")
        .arg("quoro-root")
        .arg("-E")
        .arg("nodiscard")
        .arg("-d")
        .arg(staging)
        .arg(out)
        .arg(format!("{size_kib}k"))
        .output()
        .map_err(|err| MkfsFailure {
            message: format!("running {}: {err}", mkfs.display()),
            out_of_space: false,
        })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let message = format!("{} ({})", stderr.trim(), output.status);
    Err(MkfsFailure {
        message,
        out_of_space: is_out_of_space(&stderr),
    })
}

/// Whether `mkfs.ext4`'s stderr reads like a full disk.
fn is_out_of_space(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    [
        "no space left",
        "not enough space",
        "could not allocate",
        "out of space",
        "no room",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn round_up(value: u64, unit: u64) -> u64 {
    value.div_ceil(unit) * unit
}

fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> std::path::PathBuf {
        let root = crate::image::cache::tests_support::temp_dir("ext4-tree");
        std::fs::create_dir_all(root.join("etc/ssl")).expect("dirs");
        std::fs::write(root.join("etc/passwd"), vec![0u8; 100]).expect("small file");
        std::fs::write(root.join("etc/ssl/ca.pem"), vec![0u8; 5000]).expect("large file");
        std::os::unix::fs::symlink("/run", root.join("var-run")).expect("symlink");
        std::os::unix::fs::symlink("/does/not/exist", root.join("dangling")).expect("dangling");
        root
    }

    #[test]
    fn staging_size_counts_blocks_and_inodes() {
        let root = tree();
        let size = staging_size(&root).expect("size");
        // Root plus `etc` and `etc/ssl`: three directories.
        // passwd rounds to one block, ca.pem to two blocks.
        assert_eq!(size.inodes, 7);
        assert_eq!(size.data_bytes, 3 * BLOCK + BLOCK + 2 * BLOCK);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn image_size_adds_slack_and_rounds_to_a_mebibyte() {
        assert_eq!(image_size(0, 0), 32 * MIB);
        assert_eq!(image_size(0, 1), round_up(32 * MIB + 256, MIB));
        assert_eq!(
            image_size(MIB, 0),
            round_up(MIB + MIB * 15 / 100 + 32 * MIB, MIB)
        );
        assert_eq!(image_size(10 * MIB, 0) % MIB, 0);
        assert!(image_size(10 * MIB, 0) > 10 * MIB + 32 * MIB);
        assert!(image_size(10 * MIB, 100_000) > image_size(10 * MIB, 0));
    }

    #[test]
    fn a_missing_staging_tree_is_an_io_error() {
        let root = crate::image::cache::tests_support::temp_dir("ext4-missing");
        let missing = root.join("nope");
        assert!(matches!(staging_size(&missing), Err(EngineError::Io(_))));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn a_failing_mkfs_reports_its_stderr() {
        let root = crate::image::cache::tests_support::temp_dir("ext4-mkfs-fail");
        let out = root.join("rootfs.ext4");
        let err = build(Path::new("/bin/false"), &root, &out).expect_err("mkfs fails");
        assert!(err.to_string().contains("mkfs.ext4 failed"), "{err}");
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

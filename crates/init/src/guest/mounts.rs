// SPDX-License-Identifier: AGPL-3.0-only
//! The mounts that turn the initramfs into the app's root filesystem.
//!
//! Order matters: the kernel filesystems first (everything else needs `/dev`,
//! `/proc` and `/sys`), then the overlay built from the read-only root image
//! and the writable data disk, then the move into the overlay and `chroot`,
//! and finally the pseudo-filesystems that only exist inside the app's root.

use std::ffi::CString;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::Path;

use rustix::io::Errno;
use rustix::mount::{MountFlags, mount, mount_move};
use rustix::process::{chdir, chroot};

use crate::guest::Fault;

/// Mount a filesystem. `data` is the comma-separated option string, when the
/// filesystem takes one. `EBUSY` means it is already mounted, which the boot
/// wants anyway.
fn mount_fs(
    source: &str,
    target: &str,
    fstype: &str,
    flags: MountFlags,
    data: Option<&str>,
) -> Result<(), Fault> {
    let options = match data {
        Some(text) => Some(
            CString::new(text)
                .map_err(|_| Fault::new(format!("mount options for {target} contain a NUL")))?,
        ),
        None => None,
    };
    match mount(source, target, fstype, flags, options.as_deref()) {
        Ok(()) => Ok(()),
        Err(Errno::BUSY) => Ok(()),
        Err(err) => Err(Fault::new(format!("mount {fstype} on {target}: {err}"))),
    }
}

/// `mkdir -p` with an explicit mode for the directories it creates.
fn make_dir(path: &str, mode: u32) -> Result<(), Fault> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(mode);
    builder
        .create(path)
        .map_err(|err| Fault::new(format!("mkdir {path}: {err}")))
}

/// Step 1: devtmpfs, proc and sysfs, on the initramfs.
pub(crate) fn mount_early() -> Result<(), Fault> {
    for (target, fstype) in [("/dev", "devtmpfs"), ("/proc", "proc"), ("/sys", "sysfs")] {
        make_dir(target, 0o755)?;
        mount_fs(fstype, target, fstype, MountFlags::empty(), None)?;
    }
    Ok(())
}

/// Step 3: the read-only root image below, the writable data disk beside it,
/// and the overlay that joins them on `/newroot`.
pub(crate) fn mount_root(root_device: &str, data_device: &str) -> Result<(), Fault> {
    for path in ["/quoro", "/quoro/lower", "/quoro/data", "/newroot"] {
        make_dir(path, 0o755)?;
    }

    mount_fs(
        root_device,
        "/quoro/lower",
        "ext4",
        MountFlags::RDONLY,
        None,
    )?;
    mount_fs(
        data_device,
        "/quoro/data",
        "ext4",
        MountFlags::NOATIME,
        None,
    )?;

    // The data disk outlives stop/start, so these keep the app's writes to `/`
    // until the VM is deleted.
    for path in [
        "/quoro/data/root",
        "/quoro/data/root/upper",
        "/quoro/data/root/work",
    ] {
        make_dir(path, 0o755)?;
    }

    let options =
        "lowerdir=/quoro/lower,upperdir=/quoro/data/root/upper,workdir=/quoro/data/root/work";
    mount_fs(
        "overlay",
        "/newroot",
        "overlay",
        MountFlags::empty(),
        Some(options),
    )
}

/// Step 4: carry `/dev`, `/proc` and `/sys` into the new root, move the overlay
/// over the initramfs root, and `chroot` into it.
pub(crate) fn switch_root() -> Result<(), Fault> {
    for name in ["dev", "proc", "sys"] {
        let target = format!("/newroot/{name}");
        make_dir(&target, 0o755)?;
        mount_move(format!("/{name}"), &target)
            .map_err(|err| Fault::new(format!("move /{name} into the new root: {err}")))?;
    }

    chdir("/newroot").map_err(|err| Fault::new(format!("chdir /newroot: {err}")))?;
    mount_move(".", "/").map_err(|err| Fault::new(format!("move the overlay to /: {err}")))?;
    chroot(".").map_err(|err| Fault::new(format!("chroot: {err}")))?;
    chdir("/").map_err(|err| Fault::new(format!("chdir /: {err}")))?;
    Ok(())
}

/// Steps 5: the pseudo-filesystems and the `/dev` symlinks the app expects,
/// created only where the image has a place for them.
pub(crate) fn mount_late() -> Result<(), Fault> {
    make_dir("/dev/pts", 0o755)?;
    mount_fs(
        "devpts",
        "/dev/pts",
        "devpts",
        MountFlags::NOEXEC | MountFlags::NOSUID,
        Some("newinstance,ptmxmode=0666,mode=0620"),
    )?;

    make_dir("/dev/shm", 0o755)?;
    mount_fs(
        "tmpfs",
        "/dev/shm",
        "tmpfs",
        MountFlags::NOSUID | MountFlags::NODEV,
        Some("mode=1777"),
    )?;

    // `/run` is only mounted when the image has it: the app's filesystem is not
    // ours to invent mount points in.
    if Path::new("/run").is_dir() {
        mount_fs(
            "tmpfs",
            "/run",
            "tmpfs",
            MountFlags::NOSUID | MountFlags::NODEV,
            Some("mode=0755"),
        )?;
    }

    if !Path::new("/tmp").exists() {
        make_dir("/tmp", 0o1777)?;
    }

    for (link, target) in [
        ("/dev/fd", "/proc/self/fd"),
        ("/dev/stdin", "/proc/self/fd/0"),
        ("/dev/stdout", "/proc/self/fd/1"),
        ("/dev/stderr", "/proc/self/fd/2"),
    ] {
        if std::fs::symlink_metadata(link).is_err() {
            std::os::unix::fs::symlink(target, link)
                .map_err(|err| Fault::new(format!("symlink {link} -> {target}: {err}")))?;
        }
    }

    Ok(())
}

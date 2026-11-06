// SPDX-License-Identifier: AGPL-3.0-only
//! The loop-mounted XFS volume pool (engine-trait.md §3.16, ADR 0156).
//!
//! On a data directory whose filesystem does not reflink (ext4), the volume
//! directory is the mount point of a sparse XFS image, `volume-pool.img` in
//! the state directory, mounted through a loop device with `nodev,nosuid,noexec`.
//! Volume files and their snapshots live inside it, so `snapshot_volume` stays
//! a `FICLONE` reflink with no pause.
//!
//! The pool has no fstab entry and no unit: [`attach`] runs at every start-up,
//! before any volume call, and reuses a pool that is already mounted (an agent
//! restart while VMs run). An image that exists but does not mount is never
//! replaced by the empty directory underneath: [`VolumePool::Broken`] makes
//! every volume call `Unsupported` until an operator repairs it.
//!
//! Mounting and growing run as root on real hosts, so every failure path leaves
//! the image and its mount in a state the next start-up can reason about: a new
//! image that cannot mount is deleted, an image that pre-existed is kept.
//!
//! On a real node the agent runs in its own mount namespace (a slave of the
//! host's, from the systemd unit's sandboxing), where a mount it makes is
//! invisible on the host and dies with the namespace. So `mount`, `umount` and
//! `xfs_growfs` run as children of `nsenter -t 1 -m -- …`, the pool is looked
//! for in `/proc/1/mountinfo` (the host's table) and the node agent only
//! reuses a pool that is the image it created. This lives in [`host_argv`],
//! [`mount_entry_for`] and [`is_pool_image`].

use std::path::{Path, PathBuf};

use super::engine::{Inner, io_error};
use super::tools;
use crate::EngineError;
use crate::SnapshotMethod;

/// File name of the pool image, inside the engine's state directory (§3.16).
pub(super) const POOL_IMAGE: &str = "volume-pool.img";

/// Size of a new pool image: 2 GiB, sparse.
const INITIAL_BYTES: u64 = 2 << 30;

/// Room the pool keeps above twice its volumes' sizes: one snapshot of every
/// volume plus the filesystem's own metadata (§3.16).
const GROWTH_MARGIN: u64 = 1 << 30;

/// The XFS label of the pool image.
const POOL_LABEL: &str = "quoro-pool";

/// The mount options of the pool: a loop mount that runs no setuid binary, no
/// device node and no code, so a guest's volume data is never executable.
const MOUNT_OPTIONS: &str = "loop,nodev,nosuid,noexec";

/// The mount table of PID 1, the host's (§3.16).
const HOST_MOUNTINFO: &str = "/proc/1/mountinfo";

/// The mount table of this process's mount namespace.
const OWN_MOUNTINFO: &str = "/proc/self/mountinfo";

/// The reason a pool that is on the host but not here is reported broken
/// (§3.16): mounting it again would give the image two mounts.
const PROPAGATION_REASON: &str =
    "the volume pool is mounted on the host but not visible to the node agent (mount propagation)";

/// Where the pool stands on this start-up (§3.16).
pub(super) enum VolumePool {
    /// The volume directory is a plain directory: no pool on this node.
    Absent,
    /// The volume directory is the pool's mount point.
    Mounted {
        /// The sparse XFS image mounted there.
        image: PathBuf,
        /// The loop device the kernel mounted it through, when the mount
        /// source is one; `losetup -c` refreshes its capacity.
        loop_device: Option<PathBuf>,
    },
    /// A pool image exists but does not mount. It is never replaced by the
    /// empty directory underneath: every volume call is `Unsupported`.
    Broken {
        /// Why the pool could not be attached, for the error message.
        reason: String,
    },
}

/// What [`attach`] decided: the pool, and the snapshot method to use with it.
pub(super) struct Attachment {
    pub(super) pool: VolumePool,
    pub(super) method: SnapshotMethod,
}

/// One line of a mount table, with the fields the pool needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MountEntry {
    /// The mount point, with the kernel's octal escapes decoded.
    pub(super) mount_point: PathBuf,
    /// The filesystem type, for example `xfs`.
    pub(super) fstype: String,
    /// The mounted device, for example `/dev/loop3`.
    pub(super) source: String,
}

/// The facts the start-up decision is a function of (§3.16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StartupFacts {
    /// The fstype, when the volume directory is already a mount point.
    pub(super) mounted: Option<String>,
    /// Whether that mount is the pool's own image through a loop device.
    pub(super) pool_owned: bool,
    /// Whether `FICLONE` works in the volume directory itself.
    pub(super) native_reflink: bool,
    /// Whether `volume-pool.img` exists.
    pub(super) image_exists: bool,
    /// Whether the volume directory holds a volume made before the pool.
    pub(super) plain_volumes: bool,
    /// Whether every tool the pool needs is installed.
    pub(super) tools_present: bool,
}

/// What start-up does with the volume directory (§3.16).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StartupAction {
    /// The volume directory is the pool's own xfs mount: reuse it.
    ReusePool,
    /// It is a mount point of something else: no pool, `PauseCopy`.
    OtherMount,
    /// The directory itself reflinks: `Reflink` natively, no pool.
    NativeReflink,
    /// An image exists: mount it, `Broken` when that fails.
    MountExisting,
    /// No pool yet and no volume in the way: create and mount the image.
    CreatePool,
    /// Volumes were created before the pool: keep `PauseCopy`.
    KeepPlainVolumes,
    /// The pool's tools are missing: keep `PauseCopy`.
    NoTools,
}

/// The host programs the pool needs, resolved when the pool is used.
struct PoolTools {
    mkfs_xfs: PathBuf,
    xfs_growfs: PathBuf,
    mount: PathBuf,
    umount: PathBuf,
    losetup: PathBuf,
    /// `nsenter`, from util-linux; only needed when the agent has its own
    /// mount namespace (§3.16).
    nsenter: Option<PathBuf>,
}

impl PoolTools {
    /// The tools, or `Unsupported` naming the first missing required one.
    ///
    /// `nsenter` is optional here: a host where the agent shares PID 1's mount
    /// namespace needs no prefix, and [`Self::in_host`] reports the missing
    /// tool when one is required.
    fn discover() -> Result<PoolTools, EngineError> {
        Ok(PoolTools {
            mkfs_xfs: require("mkfs.xfs", "xfsprogs")?,
            xfs_growfs: require("xfs_growfs", "xfsprogs")?,
            mount: require("mount", "util-linux")?,
            umount: require("umount", "util-linux")?,
            losetup: require("losetup", "util-linux")?,
            nsenter: tools::find_optional("nsenter"),
        })
    }

    /// The program and arguments of one mount-namespace-sensitive tool (§3.16).
    fn in_host(
        &self,
        split: bool,
        tool: &Path,
        args: &[&str],
    ) -> Result<(PathBuf, Vec<String>), EngineError> {
        host_argv(split, self.nsenter.as_deref(), tool, args).ok_or_else(|| {
            EngineError::Unsupported("nsenter not found; install util-linux".to_owned())
        })
    }
}

/// One optional host tool, or `Unsupported` naming its package.
fn require(name: &str, package: &str) -> Result<PathBuf, EngineError> {
    tools::find_optional(name)
        .ok_or_else(|| EngineError::Unsupported(format!("{name} not found; install {package}")))
}

/// The program and arguments that run `tool args…` in the host's mount
/// namespace when this process has its own (§3.16).
///
/// `None` when the prefix is needed and `nsenter` is missing: running the tool
/// directly would mount where only this namespace sees it.
pub(super) fn host_argv(
    split: bool,
    nsenter: Option<&Path>,
    tool: &Path,
    args: &[&str],
) -> Option<(PathBuf, Vec<String>)> {
    let arguments = args.iter().map(|arg| (*arg).to_owned());
    if !split {
        return Some((tool.to_path_buf(), arguments.collect()));
    }
    let nsenter = nsenter?;
    let mut argv = vec![
        "-t".to_owned(),
        "1".to_owned(),
        "-m".to_owned(),
        "--".to_owned(),
    ];
    argv.push(tool.to_string_lossy().into_owned());
    argv.extend(arguments);
    Some((nsenter.to_path_buf(), argv))
}

/// True when the agent runs in its own mount namespace, so a mount it makes
/// would stay invisible to the host until it runs through `nsenter` (§3.16).
///
/// Both links unreadable means "assume shared": running the tools directly is
/// right in the host namespace and in a container, and a wrong guess here only
/// changes which namespace the mount lands in, never whether it is attempted.
fn host_namespace_split() -> bool {
    let host = std::fs::read_link("/proc/1/ns/mnt");
    let own = std::fs::read_link("/proc/self/ns/mnt");
    matches!((host, own), (Ok(host), Ok(own)) if host != own)
}

/// Attaches the volume pool, or decides not to (§3.16).
///
/// Never fails the start-up: a pool that cannot be created or mounted degrades
/// to `PauseCopy`, and one that exists but does not mount is reported as
/// [`VolumePool::Broken`] so no volume call touches the empty directory. A pool
/// the host already has but this namespace cannot see is broken rather than
/// mounted a second time.
pub(super) async fn attach(state_dir: &Path, dir: &Path) -> Attachment {
    let image = state_dir.join(POOL_IMAGE);
    let mounted = match mounted_pool(dir) {
        Ok(mounted) => mounted,
        Err(reason) => {
            eprintln!("quoro-engine: {reason}");
            return Attachment {
                pool: VolumePool::Broken { reason },
                method: SnapshotMethod::PauseCopy,
            };
        }
    };
    let native_reflink =
        mounted.is_none() && super::volume::probe_method(dir) == SnapshotMethod::Reflink;
    let facts = StartupFacts {
        mounted: mounted.as_ref().map(|entry| entry.fstype.clone()),
        pool_owned: mounted
            .as_ref()
            .is_some_and(|entry| is_pool_image(entry, &image)),
        native_reflink,
        image_exists: image.exists(),
        plain_volumes: has_volume_files(dir),
        tools_present: pool_tools_present(),
    };
    match startup_action(&facts) {
        StartupAction::ReusePool => {
            let method = super::volume::probe_method(dir);
            if method != SnapshotMethod::Reflink {
                eprintln!(
                    "quoro-engine: the volume pool does not reflink; volumes stay on pause_copy"
                );
            }
            Attachment {
                pool: VolumePool::Mounted {
                    image,
                    loop_device: mounted.as_ref().and_then(loop_device),
                },
                method,
            }
        }
        StartupAction::OtherMount => {
            eprintln!(
                "quoro-engine: {} is mounted with {} but is not the volume pool; volumes stay on pause_copy",
                dir.display(),
                facts.mounted.as_deref().unwrap_or("another filesystem")
            );
            Attachment {
                pool: VolumePool::Absent,
                method: SnapshotMethod::PauseCopy,
            }
        }
        StartupAction::NativeReflink => Attachment {
            pool: VolumePool::Absent,
            method: SnapshotMethod::Reflink,
        },
        StartupAction::MountExisting => mount_existing(image, dir).await,
        StartupAction::CreatePool => create(image, dir).await,
        StartupAction::KeepPlainVolumes => {
            eprintln!("quoro-engine: volumes created before the pool stay on pause_copy");
            Attachment {
                pool: VolumePool::Absent,
                method: SnapshotMethod::PauseCopy,
            }
        }
        StartupAction::NoTools => {
            eprintln!("quoro-engine: the xfs pool tools are missing; volumes stay on pause_copy");
            Attachment {
                pool: VolumePool::Absent,
                method: SnapshotMethod::PauseCopy,
            }
        }
    }
}

/// The start-up decision, as a pure function of [`StartupFacts`] (§3.16).
pub(super) fn startup_action(facts: &StartupFacts) -> StartupAction {
    if let Some(fstype) = &facts.mounted {
        return if fstype == "xfs" && facts.pool_owned {
            StartupAction::ReusePool
        } else {
            StartupAction::OtherMount
        };
    }
    if facts.native_reflink {
        return StartupAction::NativeReflink;
    }
    if facts.image_exists {
        return StartupAction::MountExisting;
    }
    if facts.plain_volumes {
        return StartupAction::KeepPlainVolumes;
    }
    if facts.tools_present {
        StartupAction::CreatePool
    } else {
        StartupAction::NoTools
    }
}

/// Mounts an image that already exists, and probes `FICLONE` inside it.
///
/// A mount that fails, or a pool that does not reflink, is
/// [`VolumePool::Broken`]: the image pre-existed, so it is kept for an operator
/// and no volume call may use the directory underneath it.
async fn mount_existing(image: PathBuf, dir: &Path) -> Attachment {
    let pool_tools = match PoolTools::discover() {
        Ok(pool_tools) => pool_tools,
        Err(err) => {
            let failure = reason(&err);
            eprintln!("quoro-engine: the volume pool does not mount: {failure}");
            return Attachment {
                pool: VolumePool::Broken { reason: failure },
                method: SnapshotMethod::PauseCopy,
            };
        }
    };
    let split = host_namespace_split();
    let failure = match mount_pool(&pool_tools, split, &image, dir).await {
        Ok(()) => {
            if super::volume::probe_method(dir) == SnapshotMethod::Reflink {
                return Attachment {
                    pool: VolumePool::Mounted {
                        image,
                        loop_device: pool_mount(dir).as_ref().and_then(loop_device),
                    },
                    method: SnapshotMethod::Reflink,
                };
            }
            // A pool that mounted but does not reflink is not trusted: unmount
            // it, so its contents are never used either.
            match unmount(&pool_tools, split, dir).await {
                Ok(()) => "the volume pool does not reflink".to_owned(),
                Err(err) => format!(
                    "the volume pool does not reflink and does not unmount: {}",
                    reason(&err)
                ),
            }
        }
        Err(err) => reason(&err),
    };
    eprintln!("quoro-engine: the volume pool does not mount: {failure}");
    Attachment {
        pool: VolumePool::Broken { reason: failure },
        method: SnapshotMethod::PauseCopy,
    }
}

/// Creates, formats and mounts a new pool image (§3.16).
async fn create(image: PathBuf, dir: &Path) -> Attachment {
    match create_pool(&image, dir).await {
        Ok(loop_device) => Attachment {
            pool: VolumePool::Mounted { image, loop_device },
            method: SnapshotMethod::Reflink,
        },
        Err(err) => {
            eprintln!(
                "quoro-engine: cannot create the volume pool: {}",
                reason(&err)
            );
            Attachment {
                pool: VolumePool::Absent,
                method: SnapshotMethod::PauseCopy,
            }
        }
    }
}

/// The work of [`create`], with the image deleted on every failure.
async fn create_pool(image: &Path, dir: &Path) -> Result<Option<PathBuf>, EngineError> {
    let pool_tools = PoolTools::discover()?;
    let split = host_namespace_split();
    create_image(image)?;
    let image_text = utf8(image)?;
    if let Err(err) = tools::run(
        &pool_tools.mkfs_xfs,
        &["-q", "-f", "-m", "reflink=1", "-L", POOL_LABEL, &image_text],
    )
    .await
    {
        let _ = std::fs::remove_file(image);
        return Err(err);
    }
    if let Err(err) = mount_pool(&pool_tools, split, image, dir).await {
        let _ = std::fs::remove_file(image);
        return Err(err);
    }
    if super::volume::probe_method(dir) == SnapshotMethod::Reflink {
        return Ok(pool_mount(dir).as_ref().and_then(loop_device));
    }
    match unmount(&pool_tools, split, dir).await {
        Ok(()) => {
            let _ = std::fs::remove_file(image);
            Err(EngineError::Io(
                "the new volume pool does not reflink".to_owned(),
            ))
        }
        // The image is still mounted, so it can never be deleted from under
        // the loop device; the next start-up sees it and reports it broken.
        Err(err) => Err(EngineError::Io(format!(
            "the new volume pool does not reflink and does not unmount: {}",
            reason(&err)
        ))),
    }
}

/// Creates the sparse pool image at its initial size.
fn create_image(image: &Path) -> Result<(), EngineError> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(image)
        .map_err(|err| io_error(image, &err))?;
    if let Err(err) = file.set_len(INITIAL_BYTES) {
        drop(file);
        let _ = std::fs::remove_file(image);
        return Err(if err.kind() == std::io::ErrorKind::StorageFull {
            EngineError::ResourceExhausted(format!(
                "{}: no space left for the {INITIAL_BYTES} byte volume pool",
                image.display()
            ))
        } else {
            io_error(image, &err)
        });
    }
    Ok(())
}

/// Mounts `image` on `dir` through a loop device with [`MOUNT_OPTIONS`], on
/// the host when the agent has its own mount namespace (§3.16).
async fn mount_pool(
    pool_tools: &PoolTools,
    split: bool,
    image: &Path,
    dir: &Path,
) -> Result<(), EngineError> {
    let image_text = utf8(image)?;
    let dir_text = utf8(dir)?;
    let args = ["-t", "xfs", "-o", MOUNT_OPTIONS, &image_text, &dir_text];
    run_in_host(pool_tools, split, &pool_tools.mount, &args)
        .await
        .map(|_| ())
}

/// Unmounts `dir` on the host; the auto-clear loop device goes with it (§3.16).
async fn unmount(pool_tools: &PoolTools, split: bool, dir: &Path) -> Result<(), EngineError> {
    let dir_text = utf8(dir)?;
    run_in_host(pool_tools, split, &pool_tools.umount, &[&dir_text])
        .await
        .map(|_| ())
}

/// Runs one mount-namespace-sensitive tool, through `nsenter` when needed.
async fn run_in_host(
    pool_tools: &PoolTools,
    split: bool,
    tool: &Path,
    args: &[&str],
) -> Result<String, EngineError> {
    let (program, argv) = pool_tools.in_host(split, tool, args)?;
    let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
    tools::run(&program, &argv).await
}

/// The growth target of a pool holding `sum` bytes of volumes and about to
/// hold `new_bytes` more: twice their total plus [`GROWTH_MARGIN`] (§3.16).
pub(super) fn growth_target(sum: u64, new_bytes: u64) -> Option<u64> {
    sum.checked_add(new_bytes)?
        .checked_mul(2)?
        .checked_add(GROWTH_MARGIN)
}

/// The device to refresh with `losetup -c`, when the mount source is a loop.
fn loop_device(entry: &MountEntry) -> Option<PathBuf> {
    let name = entry.source.strip_prefix("/dev/")?;
    let number = name.strip_prefix("loop")?;
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(PathBuf::from(&entry.source))
}

/// True when `entry` mounts the pool's own image through a loop device
/// (§3.16).
///
/// An xfs an operator mounted on the volume directory is not the pool: it must
/// not receive volumes, and its mount must never be reused or unmounted.
fn is_pool_image(entry: &MountEntry, image: &Path) -> bool {
    let Some(device) = loop_device(entry) else {
        return false;
    };
    let Some(name) = device.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let backing = std::fs::read_to_string(format!("/sys/block/{name}/loop/backing_file"));
    let Ok(backing) = backing else {
        return false;
    };
    backing_file_is(&backing, &canonical(image))
}

/// True when the kernel's recorded backing file, which sysfs prints with a
/// trailing newline, is `image` (§3.16).
pub(super) fn backing_file_is(backing: &str, image: &Path) -> bool {
    let backing = backing.trim();
    !backing.is_empty() && Path::new(backing) == image
}

/// `path` with symlinks resolved, or itself when it does not exist.
fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// The engine's own error text, without the variant's prefix.
fn reason(err: &EngineError) -> String {
    match err {
        EngineError::Conflict(text)
        | EngineError::Unsupported(text)
        | EngineError::ResourceExhausted(text)
        | EngineError::Image(text)
        | EngineError::Hypervisor(text)
        | EngineError::Io(text) => text.clone(),
        other => other.to_string(),
    }
}

/// One path as an argument to a host tool.
fn utf8(path: &Path) -> Result<String, EngineError> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        EngineError::Conflict(format!("pool path {} is not valid UTF-8", path.display()))
    })
}

/// The mount on the volume directory, from the host's table and this
/// namespace's (§3.16).
///
/// The host's `/proc/1/mountinfo` is authoritative: a pool it lists and this
/// namespace does not means mount propagation is broken, and mounting again
/// would give the image two mounts. When the host's table is unreadable the
/// pool is decided from this namespace's alone.
fn mounted_pool(dir: &Path) -> Result<Option<MountEntry>, String> {
    let dir = canonical(dir);
    match (
        read_mount_entries(HOST_MOUNTINFO),
        read_mount_entries(OWN_MOUNTINFO),
    ) {
        (Some(host), Some(own)) => {
            match (mount_entry_for(&host, &dir), mount_entry_for(&own, &dir)) {
                (Some(_), None) => Err(PROPAGATION_REASON.to_owned()),
                (Some(host), Some(_)) => Ok(Some(host.clone())),
                (None, Some(own)) => Ok(Some(own.clone())),
                (None, None) => Ok(None),
            }
        }
        (Some(host), None) => Ok(mount_entry_for(&host, &dir).cloned()),
        (None, Some(own)) => Ok(mount_entry_for(&own, &dir).cloned()),
        (None, None) => Ok(None),
    }
}

/// The pool mount as the tables see it right after the engine attached it.
fn pool_mount(dir: &Path) -> Option<MountEntry> {
    mounted_pool(dir).ok().flatten()
}

/// The first table that lists a mount on `dir`: the host's, then this
/// namespace's.
fn any_mount(dir: &Path) -> Option<MountEntry> {
    let dir = canonical(dir);
    [HOST_MOUNTINFO, OWN_MOUNTINFO]
        .into_iter()
        .find_map(|path| {
            read_mount_entries(path).and_then(|entries| mount_entry_for(&entries, &dir).cloned())
        })
}

/// One mount table, when it is readable.
fn read_mount_entries(path: &str) -> Option<Vec<MountEntry>> {
    std::fs::read_to_string(path)
        .ok()
        .map(|text| parse_mountinfo(&text))
}

/// Whether every tool the pool needs is installed, `nsenter` included when the
/// agent has its own mount namespace (§3.16).
fn pool_tools_present() -> bool {
    PoolTools::discover().is_ok_and(|tools| !host_namespace_split() || tools.nsenter.is_some())
}

/// The topmost mount on `dir` in `entries`.
///
/// The last line for a mount point is the one on top, so the search runs
/// backwards (§3.16).
pub(super) fn mount_entry_for<'a>(entries: &'a [MountEntry], dir: &Path) -> Option<&'a MountEntry> {
    entries.iter().rev().find(|entry| entry.mount_point == dir)
}

/// Every usable line of a mount table.
pub(super) fn parse_mountinfo(text: &str) -> Vec<MountEntry> {
    text.lines().filter_map(parse_mountinfo_line).collect()
}

/// One line of a mount table.
fn parse_mountinfo_line(line: &str) -> Option<MountEntry> {
    // `36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=…`
    let (left, right) = line.split_once(" - ")?;
    let fields: Vec<&str> = left.split_whitespace().collect();
    let rest: Vec<&str> = right.split_whitespace().collect();
    if fields.len() < 5 || rest.len() < 2 {
        return None;
    }
    Some(MountEntry {
        mount_point: PathBuf::from(unescape(fields[4])),
        fstype: rest[0].to_owned(),
        source: unescape(rest[1]),
    })
}

/// Decodes the kernel's octal escapes (`\040` for a space) in a mount field.
fn unescape(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 4 <= bytes.len()
            && let Some(byte) = octal(&bytes[index + 1..index + 4])
        {
            out.push(byte);
            index += 4;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Three octal digits as one byte.
fn octal(digits: &[u8]) -> Option<u8> {
    let mut value = 0_u32;
    for digit in digits {
        if !(b'0'..=b'7').contains(digit) {
            return None;
        }
        value = value * 8 + u32::from(digit - b'0');
    }
    u8::try_from(value).ok()
}

/// True for a volume file name: `vol_` and not one of its side files.
fn is_volume_file(name: &str) -> bool {
    name.starts_with("vol_")
        && !name.ends_with(".snapshot")
        && !name.ends_with(super::warm::STAGING_SUFFIX)
        && !name.ends_with(".tmp")
}

/// True when `dir` holds a volume file, which keeps the pool from being
/// created (§3.16: volumes created before the pool stay on `PauseCopy`).
fn has_volume_files(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries
        .flatten()
        .any(|entry| entry.file_name().to_str().is_some_and(is_volume_file))
}

/// The sum of the sizes of every volume file in `dir`.
fn volume_bytes(dir: &Path) -> Result<u64, EngineError> {
    let entries = std::fs::read_dir(dir).map_err(|err| io_error(dir, &err))?;
    let mut sum = 0_u64;
    for entry in entries {
        let entry = entry.map_err(|err| io_error(dir, &err))?;
        let name = entry.file_name();
        if !name.to_str().is_some_and(is_volume_file) {
            continue;
        }
        let metadata = entry
            .metadata()
            .map_err(|err| io_error(&entry.path(), &err))?;
        if metadata.is_file() {
            sum = sum.saturating_add(metadata.len());
        }
    }
    Ok(sum)
}

impl Inner {
    /// Fails when a pool image exists but does not mount (§3.16): every volume
    /// call is then `Unsupported`, so the directory underneath is never used.
    pub(super) fn volume_pool_error(&self) -> Result<(), EngineError> {
        match &self.pool {
            VolumePool::Broken { reason } => Err(EngineError::Unsupported(format!(
                "the volume pool does not mount: {reason}"
            ))),
            VolumePool::Absent | VolumePool::Mounted { .. } => Ok(()),
        }
    }

    /// Grows the pool so it can hold `new_bytes` more of volume data (§3.16).
    ///
    /// Called while the caller holds `pool_grow`, so two creates never race on
    /// the sum. Never shrinks, and checks the host filesystem's free bytes
    /// first: the image is sparse, but its data must fit when it is written.
    ///
    /// # Errors
    ///
    /// `ResourceExhausted` when the host cannot commit `new_bytes`,
    /// `Unsupported` for a broken pool, and `Io` naming the failed step
    /// otherwise.
    pub(super) async fn grow_pool_for(&self, new_bytes: u64) -> Result<(), EngineError> {
        self.volume_pool_error()?;
        let VolumePool::Mounted { image, loop_device } = &self.pool else {
            return Ok(());
        };
        let dir = self.volumes_dir();
        let Some(target) = growth_target(volume_bytes(&dir)?, new_bytes) else {
            return Err(EngineError::ResourceExhausted(
                "the volume pool would grow past 2^64 bytes".to_owned(),
            ));
        };
        let current = std::fs::metadata(image)
            .map_err(|err| io_error(image, &err))?
            .len();
        if target <= current {
            return Ok(());
        }
        let loop_device = loop_device.as_deref().ok_or_else(|| {
            EngineError::Io("the volume pool is not mounted through a loop device".to_owned())
        })?;
        let pool_tools = PoolTools::discover()?;
        let available = super::volume::available_bytes(&self.config.state_dir)?;
        let needed = super::volume::METADATA_MARGIN.saturating_add(new_bytes);
        if available < needed {
            return Err(EngineError::ResourceExhausted(format!(
                "{} has {available} bytes free, {needed} are needed",
                self.config.state_dir.display()
            )));
        }

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(image)
            .map_err(|err| io_error(image, &err))?;
        if let Err(err) = file.set_len(target) {
            return Err(if err.kind() == std::io::ErrorKind::StorageFull {
                EngineError::ResourceExhausted(format!(
                    "{}: no space left to grow the volume pool to {target} bytes",
                    image.display()
                ))
            } else {
                io_error(image, &err)
            });
        }
        drop(file);
        // `losetup -c` acts on the device node, not on a mount: no prefix.
        let device_text = utf8(loop_device)?;
        tools::run(&pool_tools.losetup, &["-c", &device_text]).await?;
        let dir_text = utf8(&dir)?;
        run_in_host(
            &pool_tools,
            host_namespace_split(),
            &pool_tools.xfs_growfs,
            &[&dir_text],
        )
        .await?;
        Ok(())
    }
}

/// Detaches the pool, after checking that it holds no volume data (§3.16).
///
/// The `umount` runs through `nsenter` when the agent has its own mount
/// namespace, so the host's table is the one that changes.
pub(super) fn detach(state_dir: &Path) -> Result<(), EngineError> {
    let dir = canonical(&state_dir.join("volumes"));
    let Some(entry) = any_mount(&dir) else {
        return Ok(());
    };
    if entry.fstype != "xfs" {
        return Err(EngineError::Conflict(format!(
            "{} is mounted with {}, not the volume pool",
            dir.display(),
            entry.fstype
        )));
    }
    if let Some(name) = first_volume_file(&dir) {
        return Err(EngineError::Conflict(format!(
            "volume {name} still lives in the volume pool"
        )));
    }
    let umount = tools::find_optional("umount").ok_or_else(|| {
        EngineError::Unsupported("umount not found; install util-linux".to_owned())
    })?;
    let dir_text = utf8(&dir)?;
    let (program, argv) = host_argv(
        host_namespace_split(),
        tools::find_optional("nsenter").as_deref(),
        &umount,
        &[&dir_text],
    )
    .ok_or_else(|| EngineError::Unsupported("nsenter not found; install util-linux".to_owned()))?;
    let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
    tools::run_blocking(&program, &argv)?;
    if any_mount(&dir).is_some() {
        return Err(EngineError::Io(format!(
            "{} is still a mount point after umount",
            dir.display()
        )));
    }
    Ok(())
}

/// The first name in `dir` that carries volume data, volume or snapshot.
fn first_volume_file(dir: &Path) -> Option<String> {
    let entries = std::fs::read_dir(dir).ok()?;
    entries.flatten().find_map(|entry| {
        let name = entry.file_name().to_string_lossy().into_owned();
        name.starts_with("vol_").then_some(name)
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        GROWTH_MARGIN, MountEntry, StartupAction, StartupFacts, backing_file_is, growth_target,
        host_argv, is_volume_file, loop_device, mount_entry_for, parse_mountinfo, startup_action,
        unescape,
    };

    const GIB: u64 = 1 << 30;

    fn facts(mounted: Option<&str>) -> StartupFacts {
        StartupFacts {
            mounted: mounted.map(str::to_owned),
            pool_owned: true,
            native_reflink: false,
            image_exists: false,
            plain_volumes: false,
            tools_present: true,
        }
    }

    #[test]
    fn mountinfo_decodes_escaped_mount_points() {
        assert_eq!(unescape("/var/lib/quoro/volumes"), "/var/lib/quoro/volumes");
        assert_eq!(unescape("/mnt/with\\040space"), "/mnt/with space");
        assert_eq!(unescape("/mnt/with\\011tab"), "/mnt/with\ttab");
        assert_eq!(unescape("/mnt/with\\134backslash"), "/mnt/with\\backslash");
        // An escape that is not octal stays literal, so a path is never lost.
        assert_eq!(unescape("/mnt/with\\q"), "/mnt/with\\q");
        assert_eq!(unescape("/mnt/with\\77"), "/mnt/with\\77");
    }

    #[test]
    fn mountinfo_picks_the_topmost_entry_of_the_volume_directory() {
        let text = "\
25 1 252:1 / / rw,relatime - ext4 /dev/vda1 rw
36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw
40 35 0:32 / /var/lib/quoro/volumes rw,nosuid,nodev - xfs /dev/loop3 rw,noexec
41 35 0:33 / /var/lib/quoro/volumes\\040old rw - xfs /dev/loop4 rw
not a mount line
49 48 0:1 / /var/lib/quoro/volumes rw - ext4 /dev/vdb rw
";
        let entries = parse_mountinfo(text);
        assert_eq!(entries.len(), 5, "{entries:?}");
        let dir = Path::new("/var/lib/quoro/volumes");
        // The last line for a mount point is the one on top.
        let entry = mount_entry_for(&entries, dir).expect("the volume directory is mounted");
        assert_eq!(entry.fstype, "ext4");
        assert_eq!(entry.source, "/dev/vdb");
        assert_eq!(entry.mount_point, PathBuf::from("/var/lib/quoro/volumes"));
        let escaped = mount_entry_for(&entries, Path::new("/var/lib/quoro/volumes old"))
            .expect("the escaped line matches its decoded path");
        assert_eq!(escaped.source, "/dev/loop4");
        assert!(mount_entry_for(&entries, Path::new("/var/lib/quoro")).is_none());
    }

    #[test]
    fn mountinfo_returns_xfs_when_the_pool_is_on_top() {
        let text = "\
49 48 0:1 / /var/lib/quoro/volumes rw - ext4 /dev/vdb rw
50 49 0:32 / /var/lib/quoro/volumes rw,nosuid,nodev - xfs /dev/loop3 rw,noexec
";
        let entries = parse_mountinfo(text);
        let entry = mount_entry_for(&entries, Path::new("/var/lib/quoro/volumes"))
            .expect("the volume directory is mounted");
        assert_eq!(entry.fstype, "xfs");
        assert_eq!(entry.source, "/dev/loop3");
    }

    #[test]
    fn mountinfo_skips_incomplete_lines() {
        let entries = parse_mountinfo("40 35 0:32 / /srv rw - xfs\n");
        assert!(entries.is_empty(), "a line without a source is unusable");
        assert!(parse_mountinfo("").is_empty());
    }

    #[test]
    fn pool_growth_targets_twice_the_volumes_plus_one_gibibyte() {
        // 2 × (no volume yet + the new 1 GiB) + 1 GiB.
        assert_eq!(growth_target(0, GIB), Some(3 * GIB));
        assert_eq!(growth_target(0, 0), Some(GROWTH_MARGIN));
        assert_eq!(growth_target(3 * GIB, 2 * GIB), Some(11 * GIB));
        assert_eq!(growth_target(u64::MAX, 1), None);
    }

    #[test]
    fn startup_decision_follows_the_contract_order() {
        // A mount point is decided first: only the pool's own xfs is reused,
        // even when the directory would reflink or an image exists.
        let mut already = facts(Some("xfs"));
        already.native_reflink = true;
        already.image_exists = true;
        assert_eq!(startup_action(&already), StartupAction::ReusePool);

        // Another xfs on the volume directory is not the pool.
        let mut foreign_xfs = facts(Some("xfs"));
        foreign_xfs.pool_owned = false;
        assert_eq!(startup_action(&foreign_xfs), StartupAction::OtherMount);

        let mut other = facts(Some("ext4"));
        other.native_reflink = true;
        assert_eq!(startup_action(&other), StartupAction::OtherMount);

        // Then the native probe wins over an existing image.
        let mut native = facts(None);
        native.native_reflink = true;
        native.image_exists = true;
        assert_eq!(startup_action(&native), StartupAction::NativeReflink);

        // Then an existing image is mounted, whatever sits in the directory.
        let mut image = facts(None);
        image.image_exists = true;
        image.plain_volumes = true;
        assert_eq!(startup_action(&image), StartupAction::MountExisting);

        // Volumes created before the pool keep pause_copy.
        let mut plain = facts(None);
        plain.plain_volumes = true;
        assert_eq!(startup_action(&plain), StartupAction::KeepPlainVolumes);

        // An empty directory gets a pool only when every tool is installed.
        assert_eq!(startup_action(&facts(None)), StartupAction::CreatePool);
        let mut no_tools = facts(None);
        no_tools.tools_present = false;
        assert_eq!(startup_action(&no_tools), StartupAction::NoTools);
    }

    #[test]
    fn host_argv_prefixes_only_when_the_namespaces_differ() {
        let nsenter = Path::new("/usr/bin/nsenter");
        let mount = Path::new("/usr/bin/mount");
        let args = ["-t", "xfs", "-o", "loop", "/img", "/vol"];

        let direct = host_argv(false, Some(nsenter), mount, &args).expect("a direct call");
        assert_eq!(direct.0, PathBuf::from("/usr/bin/mount"));
        assert_eq!(direct.1, vec!["-t", "xfs", "-o", "loop", "/img", "/vol"]);

        let nested = host_argv(true, Some(nsenter), mount, &args).expect("a nested call");
        assert_eq!(nested.0, PathBuf::from("/usr/bin/nsenter"));
        assert_eq!(
            nested.1,
            vec![
                "-t",
                "1",
                "-m",
                "--",
                "/usr/bin/mount",
                "-t",
                "xfs",
                "-o",
                "loop",
                "/img",
                "/vol"
            ]
        );

        // A direct call never needs `nsenter`; a nested one cannot run without
        // it, because the mount would stay in this namespace.
        assert!(host_argv(false, None, mount, &args).is_some());
        assert!(host_argv(true, None, mount, &args).is_none());
    }

    #[test]
    fn backing_files_must_name_the_pool_image() {
        let image = Path::new("/var/lib/quoro/volume-pool.img");
        assert!(backing_file_is("/var/lib/quoro/volume-pool.img\n", image));
        assert!(backing_file_is(
            "  /var/lib/quoro/volume-pool.img  \n",
            image
        ));
        assert!(!backing_file_is("/var/lib/quoro/other.img", image));
        assert!(!backing_file_is("", image));
        assert!(!backing_file_is("\n", image));
    }

    #[test]
    fn only_loop_devices_name_a_pool_device() {
        let entry = |source: &str| MountEntry {
            mount_point: PathBuf::from("/var/lib/quoro/volumes"),
            fstype: "xfs".to_owned(),
            source: source.to_owned(),
        };
        assert_eq!(
            loop_device(&entry("/dev/loop3")),
            Some(PathBuf::from("/dev/loop3"))
        );
        assert_eq!(
            loop_device(&entry("/dev/loop12")),
            Some(PathBuf::from("/dev/loop12"))
        );
        assert!(loop_device(&entry("/dev/vdb")).is_none());
        assert!(loop_device(&entry("/dev/loop")).is_none());
        assert!(loop_device(&entry("/dev/loopx3")).is_none());
        assert!(loop_device(&entry("/dev/loop3/loop4")).is_none());
    }

    #[test]
    fn volume_file_names_exclude_snapshots_and_temporaries() {
        assert!(is_volume_file("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF"));
        assert!(!is_volume_file("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.snapshot"));
        assert!(!is_volume_file("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.tmp"));
        assert!(!is_volume_file("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.staging"));
        assert!(!is_volume_file(
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.19a2b3c4d5e-0.snapshot"
        ));
        assert!(!is_volume_file("volume-pool.img"));
        assert!(!is_volume_file(".probe-1-source"));
    }

    #[test]
    fn mount_entries_compare_decoded_paths() {
        let entry = MountEntry {
            mount_point: PathBuf::from("/var/lib/quoro/volumes"),
            fstype: "xfs".to_owned(),
            source: "/dev/loop7".to_owned(),
        };
        assert!(mount_entry_for(std::slice::from_ref(&entry), &entry.mount_point).is_some());
        assert!(mount_entry_for(&[entry], Path::new("/var/lib/quoro")).is_none());
    }
}

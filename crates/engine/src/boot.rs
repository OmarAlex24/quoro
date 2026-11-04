// SPDX-License-Identifier: AGPL-3.0-only
//! Per-VM boot artifacts.
//!
//! - The **initramfs**: a `newc` cpio archive with `/init` (the static
//!   `quoro-init` binary), `/dev` and `/dev/console`. The kernel runs `/init`
//!   from it; the init mounts the read-only rootfs and the data disk as an
//!   overlay and switches into it. The image cache therefore never contains
//!   the init, and an init upgrade does not invalidate prepared images.
//! - The **config disk**: a raw file of [`quoro_init::CONFIG_DISK_LEN`] bytes,
//!   the encoded [`quoro_init::GuestConfig`] at offset 0 and the status record
//!   region at [`quoro_init::STATUS_OFFSET`].
//! - The **data disk**: a sparse ext4 file of `disk_gb`, the overlay's upper
//!   layer.
//!
//! INTERFACE FROZEN BY THE SUB-ORCHESTRATOR: keep the public names and
//! signatures.

use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use quoro_init::{
    AppStatus, CONFIG_DISK_LEN, CONFIG_REGION_LEN, GuestConfig, STATUS_OFFSET, STATUS_REGION_LEN,
    decode_status,
};

use crate::EngineError;

/// The kernel command line every microVM boots with.
///
/// One serial console with kernel-log level 4, no panic timeout (the VMM owns
/// the machine's lifetime, not the guest kernel) and the initramfs's `/init`
/// as PID 1.
#[must_use]
pub fn kernel_cmdline() -> String {
    "console=ttyS0 loglevel=4 panic=-1 rdinit=/init".to_owned()
}

/// Writes the initramfs for `init_binary` to `out` (atomically: temp file in
/// the same directory, then rename).
///
/// The archive is byte-for-byte reproducible for the same binary: fixed
/// inode numbers, mode bits, zero uid/gid and mtime, and no padding beyond the
/// format's 4-byte alignment.
///
/// # Errors
///
/// `Io` when the binary cannot be read or the archive cannot be written,
/// `Conflict` when `init_binary` is not a regular file or is empty.
pub fn write_initramfs(init_binary: &Path, out: &Path) -> Result<(), EngineError> {
    let size = init_binary_size(init_binary)?;
    let (tmp, mut file) = create_temp(out)?;

    let result = write_archive(&mut file, init_binary, size);
    match result {
        Ok(()) => finish(file, &tmp, out),
        Err(err) => {
            drop(file);
            let _ = std::fs::remove_file(&tmp);
            Err(err)
        }
    }
}

/// Checks that `init_binary` is a non-empty regular file and returns its size.
fn init_binary_size(init_binary: &Path) -> Result<u32, EngineError> {
    let metadata: Metadata =
        std::fs::metadata(init_binary).map_err(|err| io_error(init_binary, &err))?;
    if !metadata.is_file() {
        return Err(EngineError::Conflict(format!(
            "init binary {} is not a regular file",
            init_binary.display()
        )));
    }
    let len = metadata.len();
    if len == 0 {
        return Err(EngineError::Conflict(format!(
            "init binary {} is empty",
            init_binary.display()
        )));
    }
    u32::try_from(len).map_err(|_| {
        EngineError::Conflict(format!(
            "init binary {} is {} bytes, too large for one cpio newc entry",
            init_binary.display(),
            len
        ))
    })
}

/// One `newc` entry header, before its data.
struct Entry<'a> {
    ino: u32,
    mode: u32,
    nlink: u32,
    /// Device numbers for a character or block device; `(0, 0)` otherwise.
    rdev: (u32, u32),
    /// Data length the following bytes carry.
    size: u32,
    name: &'a str,
}

/// Writes the whole `newc` archive: `dev`, `dev/console`, `init`, trailer.
fn write_archive(file: &mut File, init_binary: &Path, size: u32) -> Result<(), EngineError> {
    let mut archive = ArchiveWriter::new(file);

    archive
        .entry(&Entry {
            ino: 1,
            mode: 0o040_755,
            nlink: 2,
            rdev: (0, 0),
            size: 0,
            name: "dev",
        })
        .map_err(|err| io_error(init_binary, &err))?;
    archive
        .entry(&Entry {
            ino: 2,
            mode: 0o020_600,
            nlink: 1,
            rdev: (5, 1),
            size: 0,
            name: "dev/console",
        })
        .map_err(|err| io_error(init_binary, &err))?;

    archive
        .entry(&Entry {
            ino: 3,
            mode: 0o100_755,
            nlink: 1,
            rdev: (0, 0),
            size,
            name: "init",
        })
        .map_err(|err| io_error(init_binary, &err))?;
    let mut reader = File::open(init_binary).map_err(|err| io_error(init_binary, &err))?;
    archive
        .copy_from(&mut reader, u64::from(size))
        .map_err(|err| io_error(init_binary, &err))?;
    archive.pad().map_err(|err| io_error(init_binary, &err))?;

    archive
        .trailer()
        .map_err(|err| io_error(init_binary, &err))?;
    Ok(())
}

/// A `newc` cpio writer over one file, tracking the archive offset so every
/// header and every data block starts on a 4-byte boundary.
struct ArchiveWriter<'a> {
    file: &'a mut File,
    pos: u64,
}

impl<'a> ArchiveWriter<'a> {
    fn new(file: &'a mut File) -> Self {
        Self { file, pos: 0 }
    }

    /// Writes one entry header and its name with the padding that follows it.
    /// The entry's data is written by [`Self::copy_from`] afterwards.
    fn entry(&mut self, entry: &Entry<'_>) -> io::Result<()> {
        let namesize = entry.name.len() + 1;
        let fields = [
            entry.ino,
            entry.mode,
            0, // uid
            0, // gid
            entry.nlink,
            0, // mtime
            entry.size,
            0, // devmajor
            0, // devminor
            entry.rdev.0,
            entry.rdev.1,
            u32::try_from(namesize).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "entry name is too long")
            })?,
            0, // check
        ];
        let mut header = Vec::with_capacity(110 + namesize + 3);
        header.extend_from_slice(b"070701");
        for field in fields {
            header.extend_from_slice(format!("{field:08x}").as_bytes());
        }
        header.extend_from_slice(entry.name.as_bytes());
        header.push(0);
        self.write_all(&header)?;
        self.pad()
    }

    /// Copies exactly `len` bytes from `reader` as the current entry's data,
    /// then pads the data block to a multiple of 4.
    fn copy_from(&mut self, reader: &mut File, len: u64) -> io::Result<()> {
        let mut buffer = [0_u8; 64 * 1024];
        let mut remaining = len;
        while remaining > 0 {
            let want = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = reader.read(&mut buffer[..want])?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "init binary shrank while it was being archived",
                ));
            }
            self.write_all(&buffer[..read])?;
            remaining -= read as u64;
        }
        Ok(())
    }

    /// The final `TRAILER!!!` entry; all header fields are zero except the
    /// name size.
    fn trailer(&mut self) -> io::Result<()> {
        self.entry(&Entry {
            ino: 0,
            mode: 0,
            nlink: 0,
            rdev: (0, 0),
            size: 0,
            name: "TRAILER!!!",
        })
    }

    /// Pads the archive up to a multiple of 4 bytes.
    fn pad(&mut self) -> io::Result<()> {
        let misalignment = self.pos % 4;
        if misalignment == 0 {
            return Ok(());
        }
        let padding = [0_u8; 3];
        self.write_all(&padding[..usize::try_from(4 - misalignment).unwrap_or(3)])
    }

    fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data)?;
        self.pos += data.len() as u64;
        Ok(())
    }
}

/// Creates the config disk at `path` with `config` encoded at offset 0 and an
/// all-zero status region. Replaces an existing file atomically.
///
/// # Errors
///
/// `Conflict` when the configuration cannot be encoded (it names the field),
/// `Io` on write failure.
pub fn write_config_disk(path: &Path, config: &GuestConfig) -> Result<(), EngineError> {
    let encoded = config
        .encode()
        .map_err(|err| EngineError::Conflict(format!("invalid guest configuration: {err}")))?;
    if encoded.len() as u64 > CONFIG_REGION_LEN {
        return Err(EngineError::Conflict(format!(
            "invalid guest configuration: {} encoded bytes do not fit the {CONFIG_REGION_LEN}-byte region",
            encoded.len()
        )));
    }

    let (tmp, mut file) = create_temp(path)?;
    let result = write_config_bytes(&mut file, &encoded);
    match result {
        Ok(()) => finish(file, &tmp, path),
        Err(err) => {
            drop(file);
            let _ = std::fs::remove_file(&tmp);
            Err(io_error(&tmp, &err))
        }
    }
}

/// Writes the encoded configuration and zero padding up to the disk length.
fn write_config_bytes(file: &mut File, encoded: &[u8]) -> io::Result<()> {
    file.write_all(encoded)?;
    write_zeros(file, CONFIG_DISK_LEN - encoded.len() as u64)
}

/// Zeroes the status region of an existing config disk, before every boot.
///
/// # Errors
///
/// `Io` when the file is missing or cannot be written.
pub fn reset_status(path: &Path) -> Result<(), EngineError> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|err| io_error(path, &err))?;
    write_zeros_at(&file, STATUS_OFFSET, STATUS_REGION_LEN).map_err(|err| io_error(path, &err))?;
    file.sync_all().map_err(|err| io_error(path, &err))
}

/// Reads the status record the init left on the config disk, if any.
///
/// An all-zero region (a freshly written or reset disk) is `Ok(None)`.
///
/// # Errors
///
/// `Io` when the file cannot be read, `Hypervisor` when the region holds a
/// malformed record.
pub fn read_status(path: &Path) -> Result<Option<AppStatus>, EngineError> {
    let file = File::open(path).map_err(|err| io_error(path, &err))?;
    let mut region = vec![0_u8; usize::try_from(STATUS_REGION_LEN).unwrap_or(4096)];
    file.read_exact_at(&mut region, STATUS_OFFSET)
        .map_err(|err| io_error(path, &err))?;
    decode_status(&region).map_err(|err| {
        EngineError::Hypervisor(format!("malformed status record on the config disk: {err}"))
    })
}

/// Creates a sparse `size_gb` GiB file at `path` and formats it ext4 with
/// `mkfs_ext4` (label `quoro-data`). Fails if `path` exists.
///
/// The format is lazy (`lazy_itable_init`, `lazy_journal_init`) and skips the
/// discard pass: the guest completes it in the background, so creating a disk
/// is a metadata operation, not a write of `size_gb` GiB.
///
/// # Errors
///
/// `Conflict` for a zero size or an existing file, `Io` when the file cannot
/// be created or `mkfs.ext4` fails (its stderr is in the message),
/// `ResourceExhausted` on `ENOSPC`.
pub fn create_data_disk(mkfs_ext4: &Path, path: &Path, size_gb: u32) -> Result<(), EngineError> {
    if size_gb == 0 {
        return Err(EngineError::Conflict(
            "data disk size must be at least 1 GiB".to_owned(),
        ));
    }

    let file = File::create_new(path).map_err(|err| {
        if err.kind() == io::ErrorKind::AlreadyExists {
            EngineError::Conflict(format!("data disk {} already exists", path.display()))
        } else {
            io_error(path, &err)
        }
    })?;
    let len = u64::from(size_gb) << 30;
    if let Err(err) = file.set_len(len) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(if is_no_space_left(&err) {
            EngineError::ResourceExhausted(format!(
                "{}: no space left for a {size_gb} GiB data disk",
                path.display()
            ))
        } else {
            io_error(path, &err)
        });
    }
    drop(file);

    let output = Command::new(mkfs_ext4)
        .args([
            "-q",
            "-F",
            "-t",
            "ext4",
            "-m",
            "0",
            "-L",
            "quoro-data",
            "-E",
            "lazy_itable_init=1,lazy_journal_init=1,nodiscard",
        ])
        .arg(path)
        .output();

    match output {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let _ = std::fs::remove_file(path);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stderr = stderr.trim();
            if stderr.contains("No space left") {
                Err(EngineError::ResourceExhausted(format!(
                    "{}: formatting a {size_gb} GiB data disk ran out of space: {stderr}",
                    path.display()
                )))
            } else if stderr.is_empty() {
                Err(EngineError::Io(format!(
                    "{}: {} failed with {}",
                    path.display(),
                    mkfs_ext4.display(),
                    output.status
                )))
            } else {
                Err(EngineError::Io(format!(
                    "{}: {} failed: {stderr}",
                    path.display(),
                    mkfs_ext4.display()
                )))
            }
        }
        Err(err) => {
            let _ = std::fs::remove_file(path);
            Err(io_error(mkfs_ext4, &err))
        }
    }
}

/// Creates a unique temporary file next to `out` and returns its path and
/// handle.
fn create_temp(out: &Path) -> Result<(PathBuf, File), EngineError> {
    let dir = match out.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let name = out
        .file_name()
        .ok_or_else(|| EngineError::Io(format!("{}: not a file path", out.display())))?;
    let pid = std::process::id();
    for attempt in 0..100 {
        let candidate = dir.join(format!(".{}.{pid}.{attempt}.tmp", name.to_string_lossy()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(io_error(&candidate, &err)),
        }
    }
    Err(EngineError::Io(format!(
        "{}: no free temporary file name in {}",
        out.display(),
        dir.display()
    )))
}

/// Flushes and renames a finished temporary file over `out`.
fn finish(mut file: File, tmp: &Path, out: &Path) -> Result<(), EngineError> {
    let flushed = file
        .flush()
        .and_then(|()| file.sync_all())
        .map_err(|err| io_error(tmp, &err));
    drop(file);
    if let Err(err) = flushed {
        let _ = std::fs::remove_file(tmp);
        return Err(err);
    }
    std::fs::rename(tmp, out).map_err(|err| {
        let _ = std::fs::remove_file(tmp);
        io_error(out, &err)
    })
}

/// Writes `len` zero bytes with a fixed-size buffer, keeping the file dense
/// where the caller needs every byte readable.
fn write_zeros(file: &mut File, len: u64) -> io::Result<()> {
    let zeros = [0_u8; 64 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let want = usize::try_from(remaining.min(zeros.len() as u64)).unwrap_or(zeros.len());
        file.write_all(&zeros[..want])?;
        remaining -= want as u64;
    }
    Ok(())
}

/// Writes `len` zero bytes at `offset` of an already-open file.
fn write_zeros_at(file: &File, offset: u64, len: u64) -> io::Result<()> {
    let zeros = [0_u8; 4096];
    let mut written = 0_u64;
    while written < len {
        let want = usize::try_from((len - written).min(zeros.len() as u64)).unwrap_or(zeros.len());
        file.write_all_at(&zeros[..want], offset + written)?;
        written += want as u64;
    }
    Ok(())
}

/// `true` when the error is `ENOSPC`, from `set_len` or a child process.
fn is_no_space_left(err: &io::Error) -> bool {
    // ENOSPC is 28 on Linux; the module only builds for Linux.
    err.raw_os_error() == Some(28)
}

/// The one error mapping for this module: `Io` with the path in the message.
fn io_error(path: &Path, err: &io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::os::unix::fs::{FileExt, MetadataExt};
    use std::sync::atomic::{AtomicU64, Ordering};

    use quoro_init::{
        AppStatus, CONFIG_DISK_LEN, CONFIG_REGION_LEN, GuestConfig, LogConsole, NetConfig,
        STATUS_OFFSET, encode_status,
    };

    use super::{
        create_data_disk, kernel_cmdline, read_status, reset_status, write_config_disk,
        write_initramfs,
    };
    use crate::EngineError;

    /// A scratch directory that removes itself, unique per test process.
    struct TestDir(std::path::PathBuf);

    impl TestDir {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let seq = NEXT.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("quoro-boot-{}-{tag}-{seq}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create test directory");
            Self(dir)
        }

        fn path(&self, name: &str) -> std::path::PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn sample_config() -> GuestConfig {
        GuestConfig {
            args: vec![
                "/bin/server".to_owned(),
                "--port".to_owned(),
                "8080".to_owned(),
            ],
            env: vec![
                ("PATH".to_owned(), "/usr/local/bin:/usr/bin".to_owned()),
                ("HOME".to_owned(), "/app".to_owned()),
            ],
            user: Some("1000:1000".to_owned()),
            workdir: Some("/app".to_owned()),
            hostname: Some("demo".to_owned()),
            log_console: LogConsole::Virtio,
            net: Some(NetConfig {
                mac: [0x02, 0, 0, 0, 0, 0x17],
                address: Ipv4Addr::new(10, 42, 1, 7),
                prefix_len: 24,
                gateway: Ipv4Addr::new(10, 42, 1, 1),
            }),
            dns: vec![Ipv4Addr::new(1, 1, 1, 1)],
            stop_timeout_ms: 5_000,
            exec_port: None,
        }
    }

    #[test]
    fn kernel_cmdline_is_fixed() {
        assert_eq!(
            kernel_cmdline(),
            "console=ttyS0 loglevel=4 panic=-1 rdinit=/init"
        );
    }

    struct CpioEntry {
        ino: u32,
        mode: u32,
        uid: u32,
        gid: u32,
        nlink: u32,
        mtime: u32,
        name: String,
        rdev: (u32, u32),
        data: Vec<u8>,
    }

    /// A minimal `newc` reader: enough to check what the writer produced.
    fn parse_newc(bytes: &[u8]) -> Vec<CpioEntry> {
        let mut entries = Vec::new();
        let mut pos = 0_usize;
        loop {
            assert_eq!(pos % 4, 0, "entry at {pos} is not 4-byte aligned");
            assert_eq!(&bytes[pos..pos + 6], b"070701", "magic at {pos}");
            let mut fields = [0_u32; 13];
            for (index, field) in fields.iter_mut().enumerate() {
                let start = pos + 6 + index * 8;
                let text = std::str::from_utf8(&bytes[start..start + 8]).expect("hex field");
                *field = u32::from_str_radix(text, 16).expect("hex value");
            }
            let namesize = fields[11] as usize;
            let name_start = pos + 110;
            let name_bytes = &bytes[name_start..name_start + namesize];
            assert_eq!(name_bytes.last(), Some(&0), "name is NUL terminated");
            let name = String::from_utf8(name_bytes[..namesize - 1].to_vec()).expect("utf-8 name");
            pos += (110 + namesize).div_ceil(4) * 4;

            let filesize = fields[6] as usize;
            let data = bytes[pos..pos + filesize].to_vec();
            pos += filesize.div_ceil(4) * 4;

            entries.push(CpioEntry {
                ino: fields[0],
                mode: fields[1],
                uid: fields[2],
                gid: fields[3],
                nlink: fields[4],
                mtime: fields[5],
                name: name.clone(),
                rdev: (fields[9], fields[10]),
                data,
            });

            if name == "TRAILER!!!" {
                break;
            }
        }
        assert_eq!(pos, bytes.len(), "no bytes after the trailer");
        entries
    }

    #[test]
    fn initramfs_holds_dev_console_and_init() {
        let dir = TestDir::new("initramfs");
        let init = dir.path("quoro-init");
        // 91 bytes: not a multiple of 4, so the data padding is exercised.
        let payload: Vec<u8> = (0_u8..91).map(|byte| byte.wrapping_mul(7)).collect();
        std::fs::write(&init, &payload).expect("write init binary");

        let out = dir.path("initramfs.cpio");
        write_initramfs(&init, &out).expect("write initramfs");

        let bytes = std::fs::read(&out).expect("read initramfs");
        let entries = parse_newc(&bytes);
        let names: Vec<&str> = entries.iter().map(|entry| entry.name.as_str()).collect();
        assert_eq!(names, ["dev", "dev/console", "init", "TRAILER!!!"]);

        for (index, entry) in entries.iter().take(3).enumerate() {
            assert_eq!(entry.ino, index as u32 + 1, "inode numbers are 1..");
            assert_eq!(entry.uid, 0);
            assert_eq!(entry.gid, 0);
            assert_eq!(entry.mtime, 0);
        }

        let dev = &entries[0];
        assert_eq!(dev.mode, 0o040_755, "dev is a directory");
        assert_eq!(dev.nlink, 2);
        assert_eq!(dev.rdev, (0, 0));
        assert!(dev.data.is_empty());

        let console = &entries[1];
        assert_eq!(console.mode, 0o020_600, "dev/console is a char device");
        assert_eq!(console.nlink, 1);
        assert_eq!(console.rdev, (5, 1), "dev/console is 5:1");
        assert!(console.data.is_empty());

        let init_entry = &entries[2];
        assert_eq!(init_entry.mode, 0o100_755, "init is an executable file");
        assert_eq!(init_entry.nlink, 1);
        assert_eq!(init_entry.data, payload);

        let trailer = &entries[3];
        assert_eq!(trailer.ino, 0);
        assert_eq!(trailer.mode, 0);
        assert!(trailer.data.is_empty());
    }

    #[test]
    fn initramfs_is_byte_for_byte_reproducible() {
        let dir = TestDir::new("initramfs-repro");
        let init = dir.path("quoro-init");
        std::fs::write(&init, b"fake init binary with an odd length").expect("write init binary");

        let first = dir.path("first.cpio");
        let second = dir.path("second.cpio");
        write_initramfs(&init, &first).expect("first archive");
        write_initramfs(&init, &second).expect("second archive");
        assert_eq!(
            std::fs::read(&first).expect("read first"),
            std::fs::read(&second).expect("read second")
        );
    }

    #[test]
    fn initramfs_refuses_empty_and_non_regular_binaries() {
        let dir = TestDir::new("initramfs-refuse");
        let empty = dir.path("empty");
        std::fs::write(&empty, b"").expect("write empty file");
        assert!(matches!(
            write_initramfs(&empty, &dir.path("out.cpio")),
            Err(EngineError::Conflict(_))
        ));
        assert!(matches!(
            write_initramfs(&dir.0, &dir.path("out.cpio")),
            Err(EngineError::Conflict(_))
        ));
        assert!(matches!(
            write_initramfs(&dir.path("missing"), &dir.path("out.cpio")),
            Err(EngineError::Io(_))
        ));
        assert!(
            !dir.path("out.cpio").exists(),
            "nothing was renamed into place"
        );
    }

    #[test]
    fn config_disk_is_exactly_the_fixed_length() {
        let dir = TestDir::new("config-disk");
        let config = sample_config();
        let encoded = config.encode().expect("encode sample config");

        let path = dir.path("config.img");
        write_config_disk(&path, &config).expect("write config disk");

        let bytes = std::fs::read(&path).expect("read config disk");
        assert_eq!(bytes.len() as u64, CONFIG_DISK_LEN);
        assert_eq!(
            &bytes[..encoded.len()],
            encoded.as_slice(),
            "configuration at offset 0"
        );
        assert!(
            bytes[encoded.len()..].iter().all(|byte| *byte == 0),
            "everything after the configuration is zero"
        );
        let decoded = GuestConfig::decode(&bytes[..CONFIG_REGION_LEN as usize])
            .expect("decode the config region");
        assert_eq!(decoded, config);
        assert_eq!(
            read_status(&path).expect("fresh config disk has no status"),
            None,
            "a fresh config disk reports no status"
        );
    }

    #[test]
    fn config_disk_refuses_an_invalid_configuration() {
        let dir = TestDir::new("config-disk-invalid");
        let mut config = sample_config();
        config.args.clear();
        assert!(matches!(
            write_config_disk(&dir.path("config.img"), &config),
            Err(EngineError::Conflict(_))
        ));
    }

    #[test]
    fn status_region_round_trips_and_resets() {
        let dir = TestDir::new("status");
        let path = dir.path("config.img");
        std::fs::write(&path, vec![0_u8; CONFIG_DISK_LEN as usize]).expect("blank config disk");

        assert_eq!(read_status(&path).expect("blank status"), None);

        for status in [
            AppStatus::Exited { code: 0 },
            AppStatus::Exited { code: 143 },
            AppStatus::Fatal {
                code: 127,
                message: "entrypoint /bin/server not found".to_owned(),
            },
        ] {
            let record = encode_status(&status);
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open config disk");
            file.write_all_at(&record, STATUS_OFFSET)
                .expect("write status record");
            drop(file);
            assert_eq!(read_status(&path).expect("read status"), Some(status));
        }

        reset_status(&path).expect("reset status");
        assert_eq!(read_status(&path).expect("reset status"), None);
        reset_status(&dir.path("missing.img")).expect_err("missing file is an error");
    }

    #[test]
    fn data_disk_is_a_sparse_ext4_file() {
        let mkfs = ["/usr/sbin/mkfs.ext4", "/sbin/mkfs.ext4"]
            .into_iter()
            .map(std::path::PathBuf::from)
            .find(|path| path.is_file());
        let Some(mkfs) = mkfs else {
            eprintln!("skipping: no mkfs.ext4 on this host");
            return;
        };

        let dir = TestDir::new("data-disk");
        let path = dir.path("quoro-data.img");
        create_data_disk(&mkfs, &path, 1).expect("create data disk");

        let metadata = std::fs::metadata(&path).expect("stat data disk");
        assert_eq!(metadata.len(), 1 << 30);
        assert!(
            metadata.blocks() * 512 < metadata.len(),
            "sparse: {} bytes allocated for {} bytes",
            metadata.blocks() * 512,
            metadata.len()
        );

        let mut superblock = [0_u8; 2];
        std::fs::File::open(&path)
            .expect("open data disk")
            .read_exact_at(&mut superblock, 0x438)
            .expect("read ext4 superblock magic");
        assert_eq!(superblock, [0x53, 0xef], "ext4 superblock magic at 0x438");

        assert!(
            matches!(
                create_data_disk(&mkfs, &path, 1),
                Err(EngineError::Conflict(_))
            ),
            "an existing data disk is a conflict"
        );
        assert!(
            matches!(
                create_data_disk(&mkfs, &dir.path("zero.img"), 0),
                Err(EngineError::Conflict(_))
            ),
            "a zero size is a conflict"
        );
    }

    #[test]
    fn data_disk_failure_removes_the_file() {
        let dir = TestDir::new("data-disk-failure");
        let path = dir.path("quoro-data.img");
        let missing_mkfs = dir.path("no-such-mkfs");
        assert!(matches!(
            create_data_disk(&missing_mkfs, &path, 1),
            Err(EngineError::Io(_))
        ));
        assert!(!path.exists(), "the half-made file was removed");
    }
}

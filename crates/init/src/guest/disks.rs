// SPDX-License-Identifier: AGPL-3.0-only
//! Finding the three virtio-blk disks and reading the configuration.
//!
//! The disks are identified by their virtio-blk serial number, which virtio
//! publishes as `/sys/block/vd*/serial`, never by probe order. The kernel
//! probes them asynchronously, so the wait is a poll, not a single scan.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt as _;
use std::thread;
use std::time::{Duration, Instant};

use quoro_init::{
    AppStatus, CONFIG_REGION_LEN, GuestConfig, SERIAL_CONFIG, SERIAL_DATA, SERIAL_ROOT,
};

use crate::guest::Fault;

/// How long the boot waits for the disks to appear.
pub(crate) const SERIAL_WAIT: Duration = Duration::from_millis(5000);
/// How often it looks.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The device nodes found so far, by role.
#[derive(Debug, Default)]
pub(crate) struct Disks {
    pub(crate) root: Option<String>,
    pub(crate) data: Option<String>,
    pub(crate) config: Option<String>,
}

/// Block devices under `/sys/block`, by serial.
fn scan() -> Disks {
    let mut found = Disks::default();
    let Ok(entries) = std::fs::read_dir("/sys/block") else {
        return found;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("vd") {
            continue;
        }
        let Ok(serial) = std::fs::read_to_string(entry.path().join("serial")) else {
            continue;
        };
        let device = format!("/dev/{name}");
        match serial.trim() {
            SERIAL_ROOT => found.root = Some(device),
            SERIAL_DATA => found.data = Some(device),
            SERIAL_CONFIG => found.config = Some(device),
            _ => {}
        }
    }
    found
}

/// Poll for the three serials. Whatever is there when the budget runs out is
/// returned; the caller decides what a missing disk means.
pub(crate) fn wait_for_serials() -> Disks {
    let deadline = Instant::now() + SERIAL_WAIT;
    loop {
        let found = scan();
        if found.root.is_some() && found.data.is_some() && found.config.is_some() {
            return found;
        }
        if Instant::now() >= deadline {
            return found;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Open the config disk read-write (the status record goes back here) and hand
/// it to the shutdown path, so a later failure can still be reported.
pub(crate) fn open_config_device(path: &str) -> Result<File, Fault> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|err| Fault::new(format!("open config disk {path}: {err}")))
}

/// Read the config region and decode it. The status region is not part of this
/// read; it starts at [`quoro_init::STATUS_OFFSET`].
pub(crate) fn read_config(path: &str) -> Result<GuestConfig, Fault> {
    let file =
        File::open(path).map_err(|err| Fault::new(format!("open config disk {path}: {err}")))?;
    let mut region = vec![0u8; CONFIG_REGION_LEN as usize];
    let mut filled = 0usize;
    while filled < region.len() {
        match file.read_at(&mut region[filled..], filled as u64) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                return Err(Fault::new(format!("read config disk {path}: {err}")));
            }
        }
    }
    GuestConfig::decode(&region[..filled])
        .map_err(|err| Fault::new(format!("config disk {path}: {err}")))
}

/// The status the engine reads back after the VMM exits, written at
/// [`quoro_init::STATUS_OFFSET`]. Used by the shutdown path only.
pub(crate) fn write_status(file: &File, status: &AppStatus) -> io::Result<()> {
    let bytes = quoro_init::encode_status(status);
    file.write_all_at(&bytes, quoro_init::STATUS_OFFSET)
        .and_then(|()| file.sync_all())
}

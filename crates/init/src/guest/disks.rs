// SPDX-License-Identifier: AGPL-3.0-only
//! Finding the virtio-blk disks and reading the configuration.
//!
//! The disks are identified by their virtio-blk serial number, which virtio
//! publishes as `/sys/block/vd*/serial`, never by probe order. The kernel
//! probes them asynchronously, so the wait is a poll, not a single scan. The
//! three role disks are looked for together; a VM's volume disks (M5a) are
//! looked for when the configuration names them.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt as _;
use std::thread;
use std::time::{Duration, Instant};

use quoro_init::{
    AppStatus, CONFIG_REGION_LEN, GuestConfig, SERIAL_CONFIG, SERIAL_DATA, SERIAL_ROOT,
    SERIAL_VOLUME_PREFIX, VolumeMount,
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
    /// Every `quoro-vol-*` disk, as `(serial, device node)`.
    pub(crate) volumes: Vec<(String, String)>,
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
        let serial = serial.trim();
        match serial {
            SERIAL_ROOT => found.root = Some(device),
            SERIAL_DATA => found.data = Some(device),
            SERIAL_CONFIG => found.config = Some(device),
            other if other.starts_with(SERIAL_VOLUME_PREFIX) => {
                found.volumes.push((other.to_string(), device));
            }
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

/// The device node of every volume disk, in the order of `volumes`, each with
/// the mount the configuration gives it. Called before the switch into the
/// app's root: the nodes under `/dev` stay reachable afterwards, and the boot
/// fails early when a disk is missing.
pub(crate) fn wait_for_volumes(
    volumes: &[VolumeMount],
) -> Result<Vec<(VolumeMount, String)>, Fault> {
    if volumes.is_empty() {
        return Ok(Vec::new());
    }
    let deadline = Instant::now() + SERIAL_WAIT;
    loop {
        let found = scan().volumes;
        if let Ok(devices) = devices_for_volumes(&found, volumes) {
            return Ok(volumes.iter().cloned().zip(devices).collect());
        }
        if Instant::now() >= deadline {
            let missing = devices_for_volumes(&found, volumes)
                .err()
                .unwrap_or_default();
            return Err(Fault::new(format!(
                "no volume disk {missing} found within {} ms",
                SERIAL_WAIT.as_millis()
            )));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Pick the device node for each wanted volume from `(serial, device node)`
/// pairs found under `/sys/block`, in the order given. The error is the serial
/// of the first volume that is not there.
pub(crate) fn devices_for_volumes(
    available: &[(String, String)],
    volumes: &[VolumeMount],
) -> Result<Vec<String>, String> {
    volumes
        .iter()
        .map(|volume| {
            available
                .iter()
                .find(|(serial, _)| *serial == volume.serial)
                .map(|(_, device)| device.clone())
                .ok_or_else(|| volume.serial.clone())
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mount(serial: &str, path: &str) -> VolumeMount {
        VolumeMount {
            serial: serial.to_string(),
            mount_path: path.to_string(),
        }
    }

    fn available(serial: &str, device: &str) -> (String, String) {
        (serial.to_string(), device.to_string())
    }

    #[test]
    fn picks_the_device_for_each_serial_in_volume_order() {
        let found = vec![
            available("quoro-vol-1", "/dev/vde"),
            available("quoro-vol-0", "/dev/vdd"),
        ];
        let volumes = [
            mount("quoro-vol-0", "/data"),
            mount("quoro-vol-1", "/srv/uploads"),
        ];
        assert_eq!(
            devices_for_volumes(&found, &volumes),
            Ok(vec!["/dev/vdd".to_string(), "/dev/vde".to_string()])
        );
    }

    #[test]
    fn a_volume_beyond_the_probe_order_is_still_found() {
        let found = vec![available("quoro-vol-9", "/dev/vdz")];
        let volumes = [mount("quoro-vol-9", "/data")];
        assert_eq!(
            devices_for_volumes(&found, &volumes),
            Ok(vec!["/dev/vdz".to_string()])
        );
    }

    #[test]
    fn the_first_missing_serial_is_reported() {
        let found = vec![available("quoro-vol-0", "/dev/vdd")];
        let volumes = [mount("quoro-vol-0", "/data"), mount("quoro-vol-3", "/srv")];
        assert_eq!(
            devices_for_volumes(&found, &volumes),
            Err("quoro-vol-3".to_string())
        );
    }

    #[test]
    fn no_volumes_means_no_devices() {
        assert_eq!(devices_for_volumes(&[], &[]), Ok(Vec::new()));
    }
}

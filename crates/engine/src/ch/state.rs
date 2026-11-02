// SPDX-License-Identifier: AGPL-3.0-only
//! Per-VM runtime state files under `<vms_root>/<vm id>/`
//! (engine-trait.md §3.11, §5).
//!
//! Every VM owns one directory. The files in it are the whole truth the engine
//! needs to rediscover its world after an agent restart: the spec that created
//! the VM, the VMM process identity, the marker of a requested stop and the
//! record of the last exit. The directory also holds the disks and sockets a
//! VMM uses, so the layout is fixed here once and every other module builds
//! paths through [`VmPaths`].
//!
//! JSON is written atomically (temporary file, `sync_all`, rename, fsync of
//! the directory) so a crash never leaves a half-written record behind; the
//! exception is a corrupt file from another cause, which is reported as
//! [`EngineError::Hypervisor`] rather than silently ignored.
//!
//! [`classify`] is the single place that turns an exit into a state and an
//! event (§3.8, §3.11, §3.12).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::ch::vmm::{ProcessId, VmmExit};
use crate::{
    ConsoleMode, EngineError, EngineEvent, ImageDigest, InitConfig, MacAddr, NetAttach, VmId,
    VmSpec, VolumeAttach, VolumeId,
};

/// The version written into every [`SpecRecord`].
const SPEC_VERSION: u32 = 1;

/// The exit code reported for a VM stopped without a guest status (a forced
/// stop).
const FORCED_STOP_EXIT_CODE: i32 = 137;

/// Name of the spec file, used in error messages.
const SPEC_FILE: &str = "spec.json";

/// Every file of one VM, under `<vms_root>/<vm id>/`.
#[derive(Clone, Debug)]
pub struct VmPaths {
    /// `<vms_root>/<id>`.
    pub dir: PathBuf,
    /// `spec.json`: the [`SpecRecord`] written at create.
    pub spec: PathBuf,
    /// `vmm.json`: the [`VmmRecord`], present while a VMM may exist.
    pub vmm: PathBuf,
    /// `stop-requested`: an empty marker meaning a stop or delete asked for
    /// the exit.
    pub stop_requested: PathBuf,
    /// `last-exit.json`: the [`ExitRecord`] of the last exit.
    pub last_exit: PathBuf,
    /// `config.img`: the config disk the init reads.
    pub config_disk: PathBuf,
    /// `data.img`: the VM's writable data disk.
    pub data_disk: PathBuf,
    /// `api.sock`: the VMM's API socket.
    pub api_socket: PathBuf,
    /// `events.json`: the VMM's event-monitor file.
    pub event_log: PathBuf,
    /// `serial.log`: where the serial console goes when it is not a socket.
    pub serial_log: PathBuf,
    /// `vmm.log`: the VMM's own stdout and stderr.
    pub vmm_log: PathBuf,
}

impl VmPaths {
    /// Builds every path of one VM; nothing is created or checked here.
    pub fn new(vms_root: &Path, id: &VmId) -> VmPaths {
        let dir = vms_root.join(&id.0);
        VmPaths {
            spec: dir.join("spec.json"),
            vmm: dir.join("vmm.json"),
            stop_requested: dir.join("stop-requested"),
            last_exit: dir.join("last-exit.json"),
            config_disk: dir.join("config.img"),
            data_disk: dir.join("data.img"),
            api_socket: dir.join("api.sock"),
            event_log: dir.join("events.json"),
            serial_log: dir.join("serial.log"),
            vmm_log: dir.join("vmm.log"),
            dir,
        }
    }
}

/// `vm_`-style ids become directory and file names: 1..=64 chars of
/// `[A-Za-z0-9_-]`, not starting with `-`.
///
/// # Errors
///
/// `Conflict("invalid VM id …")` for anything else, so a hostile id can never
/// escape the runtime directory.
pub fn validate_vm_id(id: &VmId) -> Result<(), EngineError> {
    let text = id.0.as_str();
    let invalid =
        |reason: &str| EngineError::Conflict(format!("invalid VM id {:?}: {reason}", text));
    if text.is_empty() {
        return Err(invalid("it is empty"));
    }
    if text.len() > 64 {
        return Err(invalid("it is longer than 64 characters"));
    }
    if text.starts_with('-') {
        return Err(invalid("it starts with '-'"));
    }
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(invalid("only A-Z, a-z, 0-9, '_' and '-' are allowed"));
    }
    Ok(())
}

/// The serde mirror of [`VmSpec`].
///
/// The trait types have no serde derives and `lib.rs` is frozen, so the on-disk
/// form is this separate struct, field for field. A [`MacAddr`] is
/// `"02:00:00:00:00:01"`, an [`Ipv4Addr`] is a string, and [`ConsoleMode`] is
/// `{"mode":"off"}`, `{"mode":"virtio_console","socket":"…"}` or
/// `{"mode":"serial","socket":"…"}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecFile {
    /// The VM id, for example `vm_01HX…`.
    pub id: String,
    pub vcpus: u8,
    pub memory_mb: u32,
    /// An image digest, `"sha256:<64 hex>"`.
    pub rootfs: String,
    pub disk_gb: u32,
    pub volumes: Vec<VolumeFile>,
    pub net: NetFile,
    pub init: InitFile,
    pub console: ConsoleFile,
}

/// One attached volume in a [`SpecFile`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeFile {
    pub volume_id: String,
    pub mount_path: String,
}

/// The VM's network attachment in a [`SpecFile`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetFile {
    /// The guest address, as a string.
    pub ip: String,
    /// The node gateway, as a string.
    pub gateway: String,
    /// The MAC address, as `"02:00:00:00:00:01"`.
    pub mac: String,
}

/// The app's identity in a [`SpecFile`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitFile {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub user: Option<String>,
    pub workdir: Option<String>,
}

/// The console device in a [`SpecFile`], tagged by `mode`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ConsoleFile {
    /// No console device.
    Off,
    /// virtio-console on this UNIX socket.
    VirtioConsole {
        /// The guest-visible console socket.
        socket: PathBuf,
    },
    /// The serial port on this UNIX socket.
    Serial {
        /// The guest-visible console socket.
        socket: PathBuf,
    },
}

impl From<&VmSpec> for SpecFile {
    fn from(spec: &VmSpec) -> SpecFile {
        SpecFile {
            id: spec.id.0.clone(),
            vcpus: spec.vcpus,
            memory_mb: spec.memory_mb,
            rootfs: spec.rootfs.0.clone(),
            disk_gb: spec.disk_gb,
            volumes: spec
                .volumes
                .iter()
                .map(|volume| VolumeFile {
                    volume_id: volume.volume_id.0.clone(),
                    mount_path: volume.mount_path.clone(),
                })
                .collect(),
            net: NetFile {
                ip: spec.net.ip.to_string(),
                gateway: spec.net.gateway.to_string(),
                mac: format_mac(spec.net.mac),
            },
            init: InitFile {
                entrypoint: spec.init.entrypoint.clone(),
                cmd: spec.init.cmd.clone(),
                env: spec.init.env.clone(),
                user: spec.init.user.clone(),
                workdir: spec.init.workdir.clone(),
            },
            console: match &spec.console {
                ConsoleMode::Off => ConsoleFile::Off,
                ConsoleMode::VirtioConsole { socket } => ConsoleFile::VirtioConsole {
                    socket: socket.clone(),
                },
                ConsoleMode::Serial { socket } => ConsoleFile::Serial {
                    socket: socket.clone(),
                },
            },
        }
    }
}

impl SpecFile {
    /// The [`VmSpec`] this file states.
    ///
    /// # Errors
    ///
    /// `Hypervisor("corrupt spec.json: …")` for a MAC address or an IP address
    /// that is not a valid one, and for an empty `id`.
    pub fn to_spec(&self) -> Result<VmSpec, EngineError> {
        let corrupt =
            |reason: String| EngineError::Hypervisor(format!("corrupt {SPEC_FILE}: {reason}"));
        if self.id.is_empty() {
            return Err(corrupt("the VM id is empty".to_string()));
        }
        let mac = parse_mac(&self.net.mac)
            .ok_or_else(|| corrupt(format!("invalid MAC address {:?}", self.net.mac)))?;
        let ip = self
            .net
            .ip
            .parse::<Ipv4Addr>()
            .map_err(|_| corrupt(format!("invalid IP address {:?}", self.net.ip)))?;
        let gateway = self
            .net
            .gateway
            .parse::<Ipv4Addr>()
            .map_err(|_| corrupt(format!("invalid gateway address {:?}", self.net.gateway)))?;
        Ok(VmSpec {
            id: VmId(self.id.clone()),
            vcpus: self.vcpus,
            memory_mb: self.memory_mb,
            rootfs: ImageDigest(self.rootfs.clone()),
            disk_gb: self.disk_gb,
            volumes: self
                .volumes
                .iter()
                .map(|volume| VolumeAttach {
                    volume_id: VolumeId(volume.volume_id.clone()),
                    mount_path: volume.mount_path.clone(),
                })
                .collect(),
            net: NetAttach { ip, gateway, mac },
            init: InitConfig {
                entrypoint: self.init.entrypoint.clone(),
                cmd: self.init.cmd.clone(),
                env: self.init.env.clone(),
                user: self.init.user.clone(),
                workdir: self.init.workdir.clone(),
            },
            console: match &self.console {
                ConsoleFile::Off => ConsoleMode::Off,
                ConsoleFile::VirtioConsole { socket } => ConsoleMode::VirtioConsole {
                    socket: socket.clone(),
                },
                ConsoleFile::Serial { socket } => ConsoleMode::Serial {
                    socket: socket.clone(),
                },
            },
        })
    }

    /// The record this spec is written as.
    fn into_record(self, tap: String, bridge: String, created_at_ms: u64) -> SpecRecord {
        SpecRecord {
            version: SPEC_VERSION,
            spec: self,
            tap,
            bridge,
            created_at_ms,
        }
    }
}

/// The contents of `spec.json`: the spec, plus what create reserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecRecord {
    /// The format version; [`SPEC_VERSION`] today.
    pub version: u32,
    pub spec: SpecFile,
    /// The TAP device name the VM's network uses.
    pub tap: String,
    /// The Linux bridge that TAP is attached to.
    pub bridge: String,
    /// When create reserved the resources, in unix epoch milliseconds.
    pub created_at_ms: u64,
}

impl SpecRecord {
    /// The record for `spec` on the network resources `tap` and `bridge`,
    /// created now.
    pub fn new(spec: &VmSpec, tap: &str, bridge: &str) -> SpecRecord {
        SpecFile::from(spec).into_record(tap.to_string(), bridge.to_string(), now_ms())
    }

    /// The spec this record holds.
    ///
    /// # Errors
    ///
    /// `Hypervisor("corrupt spec.json: …")` for an unknown version, and
    /// whatever [`SpecFile::to_spec`] reports.
    pub fn to_spec(&self) -> Result<VmSpec, EngineError> {
        if self.version != SPEC_VERSION {
            return Err(EngineError::Hypervisor(format!(
                "corrupt {SPEC_FILE}: unsupported version {}",
                self.version
            )));
        }
        self.spec.to_spec()
    }
}

/// The contents of `vmm.json`: which process is this VM's VMM.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VmmRecord {
    /// The pid plus start time that identify the VMM process.
    pub process: ProcessId,
    /// True once `/vm.boot` succeeded for this VMM process.
    pub booted: bool,
    /// When the VMM was started, in unix epoch milliseconds.
    pub started_at_ms: Option<u64>,
}

/// How a VM's last exit was classified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitState {
    /// The exit followed a requested stop or delete.
    Stopped,
    /// The VM went away without anyone asking.
    Crashed,
}

/// The contents of `last-exit.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitRecord {
    pub state: ExitState,
    /// The app's exit code, when the guest reported one.
    pub exit_code: Option<i32>,
    /// Why the VM ended, in words for the operator.
    pub reason: String,
    /// When the exit was recorded, in unix epoch milliseconds.
    pub at_ms: u64,
}

/// Everything known when a VMM process has ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitFacts {
    /// `/vm.boot` had succeeded for that VMM.
    pub booted: bool,
    /// The stop-requested marker existed (a stop or delete asked for this
    /// exit).
    pub stop_requested: bool,
    /// The event log showed a guest reset, or the supervisor killed the VMM
    /// because of one.
    pub reset_seen: bool,
    /// What the init wrote to the config disk, if anything (read after the
    /// exit).
    pub status: Option<quoro_init::AppStatus>,
    /// How the process ended; `None` when unknown.
    pub vmm_exit: Option<VmmExit>,
}

/// What to record and which event to emit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classified {
    /// The record to write as `last-exit.json`.
    pub record: ExitRecord,
    /// The event to emit, if any.
    pub event: Option<EngineEvent>,
}

/// Maps one exit to its state, record and event (contract §3.8, §3.11, §3.12).
///
/// - Not booted: `Stopped` with no exit code and no event when a stop was
///   requested; otherwise `Crashed` with
///   `"VMM exited before the VM booted (<exit>)"` and a `Crashed` event.
/// - Booted and a stop was requested: `Stopped`; the exit code is the guest's
///   status code, or 137 when there is none (a forced stop); event
///   `Exited { code }`.
/// - Booted without a requested stop: `Crashed`; the exit code is the guest's
///   status code when there is one; the reason is, in priority order, the
///   guest reset, a fatal boot failure, the app's exit, or the VMM going away
///   without a guest status; event `Crashed { id, reason }`.
#[must_use]
pub fn classify(id: &VmId, facts: &ExitFacts) -> Classified {
    let at_ms = now_ms();
    let exit = exit_description(facts.vmm_exit);

    if !facts.booted {
        if facts.stop_requested {
            return Classified {
                record: ExitRecord {
                    state: ExitState::Stopped,
                    exit_code: None,
                    reason: "stop requested before the VM booted".to_string(),
                    at_ms,
                },
                event: None,
            };
        }
        let reason = format!("VMM exited before the VM booted ({exit})");
        return Classified {
            record: ExitRecord {
                state: ExitState::Crashed,
                exit_code: None,
                reason: reason.clone(),
                at_ms,
            },
            event: Some(EngineEvent::Crashed {
                id: id.clone(),
                reason,
            }),
        };
    }

    if facts.stop_requested {
        let code = status_code(facts.status.as_ref()).unwrap_or(FORCED_STOP_EXIT_CODE);
        return Classified {
            record: ExitRecord {
                state: ExitState::Stopped,
                exit_code: Some(code),
                reason: "stop requested".to_string(),
                at_ms,
            },
            event: Some(EngineEvent::Exited {
                id: id.clone(),
                code,
            }),
        };
    }

    let reason = crash_reason(facts, &exit);
    Classified {
        record: ExitRecord {
            state: ExitState::Crashed,
            exit_code: status_code(facts.status.as_ref()),
            reason: reason.clone(),
            at_ms,
        },
        event: Some(EngineEvent::Crashed {
            id: id.clone(),
            reason,
        }),
    }
}

/// The guest's exit code, when the init reported one.
fn status_code(status: Option<&quoro_init::AppStatus>) -> Option<i32> {
    match status {
        Some(quoro_init::AppStatus::Exited { code }) => Some(*code),
        Some(quoro_init::AppStatus::Fatal { code, .. }) => Some(*code),
        None => None,
    }
}

/// Why a booted VM vanished without anyone asking, in priority order.
fn crash_reason(facts: &ExitFacts, exit: &str) -> String {
    if facts.reset_seen {
        return "guest reset (kernel panic or init crash)".to_string();
    }
    match &facts.status {
        Some(quoro_init::AppStatus::Fatal { message, .. }) => format!("boot failed: {message}"),
        Some(quoro_init::AppStatus::Exited { code }) => format!("app exited with code {code}"),
        None => format!("VMM exited without a guest status ({exit}; see vmm.log)"),
    }
}

/// A short description of how the VMM process ended.
fn exit_description(exit: Option<VmmExit>) -> String {
    match exit {
        None => "exit status unknown".to_string(),
        Some(VmmExit::Gone) => "process gone, wait status unavailable".to_string(),
        Some(VmmExit::Exited { code, signal }) => match (code, signal) {
            (Some(code), None) => format!("exit code {code}"),
            (Some(code), Some(signal)) => format!("exit code {code}, signal {signal}"),
            (None, Some(signal)) => format!("killed by signal {signal}"),
            (None, None) => "unknown wait status".to_string(),
        },
    }
}

/// Formats a MAC address the way the init's config and the VMM API want it.
fn format_mac(mac: MacAddr) -> String {
    let [a, b, c, d, e, f] = mac.0;
    format!("{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}")
}

/// Parses `"02:00:00:00:00:01"` (either case).
fn parse_mac(text: &str) -> Option<MacAddr> {
    let mut bytes = [0u8; 6];
    let mut parts = text.split(':');
    for byte in &mut bytes {
        let part = parts.next()?;
        if part.len() != 2 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        *byte = u8::from_str_radix(part, 16).ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(MacAddr(bytes))
}

/// Serial number of one temporary file, so two writers never collide.
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Writes JSON atomically: `<path>.tmp-<pid>-<n>`, write, `sync_all`, rename,
/// fsync the parent directory.
///
/// # Errors
///
/// `Io` when the value cannot be encoded, or when any file operation fails.
pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), EngineError> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|err| EngineError::Io(format!("cannot encode {}: {err}", path.display())))?;
    bytes.push(b'\n');

    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "state".to_string());
    let temp = path.with_file_name(format!("{name}.tmp-{}-{sequence}", std::process::id()));
    let result = write_atomic(path, &temp, &bytes);
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// The file work behind [`write_json_atomic`].
fn write_atomic(path: &Path, temp: &Path, bytes: &[u8]) -> Result<(), EngineError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temp)
        .map_err(|err| io_error(temp, &err))?;
    file.write_all(bytes).map_err(|err| io_error(temp, &err))?;
    file.sync_all().map_err(|err| io_error(temp, &err))?;
    drop(file);
    fs::rename(temp, path).map_err(|err| io_error(path, &err))?;
    sync_dir(path)
}

/// Reads one JSON file.
///
/// # Errors
///
/// `Ok(None)` when the file does not exist; `Hypervisor("corrupt <file>: …")`
/// when it does not parse; `Io` for any other read failure.
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, EngineError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_error(path, &err)),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|err| EngineError::Hypervisor(format!("corrupt {}: {err}", file_label(path))))
}

/// The file's own name, for error messages.
fn file_label(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// Creates the empty marker file and fsyncs its directory.
///
/// # Errors
///
/// `Io` when the marker cannot be created.
pub fn touch_marker(path: &Path) -> Result<(), EngineError> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|err| io_error(path, &err))?;
    file.sync_all().map_err(|err| io_error(path, &err))?;
    drop(file);
    sync_dir(path)
}

/// Removes the marker file; a missing file is fine.
///
/// # Errors
///
/// `Io` for any failure other than "not found".
pub fn remove_marker(path: &Path) -> Result<(), EngineError> {
    match fs::remove_file(path) {
        Ok(()) => sync_dir(path),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(io_error(path, &err)),
    }
}

/// True when the marker file exists.
#[must_use]
pub fn marker_exists(path: &Path) -> bool {
    path.exists()
}

/// Milliseconds since the unix epoch.
#[must_use]
pub fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

/// The wall clock time `ms` milliseconds after the unix epoch.
#[must_use]
pub fn ms_to_system_time(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

/// An `Io` error naming the file it happened on.
fn io_error(path: &Path, err: &io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

/// fsyncs the directory holding `path`, so a rename or removal survives a
/// crash.
fn sync_dir(path: &Path) -> Result<(), EngineError> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let file = File::open(dir).map_err(|err| io_error(dir, &err))?;
    file.sync_all().map_err(|err| io_error(dir, &err))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        Classified, ConsoleFile, ExitFacts, ExitRecord, ExitState, SpecFile, SpecRecord, VmPaths,
        VmmRecord, classify, marker_exists, ms_to_system_time, now_ms, read_json, remove_marker,
        touch_marker, validate_vm_id, write_json_atomic,
    };
    use crate::ch::vmm::{ProcessId, VmmExit};
    use crate::{
        ConsoleMode, EngineError, EngineEvent, ImageDigest, InitConfig, MacAddr, NetAttach, VmId,
        VmSpec, VolumeAttach, VolumeId,
    };
    use quoro_init::AppStatus;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quoro-state-{tag}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn spec(console: ConsoleMode) -> VmSpec {
        VmSpec {
            id: VmId("vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string()),
            vcpus: 2,
            memory_mb: 512,
            rootfs: ImageDigest("sha256:aa".to_string()),
            disk_gb: 1,
            volumes: vec![VolumeAttach {
                volume_id: VolumeId("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string()),
                mount_path: "/data".to_string(),
            }],
            net: NetAttach {
                ip: "10.42.1.7".parse().expect("ip"),
                gateway: "10.42.1.1".parse().expect("gateway"),
                mac: MacAddr([0x02, 0, 0, 0, 0, 1]),
            },
            init: InitConfig {
                entrypoint: vec!["/bin/sh".to_string()],
                cmd: vec!["-c".to_string(), "echo hi".to_string()],
                env: vec![("PATH".to_string(), "/usr/bin".to_string())],
                user: Some("1000:1000".to_string()),
                workdir: Some("/app".to_string()),
            },
            console,
        }
    }

    fn socket_path() -> PathBuf {
        PathBuf::from("/run/quoro/vm_1/console.sock")
    }

    #[test]
    fn spec_file_round_trips_every_console_mode() {
        let modes = [
            ConsoleMode::Off,
            ConsoleMode::VirtioConsole {
                socket: socket_path(),
            },
            ConsoleMode::Serial {
                socket: socket_path(),
            },
        ];
        for console in modes {
            let original = spec(console);
            let file = SpecFile::from(&original);
            let restored = file.to_spec().expect("to_spec");
            assert_eq!(restored, original);
            let json = serde_json::to_string(&file).expect("encode");
            let decoded: SpecFile = serde_json::from_str(&json).expect("decode");
            assert_eq!(decoded, file);
        }
    }

    #[test]
    fn spec_file_json_shape() {
        let file = SpecFile::from(&spec(ConsoleMode::VirtioConsole {
            socket: socket_path(),
        }));
        let json = serde_json::to_value(&file).expect("encode");
        assert_eq!(json["net"]["mac"], "02:00:00:00:00:01");
        assert_eq!(json["net"]["ip"], "10.42.1.7");
        assert_eq!(json["console"]["mode"], "virtio_console");
        assert_eq!(json["console"]["socket"], "/run/quoro/vm_1/console.sock");
        assert_eq!(json["init"]["env"][0][0], "PATH");

        let off = SpecFile::from(&spec(ConsoleMode::Off));
        assert_eq!(
            serde_json::to_value(&off).expect("encode")["console"],
            serde_json::json!({"mode": "off"})
        );
    }

    #[test]
    fn spec_file_rejects_bad_mac_and_ip() {
        let mut file = SpecFile::from(&spec(ConsoleMode::Off));
        file.net.mac = "02:00:00:00:00".to_string();
        match file.to_spec() {
            Err(EngineError::Hypervisor(reason)) => {
                assert!(reason.starts_with("corrupt spec.json: "), "{reason}");
                assert!(reason.contains("MAC"), "{reason}");
            }
            other => panic!("expected Hypervisor, got {other:?}"),
        }

        let mut file = SpecFile::from(&spec(ConsoleMode::Off));
        file.net.ip = "10.42.1.999".to_string();
        assert!(matches!(
            file.to_spec(),
            Err(EngineError::Hypervisor(reason)) if reason.contains("IP address")
        ));

        let mut file = SpecFile::from(&spec(ConsoleMode::Off));
        file.net.gateway = "gateway".to_string();
        assert!(matches!(
            file.to_spec(),
            Err(EngineError::Hypervisor(reason)) if reason.contains("gateway")
        ));

        let mut file = SpecFile::from(&spec(ConsoleMode::Off));
        file.id = String::new();
        assert!(matches!(
            file.to_spec(),
            Err(EngineError::Hypervisor(reason)) if reason.contains("id is empty")
        ));
    }

    #[test]
    fn spec_record_round_trip_and_version_check() {
        let original = spec(ConsoleMode::Serial {
            socket: socket_path(),
        });
        let record = SpecRecord::new(&original, "tap0", "br-quoro");
        assert_eq!(record.version, 1);
        assert_eq!(record.tap, "tap0");
        assert_eq!(record.spec.to_spec().expect("to_spec"), original);

        let json = serde_json::to_string(&record).expect("encode");
        let decoded: SpecRecord = serde_json::from_str(&json).expect("decode");
        assert_eq!(decoded.to_spec().expect("to_spec"), original);

        let mut newer = record;
        newer.version = 2;
        assert!(matches!(
            newer.to_spec(),
            Err(EngineError::Hypervisor(reason)) if reason.contains("unsupported version")
        ));
    }

    #[test]
    fn console_file_is_tagged_by_mode() {
        let off = ConsoleFile::Off;
        assert_eq!(
            serde_json::to_value(&off).expect("encode"),
            serde_json::json!({"mode": "off"})
        );
        let serial = ConsoleFile::Serial {
            socket: socket_path(),
        };
        assert_eq!(
            serde_json::to_value(&serial).expect("encode"),
            serde_json::json!({"mode": "serial", "socket": "/run/quoro/vm_1/console.sock"})
        );
    }

    #[test]
    fn vm_paths_are_under_the_vm_directory() {
        let root = PathBuf::from("/var/lib/quoro/vms");
        let id = VmId("vm_suite".to_string());
        let paths = VmPaths::new(&root, &id);
        assert_eq!(paths.dir, PathBuf::from("/var/lib/quoro/vms/vm_suite"));
        assert_eq!(paths.spec, paths.dir.join("spec.json"));
        assert_eq!(paths.vmm, paths.dir.join("vmm.json"));
        assert_eq!(paths.stop_requested, paths.dir.join("stop-requested"));
        assert_eq!(paths.last_exit, paths.dir.join("last-exit.json"));
        assert_eq!(paths.config_disk, paths.dir.join("config.img"));
        assert_eq!(paths.data_disk, paths.dir.join("data.img"));
        assert_eq!(paths.api_socket, paths.dir.join("api.sock"));
        assert_eq!(paths.event_log, paths.dir.join("events.json"));
        assert_eq!(paths.serial_log, paths.dir.join("serial.log"));
        assert_eq!(paths.vmm_log, paths.dir.join("vmm.log"));
    }

    #[test]
    fn vm_id_validation() {
        for good in ["vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF", "vm_suite", "a", "A-b_9"] {
            assert!(
                validate_vm_id(&VmId(good.to_string())).is_ok(),
                "{good} should be valid"
            );
        }
        let long = "a".repeat(65);
        for bad in ["", "../x", "a/b", ".x", "-x", "with space", long.as_str()] {
            match validate_vm_id(&VmId(bad.to_string())) {
                Err(EngineError::Conflict(reason)) => {
                    assert!(reason.starts_with("invalid VM id "), "{reason}");
                }
                other => panic!("{bad:?} should be rejected, got {other:?}"),
            }
        }
        assert!(validate_vm_id(&VmId("a".repeat(64))).is_ok());
    }

    #[test]
    fn json_write_and_read_round_trip() {
        let dir = temp_dir("json");
        let path = dir.join("vmm.json");
        let record = VmmRecord {
            process: ProcessId {
                pid: 4242,
                start_time: 99,
            },
            booted: true,
            started_at_ms: Some(1_700_000_000_000),
        };
        write_json_atomic(&path, &record).expect("write");
        assert_eq!(read_json::<VmmRecord>(&path).expect("read"), Some(record));

        // No temporary file is left behind.
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("read_dir")
            .map(|entry| entry.expect("entry").file_name())
            .filter(|name| name.to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn json_read_missing_is_none_and_corrupt_is_hypervisor() {
        let dir = temp_dir("corrupt");
        let missing = dir.join("last-exit.json");
        assert_eq!(read_json::<ExitRecord>(&missing).expect("missing"), None);

        let path = dir.join("last-exit.json");
        std::fs::write(&path, b"{ not json").expect("write");
        match read_json::<ExitRecord>(&path) {
            Err(EngineError::Hypervisor(reason)) => {
                assert!(reason.starts_with("corrupt last-exit.json: "), "{reason}");
            }
            other => panic!("expected Hypervisor, got {other:?}"),
        }

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn markers_touch_exist_and_remove() {
        let dir = temp_dir("marker");
        let path = dir.join("stop-requested");
        assert!(!marker_exists(&path));
        touch_marker(&path).expect("touch");
        assert!(marker_exists(&path));
        assert_eq!(std::fs::read(&path).expect("read"), b"");
        remove_marker(&path).expect("remove");
        assert!(!marker_exists(&path));
        remove_marker(&path).expect("remove is idempotent");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn clock_helpers() {
        let ms = now_ms();
        assert!(ms > 1_700_000_000_000, "{ms}");
        let time = ms_to_system_time(1_700_000_000_000);
        assert_eq!(
            time.duration_since(std::time::UNIX_EPOCH)
                .expect("after epoch")
                .as_millis(),
            1_700_000_000_000
        );
        assert_eq!(ms_to_system_time(0), std::time::UNIX_EPOCH);
    }

    fn base_facts(booted: bool) -> ExitFacts {
        ExitFacts {
            booted,
            stop_requested: false,
            reset_seen: false,
            status: None,
            vmm_exit: Some(VmmExit::Exited {
                code: Some(0),
                signal: None,
            }),
        }
    }

    fn id() -> VmId {
        VmId("vm_test".to_string())
    }

    #[test]
    fn classify_not_booted_stop_requested() {
        let mut facts = base_facts(false);
        facts.stop_requested = true;
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.state, ExitState::Stopped);
        assert_eq!(record.exit_code, None);
        assert!(record.at_ms > 0);
        assert_eq!(event, None);
    }

    #[test]
    fn classify_not_booted_crashed() {
        let facts = base_facts(false);
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.state, ExitState::Crashed);
        assert_eq!(record.exit_code, None);
        assert_eq!(
            record.reason,
            "VMM exited before the VM booted (exit code 0)"
        );
        assert_eq!(
            event,
            Some(EngineEvent::Crashed {
                id: id(),
                reason: record.reason.clone(),
            })
        );
    }

    #[test]
    fn classify_not_booted_with_signal() {
        let mut facts = base_facts(false);
        facts.vmm_exit = Some(VmmExit::Exited {
            code: None,
            signal: Some(9),
        });
        let record = classify(&id(), &facts).record;
        assert_eq!(
            record.reason,
            "VMM exited before the VM booted (killed by signal 9)"
        );

        facts.vmm_exit = Some(VmmExit::Gone);
        let record = classify(&id(), &facts).record;
        assert_eq!(
            record.reason,
            "VMM exited before the VM booted (process gone, wait status unavailable)"
        );

        facts.vmm_exit = None;
        let record = classify(&id(), &facts).record;
        assert_eq!(
            record.reason,
            "VMM exited before the VM booted (exit status unknown)"
        );
    }

    #[test]
    fn classify_booted_stop_requested_with_status() {
        let mut facts = base_facts(true);
        facts.stop_requested = true;
        facts.status = Some(AppStatus::Exited { code: 0 });
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.state, ExitState::Stopped);
        assert_eq!(record.exit_code, Some(0));
        assert_eq!(event, Some(EngineEvent::Exited { id: id(), code: 0 }));

        facts.status = Some(AppStatus::Fatal {
            code: 127,
            message: "no such file".to_string(),
        });
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.state, ExitState::Stopped);
        assert_eq!(record.exit_code, Some(127));
        assert_eq!(
            event,
            Some(EngineEvent::Exited {
                id: id(),
                code: 127
            })
        );
    }

    #[test]
    fn classify_booted_stop_requested_without_status() {
        let mut facts = base_facts(true);
        facts.stop_requested = true;
        facts.vmm_exit = Some(VmmExit::Exited {
            code: None,
            signal: Some(9),
        });
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.state, ExitState::Stopped);
        assert_eq!(record.exit_code, Some(137));
        assert_eq!(
            event,
            Some(EngineEvent::Exited {
                id: id(),
                code: 137
            })
        );
    }

    #[test]
    fn classify_booted_crashed_reason_priority() {
        // 1. A guest reset wins over everything.
        let mut facts = base_facts(true);
        facts.reset_seen = true;
        facts.status = Some(AppStatus::Fatal {
            code: 1,
            message: "boot broke".to_string(),
        });
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.state, ExitState::Crashed);
        assert_eq!(record.exit_code, Some(1));
        assert_eq!(record.reason, "guest reset (kernel panic or init crash)");
        assert_eq!(
            event,
            Some(EngineEvent::Crashed {
                id: id(),
                reason: record.reason.clone(),
            })
        );

        // 2. A fatal boot failure.
        let mut facts = base_facts(true);
        facts.status = Some(AppStatus::Fatal {
            code: 127,
            message: "entrypoint /app/run not found".to_string(),
        });
        let record = classify(&id(), &facts).record;
        assert_eq!(record.exit_code, Some(127));
        assert_eq!(record.reason, "boot failed: entrypoint /app/run not found");

        // 3. The app exited by itself.
        let mut facts = base_facts(true);
        facts.status = Some(AppStatus::Exited { code: 3 });
        let Classified { record, event } = classify(&id(), &facts);
        assert_eq!(record.exit_code, Some(3));
        assert_eq!(record.reason, "app exited with code 3");
        assert_eq!(
            event,
            Some(EngineEvent::Crashed {
                id: id(),
                reason: "app exited with code 3".to_string(),
            })
        );

        // 4. No guest status at all.
        let mut facts = base_facts(true);
        facts.vmm_exit = Some(VmmExit::Exited {
            code: Some(1),
            signal: None,
        });
        let record = classify(&id(), &facts).record;
        assert_eq!(record.exit_code, None);
        assert_eq!(
            record.reason,
            "VMM exited without a guest status (exit code 1; see vmm.log)"
        );
    }

    #[test]
    fn exit_record_serde_shape() {
        let record = ExitRecord {
            state: ExitState::Crashed,
            exit_code: None,
            reason: "boom".to_string(),
            at_ms: 7,
        };
        let json = serde_json::to_value(&record).expect("encode");
        assert_eq!(json["state"], "crashed");
        assert_eq!(json["exit_code"], serde_json::Value::Null);
        assert_eq!(json["reason"], "boom");
        let decoded: ExitRecord = serde_json::from_value(json).expect("decode");
        assert_eq!(decoded, record);
    }
}

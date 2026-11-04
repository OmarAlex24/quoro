// SPDX-License-Identifier: AGPL-3.0-only
//! What the init is told, and the Cloud Hypervisor configuration built from a
//! spec (engine-trait.md §5, §6).
//!
//! Two pure builders live here. [`guest_config`] is the init's configuration:
//! the guest contract of §6 as a [`GuestConfig`], validated by the same encoder
//! the init uses, so a VM that would fail its boot is rejected at create time.
//! [`vm_create_body`] is the JSON body of `PUT /api/v1/vm.create` for Cloud
//! Hypervisor v54. Neither touches the host: the caller owns every path and
//! device name, which keeps the mapping testable without KVM.

use std::net::Ipv4Addr;
use std::path::Path;
use std::time::Duration;

use quoro_init::{GuestConfig, LogConsole, NetConfig};
use serde_json::{Value, json};

use crate::ch::net::Subnet;
use crate::ch::state::VmPaths;
use crate::{ConsoleMode, EngineError, MacAddr, SerialMode, VmId, VmSpec};

/// The longest `stop_timeout_ms` the init's grammar accepts: one day.
const MAX_STOP_TIMEOUT_MS: u64 = 86_400_000;

/// Largest hostname the init accepts.
const MAX_HOSTNAME_CHARS: usize = 63;

/// One megabyte in bytes, the unit of `memory.size` in the VMM API.
const MIB: u64 = 1_048_576;

/// A work disk's serial is the virtio-blk serial limit: 20 bytes.
const MAX_WORK_DISK_SERIAL: usize = 20;

/// The host paths a VM boots from.
#[derive(Clone, Copy, Debug)]
pub struct BootFiles<'a> {
    /// The Quoro-provided guest kernel.
    pub kernel: &'a Path,
    /// The initramfs that carries `quoro-init`.
    pub initramfs: &'a Path,
    /// The prepared image, attached read-only.
    pub rootfs: &'a Path,
}

/// The init's configuration for this VM (contract §6).
///
/// # Errors
///
/// `Conflict("VM <id> has no command: entrypoint and cmd are both empty")`
/// when there is nothing to run, and
/// `Conflict("invalid guest configuration: …")` when the configuration the
/// spec implies would not encode — a bad user, workdir, hostname, environment
/// name or network prefix.
pub fn guest_config(
    spec: &VmSpec,
    subnet: &Subnet,
    dns: &[Ipv4Addr],
    stop_timeout: Duration,
) -> Result<GuestConfig, EngineError> {
    let mut args = spec.init.entrypoint.clone();
    args.extend(spec.init.cmd.iter().cloned());
    if args.is_empty() {
        return Err(EngineError::Conflict(format!(
            "VM {} has no command: entrypoint and cmd are both empty",
            spec.id.0
        )));
    }

    let config = GuestConfig {
        args,
        env: spec.init.env.clone(),
        user: spec.init.user.clone(),
        workdir: spec.init.workdir.clone(),
        hostname: Some(hostname_for(&spec.id)),
        log_console: match &spec.console {
            ConsoleMode::VirtioConsole { .. } => LogConsole::Virtio,
            ConsoleMode::Serial { .. } => LogConsole::Serial,
            ConsoleMode::Off => LogConsole::None,
        },
        net: Some(NetConfig {
            mac: spec.net.mac.0,
            address: spec.net.ip,
            prefix_len: subnet.prefix_len,
            gateway: spec.net.gateway,
        }),
        dns: dns.to_vec(),
        stop_timeout_ms: stop_timeout_ms(stop_timeout),
        exec_port: spec.vsock.as_ref().map(|_| quoro_init::EXEC_PORT),
    };
    config
        .encode()
        .map_err(|err| EngineError::Conflict(format!("invalid guest configuration: {err}")))?;
    Ok(config)
}

/// Rejects a spec that gives the serial port two owners (section 3.14).
///
/// [`ConsoleMode::Serial`] is superseded in M4 by [`VmSpec::serial`]; a spec
/// that still uses it must leave `serial` at [`SerialMode::Off`], so there is
/// exactly one serial device. Both set is a caller bug, not a preference to
/// reconcile here.
///
/// # Errors
///
/// `Conflict` naming the VM when both are set.
pub(super) fn check_consoles(spec: &VmSpec) -> Result<(), EngineError> {
    if matches!(spec.console, ConsoleMode::Serial { .. }) && !matches!(spec.serial, SerialMode::Off)
    {
        return Err(EngineError::Conflict(format!(
            "VM {} sets both ConsoleMode::Serial and VmSpec::serial; the serial port has one owner (section 3.14)",
            spec.id.0
        )));
    }
    Ok(())
}

/// The stop timeout in milliseconds, saturating and capped at one day, so the
/// init never rejects the configuration for it.
fn stop_timeout_ms(timeout: Duration) -> u64 {
    let millis = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    millis.min(MAX_STOP_TIMEOUT_MS)
}

/// The VM id as a hostname: lowercase, `_` → `-`, anything outside
/// `[a-z0-9-]` → `-`, trimmed of `-`, cut to 63 characters, `"vm"` if empty.
///
/// The result is trimmed of `-` again after the cut, so a truncation can never
/// produce a hostname the init rejects for ending in `-`.
#[must_use]
pub fn hostname_for(id: &VmId) -> String {
    let mapped: String =
        id.0.chars()
            .map(|character| {
                let lower = character.to_ascii_lowercase();
                if lower.is_ascii_lowercase() || lower.is_ascii_digit() || lower == '-' {
                    lower
                } else {
                    '-'
                }
            })
            .collect();
    let trimmed = mapped.trim_matches('-');
    let cut: String = trimmed.chars().take(MAX_HOSTNAME_CHARS).collect();
    let cut = cut.trim_end_matches('-');
    if cut.is_empty() {
        "vm".to_string()
    } else {
        cut.to_string()
    }
}

/// Formats a MAC address the way the VMM API wants it.
fn format_mac(mac: MacAddr) -> String {
    let [a, b, c, d, e, f] = mac.0;
    format!("{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}")
}

/// The checks `create` applies to a spec before it touches the host
/// (§3.3, §3.6).
///
/// # Errors
///
/// `Conflict` for an egress-only VM that carries an isolation group, and for
/// every work-disk rule [`validate_work_disks`] enforces.
pub fn validate_spec(spec: &VmSpec) -> Result<(), EngineError> {
    if spec.net.egress_only && spec.net.isolation_group.is_some() {
        return Err(EngineError::Conflict(format!(
            "VM {} is egress only and cannot carry an isolation group",
            spec.id.0
        )));
    }
    validate_work_disks(spec)
}

/// Checks the work disks of one spec (§3.6).
///
/// A work-disk path is absolute; its serial is 1 to 20 bytes of
/// `[A-Za-z0-9_-]` and not one of the engine's own serials; two work disks of
/// one spec share neither a path nor a serial; and each size is at least
/// 1 GiB.
///
/// # Errors
///
/// `Conflict` naming the work disk and the rule it breaks.
pub fn validate_work_disks(spec: &VmSpec) -> Result<(), EngineError> {
    let own_serials = [
        quoro_init::SERIAL_ROOT,
        quoro_init::SERIAL_DATA,
        quoro_init::SERIAL_CONFIG,
    ];
    for (index, disk) in spec.work_disks.iter().enumerate() {
        let label = format!("work disk {index} ({})", disk.serial);
        if !disk.path.is_absolute() {
            return Err(EngineError::Conflict(format!(
                "VM {}: {label} path {} is not absolute",
                spec.id.0,
                disk.path.display()
            )));
        }
        let serial = disk.serial.as_str();
        if serial.is_empty() || serial.len() > MAX_WORK_DISK_SERIAL {
            return Err(EngineError::Conflict(format!(
                "VM {}: {label} serial must be 1 to {MAX_WORK_DISK_SERIAL} bytes",
                spec.id.0
            )));
        }
        if !serial
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(EngineError::Conflict(format!(
                "VM {}: {label} serial may only contain A-Z, a-z, 0-9, '_' and '-'",
                spec.id.0
            )));
        }
        if own_serials.contains(&serial) {
            return Err(EngineError::Conflict(format!(
                "VM {}: {label} serial is one the engine uses itself",
                spec.id.0
            )));
        }
        if disk.size_gb < 1 {
            return Err(EngineError::Conflict(format!(
                "VM {}: {label} size_gb must be at least 1",
                spec.id.0
            )));
        }
        for other in &spec.work_disks[..index] {
            if other.path == disk.path {
                return Err(EngineError::Conflict(format!(
                    "VM {}: two work disks name the same path {}",
                    spec.id.0,
                    disk.path.display()
                )));
            }
            if other.serial == disk.serial {
                return Err(EngineError::Conflict(format!(
                    "VM {}: two work disks use the serial {}",
                    spec.id.0, disk.serial
                )));
            }
        }
    }
    Ok(())
}

/// The body of `PUT /api/v1/vm.create` for Cloud Hypervisor v54 (contract §5).
///
/// All paths are used as given. `tap` is the TAP device the caller created on
/// the node bridge.
///
/// `serial` comes from [`VmSpec::serial`] (section 3.19): `Off` is the pre-M4
/// default and still means "append to `paths.serial_log`", because a
/// `spec.json` written before M4 reads back as `Off` and its VM logged its boot
/// there; turning that into a VM with no serial device would silently lose the
/// boot diagnostics of every VM created before M4. A spec that still uses the
/// superseded [`ConsoleMode::Serial`] keeps getting a serial socket at its
/// console socket, as it did before `serial` existed. `create` rejects a spec
/// that sets both ([`check_consoles`]).
///
/// `console` follows [`VmSpec::console`] and is independent of `serial`, so a
/// VM can carry the app's tagged output and a serial socket at once (M4).
/// `vsock` is attached only when the spec names it (section 3.18).
#[must_use]
pub fn vm_create_body(spec: &VmSpec, paths: &VmPaths, boot: &BootFiles<'_>, tap: &str) -> Value {
    let serial = match &spec.serial {
        SerialMode::Socket { path } => json!({"mode": "Socket", "socket": path}),
        SerialMode::File { path } => json!({"mode": "File", "file": path}),
        SerialMode::Off => match &spec.console {
            ConsoleMode::Serial { socket } => json!({"mode": "Socket", "socket": socket}),
            ConsoleMode::Off | ConsoleMode::VirtioConsole { .. } => {
                json!({"mode": "File", "file": paths.serial_log})
            }
        },
    };
    let console = match &spec.console {
        ConsoleMode::VirtioConsole { socket } => json!({"mode": "Socket", "socket": socket}),
        ConsoleMode::Off | ConsoleMode::Serial { .. } => json!({"mode": "Off"}),
    };

    let mut disks = vec![
        json!({
            "id": "root",
            "path": boot.rootfs,
            "readonly": true,
            "image_type": "Raw",
            "serial": quoro_init::SERIAL_ROOT,
        }),
        json!({
            "id": "data",
            "path": paths.data_disk,
            "image_type": "Raw",
            "serial": quoro_init::SERIAL_DATA,
        }),
        json!({
            "id": "config",
            "path": paths.config_disk,
            "image_type": "Raw",
            "serial": quoro_init::SERIAL_CONFIG,
        }),
    ];
    // Work disks come after the engine's own, in the order the spec lists
    // them (§3.6). They are writable and outlive the VM.
    for (index, disk) in spec.work_disks.iter().enumerate() {
        disks.push(json!({
            "id": format!("work-{index}"),
            "path": disk.path,
            "image_type": "Raw",
            "serial": disk.serial,
        }));
    }

    let mut body = json!({
        "cpus": {"boot_vcpus": spec.vcpus, "max_vcpus": spec.vcpus},
        "memory": {"size": u64::from(spec.memory_mb) * MIB},
        "payload": {
            "kernel": boot.kernel,
            "initramfs": boot.initramfs,
            "cmdline": crate::boot::kernel_cmdline(),
        },
        "disks": disks,
        "net": [{"id": "net0", "tap": tap, "mac": format_mac(spec.net.mac)}],
        "serial": serial,
        "console": console,
        "rng": {"src": "/dev/urandom"},
    });
    if let Some(vsock) = &spec.vsock {
        // Hybrid vsock: one UNIX socket on the host, no vhost device
        // (`docs/research/m4-terminal-and-console.md`).
        body["vsock"] = json!({"cid": vsock.cid, "socket": vsock.socket});
    }
    body
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::{
        BootFiles, check_consoles, guest_config, hostname_for, stop_timeout_ms, validate_spec,
        validate_work_disks, vm_create_body,
    };
    use crate::ch::net::Subnet;
    use crate::ch::state::VmPaths;
    use crate::{
        ConsoleMode, EngineError, ImageDigest, InitConfig, MacAddr, NetAttach, SerialMode, VmId,
        VmSpec, VolumeAttach, VolumeId, VsockAttach, WorkDisk,
    };

    fn subnet() -> Subnet {
        Subnet {
            network: "10.42.1.0".parse().expect("network"),
            prefix_len: 24,
        }
    }

    fn make_spec(console: ConsoleMode) -> VmSpec {
        VmSpec {
            id: VmId("vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string()),
            vcpus: 2,
            memory_mb: 512,
            rootfs: ImageDigest("sha256:bb".to_string()),
            disk_gb: 1,
            volumes: vec![VolumeAttach {
                volume_id: VolumeId("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string()),
                mount_path: "/data".to_string(),
            }],
            net: NetAttach {
                ip: "10.42.1.7".parse().expect("ip"),
                gateway: "10.42.1.1".parse().expect("gateway"),
                mac: MacAddr([0x02, 0, 0, 0, 0, 1]),
                isolation_group: None,
                egress_only: false,
            },
            init: InitConfig {
                entrypoint: vec!["/app/server".to_string()],
                cmd: vec!["--port".to_string(), "8080".to_string()],
                env: vec![
                    ("PORT".to_string(), "8080".to_string()),
                    ("RUST_LOG".to_string(), "info".to_string()),
                ],
                user: Some("nobody:nogroup".to_string()),
                workdir: Some("/app".to_string()),
            },
            console,
            serial: SerialMode::Off,
            vsock: None,
            work_disks: Vec::new(),
        }
    }

    fn paths() -> VmPaths {
        VmPaths::new(
            Path::new("/var/lib/quoro/vms"),
            &VmId("vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string()),
        )
    }

    fn boot() -> (PathBuf, PathBuf, PathBuf) {
        (
            PathBuf::from("/var/lib/quoro/kernel/vmlinux"),
            PathBuf::from("/var/lib/quoro/boot/initramfs"),
            PathBuf::from("/var/lib/quoro/images/sha256-bb/rootfs.img"),
        )
    }

    #[test]
    fn guest_config_maps_every_field() {
        let spec = make_spec(ConsoleMode::VirtioConsole {
            socket: PathBuf::from("/run/quoro/vm/console.sock"),
        });
        let config = guest_config(
            &spec,
            &subnet(),
            &[
                "1.1.1.1".parse().expect("dns"),
                "9.9.9.9".parse().expect("dns"),
            ],
            Duration::from_secs(10),
        )
        .expect("guest config");

        assert_eq!(
            config.args,
            vec![
                "/app/server".to_string(),
                "--port".to_string(),
                "8080".to_string()
            ]
        );
        assert_eq!(config.env, spec.init.env);
        assert_eq!(config.user.as_deref(), Some("nobody:nogroup"));
        assert_eq!(config.workdir.as_deref(), Some("/app"));
        assert_eq!(
            config.hostname.as_deref(),
            Some("vm-01hx8z4q2m9w7t3yv6b1c0d5ef")
        );
        assert_eq!(config.log_console, quoro_init::LogConsole::Virtio);
        let net = config.net.as_ref().expect("net");
        assert_eq!(net.mac, [0x02, 0, 0, 0, 0, 1]);
        assert_eq!(net.address, "10.42.1.7".parse::<Ipv4Addr>().expect("ip"));
        assert_eq!(net.prefix_len, 24);
        assert_eq!(
            net.gateway,
            "10.42.1.1".parse::<Ipv4Addr>().expect("gateway")
        );
        assert_eq!(
            config.dns,
            vec![
                "1.1.1.1".parse::<Ipv4Addr>().expect("dns"),
                "9.9.9.9".parse::<Ipv4Addr>().expect("dns")
            ]
        );
        assert_eq!(config.stop_timeout_ms, 10_000);
        assert!(config.encode().is_ok());
    }

    #[test]
    fn guest_config_console_mapping() {
        let off = guest_config(&make_spec(ConsoleMode::Off), &subnet(), &[], Duration::ZERO)
            .expect("guest config");
        assert_eq!(off.log_console, quoro_init::LogConsole::None);

        let serial = guest_config(
            &make_spec(ConsoleMode::Serial {
                socket: PathBuf::from("/run/quoro/vm/serial.sock"),
            }),
            &subnet(),
            &[],
            Duration::ZERO,
        )
        .expect("guest config");
        assert_eq!(serial.log_console, quoro_init::LogConsole::Serial);
    }

    #[test]
    fn guest_config_rejects_an_empty_command() {
        let mut spec = make_spec(ConsoleMode::Off);
        spec.init.entrypoint.clear();
        spec.init.cmd.clear();
        match guest_config(&spec, &subnet(), &[], Duration::ZERO) {
            Err(EngineError::Conflict(reason)) => assert_eq!(
                reason,
                "VM vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF has no command: entrypoint and cmd are both empty"
            ),
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[test]
    fn guest_config_with_only_a_cmd() {
        let mut spec = make_spec(ConsoleMode::Off);
        spec.init.entrypoint.clear();
        let config = guest_config(&spec, &subnet(), &[], Duration::ZERO).expect("guest config");
        assert_eq!(config.args, vec!["--port".to_string(), "8080".to_string()]);
    }

    #[test]
    fn guest_config_reports_an_unencodable_configuration() {
        let mut spec = make_spec(ConsoleMode::Off);
        spec.init.user = Some("bad user".to_string());
        match guest_config(&spec, &subnet(), &[], Duration::ZERO) {
            Err(EngineError::Conflict(reason)) => {
                assert!(
                    reason.starts_with("invalid guest configuration: "),
                    "{reason}"
                );
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        // A workdir that is not absolute is rejected by the init's rules.
        let mut spec = make_spec(ConsoleMode::Off);
        spec.init.workdir = Some("relative".to_string());
        assert!(matches!(
            guest_config(&spec, &subnet(), &[], Duration::ZERO),
            Err(EngineError::Conflict(reason))
                if reason.starts_with("invalid guest configuration: ")
        ));
    }

    #[test]
    fn stop_timeout_is_saturating_and_capped() {
        assert_eq!(stop_timeout_ms(Duration::ZERO), 0);
        assert_eq!(stop_timeout_ms(Duration::from_millis(1500)), 1500);
        assert_eq!(stop_timeout_ms(Duration::from_secs(86_401)), 86_400_000);
        assert_eq!(stop_timeout_ms(Duration::MAX), 86_400_000);
    }

    #[test]
    fn hostname_rules() {
        assert_eq!(hostname_for(&VmId("vm_suite".to_string())), "vm-suite");
        assert_eq!(
            hostname_for(&VmId("VM_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string())),
            "vm-01hx8z4q2m9w7t3yv6b1c0d5ef"
        );
        assert_eq!(hostname_for(&VmId("__a.b__".to_string())), "a-b");
        assert_eq!(hostname_for(&VmId("___".to_string())), "vm");
        assert_eq!(hostname_for(&VmId(String::new())), "vm");
        assert_eq!(hostname_for(&VmId("--a--".to_string())), "a");

        let long = hostname_for(&VmId("a".repeat(80)));
        assert_eq!(long.len(), 63);
        assert!(long.chars().all(|c| c == 'a'));

        // A cut that lands on '-' is trimmed, so the init accepts it.
        let dashes = format!("{}-b", "a".repeat(63));
        let cut = hostname_for(&VmId(dashes));
        assert_eq!(cut, "a".repeat(63));
        assert!(!cut.ends_with('-'));
    }

    #[test]
    fn create_body_for_off() {
        let spec = make_spec(ConsoleMode::Off);
        let paths = paths();
        let (kernel, initramfs, rootfs) = boot();
        let body = vm_create_body(
            &spec,
            &paths,
            &BootFiles {
                kernel: &kernel,
                initramfs: &initramfs,
                rootfs: &rootfs,
            },
            "tap0",
        );

        assert_eq!(
            body,
            serde_json::json!({
                "cpus": {"boot_vcpus": 2, "max_vcpus": 2},
                "memory": {"size": 536_870_912_u64},
                "payload": {
                    "kernel": "/var/lib/quoro/kernel/vmlinux",
                    "initramfs": "/var/lib/quoro/boot/initramfs",
                    "cmdline": crate::boot::kernel_cmdline(),
                },
                "disks": [
                    {
                        "id": "root",
                        "path": "/var/lib/quoro/images/sha256-bb/rootfs.img",
                        "readonly": true,
                        "image_type": "Raw",
                        "serial": "quoro-root",
                    },
                    {
                        "id": "data",
                        "path": "/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/data.img",
                        "image_type": "Raw",
                        "serial": "quoro-data",
                    },
                    {
                        "id": "config",
                        "path": "/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/config.img",
                        "image_type": "Raw",
                        "serial": "quoro-config",
                    },
                ],
                "net": [{"id": "net0", "tap": "tap0", "mac": "02:00:00:00:00:01"}],
                "serial": {
                    "mode": "File",
                    "file": "/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/serial.log",
                },
                "console": {"mode": "Off"},
                "rng": {"src": "/dev/urandom"},
            })
        );
    }

    #[test]
    fn create_body_for_serial() {
        let console = ConsoleMode::Serial {
            socket: PathBuf::from("/run/quoro/vm/serial.sock"),
        };
        let paths = paths();
        let (kernel, initramfs, rootfs) = boot();
        let body = vm_create_body(
            &make_spec(console),
            &paths,
            &BootFiles {
                kernel: &kernel,
                initramfs: &initramfs,
                rootfs: &rootfs,
            },
            "tap7",
        );
        assert_eq!(
            body["serial"],
            serde_json::json!({"mode": "Socket", "socket": "/run/quoro/vm/serial.sock"})
        );
        assert_eq!(body["console"], serde_json::json!({"mode": "Off"}));
        assert_eq!(body["net"][0]["tap"], "tap7");
    }

    #[test]
    fn create_body_for_virtio_console() {
        let console = ConsoleMode::VirtioConsole {
            socket: PathBuf::from("/run/quoro/vm/console.sock"),
        };
        let paths = paths();
        let (kernel, initramfs, rootfs) = boot();
        let body = vm_create_body(
            &make_spec(console),
            &paths,
            &BootFiles {
                kernel: &kernel,
                initramfs: &initramfs,
                rootfs: &rootfs,
            },
            "tap1",
        );
        assert_eq!(
            body["console"],
            serde_json::json!({"mode": "Socket", "socket": "/run/quoro/vm/console.sock"})
        );
        assert_eq!(
            body["serial"],
            serde_json::json!({
                "mode": "File",
                "file": "/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/serial.log",
            })
        );
        assert_eq!(body["memory"]["size"], 536_870_912_u64);
    }

    #[test]
    fn create_body_takes_the_serial_port_from_the_spec() {
        let paths = paths();
        let (kernel, initramfs, rootfs) = boot();
        let boot = BootFiles {
            kernel: &kernel,
            initramfs: &initramfs,
            rootfs: &rootfs,
        };

        // A serial socket is independent of the app's console.
        let mut spec = make_spec(ConsoleMode::VirtioConsole {
            socket: PathBuf::from("/run/quoro/vm/console.sock"),
        });
        spec.serial = SerialMode::Socket {
            path: PathBuf::from("/run/quoro/vm/serial.sock"),
        };
        let body = vm_create_body(&spec, &paths, &boot, "tap0");
        assert_eq!(
            body["serial"],
            serde_json::json!({"mode": "Socket", "socket": "/run/quoro/vm/serial.sock"})
        );
        assert_eq!(
            body["console"],
            serde_json::json!({"mode": "Socket", "socket": "/run/quoro/vm/console.sock"})
        );

        // A file serial port names the file, not `serial.log`.
        spec.serial = SerialMode::File {
            path: PathBuf::from("/var/log/quoro/serial.log"),
        };
        let body = vm_create_body(&spec, &paths, &boot, "tap0");
        assert_eq!(
            body["serial"],
            serde_json::json!({"mode": "File", "file": "/var/log/quoro/serial.log"})
        );

        // An explicit `Off` keeps the pre-M4 `serial.log` (see the note on
        // `vm_create_body`), and so does the superseded serial console.
        spec.serial = SerialMode::Off;
        let body = vm_create_body(&spec, &paths, &boot, "tap0");
        assert_eq!(
            body["serial"],
            serde_json::json!({
                "mode": "File",
                "file": "/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/serial.log",
            })
        );
    }

    #[test]
    fn create_body_for_a_vsock_device() {
        let mut spec = make_spec(ConsoleMode::Off);
        let paths = paths();
        let (kernel, initramfs, rootfs) = boot();
        let boot = BootFiles {
            kernel: &kernel,
            initramfs: &initramfs,
            rootfs: &rootfs,
        };
        assert!(
            vm_create_body(&spec, &paths, &boot, "tap0")
                .get("vsock")
                .is_none(),
            "no vsock key without a device"
        );

        spec.vsock = Some(VsockAttach {
            cid: 3,
            socket: PathBuf::from("/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/vsock.sock"),
        });
        let body = vm_create_body(&spec, &paths, &boot, "tap0");
        assert_eq!(
            body["vsock"],
            serde_json::json!({
                "cid": 3,
                "socket": "/var/lib/quoro/vms/vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF/vsock.sock",
            })
        );
    }

    #[test]
    fn a_serial_port_has_one_owner() {
        let mut spec = make_spec(ConsoleMode::Serial {
            socket: PathBuf::from("/run/quoro/vm/serial.sock"),
        });
        // The superseded console mode on its own is still accepted.
        assert!(check_consoles(&spec).is_ok());

        spec.serial = SerialMode::File {
            path: PathBuf::from("/var/log/quoro/serial.log"),
        };
        match check_consoles(&spec) {
            Err(EngineError::Conflict(reason)) => {
                assert!(
                    reason.starts_with("VM vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF "),
                    "{reason}"
                );
                assert!(reason.contains("one owner"), "{reason}");
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        // A virtio-console with a serial socket is the M4 shape.
        let mut spec = make_spec(ConsoleMode::VirtioConsole {
            socket: PathBuf::from("/run/quoro/vm/console.sock"),
        });
        spec.serial = SerialMode::Socket {
            path: PathBuf::from("/run/quoro/vm/serial.sock"),
        };
        assert!(check_consoles(&spec).is_ok());
    }

    fn work_disk(path: &str, size_gb: u32, serial: &str) -> WorkDisk {
        WorkDisk {
            path: PathBuf::from(path),
            size_gb,
            serial: serial.to_string(),
        }
    }

    #[test]
    fn create_body_appends_work_disks_after_the_engine_disks() {
        let mut spec = make_spec(ConsoleMode::Off);
        spec.work_disks = vec![
            work_disk("/var/lib/quoro/work/quoro-work.img", 12, "quoro-work"),
            work_disk("/var/lib/quoro/work/src.img", 1, "quoro-src"),
            work_disk("/var/lib/quoro/work/out.img", 1, "quoro-out"),
        ];
        let paths = paths();
        let (kernel, initramfs, rootfs) = boot();
        let body = vm_create_body(
            &spec,
            &paths,
            &BootFiles {
                kernel: &kernel,
                initramfs: &initramfs,
                rootfs: &rootfs,
            },
            "tap2",
        );
        let disks = body["disks"].as_array().expect("disks");
        let ids: Vec<&str> = disks
            .iter()
            .map(|disk| disk["id"].as_str().expect("id"))
            .collect();
        assert_eq!(
            ids,
            vec!["root", "data", "config", "work-0", "work-1", "work-2"]
        );
        for (index, disk) in spec.work_disks.iter().enumerate() {
            let entry = &disks[index + 3];
            assert_eq!(entry["path"], serde_json::json!(disk.path));
            assert_eq!(entry["serial"], serde_json::json!(disk.serial));
            assert_eq!(entry["image_type"], "Raw");
            assert!(entry.get("readonly").is_none(), "a work disk is writable");
        }
        assert_eq!(disks[0]["readonly"], true);
    }

    #[test]
    fn work_disks_are_validated() {
        let mut spec = make_spec(ConsoleMode::Off);
        spec.work_disks = vec![work_disk("/var/lib/quoro/work/work.img", 8, "quoro-work")];
        validate_work_disks(&spec).expect("a valid work disk");
        validate_spec(&spec).expect("a valid spec");

        let mut relative = spec.clone();
        relative.work_disks[0].path = PathBuf::from("work.img");
        assert!(matches!(
            validate_work_disks(&relative),
            Err(EngineError::Conflict(_))
        ));

        for serial in ["", "abcdefghijklmnopqrstu", "bad serial", "quo:ro"] {
            let mut bad = spec.clone();
            bad.work_disks[0].serial = serial.to_string();
            assert!(
                matches!(validate_work_disks(&bad), Err(EngineError::Conflict(_))),
                "{serial:?} should be refused"
            );
        }

        for own in [
            quoro_init::SERIAL_ROOT,
            quoro_init::SERIAL_DATA,
            quoro_init::SERIAL_CONFIG,
        ] {
            let mut bad = spec.clone();
            bad.work_disks[0].serial = own.to_string();
            assert!(
                matches!(validate_work_disks(&bad), Err(EngineError::Conflict(_))),
                "{own:?} should be refused"
            );
        }

        let mut zero = spec.clone();
        zero.work_disks[0].size_gb = 0;
        assert!(matches!(
            validate_work_disks(&zero),
            Err(EngineError::Conflict(_))
        ));

        let mut duplicate_path = spec.clone();
        duplicate_path
            .work_disks
            .push(work_disk("/var/lib/quoro/work/work.img", 1, "quoro-src"));
        assert!(matches!(
            validate_work_disks(&duplicate_path),
            Err(EngineError::Conflict(_))
        ));

        let mut duplicate_serial = spec.clone();
        duplicate_serial.work_disks.push(work_disk(
            "/var/lib/quoro/work/other.img",
            1,
            "quoro-work",
        ));
        assert!(matches!(
            validate_work_disks(&duplicate_serial),
            Err(EngineError::Conflict(_))
        ));

        // The maximum serial length is accepted.
        let mut longest = spec.clone();
        longest.work_disks[0].serial = "abcdefghijklmnopqrst".to_string();
        validate_work_disks(&longest).expect("20 bytes is the limit");
    }

    #[test]
    fn an_egress_only_vm_cannot_carry_an_isolation_group() {
        let mut spec = make_spec(ConsoleMode::Off);
        spec.net.egress_only = true;
        spec.net.isolation_group = Some("prj_01".to_string());
        match validate_spec(&spec) {
            Err(EngineError::Conflict(reason)) => {
                assert!(reason.contains("egress only"), "{reason}");
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        spec.net.isolation_group = None;
        validate_spec(&spec).expect("egress only alone is valid");
    }
}

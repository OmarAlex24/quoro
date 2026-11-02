// SPDX-License-Identifier: AGPL-3.0-only
//! The host facts the engine probes, and the mapping of Cloud Hypervisor's
//! counters document (contract §3.2, §3.15).

use std::fs;

use serde_json::Value;

use crate::{EngineError, VmCounters};

/// `/dev/kvm`, opened read-write exactly the way the engine needs it.
const KVM: &str = "/dev/kvm";
/// The file holding the running kernel's release.
const OSRELEASE: &str = "/proc/sys/kernel/osrelease";
/// The file holding the CPU flags of the host's processors.
const CPUINFO: &str = "/proc/cpuinfo";
/// The file holding the host's memory summary.
const MEMINFO: &str = "/proc/meminfo";
/// Clock ticks per second assumed by `/proc/<pid>/stat`; Linux's `USER_HZ`.
const CLOCK_TICKS_PER_SECOND: u64 = 100;
/// A kernel with this version or newer runs Cloud Hypervisor v54 (F-0036).
const MINIMUM_KERNEL: (u32, u32) = (6, 8);

/// True when `/dev/kvm` can be opened read-write.
#[must_use]
pub(super) fn kvm_available() -> bool {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(KVM)
        .is_ok()
}

/// The running kernel's release, for example `6.8.0-1-amd64`.
///
/// # Errors
///
/// `Io` when `/proc/sys/kernel/osrelease` cannot be read.
pub(super) fn kernel_release() -> Result<String, EngineError> {
    let release = fs::read_to_string(OSRELEASE).map_err(|err| io_error(OSRELEASE, &err))?;
    Ok(release.trim().to_string())
}

/// The major and minor version at the start of `release`, if it has both.
#[must_use]
pub(super) fn kernel_version(release: &str) -> Option<(u32, u32)> {
    let mut parts = release.trim().split('.');
    let major = leading_number(parts.next()?)?;
    let minor = leading_number(parts.next()?)?;
    Some((major, minor))
}

/// The decimal number `text` starts with.
fn leading_number(text: &str) -> Option<u32> {
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// True when the release is at least the minimum Cloud Hypervisor v54 needs.
#[must_use]
pub(super) fn kernel_supported(release: &str) -> bool {
    matches!(kernel_version(release), Some(version) if version >= MINIMUM_KERNEL)
}

/// The CPU flags line from `/proc/cpuinfo` (the first processor's).
///
/// # Errors
///
/// `Io` when `/proc/cpuinfo` cannot be read.
pub(super) fn cpu_flags() -> Result<String, EngineError> {
    let text = fs::read_to_string(CPUINFO).map_err(|err| io_error(CPUINFO, &err))?;
    let flags = text
        .lines()
        .find_map(|line| line.strip_prefix("flags"))
        .and_then(|rest| rest.split_once(':'))
        .map(|(_, flags)| flags.trim().to_string());
    Ok(flags.unwrap_or_default())
}

/// True when the space-separated `flags` contain the whole word `flag`.
#[must_use]
pub(super) fn cpu_has_flag(flags: &str, flag: &str) -> bool {
    flags.split_whitespace().any(|word| word == flag)
}

/// The host's `MemTotal` in kilobytes.
///
/// # Errors
///
/// `Io` when `/proc/meminfo` cannot be read or has no `MemTotal` line.
pub(super) fn mem_total_kb() -> Result<u64, EngineError> {
    let text = fs::read_to_string(MEMINFO).map_err(|err| io_error(MEMINFO, &err))?;
    parse_mem_total_kb(&text)
        .ok_or_else(|| EngineError::Io(format!("{MEMINFO}: no MemTotal line with a size in kB")))
}

/// The host's `MemTotal` in megabytes (rounded down).
///
/// # Errors
///
/// What [`mem_total_kb`] reports.
pub(super) fn mem_total_mb() -> Result<u64, EngineError> {
    Ok(mem_total_kb()? / 1024)
}

/// Parses the `MemTotal` line of `/proc/meminfo` into kilobytes.
#[must_use]
pub(super) fn parse_mem_total_kb(text: &str) -> Option<u64> {
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let value = fields.next()?.parse::<u64>().ok()?;
        if fields.next() == Some("kB") {
            return Some(value);
        }
        return None;
    }
    None
}

/// A live process's consumed CPU time in milliseconds, from
/// `/proc/<pid>/stat` (utime + stime, 100 ticks a second).
#[must_use]
pub(super) fn process_cpu_time_ms(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    let rest = stat.get(close + 1..)?;
    // Fields after the command name start at the state (field 3); utime is
    // field 14 and stime field 15, indices 11 and 12 here.
    let mut fields = rest.split_whitespace();
    let _state = fields.next()?;
    let utime = fields.nth(10)?.parse::<u64>().ok()?;
    let stime = fields.next()?.parse::<u64>().ok()?;
    Some(
        utime
            .saturating_add(stime)
            .saturating_mul(1000 / CLOCK_TICKS_PER_SECOND),
    )
}

/// A live process's resident memory in bytes, from `/proc/<pid>/status`.
#[must_use]
pub(super) fn process_rss_bytes(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let kilobytes = fields.next()?.parse::<u64>().ok()?;
        return Some(kilobytes.saturating_mul(1024));
    }
    None
}

/// Maps Cloud Hypervisor's counters document to [`VmCounters`].
///
/// The document is an object of device-id → counter object; `read_bytes` and
/// `write_bytes` of every block device are summed, `rx_bytes` and `tx_bytes` of
/// every net device. Unexpected or missing fields count as zero, so a backend
/// that renames one group still reports the others (§3.15, best effort).
#[must_use]
pub(super) fn counters_from_ch(document: &Value) -> VmCounters {
    let mut counters = VmCounters {
        cpu_time_ms: 0,
        memory_rss_bytes: 0,
        net_rx_bytes: 0,
        net_tx_bytes: 0,
        block_read_bytes: 0,
        block_write_bytes: 0,
    };
    let Some(devices) = document.as_object() else {
        return counters;
    };
    for device in devices.values() {
        let Some(device) = device.as_object() else {
            continue;
        };
        counters.block_read_bytes = counters
            .block_read_bytes
            .saturating_add(counter(device.get("read_bytes")));
        counters.block_write_bytes = counters
            .block_write_bytes
            .saturating_add(counter(device.get("write_bytes")));
        counters.net_rx_bytes = counters
            .net_rx_bytes
            .saturating_add(counter(device.get("rx_bytes")));
        counters.net_tx_bytes = counters
            .net_tx_bytes
            .saturating_add(counter(device.get("tx_bytes")));
    }
    counters
}

/// One counter value, zero when it is absent or not a number.
fn counter(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or(0)
}

/// An `Io` error naming the file it happened on.
fn io_error(path: impl std::fmt::Display, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{path}: {err}"))
}

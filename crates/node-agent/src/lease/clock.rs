// SPDX-License-Identifier: AGPL-3.0-only
//! The host clock that lets a held `expiring` lease survive an agent restart
//! within the same host boot (`node-agent-protocol.md` section 8.4 "Agent
//! restart", ADR 0072).
//!
//! The monotonic instants the lease machine uses mean nothing to the next
//! agent process, so a lease's validity is stored as a deadline on
//! `CLOCK_BOOTTIME` next to the kernel's boot id. A restarted agent on the same
//! boot reads `CLOCK_BOOTTIME` again and knows exactly how much validity is
//! left; after a reboot the boot id differs and the lease is fenced.
//! `CLOCK_BOOTTIME` keeps counting while the machine is suspended, so a suspend
//! can only shorten what is left.
//!
//! Every conversion errs early: a stored deadline is never later than the
//! in-memory one it came from, and a restored one never later than the stored
//! one.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

/// Where the kernel publishes the id of the current boot.
#[cfg(target_os = "linux")]
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// The two host facts a stored lease validity depends on.
pub trait HostClock: Send + Sync + std::fmt::Debug {
    /// The kernel's boot id, or `None` when the host cannot tell.
    fn boot_id(&self) -> Option<String>;
    /// `CLOCK_BOOTTIME`, or `None` when the host has no such clock.
    fn boottime(&self) -> Option<Duration>;
}

/// The running host's clock: the kernel boot id and `CLOCK_BOOTTIME` on
/// Linux, nothing elsewhere (so every held `expiring` lease is fenced on
/// restart, the conservative M0 behaviour).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemHostClock;

impl HostClock for SystemHostClock {
    #[cfg(target_os = "linux")]
    fn boot_id(&self) -> Option<String> {
        std::fs::read_to_string(BOOT_ID_PATH)
            .ok()
            .map(|text| text.trim().to_owned())
            .filter(|id| !id.is_empty())
    }

    #[cfg(not(target_os = "linux"))]
    fn boot_id(&self) -> Option<String> {
        None
    }

    #[cfg(target_os = "linux")]
    fn boottime(&self) -> Option<Duration> {
        let now = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
        let secs = u64::try_from(now.tv_sec).ok()?;
        let nanos = u32::try_from(now.tv_nsec).ok()?;
        Some(Duration::new(secs, nanos))
    }

    #[cfg(not(target_os = "linux"))]
    fn boottime(&self) -> Option<Duration> {
        None
    }
}

/// A host clock a test sets by hand: a boot id and a `CLOCK_BOOTTIME` that
/// follows the tokio clock from a chosen starting value.
#[derive(Clone, Debug)]
pub struct ManualHostClock {
    inner: Arc<Mutex<ManualInner>>,
}

#[derive(Debug)]
struct ManualInner {
    boot_id: Option<String>,
    /// `CLOCK_BOOTTIME` at `origin`.
    at_origin: Duration,
    origin: Instant,
}

impl ManualHostClock {
    /// A clock on boot `boot_id` whose `CLOCK_BOOTTIME` reads `boottime` now.
    #[must_use]
    pub fn new(boot_id: Option<&str>, boottime: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ManualInner {
                boot_id: boot_id.map(str::to_owned),
                at_origin: boottime,
                origin: Instant::now(),
            })),
        }
    }

    /// Models a reboot: a new boot id and `CLOCK_BOOTTIME` starting again
    /// from `boottime`.
    pub fn reboot(&self, boot_id: &str, boottime: Duration) {
        let mut inner = self.lock();
        inner.boot_id = Some(boot_id.to_owned());
        inner.at_origin = boottime;
        inner.origin = Instant::now();
    }

    /// Models a suspend of `slept`: `CLOCK_BOOTTIME` jumps ahead, the
    /// monotonic clock does not.
    pub fn suspend(&self, slept: Duration) {
        self.lock().at_origin += slept;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ManualInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl HostClock for ManualHostClock {
    fn boot_id(&self) -> Option<String> {
        self.lock().boot_id.clone()
    }

    fn boottime(&self) -> Option<Duration> {
        let inner = self.lock();
        Some(inner.at_origin + Instant::now().saturating_duration_since(inner.origin))
    }
}

/// A lease's remaining validity as it is stored (section 12 `leases`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredValidity {
    /// The kernel boot id the deadline belongs to.
    pub boot_id: String,
    /// `valid_until` on that boot's `CLOCK_BOOTTIME`.
    pub until_boottime: Duration,
}

/// One reading of the host clock and the monotonic clock together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostNow {
    /// The kernel boot id.
    pub boot_id: Option<String>,
    /// `CLOCK_BOOTTIME` read just before `instant`.
    boottime_before: Option<Duration>,
    /// The monotonic instant of the reading.
    pub instant: Instant,
    /// `CLOCK_BOOTTIME` read just after `instant`.
    boottime_after: Option<Duration>,
}

impl HostNow {
    /// Reads `CLOCK_BOOTTIME`, the monotonic clock, and `CLOCK_BOOTTIME`
    /// again, so each conversion can use the reading that errs early.
    #[must_use]
    pub fn read(clock: &dyn HostClock) -> Self {
        let boot_id = clock.boot_id();
        let boottime_before = clock.boottime();
        let instant = Instant::now();
        let boottime_after = clock.boottime();
        Self {
            boot_id,
            boottime_before,
            instant,
            boottime_after,
        }
    }

    /// A reading on a host that cannot tell its boot: nothing can be stored,
    /// so nothing survives a restart.
    #[must_use]
    pub fn unknown(instant: Instant) -> Self {
        Self {
            boot_id: None,
            boottime_before: None,
            instant,
            boottime_after: None,
        }
    }

    /// The stored form of a monotonic `valid_until`, or `None` when the host
    /// cannot tell its boot.
    ///
    /// It uses `CLOCK_BOOTTIME` read before the instant, so the stored
    /// deadline is never later than `valid_until`.
    #[must_use]
    pub fn store(&self, valid_until: Instant) -> Option<StoredValidity> {
        let boot_id = self.boot_id.clone()?;
        let boottime = self.boottime_before?;
        let remaining = valid_until.saturating_duration_since(self.instant);
        Some(StoredValidity {
            boot_id,
            until_boottime: boottime + remaining,
        })
    }

    /// The monotonic `valid_until` of a stored validity, when it belongs to
    /// this boot and has not run out; `None` otherwise.
    ///
    /// It uses `CLOCK_BOOTTIME` read after the instant, so the restored
    /// deadline is never later than the stored one.
    #[must_use]
    pub fn restore(&self, stored: &StoredValidity) -> Option<Instant> {
        if self.boot_id.as_deref() != Some(stored.boot_id.as_str()) {
            return None;
        }
        let boottime = self.boottime_after?;
        let remaining = stored.until_boottime.checked_sub(boottime)?;
        if remaining.is_zero() {
            return None;
        }
        Some(self.instant + remaining)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{HostNow, ManualHostClock, StoredValidity};

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[tokio::test(start_paused = true)]
    async fn a_validity_comes_back_with_what_is_left_on_the_same_boot() {
        let clock = ManualHostClock::new(Some("boot-a"), secs(1000));
        let now = HostNow::read(&clock);
        let stored = now
            .store(now.instant + secs(10))
            .expect("a Linux-like host stores");
        assert_eq!(
            stored,
            StoredValidity {
                boot_id: "boot-a".to_owned(),
                until_boottime: secs(1010),
            }
        );

        tokio::time::advance(secs(4)).await;
        let later = HostNow::read(&clock);
        assert_eq!(later.restore(&stored), Some(later.instant + secs(6)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_validity_never_survives_a_reboot() {
        let clock = ManualHostClock::new(Some("boot-a"), secs(1000));
        let now = HostNow::read(&clock);
        let stored = now.store(now.instant + secs(10)).expect("stored");
        // A fresh boot whose CLOCK_BOOTTIME is low again would make the old
        // deadline look far away; the boot id is what refuses it.
        clock.reboot("boot-b", secs(1));
        assert_eq!(HostNow::read(&clock).restore(&stored), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_suspend_only_shortens_what_is_left() {
        let clock = ManualHostClock::new(Some("boot-a"), secs(1000));
        let now = HostNow::read(&clock);
        let stored = now.store(now.instant + secs(10)).expect("stored");
        clock.suspend(secs(7));
        let later = HostNow::read(&clock);
        assert_eq!(later.restore(&stored), Some(later.instant + secs(3)));
        clock.suspend(secs(3));
        assert_eq!(HostNow::read(&clock).restore(&stored), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_host_without_a_boot_id_stores_nothing() {
        let clock = ManualHostClock::new(None, secs(1000));
        let now = HostNow::read(&clock);
        assert_eq!(now.store(now.instant + secs(10)), None);
        let unknown = HostNow::unknown(Instant::now());
        assert_eq!(unknown.store(unknown.instant + secs(10)), None);
        let stored = StoredValidity {
            boot_id: "boot-a".to_owned(),
            until_boottime: secs(2000),
        };
        assert_eq!(unknown.restore(&stored), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_already_passed_stores_no_time_left() {
        let clock = ManualHostClock::new(Some("boot-a"), secs(1000));
        let now = HostNow::read(&clock);
        tokio::time::advance(secs(5)).await;
        let past = now.instant;
        let later = HostNow::read(&clock);
        let stored = later.store(past).expect("stored");
        assert_eq!(stored.until_boottime, secs(1005));
        assert_eq!(later.restore(&stored), None);
    }
}

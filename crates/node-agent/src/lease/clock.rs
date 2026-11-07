// SPDX-License-Identifier: AGPL-3.0-only
//! The host clock behind the lease clock of `node-agent-protocol.md`
//! section 8.4: a monotonic clock that keeps counting while the host is
//! suspended, `CLOCK_BOOTTIME` on Linux (ADR 0240).
//!
//! The lease machine's instants are tokio instants, whose clock stops during a
//! suspend (`CLOCK_MONOTONIC`). [`SuspendWatch`] measures how far
//! `CLOCK_BOOTTIME` has moved beyond that clock, which is the time the host
//! spent suspended, so the lease service can bring every `expiring` deadline
//! earlier by it ("Suspend"). [`HostClock::sleep`] waits on `CLOCK_BOOTTIME`,
//! so a check that waits on it runs as soon as the host wakes.
//!
//! The same clock lets a held `expiring` lease survive an agent restart within
//! the same host boot ("Agent restart", ADR 0072). The monotonic instants mean
//! nothing to the next agent process, so a lease's validity is stored as a
//! deadline on `CLOCK_BOOTTIME` next to the kernel's boot id, taken from the
//! reading made when its renewal was sent. A restarted agent on the same boot
//! reads `CLOCK_BOOTTIME` again and knows exactly how much validity is left;
//! after a reboot the boot id differs and the lease is fenced.
//!
//! Every conversion errs early: a stored deadline is never later than the
//! in-memory one it came from, a restored one never later than the stored
//! one, and a measured suspend never shorter than the real one.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

/// Where the kernel publishes the id of the current boot.
#[cfg(target_os = "linux")]
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// A future the lease service waits on.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// The host facts the lease clock depends on.
pub trait HostClock: Send + Sync + std::fmt::Debug {
    /// The kernel's boot id, or `None` when the host cannot tell.
    fn boot_id(&self) -> Option<String>;
    /// `CLOCK_BOOTTIME`, or `None` when the host has no such clock.
    fn boottime(&self) -> Option<Duration>;
    /// Resolves once `period` has passed on `CLOCK_BOOTTIME`, so at once when
    /// the host wakes from a suspend longer than what was left of `period`.
    ///
    /// A host without `CLOCK_BOOTTIME` waits `period` on the tokio clock: it
    /// cannot measure a suspend either (see [`SuspendWatch`]).
    fn sleep(&self, period: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(period))
    }
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

    #[cfg(target_os = "linux")]
    fn sleep(&self, period: Duration) -> Sleep {
        Box::pin(sleep_on_boottime(period))
    }
}

/// [`HostClock::sleep`] on Linux: a one-shot `timerfd` on `CLOCK_BOOTTIME`,
/// read on tokio's blocking pool. The read returns once the timer fires: after
/// `period` on `CLOCK_BOOTTIME`, so at once when the host wakes from a longer
/// suspend. A wait dropped before that leaves its thread blocked until the
/// timer fires, at most `period` later while the host runs. A timer that
/// cannot be armed or read falls back to the tokio clock, which waits as long
/// without a suspend and up to `period` after a wake-up.
#[cfg(target_os = "linux")]
async fn sleep_on_boottime(period: Duration) {
    use rustix::time::{
        Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec, timerfd_create,
        timerfd_settime,
    };

    let armed = (|| -> std::io::Result<rustix::fd::OwnedFd> {
        let fd = timerfd_create(TimerfdClockId::Boottime, TimerfdFlags::CLOEXEC)?;
        // A zero `it_value` would disarm the timer instead of firing it.
        let value = Timespec::try_from(period.max(Duration::from_micros(1)))
            .map_err(|_| std::io::Error::other("the period does not fit a timespec"))?;
        let once = Itimerspec {
            it_interval: Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: value,
        };
        timerfd_settime(&fd, TimerfdTimerFlags::empty(), &once)?;
        Ok(fd)
    })();
    let fd = match armed {
        Ok(fd) => fd,
        Err(e) => {
            timer_failed(
                &e,
                "could not arm a CLOCK_BOOTTIME timer; waiting on the monotonic clock",
            );
            tokio::time::sleep(period).await;
            return;
        }
    };
    let fired = tokio::task::spawn_blocking(move || {
        let mut expirations = [0_u8; 8];
        loop {
            match rustix::io::read(&fd, &mut expirations[..]) {
                Err(rustix::io::Errno::INTR) => {}
                read => return read.map(|_| ()),
            }
        }
    })
    .await;
    match fired {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            timer_failed(
                &e,
                "could not read a CLOCK_BOOTTIME timer; waiting on the monotonic clock",
            );
            tokio::time::sleep(period).await;
        }
        // The runtime is shutting down.
        Err(_) => tokio::time::sleep(period).await,
    }
}

/// Logs a `CLOCK_BOOTTIME` timer failure: a warning the first time, then at
/// debug level: the lease service arms a timer every 250 ms, and a host that
/// cannot arm one once is unlikely to arm the next.
#[cfg(target_os = "linux")]
fn timer_failed(error: &dyn std::fmt::Display, message: &'static str) {
    use std::sync::atomic::{AtomicBool, Ordering};

    static WARNED: AtomicBool = AtomicBool::new(false);
    if WARNED.swap(true, Ordering::Relaxed) {
        tracing::debug!(error = %error, "{message}");
    } else {
        tracing::warn!(error = %error, "{message}");
    }
}

/// A host clock a test sets by hand: a boot id and a `CLOCK_BOOTTIME` that
/// follows the tokio clock from a chosen starting value.
#[derive(Clone, Debug)]
pub struct ManualHostClock {
    inner: Arc<Mutex<ManualInner>>,
    /// Counts the suspends, so a [`HostClock::sleep`] ends at one, as a timer
    /// on `CLOCK_BOOTTIME` does when the host wakes.
    suspends: Arc<watch::Sender<u64>>,
    /// A suspend [`Self::suspend_when`] armed.
    armed: Arc<Mutex<Option<ArmedSuspend>>>,
}

/// A suspend that begins at the first read of `CLOCK_BOOTTIME` at which
/// `when` holds.
struct ArmedSuspend {
    slept: Duration,
    when: Box<dyn Fn() -> bool + Send + Sync>,
}

impl std::fmt::Debug for ArmedSuspend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArmedSuspend")
            .field("slept", &self.slept)
            .finish_non_exhaustive()
    }
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
            suspends: Arc::new(watch::Sender::new(0)),
            armed: Arc::new(Mutex::new(None)),
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

    /// Models a suspend of `slept` and the wake-up after it:
    /// `CLOCK_BOOTTIME` jumps ahead, the monotonic clock does not, and every
    /// [`HostClock::sleep`] under way ends.
    pub fn suspend(&self, slept: Duration) {
        self.lock().at_origin += slept;
        self.suspends.send_modify(|count| *count += 1);
    }

    /// Models a suspend of `slept` whose wake-up no timer has seen yet:
    /// `CLOCK_BOOTTIME` jumps ahead and no [`HostClock::sleep`] ends. A test
    /// uses it to hand the lease service a message before the wake-up's
    /// timer fires.
    pub fn suspend_unnoticed(&self, slept: Duration) {
        self.lock().at_origin += slept;
    }

    /// Models a suspend of `slept`, as [`Self::suspend`] does, that begins at
    /// the first read of `CLOCK_BOOTTIME` at which `when` holds, just before
    /// that read. A test uses it to put a suspend between two steps of the
    /// lease service that no test can reach between. `when` runs on every
    /// read until it holds, under whatever lock the reader holds, so it must
    /// not take the lease table's lock or read this clock.
    pub fn suspend_when(&self, slept: Duration, when: impl Fn() -> bool + Send + Sync + 'static) {
        *self.armed_lock() = Some(ArmedSuspend {
            slept,
            when: Box::new(when),
        });
    }

    /// The suspend [`Self::suspend_when`] armed, once its condition holds.
    fn fire_armed(&self) {
        let fired = {
            let mut armed = self.armed_lock();
            if armed.as_ref().is_some_and(|armed| (armed.when)()) {
                armed.take()
            } else {
                None
            }
        };
        if let Some(fired) = fired {
            self.suspend(fired.slept);
        }
    }

    fn armed_lock(&self) -> std::sync::MutexGuard<'_, Option<ArmedSuspend>> {
        self.armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        self.fire_armed();
        let inner = self.lock();
        Some(inner.at_origin + Instant::now().saturating_duration_since(inner.origin))
    }

    fn sleep(&self, period: Duration) -> Sleep {
        // Subscribing marks the current count seen, so only a later suspend
        // ends the wait.
        let mut suspends = self.suspends.subscribe();
        Box::pin(async move {
            tokio::select! {
                () = tokio::time::sleep(period) => {}
                _ = suspends.changed() => {}
            }
        })
    }
}

/// The widest spread between the two `CLOCK_BOOTTIME` reads around a
/// measurement's instant that [`SuspendWatch::suspended`] accepts at once. Two
/// reads with nothing between them take well under a microsecond.
const TIGHT_READ: Duration = Duration::from_micros(100);

/// How many times [`SuspendWatch::suspended`] reads before it accepts a wide
/// spread.
const TIGHT_READ_ATTEMPTS: u32 = 3;

/// Measures how long the host has been suspended since a reading
/// (`node-agent-protocol.md` section 8.4 "Suspend", ADR 0240).
///
/// The tokio clock stops during a suspend and `CLOCK_BOOTTIME` does not, so
/// whatever `CLOCK_BOOTTIME` gained beyond the tokio clock since the reading is
/// time spent suspended. It errs long: `CLOCK_BOOTTIME` is read before the
/// reading's instant and after each measurement's, so a measured suspend is
/// never shorter than the real one, by at most the time between two reads.
#[derive(Clone, Debug)]
pub struct SuspendWatch {
    clock: Option<Arc<dyn HostClock>>,
    /// `CLOCK_BOOTTIME` and the tokio instant of the reading.
    origin: Option<(Duration, Instant)>,
}

impl SuspendWatch {
    /// A watch that never measures a suspend: a lease table a test drives
    /// without a host clock.
    #[must_use]
    pub fn none() -> Self {
        Self {
            clock: None,
            origin: None,
        }
    }

    /// A watch that measures from `reading`, taken from `clock`. A host
    /// without `CLOCK_BOOTTIME` gets a watch that never measures a suspend.
    #[must_use]
    pub fn new(clock: Arc<dyn HostClock>, reading: &HostNow) -> Self {
        Self {
            origin: reading
                .boottime_before
                .map(|boottime| (boottime, reading.instant)),
            clock: Some(clock),
        }
    }

    /// The time the host has spent suspended since the reading.
    ///
    /// The instant is read between two reads of `CLOCK_BOOTTIME`, and the
    /// measurement uses the later one. A thread stall between the reads would
    /// count as a suspend, and the lease table keeps what it applied as the
    /// baseline for the next suspend, which would then be applied short by
    /// the stall. So a measurement whose two reads lie [`TIGHT_READ`] or more
    /// apart is taken again, a few times at most; the last one is used
    /// whatever its spread, since it errs long.
    #[must_use]
    pub fn suspended(&self) -> Duration {
        let (Some(clock), Some((boot_origin, origin))) = (&self.clock, self.origin) else {
            return Duration::ZERO;
        };
        let mut attempt = 1;
        let (instant, boottime) = loop {
            let Some(before) = clock.boottime() else {
                return Duration::ZERO;
            };
            let instant = Instant::now();
            let Some(after) = clock.boottime() else {
                return Duration::ZERO;
            };
            if after.saturating_sub(before) < TIGHT_READ || attempt == TIGHT_READ_ATTEMPTS {
                break (instant, after);
            }
            attempt += 1;
        };
        // `CLOCK_BOOTTIME` never goes back within one boot; a reading from
        // another boot measures nothing.
        boottime
            .checked_sub(boot_origin)
            .map_or(Duration::ZERO, |boot| {
                boot.saturating_sub(instant.saturating_duration_since(origin))
            })
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

impl StoredValidity {
    /// The same boot's `CLOCK_BOOTTIME` `span` later.
    #[must_use]
    pub fn later_by(&self, span: Duration) -> Self {
        Self {
            boot_id: self.boot_id.clone(),
            until_boottime: self.until_boottime.saturating_add(span),
        }
    }
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

    use std::sync::Arc;

    use super::{HostClock, HostNow, ManualHostClock, StoredValidity, SuspendWatch};

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

    /// ADR 0240: the watch measures what `CLOCK_BOOTTIME` gained beyond the
    /// tokio clock, so the time that passes while the host runs counts for
    /// nothing and every suspend adds up.
    #[tokio::test(start_paused = true)]
    async fn the_watch_measures_only_the_suspends() {
        let clock = Arc::new(ManualHostClock::new(Some("boot-a"), secs(1000)));
        let watch = SuspendWatch::new(clock.clone(), &HostNow::read(&*clock));
        assert_eq!(watch.suspended(), Duration::ZERO);
        tokio::time::advance(secs(30)).await;
        assert_eq!(
            watch.suspended(),
            Duration::ZERO,
            "running time is not a suspend"
        );
        clock.suspend(secs(7));
        assert_eq!(watch.suspended(), secs(7));
        tokio::time::advance(secs(5)).await;
        clock.suspend(secs(2));
        assert_eq!(watch.suspended(), secs(9));
    }

    /// A host without `CLOCK_BOOTTIME`, a watch without a clock, and a clock
    /// whose boot changed measure nothing.
    #[tokio::test(start_paused = true)]
    async fn a_watch_without_a_boottime_measures_nothing() {
        let clock = Arc::new(ManualHostClock::new(Some("boot-a"), secs(1000)));
        let unknown = SuspendWatch::new(clock.clone(), &HostNow::unknown(Instant::now()));
        let none = SuspendWatch::none();
        let rebooted = SuspendWatch::new(clock.clone(), &HostNow::read(&*clock));
        clock.suspend(secs(7));
        assert_eq!(unknown.suspended(), Duration::ZERO);
        assert_eq!(none.suspended(), Duration::ZERO);
        clock.reboot("boot-b", secs(1));
        assert_eq!(rebooted.suspended(), Duration::ZERO);
    }

    /// The test clock's sleep ends at a suspend, as a timer on
    /// `CLOCK_BOOTTIME` does when the host wakes, and otherwise after its
    /// period.
    #[tokio::test(start_paused = true)]
    async fn a_manual_sleep_ends_at_a_suspend() {
        let clock = ManualHostClock::new(Some("boot-a"), secs(1000));
        let began = Instant::now();
        let sleep = clock.sleep(secs(60));
        let suspender = clock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(secs(1)).await;
            suspender.suspend(secs(600));
        });
        sleep.await;
        assert_eq!(began.elapsed(), secs(1));

        let began = Instant::now();
        clock.sleep(secs(2)).await;
        assert_eq!(
            began.elapsed(),
            secs(2),
            "a suspend before the sleep does not end it"
        );
    }

    /// On Linux the system clock's sleep is a timer on `CLOCK_BOOTTIME`: it
    /// lasts its period while the host runs.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_system_sleep_lasts_its_period() {
        let began = std::time::Instant::now();
        super::SystemHostClock
            .sleep(Duration::from_millis(50))
            .await;
        let elapsed = began.elapsed();
        assert!(elapsed >= Duration::from_millis(50), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    }
}

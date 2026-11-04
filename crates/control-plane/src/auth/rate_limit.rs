//! In-memory, per-identity sliding-window rate limits.
//!
//! `POST /api/v1/auth/password` is limited per user (permission-model.md
//! section 12): at most [`PASSWORD_LIMIT`] attempts in [`PASSWORD_WINDOW`],
//! after which the attempt is refused with `rate_limited` and audited. The
//! state lives in memory only, so a restart clears it; the audit log keeps the
//! durable trail of refused attempts.
//!
//! Memory is bounded: a key is dropped as soon as its queue is empty, and the
//! whole map is swept when it grows past [`MAX_TRACKED`] keys.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Attempts allowed per user in one window.
pub const PASSWORD_LIMIT: usize = 5;

/// Length of the sliding window: 15 minutes.
pub const PASSWORD_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Upper bound on tracked keys before a full sweep of expired entries.
const MAX_TRACKED: usize = 4096;

#[derive(Debug)]
struct Inner {
    limit: usize,
    window: Duration,
    attempts: HashMap<String, VecDeque<Instant>>,
}

/// A per-key sliding-window limiter. Cloning shares the same state.
#[derive(Clone, Debug)]
pub struct RateLimiter {
    inner: Arc<Mutex<Inner>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(PASSWORD_LIMIT, PASSWORD_WINDOW)
    }
}

impl RateLimiter {
    /// A limiter allowing `limit` attempts per `window`.
    #[must_use]
    pub fn new(limit: usize, window: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                limit,
                window,
                attempts: HashMap::new(),
            })),
        }
    }

    /// Records one attempt for `key` at `now` and reports whether it is
    /// allowed. Calls beyond the limit are refused and not recorded, so the
    /// window drains on its own.
    pub fn attempt_at(&self, key: &str, now: Instant) -> bool {
        let Ok(mut inner) = self.inner.lock() else {
            // A poisoned lock must not turn into an unlimited endpoint.
            return false;
        };
        if inner.attempts.len() > MAX_TRACKED {
            inner.sweep(now);
        }
        let window = inner.window;
        let limit = inner.limit;
        let allowed = {
            let attempts = inner.attempts.entry(key.to_string()).or_default();
            while attempts
                .front()
                .is_some_and(|first| now.duration_since(*first) >= window)
            {
                attempts.pop_front();
            }
            if attempts.len() >= limit {
                false
            } else {
                attempts.push_back(now);
                true
            }
        };
        if inner.attempts.get(key).is_some_and(VecDeque::is_empty) {
            inner.attempts.remove(key);
        }
        allowed
    }

    /// [`RateLimiter::attempt_at`] with the current clock.
    pub fn attempt(&self, key: &str) -> bool {
        self.attempt_at(key, Instant::now())
    }

    /// Number of tracked keys; for tests and diagnostics.
    #[cfg(test)]
    fn tracked_keys(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| inner.attempts.len())
            .unwrap_or(0)
    }
}

impl Inner {
    fn sweep(&mut self, now: Instant) {
        let window = self.window;
        self.attempts.retain(|_, attempts| {
            while attempts
                .front()
                .is_some_and(|first| now.duration_since(*first) >= window)
            {
                attempts.pop_front();
            }
            !attempts.is_empty()
        });
        // Still too many live keys: drop the least recently used ones, so a
        // flood of distinct keys cannot grow the map without bound.
        if self.attempts.len() > MAX_TRACKED {
            let mut latest: Vec<(Instant, String)> = self
                .attempts
                .iter()
                .filter_map(|(key, attempts)| attempts.back().map(|last| (*last, key.clone())))
                .collect();
            latest.sort_by_key(|(last, _)| *last);
            let excess = self.attempts.len() - MAX_TRACKED;
            for (_, key) in latest.into_iter().take(excess) {
                self.attempts.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_the_limit_then_refuses() {
        let limiter = RateLimiter::new(3, Duration::from_secs(60));
        let now = Instant::now();
        for _ in 0..3 {
            assert!(limiter.attempt_at("usr_a", now));
        }
        assert!(!limiter.attempt_at("usr_a", now));
        // Another key has its own window.
        assert!(limiter.attempt_at("usr_b", now));
    }

    #[test]
    fn the_window_slides() {
        let limiter = RateLimiter::new(2, Duration::from_secs(60));
        let t0 = Instant::now();
        assert!(limiter.attempt_at("usr_a", t0));
        assert!(limiter.attempt_at("usr_a", t0 + Duration::from_secs(30)));
        assert!(!limiter.attempt_at("usr_a", t0 + Duration::from_secs(59)));
        // The first attempt is outside the window again.
        assert!(limiter.attempt_at("usr_a", t0 + Duration::from_secs(60)));
        assert!(!limiter.attempt_at("usr_a", t0 + Duration::from_secs(61)));
    }

    #[test]
    fn expired_keys_are_dropped() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let t0 = Instant::now();
        assert!(limiter.attempt_at("usr_a", t0));
        assert_eq!(limiter.tracked_keys(), 1);
        assert!(limiter.attempt_at("usr_a", t0 + Duration::from_secs(61)));
        assert_eq!(limiter.tracked_keys(), 1);
        // A refused key at the limit keeps its queue; once expired it is gone.
        assert!(!limiter.attempt_at("usr_a", t0 + Duration::from_secs(62)));
        assert!(limiter.attempt_at("usr_a", t0 + Duration::from_secs(200)));
        assert_eq!(limiter.tracked_keys(), 1);
    }
}

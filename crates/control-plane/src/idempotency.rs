//! `Idempotency-Key` handling for creating calls (README "Request headers").
//!
//! The cache is local, in-memory state of the primary, like `sessions`: a
//! replayed key returns the original status and body for 24 hours. A key that
//! arrives again while the first call is still running is refused with
//! `conflict`, and a key reused with a different body is refused with
//! `conflict`, so a client bug never creates a second resource silently.

use crate::store::sha256_hex;
use axum::http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long a completed result is replayed.
pub const TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    /// The caller's identity id, or `public` for unauthenticated creates.
    identity: String,
    operation: &'static str,
    key: String,
}

#[derive(Clone, Debug)]
enum Slot {
    InFlight {
        body_hash: String,
    },
    Done {
        body_hash: String,
        status: StatusCode,
        body: Value,
        at: Instant,
    },
}

/// What a caller should do with its request.
#[derive(Debug)]
pub enum Begin {
    /// First use: run the call, then [`Cache::finish`] or [`Cache::abandon`].
    Proceed,
    /// Already completed: answer with this status and body.
    Replay(StatusCode, Value),
    /// Running right now, or used with a different body.
    Conflict,
}

/// The process-wide cache.
#[derive(Clone, Default)]
pub struct Cache(Arc<Mutex<HashMap<Key, Slot>>>);

fn body_hash(body: &Value) -> String {
    sha256_hex(
        crate::jcs::canonicalize(body)
            .unwrap_or_else(|_| body.to_string())
            .as_bytes(),
    )
}

impl Cache {
    /// Reserves `key` for this call, or reports the earlier result.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn begin(&self, identity: &str, operation: &'static str, key: &str, body: &Value) -> Begin {
        let hash = body_hash(body);
        let mut map = self.0.lock().expect("idempotency lock");
        map.retain(|_, slot| match slot {
            Slot::Done { at, .. } => at.elapsed() < TTL,
            Slot::InFlight { .. } => true,
        });
        let k = Key {
            identity: identity.to_string(),
            operation,
            key: key.to_string(),
        };
        match map.get(&k) {
            None => {
                map.insert(k, Slot::InFlight { body_hash: hash });
                Begin::Proceed
            }
            Some(Slot::Done {
                body_hash,
                status,
                body,
                ..
            }) if *body_hash == hash => Begin::Replay(*status, body.clone()),
            Some(_) => Begin::Conflict,
        }
    }

    /// Stores the result of a call that [`Cache::begin`] let proceed.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn finish(
        &self,
        identity: &str,
        operation: &'static str,
        key: &str,
        status: StatusCode,
        response: Value,
    ) {
        let mut map = self.0.lock().expect("idempotency lock");
        let k = Key {
            identity: identity.to_string(),
            operation,
            key: key.to_string(),
        };
        if let Some(Slot::InFlight { body_hash }) = map.get(&k).cloned() {
            map.insert(
                k,
                Slot::Done {
                    body_hash,
                    status,
                    body: response,
                    at: Instant::now(),
                },
            );
        }
    }

    /// Releases the key of a call that failed, so the client may retry it.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn abandon(&self, identity: &str, operation: &'static str, key: &str) {
        let mut map = self.0.lock().expect("idempotency lock");
        let k = Key {
            identity: identity.to_string(),
            operation,
            key: key.to_string(),
        };
        if matches!(map.get(&k), Some(Slot::InFlight { .. })) {
            map.remove(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn replays_conflicts_and_retries() {
        let cache = Cache::default();
        let body = json!({ "name": "a" });
        assert!(matches!(
            cache.begin("usr_1", "POST /x", "k", &body),
            Begin::Proceed
        ));
        assert!(matches!(
            cache.begin("usr_1", "POST /x", "k", &body),
            Begin::Conflict
        ));
        cache.finish(
            "usr_1",
            "POST /x",
            "k",
            StatusCode::CREATED,
            json!({ "id": 1 }),
        );
        match cache.begin("usr_1", "POST /x", "k", &body) {
            Begin::Replay(status, value) => {
                assert_eq!(status, StatusCode::CREATED);
                assert_eq!(value, json!({ "id": 1 }));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            cache.begin("usr_1", "POST /x", "k", &json!({ "name": "b" })),
            Begin::Conflict
        ));
        assert!(matches!(
            cache.begin("usr_2", "POST /x", "k", &body),
            Begin::Proceed
        ));
        cache.abandon("usr_2", "POST /x", "k");
        assert!(matches!(
            cache.begin("usr_2", "POST /x", "k", &body),
            Begin::Proceed
        ));
    }
}

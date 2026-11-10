//! Following the change log as a node does (`node-agent-protocol.md`
//! section 7.1): the checks a node runs on `log_entries`, and the primary's
//! log as the tests read it. Shared by the node-session, support and
//! held-grant tests.

use super::Server;
use quoro_proto::jcs;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// 64 zeros: the `log_hash` of an empty copy and the `prev_hash` of `seq` 1.
pub fn genesis() -> String {
    "0".repeat(64)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The primary's newest change-log `seq`.
pub async fn log_head(server: &Server) -> u64 {
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    u64::try_from(head.unwrap_or(0)).unwrap()
}

/// The primary's `hash` at `seq`.
pub async fn log_hash_at(server: &Server, seq: u64) -> String {
    sqlx::query_scalar("SELECT hash FROM change_log WHERE seq = ?")
        .bind(i64::try_from(seq).unwrap())
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Checks one `log_entries` body as a node does (section 7.1,
/// "Verification"): its entries run without a gap from `from_seq`, each has
/// exactly the 11 fields of section 5.14, hashes to its `hash` (SHA-256 over
/// the RFC 8785 form of the entry without `hash`) and links to the hash
/// before it, starting from `prev_hash`; `head_seq` is at least the last
/// `seq`. Returns the last entry's `seq` and `hash`.
pub fn check_entries(body: &Value, prev_hash: &str) -> (u64, String) {
    let from_seq = body["from_seq"].as_u64().unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert!(!entries.is_empty() && entries.len() <= 500, "{body}");
    let mut prev = prev_hash.to_string();
    let mut seq = from_seq;
    for entry in entries {
        let mut fields = entry.as_object().unwrap().clone();
        assert_eq!(fields.len(), 11, "{entry}");
        assert_eq!(entry["seq"], seq, "{entry}");
        assert_eq!(entry["prev_hash"], prev.as_str(), "{entry}");
        let hash = fields.remove("hash").unwrap();
        let canonical = jcs::canonicalize(&Value::Object(fields)).unwrap();
        prev = sha256_hex(canonical.as_bytes());
        assert_eq!(hash, prev.as_str(), "{entry}");
        seq += 1;
    }
    let last = seq - 1;
    assert!(body["head_seq"].as_u64().unwrap() >= last, "{body}");
    (last, prev)
}

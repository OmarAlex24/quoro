//! The keys in use and their seals (`node-agent-protocol.md` sections 7.2
//! and 7.3): the last refusal before a promotion.
//!
//! The keys in use, as decided by the maintainer:
//!
//! - **signing:** the key id in the agent's identity, and the key id of
//!   every `signing_keys` row in the followed log (the current key and,
//!   during a rotation, the next one, section 7.5);
//! - **TLS:** the fingerprint the agent pins (section 2.2);
//! - **data encryption:** every `dek` seal made for this machine in the
//!   followed log. There must be exactly one: a primary keeps one data key,
//!   so more than one refuses.
//!
//! For each, the followed log must hold a `node_key_custody` row for this
//! machine, kind and key id; its `sealed` must open with this machine's
//! custody key under that context (section 7.2, "Construction"); and the
//! opened key must be the one its key id names. A signing seed must give a
//! public key whose SHA-256 is the key id, and a TLS pair's leaf certificate
//! must hash to the key id. A data key's id is random, not derived from the
//! key: the seal's `info` binds it, so a data key passes when its seal opens
//! under its key id and holds 32 bytes.
//!
//! The two tables are read by folding only their entries with
//! `quoro_control_plane::rebuild::fold`, the rule step 4 builds the database
//! with, against this binary's schema in memory. The custody key is loaded,
//! never created.
//!
//! Every problem is listed, not only the first: a data-key count other than
//! one, a custody key that cannot be loaded, and each key in use whose seal
//! is missing, does not open or opens to another key. Without the custody
//! key, a key's seal is still checked for what needs no key: that it is in
//! the log and is base64.
//!
//! [`open`] makes the same checks and returns what each seal holds, with
//! the key ids of the `current` and `next` rows of `signing_keys`, for the
//! key installation of step 6 (`install_keys`); [`check`] returns only the
//! keys.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use quoro_control_plane::custody::TlsIdentity;
use quoro_control_plane::rebuild::{self, Cell, Entry, Row, Schema};
use quoro_control_plane::store::Store;
use quoro_node_agent::follow::LogEntry;
use quoro_node_agent::identity::{self, Identity};
use quoro_proto::custody::{self, CustodyKey, KeyKind, SealContext};
use quoro_proto::sign;
use serde_json::Value;
use zeroize::Zeroizing;

use super::command::kind_name;

/// The `entity_type` of a seal's change-log entries (migration 0012).
const SEAL_ENTITY: &str = "node_key_custody";
/// The `entity_type` of a signing key's change-log entries (migration 0012).
const SIGNING_ENTITY: &str = "signing_key";
/// The entries [`check`] reads: the agent's store selects only these.
pub(crate) const ENTITY_TYPES: [&str; 2] = [SEAL_ENTITY, SIGNING_ENTITY];
/// The table of the seals.
const SEAL_TABLE: &str = "node_key_custody";
/// The table of the signing keys.
const SIGNING_TABLE: &str = "signing_keys";
/// The prefix of a pinned fingerprint.
const FINGERPRINT_PREFIX: &str = "sha256:";
/// The `state` of the `signing_keys` row of the key in use outside a
/// rotation (section 7.5).
const CURRENT: &str = "current";
/// The `state` of the `signing_keys` row of the next key, during a rotation
/// (section 7.5).
const NEXT: &str = "next";

/// What the followed log says about the keys.
#[derive(Debug, Default)]
struct Ledger {
    /// The key id of every `signing_keys` row.
    signing_keys: BTreeSet<String>,
    /// The key id of the `current` row of `signing_keys`, if there is one.
    current_signing: Option<String>,
    /// The key id of every `next` row of `signing_keys`.
    next_signing: BTreeSet<String>,
    /// This machine's seals: `(key_kind, key_id)` to `sealed`, the base64
    /// text a change-log payload carries.
    seals: BTreeMap<(String, String), String>,
}

/// The TLS seal's plaintext: `{"cert_pem": ..., "key_pem": ...}`.
#[derive(serde::Deserialize)]
pub(crate) struct TlsPair {
    /// The certificate chain, PEM.
    pub(crate) cert_pem: String,
    /// The private key, PEM.
    pub(crate) key_pem: String,
}

/// A key in use, opened from this machine's seal of it ([`open`]).
pub(crate) struct OpenedKey {
    /// Its kind.
    pub(crate) kind: KeyKind,
    /// Its key id.
    pub(crate) key_id: String,
    /// What its seal holds (section 7.2), wiped when dropped.
    pub(crate) plaintext: Zeroizing<Vec<u8>>,
}

/// The keys in use, opened ([`open`]).
pub(crate) struct Opened {
    /// Every key in use with what its seal holds, in [`check`]'s order.
    pub(crate) keys: Vec<OpenedKey>,
    /// The key id of the `current` row of `signing_keys` in the followed
    /// log, when it holds one.
    pub(crate) current_signing: Option<String>,
    /// The key id of every `next` row of `signing_keys` in the followed log:
    /// during a rotation, the next key (section 7.5).
    pub(crate) next_signing: BTreeSet<String>,
}

/// Checks that this machine can open a seal of every key in use, each to
/// the key its id names (module documentation). Returns the keys, signing
/// keys first in key-id order, then the data key, then the TLS key.
///
/// `entries` are the followed log's entries of [`ENTITY_TYPES`], in `seq`
/// order; an entry of another type is skipped.
///
/// # Errors
///
/// Every problem found, one message each: more than one data key or none, a
/// custody key that cannot be loaded, a key without a seal for this
/// machine, a seal that does not open, or a seal that opens to another key.
/// Only a log whose key tables cannot be read stops the check at once, with
/// that one message.
pub(crate) async fn check(
    identity: &Identity,
    entries: &[LogEntry],
    node_data_dir: &Path,
) -> Result<Vec<(KeyKind, String)>, Vec<String>> {
    open(identity, entries, node_data_dir).await.map(|opened| {
        opened
            .keys
            .into_iter()
            .map(|key| (key.kind, key.key_id))
            .collect()
    })
}

/// [`check`], returning what each seal holds and the key ids of the
/// `current` and `next` rows of `signing_keys`.
///
/// # Errors
///
/// As [`check`].
pub(crate) async fn open(
    identity: &Identity,
    entries: &[LogEntry],
    node_data_dir: &Path,
) -> Result<Opened, Vec<String>> {
    let ledger = read_ledger(&identity.node_id, entries)
        .await
        .map_err(|message| vec![message])?;
    let mut problems = Vec::new();
    let in_use = in_use(identity, &ledger, &mut problems);
    let custody_key = match identity::load_custody_key(node_data_dir) {
        Ok(key) => Some(key),
        Err(e) => {
            problems.push(format!("cannot load this machine's custody key: {e}"));
            None
        }
    };
    let mut keys = Vec::with_capacity(in_use.len());
    for (kind, key_id) in in_use {
        match open_seal(identity, &ledger, custody_key.as_ref(), kind, &key_id) {
            Ok(Some(plaintext)) => keys.push(OpenedKey {
                kind,
                key_id,
                plaintext,
            }),
            // Without the custody key, which is its own problem.
            Ok(None) => {}
            Err(problem) => problems.push(format!("the {} {key_id}: {problem}", kind_name(kind))),
        }
    }
    if problems.is_empty() {
        Ok(Opened {
            keys,
            current_signing: ledger.current_signing,
            next_signing: ledger.next_signing,
        })
    } else {
        Err(problems)
    }
}

/// The keys in use (module documentation). A data-key count other than one
/// is pushed to `problems`, and the data key is then left out.
fn in_use(
    identity: &Identity,
    ledger: &Ledger,
    problems: &mut Vec<String>,
) -> Vec<(KeyKind, String)> {
    let mut signing: BTreeSet<String> = ledger.signing_keys.clone();
    signing.insert(identity.cluster_key_id.clone());
    let deks: Vec<&String> = ledger
        .seals
        .keys()
        .filter(|(kind, _)| kind == KeyKind::Dek.as_str())
        .map(|(_, key_id)| key_id)
        .collect();
    let dek = match deks.as_slice() {
        [one] => Some((*one).clone()),
        [] => {
            problems.push(
                "the followed log holds no data-encryption key sealed to this machine".to_string(),
            );
            None
        }
        many => {
            let ids: Vec<&str> = many.iter().map(|id| id.as_str()).collect();
            problems.push(format!(
                "the followed log holds {} data-encryption keys sealed to this machine ({}), and \
                 a primary keeps exactly one",
                many.len(),
                ids.join(", ")
            ));
            None
        }
    };
    let fingerprint = identity
        .fingerprint
        .strip_prefix(FINGERPRINT_PREFIX)
        .unwrap_or(&identity.fingerprint)
        .to_ascii_lowercase();
    let mut keys: Vec<(KeyKind, String)> = signing
        .into_iter()
        .map(|key_id| (KeyKind::Signing, key_id))
        .collect();
    keys.extend(dek.map(|dek| (KeyKind::Dek, dek)));
    keys.push((KeyKind::Tls, fingerprint));
    keys
}

/// Opens this machine's seal of the `kind` key `key_id`, checks that it
/// holds that key and returns what it holds. Without `custody_key`, whose
/// absence is its own problem, it checks only that the seal is in the log
/// and is base64, and returns `None`.
///
/// # Errors
///
/// Why it does not: no seal, a `sealed` that is not base64, a seal that does
/// not open, or a key other than `key_id`.
fn open_seal(
    identity: &Identity,
    ledger: &Ledger,
    custody_key: Option<&CustodyKey>,
    kind: KeyKind,
    key_id: &str,
) -> Result<Option<Zeroizing<Vec<u8>>>, String> {
    let Some(text) = ledger
        .seals
        .get(&(kind.as_str().to_string(), key_id.to_string()))
    else {
        return Err("the followed log holds no seal of it for this machine".to_string());
    };
    let sealed = crate::tls::base64_decode(text)
        .map_err(|_| "its seal in the followed log is not base64".to_string())?;
    let Some(custody_key) = custody_key else {
        return Ok(None);
    };
    let context = SealContext {
        cluster_id: &identity.cluster_id,
        node_id: &identity.node_id,
        key_kind: kind,
        key_id,
    };
    let opened = custody::open(custody_key, &context, &sealed)
        .map_err(|e| format!("its seal does not open with this machine's custody key ({e})"))?;
    holds(kind, key_id, &opened)?;
    Ok(Some(opened))
}

/// Whether `plaintext`, an opened seal, is the `kind` key `key_id`.
///
/// # Errors
///
/// What the seal holds instead.
fn holds(kind: KeyKind, key_id: &str, plaintext: &[u8]) -> Result<(), String> {
    match kind {
        KeyKind::Signing => {
            let seed: &[u8; 32] = plaintext.try_into().map_err(|_| {
                format!(
                    "its seal opens to {} bytes, not a 32-byte seed",
                    plaintext.len()
                )
            })?;
            let public =
                sign::public_key_to_base64(&sign::signing_key_from_seed(seed).verifying_key());
            let opened = Identity::key_id_for(&public).map_err(|e| e.to_string())?;
            if opened == key_id {
                Ok(())
            } else {
                Err(format!("its seal opens to the signing key {opened}"))
            }
        }
        KeyKind::Dek => {
            if plaintext.len() == 32 {
                Ok(())
            } else {
                Err(format!(
                    "its seal opens to {} bytes, not a 32-byte key",
                    plaintext.len()
                ))
            }
        }
        KeyKind::Tls => {
            let pair: TlsPair = serde_json::from_slice(plaintext)
                .map_err(|_| "its seal does not open to a certificate and a key".to_string())?;
            let tls = TlsIdentity::from_pem(pair.cert_pem, pair.key_pem)
                .map_err(|e| format!("its seal does not open to a usable pair: {e}"))?;
            if tls.key_id() == key_id {
                Ok(())
            } else {
                Err(format!(
                    "its seal opens to the certificate {}",
                    tls.key_id()
                ))
            }
        }
    }
}

/// Reads the `signing_keys` rows and this machine's `node_key_custody` rows
/// from `entries` by folding only their entries.
///
/// # Errors
///
/// A message when this binary's schema cannot be made in memory, or the
/// fold of those entries stops.
async fn read_ledger(node_id: &str, entries: &[LogEntry]) -> Result<Ledger, String> {
    let schema = schema().await?;
    let folded = rebuild::fold(
        &schema,
        entries
            .iter()
            .filter(|entry| entry.entity_type == SEAL_ENTITY || entry.entity_type == SIGNING_ENTITY)
            .map(|entry| Entry {
                seq: entry.seq,
                entity_type: &entry.entity_type,
                entity_id: &entry.entity_id,
                op: &entry.op,
                payload: &entry.payload,
            }),
    )
    .map_err(|e| format!("cannot read the keys in use from the followed log: {e}"))?;

    let mut ledger = Ledger::default();
    if let Some(rows) = folded.tables.get(SIGNING_TABLE) {
        for (entity_id, row) in rows {
            let key_id = text(&row.cells, "key_id").unwrap_or(entity_id.as_str());
            match text(&row.cells, "state") {
                Some(CURRENT) => ledger.current_signing = Some(key_id.to_string()),
                Some(NEXT) => {
                    ledger.next_signing.insert(key_id.to_string());
                }
                _ => {}
            }
            ledger.signing_keys.insert(key_id.to_string());
        }
    }
    if let Some(rows) = folded.tables.get(SEAL_TABLE) {
        for row in rows.values() {
            let cells = &row.cells;
            if text(cells, "node_id") != Some(node_id) {
                continue;
            }
            let (Some(kind), Some(key_id)) = (text(cells, "key_kind"), text(cells, "key_id"))
            else {
                continue;
            };
            ledger.seals.insert(
                (kind.to_string(), key_id.to_string()),
                text(cells, "sealed").unwrap_or_default().to_string(),
            );
        }
    }
    Ok(ledger)
}

/// This binary's schema, read from a database made in memory. The token-cap
/// check of `--ack-fence` folds the `leases` and `nodes` entries against it
/// too (`super::steps`).
pub(crate) async fn schema() -> Result<Schema, String> {
    let store = Store::open_in_memory()
        .await
        .map_err(|e| format!("cannot make this binary's schema in memory: {e}"))?;
    let mut conn = store
        .pool()
        .acquire()
        .await
        .map_err(|e| format!("cannot read this binary's schema: {e}"))?;
    Schema::read(&mut conn)
        .await
        .map_err(|e| format!("cannot read this binary's schema: {e}"))
}

/// The text a folded row holds in `column`, when it is a string.
fn text<'r>(cells: &'r Row, column: &str) -> Option<&'r str> {
    match cells.get(column)? {
        Cell::Value(Value::String(text)) => Some(text.as_str()),
        _ => None,
    }
}

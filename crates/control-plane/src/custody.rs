// SPDX-License-Identifier: AGPL-3.0-only
//! Key custody on the primary (`node-agent-protocol.md` section 7.2, ADR
//! 0180, ADR 0181, ADR 0231).
//!
//! Every machine with the `node` role reports the public half of its custody
//! key in `hello`. The primary binds the first key it sees
//! ([`bind_from_hello`]) and seals every key it holds ([`Keyring::keys_in_use`])
//! to it with [`quoro_proto::custody::seal`], one row of the replicated table
//! `node_key_custody` per machine, kind and key. A later, different key is
//! refused: it is answered with `custody_key_changed`, recorded once, and gets
//! no seal. At every start the primary seals what is missing, and deletes the
//! seals of retired keys and of removed machines ([`reconcile`]).
//!
//! Every write here goes through the change log, so a follower holds every
//! seal and can open only its own.

use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::crypto::DataKey;
use crate::jcs;
use crate::keys::ClusterKeys;
use crate::store::{Store, now_rfc3339, sha256_hex};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use quoro_proto::custody::{self as seals, CustodyPublicKey, KeyKind, SealContext};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::SqliteConnection;
use std::collections::HashSet;
use std::sync::Arc;
use zeroize::{Zeroize as _, Zeroizing};

/// The `entity_type` of a seal's change-log entries (migration 0012).
const ENTITY_TYPE: &str = "node_key_custody";
/// The `system` actor of the seal writes.
const SEAL_ACTOR: &str = "custody";
/// The `system` actor of the node-row writes and of the audit record
/// `node.custody_key_changed` (`audit-log-format.md` section 3.9).
const NODES_ACTOR: &str = "nodes";
/// The audit operation of a refused custody key.
const KEY_CHANGED_OPERATION: &str = "node.custody_key_changed";

/// The primary's TLS certificate chain and private key, as the PEM text of
/// `<data_dir>/tls/cert.pem` and `key.pem` (D-0054). The `tls` seal carries
/// both (section 7.2, ADR 0181), so a promoted primary presents the
/// certificate every node pins.
///
/// Cloning is cheap. The private key is wiped when the last clone is
/// dropped, and the [`std::fmt::Debug`] form shows only the key id.
#[derive(Clone)]
pub struct TlsIdentity(Arc<TlsInner>);

struct TlsInner {
    cert_pem: String,
    key_pem: Zeroizing<String>,
    key_id: String,
}

impl TlsIdentity {
    /// Takes the certificate chain and the private key as PEM text, exactly
    /// as the files hold them.
    ///
    /// # Errors
    ///
    /// A message when `cert_pem` has no `CERTIFICATE` block of valid base64,
    /// or `key_pem` is blank.
    pub fn from_pem(cert_pem: String, key_pem: String) -> Result<Self, String> {
        let key_pem = Zeroizing::new(key_pem);
        if key_pem.trim().is_empty() {
            return Err("the TLS private key is empty".to_string());
        }
        let leaf = leaf_der(&cert_pem)?;
        Ok(Self(Arc::new(TlsInner {
            key_id: sha256_hex(&leaf),
            cert_pem,
            key_pem,
        })))
    }

    /// The certificate chain, PEM.
    #[must_use]
    pub fn cert_pem(&self) -> &str {
        &self.0.cert_pem
    }

    /// The private key, PEM.
    #[must_use]
    pub fn key_pem(&self) -> &str {
        &self.0.key_pem
    }

    /// The `tls` seal's `key_id`: lowercase hex SHA-256 of the leaf
    /// certificate's DER, the fingerprint every node pins (section 2.2).
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.0.key_id
    }
}

impl std::fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsIdentity")
            .field("key_id", &self.0.key_id)
            .finish_non_exhaustive()
    }
}

/// The DER of the first `CERTIFICATE` block of `pem`: the leaf.
fn leaf_der(pem: &str) -> Result<Vec<u8>, String> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let start = pem
        .find(BEGIN)
        .ok_or_else(|| "the TLS certificate has no CERTIFICATE block".to_string())?;
    let rest = &pem[start + BEGIN.len()..];
    let stop = rest
        .find(END)
        .ok_or_else(|| "the TLS certificate's CERTIFICATE block has no end".to_string())?;
    let body: String = rest[..stop]
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let der = STANDARD
        .decode(body)
        .map_err(|e| format!("the TLS certificate is not valid base64: {e}"))?;
    if der.is_empty() {
        return Err("the TLS certificate is empty".to_string());
    }
    Ok(der)
}

/// One key the primary holds, ready to be sealed (section 7.2, "Seals").
pub(crate) struct KeyInUse {
    pub(crate) kind: KeyKind,
    pub(crate) key_id: String,
    /// What the seal carries, wiped when dropped.
    pub(crate) plaintext: Zeroizing<Vec<u8>>,
}

/// The keys the primary holds and the cluster they belong to.
#[derive(Clone, Copy)]
pub(crate) struct Keyring<'a> {
    pub(crate) cluster_id: &'a str,
    pub(crate) keys: &'a ClusterKeys,
    pub(crate) data_key: &'a DataKey,
    pub(crate) tls: &'a TlsIdentity,
}

impl Keyring<'_> {
    /// Every key the primary holds now, as `(kind, key_id, plaintext)`
    /// (section 7.2, "Seals"):
    ///
    /// - `signing`: the 32-byte Ed25519 seed of the current cluster signing
    ///   key; `key_id` is lowercase hex SHA-256 of its public key
    ///   ([`ClusterKeys::key_id`]).
    /// - `dek`: for each data-encryption key id the primary holds, the 32-byte
    ///   key; `key_id` is the 32 lowercase hex characters of the id.
    /// - `tls`: the RFC 8785 serialization of `{"cert_pem", "key_pem"}`;
    ///   `key_id` is [`TlsIdentity::key_id`].
    ///
    /// # Errors
    ///
    /// A message when the TLS pair has no RFC 8785 form, which PEM text
    /// always has.
    pub(crate) fn keys_in_use(&self) -> Result<Vec<KeyInUse>, String> {
        let mut held = vec![KeyInUse {
            kind: KeyKind::Signing,
            key_id: self.keys.key_id().to_string(),
            plaintext: Zeroizing::new(self.keys.seed().to_vec()),
        }];
        for (key_id, key) in self.data_key.held_keys() {
            held.push(KeyInUse {
                kind: KeyKind::Dek,
                key_id,
                plaintext: Zeroizing::new(key.to_vec()),
            });
        }
        held.push(KeyInUse {
            kind: KeyKind::Tls,
            key_id: self.tls.key_id().to_string(),
            plaintext: tls_plaintext(self.tls)?,
        });
        Ok(held)
    }

    /// The `(kind, key_id)` of every key [`Keyring::keys_in_use`] returns,
    /// without reading any key.
    pub(crate) fn key_ids_in_use(&self) -> Vec<(KeyKind, String)> {
        let mut ids = vec![(KeyKind::Signing, self.keys.key_id().to_string())];
        ids.extend(
            self.data_key
                .held_key_ids()
                .into_iter()
                .map(|key_id| (KeyKind::Dek, key_id)),
        );
        ids.push((KeyKind::Tls, self.tls.key_id().to_string()));
        ids
    }
}

/// The `tls` plaintext: `{"cert_pem": ..., "key_pem": ...}` in its RFC 8785
/// form. The copy of the private key the JSON value holds is wiped.
fn tls_plaintext(tls: &TlsIdentity) -> Result<Zeroizing<Vec<u8>>, String> {
    let mut value = json!({ "cert_pem": tls.cert_pem(), "key_pem": tls.key_pem() });
    let canonical = jcs::canonicalize(&value);
    if let Some(Value::String(key)) = value.get_mut("key_pem") {
        key.zeroize();
    }
    canonical
        .map(|text| Zeroizing::new(text.into_bytes()))
        .map_err(|e| format!("the TLS pair has no RFC 8785 form: {}", e.0))
}

/// `sealed` as a `node_key_custody` change-log payload carries it: standard
/// base64 with padding, as migration 0012 says ("in a change-log payload it
/// is base64"). The table holds the raw bytes as a BLOB, so a promotion's
/// rebuild (section 7.3 step 4) decodes this text back to them.
fn encode_sealed(sealed: &[u8]) -> String {
    STANDARD.encode(sealed)
}

/// A seal's `entity_id`: its composite key joined with `/` (section 7.3
/// step 4).
fn entity_id(node_id: &str, key_kind: &str, key_id: &str) -> String {
    format!("{node_id}/{key_kind}/{key_id}")
}

/// Whether a `nodes.roles` JSON array holds `node`.
fn has_node_role(roles: &str) -> bool {
    serde_json::from_str::<Vec<String>>(roles)
        .is_ok_and(|roles| roles.iter().any(|role| role == "node"))
}

/// Seals `key` to `recipient` for `node_id` and inserts the row with its
/// change-log entry, op `create`, whose payload holds every column.
async fn insert_seal(
    conn: &mut SqliteConnection,
    cluster_id: &str,
    node_id: &str,
    recipient: &CustodyPublicKey,
    key: &KeyInUse,
) -> Result<(), String> {
    let kind = key.kind.as_str();
    let context = SealContext {
        cluster_id,
        node_id,
        key_kind: key.kind,
        key_id: &key.key_id,
    };
    let sealed =
        seals::seal(recipient, &context, &key.plaintext, &mut seals::os_rng()).map_err(|e| {
            format!(
                "cannot seal the {kind} key {} to {node_id}: {e}",
                key.key_id
            )
        })?;
    let created_at = now_rfc3339();
    sqlx::query(
        "INSERT INTO node_key_custody (node_id, key_kind, key_id, sealed, created_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(node_id)
    .bind(kind)
    .bind(&key.key_id)
    .bind(&sealed)
    .bind(&created_at)
    .execute(&mut *conn)
    .await
    .map_err(|e| format!("cannot store the {kind} seal of {node_id}: {e}"))?;
    changelog::append(
        conn,
        &Actor::system(SEAL_ACTOR),
        ENTITY_TYPE,
        &entity_id(node_id, kind, &key.key_id),
        Op::Create,
        json!({
            "node_id": node_id,
            "key_kind": kind,
            "key_id": key.key_id,
            "sealed": encode_sealed(&sealed),
            "created_at": created_at,
        }),
    )
    .await
    .map_err(|e| format!("cannot record the {kind} seal of {node_id}: {e}"))?;
    Ok(())
}

/// Deletes one seal row with its change-log entry, op `delete`.
async fn delete_seal(
    conn: &mut SqliteConnection,
    node_id: &str,
    key_kind: &str,
    key_id: &str,
) -> Result<(), String> {
    sqlx::query("DELETE FROM node_key_custody WHERE node_id = ? AND key_kind = ? AND key_id = ?")
        .bind(node_id)
        .bind(key_kind)
        .bind(key_id)
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("cannot delete the {key_kind} seal of {node_id}: {e}"))?;
    changelog::append(
        conn,
        &Actor::system(SEAL_ACTOR),
        ENTITY_TYPE,
        &entity_id(node_id, key_kind, key_id),
        Op::Delete,
        json!({}),
    )
    .await
    .map_err(|e| format!("cannot record the deletion of the {key_kind} seal of {node_id}: {e}"))?;
    Ok(())
}

/// Deletes every seal made for `node_id`, each through the change log, in
/// the caller's transaction: removing a node deletes its seals in the same
/// transaction (section 10). Returns how many it deleted.
///
/// # Errors
///
/// A database error, which the caller's transaction rolls back with the
/// rest.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "removeNode, not built yet, calls it in its transaction"
    )
)]
pub(crate) async fn delete_seals(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<usize, String> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT key_kind, key_id FROM node_key_custody WHERE node_id = ? ORDER BY key_kind, key_id",
    )
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| format!("cannot read the seals of {node_id}: {e}"))?;
    for (key_kind, key_id) in &rows {
        delete_seal(conn, node_id, key_kind, key_id).await?;
    }
    Ok(rows.len())
}

/// What a `hello`'s `custody_public_key` did (section 7.2, "Binding").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Binding {
    /// Nothing was written: no key, an unusable key, a machine without the
    /// `node` role, or the key already bound.
    Unchanged,
    /// The machine's first key was stored and every key in use sealed to it.
    Bound,
    /// The key differs from the bound one. The session answers
    /// `custody_key_changed`, non-fatal.
    KeyChanged,
}

/// Binds the custody key a `hello` reported (section 7.2, "Binding", ADR
/// 0231), in one write:
///
/// - for a machine with the `node` role and no bound key, the key is stored,
///   re-encoded from its 32 bytes, and every key in use is sealed to it;
/// - the same key changes nothing;
/// - a different key sets `nodes.custody_key_changed_at` and writes the audit
///   record `node.custody_key_changed` the first time only, seals nothing,
///   and returns [`Binding::KeyChanged`] every time;
/// - a key that is not base64 of 32 bytes is ignored as if absent, with a
///   warning, and a machine without the `node` role binds nothing.
///
/// # Errors
///
/// A message when the first key cannot be stored or sealed; nothing is then
/// written. A failed write for a changed key is only logged, and the key is
/// still reported changed.
pub(crate) async fn bind_from_hello(
    store: &Store,
    keyring: Keyring<'_>,
    node_id: &str,
    reported: Option<&str>,
) -> Result<Binding, String> {
    let Some(reported) = reported else {
        return Ok(Binding::Unchanged);
    };
    let mut w = store.begin_write().await.map_err(|e| e.to_string())?;
    let row: Option<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT roles, status, custody_public_key, custody_key_changed_at FROM nodes WHERE id = ?",
    )
    .bind(node_id)
    .fetch_optional(&mut *w.tx)
    .await
    .map_err(|e| format!("cannot read the custody key of {node_id}: {e}"))?;
    let Some((roles, status, stored, changed_at)) = row else {
        return Ok(Binding::Unchanged);
    };
    // Edge-only and witness-only machines get no seal (section 7.2).
    if status == "removed" || !has_node_role(&roles) {
        return Ok(Binding::Unchanged);
    }
    let Ok(reported) = CustodyPublicKey::from_base64(reported) else {
        tracing::warn!(
            node_id,
            "ignoring a custody public key that is not base64 of 32 bytes"
        );
        return Ok(Binding::Unchanged);
    };
    match stored {
        None => {
            sqlx::query(
                "UPDATE nodes SET custody_public_key = ? WHERE id = ? AND custody_public_key IS NULL",
            )
            .bind(reported.to_base64())
            .bind(node_id)
            .execute(&mut *w.tx)
            .await
            .map_err(|e| format!("cannot store the custody key of {node_id}: {e}"))?;
            changelog::append(
                &mut w.tx,
                &Actor::system(NODES_ACTOR),
                "node",
                node_id,
                Op::Update,
                json!({ "id": node_id, "custody_public_key": reported.to_base64() }),
            )
            .await
            .map_err(|e| format!("cannot record the custody key of {node_id}: {e}"))?;
            for key in keyring.keys_in_use()? {
                insert_seal(&mut w.tx, keyring.cluster_id, node_id, &reported, &key).await?;
            }
            w.commit()
                .await
                .map_err(|e| format!("cannot bind the custody key of {node_id}: {e}"))?;
            Ok(Binding::Bound)
        }
        Some(stored)
            if CustodyPublicKey::from_base64(&stored)
                .is_ok_and(|stored| stored.to_bytes() == reported.to_bytes()) =>
        {
            Ok(Binding::Unchanged)
        }
        Some(_) => {
            if changed_at.is_none() {
                let recorded = match record_key_changed(&mut w.tx, node_id).await {
                    Ok(()) => w.commit().await.map_err(|e| e.to_string()),
                    Err(e) => Err(e),
                };
                if let Err(e) = recorded {
                    tracing::warn!(%e, node_id, "cannot record a changed custody key");
                }
            }
            Ok(Binding::KeyChanged)
        }
    }
}

/// Sets `nodes.custody_key_changed_at` and writes the audit record
/// `node.custody_key_changed`, actor `system` `nodes`, with its change-log
/// entry (`audit-log-format.md` section 3.9).
async fn record_key_changed(conn: &mut SqliteConnection, node_id: &str) -> Result<(), String> {
    let now = now_rfc3339();
    sqlx::query(
        "UPDATE nodes SET custody_key_changed_at = ? WHERE id = ? AND custody_key_changed_at IS NULL",
    )
    .bind(&now)
    .bind(node_id)
    .execute(&mut *conn)
    .await
    .map_err(|e| format!("cannot mark the custody key of {node_id} changed: {e}"))?;
    let actor = Actor::system(NODES_ACTOR);
    let seq = changelog::append(
        conn,
        &actor,
        "node",
        node_id,
        Op::Update,
        json!({ "id": node_id, "custody_key_changed_at": now }),
    )
    .await
    .map_err(|e| format!("cannot record the changed custody key of {node_id}: {e}"))?;
    let record = Record {
        actor,
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: KEY_CHANGED_OPERATION.to_string(),
        scope: None,
        target_type: Some("node".to_string()),
        target_id: Some(node_id.to_string()),
        project_id: None,
        params: json!({ "node_id": node_id }),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: new_id(Prefix::Request),
        result: Outcome::Ok,
        result_detail: None,
        change_seq: Some(seq),
    };
    audit::append(conn, &record)
        .await
        .map_err(|e| format!("cannot audit the changed custody key of {node_id}: {e}"))?;
    Ok(())
}

/// A node's `id`, `roles`, `status`, `custody_public_key` and
/// `custody_key_changed_at`, as [`reconcile`] reads them.
type NodeRow = (String, String, String, Option<String>, Option<String>);

/// What [`reconcile`] changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Reconciled {
    /// Seals made.
    pub(crate) sealed: usize,
    /// Seals deleted.
    pub(crate) deleted: usize,
}

/// One pass over the seals, at every start of the primary (section 7.2,
/// "When"), in one change-log write when anything changes:
///
/// - every machine with the `node` role that is not removed, has a bound key
///   and no refused change gets a seal of each key in use it lacks;
/// - the seals of key ids no longer in use are deleted, such as those of a
///   replaced TLS certificate;
/// - the seals of removed machines are deleted.
///
/// # Errors
///
/// A message when the keys or the database cannot be read or written; the
/// write is then rolled back as a whole.
pub(crate) async fn reconcile(store: &Store, keyring: Keyring<'_>) -> Result<Reconciled, String> {
    let held = keyring.keys_in_use()?;
    let in_use: HashSet<(&str, &str)> = held
        .iter()
        .map(|key| (key.kind.as_str(), key.key_id.as_str()))
        .collect();
    let mut w = store.begin_write().await.map_err(|e| e.to_string())?;
    let nodes: Vec<NodeRow> = sqlx::query_as(
        "SELECT id, roles, status, custody_public_key, custody_key_changed_at FROM nodes ORDER BY id",
    )
    .fetch_all(&mut *w.tx)
    .await
    .map_err(|e| format!("cannot read the nodes: {e}"))?;
    let existing: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT node_id, key_kind, key_id FROM node_key_custody ORDER BY node_id, key_kind, key_id",
    )
    .fetch_all(&mut *w.tx)
    .await
    .map_err(|e| format!("cannot read the seals: {e}"))?;
    let removed: HashSet<&str> = nodes
        .iter()
        .filter(|(_, _, status, _, _)| status == "removed")
        .map(|(id, _, _, _, _)| id.as_str())
        .collect();

    let mut done = Reconciled::default();
    let mut kept: HashSet<(&str, &str, &str)> = HashSet::new();
    for (node_id, key_kind, key_id) in &existing {
        if removed.contains(node_id.as_str())
            || !in_use.contains(&(key_kind.as_str(), key_id.as_str()))
        {
            delete_seal(&mut w.tx, node_id, key_kind, key_id).await?;
            done.deleted += 1;
        } else {
            kept.insert((node_id, key_kind, key_id));
        }
    }
    for (node_id, roles, status, bound, changed_at) in &nodes {
        let Some(bound) = bound else { continue };
        if status == "removed" || changed_at.is_some() || !has_node_role(roles) {
            continue;
        }
        let Ok(recipient) = CustodyPublicKey::from_base64(bound) else {
            tracing::warn!(
                node_id = %node_id,
                "the bound custody key is not base64 of 32 bytes; nothing is sealed to it"
            );
            continue;
        };
        for key in &held {
            if !kept.contains(&(node_id.as_str(), key.kind.as_str(), key.key_id.as_str())) {
                insert_seal(&mut w.tx, keyring.cluster_id, node_id, &recipient, key).await?;
                done.sealed += 1;
            }
        }
    }
    if done != Reconciled::default() {
        w.commit()
            .await
            .map_err(|e| format!("cannot reconcile the seals: {e}"))?;
    }
    Ok(done)
}

/// A self-signed TLS pair from rcgen, for the unit tests that build an
/// `AppState`.
#[cfg(test)]
pub(crate) fn test_tls() -> TlsIdentity {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    TlsIdentity::from_pem(certified.cert.pem(), certified.key_pair.serialize_pem()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{self, Capacity, Kvm, LocalNode};
    use quoro_proto::custody::CustodyKey;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quoro-custody-{}",
            &crate::store::random_secret()[..16]
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn local_node(store: &Store) -> String {
        driver::register_local_node(
            store,
            &LocalNode {
                name: "node-a".to_string(),
                capacity: Capacity {
                    cpus: 1,
                    memory_mb: 1_024,
                    disk_gb: 10,
                },
                kvm: Kvm {
                    available: true,
                    kernel: "6.8.0".to_string(),
                    nested: None,
                },
                engine_version: "engine".to_string(),
                agent_version: "agent".to_string(),
            },
        )
        .await
        .unwrap()
    }

    #[test]
    fn the_keys_in_use_carry_the_contract_key_ids() {
        let dir = temp_dir();
        let keys = ClusterKeys::load_or_generate(&dir).unwrap();
        let data_key = DataKey::ephemeral();
        let tls = test_tls();
        let keyring = Keyring {
            cluster_id: "cls_01JC9Y0000000000000000000A",
            keys: &keys,
            data_key: &data_key,
            tls: &tls,
        };
        let held = keyring.keys_in_use().unwrap();
        let ids: Vec<(KeyKind, String)> = held
            .iter()
            .map(|key| (key.kind, key.key_id.clone()))
            .collect();
        assert_eq!(ids, keyring.key_ids_in_use());
        // The signing key id is the hex SHA-256 of the public key.
        assert_eq!(
            held[0].key_id,
            sha256_hex(keys.signing_key().verifying_key().as_bytes())
        );
        assert_eq!(held[0].key_id, keys.key_id());
        assert_eq!(&held[0].plaintext[..], &keys.signing_key().to_bytes()[..]);
        assert_eq!(held[1].key_id, data_key.key_id_hex());
        assert_eq!(held[1].key_id.len(), 32);
        assert_eq!(held[1].plaintext.len(), 32);
        let pair: Value = serde_json::from_slice(&held[2].plaintext).unwrap();
        assert_eq!(pair["cert_pem"], tls.cert_pem());
        assert_eq!(pair["key_pem"], tls.key_pem());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_pem_without_a_certificate_is_refused() {
        assert!(TlsIdentity::from_pem("no pem".to_string(), "key".to_string()).is_err());
        let tls = test_tls();
        assert!(TlsIdentity::from_pem(tls.cert_pem().to_string(), " ".to_string()).is_err());
        let debug = format!("{tls:?}");
        assert!(!debug.contains("PRIVATE"), "{debug}");
    }

    #[tokio::test]
    async fn delete_seals_writes_delete_entries_and_leaves_no_row() {
        let dir = temp_dir();
        let store = Store::open_in_memory().await.unwrap();
        let node_id = local_node(&store).await;
        let keys = ClusterKeys::load_or_generate(&dir).unwrap();
        let data_key = DataKey::ephemeral();
        let tls = test_tls();
        let keyring = Keyring {
            cluster_id: "cls_01JC9Y0000000000000000000A",
            keys: &keys,
            data_key: &data_key,
            tls: &tls,
        };
        let custody = CustodyKey::generate(&mut seals::os_rng());
        let bound = bind_from_hello(
            &store,
            keyring,
            &node_id,
            Some(&custody.public_key().to_base64()),
        )
        .await
        .unwrap();
        assert_eq!(bound, Binding::Bound);
        let rows = || async {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_key_custody WHERE node_id = ?")
                .bind(&node_id)
                .fetch_one(store.pool())
                .await
                .unwrap()
        };
        assert_eq!(rows().await, 3);

        let mut w = store.begin_write().await.unwrap();
        assert_eq!(delete_seals(&mut w.tx, &node_id).await.unwrap(), 3);
        w.commit().await.unwrap();
        assert_eq!(rows().await, 0);
        let deletes: Vec<(String, String)> = sqlx::query_as(
            "SELECT entity_id, payload FROM change_log WHERE entity_type = 'node_key_custody' AND op = 'delete' ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        let mut expected: Vec<String> = keyring
            .key_ids_in_use()
            .into_iter()
            .map(|(kind, key_id)| entity_id(&node_id, kind.as_str(), &key_id))
            .collect();
        expected.sort();
        let mut got: Vec<String> = deletes.iter().map(|(id, _)| id.clone()).collect();
        got.sort();
        assert_eq!(got, expected);
        assert!(deletes.iter().all(|(_, payload)| payload == "{}"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

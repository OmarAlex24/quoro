// SPDX-License-Identifier: AGPL-3.0-only
//! The change-log entries migration 0012 asks the primary to write before it
//! serves, so that a promoted primary rebuilds `node_credentials` and
//! `signing_keys` from the log like any other table
//! (`node-agent-protocol.md` section 7.3 step 4, ADR 0226, ADR 0317).
//!
//! A migration cannot write change-log entries, whose hashes are computed
//! outside SQL. So at its start, before it serves, the primary writes in one
//! change-log write what the log still lacks:
//!
//! - a `create` entry carrying the whole row for each `node_credentials` row
//!   whose newest `create` or `delete` entry is not a `create`: a row written
//!   before every insert and update was logged, one whose only entries are
//!   updates, which the promotion's fold refuses to turn into a row (ADR
//!   0317), or one inserted again without an entry after a logged delete;
//! - its current signing key as the `current` row of `signing_keys`, with
//!   its `create` entry, when the table holds no `current` row. When the
//!   `current` row is the loaded key's and its newest `create` or `delete`
//!   entry is not a `create`, the same rule as for a credential, only the
//!   `create` entry carrying that row is written. The row and the entry hold
//!   only the public key; the private key stays a file outside the database.
//!
//! The step acts on what is missing, not on the schema version, so a
//! database that reached schema 13 under a binary that wrote none of this
//! gets the entries at its next start, and a second start writes nothing.
//! The seals of the same paragraph of migration 0012 are
//! [`crate::custody::reconcile`], which runs after this step.

use crate::api::enroll::credential_payload;
use crate::changelog::{self, Actor, Op};
use crate::keys::ClusterKeys;
use crate::store::{Store, now_rfc3339};
use serde_json::json;
use sqlx::SqliteConnection;

/// The `system` actor of the `node_credential` entries.
const CREDENTIALS_ACTOR: &str = "nodes";
/// The `system` actor of the `signing_key` entry.
const SIGNING_KEY_ACTOR: &str = "keys";

/// The `node_credentials` rows whose newest `create` or `delete` entry is
/// not a `create`: rows the log never created, or created and then deleted.
/// The promotion's fold would rebuild none of them (section 7.3 step 4).
/// [`NEWEST_CREATE_OR_DELETE`] applies the same rule to one entity.
const UNLOGGED_CREDENTIALS: &str = "\
    SELECT c.node_id FROM node_credentials c \
    WHERE IFNULL( \
        (SELECT l.op FROM change_log l \
         WHERE l.entity_type = 'node_credential' AND l.entity_id = c.node_id \
           AND l.op IN ('create', 'delete') \
         ORDER BY l.seq DESC LIMIT 1), \
        'delete') <> 'create' \
    ORDER BY c.created_at, c.node_id";

/// The op of one entity's newest `create` or `delete` entry, if any: the
/// fold rebuilds the entity's row only when it is `create`.
const NEWEST_CREATE_OR_DELETE: &str = "\
    SELECT op FROM change_log \
    WHERE entity_type = ? AND entity_id = ? AND op IN ('create', 'delete') \
    ORDER BY seq DESC LIMIT 1";

/// What [`run`] wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Backfilled {
    /// `node_credential` `create` entries written.
    pub(crate) credentials: usize,
    /// Whether the current signing key's `create` entry was written, with
    /// its `signing_keys` row when the table held no `current` row.
    pub(crate) signing_key: bool,
}

/// Writes, in one change-log write, the `node_credential` and `signing_key`
/// entries the log lacks (module docs). Nothing is written, and no
/// transaction is committed, when the log already holds them.
///
/// # Errors
///
/// A message when the rows cannot be read, or a row or an entry cannot be
/// written; the write is then rolled back as a whole.
pub(crate) async fn run(store: &Store, keys: &ClusterKeys) -> Result<Backfilled, String> {
    let mut w = store
        .begin_write()
        .await
        .map_err(|e| format!("cannot start the change-log backfill: {e}"))?;
    let mut done = Backfilled::default();

    let unlogged: Vec<(String,)> = sqlx::query_as(UNLOGGED_CREDENTIALS)
        .fetch_all(&mut *w.tx)
        .await
        .map_err(|e| format!("cannot read the node credentials the change log lacks: {e}"))?;
    let actor = Actor::system(CREDENTIALS_ACTOR);
    for (node_id,) in unlogged {
        let row = credential_payload(&mut w.tx, &node_id)
            .await
            .map_err(|e| format!("cannot read the node credential of {node_id}: {e}"))?;
        changelog::append(
            &mut w.tx,
            &actor,
            "node_credential",
            &node_id,
            Op::Create,
            row,
        )
        .await
        .map_err(|e| format!("cannot record the node credential of {node_id}: {e}"))?;
        done.credentials += 1;
    }

    let current: Option<(String,)> =
        sqlx::query_as("SELECT key_id FROM signing_keys WHERE state = 'current'")
            .fetch_optional(&mut *w.tx)
            .await
            .map_err(|e| format!("cannot read the current signing key's row: {e}"))?;
    let loaded = keys.key_id();
    match current {
        None => {
            insert_current_key(&mut w.tx, keys).await?;
            done.signing_key = true;
        }
        // No contract text covers this case, which stays open: the step
        // neither fails nor changes the row, it only says what follows.
        Some((key_id,)) if key_id != loaded => tracing::warn!(
            row_key_id = %key_id,
            loaded_key_id = %loaded,
            "the current signing_keys row names key {key_id}, but this primary loaded key \
             {loaded} from keys/signing.key and signs with it; the row and its change-log \
             entries are left as they are, and custody's reconcile at this start treats the \
             loaded key {loaded} as the signing key in use: it seals that key to every machine \
             with a bound custody key and deletes the seals of key {key_id}"
        ),
        Some((key_id,)) => {
            let newest: Option<String> = sqlx::query_scalar(NEWEST_CREATE_OR_DELETE)
                .bind("signing_key")
                .bind(&key_id)
                .fetch_optional(&mut *w.tx)
                .await
                .map_err(|e| format!("cannot read the change log of signing key {key_id}: {e}"))?;
            if newest.as_deref() != Some("create") {
                record_current_key(&mut w.tx, &key_id).await?;
                done.signing_key = true;
            }
        }
    }

    if done != Backfilled::default() {
        w.commit()
            .await
            .map_err(|e| format!("cannot commit the change-log backfill: {e}"))?;
    }
    Ok(done)
}

/// Inserts the primary's signing key as the `current` row of `signing_keys`
/// (migration 0012), `key_id` being the lowercase hex SHA-256 of the 32-byte
/// public key (section 7.2) and `public_key` base64, then records it with
/// [`record_current_key`]. Only the public half is read.
async fn insert_current_key(conn: &mut SqliteConnection, keys: &ClusterKeys) -> Result<(), String> {
    let key_id = keys.key_id();
    sqlx::query(
        "INSERT INTO signing_keys (key_id, public_key, state, created_at) \
         VALUES (?, ?, 'current', ?)",
    )
    .bind(key_id)
    .bind(keys.public_key_base64())
    .bind(now_rfc3339())
    .execute(&mut *conn)
    .await
    .map_err(|e| format!("cannot store the current signing key {key_id}: {e}"))?;
    record_current_key(conn, key_id).await
}

/// Writes the `create` entry of the `signing_keys` row `key_id`, whose
/// payload is the whole row as the table holds it (migration 0012):
/// `key_id`, `public_key`, `state` and `created_at`.
async fn record_current_key(conn: &mut SqliteConnection, key_id: &str) -> Result<(), String> {
    let (public_key, state, created_at): (String, String, String) =
        sqlx::query_as("SELECT public_key, state, created_at FROM signing_keys WHERE key_id = ?")
            .bind(key_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| format!("cannot read the signing_keys row of {key_id}: {e}"))?;
    changelog::append(
        conn,
        &Actor::system(SIGNING_KEY_ACTOR),
        "signing_key",
        key_id,
        Op::Create,
        json!({
            "key_id": key_id,
            "public_key": public_key,
            "state": state,
            "created_at": created_at,
        }),
    )
    .await
    .map_err(|e| format!("cannot record the current signing key {key_id}: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quoro-log-backfill-{}",
            &crate::store::random_secret()[..16]
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A node and its credential row written the old way: no change-log
    /// entry for either.
    async fn unlogged_credential(store: &Store, name: &str) -> String {
        let id = quoro_types::new_id(quoro_types::Prefix::Node);
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at) \
             VALUES (?, ?, '[\"edge\"]', NULL, 'disconnected', '', NULL, 1, '{}', '{}', '{}', '{}', \
             NULL, NULL, 0, 0, 0, NULL, ?)",
        )
        .bind(&id)
        .bind(name)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO node_credentials (node_id, credential_hash, created_at, revoked_at) \
             VALUES (?, ?, ?, NULL)",
        )
        .bind(&id)
        .bind(crate::store::sha256_hex(name.as_bytes()))
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        id
    }

    async fn log_head(store: &Store) -> (i64, i64) {
        sqlx::query_as("SELECT IFNULL(MAX(seq), 0), COUNT(*) FROM change_log")
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    /// A second run on the same database appends nothing to the change log,
    /// of any entity type, and writes no row.
    #[tokio::test]
    async fn a_second_run_appends_nothing() {
        let dir = temp_dir();
        let store = Store::open_in_memory().await.unwrap();
        let keys = ClusterKeys::load_or_generate(&dir).unwrap();
        unlogged_credential(&store, "edge-a").await;
        unlogged_credential(&store, "edge-b").await;

        let first = run(&store, &keys).await.unwrap();
        assert_eq!(
            first,
            Backfilled {
                credentials: 2,
                signing_key: true
            }
        );
        let head = log_head(&store).await;
        assert_eq!(head, (3, 3), "two credentials and the signing key");

        let second = run(&store, &keys).await.unwrap();
        assert_eq!(second, Backfilled::default());
        assert_eq!(
            log_head(&store).await,
            head,
            "the second run appends nothing"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signing_keys")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(rows, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Inserts a `current` row of `signing_keys`, with no change-log entry.
    async fn unlogged_signing_key(store: &Store, key_id: &str, public_key: &str) {
        sqlx::query(
            "INSERT INTO signing_keys (key_id, public_key, state, created_at) \
             VALUES (?, ?, 'current', '2026-10-01T10:00:00.000Z')",
        )
        .bind(key_id)
        .bind(public_key)
        .execute(store.pool())
        .await
        .unwrap();
    }

    /// Every `signing_key` entry, oldest first: `(entity_id, op, payload)`.
    async fn signing_key_entries(store: &Store) -> Vec<(String, String, serde_json::Value)> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT entity_id, op, payload FROM change_log \
             WHERE entity_type = 'signing_key' ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        rows.into_iter()
            .map(|(id, op, payload)| (id, op, serde_json::from_str(&payload).unwrap()))
            .collect()
    }

    /// The loaded key's `current` row whose newest `create` or `delete`
    /// entry is not a `create` gets a `create` entry carrying the row as the
    /// table holds it, as a credential does: once when the log holds no
    /// entry of it, and again after a logged `delete`. The row is not
    /// written again.
    #[tokio::test]
    async fn the_loaded_keys_row_without_a_create_gets_one() {
        let dir = temp_dir();
        let store = Store::open_in_memory().await.unwrap();
        let keys = ClusterKeys::load_or_generate(&dir).unwrap();
        let key_id = keys.key_id().to_string();
        unlogged_signing_key(&store, &key_id, &keys.public_key_base64()).await;
        let row = json!({
            "key_id": key_id,
            "public_key": keys.public_key_base64(),
            "state": "current",
            "created_at": "2026-10-01T10:00:00.000Z",
        });

        let first = run(&store, &keys).await.unwrap();
        assert_eq!(
            first,
            Backfilled {
                credentials: 0,
                signing_key: true
            }
        );
        assert_eq!(
            signing_key_entries(&store).await,
            [(key_id.clone(), "create".to_string(), row.clone())]
        );

        let mut w = store.begin_write().await.unwrap();
        changelog::append(
            &mut w.tx,
            &Actor::system("test"),
            "signing_key",
            &key_id,
            Op::Delete,
            json!({}),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();
        let second = run(&store, &keys).await.unwrap();
        assert!(second.signing_key, "a delete is newer than the create");
        let entries = signing_key_entries(&store).await;
        assert_eq!(entries.len(), 3, "{entries:?}");
        assert_eq!(entries[2], (key_id.clone(), "create".to_string(), row));

        assert_eq!(run(&store, &keys).await.unwrap(), Backfilled::default());
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signing_keys")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(rows, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A `current` row that names another key than the loaded one is left as
    /// it is, with no entry and no second row; the step only warns.
    #[tokio::test]
    async fn a_current_row_of_another_key_is_left_as_it_is() {
        let dir = temp_dir();
        let store = Store::open_in_memory().await.unwrap();
        let keys = ClusterKeys::load_or_generate(&dir).unwrap();
        let other = crate::store::sha256_hex(b"another key");
        unlogged_signing_key(&store, &other, "b3RoZXI=").await;

        assert_eq!(run(&store, &keys).await.unwrap(), Backfilled::default());
        assert!(signing_key_entries(&store).await.is_empty());
        let rows: Vec<(String,)> = sqlx::query_as("SELECT key_id FROM signing_keys")
            .fetch_all(store.pool())
            .await
            .unwrap();
        assert_eq!(rows, [(other,)]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

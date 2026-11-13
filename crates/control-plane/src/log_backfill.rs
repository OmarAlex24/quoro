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
//!   the log holds no `create` of: a row written before every insert and
//!   update was logged, or one whose only entries are updates, which the
//!   promotion's fold refuses to turn into a row (ADR 0317);
//! - its current signing key as the `current` row of `signing_keys`, with
//!   its `create` entry, when the table holds no `current` row. The row and
//!   the entry hold only the public key; the private key stays a file
//!   outside the database.
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
const UNLOGGED_CREDENTIALS: &str = "\
    SELECT c.node_id FROM node_credentials c \
    WHERE IFNULL( \
        (SELECT l.op FROM change_log l \
         WHERE l.entity_type = 'node_credential' AND l.entity_id = c.node_id \
           AND l.op IN ('create', 'delete') \
         ORDER BY l.seq DESC LIMIT 1), \
        'delete') <> 'create' \
    ORDER BY c.created_at, c.node_id";

/// What [`run`] wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Backfilled {
    /// `node_credential` `create` entries written.
    pub(crate) credentials: usize,
    /// Whether the current signing key's `signing_keys` row and its entry
    /// were written.
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
    match current {
        None => {
            insert_current_key(&mut w.tx, keys).await?;
            done.signing_key = true;
        }
        Some((key_id,)) if key_id != keys.key_id() => tracing::warn!(
            row = %key_id,
            loaded = %keys.key_id(),
            "the current signing_keys row names another key than the one this primary signs with; \
             it is left as it is"
        ),
        Some(_) => {}
    }

    if done != Backfilled::default() {
        w.commit()
            .await
            .map_err(|e| format!("cannot commit the change-log backfill: {e}"))?;
    }
    Ok(done)
}

/// Inserts the primary's signing key as the `current` row of `signing_keys`
/// with its change-log entry, op `create`, whose payload holds every column
/// (migration 0012): `key_id`, the lowercase hex SHA-256 of the 32-byte
/// public key (section 7.2), and `public_key`, base64. Only the public half
/// is read.
async fn insert_current_key(conn: &mut SqliteConnection, keys: &ClusterKeys) -> Result<(), String> {
    let key_id = keys.key_id();
    let public_key = keys.public_key_base64();
    let created_at = now_rfc3339();
    sqlx::query(
        "INSERT INTO signing_keys (key_id, public_key, state, created_at) \
         VALUES (?, ?, 'current', ?)",
    )
    .bind(key_id)
    .bind(&public_key)
    .bind(&created_at)
    .execute(&mut *conn)
    .await
    .map_err(|e| format!("cannot store the current signing key {key_id}: {e}"))?;
    changelog::append(
        conn,
        &Actor::system(SIGNING_KEY_ACTOR),
        "signing_key",
        key_id,
        Op::Create,
        json!({
            "key_id": key_id,
            "public_key": public_key,
            "state": "current",
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
}

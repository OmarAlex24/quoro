//! Rebuilding tables from the change log as a promotion does
//! (`node-agent-protocol.md` section 7.3 step 4, ADR 0297, ADR 0317), with
//! the promotion's own code: the entries of the chosen entity types are
//! folded per entity (`quoro_control_plane::rebuild::fold`) and the final
//! rows inserted into an empty database built from the same migrations,
//! each column decoded as its writer encodes it
//! (`quoro_control_plane::rebuild::database::insert_rows`).
//!
//! Unlike the whole build (`rebuild::database::build`), only the chosen
//! entity types are rebuilt, so a test may insert other rows directly, and
//! the rebuild runs with foreign keys off and applies no cascade: a row the
//! log does not remove stays, so a cascade the primary ran without writing
//! entries shows up as a row the rebuild kept. An entity that never received
//! a `create`, and a row made only of updates after a `delete`, fail the
//! test, as they stop or are left out of a promotion.

use super::Server;
use quoro_control_plane::rebuild::{self, database};
use quoro_control_plane::store::Store;
use serde_json::Value;
use sqlx::Row as _;

struct Column {
    name: String,
    pk: i64,
}

async fn columns(store: &Store, table: &str) -> Vec<Column> {
    let rows = sqlx::query("SELECT name, pk FROM pragma_table_info(?) ORDER BY cid")
        .bind(table)
        .fetch_all(store.pool())
        .await
        .unwrap();
    assert!(!rows.is_empty(), "no table {table}");
    rows.into_iter()
        .map(|row| Column {
            name: row.get(0),
            pk: row.get(1),
        })
        .collect()
}

/// Every column of `table` in `store`'s schema, in table order, as
/// `PRAGMA table_info` lists them: a test that compares whole rows takes
/// its column list from here, so a column added by a later migration is
/// compared too.
pub async fn column_names(store: &Store, table: &str) -> Vec<String> {
    columns(store, table)
        .await
        .into_iter()
        .map(|column| column.name)
        .collect()
}

/// The key columns in key order.
fn key_columns(columns: &[Column]) -> Vec<&str> {
    let mut key: Vec<&Column> = columns.iter().filter(|c| c.pk > 0).collect();
    key.sort_by_key(|c| c.pk);
    key.into_iter().map(|c| c.name.as_str()).collect()
}

/// Rebuilds the tables of `entity_types` from `server`'s change log into a
/// new in-memory database of the same schema, as a promotion folds and
/// inserts them (module documentation). A `create` that lacks a NOT NULL
/// column with no default, and a value its column cannot hold, fail the
/// rebuild.
pub async fn rebuild(server: &Server, entity_types: &[&str]) -> Store {
    for entity_type in entity_types {
        assert!(
            rebuild::table_of(entity_type).is_some(),
            "no table for entity_type {entity_type}"
        );
    }
    let logged: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, entity_type, entity_id, op, payload FROM change_log ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    let entries: Vec<rebuild::Entry<'_>> = logged
        .iter()
        .filter(|(_, entity_type, ..)| entity_types.contains(&entity_type.as_str()))
        .map(
            |(seq, entity_type, entity_id, op, payload)| rebuild::Entry {
                seq: seq.unsigned_abs(),
                entity_type,
                entity_id,
                op,
                payload,
            },
        )
        .collect();
    assert!(
        !entries.is_empty(),
        "no entries of {entity_types:?} in the log"
    );

    let store = Store::open_in_memory().await.unwrap();
    // One connection: the pragma holds for every statement below.
    let mut conn = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .unwrap();
    let schema = rebuild::Schema::read(&mut conn).await.unwrap();
    let folded = rebuild::fold(&schema, entries.iter().copied()).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        folded.dropped.is_empty(),
        "rows made only of updates after a delete: {:?}",
        folded.dropped
    );
    assert!(
        folded.mismatches.is_empty(),
        "creates whose key is not their entity_id: {:?}",
        folded.mismatches
    );
    database::insert_rows(&mut conn, &schema, &folded, &entries)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    drop(conn);
    store
}

/// The change log of `store` in `seq` order, as a node follows and stores
/// it: what `rebuild::database::build` takes.
pub async fn followed(store: &Store) -> Vec<database::FollowedEntry> {
    let rows = sqlx::query(
        "SELECT seq, term, time, actor_type, actor_id, entity_type, entity_id, op, payload, \
         prev_hash, hash FROM change_log ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    rows.iter()
        .map(|row| database::FollowedEntry {
            seq: row.get::<i64, _>(0).unsigned_abs(),
            term: row.get::<i64, _>(1).unsigned_abs(),
            time: row.get(2),
            actor_type: row.get(3),
            actor_id: row.get(4),
            entity_type: row.get(5),
            entity_id: row.get(6),
            op: row.get(7),
            payload: row.get(8),
            prev_hash: row.get(9),
            hash: row.get(10),
        })
        .collect()
}

/// `columns` of every row of `table`, ordered by its key. A JSON object or
/// array in a TEXT column is parsed, so key order does not matter; a BLOB
/// is its hex.
pub async fn rows(store: &Store, table: &str, columns: &[&str]) -> Vec<Vec<Value>> {
    let all = self::columns(store, table).await;
    let key = key_columns(&all);
    let select = columns
        .iter()
        .map(|c| {
            format!(
                "typeof(\"{c}\"), CASE WHEN typeof(\"{c}\") = 'blob' THEN hex(\"{c}\") \
                 ELSE CAST(\"{c}\" AS TEXT) END"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let order = key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {select} FROM \"{table}\" ORDER BY {order}");
    sqlx::query(&sql)
        .fetch_all(store.pool())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| {
            (0..columns.len())
                .map(|i| {
                    let kind: String = row.get(2 * i);
                    let text: Option<String> = row.get(2 * i + 1);
                    match (kind.as_str(), text) {
                        (_, None) => Value::Null,
                        ("integer", Some(t)) => Value::from(t.parse::<i64>().unwrap()),
                        ("real", Some(t)) => Value::from(t.parse::<f64>().unwrap()),
                        ("blob", Some(t)) => Value::String(format!("x'{}'", t.to_lowercase())),
                        (_, Some(t)) => match serde_json::from_str::<Value>(&t) {
                            Ok(v @ (Value::Object(_) | Value::Array(_))) => v,
                            _ => Value::String(t),
                        },
                    }
                })
                .collect()
        })
        .collect()
}

/// Asserts that `rebuilt` holds the same `columns` of `table` as `server`'s
/// database, and that the table has `count` rows.
pub async fn assert_rebuilt(
    server: &Server,
    rebuilt: &Store,
    table: &str,
    columns: &[&str],
    count: usize,
) {
    let original = rows(&server.state.store, table, columns).await;
    assert_eq!(
        original.len(),
        count,
        "{table} rows on the primary: {original:?}"
    );
    let replayed = rows(rebuilt, table, columns).await;
    assert_eq!(
        replayed, original,
        "{table} {columns:?} rebuilt from the log"
    );
}

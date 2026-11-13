//! Rebuilding tables from the change log as a promotion does
//! (`node-agent-protocol.md` section 7.3 step 4, ADR 0297): the entries of
//! the chosen entity types are applied in `seq` order to an empty database
//! built from the same migrations. A `create` inserts the payload's row, an
//! `update` merges the payload over the row `entity_id` names, a `delete`
//! removes that row, and a payload field with no column is ignored.
//!
//! The rebuild runs with foreign keys off, so a row the log does not remove
//! stays: a cascade the primary ran without writing entries shows up as a
//! row the rebuild kept. An `update` or `delete` of a row the log has not
//! created yet fails the test, since the rebuild cannot apply it.
//!
//! This is a test aid, not the promotion's fold of ADR 0317
//! (`quoro_control_plane::rebuild`), whose `entity_type` map it shares.

use super::Server;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use quoro_control_plane::rebuild;
use quoro_control_plane::store::Store;
use serde_json::Value;
use sqlx::Row as _;

/// The table each `entity_type` names, as the promotion's fold maps it
/// (`quoro_control_plane::rebuild::table_of`).
fn table_of(entity_type: &str) -> &'static str {
    rebuild::table_of(entity_type)
        .unwrap_or_else(|| panic!("no table for entity_type {entity_type}"))
}

struct Column {
    name: String,
    kind: String,
    pk: i64,
}

async fn columns(store: &Store, table: &str) -> Vec<Column> {
    let rows = sqlx::query("SELECT name, type, pk FROM pragma_table_info(?) ORDER BY cid")
        .bind(table)
        .fetch_all(store.pool())
        .await
        .unwrap();
    assert!(!rows.is_empty(), "no table {table}");
    rows.into_iter()
        .map(|row| Column {
            name: row.get(0),
            kind: row.get::<String, _>(1).to_uppercase(),
            pk: row.get(2),
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

enum Bound {
    Null,
    Text(String),
    Int(i64),
    Real(f64),
    Blob(Vec<u8>),
}

fn hex_decode(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex {text}");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// A payload value as its column stores it: a JSON object or array as JSON
/// text, a boolean as 0 or 1, a BLOB from hex (base64 for a custody seal,
/// migration 0012).
fn bind_value(table: &str, column: &Column, value: &Value) -> Bound {
    match (column.kind.as_str(), value) {
        (_, Value::Null) => Bound::Null,
        ("INTEGER", Value::Bool(b)) => Bound::Int(i64::from(*b)),
        ("INTEGER", Value::Number(n)) => Bound::Int(n.as_i64().unwrap()),
        ("REAL", Value::Number(n)) => Bound::Real(n.as_f64().unwrap()),
        ("BLOB", Value::String(s)) if table == "node_key_custody" => {
            Bound::Blob(STANDARD.decode(s).unwrap())
        }
        ("BLOB", Value::String(s)) => Bound::Blob(hex_decode(s)),
        ("TEXT", Value::String(s)) => Bound::Text(s.clone()),
        ("TEXT", other @ (Value::Object(_) | Value::Array(_))) => Bound::Text(other.to_string()),
        ("TEXT", other) => Bound::Text(other.to_string()),
        (kind, other) => panic!("{table}.{}: cannot store {other} in {kind}", column.name),
    }
}

async fn execute(store: &Store, sql: &str, values: Vec<Bound>) -> u64 {
    let mut query = sqlx::query(sql);
    for value in values {
        query = match value {
            Bound::Null => query.bind(None::<String>),
            Bound::Text(t) => query.bind(t),
            Bound::Int(i) => query.bind(i),
            Bound::Real(r) => query.bind(r),
            Bound::Blob(b) => query.bind(b),
        };
    }
    query
        .execute(store.pool())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
}

fn key_filter(key: &[&str], entity_id: &str) -> (String, Vec<Bound>) {
    let parts: Vec<&str> = if key.len() == 1 {
        vec![entity_id]
    } else {
        entity_id.split('/').collect()
    };
    assert_eq!(
        parts.len(),
        key.len(),
        "entity_id {entity_id} for key {key:?}"
    );
    let sql = key
        .iter()
        .map(|c| format!("\"{c}\" = ?"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let values = parts
        .into_iter()
        .map(|p| Bound::Text(p.to_string()))
        .collect();
    (sql, values)
}

/// Rebuilds the tables of `entity_types` from `server`'s change log into a
/// new in-memory database of the same schema. A `create` inserts only what
/// its payload carries, so a NOT NULL column the log leaves out fails the
/// rebuild.
pub async fn rebuild(server: &Server, entity_types: &[&str]) -> Store {
    let store = Store::open_in_memory().await.unwrap();
    // One connection: the pragma holds for every statement below.
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(store.pool())
        .await
        .unwrap();
    let on: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(on, 0, "the rebuild runs with foreign keys off");

    let entries =
        sqlx::query("SELECT seq, entity_type, entity_id, op, payload FROM change_log ORDER BY seq")
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    let mut applied = 0;
    for entry in entries {
        let entity_type: String = entry.get(1);
        if !entity_types.contains(&entity_type.as_str()) {
            continue;
        }
        let seq: i64 = entry.get(0);
        let entity_id: String = entry.get(2);
        let op: String = entry.get(3);
        let payload: Value = serde_json::from_str(&entry.get::<String, _>(4)).unwrap();
        let table = table_of(&entity_type);
        let columns = columns(&store, table).await;
        let key = key_columns(&columns);
        let fields = payload.as_object().cloned().unwrap_or_default();
        let at = format!("seq {seq} {op} {entity_type} {entity_id}");
        match op.as_str() {
            "create" => insert_row(&store, table, &columns, &fields, &at).await,
            "update" => {
                let (filter, key_values) = key_filter(&key, &entity_id);
                let mut sets = Vec::new();
                let mut values = Vec::new();
                for column in &columns {
                    if key.contains(&column.name.as_str()) {
                        continue;
                    }
                    if let Some(value) = fields.get(&column.name) {
                        sets.push(format!("\"{}\" = ?", column.name));
                        values.push(bind_value(table, column, value));
                    }
                }
                let sql = if sets.is_empty() {
                    format!(
                        "UPDATE \"{table}\" SET \"{}\" = \"{}\" WHERE {filter}",
                        key[0], key[0]
                    )
                } else {
                    format!("UPDATE \"{table}\" SET {} WHERE {filter}", sets.join(", "))
                };
                values.extend(key_values);
                assert_eq!(
                    execute(&store, &sql, values).await,
                    1,
                    "{at}: the row does not exist yet"
                );
            }
            "delete" => {
                let (filter, key_values) = key_filter(&key, &entity_id);
                let sql = format!("DELETE FROM \"{table}\" WHERE {filter}");
                assert_eq!(
                    execute(&store, &sql, key_values).await,
                    1,
                    "{at}: the row does not exist"
                );
            }
            other => panic!("{at}: unknown op {other}"),
        }
        applied += 1;
    }
    assert!(applied > 0, "no entries of {entity_types:?} in the log");
    store
}

/// Inserts one row of `table` holding only the fields `fields` carries.
async fn insert_row(
    store: &Store,
    table: &str,
    columns: &[Column],
    fields: &serde_json::Map<String, Value>,
    at: &str,
) {
    let mut names = Vec::new();
    let mut values = Vec::new();
    for column in columns {
        if let Some(value) = fields.get(&column.name) {
            names.push(format!("\"{}\"", column.name));
            values.push(bind_value(table, column, value));
        }
    }
    let sql = format!(
        "INSERT INTO \"{table}\" ({}) VALUES ({})",
        names.join(", "),
        vec!["?"; names.len()].join(", ")
    );
    assert_eq!(execute(store, &sql, values).await, 1, "{at}");
}

/// One entity's row as the fold has it so far.
struct Folded {
    first_seq: i64,
    created: bool,
    row: Option<serde_json::Map<String, Value>>,
}

/// Rebuilds the tables of `entity_types` from `server`'s change log as the
/// promotion's fold does (section 7.3 step 4, ADR 0317), into a new
/// in-memory database of the same schema. Each entity's entries, in `seq`
/// order, fold into one row: a `create` replaces it, an `update` merges over
/// it, or over an empty row when it has none yet, and a `delete` removes it.
/// The final rows are inserted once every entry is folded, each holding only
/// what its payloads carried. An entity that never received a `create` fails
/// the test, naming the entity and the `seq` of its first entry.
///
/// Unlike [`rebuild`], a log that holds an update before its create
/// rebuilds here, as it does in a promotion.
pub async fn rebuild_folded(server: &Server, entity_types: &[&str]) -> Store {
    let store = Store::open_in_memory().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(store.pool())
        .await
        .unwrap();
    let entries =
        sqlx::query("SELECT seq, entity_type, entity_id, op, payload FROM change_log ORDER BY seq")
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    let mut folded: std::collections::BTreeMap<(String, String), Folded> =
        std::collections::BTreeMap::new();
    for entry in entries {
        let entity_type: String = entry.get(1);
        if !entity_types.contains(&entity_type.as_str()) {
            continue;
        }
        let seq: i64 = entry.get(0);
        let entity_id: String = entry.get(2);
        let op: String = entry.get(3);
        let payload: Value = serde_json::from_str(&entry.get::<String, _>(4)).unwrap();
        let fields = payload.as_object().cloned().unwrap_or_default();
        let entity = folded
            .entry((entity_type, entity_id))
            .or_insert_with(|| Folded {
                first_seq: seq,
                created: false,
                row: None,
            });
        match op.as_str() {
            "create" => {
                entity.created = true;
                entity.row = Some(fields);
            }
            "update" => entity
                .row
                .get_or_insert_with(Default::default)
                .extend(fields),
            "delete" => entity.row = None,
            other => panic!("seq {seq}: unknown op {other}"),
        }
    }
    assert!(
        !folded.is_empty(),
        "no entries of {entity_types:?} in the log"
    );
    let mut folded: Vec<_> = folded.into_iter().collect();
    folded.sort_by_key(|(_, entity)| entity.first_seq);
    for ((entity_type, entity_id), entity) in folded {
        let at = format!(
            "{entity_type} {entity_id}, first entry seq {}",
            entity.first_seq
        );
        assert!(entity.created, "{at}: never received a create");
        let Some(row) = entity.row else { continue };
        let table = table_of(&entity_type);
        let columns = columns(&store, table).await;
        insert_row(&store, table, &columns, &row, &at).await;
    }
    store
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

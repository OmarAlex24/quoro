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

use super::Server;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use quoro_control_plane::store::Store;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row as _;

/// The table each `entity_type` names (`db-schema.sql` and its migrations).
fn table_of(entity_type: &str) -> &'static str {
    match entity_type {
        "agent" => "agents",
        "agent_key" => "agent_keys",
        "api_token" => "api_tokens",
        "app" => "apps",
        "app_env" => "app_env",
        "approval" => "approvals",
        "audit" => "audit_log",
        "audit_export" => "audit_exports",
        "backup" => "backups",
        "backup_repository" => "backup_repositories",
        "build" => "builds",
        "deployment" => "deployments",
        "domain" => "domains",
        "event" => "events",
        "grant" => "grants",
        "incident" => "incidents",
        "lease" => "leases",
        "node" => "nodes",
        "node_credential" => "node_credentials",
        "node_join_token" => "node_join_tokens",
        "node_key_custody" => "node_key_custody",
        "node_tunnel" => "node_tunnels",
        "project" => "projects",
        "provider" => "providers",
        "signing_key" => "signing_keys",
        "user" => "users",
        "vm" => "vms",
        "volume" => "volumes",
        "volume_quarantine" => "volume_quarantines",
        "vulnerability_finding" => "vulnerability_findings",
        "webhook" => "webhooks",
        other => panic!("no table for entity_type {other}"),
    }
}

/// A column a `create` payload lacks for a reason outside the test, which
/// the rebuild fills with a placeholder so the row can be inserted. The
/// placeholder is the SHA-256 of `rebuilt:<entity_id>`, so it is unique and
/// passes the 64-hex CHECK of the hash columns. A test never compares it.
#[derive(Clone, Copy)]
pub struct Placeholder {
    pub table: &'static str,
    pub column: &'static str,
}

/// `api_tokens.token_hash` and `node_join_tokens.token_hash` are left out
/// of the log on purpose, pending a maintainer decision (#751).
pub const TOKEN_HASHES: &[Placeholder] = &[
    Placeholder {
        table: "api_tokens",
        column: "token_hash",
    },
    Placeholder {
        table: "node_join_tokens",
        column: "token_hash",
    },
];

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
/// new in-memory database of the same schema, filling `placeholders` when a
/// `create` lacks them.
pub async fn rebuild(
    server: &Server,
    entity_types: &[&str],
    placeholders: &[Placeholder],
) -> Store {
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
            "create" => {
                let mut names = Vec::new();
                let mut values = Vec::new();
                for column in &columns {
                    if let Some(value) = fields.get(&column.name) {
                        names.push(format!("\"{}\"", column.name));
                        values.push(bind_value(table, column, value));
                    } else if placeholders
                        .iter()
                        .any(|p| p.table == table && p.column == column.name)
                    {
                        names.push(format!("\"{}\"", column.name));
                        let hash = Sha256::digest(format!("rebuilt:{entity_id}").as_bytes());
                        values.push(Bound::Text(
                            hash.iter().map(|b| format!("{b:02x}")).collect(),
                        ));
                    }
                }
                let sql = format!(
                    "INSERT INTO \"{table}\" ({}) VALUES ({})",
                    names.join(", "),
                    vec!["?"; names.len()].join(", ")
                );
                assert_eq!(execute(&store, &sql, values).await, 1, "{at}");
            }
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

// SPDX-License-Identifier: AGPL-3.0-only
//! The fold of the promotion rebuild (`node-agent-protocol.md` section 7.3
//! step 4; ADR 0317, ADR 0297, ADR 0229, ADR 0286): the followed change-log
//! entries, in `seq` order, folded into one in-memory row per entity.
//!
//! - A `create` replaces the entity's row with the payload's row. A column
//!   the payload lacks takes the column's default ([`Cell::Default`]).
//! - An `update` merges the payload over the row, so a field the payload
//!   lacks keeps its value. An `update` for an entity that has no row merges
//!   into an empty row, so a log that holds an update before its create
//!   still rebuilds.
//! - A `delete` removes the row and ignores its payload.
//! - A payload field with no column in this schema is ignored, such as a
//!   field the primary derives on read (`App.warm`) or a column a later
//!   migration dropped.
//! - Once every entry is folded, an entity that never received a `create`
//!   stops the rebuild with [`FoldError::NoCreate`], which names the entity
//!   and the `seq` of its first entry. No row made only of updates is ever
//!   returned as a row: one built after a `delete` is listed in
//!   [`Folded::dropped`] instead.
//!
//! Each `entity_type` names one table ([`table_of`]). An entity's row is the
//! one its `entity_id` names, which for a table with a composite key joins
//! the key's columns with `/`. A `create` whose key columns, joined with
//! `/`, are not its `entity_id` does not stop the fold: its row is kept
//! under its `entity_id`, and the `create` is listed in
//! [`Folded::mismatches`] for the caller to log. An app's `warm_node_id`,
//! `warm_node_pinned` and `warm_interval_s` travel under their column names
//! in a whole-App payload and are absent from a partial one, so the merge
//! writes or keeps them like any other column (ADR 0286).
//! `backup_repositories.repo_key_enc` is in no payload (section 7.1), so the
//! rebuilt row holds an empty BLOB there ([`Cell::EmptyBlob`]).
//!
//! The schema's `ON DELETE CASCADE` deletes need the deletions and their
//! order: a row the primary removed by a cascade has no entry of its own.
//! So the fold returns every `delete` with its `seq` ([`Folded::deletions`])
//! and each row with the `seq` of the `create` it starts from
//! ([`FoldedRow::created_seq`]). With them the next step can tell that a
//! child created before its parent's `delete` went with it, even when the
//! parent was created again later under the same id.
//!
//! Writing the folded rows into the new database is not done here: the
//! insert with foreign keys deferred, the cascades, the copy of each entry
//! into `change_log`, `meta` and the checks of both chains. [`Schema`]
//! keeps each column's declared type, `NOT NULL` and default for that
//! insert ([`Column`]).

use serde_json::{Map, Value};
use sqlx::SqliteConnection;
use std::collections::BTreeMap;

/// The table each `entity_type` names (`db-schema.sql` and its migrations).
/// Tables marked LOCAL, `meta` and the change log itself are not here: the
/// log carries no entry for them.
const TABLES: [(&str, &str); 31] = [
    ("agent", "agents"),
    ("agent_key", "agent_keys"),
    ("api_token", "api_tokens"),
    ("app", "apps"),
    ("app_env", "app_env"),
    ("approval", "approvals"),
    ("audit", "audit_log"),
    ("audit_export", "audit_exports"),
    ("backup", "backups"),
    ("backup_repository", "backup_repositories"),
    ("build", "builds"),
    ("deployment", "deployments"),
    ("domain", "domains"),
    ("event", "events"),
    ("grant", "grants"),
    ("incident", "incidents"),
    ("lease", "leases"),
    ("node", "nodes"),
    ("node_credential", "node_credentials"),
    ("node_join_token", "node_join_tokens"),
    ("node_key_custody", "node_key_custody"),
    ("node_tunnel", "node_tunnels"),
    ("project", "projects"),
    ("provider", "providers"),
    ("signing_key", "signing_keys"),
    ("user", "users"),
    ("vm", "vms"),
    ("volume", "volumes"),
    ("volume_quarantine", "volume_quarantines"),
    ("vulnerability_finding", "vulnerability_findings"),
    ("webhook", "webhooks"),
];

/// The table `entity_type` names, or `None` for an `entity_type` the change
/// log does not carry.
#[must_use]
pub fn table_of(entity_type: &str) -> Option<&'static str> {
    TABLES
        .iter()
        .find(|(kind, _)| *kind == entity_type)
        .map(|(_, table)| *table)
}

/// The table and column whose rebuilt value is always an empty BLOB: the
/// wrapped repository key, which stays out of the log (section 7.1).
const REPO_KEY: (&str, &str) = ("backup_repositories", "repo_key_enc");

/// One column of a table, as `PRAGMA table_info` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// The declared type as the schema writes it (`TEXT`, `INTEGER`,
    /// `BLOB`, ...), empty when the column declares none.
    pub declared_type: String,
    /// Whether the column is `NOT NULL`.
    pub not_null: bool,
    /// The SQL text of the column's default as the schema writes it (`0`,
    /// `'none'`, ...), or `None` when it has no `DEFAULT`.
    pub default: Option<String>,
}

/// The columns of one table.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Table {
    /// Every column, in table order.
    columns: Vec<Column>,
    /// The key's columns, in key order.
    key: Vec<String>,
}

/// The columns of every table [`table_of`] names, as this binary's schema
/// (`db-schema.sql` and every migration) defines them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Schema {
    tables: BTreeMap<&'static str, Table>,
}

impl Schema {
    /// Reads the columns of every table [`table_of`] names through `conn`,
    /// a database with this binary's schema. A table the schema lacks is
    /// left out, and an entry for it stops the fold
    /// ([`FoldError::NoTable`]).
    ///
    /// # Errors
    ///
    /// A database error.
    pub async fn read(conn: &mut SqliteConnection) -> Result<Self, sqlx::Error> {
        let mut tables = BTreeMap::new();
        for (_, table) in TABLES {
            let rows: Vec<(String, String, bool, Option<String>, i64)> = sqlx::query_as(
                "SELECT name, \"type\", \"notnull\", dflt_value, pk \
                 FROM pragma_table_info(?) ORDER BY cid",
            )
            .bind(table)
            .fetch_all(&mut *conn)
            .await?;
            if rows.is_empty() {
                continue;
            }
            let mut key: Vec<(i64, String)> = rows
                .iter()
                .filter(|(.., pk)| *pk > 0)
                .map(|(name, .., pk)| (*pk, name.clone()))
                .collect();
            key.sort();
            tables.insert(
                table,
                Table {
                    columns: rows
                        .into_iter()
                        .map(|(name, declared_type, not_null, default, _)| Column {
                            name,
                            declared_type,
                            not_null,
                            default,
                        })
                        .collect(),
                    key: key.into_iter().map(|(_, name)| name).collect(),
                },
            );
        }
        Ok(Self { tables })
    }

    /// Every column of `table`, in table order, or `None` for a table this
    /// schema lacks.
    #[must_use]
    pub fn columns(&self, table: &str) -> Option<&[Column]> {
        self.tables.get(table).map(|t| t.columns.as_slice())
    }

    /// The key columns of `table`, in key order, or `None` for a table this
    /// schema lacks.
    #[must_use]
    pub fn key(&self, table: &str) -> Option<&[String]> {
        self.tables.get(table).map(|t| t.key.as_slice())
    }
}

/// One followed change-log entry, as the fold reads it (section 5.14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    pub seq: u64,
    pub entity_type: &'a str,
    pub entity_id: &'a str,
    /// `create`, `update` or `delete`.
    pub op: &'a str,
    /// The payload object's JSON text.
    pub payload: &'a str,
}

/// One column of a folded row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    /// What a payload wrote under the column's name, as the payload carries
    /// it.
    Value(Value),
    /// The column's default: a `create` lacked the column, and no `update`
    /// since wrote it.
    Default,
    /// An empty BLOB: `backup_repositories.repo_key_enc`, which no payload
    /// carries.
    EmptyBlob,
}

/// A folded row's columns, by name.
pub type Row = BTreeMap<String, Cell>;

/// A row the fold returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoldedRow {
    /// The `seq` of the `create` this row starts from: the entity's last
    /// `create`. Every `update` since is merged over it.
    pub created_seq: u64,
    /// Every column of its table, by name.
    pub cells: Row,
}

/// A row the fold does not return although its entity received a `create`:
/// after a `delete`, `update`s alone built it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dropped {
    pub entity_type: String,
    pub entity_id: String,
    /// The `seq` of the `update` that began the row after the `delete`.
    pub seq: u64,
    /// The columns those updates wrote, merged in `seq` order into an empty
    /// row. The key's columns are not among them.
    pub cells: Row,
}

/// A `delete` the fold met.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deletion {
    pub entity_type: String,
    pub entity_id: String,
    pub seq: u64,
}

/// A `create` whose row its `entity_id` does not name. The fold keeps the
/// row under `entity_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyMismatch {
    pub seq: u64,
    pub entity_type: String,
    pub entity_id: String,
    /// The payload's key columns joined with `/`, or `None` when one is
    /// missing or is neither a string nor an integer.
    pub payload_key: Option<String>,
}

/// The result of [`fold`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Folded {
    /// The final rows, per table, each under the `entity_id` that names it.
    pub tables: BTreeMap<&'static str, BTreeMap<String, FoldedRow>>,
    /// Every `delete`, in `seq` order, for the cascades its deleted row
    /// would have run.
    pub deletions: Vec<Deletion>,
    /// The rows made only of updates that the fold left out, in `seq`
    /// order. Section 7.3 step 4 inserts no such row.
    pub dropped: Vec<Dropped>,
    /// The `create`s whose key columns are not their `entity_id`, in `seq`
    /// order.
    pub mismatches: Vec<KeyMismatch>,
}

/// Why the fold stops the rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoldError {
    /// The entry at `seq` does not come after the entry at `after`.
    OutOfOrder { seq: u64, after: u64 },
    /// The entry at `seq` names an `entity_type` that names no table.
    UnknownEntityType { seq: u64, entity_type: String },
    /// The entry at `seq` names a table this schema lacks.
    NoTable { seq: u64, table: &'static str },
    /// The entry at `seq` has an `op` other than `create`, `update` or
    /// `delete`.
    BadOp { seq: u64, op: String },
    /// The payload of the entry at `seq` is not a JSON object.
    BadPayload { seq: u64 },
    /// The entity never received a `create`; `first_seq` is its first
    /// entry (ADR 0317).
    NoCreate {
        entity_type: String,
        entity_id: String,
        first_seq: u64,
    },
}

impl std::fmt::Display for FoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfOrder { seq, after } => {
                write!(f, "the entry at seq {seq} comes after seq {after}")
            }
            Self::UnknownEntityType { seq, entity_type } => write!(
                f,
                "the entry at seq {seq} has the entity_type {entity_type:?}, which names no table"
            ),
            Self::NoTable { seq, table } => write!(
                f,
                "the entry at seq {seq} is for the table {table}, which this schema lacks"
            ),
            Self::BadOp { seq, op } => {
                write!(f, "the entry at seq {seq} has the unknown op {op:?}")
            }
            Self::BadPayload { seq } => {
                write!(
                    f,
                    "the payload of the entry at seq {seq} is not a JSON object"
                )
            }
            Self::NoCreate {
                entity_type,
                entity_id,
                first_seq,
            } => write!(
                f,
                "the {entity_type} {entity_id} never received a create; its first entry is seq {first_seq}"
            ),
        }
    }
}

impl std::error::Error for FoldError {}

/// One entity while the fold runs.
struct Entity {
    table: &'static str,
    /// The `seq` of its first entry.
    first_seq: u64,
    /// Whether it ever received a `create`.
    created: bool,
    /// Its row.
    current: Current,
}

/// An entity's row while the fold runs.
enum Current {
    /// No row: before its first entry that writes one, and after a
    /// `delete`.
    Absent,
    /// The row the `create` at `seq` began, with every `update` since
    /// merged over it.
    Created { seq: u64, row: Row },
    /// A row only `update`s built, the first of them at `seq`: before the
    /// entity's first `create`, or after a `delete`.
    Updates { seq: u64, row: Row },
}

/// Folds `entries`, in `seq` order, into one row per entity, under the rules
/// of section 7.3 step 4 (module documentation).
///
/// # Errors
///
/// The first [`FoldError`] met, in entry order; then, once every entry is
/// folded, [`FoldError::NoCreate`] for the entity with the lowest first
/// `seq` among those that never received a `create`.
pub fn fold<'a>(
    schema: &Schema,
    entries: impl IntoIterator<Item = Entry<'a>>,
) -> Result<Folded, FoldError> {
    let mut entities: BTreeMap<(String, String), Entity> = BTreeMap::new();
    let mut folded = Folded::default();
    let mut last: Option<u64> = None;
    for entry in entries {
        let seq = entry.seq;
        if let Some(after) = last
            && seq <= after
        {
            return Err(FoldError::OutOfOrder { seq, after });
        }
        last = Some(seq);
        let table_name =
            table_of(entry.entity_type).ok_or_else(|| FoldError::UnknownEntityType {
                seq,
                entity_type: entry.entity_type.to_owned(),
            })?;
        let table = schema.tables.get(table_name).ok_or(FoldError::NoTable {
            seq,
            table: table_name,
        })?;
        let entity = entities
            .entry((entry.entity_type.to_owned(), entry.entity_id.to_owned()))
            .or_insert_with(|| Entity {
                table: table_name,
                first_seq: seq,
                created: false,
                current: Current::Absent,
            });
        match entry.op {
            "create" => {
                let fields = payload(&entry)?;
                let payload_key = key_of(table, &fields);
                if payload_key.as_deref() != Some(entry.entity_id) {
                    folded.mismatches.push(KeyMismatch {
                        seq,
                        entity_type: entry.entity_type.to_owned(),
                        entity_id: entry.entity_id.to_owned(),
                        payload_key,
                    });
                }
                entity.current = Current::Created {
                    seq,
                    row: created_row(table_name, table, &fields),
                };
                entity.created = true;
            }
            "update" => {
                let fields = payload(&entry)?;
                if matches!(entity.current, Current::Absent) {
                    entity.current = Current::Updates {
                        seq,
                        row: Row::new(),
                    };
                }
                if let Current::Created { row, .. } | Current::Updates { row, .. } =
                    &mut entity.current
                {
                    merge(table_name, table, row, &fields);
                }
            }
            "delete" => {
                entity.current = Current::Absent;
                folded.deletions.push(Deletion {
                    entity_type: entry.entity_type.to_owned(),
                    entity_id: entry.entity_id.to_owned(),
                    seq,
                });
            }
            other => {
                return Err(FoldError::BadOp {
                    seq,
                    op: other.to_owned(),
                });
            }
        }
    }

    if let Some(((entity_type, entity_id), entity)) = entities
        .iter()
        .filter(|(_, entity)| !entity.created)
        .min_by_key(|(_, entity)| entity.first_seq)
    {
        return Err(FoldError::NoCreate {
            entity_type: entity_type.clone(),
            entity_id: entity_id.clone(),
            first_seq: entity.first_seq,
        });
    }

    for ((entity_type, entity_id), entity) in entities {
        match entity.current {
            Current::Absent => {}
            Current::Created { seq, row } => {
                folded.tables.entry(entity.table).or_default().insert(
                    entity_id,
                    FoldedRow {
                        created_seq: seq,
                        cells: row,
                    },
                );
            }
            Current::Updates { seq, row } => folded.dropped.push(Dropped {
                entity_type,
                entity_id,
                seq,
                cells: row,
            }),
        }
    }
    folded.dropped.sort_by_key(|dropped| dropped.seq);
    Ok(folded)
}

/// The payload object of `entry`.
fn payload(entry: &Entry<'_>) -> Result<Map<String, Value>, FoldError> {
    match serde_json::from_str(entry.payload) {
        Ok(Value::Object(fields)) => Ok(fields),
        _ => Err(FoldError::BadPayload { seq: entry.seq }),
    }
}

/// The `entity_id` that names the row `fields` carries: its key columns in
/// key order, joined with `/`. `None` when a key column is missing or is
/// neither a string nor an integer, or the table has no key.
fn key_of(table: &Table, fields: &Map<String, Value>) -> Option<String> {
    let parts = table
        .key
        .iter()
        .map(|column| match fields.get(column)? {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) if number.is_i64() || number.is_u64() => Some(number.to_string()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    (!parts.is_empty()).then(|| parts.join("/"))
}

/// Whether `column` of `table` takes its value from a payload. The wrapped
/// repository key never does (section 7.1).
fn from_payload(table: &str, column: &str) -> bool {
    (table, column) != REPO_KEY
}

/// The row a `create` writes: every column of `table`, from `fields` when
/// they carry it, the column's default otherwise. A field with no column is
/// ignored.
fn created_row(table_name: &str, table: &Table, fields: &Map<String, Value>) -> Row {
    table
        .columns
        .iter()
        .map(|Column { name, .. }| {
            let cell = if !from_payload(table_name, name) {
                Cell::EmptyBlob
            } else if let Some(value) = fields.get(name) {
                Cell::Value(value.clone())
            } else {
                Cell::Default
            };
            (name.clone(), cell)
        })
        .collect()
}

/// Merges an `update`'s `fields` over `row`: each column `fields` carries is
/// written, and every other keeps its value. A field with no column is
/// ignored, and so are the key's columns: the row stays the one `entity_id`
/// names.
fn merge(table_name: &str, table: &Table, row: &mut Row, fields: &Map<String, Value>) {
    for Column { name, .. } in &table.columns {
        if table.key.contains(name) || !from_payload(table_name, name) {
            continue;
        }
        if let Some(value) = fields.get(name) {
            row.insert(name.clone(), Cell::Value(value.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use serde_json::json;

    /// This binary's schema, as a promotion reads it from a new database.
    async fn schema() -> Schema {
        let store = Store::open_in_memory().await.unwrap();
        let mut conn = store.pool().acquire().await.unwrap();
        Schema::read(&mut conn).await.unwrap()
    }

    /// The names of `table`'s columns, in table order.
    fn names(schema: &Schema, table: &str) -> Vec<String> {
        schema
            .columns(table)
            .unwrap()
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    /// An owned entry; [`run`] borrows it as an [`Entry`].
    #[derive(Clone)]
    struct Owned {
        seq: u64,
        entity_type: &'static str,
        entity_id: String,
        op: &'static str,
        payload: String,
    }

    fn entry(
        seq: u64,
        entity_type: &'static str,
        entity_id: &str,
        op: &'static str,
        payload: Value,
    ) -> Owned {
        Owned {
            seq,
            entity_type,
            entity_id: entity_id.to_owned(),
            op,
            payload: payload.to_string(),
        }
    }

    fn run(schema: &Schema, entries: &[Owned]) -> Result<Folded, FoldError> {
        fold(
            schema,
            entries.iter().map(|e| Entry {
                seq: e.seq,
                entity_type: e.entity_type,
                entity_id: &e.entity_id,
                op: e.op,
                payload: &e.payload,
            }),
        )
    }

    /// A whole App payload, with the warm columns under their own names and
    /// `App.warm` null (ADR 0286).
    fn whole_app(id: &str, name: &str) -> Value {
        json!({
            "id": id, "project_id": "prj_1", "name": name, "stateful": true,
            "ha_policy": "warm", "image": "nginx", "port": 8080,
            "health_check": {"path": "/"}, "resources": {"vcpus": 1},
            "placement": {"node_ids": []}, "status": "running",
            "current_deployment_id": null, "restart_generation": 0,
            "created_at": "2026-11-01T00:00:00Z", "updated_at": "2026-11-01T00:00:00Z",
            "source": null, "backup_schedule": {"interval_h": 24, "keep": 7},
            "replicas": 1, "warm_node_id": "nod_b", "warm_node_pinned": true,
            "warm_interval_s": 60, "warm": null,
        })
    }

    fn row_of<'f>(folded: &'f Folded, table: &str, entity_id: &str) -> &'f Row {
        &folded.tables[table][entity_id].cells
    }

    fn value(v: Value) -> Cell {
        Cell::Value(v)
    }

    #[tokio::test]
    async fn a_create_replaces_the_row_with_the_payloads_row() {
        let schema = schema().await;
        // An update before the create fills fields the create lacks; the
        // create replaces that row, so they do not survive.
        let folded = run(
            &schema,
            &[
                entry(
                    1,
                    "app",
                    "app_1",
                    "update",
                    json!({"id": "app_1", "port": 9000, "status": "down"}),
                ),
                entry(
                    2,
                    "app",
                    "app_1",
                    "create",
                    json!({"id": "app_1", "name": "blog"}),
                ),
            ],
        )
        .unwrap();
        let row = row_of(&folded, "apps", "app_1");
        assert_eq!(row["name"], value(json!("blog")));
        assert_eq!(row["port"], Cell::Default);
        assert_eq!(row["status"], Cell::Default);
    }

    #[tokio::test]
    async fn an_update_merges_over_the_row_and_absent_fields_keep_their_value() {
        let schema = schema().await;
        let folded = run(
            &schema,
            &[
                entry(1, "app", "app_1", "create", whole_app("app_1", "blog")),
                entry(
                    2,
                    "app",
                    "app_1",
                    "update",
                    json!({"id": "app_1", "status": "down", "updated_at": "t2"}),
                ),
            ],
        )
        .unwrap();
        let row = row_of(&folded, "apps", "app_1");
        assert_eq!(row["status"], value(json!("down")));
        assert_eq!(row["updated_at"], value(json!("t2")));
        assert_eq!(row["name"], value(json!("blog")));
        assert_eq!(row["port"], value(json!(8080)));
        assert_eq!(row["created_at"], value(json!("2026-11-01T00:00:00Z")));
    }

    #[tokio::test]
    async fn a_delete_removes_the_row_and_ignores_its_payload() {
        let schema = schema().await;
        let folded = run(
            &schema,
            &[
                entry(1, "app", "app_1", "create", whole_app("app_1", "blog")),
                entry(2, "app", "app_2", "create", whole_app("app_2", "shop")),
                entry(3, "app", "app_3", "create", whole_app("app_3", "wiki")),
                // A delete whose payload is the whole object, with other
                // values: nothing of it is written.
                entry(4, "app", "app_1", "delete", whole_app("app_1", "other")),
                // Its payload is not even read.
                entry(5, "app", "app_3", "delete", json!([])),
            ],
        )
        .unwrap();
        let apps = &folded.tables["apps"];
        assert_eq!(apps.keys().collect::<Vec<_>>(), ["app_2"]);
        assert_eq!(apps["app_2"].cells["name"], value(json!("shop")));
        assert!(folded.dropped.is_empty());
    }

    #[tokio::test]
    async fn an_update_without_a_row_merges_into_an_empty_row() {
        let schema = schema().await;
        // An update before its create does not stop the fold, and the
        // create replaces what it wrote: the row is the create's, with the
        // later update merged over it. The update's `role` and
        // `password_hash`, which neither the create nor the later update
        // carries, are gone.
        let folded = run(
            &schema,
            &[
                entry(
                    1,
                    "user",
                    "usr_1",
                    "update",
                    json!({"id": "usr_1", "name": "Ada", "role": "owner", "password_hash": "h1"}),
                ),
                entry(
                    2,
                    "user",
                    "usr_1",
                    "create",
                    json!({"id": "usr_1", "email": "ada@example.com"}),
                ),
                entry(
                    3,
                    "user",
                    "usr_1",
                    "update",
                    json!({"id": "usr_1", "name": "Ada L"}),
                ),
            ],
        )
        .unwrap();
        let mut expected: Row = names(&schema, "users")
            .into_iter()
            .map(|name| (name, Cell::Default))
            .collect();
        expected.insert("id".to_owned(), value(json!("usr_1")));
        expected.insert("email".to_owned(), value(json!("ada@example.com")));
        expected.insert("name".to_owned(), value(json!("Ada L")));
        assert_eq!(
            folded.tables["users"]["usr_1"],
            FoldedRow {
                created_seq: 2,
                cells: expected,
            }
        );
        assert!(folded.dropped.is_empty());

        // An update with no create: the error names it, at its own seq.
        assert_eq!(
            run(
                &schema,
                &[
                    entry(1, "user", "usr_1", "create", json!({"id": "usr_1"})),
                    entry(
                        2,
                        "user",
                        "usr_2",
                        "update",
                        json!({"id": "usr_2", "name": "Bo"}),
                    ),
                ],
            ),
            Err(FoldError::NoCreate {
                entity_type: "user".to_owned(),
                entity_id: "usr_2".to_owned(),
                first_seq: 2,
            })
        );

        // After a delete, the updates merge into an empty row, one over the
        // other: the dropped row holds exactly the fields they wrote, the
        // later value winning, and no key column.
        let folded = run(
            &schema,
            &[
                entry(
                    1,
                    "user",
                    "usr_1",
                    "create",
                    json!({"id": "usr_1", "email": "ada@example.com", "name": "Ada"}),
                ),
                entry(2, "user", "usr_1", "delete", json!({})),
                entry(
                    3,
                    "user",
                    "usr_1",
                    "update",
                    json!({"id": "usr_1", "name": "A", "role": "member"}),
                ),
                entry(
                    4,
                    "user",
                    "usr_1",
                    "update",
                    json!({"id": "usr_1", "name": "B", "disabled": 1}),
                ),
            ],
        )
        .unwrap();
        assert!(!folded.tables.contains_key("users"));
        assert_eq!(
            folded.dropped,
            [Dropped {
                entity_type: "user".to_owned(),
                entity_id: "usr_1".to_owned(),
                seq: 3,
                cells: Row::from([
                    ("disabled".to_owned(), value(json!(1))),
                    ("name".to_owned(), value(json!("B"))),
                    ("role".to_owned(), value(json!("member"))),
                ]),
            }]
        );
    }

    #[tokio::test]
    async fn an_entity_that_never_received_a_create_stops_the_fold() {
        let schema = schema().await;
        let created = entry(1, "app", "app_1", "create", whole_app("app_1", "blog"));
        // Only updates, then a later one with an update and a delete: the
        // error names the entity whose first entry comes first, and its seq.
        let entries = [
            created,
            entry(
                2,
                "vm",
                "vm_9",
                "update",
                json!({"id": "vm_9", "desired_status": "running"}),
            ),
            entry(
                3,
                "app",
                "app_1",
                "update",
                json!({"id": "app_1", "status": "down"}),
            ),
            entry(
                4,
                "vm",
                "vm_9",
                "update",
                json!({"id": "vm_9", "desired_status": "stopped"}),
            ),
            entry(
                5,
                "user",
                "usr_2",
                "update",
                json!({"id": "usr_2", "name": "x"}),
            ),
            entry(6, "user", "usr_2", "delete", json!({})),
        ];
        assert_eq!(
            run(&schema, &entries),
            Err(FoldError::NoCreate {
                entity_type: "vm".to_owned(),
                entity_id: "vm_9".to_owned(),
                first_seq: 2,
            })
        );
        // An entity whose updates end in a delete never received a create
        // either, though it leaves no row.
        assert_eq!(
            run(
                &schema,
                &[entries[0].clone(), entries[4].clone(), entries[5].clone()]
            ),
            Err(FoldError::NoCreate {
                entity_type: "user".to_owned(),
                entity_id: "usr_2".to_owned(),
                first_seq: 5,
            })
        );
    }

    #[tokio::test]
    async fn a_row_made_only_of_updates_after_a_delete_is_not_returned() {
        let schema = schema().await;
        let folded = run(
            &schema,
            &[
                entry(
                    1,
                    "vm",
                    "vm_1",
                    "create",
                    json!({"id": "vm_1", "desired_status": "running"}),
                ),
                entry(2, "vm", "vm_1", "delete", json!({})),
                entry(
                    3,
                    "vm",
                    "vm_1",
                    "update",
                    json!({"id": "vm_1", "desired_status": "stopped"}),
                ),
                entry(
                    4,
                    "vm",
                    "vm_1",
                    "update",
                    json!({"id": "vm_1", "desired_status": "running"}),
                ),
                entry(5, "vm", "vm_2", "create", json!({"id": "vm_2"})),
            ],
        )
        .unwrap();
        assert_eq!(folded.tables["vms"].keys().collect::<Vec<_>>(), ["vm_2"]);
        assert_eq!(
            folded.dropped,
            [Dropped {
                entity_type: "vm".to_owned(),
                entity_id: "vm_1".to_owned(),
                seq: 3,
                cells: Row::from([("desired_status".to_owned(), value(json!("running")))]),
            }]
        );
    }

    /// The cascades need every delete and its seq, and the seq of each
    /// row's create (section 7.3 step 4). An app deleted after its variable
    /// was created, then created again under the same id, comes back as a
    /// row whose create is after the delete, while the variable's create is
    /// before it.
    #[tokio::test]
    async fn the_fold_returns_each_delete_and_the_seq_of_each_rows_last_create() {
        let schema = schema().await;
        let env = json!({"app_id": "app_1", "name": "FOO", "value_enc": "aa", "updated_at": "t1"});
        let folded = run(
            &schema,
            &[
                entry(1, "app", "app_1", "create", whole_app("app_1", "blog")),
                entry(2, "app_env", "app_1/FOO", "create", env),
                entry(3, "app", "app_2", "create", whole_app("app_2", "shop")),
                // The primary's cascade removed app_1/FOO with no entry.
                entry(4, "app", "app_1", "delete", json!({})),
                entry(5, "app", "app_1", "create", whole_app("app_1", "blog2")),
                entry(
                    6,
                    "app",
                    "app_1",
                    "update",
                    json!({"id": "app_1", "status": "down"}),
                ),
                entry(7, "app", "app_2", "delete", json!({})),
                entry(8, "app", "app_3", "create", whole_app("app_3", "wiki")),
                entry(9, "app", "app_3", "delete", json!({})),
                entry(10, "app", "app_3", "create", whole_app("app_3", "wiki")),
                entry(11, "app", "app_3", "delete", json!({})),
            ],
        )
        .unwrap();
        let deletion = |entity_id: &str, seq| Deletion {
            entity_type: "app".to_owned(),
            entity_id: entity_id.to_owned(),
            seq,
        };
        assert_eq!(
            folded.deletions,
            [
                deletion("app_1", 4),
                deletion("app_2", 7),
                deletion("app_3", 9),
                deletion("app_3", 11),
            ]
        );
        assert_eq!(folded.tables["apps"].keys().collect::<Vec<_>>(), ["app_1"]);
        let app = &folded.tables["apps"]["app_1"];
        assert_eq!(app.created_seq, 5);
        assert_eq!(app.cells["name"], value(json!("blog2")));
        assert_eq!(app.cells["status"], value(json!("down")));
        assert_eq!(folded.tables["app_env"]["app_1/FOO"].created_seq, 2);
        assert!(folded.dropped.is_empty());
    }

    /// The tables marked LOCAL in `db-schema.sql` and its migrations: the
    /// change log carries no entry for them, and a promoted primary starts
    /// them empty (section 7.3 step 4).
    const LOCAL: [&str; 5] = [
        "sessions",
        "access_tokens",
        "webhook_deliveries",
        "interactive_sessions",
        "osv_mirror",
    ];

    /// The tables that are never replicated and are not marked LOCAL:
    /// `meta` and the change log itself. The schema keeps no other
    /// bookkeeping table: its version is `meta.schema_version`.
    const NEVER_REPLICATED: [&str; 2] = ["meta", "change_log"];

    #[tokio::test]
    async fn each_entity_type_names_one_table_of_this_schema() {
        let schema = schema().await;
        let mut tables: Vec<&str> = Vec::new();
        for (entity_type, table) in TABLES {
            assert_eq!(table_of(entity_type), Some(table));
            assert!(
                schema.columns(table).is_some(),
                "{table} is not in the schema"
            );
            assert!(!LOCAL.contains(&table), "{table} is LOCAL");
            assert!(
                !NEVER_REPLICATED.contains(&table),
                "{table} is not replicated"
            );
            assert!(!tables.contains(&table), "{table} is named twice");
            tables.push(table);
        }
        // A row lands in the table its entity_type names.
        let folded = run(
            &schema,
            &[entry(
                1,
                "vulnerability_finding",
                "vln_1",
                "create",
                json!({"id": "vln_1"}),
            )],
        )
        .unwrap();
        assert!(folded.tables["vulnerability_findings"].contains_key("vln_1"));
        // An entity_type that names no table stops the fold.
        assert_eq!(
            run(
                &schema,
                &[entry(
                    1,
                    "session",
                    "ses_1",
                    "create",
                    json!({"id": "ses_1"})
                )]
            ),
            Err(FoldError::UnknownEntityType {
                seq: 1,
                entity_type: "session".to_owned(),
            })
        );
        // So does a table this schema lacks.
        let mut older = schema.clone();
        older.tables.remove("volume_quarantines");
        assert_eq!(
            run(
                &older,
                &[entry(
                    1,
                    "volume_quarantine",
                    "qrn_1",
                    "create",
                    json!({"id": "qrn_1"})
                )]
            ),
            Err(FoldError::NoTable {
                seq: 1,
                table: "volume_quarantines",
            })
        );
    }

    /// Every table of this schema is in the map, LOCAL or never replicated,
    /// so a replicated table a migration adds and the map leaves out fails
    /// here rather than in a promotion.
    #[tokio::test]
    async fn every_table_of_this_schema_is_mapped_local_or_never_replicated() {
        let store = Store::open_in_memory().await.unwrap();
        let in_schema: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert!(in_schema.len() > TABLES.len(), "{in_schema:?}");
        for table in &in_schema {
            let mapped = TABLES.iter().any(|(_, mapped)| mapped == table);
            let local = LOCAL.contains(&table.as_str());
            let never = NEVER_REPLICATED.contains(&table.as_str());
            assert_eq!(
                u8::from(mapped) + u8::from(local) + u8::from(never),
                1,
                "{table}: mapped {mapped}, LOCAL {local}, never replicated {never}"
            );
        }
        // Each list names tables this schema has, so none goes stale.
        for table in TABLES
            .iter()
            .map(|(_, table)| *table)
            .chain(LOCAL)
            .chain(NEVER_REPLICATED)
        {
            assert!(
                in_schema.iter().any(|name| name == table),
                "{table} is not in the schema"
            );
        }
    }

    /// `Schema::read` keeps each column's declared type, `NOT NULL` and
    /// default, as `PRAGMA table_info` reports them, and the key in key
    /// order.
    #[tokio::test]
    async fn the_schema_keeps_each_columns_type_not_null_and_default() {
        let schema = schema().await;
        let column = |table: &str, name: &str| {
            schema
                .columns(table)
                .unwrap()
                .iter()
                .find(|column| column.name == name)
                .unwrap()
                .clone()
        };
        let fact = |declared_type: &str, not_null: bool, default: Option<&str>| {
            (
                declared_type.to_owned(),
                not_null,
                default.map(str::to_owned),
            )
        };
        for (table, name, expected) in [
            ("apps", "id", fact("TEXT", true, None)),
            ("apps", "ha_policy", fact("TEXT", true, Some("'none'"))),
            ("apps", "warm_interval_s", fact("INTEGER", true, Some("30"))),
            ("apps", "warm_node_id", fact("TEXT", false, None)),
            ("apps", "current_deployment_id", fact("TEXT", false, None)),
            (
                "apps",
                "backup_schedule",
                fact("TEXT", true, Some(r#"'{"interval_h":24,"keep":7}'"#)),
            ),
            (
                "backup_repositories",
                "repo_key_enc",
                fact("BLOB", true, None),
            ),
            ("app_env", "value_enc", fact("BLOB", true, None)),
            ("users", "disabled", fact("INTEGER", true, Some("0"))),
        ] {
            let Column {
                declared_type,
                not_null,
                default,
                ..
            } = column(table, name);
            assert_eq!(
                (declared_type, not_null, default),
                expected,
                "{table}.{name}"
            );
        }
        assert_eq!(
            names(&schema, "app_env"),
            ["app_id", "name", "value_enc", "updated_at"]
        );
        assert_eq!(schema.key("app_env").unwrap(), ["app_id", "name"]);
        assert_eq!(
            schema.key("node_key_custody").unwrap(),
            ["node_id", "key_kind", "key_id"]
        );
        assert_eq!(schema.key("apps").unwrap(), ["id"]);
    }

    #[tokio::test]
    async fn a_composite_entity_id_joins_the_key_columns_with_a_slash() {
        let schema = schema().await;
        let env = |name: &str, value: &str| json!({"app_id": "app_1", "name": name, "value_enc": value, "updated_at": "t1"});
        let folded = run(
            &schema,
            &[
                entry(1, "app_env", "app_1/FOO", "create", env("FOO", "aa")),
                entry(2, "app_env", "app_1/BAR", "create", env("BAR", "bb")),
                entry(3, "app_env", "app_1/FOO", "update", json!({"app_id": "app_1", "name": "FOO", "value_enc": "cc"})),
                entry(4, "app_env", "app_1/BAR", "delete", json!({})),
                entry(
                    5,
                    "node_key_custody",
                    "nod_1/dek/0123456789abcdef0123456789abcdef",
                    "create",
                    json!({"node_id": "nod_1", "key_kind": "dek", "key_id": "0123456789abcdef0123456789abcdef", "sealed": "AA==", "created_at": "t1"}),
                ),
                // An integer key: the audit record's seq.
                entry(6, "audit", "7", "create", json!({"seq": 7, "action": "x"})),
            ],
        )
        .unwrap();
        let env_rows = &folded.tables["app_env"];
        assert_eq!(env_rows.keys().collect::<Vec<_>>(), ["app_1/FOO"]);
        assert_eq!(env_rows["app_1/FOO"].cells["value_enc"], value(json!("cc")));
        assert!(
            folded.tables["node_key_custody"]
                .contains_key("nod_1/dek/0123456789abcdef0123456789abcdef")
        );
        assert!(folded.tables["audit_log"].contains_key("7"));
        assert!(folded.mismatches.is_empty(), "{:?}", folded.mismatches);

        // A create whose entity_id is not its key joined with `/`, or that
        // lacks a key column, does not stop the fold: the row is kept under
        // its entity_id, an update finds it there, and the create is
        // reported with its seq and the key its payload gives.
        for (entity_id, payload, payload_key) in [
            ("app_1", env("FOO", "aa"), Some("app_1/FOO")),
            ("FOO/app_1", env("FOO", "aa"), Some("app_1/FOO")),
            (
                "app_1/FOO",
                json!({"app_id": "app_1", "value_enc": "aa"}),
                None,
            ),
            (
                "app_1/FOO",
                json!({"app_id": "app_1", "name": ["FOO"], "value_enc": "aa"}),
                None,
            ),
        ] {
            let folded = run(
                &schema,
                &[
                    entry(1, "app_env", "app_1/BAR", "create", env("BAR", "bb")),
                    entry(2, "app_env", entity_id, "create", payload),
                    entry(
                        3,
                        "app_env",
                        entity_id,
                        "update",
                        json!({"value_enc": "dd"}),
                    ),
                ],
            )
            .unwrap();
            assert_eq!(
                folded.mismatches,
                [KeyMismatch {
                    seq: 2,
                    entity_type: "app_env".to_owned(),
                    entity_id: entity_id.to_owned(),
                    payload_key: payload_key.map(str::to_owned),
                }],
                "{entity_id}"
            );
            let kept = &folded.tables["app_env"][entity_id];
            assert_eq!(kept.created_seq, 2, "{entity_id}");
            assert_eq!(kept.cells["value_enc"], value(json!("dd")), "{entity_id}");
            assert!(folded.tables["app_env"].contains_key("app_1/BAR"));
        }
    }

    #[tokio::test]
    async fn a_payload_field_with_no_column_is_ignored() {
        let schema = schema().await;
        let mut create = whole_app("app_1", "blog");
        create["ha_cost"] = json!({"memory_mb": 512});
        let folded = run(
            &schema,
            &[
                entry(1, "app", "app_1", "create", create),
                entry(
                    2,
                    "app",
                    "app_1",
                    "update",
                    json!({"id": "app_1", "lease": {"mode": "expiring"}, "dropped_column": 1}),
                ),
            ],
        )
        .unwrap();
        let row = row_of(&folded, "apps", "app_1");
        for field in ["warm", "ha_cost", "lease", "dropped_column"] {
            assert!(!row.contains_key(field), "{field}");
        }
        // Exactly the table's columns, each once.
        let mut columns = names(&schema, "apps");
        columns.sort();
        assert_eq!(row.keys().cloned().collect::<Vec<_>>(), columns);
    }

    #[tokio::test]
    async fn a_column_a_create_lacks_takes_its_default_and_an_update_keeps_it() {
        let schema = schema().await;
        // A create written before migration 0011 lacks the warm columns.
        let mut old = whole_app("app_1", "blog");
        for column in [
            "warm_node_id",
            "warm_node_pinned",
            "warm_interval_s",
            "replicas",
        ] {
            old.as_object_mut().unwrap().remove(column);
        }
        let folded = run(
            &schema,
            &[
                entry(1, "app", "app_1", "create", old),
                entry(
                    2,
                    "app",
                    "app_1",
                    "update",
                    json!({"id": "app_1", "status": "down"}),
                ),
            ],
        )
        .unwrap();
        let row = row_of(&folded, "apps", "app_1");
        assert_eq!(row.len(), schema.columns("apps").unwrap().len());
        for column in [
            "warm_node_id",
            "warm_node_pinned",
            "warm_interval_s",
            "replicas",
        ] {
            assert_eq!(row[column], Cell::Default, "{column}");
        }
        assert_eq!(row["status"], value(json!("down")));
    }

    #[tokio::test]
    async fn a_whole_app_payload_writes_the_warm_columns_and_a_partial_one_leaves_them() {
        let schema = schema().await;
        let mut moved = whole_app("app_1", "blog");
        moved["warm_node_id"] = json!(null);
        moved["warm_node_pinned"] = json!(false);
        moved["warm_interval_s"] = json!(120);
        let warm = |folded: &Folded| {
            let row = row_of(folded, "apps", "app_1");
            ["warm_node_id", "warm_node_pinned", "warm_interval_s"]
                .map(|column| row[column].clone())
        };
        let create = entry(1, "app", "app_1", "create", whole_app("app_1", "blog"));
        // A partial app payload, as deploy, status and restore write it.
        let partial = entry(
            2,
            "app",
            "app_1",
            "update",
            json!({"id": "app_1", "status": "down", "updated_at": "t2"}),
        );
        let folded = run(&schema, &[create.clone(), partial.clone()]).unwrap();
        assert_eq!(
            warm(&folded),
            [value(json!("nod_b")), value(json!(true)), value(json!(60))]
        );
        // A whole-App update writes them, a null warm node included.
        let folded = run(
            &schema,
            &[create, partial, entry(3, "app", "app_1", "update", moved)],
        )
        .unwrap();
        assert_eq!(
            warm(&folded),
            [value(json!(null)), value(json!(false)), value(json!(120))]
        );
    }

    #[tokio::test]
    async fn the_repository_key_is_an_empty_blob() {
        let schema = schema().await;
        let repository = json!({
            "id": "brp_1", "name": "main", "provider_id": "prv_1", "prefix": "q/",
            "recovery_key_fingerprint": "0123456789abcdef", "created_by": "usr_1",
            "created_at": "t1",
        });
        let mut carrying = repository.clone();
        carrying["repo_key_enc"] = json!("aabbcc");
        for entries in [
            vec![entry(
                1,
                "backup_repository",
                "brp_1",
                "create",
                repository.clone(),
            )],
            // A payload that carries the column anyway does not fill it.
            vec![entry(
                1,
                "backup_repository",
                "brp_1",
                "create",
                carrying.clone(),
            )],
            vec![
                entry(
                    1,
                    "backup_repository",
                    "brp_1",
                    "create",
                    repository.clone(),
                ),
                entry(
                    2,
                    "backup_repository",
                    "brp_1",
                    "update",
                    json!({"id": "brp_1", "repo_key_enc": "aabbcc"}),
                ),
            ],
        ] {
            let folded = run(&schema, &entries).unwrap();
            let row = row_of(&folded, "backup_repositories", "brp_1");
            assert_eq!(row["repo_key_enc"], Cell::EmptyBlob);
            assert_eq!(row["prefix"], value(json!("q/")));
            assert_eq!(row["control_plane_schedule"], Cell::Default);
        }
    }

    #[tokio::test]
    async fn malformed_entries_stop_the_fold() {
        let schema = schema().await;
        let create = entry(1, "vm", "vm_1", "create", json!({"id": "vm_1"}));
        let mut bad_op = entry(2, "vm", "vm_1", "upsert", json!({"id": "vm_1"}));
        assert_eq!(
            run(&schema, &[create.clone(), bad_op.clone()]),
            Err(FoldError::BadOp {
                seq: 2,
                op: "upsert".to_owned()
            })
        );
        bad_op.op = "update";
        bad_op.payload = "[]".to_owned();
        assert_eq!(
            run(&schema, &[create.clone(), bad_op]),
            Err(FoldError::BadPayload { seq: 2 })
        );
        assert_eq!(
            run(&schema, &[create.clone(), create]),
            Err(FoldError::OutOfOrder { seq: 1, after: 1 })
        );
    }
}

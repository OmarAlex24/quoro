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
//!   returned.
//!
//! Each `entity_type` names one table ([`table_of`]). An entity's row is the
//! one its `entity_id` names, which for a table with a composite key joins
//! the key's columns with `/`. An app's `warm_node_id`, `warm_node_pinned`
//! and `warm_interval_s` travel under their column names in a whole-App
//! payload and are absent from a partial one, so the merge writes or keeps
//! them like any other column (ADR 0286).
//! `backup_repositories.repo_key_enc` is in no payload (section 7.1), so the
//! rebuilt row holds an empty BLOB there ([`Cell::EmptyBlob`]).
//!
//! Writing the folded rows into the new database is not done here: the
//! insert with foreign keys deferred, the schema's `ON DELETE CASCADE`
//! deletes, the copy of each entry into `change_log`, `meta` and the checks
//! of both chains.

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

/// The columns of one table.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Table {
    /// Every column, in table order.
    columns: Vec<String>,
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
            let rows: Vec<(String, i64)> =
                sqlx::query_as("SELECT name, pk FROM pragma_table_info(?) ORDER BY cid")
                    .bind(table)
                    .fetch_all(&mut *conn)
                    .await?;
            if rows.is_empty() {
                continue;
            }
            let mut key: Vec<(i64, String)> = rows
                .iter()
                .filter(|(_, pk)| *pk > 0)
                .map(|(name, pk)| (*pk, name.clone()))
                .collect();
            key.sort();
            tables.insert(
                table,
                Table {
                    columns: rows.into_iter().map(|(name, _)| name).collect(),
                    key: key.into_iter().map(|(_, name)| name).collect(),
                },
            );
        }
        Ok(Self { tables })
    }

    /// Every column of `table`, in table order, or `None` for a table this
    /// schema lacks.
    #[must_use]
    pub fn columns(&self, table: &str) -> Option<&[String]> {
        self.tables.get(table).map(|t| t.columns.as_slice())
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
#[derive(Clone, Debug, PartialEq)]
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

/// A folded row: every column of its table, by name.
pub type Row = BTreeMap<String, Cell>;

/// A row the fold does not return although its entity received a `create`:
/// after a `delete`, `update`s alone built it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dropped {
    pub entity_type: String,
    pub entity_id: String,
    /// The `seq` of the `update` that began the row after the `delete`.
    pub seq: u64,
}

/// The result of [`fold`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Folded {
    /// The final rows, per table, each under the `entity_id` that names it.
    pub tables: BTreeMap<&'static str, BTreeMap<String, Row>>,
    /// The rows made only of updates that the fold left out, in `seq`
    /// order. Section 7.3 step 4 inserts no such row.
    pub dropped: Vec<Dropped>,
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
    /// The `create` at `seq` carries a row that its `entity_id` does not
    /// name: the payload's key columns, joined with `/`, are missing or
    /// differ from `entity_id`.
    KeyMismatch {
        seq: u64,
        entity_type: String,
        entity_id: String,
    },
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
            Self::KeyMismatch {
                seq,
                entity_type,
                entity_id,
            } => write!(
                f,
                "the create at seq {seq} carries a {entity_type} row whose key is not {entity_id:?}"
            ),
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
    /// Its row, absent before its first entry that writes one and after a
    /// `delete`.
    row: Option<Row>,
    /// When `row` holds only updates, the `seq` of the first of them.
    updates_since: Option<u64>,
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
                row: None,
                updates_since: None,
            });
        match entry.op {
            "create" => {
                let fields = payload(&entry)?;
                if key_of(table, &fields).as_deref() != Some(entry.entity_id) {
                    return Err(FoldError::KeyMismatch {
                        seq,
                        entity_type: entry.entity_type.to_owned(),
                        entity_id: entry.entity_id.to_owned(),
                    });
                }
                entity.row = Some(created_row(table_name, table, &fields));
                entity.created = true;
                entity.updates_since = None;
            }
            "update" => {
                let fields = payload(&entry)?;
                if entity.row.is_none() {
                    entity.updates_since = Some(seq);
                }
                let row = entity.row.get_or_insert_with(Row::new);
                merge(table_name, table, row, &fields);
            }
            "delete" => {
                entity.row = None;
                entity.updates_since = None;
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

    let mut folded = Folded::default();
    for ((entity_type, entity_id), entity) in entities {
        let Some(row) = entity.row else {
            continue;
        };
        if let Some(seq) = entity.updates_since {
            folded.dropped.push(Dropped {
                entity_type,
                entity_id,
                seq,
            });
            continue;
        }
        folded
            .tables
            .entry(entity.table)
            .or_default()
            .insert(entity_id, row);
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
/// neither a string nor an integer.
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
        .map(|column| {
            let cell = if !from_payload(table_name, column) {
                Cell::EmptyBlob
            } else if let Some(value) = fields.get(column) {
                Cell::Value(value.clone())
            } else {
                Cell::Default
            };
            (column.clone(), cell)
        })
        .collect()
}

/// Merges an `update`'s `fields` over `row`: each column `fields` carries is
/// written, and every other keeps its value. A field with no column is
/// ignored, and so are the key's columns: the row stays the one `entity_id`
/// names.
fn merge(table_name: &str, table: &Table, row: &mut Row, fields: &Map<String, Value>) {
    for column in &table.columns {
        if table.key.contains(column) || !from_payload(table_name, column) {
            continue;
        }
        if let Some(value) = fields.get(column) {
            row.insert(column.clone(), Cell::Value(value.clone()));
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
        &folded.tables[table][entity_id]
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
        assert_eq!(apps["app_2"]["name"], value(json!("shop")));
        assert!(folded.dropped.is_empty());
    }

    #[tokio::test]
    async fn an_update_without_a_row_merges_into_an_empty_row() {
        let schema = schema().await;
        // An update before its create rebuilds: it does not stop the fold.
        let folded = run(
            &schema,
            &[
                entry(
                    1,
                    "user",
                    "usr_1",
                    "update",
                    json!({"id": "usr_1", "name": "Ada"}),
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
        let row = row_of(&folded, "users", "usr_1");
        assert_eq!(row["email"], value(json!("ada@example.com")));
        assert_eq!(row["name"], value(json!("Ada L")));
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
            }]
        );
    }

    #[tokio::test]
    async fn each_entity_type_names_one_table_of_this_schema() {
        let schema = schema().await;
        let local = [
            "sessions",
            "access_tokens",
            "webhook_deliveries",
            "interactive_sessions",
            "osv_mirror",
            "meta",
            "change_log",
        ];
        let mut tables: Vec<&str> = Vec::new();
        for (entity_type, table) in TABLES {
            assert_eq!(table_of(entity_type), Some(table));
            assert!(
                schema.columns(table).is_some(),
                "{table} is not in the schema"
            );
            assert!(!local.contains(&table), "{table} is not replicated");
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
        assert_eq!(env_rows["app_1/FOO"]["value_enc"], value(json!("cc")));
        assert!(
            folded.tables["node_key_custody"]
                .contains_key("nod_1/dek/0123456789abcdef0123456789abcdef")
        );
        assert!(folded.tables["audit_log"].contains_key("7"));

        // A create whose entity_id is not its key joined with `/` stops the
        // fold, as does one that lacks a key column.
        for (entity_id, payload) in [
            ("app_1", env("FOO", "aa")),
            ("FOO/app_1", env("FOO", "aa")),
            ("app_1/FOO", json!({"app_id": "app_1", "value_enc": "aa"})),
        ] {
            assert_eq!(
                run(
                    &schema,
                    &[entry(1, "app_env", entity_id, "create", payload)]
                ),
                Err(FoldError::KeyMismatch {
                    seq: 1,
                    entity_type: "app_env".to_owned(),
                    entity_id: entity_id.to_owned(),
                }),
                "{entity_id}"
            );
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
        let mut columns = schema.columns("apps").unwrap().to_vec();
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

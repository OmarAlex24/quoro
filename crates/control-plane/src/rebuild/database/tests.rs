// SPDX-License-Identifier: AGPL-3.0-only
//! The build of section 7.3 step 4 on logs written by this crate's own
//! change-log and audit writers.

use super::*;
use crate::audit::{Channel, Outcome, Record};
use crate::changelog::{Actor, ActorType, Op};
use crate::store::sha256_hex;
use serde_json::json;

/// A primary's database, whose change log the tests write.
async fn primary() -> Store {
    Store::open_in_memory().await.unwrap()
}

/// Appends one entry to `store`'s change log, alone in its write.
async fn log(store: &Store, entity_type: &str, entity_id: &str, op: Op, payload: Value) -> u64 {
    let mut w = store.begin_write().await.unwrap();
    let seq = changelog::append(
        &mut w.tx,
        &Actor::system("test"),
        entity_type,
        entity_id,
        op,
        payload,
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
    seq.unsigned_abs()
}

/// Appends an audit record and its `audit` entry (`audit-log-format.md`
/// section 3.2).
async fn audit_record(store: &Store, operation: &str) {
    audit_record_with(store, operation, json!({"name": "x", "n": 1})).await;
}

/// [`audit_record`] with `params`.
async fn audit_record_with(store: &Store, operation: &str, params: Value) {
    let mut w = store.begin_write().await.unwrap();
    audit::append(
        &mut w.tx,
        &Record {
            actor: Actor {
                kind: ActorType::System,
                id: "test".to_owned(),
            },
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::System,
            operation: operation.to_owned(),
            scope: None,
            target_type: None,
            target_id: None,
            project_id: None,
            params,
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: "req_01JC9Z6Q5W8N3K4T7R2M1XHB0D".to_owned(),
            result: Outcome::Ok,
            result_detail: None,
            change_seq: None,
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
}

/// `store`'s change log, as a node follows and stores it.
async fn followed(store: &Store) -> Vec<FollowedEntry> {
    let rows = sqlx::query(
        "SELECT seq, term, time, actor_type, actor_id, entity_type, entity_id, op, payload, \
         prev_hash, hash FROM change_log ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    rows.iter()
        .map(|row| FollowedEntry {
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

/// A new, empty directory for a build's target, removed with everything in
/// it on drop, including when the test panics (as `tests/common`'s
/// `TestDir`).
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("quoro-test-rebuild-{}", &random_secret()[..12]));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn target(&self) -> PathBuf {
        self.0.join("quoro.db")
    }

    /// The names of every file in the directory.
    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        // A file written into the directory while it is being removed, as by
        // a connection of a dropped `Store` still closing, fails the removal;
        // a later try, after a short wait, removes that file too.
        for attempt in 0..3_u32 {
            match std::fs::remove_dir_all(&self.0) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    if attempt == 2 {
                        eprintln!("cannot remove the test directory {}: {e}", self.0.display());
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10 << attempt));
                }
                _ => return,
            }
        }
    }
}

/// Opens a built database.
async fn open(path: &Path) -> Store {
    Store::open(path).await.unwrap()
}

const CLUSTER: &str = "cls_01JC9Z6Q5W8N3K4T7R2M1XHB0D";
const AT: &str = "2026-11-13T10:00:00.000Z";

fn hex(bytes: &[u8]) -> String {
    crate::tunnel::hex(bytes)
}

fn user(id: &str, email: &str) -> Value {
    json!({
        "id": id, "email": email, "name": "Ada", "role": "owner",
        "password_hash": "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA",
        "disabled": 0, "created_at": AT,
    })
}

fn token(id: &str, user_id: &str, secret: &str) -> Value {
    json!({
        "id": id, "user_id": user_id, "name": id, "token_hash": sha256_hex(secret.as_bytes()),
        "expires_at": null, "last_used_at": null, "created_at": AT, "revoked_at": null,
    })
}

fn project(id: &str, slug: &str) -> Value {
    json!({"id": id, "slug": slug, "name": slug, "created_at": AT, "updated_at": AT})
}

fn app(id: &str, project_id: &str) -> Value {
    json!({
        "id": id, "project_id": project_id, "name": "web", "stateful": false,
        "ha_policy": "none", "image": "nginx", "port": 8080,
        "health_check": {"path": "/", "interval_s": 10}, "resources": {"vcpus": 1, "memory_mb": 512},
        "placement": {"node_ids": []}, "status": "running", "current_deployment_id": null,
        "restart_generation": 0, "created_at": AT, "updated_at": AT, "source": null,
        "backup_schedule": {"interval_h": 24, "keep": 7}, "replicas": 1,
        "warm_node_id": null, "warm_node_pinned": true, "warm_interval_s": 60, "warm": null,
    })
}

fn env(app_id: &str, name: &str, value_enc: &[u8]) -> Value {
    json!({"app_id": app_id, "name": name, "value_enc": hex(value_enc), "updated_at": AT})
}

fn deployment(id: &str, app_id: &str) -> Value {
    json!({
        "id": id, "app_id": app_id, "image": "nginx", "image_digest": null, "status": "succeeded",
        "phase": null, "error": null, "trigger": "api", "created_by_type": "user",
        "created_by_id": "usr_1", "created_at": AT, "finished_at": AT,
    })
}

fn node(id: &str) -> Value {
    json!({
        "id": id, "name": "alpha", "roles": ["node"], "cp_status": null, "status": "connected",
        "public_key": "cHVibGlj", "address": null, "behind_nat": false, "labels": {},
        "capacity": {"cpus": 8, "memory_mb": 16384, "disk_gb": 100},
        "allocated": {"cpus": 0, "memory_mb": 0, "disk_gb": 0},
        "kvm": {"available": true, "kernel": "6.9.0"}, "created_at": AT,
    })
}

/// A finding whose scores are floats, for the `REAL` columns.
fn finding(id: &str) -> Value {
    json!({
        "id": id, "subject_kind": "quoro", "component": "quoro", "vuln_id": "CVE-2026-0001",
        "aliases": ["GHSA-x"], "ecosystem": "crates.io", "package": "x", "installed_version": "1.0.0",
        "severity": "critical", "cvss_score": 9.8, "kev": true, "epss": 0.5,
        "first_seen_at": AT, "updated_at": AT,
    })
}

/// `typeof` and the value of `column` in the row `filter` selects, a BLOB
/// as its bytes' hex and NULL as `None`.
async fn stored(
    store: &Store,
    table: &str,
    column: &str,
    filter: &str,
) -> (String, Option<String>) {
    sqlx::query_as(&format!(
        "SELECT typeof(\"{column}\"), CASE WHEN typeof(\"{column}\") = 'blob' \
         THEN lower(hex(\"{column}\")) ELSE CAST(\"{column}\" AS TEXT) END \
         FROM \"{table}\" WHERE {filter}"
    ))
    .fetch_one(store.pool())
    .await
    .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
}

/// Every BLOB column of a table the change log carries is either one a
/// writer puts in its payloads, with that writer's encoding, or the wrapped
/// repository key, which no payload carries. A migration that adds a BLOB
/// column fails here until its encoding is named.
#[tokio::test]
async fn every_blob_column_of_a_replicated_table_has_its_writers_encoding() {
    let store = primary().await;
    let mut conn = store.pool().acquire().await.unwrap();
    let schema = Schema::read(&mut conn).await.unwrap();
    let mut blobs = Vec::new();
    for (_, table) in TABLES {
        for column in schema.columns(table).unwrap() {
            if column.declared_type.eq_ignore_ascii_case("BLOB") {
                blobs.push((table, column.name.clone()));
            }
        }
    }
    for (table, column) in &blobs {
        if (*table, column.as_str()) == super::super::REPO_KEY {
            assert_eq!(blob_encoding(table, column), None);
            continue;
        }
        assert!(
            blob_encoding(table, column).is_some(),
            "{table}.{column} has no writer encoding"
        );
    }
    for (table, column, _) in BLOB_ENCODINGS {
        assert!(
            blobs.iter().any(|(t, c)| *t == table && c == column),
            "{table}.{column} is not a BLOB column"
        );
    }
}

/// Each column a payload carries is stored as its writer encodes it: hex
/// for the ciphertexts and the run token, base64 for a custody seal, 0 or 1
/// for a boolean, JSON text for an object or an array, a float in a REAL
/// column. The wrapped repository key is an empty BLOB, and `meta` holds the
/// four keys of section 7.3 step 4 and nothing else.
#[tokio::test]
async fn each_column_round_trips_as_its_writer_encodes_it() {
    let primary = primary().await;
    let value_enc = [0x00, 0x01, 0xab, 0xff, 0x10];
    let secret_enc = [0xde, 0xad, 0xbe, 0xef];
    let token_enc = [0x7f, 0x80, 0x00];
    let sealed = [0xd3, 0x4d, 0x34, 0xd3];
    let hook_secret = [0x01, 0x02];
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "ada@example.com"),
    )
    .await;
    let mut provider = json!({
        "id": "prv_1", "kind": "tunnel", "name": "cf",
        "config_json": {"account_id": "a", "zone_id": "z"},
        "secret_enc": hex(&secret_enc), "created_by": "usr_1", "created_at": AT, "rotated_at": null,
    });
    log(&primary, "provider", "prv_1", Op::Create, provider.clone()).await;
    log(&primary, "node", "nod_1", Op::Create, node("nod_1")).await;
    log(
        &primary,
        "node_tunnel",
        "nod_1",
        Op::Create,
        json!({
            "node_id": "nod_1", "provider_id": "prv_1", "tunnel_id": null, "token_enc": null,
            "origin_port": 8080, "created_at": AT, "updated_at": AT,
        }),
    )
    .await;
    log(
        &primary,
        "node_tunnel",
        "nod_1",
        Op::Update,
        json!({"node_id": "nod_1", "provider_id": "prv_1", "tunnel_id": "tun1", "token_enc": hex(&token_enc), "updated_at": AT}),
    )
    .await;
    let key_id = "0123456789abcdef0123456789abcdef";
    log(
        &primary,
        "node_key_custody",
        &format!("nod_1/dek/{key_id}"),
        Op::Create,
        json!({
            "node_id": "nod_1", "key_kind": "dek", "key_id": key_id,
            "sealed": STANDARD.encode(sealed), "created_at": AT,
        }),
    )
    .await;
    log(
        &primary,
        "project",
        "prj_1",
        Op::Create,
        project("prj_1", "demo"),
    )
    .await;
    log(&primary, "app", "app_1", Op::Create, app("app_1", "prj_1")).await;
    log(
        &primary,
        "app_env",
        "app_1/FOO",
        Op::Create,
        env("app_1", "FOO", &value_enc),
    )
    .await;
    log(
        &primary,
        "webhook",
        "whk_1",
        Op::Create,
        json!({
            "id": "whk_1", "name": "hook", "url": "https://hooks.example.com/q",
            "event_types": ["app.deleted"], "selector": {"project_id": "prj_1"},
            "secret_enc": hex(&hook_secret), "enabled": true, "wake": null,
            "created_by": "usr_1", "created_at": AT, "updated_at": AT,
        }),
    )
    .await;
    provider["id"] = json!("prv_2");
    provider["kind"] = json!("object_store");
    provider["name"] = json!("backups");
    log(&primary, "provider", "prv_2", Op::Create, provider).await;
    log(
        &primary,
        "backup_repository",
        "brp_1",
        Op::Create,
        json!({
            "id": "brp_1", "name": "main", "provider_id": "prv_2", "prefix": "q",
            "recovery_key_fingerprint": "0123456789abcdef", "created_by": "usr_1", "created_at": AT,
        }),
    )
    .await;
    log(
        &primary,
        "vulnerability_finding",
        "vln_1",
        Op::Create,
        finding("vln_1"),
    )
    .await;
    log(
        &primary,
        "event",
        "evt_1",
        Op::Create,
        json!({
            "schema_version": 1, "id": "evt_1", "seq": 7, "type": "app.deleted", "time": AT,
            "source": {"kind": "control_plane"}, "subject": {"type": "app", "id": "app_9"},
            "project_id": "prj_1", "data": {"app_id": "app_9"},
        }),
    )
    .await;
    audit_record(&primary, "test.one").await;
    audit_record(&primary, "test.two").await;

    let dir = Dir::new();
    let entries = followed(&primary).await;
    let built = build(&entries, CLUSTER, 4, &dir.target()).await.unwrap();
    assert_eq!(built.head_seq, entries.last().unwrap().seq);
    assert_eq!(dir.files(), ["quoro.db"], "no temporary file is left");
    let rebuilt = open(&dir.target()).await;

    let blob = |bytes: &[u8]| ("blob".to_owned(), Some(hex(bytes)));
    let int = |value: i64| ("integer".to_owned(), Some(value.to_string()));
    for (table, column, filter, expected) in [
        ("app_env", "value_enc", "name = 'FOO'", blob(&value_enc)),
        ("providers", "secret_enc", "id = 'prv_1'", blob(&secret_enc)),
        (
            "node_tunnels",
            "token_enc",
            "node_id = 'nod_1'",
            blob(&token_enc),
        ),
        (
            "node_key_custody",
            "sealed",
            "node_id = 'nod_1'",
            blob(&sealed),
        ),
        ("webhooks", "secret_enc", "id = 'whk_1'", blob(&hook_secret)),
        (
            "backup_repositories",
            "repo_key_enc",
            "id = 'brp_1'",
            blob(&[]),
        ),
        ("apps", "warm_node_pinned", "id = 'app_1'", int(1)),
        ("apps", "stateful", "id = 'app_1'", int(0)),
        ("nodes", "behind_nat", "id = 'nod_1'", int(0)),
        ("webhooks", "enabled", "id = 'whk_1'", int(1)),
        ("vulnerability_findings", "kev", "id = 'vln_1'", int(1)),
        (
            "vulnerability_findings",
            "cvss_score",
            "id = 'vln_1'",
            ("real".to_owned(), Some("9.8".to_owned())),
        ),
        (
            "vulnerability_findings",
            "epss",
            "id = 'vln_1'",
            ("real".to_owned(), Some("0.5".to_owned())),
        ),
        (
            "webhooks",
            "wake",
            "id = 'whk_1'",
            ("null".to_owned(), None),
        ),
        ("events", "seq", "id = 'evt_1'", int(7)),
    ] {
        assert_eq!(
            stored(&rebuilt, table, column, filter).await,
            expected,
            "{table}.{column}"
        );
    }
    for (table, column, filter, expected) in [
        (
            "apps",
            "health_check",
            "id = 'app_1'",
            json!({"path": "/", "interval_s": 10}),
        ),
        ("apps", "placement", "id = 'app_1'", json!({"node_ids": []})),
        (
            "apps",
            "backup_schedule",
            "id = 'app_1'",
            json!({"interval_h": 24, "keep": 7}),
        ),
        (
            "providers",
            "config_json",
            "id = 'prv_1'",
            json!({"account_id": "a", "zone_id": "z"}),
        ),
        ("nodes", "roles", "id = 'nod_1'", json!(["node"])),
        (
            "webhooks",
            "event_types",
            "id = 'whk_1'",
            json!(["app.deleted"]),
        ),
        (
            "vulnerability_findings",
            "aliases",
            "id = 'vln_1'",
            json!(["GHSA-x"]),
        ),
        (
            "events",
            "source",
            "id = 'evt_1'",
            json!({"kind": "control_plane"}),
        ),
        (
            "audit_log",
            "params",
            "seq = 1",
            json!({"name": "x", "n": 1}),
        ),
    ] {
        let (kind, text) = stored(&rebuilt, table, column, filter).await;
        assert_eq!(kind, "text", "{table}.{column}");
        let parsed: Value = serde_json::from_str(&text.unwrap()).unwrap();
        assert_eq!(parsed, expected, "{table}.{column}");
    }
    // The audit log keeps `params` in RFC 8785 text, as its writer does.
    assert_eq!(
        stored(&rebuilt, "audit_log", "params", "seq = 2")
            .await
            .1
            .as_deref(),
        Some(r#"{"n":1,"name":"x"}"#)
    );
    // A column the create lacked takes its default: `nodes.desired_rev`.
    assert_eq!(
        stored(&rebuilt, "nodes", "desired_rev", "id = 'nod_1'").await,
        int(0)
    );

    let meta: Vec<(String, String)> = sqlx::query_as("SELECT key, value FROM meta ORDER BY key")
        .fetch_all(rebuilt.pool())
        .await
        .unwrap();
    let version = Store::shipped_schema_version().to_string();
    assert_eq!(
        meta,
        [
            ("cluster_id".to_owned(), CLUSTER.to_owned()),
            ("schema_version".to_owned(), version),
            ("term".to_owned(), "4".to_owned()),
            ("webhook_cursor".to_owned(), "7".to_owned()),
        ]
    );
    assert_eq!(built.webhook_cursor, 7);
    assert_eq!(built.rows["audit_log"], 2);
    assert_eq!(built.rows["apps"], 1);
    assert_eq!(built.rows["grants"], 0);
    rebuilt.pool().close().await;
}

/// A value that does not fit its column stops the build with the entity,
/// the `seq` of the entry that wrote it and the column; so does a create
/// that lacks a column with neither a default nor NULL. No file is left.
#[tokio::test]
async fn a_value_that_does_not_fit_names_the_entity_the_seq_and_the_column() {
    let primary = primary().await;
    log(
        &primary,
        "project",
        "prj_1",
        Op::Create,
        project("prj_1", "demo"),
    )
    .await;
    log(&primary, "app", "app_1", Op::Create, app("app_1", "prj_1")).await;
    log(
        &primary,
        "app_env",
        "app_1/FOO",
        Op::Create,
        env("app_1", "FOO", &[1, 2]),
    )
    .await;
    let bad = log(
        &primary,
        "app_env",
        "app_1/FOO",
        Op::Update,
        json!({"app_id": "app_1", "name": "FOO", "value_enc": "not hex"}),
    )
    .await;
    log(
        &primary,
        "app",
        "app_1",
        Op::Update,
        json!({"id": "app_1", "status": "down"}),
    )
    .await;
    let dir = Dir::new();
    let error = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    let BuildError::Column {
        entity_type,
        entity_id,
        seq,
        table,
        column,
        ..
    } = &error
    else {
        panic!("{error}");
    };
    assert_eq!(
        (
            entity_type.as_str(),
            entity_id.as_str(),
            *seq,
            *table,
            column.as_str()
        ),
        ("app_env", "app_1/FOO", bad, "app_env", "value_enc"),
        "{error}"
    );
    assert!(error.to_string().contains(&format!("seq {bad}")), "{error}");
    assert!(dir.files().is_empty(), "{:?}", dir.files());

    let primary = self::primary().await;
    log(
        &primary,
        "project",
        "prj_1",
        Op::Create,
        project("prj_1", "demo"),
    )
    .await;
    log(&primary, "app", "app_1", Op::Create, app("app_1", "prj_1")).await;
    let create = log(
        &primary,
        "app_env",
        "app_1/BAR",
        Op::Create,
        json!({"app_id": "app_1", "name": "BAR", "updated_at": AT}),
    )
    .await;
    let error = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, BuildError::Column { seq, column, .. } if *seq == create && column == "value_enc"),
        "{error}"
    );
    assert!(dir.files().is_empty(), "{:?}", dir.files());
}

/// A column holds JSON text when its table checks it with `json_valid`, a
/// column added by a migration included. A JSON `null` is the text `null`
/// in such a column that is `NOT NULL`, SQL `NULL` in a nullable one, and
/// refused in a `NOT NULL` column of plain text.
#[tokio::test]
async fn a_json_null_fills_a_column_as_the_column_holds_it() {
    let store = primary().await;
    let mut conn = store.pool().acquire().await.unwrap();
    let schema = Schema::read(&mut conn).await.unwrap();
    let find = |table: &str, name: &str| {
        schema
            .columns(table)
            .unwrap()
            .iter()
            .find(|column| column.name == name)
            .unwrap_or_else(|| panic!("{table}.{name}"))
            .clone()
    };
    for (table, name, json, not_null) in [
        ("audit_log", "params", true, true),
        ("approvals", "params", true, true),
        ("events", "data", true, true),
        ("apps", "backup_schedule", true, true),
        ("events", "actor", true, false),
        ("apps", "source", true, false),
        ("audit_log", "operation", false, true),
        ("approvals", "params_hash", false, true),
        ("audit_log", "params_sha256", false, false),
    ] {
        let column = find(table, name);
        assert_eq!(
            (column.json, column.not_null),
            (json, not_null),
            "{table}.{name}"
        );
        let decoded = decode(table, &column, &Value::Null);
        match (json, not_null) {
            (true, true) => assert_eq!(decoded, Ok(Bound::Text("null".to_owned()))),
            (_, false) => assert_eq!(decoded, Ok(Bound::Null), "{table}.{name}"),
            (false, true) => assert!(decoded.is_err(), "{table}.{name}: {decoded:?}"),
        }
    }
}

/// An approval decided with no body writes its audit record with `params`
/// `null` (`service::approvals::decide`): the primary stores the text
/// `null`, the record's entry carries JSON `null`, and the built database
/// stores the text `null`, whose record the build's audit chain check
/// verifies. The app's nullable `source` stays NULL.
#[tokio::test]
async fn an_audit_record_with_null_params_is_built_as_the_primary_stores_it() {
    let primary = primary().await;
    log(
        &primary,
        "project",
        "prj_1",
        Op::Create,
        project("prj_1", "demo"),
    )
    .await;
    log(&primary, "app", "app_1", Op::Create, app("app_1", "prj_1")).await;
    audit_record_with(
        &primary,
        "POST /api/v1/approvals/{approval_id}/approve",
        Value::Null,
    )
    .await;
    let text = ("text".to_owned(), Some("null".to_owned()));
    assert_eq!(
        stored(&primary, "audit_log", "params", "seq = 1").await,
        text
    );
    let payload: String =
        sqlx::query_scalar("SELECT payload FROM change_log WHERE entity_type = 'audit'")
            .fetch_one(primary.pool())
            .await
            .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload.get("params"), Some(&Value::Null), "{payload}");

    let dir = Dir::new();
    let built = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(built.rows["audit_log"], 1);
    let rebuilt = open(&dir.target()).await;
    assert_eq!(
        stored(&rebuilt, "audit_log", "params", "seq = 1").await,
        text
    );
    assert_eq!(
        stored(&rebuilt, "apps", "source", "id = 'app_1'").await,
        ("null".to_owned(), None)
    );
    rebuilt.pool().close().await;
}

/// The cascades the schema declares between tables the log carries, read
/// with `pragma_foreign_key_list`.
async fn schema_and_cascades() -> (Schema, Vec<Cascade>) {
    let store = primary().await;
    let mut conn = store.pool().acquire().await.unwrap();
    let schema = Schema::read(&mut conn).await.unwrap();
    let found = cascades(&mut conn, &schema).await.unwrap();
    (schema, found)
}

#[tokio::test]
async fn the_cascades_are_the_schemas_on_delete_cascade_foreign_keys() {
    let (_, found) = schema_and_cascades().await;
    let has = |child: &str, column: &str, parent: &str| {
        found
            .iter()
            .any(|c| c.child == child && c.columns == [column] && c.parent == parent)
    };
    for (child, column, parent) in [
        ("api_tokens", "user_id", "users"),
        ("apps", "project_id", "projects"),
        ("vms", "project_id", "projects"),
        ("app_env", "app_id", "apps"),
        ("deployments", "app_id", "apps"),
        ("domains", "app_id", "apps"),
        ("builds", "app_id", "apps"),
        ("agent_keys", "agent_id", "agents"),
        ("grants", "agent_id", "agents"),
        ("approvals", "agent_id", "agents"),
        ("node_credentials", "node_id", "nodes"),
        ("node_key_custody", "node_id", "nodes"),
        ("node_tunnels", "node_id", "nodes"),
    ] {
        assert!(
            has(child, column, parent),
            "{child}.{column} -> {parent}: {found:?}"
        );
    }
    // `vms.app_id` is ON DELETE SET NULL, and the LOCAL tables' cascades
    // (sessions, access tokens, webhook deliveries) are left out.
    assert!(!found.iter().any(|c| c.child == "vms" && c.parent == "apps"));
    assert!(
        !found
            .iter()
            .any(|c| ["sessions", "access_tokens", "webhook_deliveries"].contains(&c.child))
    );
    assert_eq!(found.len(), 13, "{found:?}");
}

/// An owned entry for [`cascade`].
struct Owned(u64, &'static str, &'static str, &'static str, Value);

fn run_cascade(
    schema: &Schema,
    found: &[Cascade],
    entries: &[Owned],
) -> Vec<(String, String, u64, u64)> {
    let payloads: Vec<String> = entries.iter().map(|e| e.4.to_string()).collect();
    let borrowed: Vec<Entry<'_>> = entries
        .iter()
        .zip(&payloads)
        .map(|(e, payload)| Entry {
            seq: e.0,
            entity_type: e.1,
            entity_id: e.2,
            op: e.3,
            payload,
        })
        .collect();
    cascade(schema, found, &borrowed)
        .into_iter()
        .map(|c| (c.table.to_owned(), c.entity_id, c.created_seq, c.with.seq))
        .collect()
}

/// Section 7.3 step 4 (ADR 0317): each delete removes, in `seq` order, the
/// rows that pointed at the deleted row at that time and were created
/// before it, and the rows those removals cascade to. A parent created again
/// under the same id brings back none of them, and a row created after the
/// new create, or pointed at the new parent later, stays.
#[tokio::test]
async fn the_cascades_follow_the_log_in_order() {
    let (schema, found) = schema_and_cascades().await;
    let removed = run_cascade(
        &schema,
        &found,
        &[
            Owned(1, "user", "usr_1", "create", user("usr_1", "a@example.com")),
            Owned(
                2,
                "api_token",
                "tok_1",
                "create",
                token("tok_1", "usr_1", "a"),
            ),
            Owned(3, "user", "usr_1", "delete", json!({})),
            Owned(4, "user", "usr_1", "create", user("usr_1", "a@example.com")),
            Owned(
                5,
                "api_token",
                "tok_2",
                "create",
                token("tok_2", "usr_1", "b"),
            ),
            // A chain: the project takes its app, which takes its variable
            // and its deployment.
            Owned(6, "project", "prj_1", "create", project("prj_1", "one")),
            Owned(7, "project", "prj_2", "create", project("prj_2", "two")),
            Owned(8, "app", "app_1", "create", app("app_1", "prj_1")),
            Owned(
                9,
                "app_env",
                "app_1/FOO",
                "create",
                env("app_1", "FOO", &[1]),
            ),
            Owned(
                10,
                "deployment",
                "dep_1",
                "create",
                deployment("dep_1", "app_1"),
            ),
            // Created in prj_1, moved to prj_2 before the delete: it stays.
            Owned(11, "app", "app_2", "create", app("app_2", "prj_1")),
            Owned(
                12,
                "app",
                "app_2",
                "update",
                json!({"id": "app_2", "project_id": "prj_2"}),
            ),
            // Created in prj_2, moved to the re-created prj_1 after: stays.
            Owned(13, "app", "app_3", "create", app("app_3", "prj_2")),
            Owned(14, "project", "prj_1", "delete", json!({})),
            Owned(15, "project", "prj_1", "create", project("prj_1", "one")),
            Owned(
                16,
                "app",
                "app_3",
                "update",
                json!({"id": "app_3", "project_id": "prj_1"}),
            ),
            Owned(17, "app", "app_1", "create", app("app_1", "prj_1")),
            Owned(
                18,
                "app_env",
                "app_1/BAR",
                "create",
                env("app_1", "BAR", &[2]),
            ),
            // The new app_1 goes with its own delete; app_3 goes too.
            Owned(19, "project", "prj_1", "delete", json!({})),
        ],
    );
    let row = |table: &str, id: &str, created: u64, with: u64| {
        (table.to_owned(), id.to_owned(), created, with)
    };
    // In the order of the deletes; each removed row's own children follow it.
    assert_eq!(
        removed,
        [
            row("api_tokens", "tok_1", 2, 3),
            row("apps", "app_1", 8, 14),
            row("app_env", "app_1/FOO", 9, 14),
            row("deployments", "dep_1", 10, 14),
            row("apps", "app_1", 17, 19),
            row("apps", "app_3", 13, 19),
            row("app_env", "app_1/BAR", 18, 19),
        ]
    );
}

/// The build applies the cascades: a token created before its user's delete
/// is gone, one created after the user was created again is kept, and the
/// rebuilt database passes its foreign-key check.
#[tokio::test]
async fn a_build_removes_the_rows_a_cascade_removed_and_keeps_the_re_created_parents() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    log(
        &primary,
        "api_token",
        "tok_1",
        Op::Create,
        token("tok_1", "usr_1", "a"),
    )
    .await;
    // An older writer: the token went with the user, with no entry of its own.
    log(
        &primary,
        "user",
        "usr_1",
        Op::Delete,
        json!({"id": "usr_1"}),
    )
    .await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    let kept = log(
        &primary,
        "api_token",
        "tok_2",
        Op::Create,
        token("tok_2", "usr_1", "b"),
    )
    .await;
    log(
        &primary,
        "project",
        "prj_1",
        Op::Create,
        project("prj_1", "one"),
    )
    .await;
    log(&primary, "app", "app_1", Op::Create, app("app_1", "prj_1")).await;
    log(
        &primary,
        "app_env",
        "app_1/FOO",
        Op::Create,
        env("app_1", "FOO", &[1]),
    )
    .await;
    log(&primary, "app", "app_1", Op::Delete, json!({})).await;
    log(&primary, "app", "app_1", Op::Create, app("app_1", "prj_1")).await;

    let dir = Dir::new();
    let built = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap();
    assert_eq!(
        built
            .cascaded
            .iter()
            .map(|c| (c.table, c.entity_id.as_str(), c.created_seq))
            .collect::<Vec<_>>(),
        [("api_tokens", "tok_1", 2), ("app_env", "app_1/FOO", 8)]
    );
    let rebuilt = open(&dir.target()).await;
    let tokens: Vec<(String,)> = sqlx::query_as("SELECT id FROM api_tokens ORDER BY id")
        .fetch_all(rebuilt.pool())
        .await
        .unwrap();
    assert_eq!(
        tokens,
        [("tok_2".to_owned(),)],
        "the token of seq {kept} only"
    );
    let (env_rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM app_env")
        .fetch_one(rebuilt.pool())
        .await
        .unwrap();
    assert_eq!(env_rows, 0);
    assert_eq!(built.rows["apps"], 1);
    rebuilt.pool().close().await;
}

/// A row that references a row the log never created stops the build with
/// the row's key, the `seq` of its create and the parent.
#[tokio::test]
async fn a_row_whose_parent_the_log_lacks_stops_the_build() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    let orphan = log(
        &primary,
        "api_token",
        "tok_9",
        Op::Create,
        token("tok_9", "usr_9", "z"),
    )
    .await;
    let dir = Dir::new();
    let error = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    let BuildError::ForeignKey {
        table,
        entity_id,
        seq,
        columns,
        parent,
    } = &error
    else {
        panic!("{error}");
    };
    assert_eq!(
        (
            table.as_str(),
            entity_id.as_deref(),
            *seq,
            columns.as_str(),
            parent.as_str()
        ),
        (
            "api_tokens",
            Some("tok_9"),
            Some(orphan),
            "user_id",
            "users"
        )
    );
    assert!(dir.files().is_empty(), "{:?}", dir.files());
}

/// A change-log entry whose content no longer hashes to its `hash`, or whose
/// `prev_hash` is not the hash before it, stops the build at its `seq`, and
/// nothing is left in the target's directory.
#[tokio::test]
async fn a_change_log_break_stops_the_build_and_leaves_no_file() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Update,
        json!({"id": "usr_1", "name": "Ada L"}),
    )
    .await;
    log(
        &primary,
        "user",
        "usr_2",
        Op::Create,
        user("usr_2", "b@example.com"),
    )
    .await;
    let entries = followed(&primary).await;
    let dir = Dir::new();
    build(&entries, CLUSTER, 2, &dir.target()).await.unwrap();
    std::fs::remove_file(dir.target()).unwrap();

    // The same content with another name: the chain links hold, the hash
    // does not.
    let mut edited = entries.clone();
    edited[1].payload = r#"{"id":"usr_1","name":"Eve"}"#.to_owned();
    let error = build(&edited, CLUSTER, 2, &dir.target()).await.unwrap_err();
    assert!(
        matches!(error, BuildError::ChangeLog { seq: 2, .. }),
        "{error}"
    );
    assert!(dir.files().is_empty(), "{:?}", dir.files());

    let mut relinked = entries.clone();
    relinked[2].prev_hash = "1".repeat(64);
    let error = build(&relinked, CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    assert!(
        matches!(error, BuildError::ChangeLog { seq: 3, .. }),
        "{error}"
    );
    assert!(dir.files().is_empty(), "{:?}", dir.files());

    // A log that does not start at seq 1 has lost its first entries.
    let error = build(&entries[2..], CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    assert!(
        matches!(error, BuildError::ChangeLog { seq: 3, .. }),
        "{error}"
    );
    assert!(dir.files().is_empty(), "{:?}", dir.files());
}

/// An audit record whose `hash` does not cover it stops the build at that
/// record (`audit-log-format.md` section 6), even when its change-log entry
/// hashes correctly, and nothing is left.
#[tokio::test]
async fn an_audit_chain_break_stops_the_build_and_leaves_no_file() {
    let source = primary().await;
    audit_record(&source, "test.one").await;
    audit_record(&source, "test.two").await;
    let records: Vec<String> = sqlx::query_scalar(
        "SELECT payload FROM change_log WHERE entity_type = 'audit' ORDER BY seq",
    )
    .fetch_all(source.pool())
    .await
    .unwrap();
    // The same two records logged again, the second with its operation
    // edited after it was hashed: a change log that verifies, carrying an
    // audit chain that does not.
    let primary = primary().await;
    for (index, text) in records.iter().enumerate() {
        let mut record: Value = serde_json::from_str(text).unwrap();
        if index == 1 {
            record["operation"] = json!("test.edited");
        }
        let seq = record["seq"].to_string();
        log(&primary, "audit", &seq, Op::Create, record).await;
    }
    let dir = Dir::new();
    let error = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            BuildError::AuditChain {
                seq: 2,
                reason: audit::HASH_MISMATCH
            }
        ),
        "{error}"
    );
    assert!(dir.files().is_empty(), "{:?}", dir.files());
}

/// A target path that exists refuses the build and is left as it was.
#[tokio::test]
async fn an_existing_target_refuses_the_build() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    let dir = Dir::new();
    std::fs::write(dir.target(), b"someone's database").unwrap();
    let error = build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, BuildError::Exists(path) if *path == dir.target()),
        "{error}"
    );
    assert_eq!(std::fs::read(dir.target()).unwrap(), b"someone's database");
    assert_eq!(dir.files(), ["quoro.db"]);

    // An empty log and a term outside 1 to 2^53 - 1 refuse as well.
    let other = dir.0.join("other.db");
    let entries = followed(&primary).await;
    assert!(matches!(
        build(&[], CLUSTER, 2, &other).await,
        Err(BuildError::EmptyLog)
    ));
    assert!(matches!(
        build(&entries, CLUSTER, 0, &other).await,
        Err(BuildError::BadTerm(0))
    ));
    assert!(matches!(
        build(&entries, CLUSTER, 1 << 53, &other).await,
        Err(BuildError::BadTerm(_))
    ));
    assert_eq!(dir.files(), ["quoro.db"]);
}

/// A `-wal` or `-shm` file at the target's path refuses the build before it
/// starts, and one that appears while it runs refuses the link: SQLite would
/// read it as part of the new database. Nothing is left but that file.
#[tokio::test]
async fn a_wal_or_shm_file_at_the_target_refuses_the_build() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    let entries = followed(&primary).await;
    let dir = Dir::new();
    let [wal, shm] = sidecars(&dir.target());
    for side in [&wal, &shm] {
        std::fs::write(side, b"someone's pages").unwrap();
        let error = build(&entries, CLUSTER, 2, &dir.target())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, BuildError::Exists(path) if path == side),
            "{error}"
        );
        let name = side.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(dir.files(), [name]);
        assert_eq!(std::fs::read(side).unwrap(), b"someone's pages");
        std::fs::remove_file(side).unwrap();
    }

    // Just before the link: the database is written under its temporary
    // name, then a side file appears at the target.
    for side in [&wal, &shm] {
        let temporary = temporary_path(&dir.target()).unwrap();
        create_new(&temporary).unwrap();
        write(&entries, CLUSTER, 2, &temporary).await.unwrap();
        std::fs::write(side, b"someone's pages").unwrap();
        let error = settle(&temporary, &dir.target()).unwrap_err();
        assert!(
            matches!(&error, BuildError::Exists(path) if path == side),
            "{error}"
        );
        assert!(!dir.target().exists());
        assert!(temporary.exists(), "the build removes its own file");
        remove_database(&temporary);
        std::fs::remove_file(side).unwrap();
        assert!(dir.files().is_empty(), "{:?}", dir.files());
    }
    build(&entries, CLUSTER, 2, &dir.target()).await.unwrap();
    assert_eq!(dir.files(), ["quoro.db"]);
}

/// The `audit_log` rows go in the order of their own `seq`, whatever the
/// order of their entries in the change log: here the second record's entry
/// comes before the first's, which the table's triggers would refuse in
/// change-log order.
#[tokio::test]
async fn audit_records_are_inserted_in_their_own_seq_order() {
    let source = primary().await;
    audit_record(&source, "test.one").await;
    audit_record(&source, "test.two").await;
    audit_record(&source, "test.three").await;
    let records: Vec<String> = sqlx::query_scalar(
        "SELECT payload FROM change_log WHERE entity_type = 'audit' ORDER BY seq",
    )
    .fetch_all(source.pool())
    .await
    .unwrap();
    assert_eq!(records.len(), 3);
    let primary = primary().await;
    for index in [1, 2, 0] {
        let record: Value = serde_json::from_str(&records[index]).unwrap();
        let seq = record["seq"].to_string();
        log(&primary, "audit", &seq, Op::Create, record).await;
    }
    let entries = followed(&primary).await;
    let order: Vec<&str> = entries.iter().map(|e| e.entity_id.as_str()).collect();
    assert_eq!(order, ["2", "3", "1"], "the log holds them out of order");

    let dir = Dir::new();
    let built = build(&entries, CLUSTER, 2, &dir.target())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(built.rows["audit_log"], 3);
    let rebuilt = open(&dir.target()).await;
    let stored: Vec<(i64, String)> =
        sqlx::query_as("SELECT seq, operation FROM audit_log ORDER BY seq")
            .fetch_all(rebuilt.pool())
            .await
            .unwrap();
    assert_eq!(
        stored,
        [
            (1, "test.one".to_owned()),
            (2, "test.two".to_owned()),
            (3, "test.three".to_owned()),
        ]
    );
    rebuilt.pool().close().await;
}

/// Each build first removes what a build of the same target left when it
/// did not finish: the regular files named `.<name>.rebuild-<16 hex
/// digits>`, alone or with `-wal`, `-shm` or `-journal`, owned by the user
/// it runs as. Every other name, a directory and a symbolic link stay.
#[tokio::test]
async fn a_build_removes_what_a_stopped_build_left() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    let dir = Dir::new();
    let left = ".quoro.db.rebuild-0123456789abcdef";
    for suffix in ["", "-wal", "-shm", "-journal"] {
        std::fs::write(dir.0.join(format!("{left}{suffix}")), b"stale").unwrap();
    }
    let mut kept = vec![
        ".quoro.db.rebuild-0123456789ABCDEF".to_owned(),
        ".quoro.db.rebuild-0123456789abcde".to_owned(),
        ".quoro.db.rebuild-0123456789abcdef-old".to_owned(),
        ".other.db.rebuild-0123456789abcdef".to_owned(),
        "quoro.db.rebuild-0123456789abcdef".to_owned(),
        "notes.txt".to_owned(),
    ];
    for name in &kept {
        std::fs::write(dir.0.join(name), b"not a build's").unwrap();
    }
    let directory = ".quoro.db.rebuild-fedcba9876543210";
    std::fs::create_dir(dir.0.join(directory)).unwrap();
    kept.push(directory.to_owned());
    #[cfg(unix)]
    {
        let link = ".quoro.db.rebuild-aaaaaaaaaaaaaaaa";
        std::os::unix::fs::symlink(dir.0.join("notes.txt"), dir.0.join(link)).unwrap();
        kept.push(link.to_owned());
    }

    build(&followed(&primary).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap();
    kept.push("quoro.db".to_owned());
    kept.sort();
    assert_eq!(dir.files(), kept);
    assert_eq!(
        std::fs::read(dir.0.join("notes.txt")).unwrap(),
        b"not a build's"
    );
}

/// The followed entries' chain is checked before the fold reads them: an
/// entry edited into a payload the fold refuses, or into a value its column
/// cannot hold, stops the build with the change-log break at its `seq`,
/// not with a fold or column error.
#[tokio::test]
async fn a_tampered_entry_stops_at_the_chain_before_the_fold() {
    let primary = primary().await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Create,
        user("usr_1", "a@example.com"),
    )
    .await;
    log(
        &primary,
        "user",
        "usr_1",
        Op::Update,
        json!({"id": "usr_1", "disabled": 0}),
    )
    .await;
    let entries = followed(&primary).await;
    let dir = Dir::new();
    for payload in ["[]", r#"{"disabled":"no","id":"usr_1"}"#] {
        let mut edited = entries.clone();
        edited[1].payload = payload.to_owned();
        let error = build(&edited, CLUSTER, 2, &dir.target()).await.unwrap_err();
        assert!(
            matches!(&error, BuildError::ChangeLog { seq: 2, .. }),
            "{payload}: {error}"
        );
        assert!(dir.files().is_empty(), "{:?}", dir.files());
    }
}

/// The chain checks of a build read the new file a page at a time and find
/// the same break whatever the page size: none on the file as built, the
/// first edited record or entry once one is edited.
#[tokio::test]
async fn the_chain_checks_find_the_same_break_page_by_page() {
    let source = primary().await;
    for operation in ["test.one", "test.two", "test.three", "test.four"] {
        audit_record(&source, operation).await;
    }
    let dir = Dir::new();
    build(&followed(&source).await, CLUSTER, 2, &dir.target())
        .await
        .unwrap();
    let rebuilt = open(&dir.target()).await;
    let mut conn = rebuilt.pool().acquire().await.unwrap();
    for page in [1, 2, 3, PAGE] {
        verify_chains(&mut conn, page)
            .await
            .unwrap_or_else(|e| panic!("page {page}: {e}"));
    }

    sqlx::query("DROP TRIGGER audit_log_append_only_update")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("UPDATE audit_log SET operation = 'test.edited' WHERE seq = 3")
        .execute(&mut *conn)
        .await
        .unwrap();
    for page in [1, 2, 3, PAGE] {
        let error = verify_chains(&mut conn, page).await.unwrap_err();
        assert!(
            matches!(
                error,
                BuildError::AuditChain {
                    seq: 3,
                    reason: audit::HASH_MISMATCH
                }
            ),
            "page {page}: {error}"
        );
    }

    sqlx::query("DROP TRIGGER change_log_append_only_update")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("UPDATE change_log SET actor_id = 'edited' WHERE seq = 2")
        .execute(&mut *conn)
        .await
        .unwrap();
    for page in [1, 2, 3, PAGE] {
        let error = verify_chains(&mut conn, page).await.unwrap_err();
        assert!(
            matches!(error, BuildError::ChangeLog { seq: 2, .. }),
            "page {page}: {error}"
        );
    }
    drop(conn);
    rebuilt.pool().close().await;
}

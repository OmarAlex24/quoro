//! `quoro audit verify --db` (`audit-log-format.md` section 6.1): it verifies
//! a database copy with no server, and names the edited record.

use quoro_control_plane::audit::{self, Channel, Outcome, Record};
use quoro_control_plane::changelog::{Actor, ActorType};
use quoro_control_plane::store::{Store, random_secret};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn record() -> Record {
    Record {
        actor: Actor {
            kind: ActorType::User,
            id: "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into(),
        },
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::Panel,
        operation: "POST /api/v1/projects".into(),
        scope: None,
        target_type: Some("project".into()),
        target_id: Some("prj_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into()),
        project_id: None,
        params: json!({ "slug": "web", "name": "Web" }),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: "req_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into(),
        result: Outcome::Ok,
        result_detail: None,
        change_seq: None,
    }
}

#[tokio::test]
async fn db_verify_accepts_a_copy_and_names_the_tampered_seq() {
    let dir = std::env::temp_dir().join(format!("quoro-audit-cli-{}", random_secret()));
    std::fs::create_dir_all(&dir).unwrap();
    let database = dir.join("quoro.db");

    let store = Store::open(&database).await.unwrap();
    let mut w = store.begin_write().await.unwrap();
    for _ in 0..4 {
        audit::append(&mut w.tx, &record()).await.unwrap();
    }
    w.commit().await.unwrap();
    sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        .execute(store.pool())
        .await
        .unwrap();
    drop(store);

    let copy = dir.join("copy.db");
    std::fs::copy(&database, &copy).unwrap();

    // The copy verifies as it stands.
    let clean = std::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args(["audit", "verify", "--db"])
        .arg(&copy)
        .output()
        .unwrap();
    assert!(clean.status.success(), "{clean:?}");
    let stdout = String::from_utf8_lossy(&clean.stdout);
    assert!(
        stdout.starts_with("ok: checked 4 records, head "),
        "{stdout}"
    );

    // Edit one row in the copy, dropping the append-only trigger first.
    let options = SqliteConnectOptions::new().filename(&copy);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER audit_log_append_only_update")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE audit_log SET justification = 'edited after the fact' WHERE seq = 3")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let broken = std::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args(["audit", "verify", "--db"])
        .arg(&copy)
        .output()
        .unwrap();
    assert_eq!(broken.status.code(), Some(1), "{broken:?}");
    let stdout = String::from_utf8_lossy(&broken.stdout);
    assert!(
        stdout.contains("broken at seq 3: hash_mismatch"),
        "{stdout}"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn verify_accepts_records_that_keep_a_digest() {
    // Section 4.1: parameters whose change-log entry a node could not
    // receive are kept as a digest. Such records verify offline like any
    // other, from a database copy and from an exported segment.
    let dir = std::env::temp_dir().join(format!("quoro-audit-digest-{}", random_secret()));
    std::fs::create_dir_all(&dir).unwrap();
    let database = dir.join("quoro.db");

    let store = Store::open(&database).await.unwrap();
    let mut w = store.begin_write().await.unwrap();
    for params in [
        json!({ "slug": "web", "name": "Web" }),
        serde_json::from_str(r#"{"n": 1e16}"#).unwrap(),
        serde_json::from_str(r#"{"n": 18446744073709551615}"#).unwrap(),
        serde_json::from_str(r#"{"x": 18983552425072.563}"#).unwrap(),
        json!({ "blob": "x".repeat(1_048_000) }),
    ] {
        let mut with_params = record();
        with_params.params = params;
        audit::append(&mut w.tx, &with_params).await.unwrap();
    }
    w.commit().await.unwrap();
    let digests: (i64,) =
        sqlx::query_as("SELECT count(*) FROM audit_log WHERE params_sha256 IS NOT NULL")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(digests.0, 4);
    let rows = sqlx::query(&format!(
        "SELECT {} FROM audit_log ORDER BY seq",
        audit::COLUMNS
    ))
    .fetch_all(store.pool())
    .await
    .unwrap();
    let segment: String = rows
        .iter()
        .map(|row| format!("{}\n", audit::record_value_from_row(row).unwrap()))
        .collect();
    assert!(
        segment.contains("\"params_sha256\":"),
        "{}",
        &segment[..400]
    );
    let file = dir.join("segment.jsonl");
    std::fs::write(&file, segment).unwrap();
    sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        .execute(store.pool())
        .await
        .unwrap();
    drop(store);

    for source in ["--db", "--file"] {
        let path = if source == "--db" { &database } else { &file };
        let verified = std::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
            .args(["audit", "verify", source])
            .arg(path)
            .output()
            .unwrap();
        assert!(verified.status.success(), "{source}: {verified:?}");
        let stdout = String::from_utf8_lossy(&verified.stdout);
        assert!(
            stdout.starts_with("ok: checked 5 records, head "),
            "{source}: {stdout}"
        );
    }

    std::fs::remove_dir_all(&dir).unwrap();
}

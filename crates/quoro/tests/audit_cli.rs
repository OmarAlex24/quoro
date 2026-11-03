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

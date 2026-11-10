//! The off-site audit copy against an in-memory object store
//! (`audit-log-format.md` sections 8.1 to 8.3). No network is involved: every
//! exporter, probe and verifier function is generic over `ObjectStore`.

mod common;

use common::Server;
use common::replay;
use quoro_control_plane::api::audit_exports::{
    AuditExport, CreateAuditExportRequest, LockStrength,
};
use quoro_control_plane::audit::{self, Channel, Outcome, Record};
use quoro_control_plane::audit_export::{self, exporter, probe, verify};
use quoro_control_plane::auth::{Credential, Identity, Principal, UserIdentity};
use quoro_control_plane::changelog::{Actor, ActorType};
use quoro_control_plane::error::Code;
use quoro_control_plane::request::CallCtx;
use quoro_providers::object_store::{MemoryStore, ObjectStore, ObjectStoreError};
use quoro_types::{Prefix, Role, new_id};
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};

const ROOT_PREFIX: &str = "quoro-audit";

fn owner_ctx() -> CallCtx {
    let identity = Identity {
        principal: Principal::User(UserIdentity {
            user_id: "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D".to_string(),
            email: "owner@example.com".to_string(),
            name: "Owner".to_string(),
            role: Role::Owner,
        }),
        credential: Credential::Session {
            session_id: "ses_test".to_string(),
        },
    };
    CallCtx::new(
        identity,
        new_id(Prefix::Request),
        audit_export::CREATE_OPERATION,
        &axum::http::HeaderMap::new(),
    )
}

fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

async fn first_user(server: &Server) -> String {
    let (id,): (String,) = sqlx::query_as("SELECT id FROM users LIMIT 1")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    id
}

const CONFIG: &str =
    r#"{"endpoint":"https://s3.example.com","bucket":"audit","access_key_id":"AKIAEXAMPLE"}"#;

async fn insert_provider(server: &Server, kind: &str, name: &str) -> String {
    let user = first_user(server).await;
    let id = new_id(Prefix::Provider);
    sqlx::query(
        "INSERT INTO providers (id, kind, name, config_json, secret_enc, created_by, created_at, rotated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
    )
    .bind(&id)
    .bind(kind)
    .bind(name)
    .bind(CONFIG)
    .bind(vec![1u8, 2, 3])
    .bind(&user)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    id
}

async fn insert_backup_repository(server: &Server, provider_id: &str) {
    let user = first_user(server).await;
    sqlx::query(
        "INSERT INTO backup_repositories (id, name, provider_id, prefix, repo_key_enc, \
         recovery_key_fingerprint, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("brp_test")
    .bind("Test repository")
    .bind(provider_id)
    .bind("backups")
    .bind(vec![0u8])
    .bind("0123456789abcdef")
    .bind(&user)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

fn body(provider_id: &str) -> CreateAuditExportRequest {
    CreateAuditExportRequest {
        provider_id: provider_id.to_string(),
        prefix: ROOT_PREFIX.to_string(),
    }
}

fn root(server: &Server) -> String {
    format!("{ROOT_PREFIX}/{}/audit/", server.state.cluster_id)
}

async fn local_last(server: &Server) -> i64 {
    let (last,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM audit_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    last
}

async fn append_records(server: &Server, count: usize) {
    let mut w = server.state.store.begin_write().await.unwrap();
    for index in 0..count {
        let record = Record {
            actor: Actor {
                kind: ActorType::System,
                id: "test".to_string(),
            },
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::System,
            operation: format!("test.record.{index}"),
            scope: None,
            target_type: None,
            target_id: None,
            project_id: None,
            params: json!({}),
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: new_id(Prefix::Request),
            result: Outcome::Ok,
            result_detail: None,
            change_seq: None,
        };
        audit::append(&mut w.tx, &record).await.unwrap();
    }
    w.commit().await.unwrap();
}

/// The canonical JSON Lines the exporter must write for `first..=last`.
async fn expected_segment(server: &Server, first: i64, last: i64) -> Vec<u8> {
    let rows = sqlx::query(&format!(
        "SELECT {} FROM audit_log WHERE seq BETWEEN ? AND ? ORDER BY seq",
        audit::COLUMNS
    ))
    .bind(first)
    .bind(last)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    let mut out = Vec::new();
    for row in &rows {
        let value = audit::record_value_from_row(row).unwrap();
        out.extend_from_slice(quoro_proto::jcs::canonicalize(&value).unwrap().as_bytes());
        out.push(b'\n');
    }
    out
}

/// A server with an owner, one provider and an export probed by `store`.
async fn setup<S: ObjectStore>(store: &S) -> (Server, AuditExport) {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let export =
        audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), store, now())
            .await
            .unwrap();
    (server, export)
}

async fn chain_broken(server: &Server) -> Value {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT data FROM events WHERE type = 'audit.chain_broken' ORDER BY seq DESC",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    let (data,) = rows.first().expect("an audit.chain_broken event");
    serde_json::from_str(data).unwrap()
}

#[tokio::test]
async fn create_probes_and_refuses_a_second_or_a_wrong_provider() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    let ctx = owner_ctx();

    let export = audit_export::create_with(&server.state, &ctx, &body(&provider), &store, now())
        .await
        .unwrap();
    assert_eq!(export.lock_strength, LockStrength::None);
    assert_eq!(export.last_exported_seq, 0);
    assert_eq!(export.prefix, ROOT_PREFIX);
    assert_eq!(export.provider_id, provider);
    let audit = server.audit().await;
    assert!(
        audit
            .iter()
            .any(|(operation, result, _)| operation == "audit_export.probe" && result == "ok"),
        "{audit:?}"
    );
    assert!(
        audit.iter().any(|(operation, result, _)| {
            operation == audit_export::CREATE_OPERATION && result == "ok"
        }),
        "{audit:?}"
    );

    let error = audit_export::create_with(&server.state, &ctx, &body(&provider), &store, now())
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Conflict);

    // A fresh cluster, because the 409 above would win over the provider check.
    let server = common::start().await;
    let _owner = server.owner().await;
    let dns = insert_provider(&server, "dns", "DNS").await;
    let error = audit_export::create_with(
        &server.state,
        &owner_ctx(),
        &body(&dns),
        &MemoryStore::new(),
        now(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, Code::ValidationFailed);
}

/// Section 7.3 step 4 (ADR 0297): the export's change-log entries, its
/// create and the probe's update, rebuild its row when replayed in `seq`
/// order into an empty database, `target` included. The provider row is
/// inserted directly here, so only `audit_export` entries are replayed.
#[tokio::test]
async fn export_entries_rebuild_the_row() {
    let (server, _export) = setup(&MemoryStore::new()).await;
    let rebuilt = replay::rebuild(&server, &["audit_export"]).await;
    replay::assert_rebuilt(
        &server,
        &rebuilt,
        "audit_exports",
        &[
            "id",
            "target",
            "provider_id",
            "lock_strength",
            "probed_at",
            "probe_detail",
            "last_exported_seq",
            "last_exported_hash",
            "last_error",
            "updated_at",
        ],
        1,
    )
    .await;
}

#[tokio::test]
async fn create_refuses_the_backup_repositorys_provider() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Backups").await;
    insert_backup_repository(&server, &provider).await;
    let error = audit_export::create_with(
        &server.state,
        &owner_ctx(),
        &body(&provider),
        &MemoryStore::new(),
        now(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, Code::ValidationFailed);
}

#[tokio::test]
async fn the_probe_reports_the_emulated_strength() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::bucket_rule(root(&server));
    let export =
        audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
            .await
            .unwrap();
    assert_eq!(export.lock_strength, LockStrength::BucketRule);
    assert!(
        export
            .probe_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("overwrite 409"))
    );

    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::compliance();
    let export =
        audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
            .await
            .unwrap();
    assert_eq!(export.lock_strength, LockStrength::Compliance);
}

#[tokio::test]
async fn export_writes_exact_segments_and_heads_and_waits_for_more() {
    let store = MemoryStore::new();
    let (server, _export) = setup(&store).await;
    // The create and its probe wrote two records; add enough to cross 1,000.
    let total = local_last(&server).await;
    append_records(&server, (1_001 - total) as usize).await;

    exporter::step(&server.state, &store, now()).await.unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.last_exported_seq, 1_000);
    let segment = audit_export::segment_key(&root(&server), 1, 1_000);
    let head = audit_export::head_key(&root(&server), 1_000);
    let keys: Vec<String> = store
        .list(&root(&server))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    assert!(keys.contains(&segment), "{keys:?}");
    assert!(keys.contains(&head), "{keys:?}");
    assert_eq!(
        store.get(&segment).await.unwrap(),
        expected_segment(&server, 1, 1_000).await
    );
    let head_body: Value = serde_json::from_slice(&store.get(&head).await.unwrap()).unwrap();
    assert_eq!(head_body["last_seq"], json!(1_000));
    let (hash,): (String,) = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = 1000")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(head_body["head_hash"], json!(hash));
    assert_eq!(row.last_exported_hash.as_deref(), Some(hash.as_str()));

    // One record is left and is younger than five minutes: nothing new.
    exporter::step(&server.state, &store, now()).await.unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.last_exported_seq, 1_000);
    assert_eq!(store.list(&root(&server)).await.unwrap().len(), 2);

    // The injected clock passes the five-minute rule: a second segment.
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.last_exported_seq, 1_001);
    let keys: Vec<String> = store
        .list(&root(&server))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    assert!(keys.contains(&audit_export::segment_key(&root(&server), 1_001, 1_001)));
    assert!(keys.contains(&audit_export::head_key(&root(&server), 1_001)));
}

#[tokio::test]
async fn a_bucket_rule_refuses_the_exported_objects() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::bucket_rule(root(&server));
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        LockStrength::from(row.lock_strength),
        LockStrength::BucketRule
    );
    let segment = audit_export::segment_key(&root(&server), 1, row.last_exported_seq);
    assert!(matches!(
        store.put_overwrite(&segment, b"rewritten".to_vec()).await,
        Err(ObjectStoreError::Locked(_))
    ));
    assert!(matches!(
        store.delete(&segment).await,
        Err(ObjectStoreError::Locked(_))
    ));
}

#[tokio::test]
async fn a_compliance_export_keeps_the_original_version() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::compliance();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        LockStrength::from(row.lock_strength),
        LockStrength::Compliance
    );
    let segment = audit_export::segment_key(&root(&server), 1, row.last_exported_seq);
    let original = store.get(&segment).await.unwrap();
    let version = store.head(&segment).await.unwrap().version_id.unwrap();
    assert_eq!(
        store.head(&segment).await.unwrap().lock_mode.as_deref(),
        Some("COMPLIANCE")
    );
    store
        .put_overwrite(&segment, b"rewritten".to_vec())
        .await
        .unwrap();
    store.delete(&segment).await.unwrap();
    assert_eq!(
        store.get_version(&segment, &version).await.unwrap(),
        original
    );
}

#[tokio::test]
async fn crash_recovery_adopts_an_acknowledged_segment() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    append_records(&server, 3).await;
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let last = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    let segment = audit_export::segment_key(&root(&server), 1, last.last_exported_seq);
    let bytes = store.get(&segment).await.unwrap();

    // The process wrote the objects and died before the bookkeeping.
    sqlx::query(
        "UPDATE audit_exports SET last_exported_seq = 0, last_exported_hash = NULL, \
         last_error = NULL",
    )
    .execute(server.state.store.pool())
    .await
    .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(12))
        .await
        .unwrap();
    let adopted = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(adopted.last_exported_seq, last.last_exported_seq);
    assert_eq!(adopted.last_exported_hash, last.last_exported_hash);
    assert_eq!(store.get(&segment).await.unwrap(), bytes);
    assert_eq!(store.list(&root(&server)).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_corrupted_segment_stops_the_export() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    // The create and probe wrote the first records; a previous run's segment
    // for that whole range is corrupt.
    let last = local_last(&server).await;
    store
        .put_new(
            &audit_export::segment_key(&root(&server), 1, last),
            b"garbage\n".to_vec(),
            None,
        )
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.last_exported_seq, 0);
    assert!(row.last_error.is_some(), "{row:?}");
    let keys: Vec<String> = store
        .list(&root(&server))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    assert_eq!(keys.len(), 1, "{keys:?}");
}

#[tokio::test]
async fn a_truncated_local_tail_is_reported_against_the_copy() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    append_records(&server, 1).await;
    exporter::step(&server.state, &store, now() + Duration::minutes(12))
        .await
        .unwrap();
    let last = local_last(&server).await;
    let (expected,): (String,) = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
        .bind(last)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();

    // Simulate truncation of the tail by removing the last record.
    sqlx::query("DROP TRIGGER audit_log_append_only_delete")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM audit_log WHERE seq = ?")
        .bind(last)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let ok = verify::verify_copy(&server.state, &store, now() + Duration::minutes(13))
        .await
        .unwrap();
    assert!(!ok);
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.last_error.as_deref(),
        Some(format!("tail_truncated at {last}").as_str())
    );
    let event = chain_broken(&server).await;
    assert_eq!(event["reason"], json!("tail_truncated"));
    assert_eq!(event["broken_at_seq"], json!(last));
    assert_eq!(event["expected_hash"], json!(expected));
    assert_eq!(
        event["found_hash"],
        json!("0000000000000000000000000000000000000000000000000000000000000000")
    );
    let audit = server.audit().await;
    assert!(
        audit.iter().any(|(operation, result, detail)| {
            operation == "POST /api/v1/audit/verify"
                && result == "error"
                && detail.as_deref() == Some(format!("tail_truncated at {last}").as_str())
        }),
        "{audit:?}"
    );
}

#[tokio::test]
async fn a_tampered_local_hash_is_a_copy_mismatch() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    // Two segments, so a head exists at the first segment's end and at the
    // newest seq.
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let boundary = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap()
        .last_exported_seq;
    append_records(&server, 1).await;
    exporter::step(&server.state, &store, now() + Duration::minutes(12))
        .await
        .unwrap();

    let (expected,): (String,) = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
        .bind(boundary)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let tampered = "ab".repeat(32);
    sqlx::query("DROP TRIGGER audit_log_append_only_update")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE audit_log SET hash = ? WHERE seq = ?")
        .bind(&tampered)
        .bind(boundary)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let ok = verify::verify_copy(&server.state, &store, now() + Duration::minutes(13))
        .await
        .unwrap();
    assert!(!ok);
    let event = chain_broken(&server).await;
    assert_eq!(event["reason"], json!("copy_mismatch"));
    assert_eq!(event["broken_at_seq"], json!(boundary));
    assert_eq!(event["expected_hash"], json!(expected));
    assert_eq!(event["found_hash"], json!(tampered));
}

#[tokio::test]
async fn a_missing_newest_head_is_evidence_when_the_object_is_gone() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    append_records(&server, 1).await;
    exporter::step(&server.state, &store, now() + Duration::minutes(12))
        .await
        .unwrap();
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    store
        .delete(&audit_export::head_key(
            &root(&server),
            row.last_exported_seq,
        ))
        .await
        .unwrap();

    let ok = verify::verify_copy(&server.state, &store, now() + Duration::minutes(13))
        .await
        .unwrap();
    assert!(!ok);
    let event = chain_broken(&server).await;
    assert_eq!(event["reason"], json!("copy_mismatch"));
    assert_eq!(event["broken_at_seq"], json!(row.last_exported_seq));
    assert_eq!(event["expected_hash"], json!(row.last_exported_hash));
    assert_eq!(
        event["found_hash"],
        json!("0000000000000000000000000000000000000000000000000000000000000000")
    );
}

#[tokio::test]
async fn a_truncated_tail_is_evidence_even_without_its_segment() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let boundary = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap()
        .last_exported_seq;
    append_records(&server, 1).await;
    exporter::step(&server.state, &store, now() + Duration::minutes(12))
        .await
        .unwrap();
    let last = local_last(&server).await;
    let (expected,): (String,) = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
        .bind(last)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    // The newest head alone still proves the truncation.
    store
        .delete(&audit_export::segment_key(
            &root(&server),
            boundary + 1,
            last,
        ))
        .await
        .unwrap();

    sqlx::query("DROP TRIGGER audit_log_append_only_delete")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM audit_log WHERE seq = ?")
        .bind(last)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let ok = verify::verify_copy(&server.state, &store, now() + Duration::minutes(13))
        .await
        .unwrap();
    assert!(!ok);
    let event = chain_broken(&server).await;
    assert_eq!(event["reason"], json!("tail_truncated"));
    assert_eq!(event["broken_at_seq"], json!(last));
    assert_eq!(event["expected_hash"], json!(expected));
    assert_eq!(
        event["found_hash"],
        json!("0000000000000000000000000000000000000000000000000000000000000000")
    );
}

#[tokio::test]
async fn a_hole_in_the_local_log_is_a_copy_mismatch() {
    let server = common::start().await;
    let _owner = server.owner().await;
    let provider = insert_provider(&server, "object_store", "Audit copy").await;
    let store = MemoryStore::new();
    audit_export::create_with(&server.state, &owner_ctx(), &body(&provider), &store, now())
        .await
        .unwrap();
    exporter::step(&server.state, &store, now() + Duration::minutes(6))
        .await
        .unwrap();
    let boundary = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap()
        .last_exported_seq;
    append_records(&server, 1).await;
    exporter::step(&server.state, &store, now() + Duration::minutes(12))
        .await
        .unwrap();
    let (expected,): (String,) = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = ?")
        .bind(boundary)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();

    // A record in the middle disappears; the log past it still exists.
    sqlx::query("DROP TRIGGER audit_log_append_only_delete")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM audit_log WHERE seq = ?")
        .bind(boundary)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let ok = verify::verify_copy(&server.state, &store, now() + Duration::minutes(13))
        .await
        .unwrap();
    assert!(!ok);
    let event = chain_broken(&server).await;
    assert_eq!(event["reason"], json!("copy_mismatch"));
    assert_eq!(event["broken_at_seq"], json!(boundary));
    assert_eq!(event["expected_hash"], json!(expected));
    assert_eq!(
        event["found_hash"],
        json!("0000000000000000000000000000000000000000000000000000000000000000")
    );
}

#[tokio::test]
async fn the_probe_reruns_and_is_recorded() {
    let store = MemoryStore::new();
    let (server, export) = setup(&store).await;
    let row = audit_export::load(&server.state.store)
        .await
        .unwrap()
        .unwrap();
    let probed = probe::run_with(&server.state, &row, &store, now())
        .await
        .unwrap();
    assert_eq!(probed.id, export.id);
    let audit = server.audit().await;
    assert_eq!(
        audit
            .iter()
            .filter(|(operation, _, _)| operation == "audit_export.probe")
            .count(),
        2
    );
}

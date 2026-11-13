//! The restored primary's first start (`backup-format.md` section 8.2, "The
//! first write" and "Detecting the first start"; ADR 0280, ADR 0285; issue
//! #838): the `restore-pending` marker that `quoro restore` leaves, and the
//! one write the primary makes from it before it serves. That write marks
//! every `expiring` lease row unsettled (node-agent-protocol.md section 8.4,
//! "The mark") and, when `primary-node` asks for it, moves the primary status
//! and records `cluster.primary_moved`.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::start_in;
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::primary_status;
use quoro_control_plane::restore_pending::{self, Applied, Head, Marker};
use quoro_control_plane::store::{Store, now_rfc3339, random_secret};
use quoro_control_plane::{AppState, NodeNetwork, ReferenceAsDigest, ServerConfig};
use serde_json::{Value, json};

/// The node on the new host, which `primary-node` names.
const NEW_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0B";
/// The node that holds the leases.
const HOLDER: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0C";

/// An `expiring` row: marked by the first write.
const EXPIRING: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0A";
/// An `expiring` row that is revoked: every `expiring` row is marked.
const EXPIRING_REVOKED: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0B";
/// A `local` row: never touched.
const LOCAL: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0C";
/// An `expiring` row that already carries the mark: kept, with no entry.
const MARKED: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0D";

/// `(id, mode, token, revoked, unsettled)` of the restored lease rows.
const LEASES: [(&str, &str, i64, bool, bool); 4] = [
    (EXPIRING, "expiring", 3, false, false),
    (EXPIRING_REVOKED, "expiring", 4, true, false),
    (LOCAL, "local", 5, false, false),
    (MARKED, "expiring", 6, false, true),
];

fn local_node() -> LocalNode {
    LocalNode {
        name: "old-host".to_string(),
        capacity: Capacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
        },
        kvm: Kvm {
            available: true,
            kernel: "6.8.0".to_string(),
            nested: Some(true),
        },
        engine_version: "test-engine".to_string(),
        agent_version: "test-agent".to_string(),
    }
}

/// A data directory with the database `quoro restore` installed: the old
/// host's primary node, the new host's node, the leases' holder and the rows
/// of [`LEASES`]. Returns the directory, the open store, the old primary's id
/// and the log's head, which the restore's marker names.
async fn restored() -> (PathBuf, Store, String, Head) {
    let dir = std::env::temp_dir().join(format!("quoro-test-restored-{}", &random_secret()[..16]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    let old = driver::register_local_node(&store, &local_node())
        .await
        .unwrap();
    for id in [NEW_NODE, HOLDER] {
        insert_node(&store, id).await;
    }
    let now = now_rfc3339();
    // Each row with the entry of the write that gave it its token, as a
    // restored log holds it.
    let mut w = store.begin_write().await.unwrap();
    for (n, (id, mode, token, revoked, unsettled)) in LEASES.into_iter().enumerate() {
        let app_id = format!("app_01JC9Z1A2B3C4D5E6F7G8H9J0{n}");
        let duration_ms = (mode == "expiring").then_some(15_000_i64);
        let revoked_at = if revoked { Some(now.clone()) } else { None };
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, duration_ms, granted_at, revoked_at, unsettled) \
             VALUES (?, 'app', ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(&app_id)
        .bind(HOLDER)
        .bind(mode)
        .bind(token)
        .bind(duration_ms)
        .bind(&now)
        .bind(&revoked_at)
        .bind(i64::from(unsettled))
        .execute(&mut *w.tx)
        .await
        .unwrap();
        let payload = json!({
            "id": id,
            "resource_type": "app",
            "resource_id": app_id,
            "holder_node_id": HOLDER,
            "mode": mode,
            "fencing_token": token,
            "duration_ms": duration_ms,
            "granted_at": now,
            "renewed_at": null,
            "expires_at": null,
            "revoked_at": revoked_at,
            "unsettled": unsettled,
        });
        changelog::append(
            &mut w.tx,
            &Actor::system("test"),
            "lease",
            id,
            Op::Create,
            payload,
        )
        .await
        .unwrap();
    }
    w.commit().await.unwrap();
    let head = log_head_of(&store).await;
    (dir, store, old, head)
}

async fn insert_node(store: &Store, id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
         labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
         log_seq, last_seen_at, created_at) \
         VALUES (?, ?, '[\"node\"]', NULL, 'disconnected', '', NULL, 1, '{}', '{}', '{}', '{}', \
         NULL, NULL, 0, 0, 0, ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(&now)
    .bind(&now)
    .execute(store.pool())
    .await
    .unwrap();
}

async fn log_head_of(store: &Store) -> Head {
    let mut conn = store.pool().acquire().await.unwrap();
    restore_pending::log_head(&mut conn).await.unwrap()
}

fn write_marker(dir: &Path, marker: &Marker) {
    std::fs::write(dir.join(restore_pending::MARKER_FILE), marker.to_bytes()).unwrap();
}

fn write_primary_node(dir: &Path) {
    std::fs::write(
        dir.join(primary_status::MARKER_FILE),
        format!("{NEW_NODE}\n"),
    )
    .unwrap();
}

/// The `unsettled` of every lease row, by id.
async fn marks(store: &Store) -> Vec<(String, i64)> {
    sqlx::query_as("SELECT id, unsettled FROM leases ORDER BY id")
        .fetch_all(store.pool())
        .await
        .unwrap()
}

fn unmarked() -> Vec<(String, i64)> {
    LEASES
        .iter()
        .map(|(id, _, _, _, unsettled)| ((*id).to_string(), i64::from(*unsettled)))
        .collect()
}

fn all_expiring_marked() -> Vec<(String, i64)> {
    LEASES
        .iter()
        .map(|(id, mode, _, _, _)| ((*id).to_string(), i64::from(*mode == "expiring")))
        .collect()
}

/// Every change-log entry after `seq` as (seq, entity_type, entity_id,
/// actor_type, actor_id, payload).
async fn entries_after(
    store: &Store,
    seq: i64,
) -> Vec<(i64, String, String, String, String, Value)> {
    let rows: Vec<(i64, String, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, entity_type, entity_id, actor_type, actor_id, payload FROM change_log \
         WHERE seq > ? ORDER BY seq",
    )
    .bind(seq)
    .fetch_all(store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, kind, id, actor_type, actor_id, payload)| {
            (
                seq,
                kind,
                id,
                actor_type,
                actor_id,
                serde_json::from_str(&payload).unwrap(),
            )
        })
        .collect()
}

async fn cp_status(store: &Store, id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT cp_status FROM nodes WHERE id = ?")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap()
}

/// The `change_seq` of every `cluster.primary_moved` record.
async fn moved_records(store: &Store) -> Vec<Option<i64>> {
    sqlx::query_scalar(
        "SELECT change_seq FROM audit_log WHERE operation = 'cluster.primary_moved' ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap()
}

async fn restore_marker(store: &Store) -> Option<String> {
    sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
        .bind(restore_pending::META_KEY)
        .fetch_optional(store.pool())
        .await
        .unwrap()
}

fn meta_value(head: &Head) -> String {
    format!(
        "{{\"head_hash\":\"{}\",\"head_seq\":{}}}",
        head.hash, head.seq
    )
}

fn exists(dir: &Path, name: &str) -> bool {
    dir.join(name).exists()
}

/// Opens the control plane on `dir` as `quoro serve` does, keeping the error
/// of a start that stops.
async fn open_state(dir: &Path) -> Result<AppState, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    AppState::open(
        store,
        ServerConfig {
            data_dir: dir.to_path_buf(),
            secure_cookies: false,
            public_url: Some("https://primary.example:8443".to_string()),
            tls_fingerprint: common::TLS_FINGERPRINT.to_string(),
            tls: common::tls_identity(dir),
            node_network: NodeNetwork {
                subnet: "10.42.1.0/24".to_string(),
                gateway: "10.42.1.1".to_string(),
            },
            heartbeat_interval_ms: 200,
            session_timeout_ms: 800,
        },
        Arc::new(ReferenceAsDigest),
        Arc::new(DataKey::load_or_create(dir).unwrap()),
    )
    .await
}

/// The first start after a restore makes the first write before any other
/// change-log write of the start: the entries right after the restored head
/// mark every `expiring` row (the revoked one too) and only those, then move
/// the primary status, all by the `cluster` actor; `cluster.primary_moved`
/// points at the new holder's entry; `meta.restore_marker` names the
/// marker's head; and both files are gone.
#[tokio::test]
async fn the_first_start_after_a_restore_makes_the_first_write_before_any_other() {
    let (dir, store, old, head) = restored().await;
    store.pool().close().await;
    write_marker(&dir, &Marker::plain(head.clone()));
    write_primary_node(&dir);

    let server = start_in(dir.clone()).await;
    let store = &server.state.store;

    assert_eq!(marks(store).await, all_expiring_marked());
    let entries = entries_after(store, head.seq).await;
    let first: Vec<(i64, &str, &str)> = entries
        .iter()
        .take(4)
        .map(|(seq, kind, id, _, _, _)| (*seq, kind.as_str(), id.as_str()))
        .collect();
    assert_eq!(
        first,
        vec![
            (head.seq + 1, "lease", EXPIRING),
            (head.seq + 2, "lease", EXPIRING_REVOKED),
            (head.seq + 3, "node", old.as_str()),
            (head.seq + 4, "node", NEW_NODE),
        ],
        "{entries:?}"
    );
    for (_, _, _, actor_type, actor_id, _) in entries.iter().take(4) {
        assert_eq!(
            (actor_type.as_str(), actor_id.as_str()),
            ("system", "cluster")
        );
    }
    let payload = &entries[0].5;
    assert_eq!(payload["unsettled"], json!(true), "{payload}");
    assert_eq!(payload["mode"], "expiring", "{payload}");
    assert_eq!(payload["fencing_token"], 3, "{payload}");

    assert_eq!(cp_status(store, NEW_NODE).await.as_deref(), Some("primary"));
    assert_eq!(cp_status(store, &old).await, None);
    assert_eq!(moved_records(store).await, vec![Some(head.seq + 4)]);
    assert_eq!(restore_marker(store).await, Some(meta_value(&head)));
    assert!(!exists(&dir, restore_pending::MARKER_FILE));
    assert!(!exists(&dir, primary_status::MARKER_FILE));
}

/// The first write is one transaction: one that fails before its commit
/// writes nothing, neither the mark, nor the move, nor `meta`, and both files
/// stay, so the next start makes the whole write.
#[tokio::test]
async fn a_first_write_that_does_not_commit_writes_nothing_and_the_next_start_makes_it() {
    let (dir, store, old, head) = restored().await;
    write_marker(&dir, &Marker::plain(head.clone()));
    write_primary_node(&dir);
    sqlx::query(
        "CREATE TRIGGER refuse_restore_marker BEFORE INSERT ON meta \
         WHEN NEW.key = 'restore_marker' BEGIN SELECT RAISE(ABORT, 'refused by the test'); END",
    )
    .execute(store.pool())
    .await
    .unwrap();

    let error = restore_pending::apply(&store, &dir).await.unwrap_err();
    assert!(error.contains("restore_marker"), "{error}");
    assert_eq!(log_head_of(&store).await, head, "no entry is written");
    assert_eq!(marks(&store).await, unmarked());
    assert_eq!(cp_status(&store, &old).await.as_deref(), Some("primary"));
    assert!(moved_records(&store).await.is_empty());
    assert_eq!(restore_marker(&store).await, None);
    assert!(exists(&dir, restore_pending::MARKER_FILE));
    assert!(exists(&dir, primary_status::MARKER_FILE));

    sqlx::query("DROP TRIGGER refuse_restore_marker")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        restore_pending::apply(&store, &dir).await.unwrap(),
        Applied::FirstWrite {
            marked: 2,
            moved: true
        }
    );
    assert_eq!(marks(&store).await, all_expiring_marked());
    assert_eq!(
        cp_status(&store, NEW_NODE).await.as_deref(),
        Some("primary")
    );
    assert_eq!(restore_marker(&store).await, Some(meta_value(&head)));
}

/// A crash after the first write commits and before the files are deleted
/// leaves a marker that matches `meta.restore_marker`: the next start
/// deletes it, and `primary-node`, without writing again.
#[tokio::test]
async fn a_marker_whose_first_write_committed_is_deleted_without_a_second_write() {
    let (dir, store, _old, head) = restored().await;
    let marker = Marker::plain(head.clone());
    write_marker(&dir, &marker);
    write_primary_node(&dir);
    assert_eq!(
        restore_pending::apply(&store, &dir).await.unwrap(),
        Applied::FirstWrite {
            marked: 2,
            moved: true
        }
    );
    let after = log_head_of(&store).await;
    let records = moved_records(&store).await;

    // The crash: both files are still there.
    write_marker(&dir, &marker);
    write_primary_node(&dir);
    assert_eq!(
        restore_pending::apply(&store, &dir).await.unwrap(),
        Applied::AlreadyWritten
    );
    assert_eq!(log_head_of(&store).await, after, "nothing is written again");
    assert_eq!(moved_records(&store).await, records);
    assert_eq!(restore_marker(&store).await, Some(meta_value(&head)));
    assert!(!exists(&dir, restore_pending::MARKER_FILE));
    assert!(!exists(&dir, primary_status::MARKER_FILE));
}

/// A marker that names another log head, by its `seq` or by its hash,
/// belongs to another database: it is deleted and nothing is written from
/// it. `primary-node` is then applied on its own, as without a marker.
#[tokio::test]
async fn a_marker_of_another_database_is_deleted_without_a_first_write() {
    for case in ["seq", "hash"] {
        let (dir, store, old, head) = restored().await;
        let other = match case {
            "seq" => Head {
                seq: head.seq + 1,
                hash: head.hash.clone(),
            },
            _ => Head {
                seq: head.seq,
                hash: "f".repeat(64),
            },
        };
        write_marker(&dir, &Marker::plain(other));
        write_primary_node(&dir);

        assert_eq!(
            restore_pending::apply(&store, &dir).await.unwrap(),
            Applied::OtherDatabase,
            "{case}"
        );
        assert_eq!(marks(&store).await, unmarked(), "{case}");
        assert_eq!(restore_marker(&store).await, None, "{case}");
        assert!(!exists(&dir, restore_pending::MARKER_FILE), "{case}");
        // The move alone, as `apply_marker` makes it.
        let entries = entries_after(&store, head.seq).await;
        let kinds: Vec<(&str, &str)> = entries
            .iter()
            .map(|(_, kind, id, _, _, _)| (kind.as_str(), id.as_str()))
            .collect();
        assert_eq!(
            kinds,
            vec![("node", old.as_str()), ("node", NEW_NODE)],
            "{case}"
        );
        assert_eq!(
            cp_status(&store, NEW_NODE).await.as_deref(),
            Some("primary"),
            "{case}"
        );
        assert!(!exists(&dir, primary_status::MARKER_FILE), "{case}");
    }
}

/// Without a marker, `primary-node` moves the primary status on its own,
/// as before: no row is marked and `meta` records nothing.
#[tokio::test]
async fn without_a_marker_primary_node_moves_the_status_alone() {
    let (dir, store, old, head) = restored().await;
    write_primary_node(&dir);

    assert_eq!(
        restore_pending::apply(&store, &dir).await.unwrap(),
        Applied::NoMarker
    );
    assert_eq!(marks(&store).await, unmarked());
    assert_eq!(restore_marker(&store).await, None);
    assert_eq!(
        cp_status(&store, NEW_NODE).await.as_deref(),
        Some("primary")
    );
    assert_eq!(cp_status(&store, &old).await, None);
    let entries = entries_after(&store, head.seq).await;
    assert_eq!(entries.len(), 2, "{entries:?}");
    assert_eq!(moved_records(&store).await, vec![Some(head.seq + 2)]);
    assert!(!exists(&dir, primary_status::MARKER_FILE));
}

/// A first write without `primary-node` marks the rows and moves nothing.
#[tokio::test]
async fn a_first_write_without_primary_node_only_marks() {
    let (dir, store, old, head) = restored().await;
    write_marker(&dir, &Marker::plain(head.clone()));

    assert_eq!(
        restore_pending::apply(&store, &dir).await.unwrap(),
        Applied::FirstWrite {
            marked: 2,
            moved: false
        }
    );
    assert_eq!(marks(&store).await, all_expiring_marked());
    let entries = entries_after(&store, head.seq).await;
    let ids: Vec<&str> = entries.iter().map(|entry| entry.2.as_str()).collect();
    assert_eq!(ids, vec![EXPIRING, EXPIRING_REVOKED]);
    assert_eq!(cp_status(&store, &old).await.as_deref(), Some("primary"));
    assert!(moved_records(&store).await.is_empty());
    assert_eq!(restore_marker(&store).await, Some(meta_value(&head)));
    assert!(!exists(&dir, restore_pending::MARKER_FILE));
}

/// `--accept-loss` is not built yet: a marker of this database that asks
/// for it stops the start before anything is written, and both files stay,
/// so the restore is not taken as done without the removals.
#[tokio::test]
async fn a_marker_asking_for_accept_loss_stops_the_start() {
    let (dir, store, old, head) = restored().await;
    store.pool().close().await;
    write_marker(
        &dir,
        &Marker {
            head: head.clone(),
            accept_loss: true,
            statement: Some("no other node survives".to_string()),
        },
    );
    write_primary_node(&dir);

    let error = match open_state(&dir).await {
        Ok(_) => panic!("the start must stop"),
        Err(error) => error,
    };
    assert!(error.contains("--accept-loss"), "{error}");

    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    assert_eq!(log_head_of(&store).await, head, "nothing is written");
    assert_eq!(marks(&store).await, unmarked());
    assert_eq!(cp_status(&store, &old).await.as_deref(), Some("primary"));
    assert_eq!(restore_marker(&store).await, None);
    assert!(exists(&dir, restore_pending::MARKER_FILE));
    assert!(exists(&dir, primary_status::MARKER_FILE));
}

/// A marker the primary cannot parse, and a `primary-node` it cannot read
/// when the first write is due, stop the start with nothing written and the
/// files in place: going on would serve without the mark, or make the move
/// in a later, separate write.
#[tokio::test]
async fn an_unreadable_marker_or_primary_node_stops_the_start() {
    for case in ["marker", "primary-node"] {
        let (dir, store, old, head) = restored().await;
        if case == "marker" {
            std::fs::write(
                dir.join(restore_pending::MARKER_FILE),
                b"{\"head_seq\":1}\n",
            )
            .unwrap();
        } else {
            write_marker(&dir, &Marker::plain(head.clone()));
            // A directory cannot be read as a file.
            std::fs::create_dir(dir.join(primary_status::MARKER_FILE)).unwrap();
        }

        let error = restore_pending::apply(&store, &dir).await.unwrap_err();
        assert!(error.contains(case), "{case}: {error}");
        assert_eq!(log_head_of(&store).await, head, "{case}");
        assert_eq!(marks(&store).await, unmarked(), "{case}");
        assert_eq!(
            cp_status(&store, &old).await.as_deref(),
            Some("primary"),
            "{case}"
        );
        assert_eq!(restore_marker(&store).await, None, "{case}");
        assert!(exists(&dir, restore_pending::MARKER_FILE), "{case}");
    }
}

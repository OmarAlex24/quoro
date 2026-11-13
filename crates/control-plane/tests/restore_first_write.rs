//! The restored primary's first start (`backup-format.md` section 8.2, "The
//! first write" and "Detecting the first start"; ADR 0280, ADR 0285; issue
//! #838): the `restore-pending` marker that `quoro restore` leaves, and the
//! one write the primary makes from it before it serves. That write marks
//! every `expiring` lease row unsettled (node-agent-protocol.md section 8.4,
//! "The mark") and, when `primary-node` asks for it, moves the primary status
//! and records `cluster.primary_moved`.

mod common;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use common::{Server, start_in};
use quoro_control_plane::backups::{Backups, Bucket, BucketFactory, BucketStore, RepositoryTarget};
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::primary_status;
use quoro_control_plane::restore_pending::{self, Applied, Head, Marker};
use quoro_control_plane::store::{Store, now_rfc3339, random_secret};
use quoro_control_plane::{AppState, NodeNetwork, ReferenceAsDigest, ServerConfig};
use quoro_providers::object_store::{MemoryStore, ObjectInfo, ObjectStore, ObjectStoreError};
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

/// A new, empty directory under the system's temporary directory.
fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("quoro-test-{name}-{}", &random_secret()[..16]));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A data directory with the database `quoro restore` installed: the old
/// host's primary node, the new host's node, the leases' holder and the rows
/// of [`LEASES`]. Returns the directory, the open store, the old primary's id
/// and the log's head, which the restore's marker names.
async fn restored() -> (PathBuf, Store, String, Head) {
    let dir = temp_dir("restored");
    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    let old = populate(&store).await;
    let head = log_head_of(&store).await;
    (dir, store, old, head)
}

/// Writes the rows of [`restored`] into `store`: the old host's primary
/// node, the new host's node, the leases' holder and the rows of [`LEASES`],
/// each with its change-log entry. Returns the old primary's id.
async fn populate(store: &Store) -> String {
    let old = driver::register_local_node(store, &local_node())
        .await
        .unwrap();
    for id in [NEW_NODE, HOLDER] {
        insert_node(store, id).await;
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
    old
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

/// The `seq` of every `cluster.primary_moved` record, as the `entity_id` of
/// the change-log `audit` entry that carries it (`audit-log-format.md`
/// section 3.2).
async fn moved_audit_ids(store: &Store) -> Vec<String> {
    let seqs: Vec<i64> = sqlx::query_scalar(
        "SELECT seq FROM audit_log WHERE operation = 'cluster.primary_moved' ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    seqs.into_iter().map(|seq| seq.to_string()).collect()
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

/// The settings `common::start_in` uses, for a test that builds `AppState`
/// itself.
fn server_config(dir: &Path) -> ServerConfig {
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
    }
}

/// Opens the control plane on `dir` as `quoro serve` does, keeping the error
/// of a start that stops.
async fn open_state(dir: &Path) -> Result<AppState, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    AppState::open(
        store,
        server_config(dir),
        Arc::new(ReferenceAsDigest),
        Arc::new(DataKey::load_or_create(dir).unwrap()),
    )
    .await
}

/// Both steps of the marker as a start runs them, [`restore_pending::check`]
/// and then [`restore_pending::apply`], here with no migration pending
/// between them.
async fn check_and_apply(store: &Store, dir: &Path) -> Result<Applied, String> {
    let pending = restore_pending::check(store, dir).await?;
    restore_pending::apply(store, dir, pending).await
}

/// The first start after a restore makes the first write before any other
/// change-log write of the start: the entries right after the restored head
/// mark every `expiring` row (the revoked one too) and only those, then move
/// the primary status and carry its `cluster.primary_moved` record, all by
/// the `cluster` actor; that record points at the new holder's entry;
/// `meta.restore_marker` names the marker's head; and both files are gone.
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
    let audit_ids = moved_audit_ids(store).await;
    assert_eq!(audit_ids.len(), 1, "{audit_ids:?}");
    let first: Vec<(i64, &str, &str)> = entries
        .iter()
        .take(5)
        .map(|(seq, kind, id, _, _, _)| (*seq, kind.as_str(), id.as_str()))
        .collect();
    assert_eq!(
        first,
        vec![
            (head.seq + 1, "lease", EXPIRING),
            (head.seq + 2, "lease", EXPIRING_REVOKED),
            (head.seq + 3, "node", old.as_str()),
            (head.seq + 4, "node", NEW_NODE),
            (head.seq + 5, "audit", audit_ids[0].as_str()),
        ],
        "{entries:?}"
    );
    assert_eq!(
        entries[4].5["operation"], "cluster.primary_moved",
        "{entries:?}"
    );
    for (_, _, _, actor_type, actor_id, _) in entries.iter().take(5) {
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

    let error = check_and_apply(&store, &dir).await.unwrap_err();
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
        check_and_apply(&store, &dir).await.unwrap(),
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
        check_and_apply(&store, &dir).await.unwrap(),
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
        check_and_apply(&store, &dir).await.unwrap(),
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
            check_and_apply(&store, &dir).await.unwrap(),
            Applied::OtherDatabase,
            "{case}"
        );
        assert_eq!(marks(&store).await, unmarked(), "{case}");
        assert_eq!(restore_marker(&store).await, None, "{case}");
        assert!(!exists(&dir, restore_pending::MARKER_FILE), "{case}");
        // The move alone, as `apply_marker` makes it, with the change-log
        // entry that carries its audit record.
        assert_moved_alone(&store, &old, &head).await;
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
        check_and_apply(&store, &dir).await.unwrap(),
        Applied::NoMarker
    );
    assert_eq!(marks(&store).await, unmarked());
    assert_eq!(restore_marker(&store).await, None);
    assert_eq!(
        cp_status(&store, NEW_NODE).await.as_deref(),
        Some("primary")
    );
    assert_eq!(cp_status(&store, &old).await, None);
    assert_moved_alone(&store, &old, &head).await;
    assert!(!exists(&dir, primary_status::MARKER_FILE));
}

/// The entries after `head` are the primary status move alone, as
/// `primary_status::apply_marker` makes it: the old primary's node, the new
/// one's, and the `audit` entry that carries `cluster.primary_moved`, whose
/// `change_seq` is the new holder's entry.
async fn assert_moved_alone(store: &Store, old: &str, head: &Head) {
    let entries = entries_after(store, head.seq).await;
    let audit_ids = moved_audit_ids(store).await;
    assert_eq!(audit_ids.len(), 1, "{audit_ids:?}");
    let kinds: Vec<(i64, &str, &str)> = entries
        .iter()
        .map(|(seq, kind, id, _, _, _)| (*seq, kind.as_str(), id.as_str()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            (head.seq + 1, "node", old),
            (head.seq + 2, "node", NEW_NODE),
            (head.seq + 3, "audit", audit_ids[0].as_str()),
        ],
        "{entries:?}"
    );
    assert_eq!(moved_records(store).await, vec![Some(head.seq + 2)]);
}

/// A first write without `primary-node` marks the rows and moves nothing.
#[tokio::test]
async fn a_first_write_without_primary_node_only_marks() {
    let (dir, store, old, head) = restored().await;
    write_marker(&dir, &Marker::plain(head.clone()));

    assert_eq!(
        check_and_apply(&store, &dir).await.unwrap(),
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
/// for it stops the start before its migrations and before anything is
/// written, and both files stay, so the restore is not taken as done without
/// the removals.
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
    assert!(error.contains("nothing was written"), "{error}");

    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    assert_eq!(log_head_of(&store).await, head, "nothing is written");
    assert_eq!(marks(&store).await, unmarked());
    assert_eq!(cp_status(&store, &old).await.as_deref(), Some("primary"));
    assert_eq!(restore_marker(&store).await, None);
    assert!(exists(&dir, restore_pending::MARKER_FILE));
    assert!(exists(&dir, primary_status::MARKER_FILE));
}

/// A marker the primary cannot read or parse names no database it can
/// match, so it takes the path of a marker of another database
/// (`backup-format.md` section 8.2, "Otherwise"): the start goes on, the
/// marker is logged and deleted, nothing is written from it, and
/// `primary-node` moves the primary status on its own. A marker that cannot
/// be read here is a directory, which cannot be deleted as a file either:
/// the failure to delete it is logged too.
#[tokio::test]
async fn a_marker_that_cannot_be_read_or_parsed_is_deleted_without_a_first_write() {
    for case in ["not JSON", "not the object", "unreadable"] {
        let (dir, store, old, head) = restored().await;
        let path = dir.join(restore_pending::MARKER_FILE);
        match case {
            "not JSON" => std::fs::write(&path, b"restore-pending\n").unwrap(),
            "not the object" => std::fs::write(&path, b"{\"head_seq\":1}\n").unwrap(),
            _ => std::fs::create_dir(&path).unwrap(),
        }
        write_primary_node(&dir);

        let pending = restore_pending::check(&store, &dir).await.unwrap();
        assert!(
            matches!(pending, restore_pending::Pending::OtherDatabase { .. }),
            "{case}: {pending:?}"
        );
        assert_eq!(
            restore_pending::apply(&store, &dir, pending).await.unwrap(),
            Applied::OtherDatabase,
            "{case}"
        );
        assert_eq!(marks(&store).await, unmarked(), "{case}");
        assert_eq!(restore_marker(&store).await, None, "{case}");
        assert_moved_alone(&store, &old, &head).await;
        assert_eq!(
            cp_status(&store, NEW_NODE).await.as_deref(),
            Some("primary"),
            "{case}"
        );
        assert!(!exists(&dir, primary_status::MARKER_FILE), "{case}");
        if case != "unreadable" {
            assert!(!exists(&dir, restore_pending::MARKER_FILE), "{case}");
        }
    }
}

/// A `primary-node` the primary cannot read when the first write is due
/// stops the start before the pre-upgrade backup and the migrations, with
/// nothing written and both files in place: going on would make the move in
/// a later, separate write. The database is left unmigrated, so the next
/// start, once the file can be read, still finds the migrations pending: it
/// takes the pre-upgrade backup, migrates, makes the first write before any
/// other change-log write, and only then records the upgrade backup
/// (`backup-format.md` sections 7.2 and 8.2).
#[tokio::test]
async fn a_refused_start_migrates_nothing_and_the_next_start_records_the_upgrade_backup() {
    let bucket = Arc::new(MemoryStore::new());
    // The old primary, with a repository whose key the restored host holds.
    let previous = start_in(temp_dir("previous")).await;
    previous
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&bucket))));
    let owner = previous.owner().await;
    create_repository(&owner, &previous).await;
    let old = populate(&previous.state.store).await;
    let (dir, head) = installed_copy(&previous).await;
    write_marker(&dir, &Marker::plain(head.clone()));
    // A directory cannot be read as a file.
    std::fs::create_dir(dir.join(primary_status::MARKER_FILE)).unwrap();

    let error = match open_upgrading(&dir, &bucket).await {
        Ok(_) => panic!("the start must stop"),
        Err(error) => error,
    };
    assert!(error.contains("primary-node"), "{error}");
    assert!(error.contains("nothing was written"), "{error}");
    let store = upgrading_store(&dir).await;
    assert_eq!(
        store.pending_migrations().await.unwrap(),
        Some((
            Store::shipped_schema_version(),
            Store::shipped_schema_version() + 1
        )),
        "nothing is migrated"
    );
    assert_eq!(log_head_of(&store).await, head, "nothing is written");
    assert_eq!(marks(&store).await, unmarked());
    assert_eq!(cp_status(&store, &old).await.as_deref(), Some("primary"));
    assert_eq!(restore_marker(&store).await, None);
    assert!(upgrade_backups(&store).await.is_empty());
    assert!(exists(&dir, restore_pending::MARKER_FILE));
    store.pool().close().await;

    std::fs::remove_dir(dir.join(primary_status::MARKER_FILE)).unwrap();
    write_primary_node(&dir);
    let state = open_upgrading(&dir, &bucket).await.unwrap();
    let store = &state.store;
    assert_eq!(store.pending_migrations().await.unwrap(), None);
    assert_eq!(
        store.schema_version().await.unwrap(),
        Store::shipped_schema_version() + 1
    );
    assert_eq!(upgrade_backups(store).await, vec!["succeeded".to_string()]);
    assert_eq!(marks(store).await, all_expiring_marked());
    assert_eq!(cp_status(store, NEW_NODE).await.as_deref(), Some("primary"));
    assert_eq!(restore_marker(store).await, Some(meta_value(&head)));
    assert!(!exists(&dir, restore_pending::MARKER_FILE));
    assert!(!exists(&dir, primary_status::MARKER_FILE));
    // The first write comes first: two marks, the move and its record. The
    // upgrade backup's row is recorded after it.
    let entries = entries_after(store, head.seq).await;
    let kinds: Vec<&str> = entries
        .iter()
        .take(5)
        .map(|(_, kind, _, _, _, _)| kind.as_str())
        .collect();
    assert_eq!(
        kinds,
        vec!["lease", "lease", "node", "node", "audit"],
        "{entries:?}"
    );
    let backup_entries: Vec<i64> = entries
        .iter()
        .filter(|(_, kind, _, _, _, _)| kind == "backup")
        .map(|(seq, _, _, _, _, _)| *seq)
        .collect();
    assert!(!backup_entries.is_empty(), "{entries:?}");
    assert!(
        backup_entries.iter().all(|seq| *seq > head.seq + 5),
        "{entries:?}"
    );
}

// ---------------------------------------------------------------------------
// The upgrade of a restored database
// ---------------------------------------------------------------------------

const PREFIX: &str = "restore-first-write-test";

/// A bucket in memory for the pre-upgrade backup.
struct MemoryBuckets(Arc<MemoryStore>);

impl BucketStore for MemoryBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move { self.0.put_new(key, body, None).await.map(|_| ()) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.get(key))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.list(prefix))
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.delete(key))
    }
}

impl BucketFactory for MemoryBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(MemoryBuckets(Arc::clone(&self.0))))
    }
}

/// Creates the bucket provider and a repository through the API.
async fn create_repository(owner: &reqwest::Client, server: &Server) {
    let res = owner
        .post(server.url("/api/v1/providers"))
        .json(&json!({
            "kind": "object_store",
            "name": "backups",
            "config": {
                "endpoint": "https://s3.example.test",
                "bucket": "quoro",
                "access_key_id": "AKIATEST",
                "region": "auto"
            },
            "secret": "secret-access-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let provider: Value = res.json().await.unwrap();
    let res = owner
        .post(server.url("/api/v1/backup-repositories"))
        .json(&json!({
            "name": "primary",
            "provider_id": provider["id"],
            "prefix": PREFIX,
            "control_plane_schedule": { "interval_h": 24, "keep": 7 },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
}

/// The database of `server` as `quoro restore` installs it in a new data
/// directory, with the key files. `server` keeps running in the test, so the
/// copy is taken with `VACUUM INTO`, and its head is read from the copy.
/// Returns the new directory and that head.
async fn installed_copy(server: &Server) -> (PathBuf, Head) {
    let dir = temp_dir("installed");
    let keys = dir.join("keys");
    std::fs::create_dir_all(&keys).unwrap();
    for name in [quoro_control_plane::keys::SIGNING_KEY_FILE, "data.key"] {
        std::fs::copy(server.data_dir.join("keys").join(name), keys.join(name)).unwrap();
    }
    let database = dir.join("quoro.db");
    let path = database.to_str().unwrap();
    assert!(!path.contains('\''), "{path}");
    sqlx::query(&format!("VACUUM INTO '{path}'"))
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let store = Store::open_unmigrated(&database).await.unwrap();
    let head = log_head_of(&store).await;
    store.pool().close().await;
    (dir, head)
}

/// The migration the next release ships: a stand-in one version past the
/// newest shipped one, so no fake migration goes into
/// `docs/contracts/migrations/`.
fn extra_migrations() -> &'static [&'static str] {
    static EXTRA: std::sync::OnceLock<&'static [&'static str]> = std::sync::OnceLock::new();
    EXTRA.get_or_init(|| {
        let migration: &'static str = Box::leak(
            format!(
                "CREATE TABLE upgrade_probe (id INTEGER PRIMARY KEY);\n\
                 UPDATE meta SET value = '{}' WHERE key = 'schema_version';",
                Store::shipped_schema_version() + 1
            )
            .into_boxed_str(),
        );
        Box::leak(vec![migration].into_boxed_slice())
    })
}

/// The database in `dir`, unmigrated, as the next release opens it.
async fn upgrading_store(dir: &Path) -> Store {
    Store::open_unmigrated(&dir.join("quoro.db"))
        .await
        .unwrap()
        .with_extra_migrations(extra_migrations())
}

/// Opens the control plane on `dir` as the next release's `quoro serve`
/// does, with its migration pending and `bucket` as the repository's bucket.
async fn open_upgrading(dir: &Path, bucket: &Arc<MemoryStore>) -> Result<AppState, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = Backups::s3();
    runtime.set_factory(Arc::new(MemoryBuckets(Arc::clone(bucket))));
    AppState::open_with_upgrade(
        upgrading_store(dir).await,
        server_config(dir),
        Arc::new(ReferenceAsDigest),
        Arc::new(DataKey::load_or_create(dir).unwrap()),
        runtime,
        false,
    )
    .await
}

/// The `status` of every `upgrade` backup row.
async fn upgrade_backups(store: &Store) -> Vec<String> {
    sqlx::query_scalar("SELECT status FROM backups WHERE \"trigger\" = 'upgrade' ORDER BY id")
        .fetch_all(store.pool())
        .await
        .unwrap()
}

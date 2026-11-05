//! The primary status after `quoro restore` onto another host
//! (`backup-format.md` section 8.2): at its next start the primary moves
//! `cp_status = 'primary'` to the node `primary-node` names, through the change
//! log, records `cluster.primary_moved` and deletes the file.

mod common;

use std::path::{Path, PathBuf};

use common::start_in;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::primary_status::MARKER_FILE;
use quoro_control_plane::store::{Store, now_rfc3339, random_secret};
use serde_json::{Value, json};

/// The node on the new host, enrolled before the backup was taken.
const NEW_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0B";
/// A node the database holds as removed.
const REMOVED_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0D";
/// A node the database does not hold.
const UNKNOWN_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0E";

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

/// A data directory whose database holds the old host's primary node, the new
/// host's node and a removed node, as a restored database would. Returns the
/// directory and the old primary's id.
async fn restored_data_dir() -> (PathBuf, String) {
    let dir = std::env::temp_dir().join(format!("quoro-primary-{}", &random_secret()[..16]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("quoro.db")).await.unwrap();
    let old = driver::register_local_node(&store, &local_node())
        .await
        .unwrap();
    for (id, status) in [(NEW_NODE, "disconnected"), (REMOVED_NODE, "removed")] {
        insert_node(&store, id, status).await;
    }
    store.pool().close().await;
    (dir, old)
}

async fn insert_node(store: &Store, id: &str, status: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
         labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
         log_seq, last_seen_at, created_at) \
         VALUES (?, ?, '[\"node\"]', NULL, ?, '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, \
         0, ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(status)
    .bind(&now)
    .bind(&now)
    .execute(store.pool())
    .await
    .unwrap();
}

fn write_marker(dir: &Path, text: &str) {
    std::fs::write(dir.join(MARKER_FILE), text).unwrap();
}

async fn cp_status(store: &Store, id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT cp_status FROM nodes WHERE id = ?")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap()
}

/// Every `cluster.primary_moved` record as (actor_type, actor_id, channel,
/// scope, target_type, target_id, params, result, change_seq).
#[allow(clippy::type_complexity)]
async fn moved_records(
    store: &Store,
) -> Vec<(
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<i64>,
)> {
    sqlx::query_as(
        "SELECT actor_type, actor_id, channel, scope, target_type, target_id, params, result, \
         change_seq FROM audit_log WHERE operation = 'cluster.primary_moved' ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap()
}

/// The `cp_status` in the payloads of the change-log entries the `cluster`
/// actor wrote for `id`, oldest first, with their seqs.
async fn cluster_entries(store: &Store, id: &str) -> Vec<(i64, Value)> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT seq, payload FROM change_log WHERE entity_type = 'node' AND entity_id = ? \
         AND actor_type = 'system' AND actor_id = 'cluster' AND op = 'update' ORDER BY seq",
    )
    .bind(id)
    .fetch_all(store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, payload)| {
            let payload: Value = serde_json::from_str(&payload).unwrap();
            (seq, payload["cp_status"].clone())
        })
        .collect()
}

#[tokio::test]
async fn the_marker_moves_the_primary_status_at_start() {
    let (dir, old) = restored_data_dir().await;
    write_marker(&dir, &format!("{NEW_NODE}\n"));

    let server = start_in(dir.clone()).await;
    let store = &server.state.store;

    assert_eq!(cp_status(store, NEW_NODE).await.as_deref(), Some("primary"));
    assert_eq!(cp_status(store, &old).await, None);
    assert!(!dir.join(MARKER_FILE).exists(), "the file is deleted");

    let owner = server.owner().await;
    let cluster: Value = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cluster["primary_node_id"], NEW_NODE);

    let cleared = cluster_entries(store, &old).await;
    assert_eq!(cleared.len(), 1, "{cleared:?}");
    assert_eq!(cleared[0].1, Value::Null);
    let set = cluster_entries(store, NEW_NODE).await;
    assert_eq!(set.len(), 1, "{set:?}");
    assert_eq!(set[0].1, json!("primary"));

    let records = moved_records(store).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let (actor_type, actor_id, channel, scope, target_type, target_id, params, result, seq) =
        records.into_iter().next().unwrap();
    assert_eq!(actor_type, "system");
    assert_eq!(actor_id, "cluster");
    assert_eq!(channel, "system");
    assert_eq!(scope, None);
    assert_eq!(target_type.as_deref(), Some("node"));
    assert_eq!(target_id.as_deref(), Some(NEW_NODE));
    assert_eq!(
        serde_json::from_str::<Value>(&params).unwrap(),
        json!({ "from_node_id": old, "to_node_id": NEW_NODE })
    );
    assert_eq!(result, "ok");
    assert_eq!(seq, Some(set[0].0), "change_seq is the new holder's entry");
}

#[tokio::test]
async fn a_marker_naming_the_current_primary_changes_nothing() {
    let (dir, old) = restored_data_dir().await;
    write_marker(&dir, &old);

    let server = start_in(dir.clone()).await;
    let store = &server.state.store;

    assert_eq!(cp_status(store, &old).await.as_deref(), Some("primary"));
    assert!(moved_records(store).await.is_empty());
    assert!(cluster_entries(store, &old).await.is_empty());
    assert!(!dir.join(MARKER_FILE).exists(), "the file is deleted");
}

#[tokio::test]
async fn a_marker_naming_an_unknown_removed_or_garbage_node_changes_nothing() {
    for text in [UNKNOWN_NODE, REMOVED_NODE, "not a node id"] {
        let (dir, old) = restored_data_dir().await;
        write_marker(&dir, text);

        let server = start_in(dir.clone()).await;
        let store = &server.state.store;

        assert_eq!(
            cp_status(store, &old).await.as_deref(),
            Some("primary"),
            "{text}"
        );
        assert_eq!(cp_status(store, REMOVED_NODE).await, None, "{text}");
        assert_eq!(cp_status(store, NEW_NODE).await, None, "{text}");
        assert!(moved_records(store).await.is_empty(), "{text}");
        assert!(
            !dir.join(MARKER_FILE).exists(),
            "{text}: the file is deleted"
        );
    }
}

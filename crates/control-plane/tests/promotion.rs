//! The write that moves the primary status in `quoro cluster promote`
//! (`node-agent-protocol.md` section 7.3 step 5; `audit-log-format.md`
//! section 3.9), with `--ack-fence` (sections 7.3 and 8.3, ADR 0236) and
//! `--accept-loss` (section 8.4, "The mark"), and the token-cap check of
//! `--ack-fence` (section 8.6, ADR 0325, issue #931).
//!
//! Each test builds the new primary's database as the rebuild leaves it: the
//! rows, a `create` entry for each at the old term, and `meta.term` at the
//! promotion's term `T`.

mod common;

use common::start;
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::promotion::{
    self, ExhaustedLease, Promotion, PromotionError, TokenExhausted, Written,
};
use quoro_control_plane::rebuild;
use quoro_control_plane::store::{Store, now_rfc3339};
use serde_json::{Value, json};
use sqlx::SqliteConnection;

/// The token cap (section 8.6, "The cap"): 2^53 − 1.
const MAX_TOKEN: i64 = (1 << 53) - 1;
/// The term of the followed entries, and the promotion's term `T`.
const OLD_TERM: i64 = 6;
const TERM: i64 = 7;
/// The `seq` of the last followed entry the database was rebuilt from.
const LOG_SEQ: i64 = 42;
const STATEMENT: &str = "the old primary's host is powered off and stays off";

/// The old primary's node, which holds `cp_status = 'primary'`.
const OLD: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0A";
/// This machine's node, the one promoted.
const NEW: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0B";
/// Another node.
const OTHER: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0C";
/// A node whose id sorts before every other node's, for a test that inserts
/// it after them.
const EARLY: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J09";

/// The old primary's leases: `local`, `expiring`, and `expiring` revoked.
const OLD_LOCAL: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J01";
const OLD_EXPIRING: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J02";
const OLD_REVOKED: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J03";
/// The other node's leases: `expiring`, `local`, and `expiring` already
/// marked.
const OTHER_EXPIRING: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J04";
const OTHER_LOCAL: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J05";
const OTHER_MARKED: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J06";

const REVOKED_AT: &str = "2026-11-12T09:00:00.000Z";
const LOST_AT: &str = "2026-11-12T10:00:00.000Z";

/// `(id, holder, mode, token, revoked_at, unsettled)` of a fixture lease.
type FixtureLease = (
    &'static str,
    &'static str,
    &'static str,
    i64,
    Option<&'static str>,
    bool,
);

const LEASES: [FixtureLease; 6] = [
    (OLD_LOCAL, OLD, "local", 3, None, false),
    (OLD_EXPIRING, OLD, "expiring", 5, None, false),
    (OLD_REVOKED, OLD, "expiring", 4, Some(REVOKED_AT), false),
    (OTHER_EXPIRING, OTHER, "expiring", 9, None, false),
    (OTHER_LOCAL, OTHER, "local", 2, None, false),
    (OTHER_MARKED, OTHER, "expiring", 8, None, true),
];

fn fixture_actor() -> Actor {
    Actor::system("fixture")
}

async fn set_term(store: &Store, term: &str) {
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES ('term', ?) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(term)
    .execute(store.pool())
    .await
    .unwrap();
}

/// Inserts a disconnected node `id` with `cp_status`, and its `create` entry.
/// It carries the capacity, allocation and KVM probe an enrolled node has,
/// so the API can read it back as a `Node`.
async fn insert_node(conn: &mut SqliteConnection, id: &str, cp_status: Option<&str>, now: &str) {
    let capacity = json!({ "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 });
    let allocated = json!({ "cpus": 0, "memory_mb": 0, "disk_gb": 0 });
    let kvm = json!({ "available": true, "kernel": "6.9.0" });
    sqlx::query(
        "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, \
         behind_nat, labels, capacity, allocated, kvm, engine_version, agent_version, \
         desired_rev, applied_rev, log_seq, last_seen_at, created_at) \
         VALUES (?, ?, '[\"node\"]', ?, 'disconnected', '', NULL, 1, '{}', ?, ?, ?, \
         NULL, NULL, 0, 0, 0, ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(cp_status)
    .bind(capacity.to_string())
    .bind(allocated.to_string())
    .bind(kvm.to_string())
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await
    .unwrap();
    let payload = json!({
        "id": id, "name": id, "roles": ["node"], "cp_status": cp_status,
        "status": "disconnected", "public_key": "", "address": null, "behind_nat": 1,
        "labels": {}, "capacity": capacity, "allocated": allocated, "kvm": kvm,
        "engine_version": null, "agent_version": null, "desired_rev": 0, "applied_rev": 0,
        "log_seq": 0, "last_seen_at": now, "created_at": now, "lost_at": null,
    });
    changelog::append(conn, &fixture_actor(), "node", id, Op::Create, payload)
        .await
        .unwrap();
}

/// Inserts the fixture's nodes and leases into `store`, each with its
/// `create` entry. `old_holds_primary` gives the old primary's node
/// `cp_status = 'primary'`; a running control plane keeps that status on its
/// own node, so the API's copy of the fixture leaves it out.
async fn fixture(store: &Store, old_holds_primary: bool) {
    let now = now_rfc3339();
    let mut w = store.begin_write().await.unwrap();
    for (id, cp_status) in [
        (OLD, old_holds_primary.then_some("primary")),
        (NEW, None),
        (OTHER, None),
    ] {
        insert_node(&mut w.tx, id, cp_status, &now).await;
    }
    for (index, (id, holder, mode, token, revoked_at, unsettled)) in LEASES.iter().enumerate() {
        let app = format!("app_01JC9Z1A2B3C4D5E6F7G8H9J{index:02}");
        let duration_ms = (*mode == "expiring").then_some(5000);
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, duration_ms, granted_at, revoked_at, unsettled) \
             VALUES (?, 'app', ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(&app)
        .bind(holder)
        .bind(mode)
        .bind(token)
        .bind(duration_ms)
        .bind(&now)
        .bind(revoked_at)
        .bind(unsettled)
        .execute(&mut *w.tx)
        .await
        .unwrap();
        let payload = json!({
            "id": id, "resource_type": "app", "resource_id": app, "holder_node_id": holder,
            "mode": mode, "fencing_token": token, "duration_ms": duration_ms,
            "granted_at": now, "renewed_at": null, "expires_at": null,
            "revoked_at": revoked_at, "unsettled": unsettled,
        });
        changelog::append(
            &mut w.tx,
            &fixture_actor(),
            "lease",
            id,
            Op::Create,
            payload,
        )
        .await
        .unwrap();
    }
    w.commit().await.unwrap();
}

/// The new primary's database before step 5: the fixture's rows and entries
/// at the old term, and `meta.term` at `T`.
async fn rebuilt() -> Store {
    let store = Store::open_in_memory().await.unwrap();
    set_term(&store, &OLD_TERM.to_string()).await;
    fixture(&store, true).await;
    set_term(&store, &TERM.to_string()).await;
    store
}

/// Raises the token of `lease_id` to `token`, with its `update` entry, as a
/// followed log that reached the cap would hold it.
async fn set_token(store: &Store, lease_id: &str, token: i64) {
    let mut w = store.begin_write().await.unwrap();
    sqlx::query("UPDATE leases SET fencing_token = ? WHERE id = ?")
        .bind(token)
        .bind(lease_id)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    changelog::append(
        &mut w.tx,
        &fixture_actor(),
        "lease",
        lease_id,
        Op::Update,
        json!({ "id": lease_id, "fencing_token": token }),
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
}

/// Sets `cp_status` and `lost_at` of node `id`, with its `update` entry, as a
/// followed log would hold them.
async fn set_node(store: &Store, id: &str, cp_status: Option<&str>, lost_at: Option<&str>) {
    let mut w = store.begin_write().await.unwrap();
    sqlx::query("UPDATE nodes SET cp_status = ?, lost_at = ? WHERE id = ?")
        .bind(cp_status)
        .bind(lost_at)
        .bind(id)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    changelog::append(
        &mut w.tx,
        &fixture_actor(),
        "node",
        id,
        Op::Update,
        json!({ "id": id, "cp_status": cp_status, "lost_at": lost_at }),
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
}

/// The fold of every entry in `store`'s change log (section 7.3 step 4).
async fn fold(store: &Store) -> rebuild::Folded {
    let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, entity_type, entity_id, op, payload FROM change_log ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    let mut conn = store.pool().acquire().await.unwrap();
    let schema = rebuild::Schema::read(&mut conn).await.unwrap();
    drop(conn);
    rebuild::fold(
        &schema,
        rows.iter().map(
            |(seq, entity_type, entity_id, op, payload)| rebuild::Entry {
                seq: u64::try_from(*seq).unwrap(),
                entity_type,
                entity_id,
                op,
                payload,
            },
        ),
    )
    .unwrap()
}

fn request(ack_fence: bool, accept_loss: bool, statement: Option<&str>) -> Promotion {
    Promotion {
        term: TERM,
        to_node_id: NEW.to_string(),
        log_seq: LOG_SEQ,
        ack_fence,
        accept_loss,
        statement: statement.map(str::to_string),
    }
}

/// `(seq, term, actor_type, actor_id, entity_type, entity_id, payload)` of
/// every change-log entry after `after`.
type Entry = (i64, i64, String, String, String, String, Value);

async fn entries_after(store: &Store, after: i64) -> Vec<Entry> {
    let rows: Vec<(i64, i64, String, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, term, actor_type, actor_id, entity_type, entity_id, payload \
         FROM change_log WHERE seq > ? ORDER BY seq",
    )
    .bind(after)
    .fetch_all(store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, term, actor_type, actor_id, kind, id, payload)| {
            (
                seq,
                term,
                actor_type,
                actor_id,
                kind,
                id,
                serde_json::from_str(&payload).unwrap(),
            )
        })
        .collect()
}

async fn log_head(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM change_log")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn audit_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

/// `(cp_status, lost_at)` of node `id`.
async fn node(store: &Store, id: &str) -> (Option<String>, Option<String>) {
    sqlx::query_as("SELECT cp_status, lost_at FROM nodes WHERE id = ?")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap()
}

/// One lease row: `(holder, mode, token, duration_ms, revoked_at,
/// unsettled)`.
type LeaseRow = (String, String, i64, Option<i64>, Option<String>, bool);

async fn lease(store: &Store, id: &str) -> LeaseRow {
    sqlx::query_as(
        "SELECT holder_node_id, mode, fencing_token, duration_ms, revoked_at, unsettled \
         FROM leases WHERE id = ?",
    )
    .bind(id)
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn leases(store: &Store) -> Vec<(String, LeaseRow)> {
    let mut rows = Vec::new();
    for (id, ..) in LEASES {
        rows.push((id.to_string(), lease(store, id).await));
    }
    rows
}

/// Each `cluster.promoted` record: its `seq`, and its fields but `time`,
/// `request_id` and the hashes, as JSON.
#[allow(clippy::type_complexity)]
async fn promoted_records(store: &Store) -> Vec<(i64, Value)> {
    let rows: Vec<(
        i64,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
        String,
        Option<String>,
        Option<i64>,
    )> = sqlx::query_as(
        "SELECT seq, actor_type, actor_id, channel, agent_model, scope, target_type, target_id, \
         params, justification, result, result_detail, change_seq \
         FROM audit_log WHERE operation = 'cluster.promoted' ORDER BY seq",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(
            |(
                seq,
                actor_type,
                actor_id,
                channel,
                agent_model,
                scope,
                target_type,
                target_id,
                params,
                justification,
                result,
                result_detail,
                change_seq,
            )| {
                (
                    seq,
                    json!({
                        "actor_type": actor_type, "actor_id": actor_id, "channel": channel,
                        "agent_model": agent_model, "scope": scope,
                        "target_type": target_type, "target_id": target_id,
                        "params": serde_json::from_str::<Value>(&params).unwrap(),
                        "justification": justification, "result": result,
                        "result_detail": result_detail, "change_seq": change_seq,
                    }),
                )
            },
        )
        .collect()
}

/// The record `audit-log-format.md` section 3.9 asks for, with `from` as
/// `params.from_node_id`. It is compared as a `Value`, so each param's type
/// counts too: `term` and `log_seq` are integers, the flags booleans, and a
/// `from_node_id` of `null` is neither absent nor a string.
fn expected_record(
    from: Option<&str>,
    ack_fence: bool,
    accept_loss: bool,
    change_seq: i64,
) -> Value {
    json!({
        "actor_type": "system", "actor_id": "cluster", "channel": "cli",
        "agent_model": null, "scope": null, "target_type": "node", "target_id": NEW,
        "params": {
            "from_node_id": from, "to_node_id": NEW, "term": TERM, "log_seq": LOG_SEQ,
            "ack_fence": ack_fence, "accept_loss": accept_loss,
        },
        "justification": (ack_fence || accept_loss).then_some(STATEMENT),
        "result": "ok", "result_detail": null, "change_seq": change_seq,
    })
}

/// Asserts that `written` is one contiguous range of entries after `before`,
/// every one at term `T` by the `cluster` system actor, and that every
/// entry before it is at the old term. Returns the range's entries.
async fn assert_one_write_at_term(store: &Store, before: i64, written: &Written) -> Vec<Entry> {
    let entries = entries_after(store, before).await;
    assert_eq!(written.first_seq, before + 1);
    assert_eq!(written.last_seq, log_head(store).await);
    let seqs: Vec<i64> = entries.iter().map(|entry| entry.0).collect();
    assert_eq!(
        seqs,
        (written.first_seq..=written.last_seq).collect::<Vec<_>>()
    );
    for (seq, term, actor_type, actor_id, ..) in &entries {
        assert_eq!(*term, TERM, "entry {seq}");
        assert_eq!(
            (actor_type.as_str(), actor_id.as_str()),
            ("system", "cluster"),
            "entry {seq}"
        );
    }
    let older: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT term FROM change_log WHERE seq <= ?")
        .bind(before)
        .fetch_all(store.pool())
        .await
        .unwrap();
    assert_eq!(older, [OLD_TERM]);
    // The record's own entry closes the write (section 3.2).
    let last = entries.last().unwrap();
    assert_eq!(last.4, "audit");
    assert_eq!(last.6["seq"], json!(written.audit_seq));
    entries
}

/// `(entity_type, entity_id)` of each entry.
fn kinds(entries: &[Entry]) -> Vec<(&str, &str)> {
    entries
        .iter()
        .map(|entry| (entry.4.as_str(), entry.5.as_str()))
        .collect()
}

/// Section 7.3 step 5 with no flag: one change-log write at term `T` clears
/// `cp_status` on the old primary's node, sets it on the promoted node, each
/// with its node entry, and records `cluster.promoted` exactly as
/// audit-log-format.md section 3.9 says, with no `justification` and the
/// move's entry as `change_seq`. No lease and no `lost_at` changes.
#[tokio::test]
async fn a_promotion_moves_the_primary_status_in_one_write_at_its_term() {
    let store = rebuilt().await;
    let leases_before = leases(&store).await;
    let before = log_head(&store).await;
    let written = promotion::write(&store, &request(false, false, None))
        .await
        .unwrap();
    assert_eq!(written.from_node_id.as_deref(), Some(OLD));

    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(
        kinds(&entries),
        [("node", OLD), ("node", NEW), ("audit", "1")]
    );
    assert_eq!(entries[0].6["cp_status"], Value::Null);
    assert_eq!(entries[1].6["cp_status"], "primary");
    assert_eq!(node(&store, OLD).await, (None, None));
    assert_eq!(node(&store, NEW).await, (Some("primary".to_string()), None));

    assert_eq!(
        promoted_records(&store).await,
        [(
            written.audit_seq,
            expected_record(Some(OLD), false, false, entries[1].0)
        )]
    );
    assert_eq!(audit_count(&store).await, 1, "the first record of the term");
    assert_eq!(leases(&store).await, leases_before);
}

/// Section 7.3 step 5 and section 8.3: with `--ack-fence` the write declares
/// the old primary's node lost as `declareNodeLost` does. On the same
/// fixture, the API call and the promotion leave the same lease rows, raise
/// the same tokens, a revoked row's included (ADR 0202), write the same
/// lease entries in the same order, and set `lost_at` with a node entry that
/// carries it. Only the API's refusal of a connected node is not the
/// promotion's.
#[tokio::test]
async fn ack_fence_declares_the_old_primary_lost_as_declare_node_lost_does() {
    // The API on a running control plane, whose own node holds the status.
    let server = start().await;
    let owner = server.owner().await;
    fixture(&server.state.store, false).await;
    let api_before = log_head(&server.state.store).await;
    let res = owner
        .post(server.url(&format!("/api/v1/nodes/{OLD}/declare-lost")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    // The fixture's own entries: the running control plane may write others
    // meanwhile.
    let api_entries: Vec<Entry> = entries_after(&server.state.store, api_before)
        .await
        .into_iter()
        .filter(|entry| entry.5 == OLD || LEASES.iter().any(|lease| lease.0 == entry.5))
        .collect();

    // The promotion on the new primary's database.
    let store = rebuilt().await;
    let before = log_head(&store).await;
    let written = promotion::write(&store, &request(true, false, Some(STATEMENT)))
        .await
        .unwrap();
    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(
        kinds(&entries),
        [
            ("node", OLD),
            ("node", NEW),
            ("lease", OLD_LOCAL),
            ("lease", OLD_EXPIRING),
            ("lease", OLD_REVOKED),
            ("node", OLD),
            ("audit", "1"),
        ]
    );

    // The same rows: each lease row that names the node, the revoked one
    // included, is raised by one and recorded revoked with the node still its
    // holder, the revoked one keeping its `revoked_at`; the other node's are
    // left as they are.
    let (_, api_lost) = node(&server.state.store, OLD).await;
    let (cp_status, lost_at) = node(&store, OLD).await;
    assert_eq!(cp_status, None);
    let lost_at = lost_at.expect("declared lost");
    assert!(api_lost.is_some(), "the API declared it lost");
    let mut raised = 0;
    for (id, row) in leases(&store).await {
        let api_row = lease(&server.state.store, &id).await;
        let without_time = |row: &LeaseRow| (row.0.clone(), row.1.clone(), row.2, row.3, row.5);
        assert_eq!(without_time(&row), without_time(&api_row), "{id}");
        assert_eq!(row.4.is_some(), api_row.4.is_some(), "{id}");
        let fixture_row = LEASES.iter().find(|lease| lease.0 == id).unwrap();
        if row.2 == fixture_row.3 + 1 {
            raised += 1;
            let revoked_at = row.4.as_deref().unwrap();
            assert!(
                revoked_at <= lost_at.as_str(),
                "{id}: revoked before lost_at"
            );
        } else {
            assert_eq!(row.2, fixture_row.3, "{id}");
            assert_eq!(row.4.as_deref(), fixture_row.4, "{id}");
        }
    }
    assert_eq!(raised, 3, "the three lease rows that name the old primary");
    assert_eq!(lease(&store, OLD_LOCAL).await.2, 4);
    assert_eq!(lease(&store, OLD_EXPIRING).await.2, 6);
    for store in [&store, &server.state.store] {
        let (_, _, token, _, revoked_at, _) = lease(store, OLD_REVOKED).await;
        assert_eq!((token, revoked_at.as_deref()), (5, Some(REVOKED_AT)));
    }

    // The same entries, in the same order: the lease entries, then the
    // node's with `lost_at`.
    let declared: Vec<&Entry> = entries[2..6].iter().collect();
    assert_eq!(
        kinds(&api_entries),
        [
            ("lease", OLD_LOCAL),
            ("lease", OLD_EXPIRING),
            ("lease", OLD_REVOKED),
            ("node", OLD)
        ]
    );
    for (ours, api) in declared.iter().zip(&api_entries) {
        let strip = |payload: &Value| {
            let mut payload = payload.clone();
            let object = payload.as_object_mut().unwrap();
            for time in [
                "granted_at",
                "revoked_at",
                "lost_at",
                "last_seen_at",
                "created_at",
                "cp_status",
            ] {
                object.remove(time);
            }
            payload
        };
        assert_eq!(strip(&ours.6), strip(&api.6), "{}", ours.5);
    }
    assert_eq!(declared[3].6["lost_at"], json!(lost_at));
    assert!(api_entries[3].6["lost_at"].is_string());

    assert_eq!(
        promoted_records(&store).await,
        [(
            written.audit_seq,
            expected_record(Some(OLD), true, false, entries[1].0)
        )]
    );
}

/// Section 7.3 and section 8.6 (ADR 0325, issue #931): a lease the old
/// primary's node holds at 2^53 − 1 makes the `--ack-fence` write refuse
/// with `token_exhausted`, naming that lease and its token, and write
/// nothing. The pure check on the fold of the same log names the same
/// lease. A row already revoked at the cap is not named. Without
/// `--ack-fence` nothing is raised, so the promotion goes on.
#[tokio::test]
async fn ack_fence_at_the_token_cap_writes_nothing_and_names_the_lease() {
    let store = rebuilt().await;
    set_term(&store, &OLD_TERM.to_string()).await;
    set_token(&store, OLD_EXPIRING, MAX_TOKEN).await;
    set_token(&store, OLD_REVOKED, MAX_TOKEN).await;
    set_term(&store, &TERM.to_string()).await;
    let named = TokenExhausted {
        leases: vec![ExhaustedLease {
            lease_id: OLD_EXPIRING.to_string(),
            fencing_token: MAX_TOKEN,
        }],
    };

    // The check the command runs before its votes and its freeze.
    let folded = fold(&store).await;
    assert_eq!(promotion::old_primary(&folded, NEW), Some(OLD));
    assert_eq!(promotion::check_token_cap(&folded, OLD), Err(named.clone()));

    let leases_before = leases(&store).await;
    let before = log_head(&store).await;
    let error = promotion::write(&store, &request(true, false, Some(STATEMENT)))
        .await
        .unwrap_err();
    let PromotionError::TokenExhausted(refusal) = &error else {
        panic!("{error}");
    };
    assert_eq!(refusal, &named);
    let text = error.to_string();
    assert!(text.starts_with("token_exhausted: "), "{text}");
    assert!(
        text.contains(&format!(
            "lease {OLD_EXPIRING} is at fencing token {MAX_TOKEN}"
        )),
        "{text}"
    );
    assert!(!text.contains(OLD_REVOKED), "{text}");
    assert_eq!(log_head(&store).await, before, "nothing written");
    assert_eq!(audit_count(&store).await, 0);
    assert_eq!(leases(&store).await, leases_before);
    assert_eq!(node(&store, OLD).await, (Some("primary".to_string()), None));
    assert_eq!(node(&store, NEW).await, (None, None));

    // Without --ack-fence no token is raised, and the write goes on.
    let written = promotion::write(&store, &request(false, true, Some(STATEMENT)))
        .await
        .unwrap();
    assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(lease(&store, OLD_EXPIRING).await.2, MAX_TOKEN);
}

/// Section 7.3 step 5 and section 8.4, "The mark": with `--accept-loss` the
/// write marks every `expiring` row unsettled, the revoked one included,
/// each with its entry, and only those: the `local` rows and the row
/// already marked get no write. The record keeps the statement and sets
/// `params.accept_loss`; the old primary's node is not declared lost.
#[tokio::test]
async fn accept_loss_marks_every_expiring_row_and_only_those() {
    let store = rebuilt().await;
    let leases_before = leases(&store).await;
    let before = log_head(&store).await;
    let written = promotion::write(&store, &request(false, true, Some(STATEMENT)))
        .await
        .unwrap();
    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(
        kinds(&entries),
        [
            ("node", OLD),
            ("node", NEW),
            ("lease", OLD_EXPIRING),
            ("lease", OLD_REVOKED),
            ("lease", OTHER_EXPIRING),
            ("audit", "1"),
        ]
    );
    let marked = [OLD_EXPIRING, OLD_REVOKED, OTHER_EXPIRING];
    for ((id, after), (_, old)) in leases(&store).await.iter().zip(&leases_before) {
        let mut expected = old.clone();
        if marked.contains(&id.as_str()) {
            assert!(!old.5, "{id} was not marked");
            expected.5 = true;
        }
        assert_eq!(after, &expected, "{id}");
    }
    for entry in &entries[2..5] {
        assert_eq!(entry.6["unsettled"], true, "{}", entry.5);
        assert_eq!(entry.6["mode"], "expiring", "{}", entry.5);
    }
    assert_eq!(node(&store, OLD).await, (None, None), "not declared lost");
    assert_eq!(
        promoted_records(&store).await,
        [(
            written.audit_seq,
            expected_record(Some(OLD), false, true, entries[1].0)
        )]
    );
}

/// Section 7.3: with both flags one statement serves both. The one write
/// declares the old primary's node lost and marks every `expiring` row,
/// and one record sets both flags with the statement as `justification`.
#[tokio::test]
async fn both_flags_take_one_statement_in_one_write() {
    let store = rebuilt().await;
    let before = log_head(&store).await;
    let written = promotion::write(&store, &request(true, true, Some(STATEMENT)))
        .await
        .unwrap();
    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(
        kinds(&entries),
        [
            ("node", OLD),
            ("node", NEW),
            ("lease", OLD_LOCAL),
            ("lease", OLD_EXPIRING),
            ("lease", OLD_REVOKED),
            ("node", OLD),
            ("lease", OLD_EXPIRING),
            ("lease", OLD_REVOKED),
            ("lease", OTHER_EXPIRING),
            ("audit", "1"),
        ]
    );
    assert!(node(&store, OLD).await.1.is_some(), "declared lost");
    let (_, _, token, _, revoked_at, unsettled) = lease(&store, OLD_EXPIRING).await;
    assert_eq!((token, revoked_at.is_some(), unsettled), (6, true, true));
    let (_, _, token, _, _, unsettled) = lease(&store, OLD_LOCAL).await;
    assert_eq!((token, unsettled), (4, false));
    assert!(lease(&store, OTHER_EXPIRING).await.5);
    assert_eq!(
        promoted_records(&store).await,
        [(
            written.audit_seq,
            expected_record(Some(OLD), true, true, entries[1].0)
        )]
    );
}

/// The write refuses, and writes nothing, when the database is not at the
/// promotion's term (its `meta.term` is read through the one reader of the
/// term), when a flag comes without a statement, and when the promoted node
/// has no row or a removed one.
#[tokio::test]
async fn a_database_not_at_the_term_or_a_flag_without_statement_writes_nothing() {
    let store = rebuilt().await;
    let before = log_head(&store).await;
    let assert_nothing = |store: &Store| {
        let store = store.clone();
        async move {
            assert_eq!(log_head(&store).await, before);
            assert_eq!(audit_count(&store).await, 0);
            assert_eq!(node(&store, OLD).await, (Some("primary".to_string()), None));
        }
    };

    let mut other_term = request(false, false, None);
    other_term.term = TERM + 1;
    match promotion::write(&store, &other_term).await {
        Err(PromotionError::Term { expected, found }) => {
            assert_eq!((expected, found.as_str()), (TERM + 1, "7"));
        }
        other => panic!("{other:?}"),
    }
    assert_nothing(&store).await;

    set_term(&store, "07").await;
    match promotion::write(&store, &request(false, false, None)).await {
        Err(PromotionError::Term { expected, found }) => {
            assert_eq!((expected, found.as_str()), (TERM, "\"07\""));
        }
        other => panic!("{other:?}"),
    }
    set_term(&store, &TERM.to_string()).await;
    assert_nothing(&store).await;

    for (ack_fence, accept_loss, statement) in [
        (true, false, None),
        (false, true, None),
        (true, true, Some("  ")),
    ] {
        let result = promotion::write(&store, &request(ack_fence, accept_loss, statement)).await;
        assert!(
            matches!(result, Err(PromotionError::NoStatement)),
            "{result:?}"
        );
        assert_nothing(&store).await;
    }

    let mut unknown = request(false, false, None);
    unknown.to_node_id = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0Z".to_string();
    assert!(matches!(
        promotion::write(&store, &unknown).await,
        Err(PromotionError::Node { .. })
    ));
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(NEW)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        promotion::write(&store, &request(false, false, None)).await,
        Err(PromotionError::Node { .. })
    ));
    assert_nothing(&store).await;
}

/// Section 7.3 step 5 when no node but the promoted one holds the primary
/// status: the write clears nothing, `params.from_node_id` is `null`, as for
/// `cluster.primary_moved`, and `--ack-fence` declares no node lost. Only
/// the promoted node's entry and the record are written; no lease and no
/// `lost_at` changes.
#[tokio::test]
async fn with_no_old_primary_from_node_id_is_null_and_ack_fence_declares_nobody() {
    let store = rebuilt().await;
    set_term(&store, &OLD_TERM.to_string()).await;
    set_node(&store, OLD, None, None).await;
    set_term(&store, &TERM.to_string()).await;
    assert_eq!(promotion::old_primary(&fold(&store).await, NEW), None);
    let leases_before = leases(&store).await;
    let before = log_head(&store).await;

    let written = promotion::write(&store, &request(true, false, Some(STATEMENT)))
        .await
        .unwrap();
    assert_eq!(written.from_node_id, None);
    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(kinds(&entries), [("node", NEW), ("audit", "1")]);
    assert_eq!(entries[0].6["cp_status"], "primary");
    assert_eq!(entries[0].6["lost_at"], Value::Null);
    assert_eq!(node(&store, NEW).await, (Some("primary".to_string()), None));
    for id in [OLD, OTHER] {
        assert_eq!(node(&store, id).await, (None, None), "{id}");
    }
    assert_eq!(leases(&store).await, leases_before);

    let records = promoted_records(&store).await;
    assert_eq!(
        records,
        [(
            written.audit_seq,
            expected_record(None, true, false, entries[0].0)
        )]
    );
    assert_eq!(
        records[0].1["params"],
        json!({
            "from_node_id": null, "to_node_id": NEW, "term": 7, "log_seq": 42,
            "ack_fence": true, "accept_loss": false,
        })
    );
}

/// Section 7.3 step 5 when more than one node other than the promoted one
/// holds the primary status: the write clears each, and the first by id is
/// the one `params.from_node_id` names and `--ack-fence` declares lost, as
/// `promotion::old_primary` gives it from the fold. `EARLY`, inserted last
/// but first by id, is that node: it gets `lost_at`, and the other holders,
/// the old primary's node among them, keep their leases and get no
/// `lost_at`.
#[tokio::test]
async fn with_several_holders_each_is_cleared_and_the_first_by_id_is_declared_lost() {
    let store = rebuilt().await;
    set_term(&store, &OLD_TERM.to_string()).await;
    let mut w = store.begin_write().await.unwrap();
    insert_node(&mut w.tx, EARLY, Some("primary"), &now_rfc3339()).await;
    w.commit().await.unwrap();
    set_node(&store, OTHER, Some("primary"), None).await;
    set_term(&store, &TERM.to_string()).await;
    assert_eq!(
        promotion::old_primary(&fold(&store).await, NEW),
        Some(EARLY)
    );
    let leases_before = leases(&store).await;
    let before = log_head(&store).await;

    let written = promotion::write(&store, &request(true, false, Some(STATEMENT)))
        .await
        .unwrap();
    assert_eq!(written.from_node_id.as_deref(), Some(EARLY));
    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(
        kinds(&entries),
        [
            ("node", EARLY),
            ("node", OLD),
            ("node", OTHER),
            ("node", NEW),
            ("node", EARLY),
            ("audit", "1"),
        ]
    );
    for entry in &entries[..3] {
        assert_eq!(entry.6["cp_status"], Value::Null, "{}", entry.5);
        assert_eq!(entry.6["lost_at"], Value::Null, "{}", entry.5);
    }
    assert_eq!(entries[3].6["cp_status"], "primary");
    let (cp_status, lost_at) = node(&store, EARLY).await;
    assert_eq!(cp_status, None);
    let lost_at = lost_at.expect("declared lost");
    assert_eq!(entries[4].6["lost_at"], json!(lost_at));
    assert_eq!(node(&store, OLD).await, (None, None));
    assert_eq!(node(&store, OTHER).await, (None, None));
    assert_eq!(node(&store, NEW).await, (Some("primary".to_string()), None));
    assert_eq!(leases(&store).await, leases_before, "no token raised");

    let records = promoted_records(&store).await;
    assert_eq!(
        records,
        [(
            written.audit_seq,
            expected_record(Some(EARLY), true, false, entries[3].0)
        )]
    );
    assert_eq!(
        records[0].1["params"],
        json!({
            "from_node_id": EARLY, "to_node_id": NEW, "term": 7, "log_seq": 42,
            "ack_fence": true, "accept_loss": false,
        })
    );
}

/// Section 7.3 step 5 and ADR 0202: `--ack-fence` for an old primary already
/// declared lost declares nothing again. Its `lost_at` stays, no token is
/// raised, and a lease it holds at the cap refuses nothing, since nothing is
/// raised: the in-write check agrees with `check_token_cap` on the fold. The
/// write only moves the status and records the promotion with
/// `ack_fence: true`.
#[tokio::test]
async fn ack_fence_on_an_old_primary_already_lost_declares_nothing_again() {
    let store = rebuilt().await;
    set_term(&store, &OLD_TERM.to_string()).await;
    set_node(&store, OLD, Some("primary"), Some(LOST_AT)).await;
    set_token(&store, OLD_EXPIRING, MAX_TOKEN).await;
    set_term(&store, &TERM.to_string()).await;
    assert_eq!(promotion::check_token_cap(&fold(&store).await, OLD), Ok(()));
    let leases_before = leases(&store).await;
    let before = log_head(&store).await;

    let written = promotion::write(&store, &request(true, false, Some(STATEMENT)))
        .await
        .unwrap();
    assert_eq!(written.from_node_id.as_deref(), Some(OLD));
    let entries = assert_one_write_at_term(&store, before, &written).await;
    assert_eq!(
        kinds(&entries),
        [("node", OLD), ("node", NEW), ("audit", "1")]
    );
    assert_eq!(entries[0].6["cp_status"], Value::Null);
    assert_eq!(entries[0].6["lost_at"], LOST_AT);
    assert_eq!(node(&store, OLD).await, (None, Some(LOST_AT.to_string())));
    assert_eq!(node(&store, OTHER).await, (None, None));
    assert_eq!(leases(&store).await, leases_before, "no token raised");
    assert_eq!(lease(&store, OLD_EXPIRING).await.2, MAX_TOKEN);
    assert_eq!(lease(&store, OLD_LOCAL).await.2, 3);

    let records = promoted_records(&store).await;
    assert_eq!(
        records,
        [(
            written.audit_seq,
            expected_record(Some(OLD), true, false, entries[1].0)
        )]
    );
    assert_eq!(
        records[0].1["params"],
        json!({
            "from_node_id": OLD, "to_node_id": NEW, "term": 7, "log_seq": 42,
            "ack_fence": true, "accept_loss": false,
        })
    );
}

/// A failure after the write has started, here the insert of the record
/// itself, the last insert of the write, drops the whole transaction: no
/// entry, no record, `cp_status` where it was, no `lost_at`, no lease
/// change. Once the failure is gone the same write goes through.
#[tokio::test]
async fn a_failure_after_the_write_started_leaves_nothing_written() {
    let store = rebuilt().await;
    sqlx::query(
        "CREATE TRIGGER refuse_record BEFORE INSERT ON audit_log \
         BEGIN SELECT RAISE(ABORT, 'refused by the test'); END",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let leases_before = leases(&store).await;
    let before = log_head(&store).await;

    let error = promotion::write(&store, &request(true, true, Some(STATEMENT)))
        .await
        .unwrap_err();
    let PromotionError::Failed(why) = &error else {
        panic!("{error}");
    };
    assert!(why.starts_with("cannot record cluster.promoted"), "{why}");
    assert!(why.contains("refused by the test"), "{why}");
    assert_eq!(log_head(&store).await, before, "no entry");
    assert_eq!(audit_count(&store).await, 0, "no record");
    assert_eq!(node(&store, OLD).await, (Some("primary".to_string()), None));
    assert_eq!(node(&store, NEW).await, (None, None));
    assert_eq!(node(&store, OTHER).await, (None, None));
    assert_eq!(leases(&store).await, leases_before);

    sqlx::query("DROP TRIGGER refuse_record")
        .execute(store.pool())
        .await
        .unwrap();
    let written = promotion::write(&store, &request(true, true, Some(STATEMENT)))
        .await
        .unwrap();
    assert_one_write_at_term(&store, before, &written).await;
}

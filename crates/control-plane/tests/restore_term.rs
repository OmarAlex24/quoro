//! The restored primary's term on its sessions (`backup-format.md` section
//! 8.2, "Term"; `node-agent-protocol.md` sections 5.18, 5.27, 7.4 and 8.8;
//! ADR 0261, ADR 0280; issues #838 and #862). A primary restored with `quoro
//! restore --accept-loss` that has sent no `lease_grant` yet starts again at
//! one above the `term` of a `stale_term`, or of a voter's `bad_message`
//! answering its `support_request`, instead of demoting itself. After its
//! first `lease_grant` it demotes as any primary does, and so does a restart
//! that would pass the term ceiling, 2^53 - 1. A plain restore demotes on
//! `stale_term`, and a voter's `bad_message` leaves it without that voter's
//! support.
//!
//! Each test restores a primary's database as `quoro restore` installs it,
//! with the `restore-pending` marker and `primary-node` naming the new host's
//! node, and starts a control plane on it. The new host's node then dials it
//! as a fake machine. No test reads a node agent's store: the start reads
//! none, as `quoro restore` already raised `meta.term` to the agent's term.

mod common;

use common::machine::{Machine, Node, enroll};
use common::{OWNER_EMAIL, OWNER_PASSWORD, Server, TestDir, start_with_patient_node_session};
use futures_util::StreamExt;
use quoro_control_plane::keys::SIGNING_KEY_FILE;
use quoro_control_plane::primary_status;
use quoro_control_plane::restore_pending::{self, Marker};
use quoro_control_plane::store::Store;
use quoro_proto::wire::{Heartbeat, MAX_TERM, NodeMessage};
use quoro_proto::{ErrorCode, ProtocolError};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// `D`, the longest `expiring` grant support covers.
const D: u64 = 15_000;

/// A restored primary and the new host's node, which `primary-node` named.
struct Restored {
    /// The control plane started on the restored database.
    server: Server,
    /// The new host's node.
    host: Node,
    /// The primary the backup came from. It keeps running, with no session,
    /// and writes only to its own database.
    _old: Server,
}

/// A primary with the new host's node and, for `--accept-loss`, a second
/// machine with the `node` role, which the first write removes. Its database
/// is copied as `quoro restore` installs it, with its signing key, the
/// `restore-pending` marker naming the copy's log head and `primary-node`
/// naming the new host's node; then a control plane starts on the copy.
async fn restored(accept_loss: bool) -> Restored {
    restored_with(accept_loss, false).await
}

/// [`restored`]. With `marker_left_behind`, an earlier process made the
/// first write, then its deletion of the marker failed: the control plane
/// that starts finds the marker already written.
async fn restored_with(accept_loss: bool, marker_left_behind: bool) -> Restored {
    let old = start_with_patient_node_session().await;
    let owner = old.owner().await;
    let host = enroll(&old, &owner, "new-host", &["node"]).await;
    if accept_loss {
        enroll(&old, &owner, "lost", &["node"]).await;
    }

    let dir = TestDir::new("quoro-test-restored-term");
    let keys = dir.join("keys");
    std::fs::create_dir_all(&keys).unwrap();
    std::fs::copy(
        old.data_dir.join("keys").join(SIGNING_KEY_FILE),
        keys.join(SIGNING_KEY_FILE),
    )
    .unwrap();
    let database = dir.join("quoro.db");
    let path = database.to_str().unwrap();
    assert!(!path.contains('\''), "{path}");
    sqlx::query(&format!("VACUUM INTO '{path}'"))
        .execute(old.state.store.pool())
        .await
        .unwrap();
    let store = Store::open(&database).await.unwrap();
    let head = {
        let mut conn = store.pool().acquire().await.unwrap();
        restore_pending::log_head(&mut conn).await.unwrap()
    };
    let marker = if accept_loss {
        Marker {
            head,
            accept_loss: true,
            statement: Some("no other machine with the node role survives".to_string()),
        }
    } else {
        Marker::plain(head)
    };
    std::fs::write(dir.join(restore_pending::MARKER_FILE), marker.to_bytes()).unwrap();
    std::fs::write(
        dir.join(primary_status::MARKER_FILE),
        format!("{}\n", host.node_id),
    )
    .unwrap();
    // The earlier process has its own store, so its exception for a higher
    // term is not this start's, and the start opens the database after it.
    let store = if marker_left_behind {
        store.pool().close().await;
        let earlier = Store::open(&database).await.unwrap();
        let pending = restore_pending::check(&earlier, &dir).await.unwrap();
        restore_pending::apply(&earlier, &dir, pending)
            .await
            .unwrap();
        earlier.pool().close().await;
        std::fs::write(dir.join(restore_pending::MARKER_FILE), marker.to_bytes()).unwrap();
        Store::open(&database).await.unwrap()
    } else {
        store
    };
    let mut config = (*old.state.config).clone();
    config.data_dir = dir.to_path_buf();
    let state = quoro_control_plane::AppState::open(
        store,
        config,
        Arc::new(quoro_control_plane::ReferenceAsDigest),
        Arc::clone(&old.state.data_key),
    )
    .await
    .unwrap();
    let server = common::serve_in(dir, state).await;
    assert_eq!(
        server.state.store.restarts_on_a_higher_term(),
        accept_loss && !marker_left_behind,
        "only the process of an --accept-loss first write starts again on a higher term"
    );
    assert!(!server.data_dir.join(restore_pending::MARKER_FILE).exists());
    Restored {
        server,
        host,
        _old: old,
    }
}

async fn meta(server: &Server, key: &str) -> Option<String> {
    sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
        .bind(key)
        .fetch_optional(server.state.store.pool())
        .await
        .unwrap()
}

/// `meta.term`, the restored primary's current term.
async fn term(server: &Server) -> u64 {
    meta(server, "term").await.unwrap().parse().unwrap()
}

/// Waits until `meta.term` reads `expected`: the restart writes it on its own
/// task.
async fn term_becomes(server: &Server, expected: u64) {
    timeout(Duration::from_secs(10), async {
        loop {
            if term(server).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("meta.term reads {expected} within 10 s"));
}

/// `meta.demoted_by_term` once it is recorded: the demotion writes it on its
/// own task.
async fn demoted_by_term(server: &Server) -> String {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(term) = meta(server, "demoted_by_term").await {
                return term;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("meta.demoted_by_term is recorded within 10 s")
}

fn error(code: ErrorCode, fatal: bool, term: Option<u64>) -> NodeMessage {
    NodeMessage::Error(ProtocolError {
        code,
        message: "a higher term exists".to_string(),
        fatal,
        term,
    })
}

/// A `stale_term` carrying the sender's `term` (section 5.18).
fn stale_term(term: u64) -> NodeMessage {
    error(ErrorCode::StaleTerm, true, Some(term))
}

/// The `bad_message` a voter that voted in `term` for another candidate
/// answers a `support_request` with (section 5.27).
fn voted_for_another(term: u64) -> NodeMessage {
    error(ErrorCode::BadMessage, false, Some(term))
}

/// The id and the `term` of the session's `welcome`.
fn welcome(machine: &Machine) -> (String, u64) {
    let welcome = machine
        .start
        .iter()
        .find(|frame| frame["type"] == "welcome")
        .unwrap();
    (
        welcome["id"].as_str().unwrap().to_string(),
        welcome["body"]["term"].as_u64().unwrap(),
    )
}

/// Reads frames until the session ends, and returns them.
async fn until_closed(machine: &mut Machine) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        match timeout(Duration::from_secs(10), machine.socket.next())
            .await
            .expect("the session closes within 10 s")
        {
            None | Some(Err(_) | Ok(WsMessage::Close(_))) => return frames,
            Some(Ok(WsMessage::Text(text))) => {
                frames.push(serde_json::from_str(text.as_str()).unwrap());
            }
            Some(Ok(_)) => {}
        }
    }
}

/// Whether one of `frames` is the fatal `not_primary` of a demotion.
fn not_primary(frames: &[Value]) -> bool {
    frames
        .iter()
        .any(|frame| frame["type"] == "error" && frame["body"]["code"] == "not_primary")
}

/// Asserts that the primary serves: not demoted, nothing recorded for a
/// demotion, and an authenticated API request answered.
async fn serves(r: &Restored) {
    assert!(!r.server.state.store.is_demoted());
    assert_eq!(meta(&r.server, "demoted_by_term").await, None);
    let owner = r.server.client();
    r.server.login(&owner, OWNER_EMAIL, OWNER_PASSWORD).await;
    let res = owner
        .get(r.server.url("/api/v1/nodes"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}

/// The restored primary's first term is above the restored log's, and the
/// new host's node gets it in its `welcome`. The node answers that `welcome`
/// with `stale_term` carrying a higher term, before the primary has sent any
/// `lease_grant`: the primary starts again at one above that term instead of
/// demoting itself. Only that session ends, with no `not_primary`, and the
/// node's next session gets the new term.
#[tokio::test]
async fn an_accept_loss_primary_starts_again_above_a_stale_term_before_any_lease_grant() {
    let r = restored(true).await;
    let first = term(&r.server).await;
    assert!(first >= 2, "the first term is above the restored log's, 1");
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (welcome_id, welcome_term) = welcome(&on_host);
    assert_eq!(welcome_term, first);

    on_host.send(stale_term(first + 4), Some(&welcome_id)).await;
    let frames = until_closed(&mut on_host).await;
    assert!(!not_primary(&frames), "{frames:?}");
    term_becomes(&r.server, first + 5).await;
    serves(&r).await;
    assert!(r.server.state.store.restarts_on_a_higher_term());

    let again = Machine::connect(&r.server, &r.host).await;
    assert_eq!(welcome(&again).1, first + 5);
}

/// A voter that voted in the restored primary's term for another candidate
/// answers its `support_request` with `bad_message` carrying that term. The
/// primary, restored with `--accept-loss` and with no `lease_grant` sent,
/// treats it like `stale_term`: it starts again at one above that term, and
/// its next `support_request` carries the new term. The refusal counts no
/// support.
#[tokio::test]
async fn an_accept_loss_primary_starts_again_above_a_voters_bad_message() {
    let r = restored(true).await;
    let first = term(&r.server).await;
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (_, request) = on_host.request().await;
    assert_eq!(request["body"]["term"], first, "{request}");

    on_host
        .send(voted_for_another(first), request["id"].as_str())
        .await;
    on_host.settle().await;
    term_becomes(&r.server, first + 1).await;
    assert!(!r.server.state.hub.authority().supports(D));
    serves(&r).await;

    let (_, next) = on_host.request().await;
    assert_eq!(next["body"]["term"], first + 1, "{next}");
}

/// Once the restored primary has sent a `lease_grant`, here the `local`
/// grant of a new volume's lease, a `stale_term` demotes it as any primary
/// (section 7.4): every session ends with `not_primary`, it records the
/// sender's term in `meta.demoted_by_term`, and `meta.term` stays. The
/// exception ends before the grant leaves: held at its send, before its bytes
/// go to the socket, the grant finds it already off.
#[tokio::test]
async fn after_its_first_lease_grant_an_accept_loss_primary_demotes_on_a_stale_term() {
    let r = restored(true).await;
    let first = term(&r.server).await;
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (welcome_id, _) = welcome(&on_host);
    let owner = r.server.client();
    r.server.login(&owner, OWNER_EMAIL, OWNER_PASSWORD).await;
    let project: Value = owner
        .post(r.server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = owner
        .post(r.server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project["id"], "name": "web", "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let app_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let res = owner
        .post(r.server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": IMAGE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    on_host.settle().await;
    on_host
        .send(
            NodeMessage::Heartbeat(Heartbeat {
                time: "2026-11-09T10:15:32Z".to_string(),
                busy: None,
            }),
            None,
        )
        .await;
    assert!(
        r.server.state.store.restarts_on_a_higher_term(),
        "no lease_grant went out before the volume's"
    );
    let hold = r.server.state.hub.hold_next_lease_grant(&r.host.node_id);
    let volume = tokio::spawn({
        let owner = owner.clone();
        let url = r.server.url(&format!("/api/v1/apps/{app_id}/volumes"));
        async move {
            owner
                .post(url)
                .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
                .send()
                .await
                .unwrap()
        }
    });
    timeout(Duration::from_secs(10), hold.built())
        .await
        .expect("the volume's lease_grant reaches its send within 10 s");
    assert!(
        !r.server.state.store.restarts_on_a_higher_term(),
        "the exception ends before the lease_grant's bytes go out"
    );
    hold.release();
    on_host.until("lease_grant").await;
    let res = volume.await.unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());

    on_host.send(stale_term(first + 4), Some(&welcome_id)).await;
    let frames = until_closed(&mut on_host).await;
    assert!(not_primary(&frames), "{frames:?}");
    assert_eq!(demoted_by_term(&r.server).await, (first + 4).to_string());
    assert!(r.server.state.store.is_demoted());
    assert_eq!(term(&r.server).await, first);
}

/// #862: a restart at one above an error's `term` that would pass the term
/// ceiling, 2^53 - 1, demotes the primary restored with `--accept-loss`
/// instead, for a `stale_term` and for a voter's `bad_message` alike. The
/// primary decides it when the error arrives and never tries the restart,
/// whose write refuses such a term only as a backstop. It records the
/// error's term, and `meta.term` stays.
///
/// Each half reads the demotion from the primary's state. The node ends the
/// session of a `stale_term` itself, so its frames are not read; a voter's
/// `bad_message` ends nothing by itself, so only the demotion closes that
/// session, with `not_primary`.
#[tokio::test]
async fn a_restart_past_the_term_ceiling_demotes_an_accept_loss_primary() {
    let r = restored(true).await;
    let first = term(&r.server).await;
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (welcome_id, _) = welcome(&on_host);
    on_host.send(stale_term(MAX_TERM), Some(&welcome_id)).await;
    assert_eq!(
        demoted_by_term(&r.server).await,
        MAX_TERM.to_string(),
        "a stale_term past the ceiling demotes the primary"
    );
    assert!(r.server.state.store.is_demoted());
    assert_eq!(term(&r.server).await, first);
    until_closed(&mut on_host).await;

    let r = restored(true).await;
    let first = term(&r.server).await;
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (_, request) = on_host.request().await;
    on_host
        .send(voted_for_another(MAX_TERM), request["id"].as_str())
        .await;
    assert_eq!(
        demoted_by_term(&r.server).await,
        MAX_TERM.to_string(),
        "a voter's bad_message past the ceiling demotes the primary"
    );
    assert!(r.server.state.store.is_demoted());
    assert_eq!(term(&r.server).await, first);
    let frames = until_closed(&mut on_host).await;
    assert!(not_primary(&frames), "{frames:?}");
}

/// node-agent-protocol.md section 8.8: the exception lives only in the
/// process that made the first write of an `--accept-loss` restore. Here an
/// earlier process made it, then its deletion of the marker failed; it may
/// have served and sent a `lease_grant` meanwhile. The start that finds the
/// marker already written does not get the exception back: a `stale_term`
/// demotes it as any primary (section 7.4), it records the sender's term,
/// and `meta.term` stays.
#[tokio::test]
async fn a_start_that_finds_the_accept_loss_first_write_made_demotes_on_a_stale_term() {
    let r = restored_with(true, true).await;
    let first = term(&r.server).await;
    assert!(first >= 2, "the earlier process's first write set the term");
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (welcome_id, welcome_term) = welcome(&on_host);
    assert_eq!(welcome_term, first);

    on_host.send(stale_term(first + 4), Some(&welcome_id)).await;
    assert_eq!(demoted_by_term(&r.server).await, (first + 4).to_string());
    assert!(r.server.state.store.is_demoted());
    assert_eq!(term(&r.server).await, first);
    until_closed(&mut on_host).await;
}

/// A plain restore has no exception: a voter's `bad_message` carrying its
/// term leaves the primary without that voter's support, at its term and
/// serving, and a `stale_term` demotes it (section 8.8).
#[tokio::test]
async fn a_plain_restore_lacks_a_voters_support_and_demotes_on_a_stale_term() {
    let r = restored(false).await;
    let first = term(&r.server).await;
    assert!(first >= 2, "the first term is above the restored log's, 1");
    let mut on_host = Machine::connect(&r.server, &r.host).await;
    let (welcome_id, welcome_term) = welcome(&on_host);
    assert_eq!(welcome_term, first);
    let (_, request) = on_host.request().await;
    on_host
        .send(voted_for_another(first), request["id"].as_str())
        .await;
    on_host.settle().await;
    assert!(!r.server.state.hub.authority().supports(D));
    assert_eq!(term(&r.server).await, first);
    serves(&r).await;

    on_host.send(stale_term(first + 4), Some(&welcome_id)).await;
    let frames = until_closed(&mut on_host).await;
    assert!(not_primary(&frames), "{frames:?}");
    assert_eq!(demoted_by_term(&r.server).await, (first + 4).to_string());
    assert_eq!(term(&r.server).await, first);
}

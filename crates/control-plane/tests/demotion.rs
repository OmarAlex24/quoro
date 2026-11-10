//! A primary that learns of a higher term demotes itself
//! (`node-agent-protocol.md` sections 7.4 and 8.8, ADR 0230, issue #728):
//! it records `meta.demoted_by_term`, ends every node session with a fatal
//! `not_primary`, answers every later request with `503` `not_primary`
//! before any authentication, and writes and sends nothing more.

mod common;

use axum::Router;
use axum::routing::{any, post};
use common::machine::{Machine, Node, enroll};
use common::{Server, start_with_patient_node_session};
use futures_util::StreamExt;
use quoro_control_plane::events::{self, NewEvent, Source};
use quoro_control_plane::store::DEMOTED_REFUSAL;
use quoro_proto::wire::{ControlMessage, NodeMessage};
use quoro_proto::{ErrorCode, ProtocolError};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

fn stale_term(term: Option<u64>) -> NodeMessage {
    NodeMessage::Error(ProtocolError {
        code: ErrorCode::StaleTerm,
        message: "the term is lower than the highest seen".to_string(),
        fatal: true,
        term,
    })
}

async fn meta(server: &Server, key: &str) -> Option<String> {
    sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
        .bind(key)
        .fetch_optional(server.state.store.pool())
        .await
        .unwrap()
}

/// Asserts the `503` `not_primary` problem with its request id.
async fn assert_not_primary(res: reqwest::Response, what: &str) {
    assert_eq!(res.status(), 503, "{what}");
    assert!(
        res.headers()
            .get("quoro-request-id")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|id| id.starts_with("req_")),
        "{what}: the request id"
    );
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (503, "not_primary"), "{what}");
}

/// Reads frames until the fatal `error`, which must be `not_primary`, and
/// then the end of the socket.
async fn ended_with_not_primary(machine: &mut Machine) {
    let error = machine.until("error").await;
    assert_eq!(error["body"]["code"], "not_primary", "{error}");
    assert_eq!(error["body"]["fatal"], true, "{error}");
    loop {
        match timeout(Duration::from_secs(10), machine.socket.next())
            .await
            .expect("the session closes within 10 s")
        {
            None | Some(Err(_) | Ok(WsMessage::Close(_))) => return,
            Some(Ok(_)) => {}
        }
    }
}

/// Dials the session upgrade with `credential` and returns the HTTP answer
/// that refused it.
async fn refused_upgrade(server: &Server, credential: &str) -> (u16, Value) {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    match tokio_tungstenite::client_async(request, stream).await {
        Ok(_) => panic!("the upgrade was accepted"),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            assert_eq!(content_type, "application/problem+json");
            assert!(response.headers().contains_key("quoro-request-id"));
            let body = response
                .body()
                .as_ref()
                .map(|bytes| serde_json::from_slice(bytes).unwrap())
                .unwrap_or(Value::Null);
            (response.status().as_u16(), body)
        }
        Err(e) => panic!("the upgrade failed otherwise: {e}"),
    }
}

/// Serves the harness's state the way `quoro serve` composes it: the API,
/// with an MCP route and a panel fallback merged in, all behind the
/// demotion guard. The MCP route and the panel are stand-ins here: this
/// crate cannot depend on `quoro-mcp` or the binary's panel. The binary's
/// own `serve::tests::the_composed_service_answers_not_primary_once_demoted`
/// serves the real composition, and the MCP crate tests the real `/mcp`
/// behind the guard.
async fn whole_service(server: &Server) -> String {
    let service = quoro_control_plane::demotion::guard(
        quoro_control_plane::router(server.state.clone())
            .merge(Router::new().route("/mcp", post(|| async { "mcp" })))
            .fallback(any(|| async { "panel" })),
        server.state.store.clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    base
}

#[tokio::test]
async fn a_stale_term_demotes_the_primary_and_ends_every_session() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    let whole = whole_service(&server).await;
    // Before the demotion the whole service answers.
    let client = reqwest::Client::new();
    let res = client
        .get(format!("{whole}/assets/app.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    // The node answers the session's `welcome` with `stale_term` term 5.
    let welcome = on_a
        .start
        .iter()
        .find(|frame| frame["type"] == "welcome")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    on_a.send(stale_term(Some(5)), Some(&welcome)).await;
    ended_with_not_primary(&mut on_a).await;
    ended_with_not_primary(&mut on_b).await;
    assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));
    assert!(server.state.store.is_demoted());

    // A new session upgrade is refused before authentication, with a
    // credential this primary knows and with one it never saw.
    for credential in [b.credential.as_str(), "qnc_unknown_to_this_primary"] {
        let (status, body) = refused_upgrade(&server, credential).await;
        assert_eq!(status, 503, "{body}");
        assert_eq!(body["code"], "not_primary", "{body}");
    }

    // Every API route, authenticated or not.
    assert_not_primary(
        owner.get(server.url("/api/v1/nodes")).send().await.unwrap(),
        "GET /api/v1/nodes",
    )
    .await;
    assert_not_primary(
        owner
            .post(server.url("/api/v1/projects"))
            .json(&json!({ "slug": "late", "name": "late" }))
            .send()
            .await
            .unwrap(),
        "POST /api/v1/projects",
    )
    .await;
    assert_not_primary(
        client
            .post(server.url("/api/v1/setup"))
            .json(&json!({ "email": "x@example.com", "name": "x", "password": "y" }))
            .send()
            .await
            .unwrap(),
        "POST /api/v1/setup",
    )
    .await;
    assert_not_primary(
        client
            .get(server.url("/api/v1/health"))
            .send()
            .await
            .unwrap(),
        "GET /api/v1/health",
    )
    .await;
    assert_not_primary(
        client
            .get(server.url("/api/v1/no-such-route"))
            .send()
            .await
            .unwrap(),
        "an unknown API route",
    )
    .await;

    // The whole service: MCP, the panel and its assets.
    assert_not_primary(
        client
            .post(format!("{whole}/mcp"))
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .send()
            .await
            .unwrap(),
        "POST /mcp",
    )
    .await;
    assert_not_primary(
        client
            .get(format!("{whole}/assets/app.js"))
            .send()
            .await
            .unwrap(),
        "a panel asset",
    )
    .await;
    assert_not_primary(
        client.get(format!("{whole}/")).send().await.unwrap(),
        "the panel root",
    )
    .await;
}

/// The newest `seq` of the change log and of the audit log, and the row
/// counts of tables the loops write.
async fn heads(server: &Server) -> Vec<i64> {
    let mut out = Vec::new();
    for sql in [
        "SELECT IFNULL(MAX(seq), 0) FROM change_log",
        "SELECT IFNULL(MAX(seq), 0) FROM audit_log",
        "SELECT COUNT(*) FROM events",
        "SELECT COUNT(*) FROM grants WHERE status = 'active'",
        "SELECT COUNT(*) FROM webhook_deliveries",
    ] {
        let value: i64 = sqlx::query_scalar(sql)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
        out.push(value);
    }
    out
}

async fn create_app(server: &Server, owner: &reqwest::Client) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "site", "name": "site" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let project: Value = res.json().await.unwrap();
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project["id"],
            "name": "web",
            "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    app["id"].as_str().unwrap().to_string()
}

async fn emit_app_event(server: &Server, app_id: &str) {
    let mut w = server.state.store.begin_write().await.unwrap();
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "app.created",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: app_id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({ "app_id": app_id }),
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
}

/// A receiver that counts the requests it gets.
async fn counting_receiver() -> (Arc<AtomicUsize>, String) {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let app = Router::new().fallback(any(move || {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            "ok"
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (count, format!("http://localhost:{port}"))
}

#[tokio::test]
async fn nothing_is_written_or_sent_after_the_demotion() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (received, url) = counting_receiver().await;
    let res = owner
        .post(server.url("/api/v1/webhooks"))
        .json(&json!({
            "name": "app-events",
            "url": url,
            "event_types": ["app.created"],
            "selector": { "all": true },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let webhook: Value = res.json().await.unwrap();
    let webhook_id = webhook["id"].as_str().unwrap().to_string();
    let app_id = create_app(&server, &owner).await;
    // The dispatcher delivers an event before the demotion; one that met the
    // cursor read on its way is lost, so the event is emitted until one
    // arrives.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while received.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the dispatcher never delivered"
        );
        emit_app_event(&server, &app_id).await;
        let waited = tokio::time::Instant::now() + Duration::from_secs(5);
        while received.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < waited {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    let event_id: String = sqlx::query_scalar(
        "SELECT event_id FROM webhook_deliveries WHERE webhook_id = ? ORDER BY created_at LIMIT 1",
    )
    .bind(&webhook_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    // Wait until the dispatcher passed every event and wrote every
    // delivery's outcome.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (pending, behind): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'pending'), \
             (SELECT MAX(seq) FROM events) - \
             (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'webhook_cursor')",
        )
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        if pending == 0 && behind <= 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a delivery stays pending"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Taken before the demotion: the demotion itself appends nothing to the
    // change log or the audit chain.
    let sent_before = received.load(Ordering::SeqCst);
    let before = heads(&server).await;
    server.state.hub.stale_term(Some(5)).await;

    // `begin_write` refuses, and opens no transaction.
    let refused = server
        .state
        .store
        .begin_write()
        .await
        .err()
        .expect("refused");
    assert!(refused.to_string().contains(DEMOTED_REFUSAL), "{refused}");

    // A retry that is due, as one a failed send left behind, written past
    // the gate as only a test can: the dispatcher must not send it.
    sqlx::query(
        "INSERT INTO webhook_deliveries (id, webhook_id, event_id, attempt, status, \
         response_code, error, next_attempt_at, created_at, test) \
         VALUES (?, ?, ?, 2, 'pending', NULL, NULL, ?, ?, 0)",
    )
    .bind(quoro_types::new_id(quoro_types::Prefix::WebhookDelivery))
    .bind(&webhook_id)
    .bind(&event_id)
    .bind(quoro_control_plane::store::rfc3339_in(-60))
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();

    // The loops' passes, run now rather than waited for, write nothing.
    let state = &server.state;
    assert!(
        quoro_control_plane::service::grants::expire_due(state)
            .await
            .is_err()
    );
    assert!(
        quoro_control_plane::service::approvals::expire_due(state)
            .await
            .is_err()
    );
    // Nothing is due, so the backup pass only reads: whatever it answers,
    // the heads below show it wrote nothing.
    let _ =
        quoro_control_plane::backups::dispatch::run(state, time::OffsetDateTime::now_utc()).await;
    assert!(
        quoro_control_plane::session::serving::recompute(&state.store)
            .await
            .is_err()
    );
    quoro_control_plane::vulns::findings::run_once(state).await;
    // The dispatcher ticks every second: three ticks pass.
    tokio::time::sleep(Duration::from_millis(3_200)).await;

    let mut after = heads(&server).await;
    // The retry row this test wrote is the only new row.
    *after.last_mut().unwrap() -= 1;
    assert_eq!(after, before, "nothing is written after the demotion");
    assert_eq!(
        received.load(Ordering::SeqCst),
        sent_before,
        "the dispatcher sends nothing after the demotion"
    );
    let retry: (String, i64) = sqlx::query_as(
        "SELECT status, attempt FROM webhook_deliveries WHERE webhook_id = ? AND attempt = 2",
    )
    .bind(&webhook_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(retry, ("pending".to_string(), 2));
}

/// Section 8.8 sets no condition on the error's `term`, and the contract
/// gives no rule for a `stale_term` without a usable one: the primary
/// records one above its own term and demotes all the same.
#[tokio::test]
async fn a_stale_term_without_a_usable_term_records_one_above_the_current_term() {
    for (meta_term, node_term, recorded) in [
        (None, None, "2"),
        (None, Some(1), "2"),
        (Some("3"), Some(3), "4"),
        (Some("3"), Some(2), "4"),
        (Some("3"), Some(4), "4"),
    ] {
        let server = start_with_patient_node_session().await;
        if let Some(term) = meta_term {
            sqlx::query("UPDATE meta SET value = ? WHERE key = 'term'")
                .bind(term)
                .execute(server.state.store.pool())
                .await
                .unwrap();
        }
        server.state.hub.stale_term(node_term).await;
        assert_eq!(
            meta(&server, "demoted_by_term").await.as_deref(),
            Some(recorded),
            "meta.term {meta_term:?}, stale_term {node_term:?}"
        );
        assert!(server.state.store.is_demoted());
    }
}

#[tokio::test]
async fn a_second_stale_term_changes_nothing() {
    let server = start_with_patient_node_session().await;
    server.state.hub.stale_term(Some(5)).await;
    server.state.hub.stale_term(Some(9)).await;
    assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));

    // Two at once: one of them wins, and it stays.
    let server = start_with_patient_node_session().await;
    tokio::join!(
        server.state.hub.stale_term(Some(7)),
        server.state.hub.stale_term(Some(8)),
    );
    let first = meta(&server, "demoted_by_term").await.unwrap();
    assert!(first == "7" || first == "8", "{first}");
    server.state.hub.stale_term(Some(9)).await;
    assert_eq!(meta(&server, "demoted_by_term").await.unwrap(), first);
}

/// A write queued on the writer gate while the demotion holds it never
/// commits after the `meta` write: the flag is set before the gate is
/// released.
#[tokio::test]
async fn a_write_queued_on_the_gate_does_not_commit_after_the_demotion() {
    let server = start_with_patient_node_session().await;
    let store = server.state.store.clone();
    let hold = store.hold_next_demotion();
    let hub = server.state.hub.clone();
    let demoting = tokio::spawn(async move { hub.stale_term(Some(5)).await });
    hold.reached().await;
    assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));

    let queued = {
        let store = store.clone();
        tokio::spawn(async move {
            let mut w = store.begin_write().await?;
            sqlx::query("INSERT INTO meta (key, value) VALUES ('queued_write', 'committed')")
                .execute(&mut *w.tx)
                .await?;
            w.commit().await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !queued.is_finished(),
        "the queued write waits on the gate the demotion holds"
    );
    hold.release();
    demoting.await.unwrap();
    let refused = queued
        .await
        .unwrap()
        .expect_err("the queued write is refused");
    assert!(refused.to_string().contains(DEMOTED_REFUSAL), "{refused}");
    assert_eq!(meta(&server, "queued_write").await, None);
}

/// A `meta` write that fails still demotes this process: the flag is set,
/// every session ends with `not_primary`, every request gets `503`, and
/// nothing is written. Only a restart would come back as a primary, because
/// nothing records the demotion; the primary logs that at `error`.
#[tokio::test]
async fn a_failed_meta_write_still_demotes_the_running_process() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a: Node = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    sqlx::query(
        "CREATE TRIGGER refuse_demotion BEFORE INSERT ON meta \
         WHEN NEW.key = 'demoted_by_term' BEGIN SELECT RAISE(ABORT, 'refused by the test'); END",
    )
    .execute(server.state.store.pool())
    .await
    .unwrap();

    on_a.send(stale_term(Some(5)), None).await;
    ended_with_not_primary(&mut on_a).await;
    assert!(server.state.store.is_demoted());
    assert_eq!(meta(&server, "demoted_by_term").await, None);
    assert_eq!(server.state.store.demoted_by_term().await.unwrap(), None);
    assert!(server.state.store.begin_write().await.is_err());
    assert_not_primary(
        owner.get(server.url("/api/v1/nodes")).send().await.unwrap(),
        "GET /api/v1/nodes",
    )
    .await;
    let (status, body) = refused_upgrade(&server, &a.credential).await;
    assert_eq!((status, &body["code"]), (503, &json!("not_primary")));

    // A later stale_term that fails to record again changes nothing.
    server.state.hub.stale_term(Some(6)).await;
    assert_eq!(meta(&server, "demoted_by_term").await, None);

    // Once the write can go through, the next stale_term records it, past
    // the refusal every other write gets, with the term the first attempt
    // chose: the retry task, five seconds away, would record the same one.
    sqlx::query("DROP TRIGGER refuse_demotion")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    server.state.hub.stale_term(Some(7)).await;
    assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));
    assert_eq!(
        server
            .state
            .store
            .demoted_by_term()
            .await
            .unwrap()
            .as_deref(),
        Some("5")
    );
    assert!(server.state.store.begin_write().await.is_err());
    // Recorded once: the recorded term stays.
    server.state.hub.stale_term(Some(9)).await;
    assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));
}

/// No later `stale_term` is to be expected after a demotion: every session
/// ends and the guard answers `503`. A failed `meta` write is retried by a
/// task on its own until it is recorded, with the first attempt's term.
#[tokio::test]
async fn a_failed_meta_write_is_retried_until_it_is_recorded() {
    let server = start_with_patient_node_session().await;
    let store = server.state.store.clone();
    store.set_demotion_retry_for_test(Duration::from_millis(50));
    sqlx::query(
        "CREATE TRIGGER refuse_demotion BEFORE INSERT ON meta \
         WHEN NEW.key = 'demoted_by_term' BEGIN SELECT RAISE(ABORT, 'refused by the test'); END",
    )
    .execute(store.pool())
    .await
    .unwrap();
    server.state.hub.stale_term(Some(5)).await;
    assert!(store.is_demoted());
    // The retries keep failing while the write is refused.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(meta(&server, "demoted_by_term").await, None);

    sqlx::query("DROP TRIGGER refuse_demotion")
        .execute(store.pool())
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        while meta(&server, "demoted_by_term").await.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the retry records the demotion without another stale_term");
    assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));
    assert!(store.begin_write().await.is_err());
}

/// A `meta.term` that is not a number cannot rule the term: the error's own
/// term is recorded. Without one there is no term to record, so nothing is
/// recorded, the process is demoted all the same, and a later `stale_term`
/// with a term records it.
#[tokio::test]
async fn a_bad_meta_term_records_the_errors_term_or_nothing() {
    for node_term in [Some(5), None] {
        let server = start_with_patient_node_session().await;
        sqlx::query("UPDATE meta SET value = 'bad' WHERE key = 'term'")
            .execute(server.state.store.pool())
            .await
            .unwrap();
        server.state.hub.stale_term(node_term).await;
        assert!(server.state.store.is_demoted(), "{node_term:?}");
        assert!(server.state.store.begin_write().await.is_err());
        match node_term {
            Some(_) => {
                assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("5"));
            }
            None => {
                assert_eq!(meta(&server, "demoted_by_term").await, None);
                server.state.hub.stale_term(Some(8)).await;
                assert_eq!(meta(&server, "demoted_by_term").await.as_deref(), Some("8"));
            }
        }
    }
}

/// From the demotion on a session sends `not_primary` and nothing else:
/// frames queued for the node before its session task ran again are
/// dropped, though they were queued while the session still lived.
#[tokio::test]
async fn a_frame_queued_at_the_demotion_never_reaches_the_node() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    on_a.settle().await;

    // This runtime has one thread: nothing else runs between the demotion
    // and the queued frames, so they wait together for the session task.
    server.state.hub.stale_term(Some(5)).await;
    for _ in 0..8 {
        assert!(
            server
                .state
                .hub
                .queue_control_for_test(&a.node_id, &ControlMessage::Ping),
            "the session is still registered"
        );
    }

    let mut frames = Vec::new();
    loop {
        match timeout(Duration::from_secs(10), on_a.socket.next())
            .await
            .expect("the session closes within 10 s")
        {
            None | Some(Err(_) | Ok(WsMessage::Close(_))) => break,
            Some(Ok(WsMessage::Text(text))) => {
                frames.push(serde_json::from_str::<Value>(text.as_str()).unwrap());
            }
            Some(Ok(_)) => {}
        }
    }
    // A frame the session sent before the demotion may still be on the
    // wire; none of the queued ones is, and nothing follows `not_primary`.
    assert!(
        frames.iter().all(|frame| frame["type"] != "ping"),
        "a queued frame reached the node: {frames:?}"
    );
    let last = frames.last().expect("not_primary");
    assert_eq!(last["type"], "error", "{frames:?}");
    assert_eq!(last["body"]["code"], "not_primary", "{frames:?}");
    assert_eq!(last["body"]["fatal"], true, "{frames:?}");
}

/// The harness serves the guard as `quoro serve` does: before a demotion
/// every route answers as usual.
#[tokio::test]
async fn the_guard_lets_everything_through_before_a_demotion() {
    let server = start_with_patient_node_session().await;
    let res = reqwest::get(server.url("/api/v1/health")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert!(!server.state.store.is_demoted());
    assert!(server.state.store.begin_write().await.is_ok());
}

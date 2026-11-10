//! Voter support, the primary's side (`node-agent-protocol.md` sections 4.2
//! rule 7, 5.27, 5.28, 6.12, 8.8 and 8.10; ADR 0169, ADR 0170, ADR 0263;
//! issue #686), over real HTTP and WebSocket with fake machines that answer
//! as the node agent does: each acknowledges every `desired_state` and keeps
//! its `spec.votes.version`, and the test decides which `support_request` it
//! answers and how. Support is measured on a manual `CLOCK_BOOTTIME`, and no
//! test here sets the authority's support by hand.

mod common;

use common::{Server, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::support::{BootClock, ManualBootClock};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Heartbeat, Kvm as WireKvm, NodeMessage, StatusReport,
    VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorCode, LeaseId, LeaseRenew, ProtocolError};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// `D`, and the `S` and `L` every request carries.
const D: u64 = 15_000;

/// `S - M_S`: how long after `p_send` a support counts.
const WINDOW: Duration = Duration::from_millis(13_500);

struct Node {
    node_id: String,
    credential: String,
}

async fn enroll(server: &Server, owner: &reqwest::Client, name: &str, roles: &[&str]) -> Node {
    let token: Value = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token["token"].as_str().unwrap(),
            "name": name,
            "public_key": quoro_proto::sign::public_key_to_base64(
                &quoro_proto::sign::signing_key_from_seed(&[3u8; 32]).verifying_key()
            ),
            "roles": roles,
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "enroll: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    Node {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

/// A manual `CLOCK_BOOTTIME` for `server`'s support, set before any session.
fn manual_clock(server: &Server) -> Arc<ManualBootClock> {
    let clock = Arc::new(ManualBootClock::default());
    server
        .state
        .hub
        .set_support_clock(Arc::clone(&clock) as Arc<dyn BootClock>);
    clock
}

fn supports(server: &Server, duration_ms: u64) -> bool {
    server.state.hub.authority().supports(duration_ms)
}

/// The `seq` of `node_id`'s create entry: its enrollment.
async fn create_seq(server: &Server, node_id: &str) -> u64 {
    let seq: i64 = sqlx::query_scalar(
        "SELECT seq FROM change_log WHERE entity_type = 'node' AND entity_id = ? AND op = 'create'",
    )
    .bind(node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    u64::try_from(seq).unwrap()
}

/// A fresh 26-character suffix, so every frame of a fake machine has its own
/// id.
fn ulid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "01JC9Z6Q5W8N3K4T7R2M{:06}",
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

fn hello(node_id: &str) -> NodeMessage {
    NodeMessage::Hello(wire::Hello {
        node_id: node_id.to_string(),
        agent_version: "test-agent".to_string(),
        engine_version: "test-engine".to_string(),
        boot_id: "boot-1".to_string(),
        stored_rev: 0,
        applied_rev: 0,
        log_seq: 0,
        log_hash: "0".repeat(64),
        leases: Vec::new(),
        mesh_public_key: None,
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        kvm: WireKvm {
            available: true,
            kernel: "6.9.0".to_string(),
            nested: Some(true),
        },
        dropped_events: 0,
        volume_snapshot: Some(wire::SnapshotMethod::Reflink),
        custody_public_key: None,
    })
}

fn ok_result(detail: Option<String>) -> NodeMessage {
    NodeMessage::CommandResult(CommandResult {
        status: CommandStatus::Ok,
        detail,
        error: None,
        lines: None,
        metrics: None,
    })
}

/// The `support_grant` that echoes `request` at `version`.
fn echo(request: &Value, version: u64) -> wire::SupportGrant {
    let body = &request["body"];
    wire::SupportGrant {
        term: body["term"].as_u64().unwrap(),
        support_ms: body["support_ms"].as_u64().unwrap(),
        max_lease_ms: body["max_lease_ms"].as_u64().unwrap(),
        voters_version: version,
    }
}

/// A fake machine on a live session.
struct Machine {
    socket: Socket,
    /// `primary_node_id` of the session's `welcome`.
    primary: String,
    /// `spec` of the newest `desired_state`.
    spec: Value,
    /// Whether it acknowledges each `desired_state`, as the agent does.
    acks: bool,
    /// The `desired_state` frames it has not acknowledged, while it does
    /// not.
    unacked: Vec<Value>,
    /// The `support_request` frames received and not taken yet, oldest
    /// first.
    requests: VecDeque<(Instant, Value)>,
}

impl Machine {
    /// Dials, sends `hello`, and reads up to the first `desired_state`.
    async fn connect(server: &Server, node: &Node) -> Self {
        Self::connect_acking(server, node, true).await
    }

    async fn connect_acking(server: &Server, node: &Node, acks: bool) -> Self {
        let url = format!("ws://{}/api/v1/nodes/session", server.address());
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", node.credential).parse().unwrap(),
        );
        let stream = TcpStream::connect(server.address()).await.unwrap();
        let (socket, response) = tokio_tungstenite::client_async(request, stream)
            .await
            .expect("websocket upgrade");
        assert_eq!(response.status(), 101);
        let mut machine = Self {
            socket,
            primary: String::new(),
            spec: Value::Null,
            acks,
            unacked: Vec::new(),
            requests: VecDeque::new(),
        };
        machine.send(hello(&node.node_id), None).await;
        let welcome = machine.until("welcome").await;
        machine.primary = welcome["body"]["primary_node_id"]
            .as_str()
            .unwrap()
            .to_string();
        machine.until("desired_state").await;
        machine
    }

    async fn send(&mut self, message: NodeMessage, re: Option<&str>) -> String {
        let id = format!("msg_{}", ulid_like());
        let text = message.to_frame(&id, re).to_text();
        self.socket
            .send(WsMessage::Text(text.into()))
            .await
            .unwrap();
        id
    }

    /// Reads one frame and handles it as the agent would: a `desired_state`
    /// is acknowledged and its spec kept, a `support_request` is kept for
    /// the test.
    async fn next(&mut self) -> Value {
        let frame = loop {
            let message = timeout(Duration::from_secs(30), self.socket.next())
                .await
                .expect("a frame within 30 s")
                .expect("the socket is open")
                .expect("a valid websocket frame");
            match message {
                WsMessage::Text(text) => {
                    break serde_json::from_str::<Value>(text.as_str()).unwrap();
                }
                WsMessage::Close(_) => panic!("the socket closed"),
                _ => {}
            }
        };
        match frame["type"].as_str() {
            Some("desired_state") => {
                self.spec = frame["body"]["spec"].clone();
                if self.acks {
                    self.ack(&frame).await;
                } else {
                    self.unacked.push(frame.clone());
                }
            }
            Some("support_request") => self.requests.push_back((Instant::now(), frame.clone())),
            _ => {}
        }
        frame
    }

    async fn ack(&mut self, desired_state: &Value) {
        let rev = desired_state["body"]["rev"].as_u64().unwrap();
        self.send(
            ok_result(Some(format!("stored rev {rev}"))),
            desired_state["id"].as_str(),
        )
        .await;
    }

    /// Acknowledges every `desired_state` held back, and acknowledges the
    /// next ones as they come.
    async fn start_acking(&mut self) {
        self.acks = true;
        for frame in std::mem::take(&mut self.unacked) {
            self.ack(&frame).await;
        }
    }

    /// Reads frames until one of `kind`, and returns it.
    async fn until(&mut self, kind: &str) -> Value {
        loop {
            let frame = self.next().await;
            if frame["type"] == kind {
                return frame;
            }
        }
    }

    /// Every frame up to the `pong`s of two pings in a row: the session
    /// handles frames in order, so whatever the frames sent before them
    /// caused is in the result.
    async fn settle(&mut self) -> Vec<Value> {
        let mut frames = Vec::new();
        for _ in 0..2 {
            let id = self.send(NodeMessage::Ping, None).await;
            loop {
                let frame = self.next().await;
                if frame["type"] == "pong" && frame["re"] == id.as_str() {
                    break;
                }
                frames.push(frame);
            }
        }
        frames
    }

    /// The oldest `support_request` not taken yet, waiting for one.
    async fn request(&mut self) -> (Instant, Value) {
        loop {
            if let Some(request) = self.requests.pop_front() {
                return request;
            }
            self.next().await;
        }
    }

    /// The `spec.votes.version` of the newest `desired_state`.
    fn version(&self) -> u64 {
        self.spec["votes"]["version"].as_u64().expect("spec.votes")
    }

    /// Answers `request` as the agent does: the request's term, `S` and `L`,
    /// with the version of the newest `desired_state`. Waits until the
    /// primary has handled it.
    async fn grant(&mut self, request: &Value) {
        let grant = echo(request, self.version());
        self.answer(grant, request["id"].as_str()).await;
    }

    /// Sends `grant` with `re`, and waits until the primary has handled it.
    async fn answer(&mut self, grant: wire::SupportGrant, re: Option<&str>) {
        self.send(NodeMessage::SupportGrant(grant), re).await;
        self.settle().await;
    }

    /// Takes the next `support_request` and answers it.
    async fn support(&mut self) {
        let (_, request) = self.request().await;
        self.grant(&request).await;
    }
}

async fn cluster_votes(server: &Server, owner: &reqwest::Client) -> Value {
    let cluster: Value = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    cluster["votes"].clone()
}

/// Sections 6.12 and 8.10: `spec.votes` goes to every voter and names every
/// voter, oldest enrolled first, with port 51821. `spec.witness` goes to a
/// machine with the `witness` role, and neither to an edge-only machine. The
/// version is the `seq` of the entry that last changed the set, so it moves
/// with an enrollment of a voter and a removal, and not with an enrollment
/// of an edge-only machine or a node's own updates. A change of the set
/// rebuilds every voter's desired state.
#[tokio::test]
async fn spec_votes_name_every_voter_and_move_only_with_the_voter_set() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    assert_eq!(
        on_a.spec["votes"],
        json!({
            "version": create_seq(&server, &a.node_id).await,
            "port": 51821,
            "voters": [{ "node_id": a.node_id, "roles": ["node"], "mesh_address": null }],
        })
    );
    assert!(on_a.spec.get("witness").is_none(), "{}", on_a.spec);

    // A witness-only machine joins: every voter's desired state names it.
    let w = enroll(&server, &owner, "witness-w", &["witness"]).await;
    let joined = create_seq(&server, &w.node_id).await;
    while on_a.spec["votes"]["voters"].as_array().unwrap().len() < 2 {
        on_a.until("desired_state").await;
    }
    let votes = json!({
        "version": joined,
        "port": 51821,
        "voters": [
            { "node_id": a.node_id, "roles": ["node"], "mesh_address": null },
            { "node_id": w.node_id, "roles": ["witness"], "mesh_address": null },
        ],
    });
    assert_eq!(on_a.spec["votes"], votes);
    let mut on_w = Machine::connect(&server, &w).await;
    assert_eq!(on_w.spec["votes"], votes);
    assert_eq!(on_w.spec["witness"], json!({}));
    // The same voters, in the same order, as `Cluster.votes`.
    let ids = |voters: &Value| -> Vec<Value> {
        voters
            .as_array()
            .unwrap()
            .iter()
            .map(|voter| voter["node_id"].clone())
            .collect()
    };
    assert_eq!(
        ids(&votes["voters"]),
        ids(&cluster_votes(&server, &owner).await["voters"])
    );

    // An edge-only machine is no voter, and neither its enrollment nor any
    // node's own updates (its session's start and end) move the version.
    let e = enroll(&server, &owner, "edge-e", &["edge"]).await;
    let on_e = Machine::connect(&server, &e).await;
    assert!(on_e.spec.get("votes").is_none(), "{}", on_e.spec);
    assert!(on_e.spec.get("witness").is_none(), "{}", on_e.spec);
    drop(on_e);
    on_w.settle().await;
    drop(on_w);
    let on_w = Machine::connect(&server, &w).await;
    assert_eq!(on_w.spec["votes"], votes, "a reconnect changes nothing");
    for frame in on_a.settle().await {
        if frame["type"] == "desired_state" {
            assert_eq!(frame["body"]["spec"]["votes"], votes, "{frame}");
        }
    }

    // A removal moves the version to its own entry, and the voter left gets
    // a desired state without it.
    drop(on_w);
    let mut w_tx = server.state.store.begin_write().await.unwrap();
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&w.node_id)
        .execute(&mut *w_tx.tx)
        .await
        .unwrap();
    let removed = changelog::append(
        &mut w_tx.tx,
        &Actor::system("test"),
        "node",
        &w.node_id,
        Op::Update,
        json!({ "id": w.node_id, "status": "removed" }),
    )
    .await
    .unwrap();
    w_tx.commit().await.unwrap();
    while on_a.spec["votes"]["voters"].as_array().unwrap().len() != 1 {
        on_a.until("desired_state").await;
    }
    assert_eq!(on_a.version(), u64::try_from(removed).unwrap());
}

/// Sections 4.2 rule 7 and 5.27: a voter's first `support_request` follows
/// its acknowledgement of a `desired_state` with `spec.votes`, then one goes
/// out every `S / 3`, with the cluster term, the `welcome`'s
/// `primary_node_id`, `S` = `L` = 15 000 ms and the current version. A
/// change of the version reaches it with the next acknowledged desired
/// state. A machine that is not a voter gets none.
#[tokio::test]
async fn support_requests_follow_the_acknowledged_desired_state_every_five_seconds() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let e = enroll(&server, &owner, "edge-e", &["edge"]).await;
    let mut on_e = Machine::connect(&server, &e).await;
    let mut on_a = Machine::connect_acking(&server, &a, false).await;
    on_a.settle().await;
    assert!(
        on_a.requests.is_empty(),
        "no request before the acknowledgement"
    );

    on_a.start_acking().await;
    let (first_at, first) = on_a.request().await;
    assert_eq!(
        first["body"],
        json!({
            "term": 1,
            "primary_node_id": on_a.primary,
            "support_ms": 15_000,
            "max_lease_ms": 15_000,
            "voters_version": on_a.version(),
        })
    );
    let (second_at, second) = on_a.request().await;
    // `S / 3` is 5 s. The gap is read on the test's side of a loaded host,
    // so it is checked loosely: well after the first request, well before `S`.
    let gap = second_at - first_at;
    assert!(
        gap >= Duration::from_secs(4) && gap < Duration::from_secs(12),
        "every S / 3: {gap:?}"
    );
    assert_eq!(second["body"], first["body"]);
    assert_ne!(second["id"], first["id"]);

    let frames = on_e.settle().await;
    assert!(
        !frames
            .iter()
            .any(|frame| frame["type"] == "support_request"),
        "no request to an edge-only machine: {frames:?}"
    );
    assert!(on_e.requests.is_empty());

    // A second voter enrolls: the next request after A's acknowledgement of
    // the desired state that says so carries the new version.
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let moved = create_seq(&server, &b.node_id).await;
    while on_a.version() != moved {
        on_a.until("desired_state").await;
    }
    on_a.requests.clear();
    let (_, request) = on_a.request().await;
    assert_eq!(request["body"]["voters_version"], moved);
}

/// Section 8.8, "Majority": more than half of the whole voter set, connected
/// or not. With three voters, of which the third never connects, one
/// answer is not a majority and two are. A majority with `L` = `D` does not
/// support a longer grant. `Cluster.votes.ha_ready` follows it, and the
/// support ends at `p_send + S - M_S`.
#[tokio::test]
async fn two_of_three_voters_are_a_majority_with_the_third_disconnected() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let c = enroll(&server, &owner, "node-c", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    assert!(!supports(&server, D));

    on_a.support().await;
    assert!(!supports(&server, D), "1 of 3");
    let votes = cluster_votes(&server, &owner).await;
    assert_eq!(
        (votes["count"].as_i64(), votes["ha_ready"].as_bool()),
        (Some(3), Some(false))
    );

    on_b.support().await;
    assert!(supports(&server, D), "2 of 3");
    assert!(!supports(&server, D + 1), "L < d");
    let votes = cluster_votes(&server, &owner).await;
    assert_eq!(votes["ha_ready"], true, "{votes}");
    let disconnected = votes["voters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|voter| voter["node_id"] == c.node_id.as_str())
        .unwrap();
    assert_eq!(disconnected["connected"], false);

    clock.advance(WINDOW);
    assert!(!supports(&server, D), "the window ran out");
    assert_eq!(cluster_votes(&server, &owner).await["ha_ready"], false);
}

/// Section 8.8, "Majority": with two voters both must support, and
/// `ha_ready` stays false below three voters.
#[tokio::test]
async fn two_voters_need_both() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    on_a.support().await;
    assert!(!supports(&server, D), "1 of 2");
    on_b.support().await;
    assert!(supports(&server, D), "2 of 2");
    assert_eq!(cluster_votes(&server, &owner).await["ha_ready"], false);
}

/// Sections 5.28 and 8.10 rule 3: an answer counts only when it pairs by
/// `re` with an outstanding request, echoes its term, `S` and `L`, and
/// carries the primary's voter-set version. A mis-echoed answer uses up its
/// request, and so does a refusal.
#[tokio::test]
async fn an_answer_counts_only_when_it_pairs_echoes_and_carries_the_current_version() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let version = on_a.version();

    let (_, request) = on_a.request().await;
    let re = request["id"].as_str().unwrap().to_string();
    on_a.answer(echo(&request, version), None).await;
    assert!(!supports(&server, D), "no re");
    on_a.answer(
        echo(&request, version),
        Some("msg_01JC9Z6Q5W8N3K4T7R2M9999XX"),
    )
    .await;
    assert!(!supports(&server, D), "an re that pairs with nothing");
    let mut wrong = echo(&request, version);
    wrong.term += 1;
    on_a.answer(wrong, Some(&re)).await;
    assert!(!supports(&server, D), "another term");
    on_a.answer(echo(&request, version), Some(&re)).await;
    assert!(!supports(&server, D), "the request was used up");

    let (_, request) = on_a.request().await;
    on_a.answer(echo(&request, version - 1), request["id"].as_str())
        .await;
    assert!(!supports(&server, D), "another voter-set version");

    // Section 5.27: a voter that refuses a request with `bad_message` grants
    // nothing, and the refusal answers the request.
    let (_, request) = on_a.request().await;
    let re = request["id"].as_str().unwrap().to_string();
    on_a.send(
        NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "primary_node_id is not the welcome's".to_string(),
            fatal: false,
            term: None,
        }),
        Some(&re),
    )
    .await;
    on_a.settle().await;
    assert!(!supports(&server, D), "a refusal");
    on_a.answer(echo(&request, version), Some(&re)).await;
    assert!(!supports(&server, D), "the refusal answered the request");

    let (_, request) = on_a.request().await;
    on_a.grant(&request).await;
    assert!(supports(&server, D), "1 of 1");
}

/// Section 8.8 and ADR 0263: support is measured from `p_send` on
/// `CLOCK_BOOTTIME`, never from the answer: an answer that arrives 10 s
/// after its request counts until 13.5 s after the request.
#[tokio::test]
async fn support_runs_out_at_p_send_plus_s_minus_m_s() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let (_, request) = on_a.request().await;
    clock.advance(Duration::from_secs(10));
    on_a.grant(&request).await;
    assert!(supports(&server, D));
    clock.advance(Duration::from_millis(3_499));
    assert!(supports(&server, D), "1 ms before the window ends");
    clock.advance(Duration::from_millis(1));
    assert!(!supports(&server, D), "at p_send + S - M_S");
}

/// Section 8.8, "Terms" and "Restarts": after a `stale_term` from any node
/// the primary holds no support, whatever its voters answer, until it
/// restarts. A restarted primary holds none either until its voters answer
/// its own requests.
#[tokio::test]
async fn a_stale_term_stops_all_support_until_the_primary_restarts() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    on_a.support().await;
    assert!(supports(&server, D));

    on_a.send(
        NodeMessage::Error(ProtocolError {
            code: ErrorCode::StaleTerm,
            message: "the term is lower than the highest seen".to_string(),
            fatal: true,
            term: Some(2),
        }),
        None,
    )
    .await;
    on_a.settle().await;
    assert!(!supports(&server, D), "stale_term stops all support");
    on_a.support().await;
    assert!(!supports(&server, D), "and no later answer counts");

    drop(on_a);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status: String = sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
            .bind(&a.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
        if status == "disconnected" {
            break;
        }
        assert!(Instant::now() < deadline, "the session did not end");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let restarted =
        common::restart_with_resolver(&server, Arc::new(quoro_control_plane::ReferenceAsDigest))
            .await;
    manual_clock(&restarted);
    assert!(!supports(&restarted, D), "no support survives a restart");
    let mut on_a = Machine::connect(&restarted, &a).await;
    on_a.settle().await;
    assert!(!supports(&restarted, D), "not before the voter answers");
    on_a.support().await;
    assert!(supports(&restarted, D));
}

fn renew(lease_id: &str, token: u64) -> NodeMessage {
    NodeMessage::LeaseRenew(LeaseRenew {
        lease_id: LeaseId(lease_id.to_string()),
        fencing_token: token,
    })
}

fn status_report(applied_rev: u64, volumes: Vec<VolumeStatus>) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev,
        vms: Vec::new(),
        routes: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        disk_free_gb: 90.0,
        builds: Vec::new(),
        edge_backends: Vec::new(),
        backups: Vec::new(),
        images: Vec::new(),
        volumes,
        warm: Vec::new(),
        quarantines: Vec::new(),
    })
}

async fn heartbeat(machine: &mut Machine) {
    machine
        .send(
            NodeMessage::Heartbeat(Heartbeat {
                time: "2026-11-09T10:15:32Z".to_string(),
                busy: None,
            }),
            None,
        )
        .await;
}

async fn project(server: &Server, owner: &reqwest::Client) -> String {
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    project["id"].as_str().unwrap().to_string()
}

async fn app(server: &Server, owner: &reqwest::Client, project_id: &str, name: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": name, "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn set_policy(server: &Server, app_id: &str, policy: &str) {
    sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
        .bind(policy)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

async fn create_volume(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap()
}

/// #603 under real support: with three voters, the holder and a second one
/// answering and the third never connected, the holder's renewal of its
/// `expiring` lease is answered. Once the voters stop answering and their
/// window runs out, the next renewal is not.
#[tokio::test]
async fn a_renewal_is_answered_while_two_of_three_voters_support_and_not_after() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    enroll(&server, &owner, "node-c", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    heartbeat(&mut on_a).await;

    // A stateful app on A, its lease made `expiring` for `D` in the row, as
    // a first `expiring` grant would leave it.
    let project_id = project(&server, &owner).await;
    let app_id = app(&server, &owner, &project_id, "web").await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": IMAGE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let grant = on_a.until("lease_grant").await;
    let lease_id = grant["body"]["lease_id"].as_str().unwrap().to_string();
    let token = grant["body"]["fencing_token"].as_u64().unwrap();
    on_a.settle().await;
    let rev: i64 = sqlx::query_scalar("SELECT desired_rev FROM nodes WHERE id = ?")
        .bind(&a.node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    on_a.send(
        status_report(
            u64::try_from(rev).unwrap(),
            vec![VolumeStatus {
                volume_id,
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
        None,
    )
    .await;
    on_a.settle().await;
    sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    on_a.support().await;
    on_b.support().await;
    assert!(supports(&server, D));
    let id = on_a.send(renew(&lease_id, token), None).await;
    let answer = on_a.until("lease_grant").await;
    assert_eq!(answer["re"], id.as_str());
    assert_eq!(answer["body"]["fencing_token"], token);
    assert_eq!(answer["body"]["mode"], "expiring");
    assert_eq!(answer["body"]["duration_ms"], 15_000);

    // The voters answer nothing more, and the window runs out.
    clock.advance(WINDOW);
    assert!(!supports(&server, D));
    on_a.send(renew(&lease_id, token), None).await;
    let frames = on_a.settle().await;
    assert!(
        !frames
            .iter()
            .any(|frame| frame["type"] == "lease_grant" || frame["type"] == "lease_revoke"),
        "{frames:?}"
    );
}

/// #604 under real support: a `warm` app's new token over a released lease
/// row that already reads `expiring` for `D` and carries no unsettled mark
/// goes out `expiring` for `D` while two of three voters support the
/// primary, and is held, writing nothing, while none does.
#[tokio::test]
async fn a_warm_apps_new_token_goes_out_expiring_under_real_support() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    enroll(&server, &owner, "node-c", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let app_id = app(&server, &owner, &project_id, "web").await;

    // A first volume while the policy is `none`, deleted and its revoke
    // answered; then the row reads as an `expiring` lease's whose gate has
    // passed, and the policy becomes `warm`.
    set_policy(&server, &app_id, "none").await;
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let first = on_a.until("lease_grant").await;
    let lease_id = first["body"]["lease_id"].as_str().unwrap().to_string();
    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let revoke = on_a.until("lease_revoke").await;
    on_a.send(ok_result(None), revoke["id"].as_str()).await;
    on_a.settle().await;
    sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    set_policy(&server, &app_id, "warm").await;
    let row = || {
        sqlx::query_as::<_, (String, Option<i64>, i64, i64)>(
            "SELECT mode, duration_ms, unsettled, fencing_token FROM leases WHERE id = ?",
        )
        .bind(&lease_id)
        .fetch_one(server.state.store.pool())
    };
    assert_eq!(
        row().await.unwrap(),
        ("expiring".to_string(), Some(15_000), 0, 1)
    );

    // No voter answered yet: the grant is held and nothing is written.
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 409);
    let body: Value = res.json().await.unwrap();
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .contains("cannot be granted yet"),
        "{body}"
    );
    assert_eq!(row().await.unwrap().3, 1, "nothing was written");

    on_a.support().await;
    on_b.support().await;
    assert!(supports(&server, D));
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let grant = on_a.until("lease_grant").await;
    assert_eq!(grant["body"]["lease_id"], lease_id.as_str());
    assert_eq!(grant["body"]["fencing_token"], 2);
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);
    assert_eq!(
        row().await.unwrap(),
        ("expiring".to_string(), Some(15_000), 0, 2)
    );
}

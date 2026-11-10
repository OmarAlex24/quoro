//! A fake machine on a live node session that answers as the node agent
//! does: it acknowledges every `desired_state` and keeps its
//! `spec.votes.version`, keeps each `support_request` for the test to
//! answer (`node-agent-protocol.md` sections 5.27, 5.28 and 8.8), and, once
//! it follows the change log, checks and acknowledges each `log_entries`
//! (section 7.1). Shared by the support and held-grant tests; support is
//! measured on a manual `CLOCK_BOOTTIME` ([`manual_clock`]).

use super::Server;
use super::log::check_entries;
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::support::{BootClock, ManualBootClock};
use quoro_proto::wire::{self, Capacity as WireCapacity, Kvm as WireKvm, LogAck, NodeMessage};
use quoro_proto::{CommandResult, CommandStatus};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

pub type Socket = WebSocketStream<TcpStream>;

pub struct Node {
    pub node_id: String,
    pub credential: String,
}

pub async fn enroll(server: &Server, owner: &reqwest::Client, name: &str, roles: &[&str]) -> Node {
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
pub fn manual_clock(server: &Server) -> Arc<ManualBootClock> {
    let clock = Arc::new(ManualBootClock::default());
    server
        .state
        .hub
        .set_support_clock(Arc::clone(&clock) as Arc<dyn BootClock>);
    clock
}

/// A fresh 26-character suffix, so every frame of a fake machine has its own
/// id.
pub fn ulid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "01JC9Z6Q5W8N3K4T7R2M{:06}",
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

/// The `hello` of an empty machine: no lease, at the start of the log.
pub fn hello_body(node_id: &str) -> wire::Hello {
    wire::Hello {
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
    }
}

pub fn hello(node_id: &str) -> NodeMessage {
    NodeMessage::Hello(hello_body(node_id))
}

pub fn ok_result(detail: Option<String>) -> NodeMessage {
    NodeMessage::CommandResult(CommandResult {
        status: CommandStatus::Ok,
        detail,
        error: None,
        lines: None,
        metrics: None,
    })
}

/// The `support_grant` that echoes `request` at `version`.
pub fn echo(request: &Value, version: u64) -> wire::SupportGrant {
    let body = &request["body"];
    wire::SupportGrant {
        term: body["term"].as_u64().unwrap(),
        support_ms: body["support_ms"].as_u64().unwrap(),
        max_lease_ms: body["max_lease_ms"].as_u64().unwrap(),
        voters_version: version,
    }
}

/// A fake machine on a live session.
pub struct Machine {
    pub socket: Socket,
    /// `primary_node_id` of the session's `welcome`.
    pub primary: String,
    /// `spec` of the newest `desired_state`.
    pub spec: Value,
    /// Whether it acknowledges each `desired_state`, as the agent does.
    pub acks: bool,
    /// The `desired_state` frames it has not acknowledged, while it does
    /// not.
    pub unacked: Vec<Value>,
    /// The `support_request` frames received and not taken yet, oldest
    /// first.
    pub requests: VecDeque<(Instant, Value)>,
    /// Whether it checks and acknowledges each `log_entries` as it comes.
    pub follows: bool,
    /// The `log_entries` frames it has not acknowledged, while it does not
    /// follow.
    pub unfollowed: Vec<Value>,
    /// Its copy of the log: the newest `seq` and `hash` it holds.
    pub log: (u64, String),
}

impl Machine {
    /// Dials, sends `hello`, and reads up to the first `desired_state`.
    pub async fn connect(server: &Server, node: &Node) -> Self {
        Self::connect_acking(server, node, true).await
    }

    pub async fn connect_acking(server: &Server, node: &Node, acks: bool) -> Self {
        Self::connect_with(server, node, hello_body(&node.node_id), acks, false).await
    }

    /// Dials, sends `hello`, and reads up to the first `desired_state`.
    /// `follows` makes it acknowledge each `log_entries` from the position
    /// its `hello` gives.
    pub async fn connect_with(
        server: &Server,
        node: &Node,
        hello: wire::Hello,
        acks: bool,
        follows: bool,
    ) -> Self {
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
            follows,
            unfollowed: Vec::new(),
            log: (hello.log_seq, hello.log_hash.clone()),
        };
        machine.send(NodeMessage::Hello(hello), None).await;
        let welcome = machine.until("welcome").await;
        machine.primary = welcome["body"]["primary_node_id"]
            .as_str()
            .unwrap()
            .to_string();
        machine.until("desired_state").await;
        machine
    }

    pub async fn send(&mut self, message: NodeMessage, re: Option<&str>) -> String {
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
    /// the test, and a `log_entries` is checked and acknowledged while the
    /// machine follows.
    pub async fn next(&mut self) -> Value {
        let frame = self.read().await;
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
            Some("log_entries") => {
                if self.follows {
                    self.take_entries(&frame).await;
                } else {
                    self.unfollowed.push(frame.clone());
                }
            }
            _ => {}
        }
        frame
    }

    /// Reads one text frame, within 30 s.
    async fn read(&mut self) -> Value {
        loop {
            let message = timeout(Duration::from_secs(30), self.socket.next())
                .await
                .expect("a frame within 30 s")
                .expect("the socket is open")
                .expect("a valid websocket frame");
            match message {
                WsMessage::Text(text) => {
                    return serde_json::from_str::<Value>(text.as_str()).unwrap();
                }
                WsMessage::Close(_) => panic!("the socket closed"),
                _ => {}
            }
        }
    }

    pub async fn ack(&mut self, desired_state: &Value) {
        let rev = desired_state["body"]["rev"].as_u64().unwrap();
        self.send(
            ok_result(Some(format!("stored rev {rev}"))),
            desired_state["id"].as_str(),
        )
        .await;
    }

    /// Acknowledges every `desired_state` held back, and acknowledges the
    /// next ones as they come.
    pub async fn start_acking(&mut self) {
        self.acks = true;
        for frame in std::mem::take(&mut self.unacked) {
            self.ack(&frame).await;
        }
    }

    /// Checks one `log_entries` as a node does, from its copy's position,
    /// and acknowledges its last entry (section 7.1).
    async fn take_entries(&mut self, frame: &Value) {
        let body = &frame["body"];
        assert_eq!(body["from_seq"], self.log.0 + 1, "{frame}");
        let (seq, hash) = check_entries(body, &self.log.1);
        self.log = (seq, hash.clone());
        self.send(NodeMessage::LogAck(LogAck { seq, hash }), None)
            .await;
    }

    /// Checks and acknowledges every `log_entries` held back, and each next
    /// one as it comes.
    pub async fn start_following(&mut self) {
        self.follows = true;
        for frame in std::mem::take(&mut self.unfollowed) {
            self.take_entries(&frame).await;
        }
    }

    /// Reads frames until one of `kind`, and returns it.
    pub async fn until(&mut self, kind: &str) -> Value {
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
    pub async fn settle(&mut self) -> Vec<Value> {
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
    pub async fn request(&mut self) -> (Instant, Value) {
        loop {
            if let Some(request) = self.requests.pop_front() {
                return request;
            }
            self.next().await;
        }
    }

    /// The `spec.votes.version` of the newest `desired_state`.
    pub fn version(&self) -> u64 {
        self.spec["votes"]["version"].as_u64().expect("spec.votes")
    }

    /// Answers `request` as the agent does: the request's term, `S` and `L`,
    /// with the version of the newest `desired_state`. Waits until the
    /// primary has handled it.
    pub async fn grant(&mut self, request: &Value) {
        let grant = echo(request, self.version());
        self.answer(grant, request["id"].as_str()).await;
    }

    /// Sends `grant` with `re`, and waits until the primary has handled it.
    pub async fn answer(&mut self, grant: wire::SupportGrant, re: Option<&str>) {
        self.send(NodeMessage::SupportGrant(grant), re).await;
        self.settle().await;
    }

    /// Takes the next `support_request` and answers it.
    pub async fn support(&mut self) {
        let (_, request) = self.request().await;
        self.grant(&request).await;
    }
}

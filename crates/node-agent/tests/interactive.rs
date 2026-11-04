// SPDX-License-Identifier: AGPL-3.0-only
//! The web terminal, the read-only console and transcripts, driven over a
//! duplex WebSocket against `FakeEngine`.
//!
//! Each test assembles a [`Session`] the way `run` does, but with the fake
//! engine's injection hooks in reach, so output, exits, resizes and VM deletes
//! are deterministic.

mod agent_common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use agent_common::{TempDir, facts, identity_files};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_engine::{
    ConsoleMode, Engine, FakeEngine, ImageDigest, ImageRef, InitConfig, MacAddr, NetAttach,
    SerialMode, StopMode, VmId, VmSpec, VsockAttach,
};
use quoro_node_agent::interactive::Interactive;
use quoro_node_agent::session::{Session, SessionEnd, SessionSetup};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::transcript::{TranscriptLimits, transcript_path};
use quoro_proto::sign;
use quoro_proto::wire::{
    ControlMessage, StreamAck, StreamClose, StreamInput, StreamOpen, WindowSize,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::time::Instant;
use tokio::io::DuplexStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, client_async};

/// The cluster term every test snapshot uses.
const TERM: u64 = 1;

struct Node {
    dir: TempDir,
    engine: Arc<FakeEngine>,
    store: NodeStore,
}

async fn node() -> Node {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    Node {
        dir,
        engine: Arc::new(FakeEngine::new()),
        store,
    }
}

impl Node {
    fn vm_spec(&self, vm_id: &str, vsock: bool) -> VmSpec {
        let digest = ImageDigest(format!("sha256:{}", "ab".repeat(32)));
        VmSpec {
            id: VmId(vm_id.to_owned()),
            vcpus: 1,
            memory_mb: 128,
            rootfs: digest,
            disk_gb: 1,
            volumes: Vec::new(),
            net: NetAttach {
                ip: std::net::Ipv4Addr::new(10, 42, 1, 10),
                gateway: std::net::Ipv4Addr::new(10, 42, 1, 1),
                mac: MacAddr([0x52, 0x55, 10, 42, 1, 10]),
                isolation_group: None,
                egress_only: false,
            },
            init: InitConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
            },
            console: ConsoleMode::Off,
            serial: SerialMode::Socket {
                path: self.dir.path().join(format!("{vm_id}.serial.sock")),
            },
            vsock: vsock.then(|| VsockAttach {
                cid: 3,
                socket: self.dir.path().join(format!("{vm_id}.vsock.sock")),
            }),
            work_disks: Vec::new(),
        }
    }

    async fn create_vm(&self, vm_id: &str, start: bool) -> VmId {
        let digest = ImageDigest(format!("sha256:{}", "ab".repeat(32)));
        self.engine
            .prepare_image(&ImageRef {
                reference: "busybox".to_owned(),
                digest,
            })
            .await
            .unwrap();
        let spec = self.vm_spec(vm_id, true);
        let id = spec.id.clone();
        self.engine.create(&spec).await.unwrap();
        if start {
            self.engine.start(&id).await.unwrap();
        }
        id
    }
}

type PrimaryFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Runs one session over a duplex WebSocket, with `primary` driving it.
async fn drive<F>(
    node: &Node,
    limits: Option<TranscriptLimits>,
    primary: F,
) -> (SessionEnd, NodeStore)
where
    F: FnOnce(WebSocketStream<DuplexStream>, Arc<FakeEngine>) -> PrimaryFuture + Send + 'static,
{
    let mut interactive = Interactive::new(Arc::clone(&node.engine), node.store.clone());
    if let Some(limits) = limits {
        interactive = interactive.with_transcript_limits(limits);
    }
    drive_session(node, Some(interactive), primary).await
}

/// Runs a session with no interactive handles attached.
async fn drive_bare<F>(node: &Node, primary: F) -> (SessionEnd, NodeStore)
where
    F: FnOnce(WebSocketStream<DuplexStream>, Arc<FakeEngine>) -> PrimaryFuture + Send + 'static,
{
    drive_session(node, None, primary).await
}

async fn drive_session<F>(
    node: &Node,
    interactive: Option<Interactive>,
    primary: F,
) -> (SessionEnd, NodeStore)
where
    F: FnOnce(WebSocketStream<DuplexStream>, Arc<FakeEngine>) -> PrimaryFuture + Send + 'static,
{
    let key = sign::signing_key_from_seed(&[4; 32]);
    let identity = identity_files(node.dir.path(), &key);
    let store = node.store.clone();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    });
    if let Some(interactive) = interactive {
        session = session.with_interactive(interactive);
    }

    let (client, server) = tokio::io::duplex(4 << 20);
    let engine = Arc::clone(&node.engine);
    let primary = tokio::spawn(async move {
        let ws = accept_async(server).await.unwrap();
        primary(ws, engine).await;
    });
    let (client_ws, _response) = client_async("ws://primary.test/session", client)
        .await
        .unwrap();
    let end = session
        .serve(client_ws, std::future::pending::<()>())
        .await
        .unwrap();
    primary.await.unwrap();
    (end, store)
}

async fn read_frame(ws: &mut WebSocketStream<DuplexStream>) -> serde_json::Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the node sent a frame in time")
            .expect("the socket is open")
            .expect("a valid frame");
        if let Message::Text(text) = message {
            return serde_json::from_str(text.as_str()).expect("a JSON frame");
        }
    }
}

async fn read_until(ws: &mut WebSocketStream<DuplexStream>, kind: &str) -> serde_json::Value {
    loop {
        let frame = read_frame(ws).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

async fn send(ws: &mut WebSocketStream<DuplexStream>, text: String) {
    ws.send(Message::Text(text.into())).await.unwrap();
}

async fn handshake(ws: &mut WebSocketStream<DuplexStream>) {
    use quoro_proto::wire::Welcome;
    let hello = read_frame(ws).await;
    assert_eq!(hello["type"], "hello");
    send(
        ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: TERM,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 500,
            session_timeout_ms: 5_000,
            primary_node_id: "nod_primary".to_owned(),
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    read_until(ws, "status_report").await;
}

fn open(stream_id: &str, kind: &str, params: serde_json::Value) -> String {
    ControlMessage::StreamOpen(StreamOpen {
        stream_id: stream_id.to_owned(),
        kind: kind.to_owned(),
        params,
    })
    .to_frame("msg_open", None)
    .to_text()
}

fn terminal(stream_id: &str, session_id: &str, vm_id: &str) -> String {
    open(
        stream_id,
        "terminal",
        json!({
            "session_id": session_id,
            "vm_id": vm_id,
            "rows": 24,
            "cols": 80,
            "argv": ["/bin/sh"],
        }),
    )
}

fn console(stream_id: &str, vm_id: &str) -> String {
    open(stream_id, "console", json!({ "vm_id": vm_id }))
}

fn input(stream_id: &str, seq: u64, data: Option<&[u8]>, resize: Option<(u16, u16)>) -> String {
    ControlMessage::StreamInput(StreamInput {
        stream_id: stream_id.to_owned(),
        seq,
        data: data.map(|bytes| BASE64.encode(bytes)),
        resize: resize.map(|(rows, cols)| WindowSize { rows, cols }),
        eof: None,
    })
    .to_frame("msg_input", None)
    .to_text()
}

fn ack(stream_id: &str, bytes: u64) -> String {
    ControlMessage::StreamAck(StreamAck {
        stream_id: stream_id.to_owned(),
        acked_bytes: bytes,
    })
    .to_frame("msg_ack", None)
    .to_text()
}

fn close(stream_id: &str) -> String {
    ControlMessage::StreamClose(StreamClose {
        stream_id: stream_id.to_owned(),
    })
    .to_frame("msg_close", None)
    .to_text()
}

fn data_of(frame: &serde_json::Value) -> Vec<u8> {
    BASE64
        .decode(frame["body"]["data"].as_str().expect("a data frame"))
        .expect("valid base64")
}

/// Reads `data` frames until the text appears, returning everything decoded.
async fn read_data_until(ws: &mut WebSocketStream<DuplexStream>, wanted: &[u8]) -> Vec<u8> {
    let mut all = Vec::new();
    loop {
        let frame = read_frame(ws).await;
        if frame["type"] == "stream_data" {
            all.extend(data_of(&frame));
            if all.windows(wanted.len()).any(|window| window == wanted) {
                return all;
            }
        }
    }
}

/// Fails when a `stream_data` frame arrives within `within`.
async fn expect_no_data(ws: &mut WebSocketStream<DuplexStream>, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        match tokio::time::timeout(remaining, read_frame(ws)).await {
            Err(_) => return,
            Ok(frame) => assert_ne!(frame["type"], "stream_data", "unexpected data: {frame}"),
        }
    }
}

#[tokio::test]
async fn without_interactive_handles_the_streams_are_not_available() {
    let node = node().await;
    node.create_vm("vm_bare", true).await;
    let (end, _store) = drive_bare(&node, |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_t", "trm_bare", "vm_bare")).await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["status"], "error");
            assert_eq!(answer["body"]["error"]["code"], "not_available");

            send(&mut ws, console("msg_c", "vm_bare")).await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["error"]["code"], "not_available");

            send(
                &mut ws,
                open("msg_r", "transcript", json!({"session_id": "trm_bare"})),
            )
            .await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["error"]["code"], "not_available");
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn a_session_end_still_finalises_the_transcript() {
    let node = node().await;
    let vm = node.create_vm("vm_drop", true).await;
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    let (end, store) = drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_drop", "trm_drop", "vm_drop")).await;
            wait_exec(&engine, &vm_primary).await;
            engine.push_exec_output(&vm_primary, 0, b"before the drop\n");
            let data = read_until(&mut ws, "stream_data").await;
            assert_eq!(data_of(&data), b"before the drop\n");
            // Returning drops the WebSocket: the node session ends without a
            // `stream_close`, so no `stream_end` can be sent.
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);

    let row = store.transcript("trm_drop").await.unwrap().unwrap();
    let path = transcript_path(&node.store, "trm_drop").unwrap();
    assert!(path.exists(), "the transcript file is kept");
    let (sha, bytes) = file_sha(&path);
    assert_eq!(row.sha256, sha, "the row was finalised on the session end");
    assert_eq!(row.size_bytes, bytes);
    assert!(row.ended_at.is_some());
}

/// Waits until the fake holds an exec session, before its output is injected.
async fn wait_exec(engine: &FakeEngine, vm: &VmId) {
    wait_for(|| (!engine.exec_requests(vm).is_empty()).then_some(())).await;
}

async fn wait_for<T>(mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(Instant::now() < deadline, "the condition never held");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn file_sha(path: &std::path::Path) -> (String, u64) {
    let bytes = std::fs::read(path).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    let hex = digest.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
        out
    });
    (hex, bytes.len() as u64)
}

#[tokio::test]
async fn a_terminal_relays_output_input_and_resizes_then_completes_with_its_transcript() {
    let node = node().await;
    let vm = node.create_vm("vm_term", true).await;
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    let (end, store) = drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_term", "trm_round", "vm_term")).await;
            wait_exec(&engine, &vm_primary).await;

            engine.push_exec_output(&vm_primary, 0, b"hello\n");
            let data = read_until(&mut ws, "stream_data").await;
            assert_eq!(data["body"]["stream_id"], "msg_term");
            assert_eq!(data["body"]["seq"], 1);
            assert_eq!(data_of(&data), b"hello\n");

            // The fake echoes what is written to the PTY.
            send(&mut ws, input("msg_term", 1, Some(b"ls\n"), None)).await;
            let echoed = read_data_until(&mut ws, b"ls\n").await;
            assert!(echoed.ends_with(b"ls\n"), "{echoed:?}");

            send(&mut ws, input("msg_term", 2, None, Some((50, 132)))).await;
            let resizes = wait_for(|| {
                let resizes = engine.exec_resizes(&vm_primary, 0);
                (!resizes.is_empty()).then_some(resizes)
            })
            .await;
            assert_eq!(resizes, vec![(50, 132)]);

            engine.exit_exec(&vm_primary, 0, 7);
            let end = read_until(&mut ws, "stream_end").await;
            assert_eq!(end["body"]["stream_id"], "msg_term");
            assert_eq!(end["body"]["reason"], "completed");
            assert_eq!(end["body"]["exit_code"], 7);
            assert!(
                end["body"].get("transcript_sha256").is_some(),
                "a terminal end always carries its transcript hash"
            );
            assert!(end["body"]["transcript_bytes"].as_u64().unwrap() > 0);
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);

    let requests = node.engine.exec_requests(&vm);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].argv, vec!["/bin/sh".to_owned()]);
    assert_eq!((requests[0].rows, requests[0].cols), (24, 80));

    let (sha, bytes) = file_sha(&transcript_path(&node.store, "trm_round").unwrap());
    let row = store.transcript("trm_round").await.unwrap().unwrap();
    assert_eq!(row.sha256, sha);
    assert_eq!(row.size_bytes, bytes);
    assert_eq!(row.vm_id, "vm_term");
    assert!(row.ended_at.is_some());
}

#[tokio::test]
async fn malformed_or_unavailable_opens_are_answered_with_the_right_code() {
    let node = node().await;
    node.create_vm("vm_stopped", false).await;
    let (end, _store) = drive(&node, None, |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;

            send(&mut ws, terminal("msg_t1", "trm_missing", "vm_missing")).await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["status"], "error");
            assert_eq!(answer["body"]["error"]["code"], "not_found");

            send(&mut ws, terminal("msg_t2", "trm_stopped", "vm_stopped")).await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["status"], "error");
            assert_eq!(answer["body"]["error"]["code"], "not_available");

            let mut bad = StreamOpen {
                stream_id: "msg_t3".to_owned(),
                kind: "terminal".to_owned(),
                params: json!({
                    "session_id": "trm_bad",
                    "vm_id": "vm_stopped",
                    "rows": 0,
                    "cols": 80,
                    "argv": ["/bin/sh"],
                }),
            };
            send(
                &mut ws,
                ControlMessage::StreamOpen(bad.clone())
                    .to_frame("msg_open", None)
                    .to_text(),
            )
            .await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["error"]["code"], "bad_message");
            bad.params = json!({ "vm_id": "vm_stopped" });
            send(
                &mut ws,
                ControlMessage::StreamOpen(bad)
                    .to_frame("msg_open", None)
                    .to_text(),
            )
            .await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["error"]["code"], "bad_message");

            send(&mut ws, console("msg_c1", "vm_missing")).await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["error"]["code"], "not_found");

            send(
                &mut ws,
                open("msg_tr1", "transcript", json!({"session_id": "trm_none"})),
            )
            .await;
            let answer = read_until(&mut ws, "command_result").await;
            assert_eq!(answer["body"]["error"]["code"], "not_found");
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn input_on_a_console_is_a_bad_message() {
    let node = node().await;
    node.create_vm("vm_console", true).await;
    let (end, _store) = drive(&node, None, |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, console("msg_console", "vm_console")).await;
            send(&mut ws, input("msg_console", 1, Some(b"x"), None)).await;
            let error = read_until(&mut ws, "error").await;
            assert_eq!(error["body"]["code"], "bad_message");
            assert_eq!(error["body"]["fatal"], false);
            assert_eq!(error["re"], "msg_input");
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn the_window_stops_a_console_and_resumes_on_an_ack() {
    let node = node().await;
    let vm = node.create_vm("vm_window", true).await;
    node.engine.push_serial(&vm, &vec![b'x'; 512 * 1024]);
    let (end, _store) = drive(&node, None, |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(
                &mut ws,
                open(
                    "msg_window",
                    "console",
                    json!({ "vm_id": "vm_window", "tail_bytes": 512 * 1024 }),
                ),
            )
            .await;

            let mut received = 0usize;
            let mut last_seq = 0u64;
            while received < 256 * 1024 {
                let frame = read_until(&mut ws, "stream_data").await;
                assert_eq!(frame["body"]["seq"], last_seq + 1, "seq is gap-free from 1");
                last_seq = frame["body"]["seq"].as_u64().unwrap();
                let data = data_of(&frame);
                assert!(data.len() <= 64 * 1024, "a frame holds at most 64 KiB");
                received += data.len();
            }
            assert_eq!(received, 256 * 1024);

            // The window is full: the console stops polling its source.
            expect_no_data(&mut ws, Duration::from_millis(300)).await;

            send(&mut ws, ack("msg_window", 256 * 1024)).await;
            let frame = read_until(&mut ws, "stream_data").await;
            assert_eq!(frame["body"]["seq"], last_seq + 1);
            assert_eq!(data_of(&frame).len(), 64 * 1024);

            send(&mut ws, close("msg_window")).await;
            let end = read_until(&mut ws, "stream_end").await;
            assert_eq!(end["body"]["reason"], "cancelled");
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn an_ack_beyond_the_bytes_sent_is_a_bad_message() {
    let node = node().await;
    node.create_vm("vm_ack", true).await;
    let (end, _store) = drive(&node, None, |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, console("msg_ack", "vm_ack")).await;
            send(&mut ws, ack("msg_ack", 10)).await;
            let error = read_until(&mut ws, "error").await;
            assert_eq!(error["body"]["code"], "bad_message");
            assert_eq!(error["body"]["fatal"], false);
            assert_eq!(error["re"], "msg_ack");
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn console_replays_fans_out_live_output_and_ends_when_the_vm_is_deleted() {
    let node = node().await;
    let vm = node.create_vm("vm_fan", true).await;
    node.engine.push_serial(&vm, b"boot\n");
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    let (end, _store) = drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, console("msg_a", "vm_fan")).await;
            send(&mut ws, console("msg_b", "vm_fan")).await;

            // Both viewers replay the ring from the start.
            for _ in 0..2 {
                let replay = read_data_until(&mut ws, b"boot\n").await;
                assert_eq!(replay, b"boot\n");
            }

            engine.push_serial(&vm_primary, b"live\n");
            for _ in 0..2 {
                let live = read_data_until(&mut ws, b"live\n").await;
                assert!(live.ends_with(b"live\n"));
            }

            // A console outlives a VM restart and shows the next boot.
            engine
                .stop(
                    &vm_primary,
                    StopMode::Graceful {
                        timeout: Duration::from_millis(20),
                    },
                )
                .await
                .unwrap();
            engine.start(&vm_primary).await.unwrap();
            engine.push_serial(&vm_primary, b"reboot\n");
            for _ in 0..2 {
                let reboot = read_data_until(&mut ws, b"reboot\n").await;
                assert!(reboot.ends_with(b"reboot\n"));
            }

            // Deleting the VM ends its serial subscribers.
            engine
                .stop(
                    &vm_primary,
                    StopMode::Graceful {
                        timeout: Duration::from_millis(20),
                    },
                )
                .await
                .unwrap();
            engine.delete(&vm_primary).await.unwrap();
            for _ in 0..2 {
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["reason"], "vm_stopped");
            }
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn closing_a_terminal_cancels_and_still_reports_the_transcript() {
    let node = node().await;
    let vm = node.create_vm("vm_close", true).await;
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    let (end, store) = drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_close", "trm_close", "vm_close")).await;
            wait_exec(&engine, &vm_primary).await;
            engine.push_exec_output(&vm_primary, 0, b"about to close\n");
            let data = read_until(&mut ws, "stream_data").await;
            assert_eq!(data_of(&data), b"about to close\n");

            send(&mut ws, close("msg_close")).await;
            let end = read_until(&mut ws, "stream_end").await;
            assert_eq!(end["body"]["reason"], "cancelled");
            assert!(!end["body"]["transcript_sha256"].is_null());
            wait_for(|| engine.exec_closed(&vm_primary, 0).then_some(())).await;
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);

    let row = store.transcript("trm_close").await.unwrap().unwrap();
    let (sha, bytes) = file_sha(&transcript_path(&node.store, "trm_close").unwrap());
    assert_eq!(row.sha256, sha);
    assert_eq!(row.size_bytes, bytes);
}

#[tokio::test]
async fn the_transcript_cap_ends_the_session_with_limit() {
    let node = node().await;
    let vm = node.create_vm("vm_limit", true).await;
    let limits = TranscriptLimits {
        max_bytes: 300,
        ..TranscriptLimits::default()
    };
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    let (end, store) = drive(&node, Some(limits), move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_limit", "trm_limit", "vm_limit")).await;
            wait_exec(&engine, &vm_primary).await;
            engine.push_exec_output(&vm_primary, 0, &[b'a'; 100]);
            let data = read_until(&mut ws, "stream_data").await;
            assert_eq!(data_of(&data).len(), 100);

            engine.push_exec_output(&vm_primary, 0, &[b'b'; 100]);
            let end = read_until(&mut ws, "stream_end").await;
            assert_eq!(end["body"]["reason"], "limit");
            assert!(!end["body"]["transcript_sha256"].is_null());
            assert!(end["body"]["transcript_bytes"].as_u64().unwrap() <= 300);
            wait_for(|| engine.exec_closed(&vm_primary, 0).then_some(())).await;
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);

    let path = transcript_path(&node.store, "trm_limit").unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() <= 300);
    let row = store.transcript("trm_limit").await.unwrap().unwrap();
    let (sha, _) = file_sha(&path);
    assert_eq!(row.sha256, sha);
    assert!(!std::fs::read_to_string(&path).unwrap().contains("bbb"));
}

#[tokio::test]
async fn a_transcript_stream_returns_the_file_from_its_first_byte() {
    let node = node().await;
    let vm = node.create_vm("vm_replay", true).await;
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_t", "trm_replay", "vm_replay")).await;
            wait_exec(&engine, &vm_primary).await;
            engine.push_exec_output(&vm_primary, 0, b"recorded output\n");
            read_until(&mut ws, "stream_data").await;
            engine.exit_exec(&vm_primary, 0, 0);
            let end = read_until(&mut ws, "stream_end").await;
            assert_eq!(end["body"]["reason"], "completed");
        })
    })
    .await;

    let path = transcript_path(&node.store, "trm_replay").unwrap();
    let expected = std::fs::read(&path).unwrap();
    let (end, _store) = drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(
                &mut ws,
                open("msg_tr", "transcript", json!({"session_id": "trm_replay"})),
            )
            .await;
            let mut all = Vec::new();
            let mut seq = 0u64;
            loop {
                let frame = read_frame(&mut ws).await;
                match frame["type"].as_str() {
                    Some("stream_data") => {
                        seq += 1;
                        assert_eq!(frame["body"]["seq"], seq);
                        all.extend(data_of(&frame));
                    }
                    Some("stream_end") => {
                        assert_eq!(frame["body"]["reason"], "completed");
                        break;
                    }
                    _ => {}
                }
            }
            assert_eq!(all, expected);
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
}

#[tokio::test]
async fn a_guest_that_stops_reading_input_ends_the_stream_not_the_session() {
    let node = node().await;
    let vm = node.create_vm("vm_stuck", true).await;
    let engine = Arc::clone(&node.engine);
    let vm_primary = vm.clone();
    let (end, store) = drive(&node, None, move |mut ws, _engine| {
        Box::pin(async move {
            handshake(&mut ws).await;
            send(&mut ws, terminal("msg_stuck", "trm_stuck", "vm_stuck")).await;
            wait_exec(&engine, &vm_primary).await;
            // The fake echoes input as output. With no ack the window fills,
            // the relay stops reading, the echo buffer fills and the guest
            // stops taking input; the session loop must keep serving.
            let chunk = vec![b'x'; 60 * 1024];
            for seq in 1..=30 {
                send(&mut ws, input("msg_stuck", seq, Some(&chunk), None)).await;
            }
            let end = read_until(&mut ws, "stream_end").await;
            assert_eq!(end["body"]["stream_id"], "msg_stuck");
            assert_eq!(end["body"]["reason"], "error");
            assert!(!end["body"]["transcript_sha256"].is_null());
            wait_for(|| engine.exec_closed(&vm_primary, 0).then_some(())).await;
        })
    })
    .await;
    assert_eq!(end, SessionEnd::Reconnect);
    let row = store.transcript("trm_stuck").await.unwrap().unwrap();
    assert!(row.ended_at.is_some());
}

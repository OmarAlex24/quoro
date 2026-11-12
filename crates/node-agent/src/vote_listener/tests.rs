// SPDX-License-Identifier: AGPL-3.0-only
//! The vote listener end to end (section 8.9), over real sockets. Loopback
//! addresses 127.0.0.x stand in for mesh addresses: this machine is
//! 127.0.0.1, node A 127.0.0.2 and the witness 127.0.0.3. Linux routes all of
//! 127.0.0.0/8 to the loopback interface, so a client can bind any of them as
//! its source.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quoro_proto::wire::{DesiredState, Spec, SpecMesh, SpecVoter, SpecVotes, VoteRequest};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{
    BODY_LIMIT, Caller, LOCAL_WARN_EVERY, MAX_CONNECTIONS, MAX_TERM, VoteListener,
    VoteListenerOptions, WarnLimit, connection_caller, listen_address, local_caller_uid,
};
use crate::lease::{HostClock, ManualHostClock};
use crate::mesh::ListenerLink;
use crate::reconcile::Snapshot;
use crate::store::NodeStore;
use crate::util;
use crate::vote::StoredVote;

const ME: &str = "nod_me";
const CLUSTER: &str = "cls_1";
const OWN: &str = "127.0.0.1";
const NODE_A: &str = "127.0.0.2";
const WITNESS: &str = "127.0.0.3";
const STRANGER: &str = "127.0.0.4";

struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// This machine's listener address on `port`.
fn own(port: u16) -> SocketAddr {
    format!("{OWN}:{port}").parse().unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind((OWN, 0))
        .expect("probe")
        .local_addr()
        .expect("probe address")
        .port()
}

fn spec_votes(port: u16) -> SpecVotes {
    let voter = |node_id: &str, roles: &[&str], address: &str| SpecVoter {
        node_id: node_id.to_owned(),
        roles: roles.iter().map(|role| (*role).to_owned()).collect(),
        mesh_address: Some(address.to_owned()),
    };
    SpecVotes {
        version: 7,
        port,
        voters: vec![
            voter(ME, &["node"], OWN),
            voter("nod_a", &["node"], NODE_A),
            voter("nod_w", &["witness"], WITNESS),
        ],
        listen: Some(true),
    }
}

/// [`spec_votes`] with `listen` set to `listen`.
fn spec_votes_listen(port: u16, listen: Option<bool>) -> SpecVotes {
    SpecVotes {
        listen,
        ..spec_votes(port)
    }
}

fn snapshot(votes: Option<SpecVotes>) -> Option<Snapshot> {
    let state = DesiredState {
        rev: 1,
        issued_at: String::new(),
        term: 1,
        spec: Spec {
            mesh: Some(SpecMesh {
                address: format!("{OWN}/8"),
                listen_port: 51820,
                peers: Vec::new(),
                allow: Vec::new(),
            }),
            votes,
            ..Spec::default()
        },
        signature: String::new(),
    };
    Some(Snapshot { rev: 1, state })
}

struct Harness {
    dir: TempDir,
    store: NodeStore,
    desired: watch::Sender<Option<Snapshot>>,
    /// What the mesh manager would publish: the listener address its loaded
    /// firewall covers.
    firewall: watch::Sender<Option<SocketAddr>>,
    /// What the listener reports it holds ([`ListenerLink::bound`]).
    bound: watch::Receiver<Option<SocketAddr>>,
    listener: VoteListener,
    task: JoinHandle<()>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

impl Harness {
    async fn start(port: u16) -> Self {
        Self::start_with(port, VoteListenerOptions::quick()).await
    }

    async fn start_with(port: u16, options: VoteListenerOptions) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "quoro-vote-listener-{}",
            util::hex(&util::random_bytes::<8>())
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = NodeStore::open(&dir).await.unwrap();
        let clock: Arc<dyn HostClock> = Arc::new(ManualHostClock::new(
            Some("boot-a"),
            Duration::from_secs(1000),
        ));
        let (desired, desired_rx) = watch::channel(snapshot(Some(spec_votes(port))));
        let (firewall, firewall_rx) = watch::channel(Some(own(port)));
        let (link, bound) = ListenerLink::detached(firewall_rx);
        let listener = VoteListener::new(
            store.clone(),
            ME.to_owned(),
            CLUSTER.to_owned(),
            clock,
            desired_rx,
            link,
            options,
        );
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(listener.clone().run(async move {
            let _ = stopped.await;
        }));
        let harness = Self {
            dir: TempDir(dir),
            store,
            desired,
            firewall,
            bound,
            listener,
            task,
            _stop: stop,
        };
        assert!(listening(port, true).await, "the listener came up");
        harness
    }

    async fn vote(&self, port: u16, from: &str, body: &Value) -> Reply {
        post(port, from, "/v1/vote", &body.to_string())
            .await
            .expect("an answer")
    }
}

/// Whether `OWN:port` accepts connections (`up`) or refuses them, within a
/// few seconds.
async fn listening(port: u16, up: bool) -> bool {
    let addr: SocketAddr = format!("{OWN}:{port}").parse().unwrap();
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() == up {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

#[derive(Debug)]
struct Reply {
    status: u16,
    body: Value,
}

/// Sends one HTTP/1.1 request from the source address `from` to the
/// listener on `OWN:port`, and reads the reply. `None` when the connection
/// ends without one.
async fn send(port: u16, from: &str, request: Vec<u8>) -> Option<Reply> {
    let socket = TcpSocket::new_v4().unwrap();
    socket
        .bind(format!("{from}:0").parse().unwrap())
        .expect("bind the source address");
    let mut stream = socket
        .connect(format!("{OWN}:{port}").parse().unwrap())
        .await
        .expect("connect");
    if stream.write_all(&request).await.is_err() {
        return None;
    }
    let mut raw = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw)).await;
    if !matches!(read, Ok(Ok(_))) {
        return None;
    }
    parse_reply(&raw)
}

/// An HTTP/1.1 reply with a JSON body, as read off the wire.
fn parse_reply(raw: &[u8]) -> Option<Reply> {
    if raw.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n")?;
    let status = head.split_whitespace().nth(1)?.parse().ok()?;
    Some(Reply {
        status,
        body: serde_json::from_str(body).unwrap_or(Value::Null),
    })
}

async fn post(port: u16, from: &str, path: &str, body: &str) -> Option<Reply> {
    let request = format!(
        "POST {path} HTTP/1.1\r\nhost: voter\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    send(port, from, request.into_bytes()).await
}

fn vote_body(term: u64, candidate: &str) -> Value {
    json!({
        "cluster_id": CLUSTER,
        "term": term,
        "candidate_node_id": candidate,
        "voters_version": 7
    })
}

fn assert_vote(reply: &Reply, vote: &str, reason: Option<&str>) {
    assert_eq!(reply.status, 200, "{reply:?}");
    assert_eq!(reply.body["vote"], vote, "{reply:?}");
    assert_eq!(
        reply.body.get("reason").and_then(Value::as_str),
        reason,
        "{reply:?}"
    );
    assert_eq!(reply.body["node_id"], ME, "{reply:?}");
    assert_eq!(reply.body["voters_version"], 7, "{reply:?}");
}

fn stored(term: u64, candidate: &str) -> Option<StoredVote> {
    Some(StoredVote {
        term,
        candidate: candidate.to_owned(),
    })
}

/// Section 8.9: a vote from a `node` voter's mesh address, for that voter,
/// is granted and stored; one from an address not in `spec.votes`, from a
/// witness-only voter, or for a candidate other than the source's voter is
/// `not_allowed` and stores nothing. No session exists here at all.
#[tokio::test]
async fn only_a_node_voter_for_itself_is_granted() {
    let port = free_port();
    let h = Harness::start(port).await;

    for (from, candidate) in [
        (STRANGER, "nod_a"),
        (WITNESS, "nod_w"),
        (NODE_A, ME),
        (NODE_A, "nod_w"),
    ] {
        let reply = h.vote(port, from, &vote_body(2, candidate)).await;
        assert_vote(&reply, "refused", Some("not_allowed"));
        assert_eq!(reply.body["term"], 0, "{reply:?}");
    }
    assert_eq!(h.store.vote().await.unwrap(), None);

    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);
    assert_eq!(
        (
            reply.body["term"].clone(),
            reply.body["lease_wait_ms"].clone()
        ),
        (json!(2), json!(0))
    );
    assert_eq!(h.store.vote().await.unwrap(), stored(2, "nod_a"));
    assert_eq!(h.store.highest_term().await.unwrap(), Some(2));
}

/// Section 7.3: the freeze does not depend on the agent's own vote, so a
/// frozen agent still answers votes.
#[tokio::test]
async fn a_frozen_agent_answers_votes() {
    let port = free_port();
    let h = Harness::start(port).await;
    crate::freeze::freeze(&h.dir.0).await.unwrap();
    assert!(crate::freeze::is_frozen(&h.store).await.unwrap());

    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);
    assert_eq!(h.store.vote().await.unwrap(), stored(2, "nod_a"));
}

/// A body above 4 KiB is refused unread, whether its length is declared or
/// not, even when it is a vote the listener would grant; a `GET` of
/// `/v1/vote` and another path are not answered as votes.
#[tokio::test]
async fn an_oversized_body_or_another_request_is_no_vote() {
    let port = free_port();
    let h = Harness::start(port).await;

    // A grantable vote, padded past the limit with JSON whitespace.
    let vote = vote_body(2, "nod_a").to_string();
    let padded = format!("{vote}{}", " ".repeat(BODY_LIMIT + 1 - vote.len()));
    assert!(padded.len() > BODY_LIMIT);
    let reply = post(port, NODE_A, "/v1/vote", &padded).await;
    assert!(
        reply.as_ref().is_none_or(|reply| reply.status == 413),
        "{reply:?}"
    );

    let chunked = format!(
        "POST /v1/vote HTTP/1.1\r\nhost: voter\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n{:x}\r\n{padded}\r\n0\r\n\r\n",
        padded.len()
    );
    let reply = send(port, NODE_A, chunked.into_bytes()).await;
    assert!(
        reply.as_ref().is_none_or(|reply| reply.status == 413),
        "{reply:?}"
    );
    assert_eq!(h.store.vote().await.unwrap(), None, "no vote taken");

    let get = send(
        port,
        NODE_A,
        b"GET /v1/vote HTTP/1.1\r\nhost: voter\r\nconnection: close\r\n\r\n".to_vec(),
    )
    .await
    .expect("an answer");
    assert_eq!(get.status, 405, "{get:?}");
    assert_error_body(&get);

    let other = post(port, NODE_A, "/v1/votes", &vote)
        .await
        .expect("an answer");
    assert_eq!(other.status, 404, "{other:?}");
    assert_error_body(&other);
    assert_eq!(h.store.vote().await.unwrap(), None, "no vote taken");

    // The same vote within the limit is granted.
    let reply = post(port, NODE_A, "/v1/vote", &vote)
        .await
        .expect("an answer");
    assert_vote(&reply, "granted", None);
}

/// The body of every answer but `200` (section 8.9): `{"error": "<text>"}`
/// and nothing else, so no vote.
fn assert_error_body(reply: &Reply) {
    let body = reply.body.as_object().expect("an object body");
    assert_eq!(body.len(), 1, "{reply:?}");
    assert!(body["error"].is_string(), "{reply:?}");
}

/// Section 8.9: a body above 4 KiB is answered `413`, and one that does not
/// arrive in time `408`, each with the `{"error": ...}` body and before any
/// write. A declared length above the limit is refused before any of the
/// body is read.
#[tokio::test]
async fn a_body_too_large_or_too_late_is_answered_with_the_error_body() {
    let port = free_port();
    let h = Harness::start(port).await;

    // Only the head, with a length above the limit: refused unread.
    let head = format!(
        "POST /v1/vote HTTP/1.1\r\nhost: voter\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        BODY_LIMIT + 1
    );
    let reply = send(port, NODE_A, head.into_bytes())
        .await
        .expect("an answer");
    assert_eq!(reply.status, 413, "{reply:?}");
    assert_error_body(&reply);

    // A body that stops halfway and never ends: `408` once the read limit
    // has passed.
    let vote = vote_body(2, "nod_a").to_string();
    let partial = format!(
        "POST /v1/vote HTTP/1.1\r\nhost: voter\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        vote.len(),
        &vote[..vote.len() / 2]
    );
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(format!("{NODE_A}:0").parse().unwrap()).unwrap();
    let mut stream = socket.connect(own(port)).await.expect("connect");
    stream.write_all(partial.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    // The stream stays open on this side, so the answer comes from the
    // listener's own read limit.
    let mut chunk = [0u8; 1024];
    let limit = VoteListenerOptions::quick().read_timeout;
    loop {
        let read = tokio::time::timeout(limit * 10, stream.read(&mut chunk))
            .await
            .expect("an answer within the read limit")
            .unwrap();
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..read]);
    }
    let reply = parse_reply(&raw).expect("an answer");
    assert_eq!(reply.status, 408, "{reply:?}");
    assert_error_body(&reply);

    assert_eq!(h.store.vote().await.unwrap(), None, "no vote taken");
    assert_eq!(
        h.store.highest_term().await.unwrap(),
        None,
        "nothing written"
    );
}

/// A vote request whose `term` is above 2^53 - 1 ([`MAX_TERM`]) is answered
/// `400` with an `{"error": ...}` body, and nothing is decided or written.
/// serde reads a JSON integer up to 2^64 - 1 into the `u64` field and fails
/// on a larger one: both end as `400`. The ceiling itself is decided as
/// usual.
#[tokio::test]
async fn a_term_above_the_ceiling_is_a_bad_request() {
    let raw = |term: &str| {
        format!(
            r#"{{"cluster_id":"{CLUSTER}","term":{term},"candidate_node_id":"nod_a","voters_version":7}}"#
        )
    };
    let parsed = |term: &str| serde_json::from_str::<VoteRequest>(&raw(term));
    assert_eq!(parsed("18446744073709551615").unwrap().term, u64::MAX);
    assert!(parsed("18446744073709551616").is_err());

    let port = free_port();
    let h = Harness::start(port).await;
    for term in [
        "9007199254740992",
        "18446744073709551615",
        "18446744073709551616",
    ] {
        let reply = post(port, NODE_A, "/v1/vote", &raw(term))
            .await
            .expect("an answer");
        assert_eq!(reply.status, 400, "{term}: {reply:?}");
        assert!(reply.body["error"].is_string(), "{term}: {reply:?}");
        assert!(reply.body.get("vote").is_none(), "{term}: {reply:?}");
        assert_eq!(h.store.vote().await.unwrap(), None, "{term}");
        assert_eq!(h.store.highest_term().await.unwrap(), None, "{term}");
    }

    let reply = post(port, NODE_A, "/v1/vote", &raw("9007199254740991"))
        .await
        .expect("an answer");
    assert_vote(&reply, "granted", None);
    assert_eq!(reply.body["term"], json!(MAX_TERM), "{reply:?}");
    assert_eq!(h.store.vote().await.unwrap(), stored(MAX_TERM, "nod_a"));
    assert_eq!(h.store.highest_term().await.unwrap(), Some(MAX_TERM));
}

/// Section 8.9: the listener follows `spec.votes`: a new port closes the old
/// listener and opens the new one, and a desired state without `spec.votes`
/// closes it.
#[tokio::test]
async fn the_listener_rebinds_when_the_port_changes() {
    let first = free_port();
    let h = Harness::start(first).await;
    let mut second = free_port();
    while second == first {
        second = free_port();
    }
    h.desired.send_replace(snapshot(Some(spec_votes(second))));
    assert!(listening(first, false).await, "the old port is closed");
    assert!(
        listening(second, false).await,
        "not before the firewall for the new port is loaded"
    );
    h.firewall.send_replace(Some(own(second)));
    assert!(listening(second, true).await, "the new port listens");
    assert!(listening(first, false).await, "the old port is closed");
    let reply = h.vote(second, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);

    h.desired.send_replace(snapshot(None));
    assert!(listening(second, false).await, "no spec.votes, no listener");
    assert!(!h.task.is_finished(), "the loop keeps following");
}

/// Section 8.9: the listener listens only while `spec.votes.listen` is
/// `true`. With `spec.votes` and the firewall for its address loaded but no
/// `listen`, or `listen: false`, nothing listens; `listen: true` binds, and a
/// new desired state without it closes the listener, whose report to the
/// mesh manager goes back to no socket. A connection accepted before votes
/// closed is answered `503` and decides nothing.
#[tokio::test]
async fn the_listener_listens_only_while_votes_are_open() {
    let port = free_port();
    // A read limit long enough for the connection accepted early to stay
    // open through the steps below.
    let options = VoteListenerOptions {
        read_timeout: Duration::from_secs(5),
        ..VoteListenerOptions::quick()
    };
    let h = Harness::start_with(port, options).await;
    assert_eq!(*h.bound.borrow(), Some(own(port)), "reported");

    // A connection accepted while votes are open.
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(format!("{NODE_A}:0").parse().unwrap()).unwrap();
    let mut early = socket.connect(own(port)).await.expect("connect");
    tokio::time::sleep(Duration::from_millis(200)).await;

    for closed in [None, Some(false)] {
        h.desired
            .send_replace(snapshot(Some(spec_votes_listen(port, closed))));
        assert!(listening(port, false).await, "listen {closed:?}: closed");
        assert_eq!(*h.bound.borrow(), None, "listen {closed:?}: no socket");
        assert_eq!(*h.firewall.borrow(), Some(own(port)), "the firewall stays");
        tokio::time::sleep(VoteListenerOptions::quick().bind_retry * 4).await;
        assert!(
            listening(port, false).await,
            "listen {closed:?}: no retry binds"
        );
    }

    let body = vote_body(2, "nod_a").to_string();
    let request = format!(
        "POST /v1/vote HTTP/1.1\r\nhost: voter\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    early.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), early.read_to_end(&mut raw))
        .await
        .expect("an answer in time")
        .unwrap();
    let reply = parse_reply(&raw).expect("an answer");
    assert_eq!(reply.status, 503, "{reply:?}");
    assert_error_body(&reply);
    assert_eq!(reply.body["error"], "votes are not open", "{reply:?}");
    assert_eq!(h.store.vote().await.unwrap(), None, "no vote taken");
    assert_eq!(h.store.highest_term().await.unwrap(), None);

    h.desired
        .send_replace(snapshot(Some(spec_votes_listen(port, Some(true)))));
    assert!(listening(port, true).await, "votes open again");
    assert_eq!(*h.bound.borrow(), Some(own(port)));
    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);
}

/// The listener fails closed: it listens only while the mesh firewall loaded
/// for its own address and port stands. It closes when an apply fails or the
/// mesh is removed (the watch reads `None`), does not listen while the loaded
/// rules cover another port, and listens again once they cover its own.
#[tokio::test]
async fn the_listener_listens_only_while_its_firewall_is_loaded() {
    let port = free_port();
    let h = Harness::start(port).await;

    h.firewall.send_replace(None);
    assert!(listening(port, false).await, "no firewall, no listener");
    let mut other = free_port();
    while other == port {
        other = free_port();
    }
    h.firewall.send_replace(Some(own(other)));
    assert!(listening(port, false).await, "rules for another port");
    assert!(listening(other, false).await, "nor on the rules' port");

    h.firewall.send_replace(Some(own(port)));
    assert!(listening(port, true).await, "its firewall is loaded again");
    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);
}

/// A connection that sends no byte at all is closed by the header read
/// limit, and its slot is freed: with every slot held by such connections,
/// a vote is answered once the limit has closed them.
#[tokio::test]
async fn an_idle_connection_is_closed_and_frees_its_slot() {
    let port = free_port();
    let h = Harness::start(port).await;
    let mut idle = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        idle.push(TcpStream::connect(own(port)).await.expect("connect"));
    }
    let limit = VoteListenerOptions::quick().read_timeout;
    for stream in &mut idle {
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(limit * 4, stream.read(&mut byte))
            .await
            .expect("the idle connection is closed within the read limit");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "closed without an answer: {read:?}"
        );
    }
    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);
}

/// A connection whose own address cannot be read is a caller the listener
/// cannot tell about: `503` (section 8.9), never `not_allowed`.
#[tokio::test]
async fn a_connection_without_its_own_address_cannot_be_told() {
    let caller = connection_caller(
        Path::new("/nonexistent"),
        format!("{NODE_A}:40000").parse().unwrap(),
        Err(std::io::Error::other("no address")),
    )
    .await;
    assert!(
        matches!(&caller, Caller::CannotTell(why) if why.contains("own address")),
        "{caller:?}"
    );
}

/// Section 8.9: the vote is durable before its answer goes out. While
/// another transaction holds the store's write lock, no answer arrives; once
/// it commits, the vote is granted and stored.
#[tokio::test]
async fn the_answer_waits_for_the_vote_to_be_written() {
    let port = free_port();
    let h = Harness::start(port).await;
    let mut holding = h.store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO node_meta (key, value) VALUES ('holding', '1')")
        .execute(&mut *holding)
        .await
        .unwrap();

    let voting = tokio::spawn(async move {
        post(port, NODE_A, "/v1/vote", &vote_body(2, "nod_a").to_string()).await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !voting.is_finished(),
        "no answer before the vote is written"
    );
    holding.commit().await.unwrap();

    let reply = voting.await.unwrap().expect("an answer");
    assert_vote(&reply, "granted", None);
    assert_eq!(h.store.vote().await.unwrap(), stored(2, "nod_a"));
}

/// The listener's address is the own mesh address without its prefix, on
/// `spec.votes.port`, and there is none without `spec.votes` or `spec.mesh`,
/// nor while `spec.votes.listen` is absent or `false` (section 8.9).
#[test]
fn the_listen_address_needs_votes_and_a_mesh_address() {
    let with_votes = snapshot(Some(spec_votes(51821)));
    assert_eq!(
        listen_address(with_votes.as_ref()),
        Some("127.0.0.1:51821".parse().unwrap())
    );
    assert_eq!(listen_address(snapshot(None).as_ref()), None);
    for closed in [None, Some(false)] {
        assert_eq!(
            listen_address(snapshot(Some(spec_votes_listen(51821, closed))).as_ref()),
            None,
            "listen {closed:?}"
        );
    }
    let mut no_mesh = with_votes.unwrap();
    no_mesh.state.spec.mesh = None;
    assert_eq!(listen_address(Some(&no_mesh)), None);
    assert_eq!(listen_address(None), None);
}

/// This test process's effective uid, read from `/proc/self/status`.
fn effective_uid() -> u32 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let uid = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .expect("a Uid line");
    uid.split_whitespace()
        .nth(1)
        .and_then(|uid| uid.parse().ok())
        .expect("an effective uid")
}

/// Whether this test process runs as root. The Linux container that runs the
/// tests is root; a CI runner is not.
fn running_as_root() -> bool {
    effective_uid() == 0
}

/// Sends `body` as a vote to the listener on `OWN:port` from a process that
/// is not root (`nobody`, uid 65534), through `bash`'s `/dev/tcp`; the
/// connection's source is this machine's own address. Needs a root test
/// process.
async fn vote_as_nobody(port: u16, body: &Value) -> Reply {
    use std::io::Write as _;
    use std::os::unix::process::CommandExt as _;

    let body = body.to_string();
    let request = format!(
        "POST /v1/vote HTTP/1.1\r\nhost: voter\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let output = tokio::task::spawn_blocking(move || {
        let mut child = std::process::Command::new("bash")
            .arg("-c")
            .arg(r#"exec 3<>"/dev/tcp/$1/$2" && cat >&3 && cat <&3"#)
            .arg("bash")
            .arg(OWN)
            .arg(port.to_string())
            .uid(65534)
            .gid(65534)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("start a client as nobody");
        child
            .stdin
            .take()
            .expect("its stdin")
            .write_all(request.as_bytes())
            .expect("send the request");
        child.wait_with_output().expect("the client's output")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "the client as nobody failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    parse_reply(&output.stdout).expect("an answer")
}

/// Section 7.3 promotes "as root on that machine", so a caller on this host
/// is answered only when its socket is root's. A process that is not root
/// asking for this machine's vote for itself is answered `not_allowed`, and
/// the term and the vote stay as they were; root is granted.
#[tokio::test]
async fn a_caller_on_this_machine_must_be_root() {
    let port = free_port();
    let h = Harness::start(port).await;
    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);

    let reply = if running_as_root() {
        vote_as_nobody(port, &vote_body(3, ME)).await
    } else {
        // A CI runner is not root: the test process itself is the caller
        // that is not root, and there is no root caller to try.
        h.vote(port, OWN, &vote_body(3, ME)).await
    };
    assert_vote(&reply, "refused", Some("not_allowed"));
    assert_eq!(reply.body["term"], 2, "{reply:?}");
    assert_eq!(h.store.vote().await.unwrap(), stored(2, "nod_a"));
    assert_eq!(h.store.highest_term().await.unwrap(), Some(2));

    if running_as_root() {
        // The candidate asks its own agent as root, from its own address.
        let reply = h.vote(port, OWN, &vote_body(3, ME)).await;
        assert_vote(&reply, "granted", None);
        assert_eq!(reply.body["term"], 3, "{reply:?}");
        assert_eq!(h.store.vote().await.unwrap(), stored(3, ME));
    }
}

/// A caller that cannot be a voter is refused before the store's write lock:
/// while another transaction holds it, a stranger's address and a process on
/// this machine that is not root still get `not_allowed` at once, with the
/// voter's term, and nothing is written.
#[tokio::test]
async fn a_refused_caller_does_not_wait_for_the_write_lock() {
    let port = free_port();
    let h = Harness::start(port).await;
    let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
    assert_vote(&reply, "granted", None);

    let mut holding = h.store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO node_meta (key, value) VALUES ('holding', '1')")
        .execute(&mut *holding)
        .await
        .unwrap();

    let prompt = Duration::from_secs(2);
    let stranger = tokio::time::timeout(prompt, h.vote(port, STRANGER, &vote_body(3, "nod_a")))
        .await
        .expect("a stranger is answered without the write lock");
    assert_vote(&stranger, "refused", Some("not_allowed"));
    assert_eq!(stranger.body["term"], 2, "{stranger:?}");

    let local = tokio::time::timeout(prompt, async {
        if running_as_root() {
            vote_as_nobody(port, &vote_body(3, ME)).await
        } else {
            h.vote(port, OWN, &vote_body(3, ME)).await
        }
    })
    .await
    .expect("a caller that is not root is answered without the write lock");
    assert_vote(&local, "refused", Some("not_allowed"));
    assert_eq!(local.body["term"], 2, "{local:?}");

    holding.rollback().await.unwrap();
    assert_eq!(h.store.vote().await.unwrap(), stored(2, "nod_a"));
    assert_eq!(h.store.highest_term().await.unwrap(), Some(2));
}

/// Section 8.10 rule 4: a vote is decided against the `spec.votes` read in
/// its transaction. A new voter-set version published while the vote waits
/// for the store's write lock is the one it is decided against.
#[tokio::test]
async fn the_vote_reads_spec_votes_inside_its_transaction() {
    let port = free_port();
    let h = Harness::start(port).await;
    let mut holding = h.store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO node_meta (key, value) VALUES ('holding', '1')")
        .execute(&mut *holding)
        .await
        .unwrap();

    // Grantable at version 7, the version when it arrives.
    let voting = tokio::spawn(async move {
        post(port, NODE_A, "/v1/vote", &vote_body(2, "nod_a").to_string()).await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!voting.is_finished(), "the vote waits for the write lock");
    let mut newer = spec_votes(port);
    newer.version = 8;
    h.desired.send_replace(snapshot(Some(newer)));
    holding.commit().await.unwrap();

    let reply = voting.await.unwrap().expect("an answer");
    assert_eq!(reply.status, 200, "{reply:?}");
    assert_eq!(
        (
            reply.body["vote"].clone(),
            reply.body["reason"].clone(),
            reply.body["voters_version"].clone()
        ),
        (json!("refused"), json!("voters_changed"), json!(8)),
        "{reply:?}"
    );
    assert_eq!(h.store.vote().await.unwrap(), None);
}

/// Writes a `tcp` and a `tcp6` socket table in a new directory, with the
/// kernel's header line before their rows.
fn proc_net(tcp: &[&str], tcp6: &[&str]) -> TempDir {
    let dir = std::env::temp_dir().join(format!(
        "quoro-proc-net-{}",
        util::hex(&util::random_bytes::<8>())
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let table = |rows: &[&str]| {
        let mut text = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n".to_owned();
        for row in rows {
            text.push_str(row);
            text.push('\n');
        }
        text
    };
    std::fs::write(dir.join("tcp"), table(tcp)).unwrap();
    std::fs::write(dir.join("tcp6"), table(tcp6)).unwrap();
    TempDir(dir)
}

/// A row of a socket table: local and remote address, state and uid.
fn row(local: &str, remote: &str, state: &str, uid: u32) -> String {
    format!(
        "   0: {local} {remote} {state} 00000000:00000000 00:00000000 00000000 {uid:>5}        0 4242 1 0000000000000000 20 4 30 10 -1"
    )
}

// The kernel prints each 32-bit word of an address in the host's byte
// order; these fixtures are a little-endian host's. 10.43.0.3 is 03002B0A,
// ::ffff:10.43.0.3 is 0000000000000000FFFF000003002B0A, the listener's port
// 51821 is CA6D and the clients' ports 40000 to 40002 are 9C40 to 9C42.
const LISTENER: &str = "03002B0A:CA6D";
const LISTENER6: &str = "0000000000000000FFFF000003002B0A:CA6D";

fn caller(port: u16) -> SocketAddr {
    SocketAddr::new("10.43.0.3".parse().unwrap(), port)
}

fn listener_address() -> SocketAddr {
    "10.43.0.3:51821".parse().unwrap()
}

/// The rows around the caller's own: the listening socket, the listener's
/// side of the caller's connection, another client of root, and root's
/// socket that is closing (the kernel lists such a socket with uid 0
/// whoever owned it).
fn decoys() -> Vec<String> {
    vec![
        row(LISTENER, "00000000:0000", "0A", 0),
        row(LISTENER, "03002B0A:9C40", "01", 0),
        row("03002B0A:9C41", LISTENER, "01", 0),
        row("03002B0A:9C42", LISTENER, "06", 0),
    ]
}

fn uid_of(dir: &TempDir, port: u16) -> Result<u32, String> {
    local_caller_uid(Path::new(&dir.0), caller(port), listener_address())
}

/// The `/proc/net/tcp` lookup finds the caller's own established row among
/// the others, and gives its uid.
#[cfg(target_endian = "little")]
#[test]
fn the_socket_table_gives_the_caller_s_uid() {
    for uid in [0, 1000] {
        let mut rows = decoys();
        rows.insert(2, row("03002B0A:9C40", LISTENER, "01", uid));
        let rows: Vec<&str> = rows.iter().map(String::as_str).collect();
        let dir = proc_net(&rows, &[]);
        assert_eq!(uid_of(&dir, 40000), Ok(uid), "uid {uid}");
        // Another client of root, and root's closing socket, decide nothing
        // for this caller.
        assert_eq!(uid_of(&dir, 40001), Ok(0), "the other client is root's");
        assert!(uid_of(&dir, 40002).is_err(), "a closing socket is no owner");
    }
}

/// A dual-stack client socket is listed in `/proc/net/tcp6` with the
/// IPv4-mapped address.
#[cfg(target_endian = "little")]
#[test]
fn the_socket_table_of_a_mapped_address_is_tcp6() {
    for uid in [0, 1000] {
        let decoys = decoys();
        let tcp: Vec<&str> = decoys.iter().map(String::as_str).collect();
        let tcp6 = [
            row(LISTENER6, "00000000000000000000000000000000:0000", "0A", 0),
            row(
                "0000000000000000FFFF000003002B0A:9C40",
                LISTENER6,
                "01",
                uid,
            ),
        ];
        let tcp6: Vec<&str> = tcp6.iter().map(String::as_str).collect();
        let dir = proc_net(&tcp, &tcp6);
        assert_eq!(uid_of(&dir, 40000), Ok(uid), "uid {uid}");
    }
}

/// The lookup reads no further than the caller's row: a malformed row after
/// it changes nothing, and one before it fails the lookup.
#[cfg(target_endian = "little")]
#[test]
fn the_socket_table_is_read_up_to_the_caller_s_row() {
    let own = row("03002B0A:9C40", LISTENER, "01", 1000);
    let broken =
        "   9: 03002B0A:9C43 03002B0A:CA6D 01 00000000:00000000 00:00000000 00000000  root 0 1";
    let dir = proc_net(&[&own, broken], &[]);
    assert_eq!(uid_of(&dir, 40000), Ok(1000), "a bad row after the match");
    let dir = proc_net(&[broken, &own], &[]);
    assert!(uid_of(&dir, 40000).is_err(), "a bad row before the match");
}

/// A lookup that cannot show the caller is an error, never an owner: no row
/// for the caller, a table that does not parse, or one that cannot be read.
#[cfg(target_endian = "little")]
#[test]
fn a_caller_the_socket_table_cannot_show_is_an_error() {
    let decoys = decoys();
    let mut rows: Vec<&str> = decoys.iter().map(String::as_str).collect();
    let dir = proc_net(&rows, &[]);
    assert!(uid_of(&dir, 40003).is_err(), "no row for the caller");

    let own = row("03002B0A:9C40", LISTENER, "01", 0);
    rows.push(&own);
    let dir = proc_net(&rows, &[]);
    assert_eq!(uid_of(&dir, 40000), Ok(0), "the caller's row is root's");
    for broken in [
        "   9: 03002B0A:9C4 short",
        "   9: 03002B0Z:9C43 03002B0A:CA6D 01 00000000:00000000 00:00000000 00000000     0 0 1",
        "   9: 03002B0A:9C43 03002B0A:CA6D 01 00000000:00000000 00:00000000 00000000  root 0 1",
    ] {
        let mut malformed = rows.clone();
        malformed.insert(0, broken);
        let dir = proc_net(&malformed, &[]);
        assert!(uid_of(&dir, 40000).is_err(), "a malformed table: {broken}");
    }
    let empty = proc_net(&[], &[]);
    std::fs::write(empty.0.join("tcp"), "").unwrap();
    assert!(uid_of(&empty, 40000).is_err(), "an empty table");
    // No tcp6 table to look in once tcp has no row.
    let dir = proc_net(&decoys.iter().map(String::as_str).collect::<Vec<_>>(), &[]);
    std::fs::remove_file(dir.0.join("tcp6")).unwrap();
    assert!(uid_of(&dir, 40000).is_err(), "an unreadable tcp6");
    let missing = TempDir(dir.0.join("absent"));
    assert!(uid_of(&missing, 40000).is_err(), "no tables at all");
}

/// The lookup on this machine's real `/proc/net`, as whatever user runs the
/// tests: a client connected to a listener on 127.0.0.1 is owned by this
/// process's effective uid. The same client against a port it is not
/// connected to, another address against the listener, and the client once
/// it has closed have no established row from that address to that listener,
/// so they are errors: a lookup that ignored either address or the state
/// would answer them.
#[test]
fn the_real_socket_table_gives_this_process_s_uid() {
    let listener = std::net::TcpListener::bind((OWN, 0)).expect("bind");
    let server = listener.local_addr().unwrap();
    let client = std::net::TcpStream::connect(server).expect("connect");
    let (accepted, _) = listener.accept().expect("accept");
    let peer = client.local_addr().unwrap();
    let proc_net = Path::new("/proc/net");

    assert_eq!(
        local_caller_uid(proc_net, peer, server),
        Ok(effective_uid()),
        "the client's own row"
    );
    let other = std::net::TcpListener::bind((OWN, 0)).expect("bind another");
    assert!(
        local_caller_uid(proc_net, peer, other.local_addr().unwrap()).is_err(),
        "the client is not connected to another port"
    );
    assert!(
        local_caller_uid(proc_net, other.local_addr().unwrap(), server).is_err(),
        "no client of the listener at another socket's address"
    );
    drop(client);
    // The client's socket leaves the established state once it has closed,
    // while the accepted one is still open.
    let mut closed = false;
    for _ in 0..200 {
        if local_caller_uid(proc_net, peer, server).is_err() {
            closed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(closed, "a closed client has no owner");
    drop(accepted);
}

/// A caller on this machine whose owner the listener cannot find is answered
/// `503` with an `{"error": ...}` body, never `granted` nor `not_allowed`,
/// and nothing is written, even for a vote a root caller would be granted.
/// A peer at another address needs no lookup and is still granted.
#[tokio::test]
async fn a_lookup_that_fails_answers_503() {
    let tables = proc_net(&["   0: short"], &[]);
    for proc_net in [tables.0.join("absent"), tables.0.clone()] {
        let port = free_port();
        let options = VoteListenerOptions {
            proc_net,
            ..VoteListenerOptions::quick()
        };
        let h = Harness::start_with(port, options).await;

        let reply = h.vote(port, OWN, &vote_body(3, ME)).await;
        assert_eq!(reply.status, 503, "{reply:?}");
        assert!(reply.body["error"].is_string(), "{reply:?}");
        assert!(reply.body.get("vote").is_none(), "{reply:?}");
        assert_eq!(h.store.vote().await.unwrap(), None);
        assert_eq!(h.store.highest_term().await.unwrap(), None);

        let reply = h.vote(port, NODE_A, &vote_body(2, "nod_a")).await;
        assert_vote(&reply, "granted", None);
    }
}

/// Two vote requests from a process on this machine that is not root, well
/// inside 60 s, are both refused, and the listener's limit lets one `warn`
/// through and holds the other back. The log line itself is not captured: a
/// subscriber of one test thread does not reliably see the listener's events
/// while other tests run.
#[tokio::test]
async fn a_caller_that_is_not_root_is_warned_about_once_a_minute() {
    let port = free_port();
    let h = Harness::start(port).await;
    for term in [3, 4] {
        let reply = if running_as_root() {
            vote_as_nobody(port, &vote_body(term, ME)).await
        } else {
            h.vote(port, OWN, &vote_body(term, ME)).await
        };
        assert_vote(&reply, "refused", Some("not_allowed"));
    }
    {
        let limit = h.listener.inner.local_warn.lock().unwrap();
        assert!(limit.last.is_some(), "one warn went out");
        assert_eq!(limit.suppressed, 1, "the second was held back");
    }
    assert_eq!(h.store.vote().await.unwrap(), None);
}

/// The limit lets the first `warn` through, holds back the ones inside
/// [`LOCAL_WARN_EVERY`], and once that has passed lets the next through with
/// their count.
#[test]
fn the_warn_limit_counts_what_it_holds_back() {
    let start = tokio::time::Instant::now();
    let mut limit = WarnLimit::default();
    assert_eq!(limit.admit(start), Some(0));
    assert_eq!(limit.admit(start + Duration::from_secs(1)), None);
    assert_eq!(limit.admit(start + Duration::from_secs(59)), None);
    assert_eq!(limit.admit(start + LOCAL_WARN_EVERY), Some(2));
    assert_eq!(limit.admit(start + LOCAL_WARN_EVERY), None);
    assert_eq!(limit.admit(start + 2 * LOCAL_WARN_EVERY), Some(1));
}

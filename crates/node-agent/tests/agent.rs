// SPDX-License-Identifier: AGPL-3.0-only
//! The wiring `run` and the trust boundary around the local store:
//! a verified snapshot is reconciled and applied, a tampered one is ignored,
//! and an unenrolled data directory is refused.

mod agent_common;

use std::time::Duration;

use agent_common::{
    BRIDGE, RecordingEngine, TempDir, dns, facts, identity_files_with_url, snapshot, spec_vm,
};
use quoro_node_agent::reconcile::Snapshot;
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::{AgentConfig, AgentError, ControlPlaneUrls, run};
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::DesiredStatus;
use serde_json::Value;

fn config(dir: &TempDir) -> AgentConfig {
    AgentConfig {
        data_dir: dir.path().to_path_buf(),
        node_name: "node-1".to_owned(),
        bridge: BRIDGE.to_owned(),
        dns: dns(),
        roles: vec!["node".to_owned()],
        cloudflared_binary: dir.path().join("cloudflared"),
        cloudflared_metrics: "127.0.0.1:20241".parse().unwrap(),
        acme_directory_url: quoro_node_agent::DEFAULT_ACME_DIRECTORY.to_owned(),
        acme_contact: Vec::new(),
        snapshots_at_start: None,
        control_plane_urls: ControlPlaneUrls::default(),
    }
}

/// A snapshot body with a real signature over it.
fn signed_body(snapshot: &Snapshot, key: &SigningKey) -> Value {
    let mut body = snapshot.state.to_unsigned_value();
    sign::sign_body(key, &mut body).expect("sign the snapshot");
    body
}

#[tokio::test]
async fn run_reconciles_a_verified_stored_snapshot_without_a_session() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    // Port 1 refuses immediately, so the session keeps redialing while the
    // reconciler does its work.
    identity_files_with_url(dir.path(), &key, "https://127.0.0.1:1");
    let store = NodeStore::open(dir.path()).await.unwrap();
    let snapshot = snapshot(
        3,
        vec![spec_vm("vm_stored", "prj_1", DesiredStatus::Running)],
    );
    store
        .store_desired(3, &signed_body(&snapshot, &key))
        .await
        .unwrap();

    let engine = RecordingEngine::new();
    let observed = engine.clone();
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run(config(&dir), engine, facts(), async move {
        let _ = stop.await;
    }));

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if observed.create_count() == 1 && store.applied_rev().await.unwrap() == 3 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the agent reconciled the stored snapshot");

    let created = observed.created();
    assert_eq!(created[0].id.0, "vm_stored");
    assert_eq!(created[0].net.isolation_group.as_deref(), Some("prj_1"));

    shutdown.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the agent stops on shutdown")
        .expect("the agent task did not panic");
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn a_tampered_stored_snapshot_never_reaches_the_engine() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    identity_files_with_url(dir.path(), &key, "https://127.0.0.1:1");
    let store = NodeStore::open(dir.path()).await.unwrap();
    let snapshot = snapshot(
        9,
        vec![spec_vm("vm_tampered", "prj_1", DesiredStatus::Running)],
    );
    let other = sign::signing_key_from_seed(&[55; 32]);
    store
        .store_desired(9, &signed_body(&snapshot, &other))
        .await
        .unwrap();

    let engine = RecordingEngine::new();
    let observed = engine.clone();
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run(config(&dir), engine, facts(), async move {
        let _ = stop.await;
    }));

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        observed.create_count(),
        0,
        "a body signed by another key must not create a VM"
    );
    assert_eq!(store.applied_rev().await.unwrap(), 0);

    shutdown.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the agent stops on shutdown")
        .expect("the agent task did not panic");
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn run_refuses_a_node_that_is_not_enrolled() {
    let dir = TempDir::new();
    let error = run(
        config(&dir),
        RecordingEngine::new(),
        facts(),
        std::future::pending::<()>(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, AgentError::Identity(_)), "{error:?}");
}

/// Section 7.2: a machine with the `node` role upgraded to M5b generates its
/// custody key when the agent starts, `0600`, even while no session can
/// reach the primary; an edge-only or witness-only machine never writes one.
#[tokio::test]
async fn an_agent_start_keeps_a_custody_key_only_with_the_node_role() {
    for (roles, keeps) in [
        (vec!["node"], true),
        (vec!["edge"], false),
        (vec!["witness"], false),
    ] {
        let dir = TempDir::new();
        let key = sign::signing_key_from_seed(&[4; 32]);
        // Port 1 refuses immediately: no session ever sends a `hello`, so
        // only the start can write the key.
        identity_files_with_url(dir.path(), &key, "https://127.0.0.1:1");
        let path = dir
            .path()
            .join(quoro_node_agent::identity::CUSTODY_KEY_FILE);
        let mut config = config(&dir);
        config.roles = roles.iter().map(|role| (*role).to_owned()).collect();
        let (shutdown, stop) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run(config, RecordingEngine::new(), facts(), async move {
            let _ = stop.await;
        }));

        // The mesh key is written just before the custody key, and the
        // reconciler and the session start only after both.
        let mesh = dir.path().join(quoro_node_agent::mesh::KEY_FILE);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !mesh.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the agent started");
        if keeps {
            // The key is linked into place whole, so it is complete once
            // it exists.
            tokio::time::timeout(Duration::from_secs(5), async {
                while !path.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the agent wrote custody.key");
            assert_eq!(std::fs::read(&path).unwrap().len(), 32);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode, 0o600, "custody.key is root-only");
            }
        }

        shutdown.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the agent stops on shutdown")
            .expect("the agent task did not panic");
        assert!(result.is_ok(), "{result:?}");
        // `run` passes the custody key step before it can return, so a
        // machine without the `node` role that has stopped wrote none.
        assert_eq!(path.exists(), keeps, "{roles:?}");
    }
}

/// Section 6.12 (ADR 0170): a witness-only machine runs the node agent with
/// no engine work at all. Its desired state has no `spec.network` and no VM,
/// only `spec.witness` and `spec.votes`; the agent records the revision as
/// applied and never asks the engine for a network or a VM.
#[tokio::test]
async fn a_witness_only_agent_runs_without_an_engine() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    // Port 1 refuses immediately: the agent runs from its stored state.
    identity_files_with_url(dir.path(), &key, "https://127.0.0.1:1");
    let store = NodeStore::open(dir.path()).await.unwrap();
    let mut witness = snapshot(5, Vec::new());
    witness.state.spec.network = None;
    witness.state.spec.witness = Some(quoro_proto::wire::SpecWitness {});
    witness.state.spec.votes = Some(quoro_proto::wire::SpecVotes {
        version: 12,
        port: 51821,
        voters: vec![quoro_proto::wire::SpecVoter {
            node_id: "nod_test".to_owned(),
            roles: vec!["witness".to_owned()],
            mesh_address: Some("10.43.0.4".to_owned()),
        }],
        listen: None,
    });
    store
        .store_desired(5, &signed_body(&witness, &key))
        .await
        .unwrap();

    let engine = RecordingEngine::new();
    let observed = engine.clone();
    let mut config = config(&dir);
    config.roles = vec!["witness".to_owned()];
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run(config, engine, facts(), async move {
        let _ = stop.await;
    }));

    tokio::time::timeout(Duration::from_secs(5), async {
        while store.applied_rev().await.unwrap() < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the agent applied the witness's desired state");
    assert_eq!(observed.create_count(), 0, "a witness starts no VM");
    assert!(observed.networks().is_empty(), "a witness has no bridge");

    shutdown.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the agent stops on shutdown")
        .expect("the agent task did not panic");
    assert!(result.is_ok(), "{result:?}");
}

/// Sends one vote request from the source address `from` to the vote
/// listener on `127.0.0.1:port`, and reads the JSON answer.
#[cfg(target_os = "linux")]
async fn vote_from(from: &str, port: u16, term: u64, candidate: &str) -> Value {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let body = serde_json::json!({
        "cluster_id": "cls_test",
        "term": term,
        "candidate_node_id": candidate,
        "voters_version": 12
    })
    .to_string();
    let request = format!(
        "POST /v1/vote HTTP/1.1\r\nhost: voter\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind(format!("{from}:0").parse().unwrap()).unwrap();
    let mut stream = socket
        .connect(format!("127.0.0.1:{port}").parse().unwrap())
        .await
        .expect("connect to the vote listener");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw))
        .await
        .expect("an answer in time")
        .unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").expect("a reply");
    assert!(head.starts_with("HTTP/1.1 200"), "{text}");
    serde_json::from_str(body).unwrap()
}

/// Section 8.9: the vote listener belongs to the agent process. A
/// witness-only agent with no session ever up answers a vote from a `node`
/// voter's mesh address on its own mesh address and `spec.votes.port`, and
/// still answers while a promotion on its host froze it (section 7.3). It
/// listens only once the mesh, and so its firewall, is applied: while the
/// apply fails nothing listens, and the retry after it brings the listener
/// up. Loopback addresses stand in for mesh addresses.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_agent_answers_votes_without_a_session_and_while_frozen() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    // Port 1 refuses immediately: no session ever comes up.
    identity_files_with_url(dir.path(), &key, "https://127.0.0.1:1");
    let store = NodeStore::open(dir.path()).await.unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let voter = |node_id: &str, role: &str, address: &str| quoro_proto::wire::SpecVoter {
        node_id: node_id.to_owned(),
        roles: vec![role.to_owned()],
        mesh_address: Some(address.to_owned()),
    };
    let mut witness = snapshot(5, Vec::new());
    witness.state.spec.network = None;
    witness.state.spec.witness = Some(quoro_proto::wire::SpecWitness {});
    witness.state.spec.mesh = Some(quoro_proto::wire::SpecMesh {
        address: "127.0.0.1/8".to_owned(),
        listen_port: 51820,
        peers: Vec::new(),
        allow: Vec::new(),
    });
    witness.state.spec.votes = Some(quoro_proto::wire::SpecVotes {
        version: 12,
        port,
        voters: vec![
            voter("nod_test", "witness", "127.0.0.1"),
            voter("nod_a", "node", "127.0.0.2"),
        ],
        listen: Some(true),
    });
    store
        .store_desired(5, &signed_body(&witness, &key))
        .await
        .unwrap();

    let mut config = config(&dir);
    config.roles = vec!["witness".to_owned()];
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let mesh = quoro_node_agent::mesh::RecordingMeshDriver::new();
    mesh.fail_applies(true);
    let task = tokio::spawn(quoro_node_agent::run_with_mesh(
        config,
        RecordingEngine::new(),
        facts(),
        mesh.clone(),
        async move {
            let _ = stop.await;
        },
    ));

    tokio::time::timeout(Duration::from_secs(10), async {
        while mesh.calls().is_empty() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the agent applied the mesh");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err(),
        "no vote listener while the mesh firewall is not applied"
    );
    mesh.fail_applies(false);

    tokio::time::timeout(Duration::from_secs(10), async {
        while tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the vote listener came up");

    let answer = vote_from("127.0.0.2", port, 2, "nod_a").await;
    assert_eq!(answer["vote"], "granted", "{answer}");
    assert_eq!(answer["node_id"], "nod_test", "{answer}");
    assert_eq!(answer["term"], 2, "{answer}");

    quoro_node_agent::freeze::freeze(dir.path()).await.unwrap();
    let answer = vote_from("127.0.0.2", port, 3, "nod_a").await;
    assert_eq!(
        answer["vote"], "granted",
        "a frozen agent still votes: {answer}"
    );
    assert_eq!(store.highest_term().await.unwrap(), Some(3));

    shutdown.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the agent stops on shutdown")
        .expect("the agent task did not panic");
    assert!(result.is_ok(), "{result:?}");
}

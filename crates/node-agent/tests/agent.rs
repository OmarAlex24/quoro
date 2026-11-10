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
use quoro_node_agent::{AgentConfig, AgentError, run};
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
/// reach the primary; an edge-only machine never writes one.
#[tokio::test]
async fn an_agent_start_keeps_a_custody_key_only_with_the_node_role() {
    for (roles, keeps) in [(vec!["node"], true), (vec!["edge"], false)] {
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

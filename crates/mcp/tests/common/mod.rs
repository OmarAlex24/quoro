//! Shared harness for the MCP integration tests: the control plane's router
//! and the `/mcp` route on one ephemeral local port over plain HTTP, the way
//! `quoro serve` merges them on its TLS listener, with a temporary data
//! directory under `TMPDIR`.
//!
//! The tests read the audit log with SQL to compare calls; the crate under
//! test never does.

#![allow(dead_code)]

use quoro_control_plane::auth::setup_token;
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::store::{Store, random_secret};
use quoro_control_plane::{AppState, NodeNetwork, ReferenceAsDigest, ServerConfig, router};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;

pub const OWNER_EMAIL: &str = "owner@example.com";
pub const OWNER_PASSWORD: &str = "correct horse battery staple";
pub const PROTOCOL_VERSION: &str = "2025-11-25";

pub struct Server {
    pub base: String,
    pub data_dir: PathBuf,
    pub state: AppState,
}

/// An owner, a project, an app on a registered local node, and an agent with
/// a fresh access token.
pub struct Seeded {
    pub server: Server,
    pub owner: reqwest::Client,
    pub owner_id: String,
    pub project_id: String,
    pub app_id: String,
    pub agent_id: String,
    pub agent_key: String,
    pub access_token: String,
}

impl Server {
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Runs setup and logs in; returns a client holding the owner session.
    pub async fn owner(&self) -> reqwest::Client {
        let token = setup_token::issue(&self.data_dir).unwrap();
        let client = reqwest::Client::builder()
            .cookie_store(true)
            .build()
            .unwrap();
        let res = client
            .post(self.url("/api/v1/setup"))
            .header("Quoro-Setup-Token", token)
            .json(&json!({ "email": OWNER_EMAIL, "name": "Owner", "password": OWNER_PASSWORD }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "setup: {}", res.text().await.unwrap());
        let res = client
            .post(self.url("/api/v1/auth/login"))
            .json(&json!({ "email": OWNER_EMAIL, "password": OWNER_PASSWORD }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "login: {}", res.text().await.unwrap());
        client
    }

    /// Every audit record, oldest first, as JSON objects with every column.
    pub async fn audit_rows(&self) -> Vec<Value> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT json_object(\
               'seq', seq, 'time', time, 'actor_type', actor_type, 'actor_id', actor_id, \
               'agent_model', agent_model, 'on_behalf_of', on_behalf_of, 'channel', channel, \
               'operation', operation, 'scope', scope, 'target_type', target_type, \
               'target_id', target_id, 'project_id', project_id, 'params', json(params), \
               'justification', justification, 'trigger_event_id', trigger_event_id, \
               'incident_id', incident_id, 'grant_id', grant_id, 'approval_id', approval_id, \
               'request_id', request_id, 'result', result, 'result_detail', result_detail, \
               'change_seq', change_seq) \
             FROM audit_log ORDER BY seq",
        )
        .fetch_all(self.state.store.pool())
        .await
        .unwrap();
        rows.into_iter()
            .map(|(row,)| serde_json::from_str(&row).unwrap())
            .collect()
    }
}

/// Starts the control plane and the MCP route on 127.0.0.1 with an empty
/// database.
pub async fn start() -> Server {
    // reqwest may carry the binary's no-provider rustls feature in a workspace
    // build; an earlier test having installed it already is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let data_dir = std::env::temp_dir().join(format!("quoro-mcp-test-{}", &random_secret()[..16]));
    std::fs::create_dir_all(&data_dir).unwrap();
    let store = Store::open(&data_dir.join("quoro.db")).await.unwrap();
    let state = AppState::open(
        store,
        ServerConfig {
            data_dir: data_dir.clone(),
            secure_cookies: false,
            public_url: Some("https://primary.example:8443".to_string()),
            tls_fingerprint: concat!(
                "sha256:",
                "0000000000000000000000000000000000000000000000000000000000000000"
            )
            .to_string(),
            tls: {
                let certified =
                    rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
                quoro_control_plane::custody::TlsIdentity::from_pem(
                    certified.cert.pem(),
                    certified.key_pair.serialize_pem(),
                )
                .unwrap()
            },
            node_network: NodeNetwork {
                subnet: "10.42.1.0/24".to_string(),
                gateway: "10.42.1.1".to_string(),
            },
            heartbeat_interval_ms: 200,
            session_timeout_ms: 800,
        },
        Arc::new(ReferenceAsDigest),
        Arc::new(DataKey::ephemeral()),
    )
    .await
    .unwrap();
    // The demotion guard is the outermost layer, as `quoro serve` wraps its
    // whole service (`node-agent-protocol.md` section 7.4).
    let app = quoro_control_plane::demotion::guard(
        router(state.clone()).merge(quoro_mcp::router(state.clone())),
        state.store.clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base,
        data_dir,
        state,
    }
}

fn local_node() -> LocalNode {
    LocalNode {
        name: "test-node".to_string(),
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

async fn post_json(client: &reqwest::Client, url: String, body: Value) -> Value {
    let res = client.post(url).json(&body).send().await.unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
    body
}

/// Mints a fresh access token from an agent key.
pub async fn mint(server: &Server, agent_key: &str) -> String {
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {agent_key}"))
        .header("Quoro-Justification", "test run")
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 200, "mint: {body}");
    body["access_token"].as_str().unwrap().to_string()
}

/// A server with an owner, a project, an app and an agent holding a token.
pub async fn seeded() -> Seeded {
    let server = start().await;
    let owner = server.owner().await;
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let owner_id = me["user"]["id"].as_str().unwrap().to_string();
    driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
    let project = post_json(
        &owner,
        server.url("/api/v1/projects"),
        json!({ "slug": "demo", "name": "Demo" }),
    )
    .await;
    let project_id = project["id"].as_str().unwrap().to_string();
    let app = post_json(
        &owner,
        server.url("/api/v1/apps"),
        json!({
            "project_id": project_id,
            "name": "web",
            "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "port": 8080,
        }),
    )
    .await;
    let app_id = app["id"].as_str().unwrap().to_string();
    let agent = post_json(
        &owner,
        server.url("/api/v1/agents"),
        json!({ "name": "operator", "model": "test-model", "owner_user_id": owner_id }),
    )
    .await;
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let key = post_json(
        &owner,
        server.url(&format!("/api/v1/agents/{agent_id}/keys")),
        json!({}),
    )
    .await;
    let agent_key = key["key"].as_str().unwrap().to_string();
    let access_token = mint(&server, &agent_key).await;
    Seeded {
        server,
        owner,
        owner_id,
        project_id,
        app_id,
        agent_id,
        agent_key,
        access_token,
    }
}

/// One JSON-RPC request to `/mcp` with a bearer token; returns the status and
/// the JSON body (or `Value::Null` when the body is not JSON).
pub async fn rpc(server: &Server, token: Option<&str>, body: Value) -> (u16, Value) {
    let mut request = reqwest::Client::new()
        .post(server.url("/mcp"))
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", PROTOCOL_VERSION)
        .json(&body);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let res = request.send().await.unwrap();
    let status = res.status().as_u16();
    let text = res.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// `tools/call` of `name` with `arguments`; returns the JSON-RPC `result`.
pub async fn call_tool(server: &Server, token: &str, name: &str, arguments: Value) -> Value {
    let (status, body) = rpc(
        server,
        Some(token),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments },
        }),
    )
    .await;
    assert_eq!(status, 200, "tools/call {name}: {body}");
    body["result"].clone()
}

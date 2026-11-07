//! A personal API token created through `createToken` with a human's panel
//! session authenticates the client (`QUORO_TOKEN`), and stops doing so once
//! it is revoked.

mod common;

use common::{quoro, start};
use quoro_cli::exit;
use quoro_cli::operations::GET_ME;
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_from_the_tokens_api_authenticates_the_client() {
    let primary = start().await;
    let (owner_id, cookie) = primary.owner_session().await;
    let (status, created) = primary
        .with_session(
            reqwest::Method::POST,
            "/api/v1/tokens",
            &cookie,
            Some(json!({ "name": "cli" })),
        )
        .await;
    assert_eq!(status, 201, "create token");
    let token = created["token"].as_str().unwrap().to_string();
    let token_id = created["id"].as_str().unwrap().to_string();

    let environment = primary.ad_hoc_environment(&[("QUORO_TOKEN", &token)]);
    let output = quoro(&["api", GET_ME.method, GET_ME.path], &environment).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let me: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(me["user"]["id"], owner_id.as_str());
    assert!(!output.stdout.contains(&token) && !output.stderr.contains(&token));

    let (status, _) = primary
        .with_session(
            reqwest::Method::DELETE,
            &format!("/api/v1/tokens/{token_id}"),
            &cookie,
            None,
        )
        .await;
    assert_eq!(status, 204, "revoke token");
    let output = quoro(&["api", GET_ME.method, GET_ME.path], &environment).await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output
            .stderr
            .lines()
            .next()
            .unwrap_or_default()
            .ends_with("(unauthenticated)"),
        "{}",
        output.stderr
    );
}

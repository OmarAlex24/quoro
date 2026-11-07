//! `quoro api` end to end: the client against an in-process primary over
//! real TLS, pinned to the primary's self-signed certificate.

mod common;

use common::{quoro, start, write_credentials};
use quoro_cli::exit;
use quoro_cli::operations::{GET_HEALTH, GET_ME};
use serde_json::Value;

const WRONG_PIN: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_answers_over_the_pinned_certificate() {
    let primary = start().await;
    let environment = primary.ad_hoc_environment(&[]);
    let output = quoro(&["api", GET_HEALTH.method, GET_HEALTH.path], &environment).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(json(&output.stdout).is_object(), "{}", output.stdout);
    assert_eq!(primary.requests(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_fingerprint_fails_before_any_request() {
    let primary = start().await;
    let environment = primary.ad_hoc_environment(&[("QUORO_FINGERPRINT", WRONG_PIN)]);
    let output = quoro(&["api", GET_HEALTH.method, GET_HEALTH.path], &environment).await;
    assert_eq!(output.code, exit::UNREACHABLE, "{}", output.stderr);
    let expected = format!(
        "the primary's certificate fingerprint is {}, not the pinned {WRONG_PIN}",
        primary.fingerprint
    );
    assert!(output.stderr.contains(&expected), "{}", output.stderr);
    assert_eq!(output.stderr.lines().count(), 1, "one line, no request id");
    assert_eq!(
        primary.requests(),
        0,
        "nothing was sent before the pin matched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn me_answers_with_the_stored_token_or_quoro_token() {
    let primary = start().await;
    let (owner_id, token) = primary.owner_token().await;
    let home = primary.config_home("stored");
    write_credentials(&home, &token, 0o600);

    let environment = primary.context_environment(&home);
    let output = quoro(
        &["--context", "lab", "api", GET_ME.method, GET_ME.path],
        &environment,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(json(&output.stdout)["user"]["id"], owner_id.as_str());
    assert!(!output.stderr.contains(&token) && !output.stdout.contains(&token));

    let environment = primary.ad_hoc_environment(&[("QUORO_TOKEN", &token)]);
    let output = quoro(&["api", "get", GET_ME.path, "--json"], &environment).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(json(&output.stdout)["user"]["id"], owner_id.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn me_without_a_token_is_unauthenticated() {
    let primary = start().await;
    let environment = primary.ad_hoc_environment(&[]);
    let output = quoro(&["api", GET_ME.method, GET_ME.path], &environment).await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    let lines: Vec<&str> = output.stderr.lines().collect();
    assert_eq!(lines.len(), 2, "{}", output.stderr);
    assert!(lines[0].starts_with("quoro: ") && lines[0].ends_with("(unauthenticated)"));
    assert!(lines[1].starts_with("request id: req_"), "{}", lines[1]);
    assert!(
        output.stdout.is_empty(),
        "the body is printed only with --json"
    );

    let output = quoro(&["api", GET_ME.method, GET_ME.path, "--json"], &environment).await;
    assert_eq!(json(&output.stdout)["code"], "unauthenticated");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_credentials_file_others_can_read_is_refused() {
    let primary = start().await;
    let (_, token) = primary.owner_token().await;
    let home = primary.config_home("shared");
    write_credentials(&home, &token, 0o644);

    let output = quoro(
        &["api", GET_ME.method, GET_ME.path],
        &primary.context_environment(&home),
    )
    .await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(output.stderr.contains("mode 644"), "{}", output.stderr);
    assert!(output.stderr.contains("chmod 600"), "{}", output.stderr);
    assert!(!output.stderr.contains(&token), "the token is never echoed");
    assert_eq!(primary.requests(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_carries_the_reason_into_the_audit_log() {
    let primary = start().await;
    let (_, token) = primary.owner_token().await;
    let body = primary.dir.join("project.json");
    std::fs::write(&body, r#"{"slug":"demo","name":"Demo"}"#).unwrap();
    let data = format!("@{}", body.display());
    let environment = primary.ad_hoc_environment(&[("QUORO_TOKEN", &token)]);

    let output = quoro(
        &[
            "api",
            "POST",
            "/api/v1/projects",
            "--data",
            &data,
            "--reason",
            "set up the demo",
        ],
        &environment,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(json(&output.stdout)["slug"], "demo");
    let (justification, channel, operation) = primary.last_audit().await;
    assert_eq!(operation, "POST /api/v1/projects");
    assert_eq!(channel, "api");
    assert_eq!(justification.as_deref(), Some("set up the demo"));

    // The same slug again is a conflict, and a project that is not there is
    // not found: each has its exit code.
    let again = quoro(
        &["api", "POST", "/api/v1/projects", "--data", &data],
        &environment,
    )
    .await;
    assert_eq!(again.code, exit::CONFLICT, "{}", again.stderr);
    let missing = format!("/api/v1/projects/prj_{}", "0".repeat(26));
    let gone = quoro(&["api", "GET", &missing], &environment).await;
    assert_eq!(gone.code, exit::NOT_FOUND, "{}", gone.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_mistakes_are_usage_errors_and_send_nothing() {
    let primary = start().await;
    let environment = primary.ad_hoc_environment(&[]);
    for args in [
        &["api", "TRACE", "/api/v1/health"][..],
        &["api", "GET", "api/v1/health"],
        &["api", "POST", "/api/v1/projects", "--data", "{}"],
        &["nope"],
    ] {
        let output = quoro(args, &environment).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
    }
    assert_eq!(primary.requests(), 0);
}

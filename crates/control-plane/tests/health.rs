mod common;

#[tokio::test]
async fn health_is_public_and_carries_a_request_id() {
    let server = common::start().await;
    let res = reqwest::get(server.url("/api/v1/health")).await.unwrap();
    assert_eq!(res.status(), 200);
    let id = res.headers()["quoro-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        quoro_types::is_id(quoro_types::Prefix::Request, &id),
        "{id}"
    );
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body, serde_json::json!({ "status": "ok" }));
}

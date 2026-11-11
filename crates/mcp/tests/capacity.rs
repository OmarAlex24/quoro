//! The MCP tool `get_capacity` end to end: it answers `getClusterCapacity`
//! with the HTTP body for the same token, and refuses a missing or malformed
//! `lost_node_id` with `422`, as the HTTP API does.

mod common;

use common::{Seeded, call_tool, seeded};
use serde_json::{Value, json};

/// Issues a `cluster:read` grant over the whole cluster with the owner's
/// session.
async fn grant_cluster_read(seeded: &Seeded) {
    let res = seeded
        .owner
        .post(seeded.server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": seeded.agent_id,
            "scopes": ["cluster:read"],
            "selector": { "all": true },
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create grant: {body}");
}

#[tokio::test]
async fn get_capacity_answers_the_http_body_and_refuses_a_bad_node_id() {
    let seeded = seeded().await;
    grant_cluster_read(&seeded).await;
    let node_id: String = sqlx::query_scalar("SELECT id FROM nodes WHERE cp_status = 'primary'")
        .fetch_one(seeded.server.state.store.pool())
        .await
        .unwrap();

    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_capacity",
        json!({ "lost_node_id": node_id }),
    )
    .await;
    assert_ne!(result["isError"], true, "get_capacity: {result}");
    let mut structured = result["structuredContent"].clone();
    assert_eq!(structured["lost_node_id"], node_id.as_str(), "{structured}");

    let res = reqwest::Client::new()
        .get(
            seeded
                .server
                .url(&format!("/api/v1/cluster/capacity?lost_node_id={node_id}")),
        )
        .header("Authorization", format!("Bearer {}", seeded.access_token))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let mut body: Value = res.json().await.unwrap();
    // `computed_at` is the time of each read; everything else is the same.
    for read in [&mut structured, &mut body] {
        read["computed_at"] = Value::Null;
    }
    assert_eq!(structured, body, "the MCP read must return the HTTP body");

    for arguments in [json!({}), json!({ "lost_node_id": "nod_short" })] {
        let result = call_tool(
            &seeded.server,
            &seeded.access_token,
            "get_capacity",
            arguments.clone(),
        )
        .await;
        assert_eq!(result["isError"], true, "{arguments}: {result}");
        let problem = &result["structuredContent"];
        assert_eq!(
            problem["code"], "validation_failed",
            "{arguments}: {result}"
        );
        assert_eq!(problem["status"], 422, "{arguments}: {result}");
    }
}

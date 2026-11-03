//! `GET /api/v1/audit`, `GET /api/v1/audit/{seq}` and
//! `POST /api/v1/audit/verify` (`audit-log-format.md` sections 2.4 and 6).

mod common;

use serde_json::{Value, json};

async fn list(owner: &reqwest::Client, url: &str) -> Value {
    let res = owner.get(url).send().await.unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    res.json().await.unwrap()
}

fn items(page: &Value) -> Vec<Value> {
    page["items"].as_array().unwrap().clone()
}

#[tokio::test]
async fn audit_lists_verifies_and_reports_a_tampered_record() {
    let server = common::start().await;
    let owner = server.owner().await;

    // Setup already wrote record 1; three mutating calls follow.
    for slug in ["one", "two", "three"] {
        let res = owner
            .post(server.url("/api/v1/projects"))
            .json(&json!({ "slug": slug, "name": slug }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    }

    // Oldest first, gap-free, each record linking to the one before.
    let page = list(&owner, &server.url("/api/v1/audit?limit=500")).await;
    let records = items(&page);
    assert!(records.len() >= 4, "{page}");
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record["seq"].as_i64(), Some(index as i64 + 1), "{page}");
    }
    assert_eq!(
        records[0]["prev_hash"].as_str(),
        Some(quoro_control_plane::store::GENESIS_HASH)
    );
    for pair in records.windows(2) {
        assert_eq!(pair[1]["prev_hash"], pair[0]["hash"], "{page}");
    }
    let (stored,): (i64,) = sqlx::query_as("SELECT count(*) FROM audit_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(records.len() as i64, stored);

    // One record by seq, and a 404 for an unknown seq.
    let seq = records[1]["seq"].as_i64().unwrap();
    let one = list(&owner, &server.url(&format!("/api/v1/audit/{seq}"))).await;
    assert_eq!(one, records[1]);
    let res = owner
        .get(server.url("/api/v1/audit/999999"))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // The chain verifies, and the verification run is itself audited.
    let verified: Value = owner
        .post(server.url("/api/v1/audit/verify"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(verified["ok"], true, "{verified}");
    assert_eq!(verified["checked"].as_i64(), Some(stored), "{verified}");
    assert_eq!(verified["first_seq"].as_i64(), Some(1));
    assert_eq!(verified["last_seq"].as_i64(), Some(stored));
    assert_eq!(verified["head_hash"], records.last().unwrap()["hash"]);

    let after_run = items(&list(&owner, &server.url("/api/v1/audit?limit=500")).await);
    let run = after_run.last().unwrap();
    assert_eq!(run["operation"], "POST /api/v1/audit/verify");
    assert_eq!(run["result"], "ok");

    // Tamper with a copied row: the append-only triggers are the contract, so
    // a test drops them first, exactly as an operator with database access
    // would (section 7).
    let tampered_seq = 2;
    sqlx::query("DROP TRIGGER audit_log_append_only_update")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE audit_log SET justification = 'edited after the fact' WHERE seq = ?")
        .bind(tampered_seq)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let broken: Value = owner
        .post(server.url("/api/v1/audit/verify"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(broken["ok"], false, "{broken}");
    assert_eq!(broken["broken_at_seq"].as_i64(), Some(tampered_seq));
    assert_eq!(broken["reason"], "hash_mismatch");

    // The failure emitted `audit.chain_broken` and the run is audited as an
    // error carrying the reason and the seq.
    let (data,): (String,) = sqlx::query_as(
        "SELECT data FROM events WHERE type = 'audit.chain_broken' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let data: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(data["broken_at_seq"].as_i64(), Some(tampered_seq));
    assert_eq!(data["expected_hash"].as_str().unwrap().len(), 64);
    assert_eq!(data["found_hash"].as_str().unwrap().len(), 64);

    let after_failure = items(&list(&owner, &server.url("/api/v1/audit?limit=500")).await);
    let failed_run = after_failure.last().unwrap();
    assert_eq!(failed_run["operation"], "POST /api/v1/audit/verify");
    assert_eq!(failed_run["result"], "error");
    assert_eq!(
        failed_run["result_detail"],
        format!("hash_mismatch at {tampered_seq}")
    );

    // Walking the log two records at a time reaches every record once.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let url = match &cursor {
            None => server.url("/api/v1/audit?limit=2"),
            Some(cursor) => server.url(&format!("/api/v1/audit?limit=2&cursor={cursor}")),
        };
        let page = list(&owner, &url).await;
        let batch = items(&page);
        assert!(batch.len() <= 2, "{page}");
        seen.extend(batch.iter().map(|record| record["seq"].as_i64().unwrap()));
        match page["next_cursor"].as_str() {
            Some(next) if !batch.is_empty() => cursor = Some(next.to_string()),
            _ => break,
        }
    }
    let (all,): (i64,) = sqlx::query_as("SELECT count(*) FROM audit_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(seen, (1..=all).collect::<Vec<_>>(), "pages walk the log");
}

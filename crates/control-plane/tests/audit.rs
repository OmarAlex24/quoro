//! `GET /api/v1/audit`, `GET /api/v1/audit/{seq}` and
//! `POST /api/v1/audit/verify` (`audit-log-format.md` sections 2.4 and 6),
//! and what the audit records of the request body limit and of parameters
//! kept as a digest (README "Request body limit", section 4.1).

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

fn seqs(page: &Value) -> Vec<i64> {
    items(page)
        .iter()
        .map(|record| record["seq"].as_i64().unwrap())
        .collect()
}

/// Every `seq` of `listAudit` with `query`, read `limit` records at a time by
/// following `next_cursor` to the last page.
async fn walk(
    server: &common::Server,
    owner: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Vec<i64> {
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let url = match &cursor {
            None => server.url(&format!("/api/v1/audit?{query}&limit={limit}")),
            Some(cursor) => server.url(&format!(
                "/api/v1/audit?{query}&limit={limit}&cursor={cursor}"
            )),
        };
        let page = list(owner, &url).await;
        let batch = seqs(&page);
        assert!(batch.len() <= limit, "{page}");
        seen.extend(&batch);
        match page["next_cursor"].as_str() {
            Some(next) => {
                assert_eq!(
                    batch.len(),
                    limit,
                    "only a full page has a next page: {page}"
                );
                cursor = Some(next.to_string());
            }
            None => break,
        }
    }
    seen
}

/// The stored `seq`s, oldest first, of the records `filter` selects.
async fn stored_seqs(server: &common::Server, filter: &str) -> Vec<i64> {
    let rows: Vec<(i64,)> = sqlx::query_as(&format!(
        "SELECT seq FROM audit_log WHERE {filter} ORDER BY seq"
    ))
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter().map(|(seq,)| seq).collect()
}

#[tokio::test]
async fn audit_lists_newest_first_within_a_window() {
    let server = common::start().await;
    let owner = server.owner().await;

    // Each project is created once and then again under the same slug, which
    // is a conflict: the log interleaves `ok` and `error` records. An owner's
    // reads are not audited, so the log stays as it is while the test reads.
    for slug in ["one", "two", "three"] {
        for status in [201, 409] {
            let res = owner
                .post(server.url("/api/v1/projects"))
                .json(&json!({ "slug": slug, "name": slug }))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), status, "{}", res.text().await.unwrap());
        }
    }
    let all = stored_seqs(&server, "1 = 1").await;
    assert!(all.len() >= 7, "{all:?}");
    let newest_first: Vec<i64> = all.iter().rev().copied().collect();

    // No `order` reads oldest first, exactly as `order=asc` does.
    let plain = list(&owner, &server.url("/api/v1/audit?limit=500")).await;
    let ascending = list(&owner, &server.url("/api/v1/audit?order=asc&limit=500")).await;
    assert_eq!(seqs(&plain), all, "{plain}");
    assert_eq!(plain, ascending);
    assert_eq!(walk(&server, &owner, "order=asc", 2).await, all);

    // `desc` returns the same records newest first, and its cursor walks
    // toward older records across pages.
    let descending = list(&owner, &server.url("/api/v1/audit?order=desc&limit=500")).await;
    let mut reversed = items(&ascending);
    reversed.reverse();
    assert_eq!(items(&descending), reversed, "{descending}");
    assert!(descending["next_cursor"].is_null(), "{descending}");
    assert_eq!(walk(&server, &owner, "order=desc", 2).await, newest_first);
    assert_eq!(walk(&server, &owner, "order=desc", 3).await, newest_first);

    // Both bounds are exclusive and bound the window in both orders.
    assert_eq!(
        walk(&server, &owner, "after_seq=2&before_seq=6", 2).await,
        vec![3, 4, 5]
    );
    assert_eq!(
        walk(&server, &owner, "order=desc&after_seq=2&before_seq=6", 2).await,
        vec![5, 4, 3]
    );
    assert_eq!(
        walk(&server, &owner, "order=desc&before_seq=3", 1).await,
        vec![2, 1]
    );
    assert_eq!(
        walk(&server, &owner, "order=asc&before_seq=3", 1).await,
        vec![1, 2]
    );

    // A window that holds no `seq` is an empty last page, not an error.
    for window in [
        "after_seq=5&before_seq=5",
        "after_seq=5&before_seq=3",
        "after_seq=4&before_seq=5",
    ] {
        for order in ["asc", "desc"] {
            let page = list(
                &owner,
                &server.url(&format!("/api/v1/audit?order={order}&{window}")),
            )
            .await;
            assert!(items(&page).is_empty(), "{order} {window}: {page}");
            assert!(page["next_cursor"].is_null(), "{order} {window}: {page}");
        }
    }

    // A filter selects the same records in both orders.
    let errors = stored_seqs(&server, "result = 'error'").await;
    assert_eq!(errors.len(), 3, "{errors:?}");
    let errors_newest_first: Vec<i64> = errors.iter().rev().copied().collect();
    assert_eq!(walk(&server, &owner, "result=error", 2).await, errors);
    assert_eq!(
        walk(&server, &owner, "order=desc&result=error", 2).await,
        errors_newest_first
    );
    let filtered = list(
        &owner,
        &server.url("/api/v1/audit?order=desc&result=error&limit=500"),
    )
    .await;
    assert!(
        items(&filtered)
            .iter()
            .all(|record| record["result"] == "error"),
        "{filtered}"
    );
    let below = errors[2];
    assert_eq!(
        walk(
            &server,
            &owner,
            &format!("order=desc&result=error&before_seq={below}"),
            1
        )
        .await,
        vec![errors[1], errors[0]]
    );

    // An unknown order and a `before_seq` below 1 are refused.
    for query in ["order=newest", "before_seq=0"] {
        let res = owner
            .get(server.url(&format!("/api/v1/audit?{query}")))
            .send()
            .await
            .unwrap();
        let (status, code, _) = common::problem(res).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{query}"
        );
    }
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

// ---------------------------------------------------------------------------
// The request body limit and the parameters digest (README "Request body
// limit", `audit-log-format.md` section 4.1, ADR 0306).
// ---------------------------------------------------------------------------

/// The limit: 256 KiB.
const LIMIT: usize = 262_144;

/// A `createProject` body of exactly `len` bytes.
fn project_body(len: usize) -> String {
    let empty = r#"{"slug":"big","name":""}"#;
    let body = format!(
        r#"{{"slug":"big","name":"{}"}}"#,
        "n".repeat(len - empty.len())
    );
    assert_eq!(body.len(), len);
    body
}

async fn audit_count(server: &common::Server) -> i64 {
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM audit_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    count
}

/// The newest record, as `listAudit` returns it.
async fn newest(server: &common::Server, owner: &reqwest::Client) -> Value {
    let page = list(owner, &server.url("/api/v1/audit?order=desc&limit=1")).await;
    items(&page).remove(0)
}

/// Lowercase hex SHA-256, computed here rather than by the server.
fn sha256(text: &str) -> String {
    use sha2::Digest as _;
    sha2::Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Asserts that `record` is the audit of a 413: refused, `payload_too_large`,
/// `params` `{}` and neither digest field.
fn assert_too_large_record(record: &Value, operation: &str) {
    assert_eq!(record["operation"], operation, "{record}");
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], "payload_too_large", "{record}");
    assert_eq!(record["params"], json!({}), "{record}");
    assert!(record["params_sha256"].is_null(), "{record}");
    assert!(record["params_bytes"].is_null(), "{record}");
}

/// The owner's `quoro_session=...` cookie, from a login of its own.
async fn session_cookie(server: &common::Server) -> String {
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": common::OWNER_EMAIL, "password": common::OWNER_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let cookie = res
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|value| value.starts_with("quoro_session="))
        .unwrap()
        .to_string();
    cookie.split(';').next().unwrap().to_string()
}

/// `POST path` with `body` sent chunked, so with no `Content-Length`; returns
/// the status and the raw response.
async fn post_chunked(
    server: &common::Server,
    path: &str,
    cookie: &str,
    body: &[u8],
) -> (u16, String) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut stream = tokio::net::TcpStream::connect(server.address())
        .await
        .unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {}\r\nCookie: {cookie}\r\n\
         Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\
         Connection: close\r\n\r\n",
        server.address()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    for chunk in body.chunks(16 * 1024) {
        stream
            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await
            .unwrap();
        stream.write_all(chunk).await.unwrap();
        stream.write_all(b"\r\n").await.unwrap();
    }
    // The server may answer before it reads the terminator.
    let _ = stream.write_all(b"0\r\n\r\n").await;
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response).await;
    let text = String::from_utf8_lossy(&response).to_string();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|status| status.parse().ok())
        .unwrap_or_else(|| panic!("no status line: {text:?}"));
    (status, text)
}

#[tokio::test]
async fn a_body_above_the_limit_is_refused_and_audited() {
    let server = common::start().await;
    let owner = server.owner().await;

    // With a Content-Length: 413, unparsed, and audited as a refusal of the
    // call with `params` `{}`.
    let before = audit_count(&server).await;
    let res = owner
        .post(server.url("/api/v1/projects"))
        .header("Content-Type", "application/json")
        .body(project_body(LIMIT + 1))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (413, "payload_too_large"));
    assert_eq!(audit_count(&server).await, before + 1);
    let record = newest(&server, &owner).await;
    assert_too_large_record(&record, "POST /api/v1/projects");
    assert_eq!(record["actor_type"], "user");
    assert_eq!(record["channel"], "panel");
    let (projects,): (i64,) = sqlx::query_as("SELECT count(*) FROM projects")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(projects, 0, "the body was not acted on");

    // Chunked, with no Content-Length: the same.
    let cookie = session_cookie(&server).await;
    let before = audit_count(&server).await;
    let (status, text) = post_chunked(
        &server,
        "/api/v1/projects",
        &cookie,
        project_body(LIMIT + 1).as_bytes(),
    )
    .await;
    assert_eq!(status, 413, "{text}");
    assert!(text.contains("payload_too_large"), "{text}");
    assert_eq!(audit_count(&server).await, before + 1);
    assert_too_large_record(&newest(&server, &owner).await, "POST /api/v1/projects");

    // The chain with these records verifies.
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
}

#[tokio::test]
async fn a_body_at_the_limit_is_not_refused_for_its_size() {
    let server = common::start().await;
    let owner = server.owner().await;
    let cookie = session_cookie(&server).await;
    for chunked in [false, true] {
        let status = if chunked {
            post_chunked(
                &server,
                "/api/v1/projects",
                &cookie,
                project_body(LIMIT).as_bytes(),
            )
            .await
            .0
        } else {
            owner
                .post(server.url("/api/v1/projects"))
                .header("Content-Type", "application/json")
                .body(project_body(LIMIT))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        };
        assert_ne!(status, 413, "chunked: {chunked}");
    }
    let refused: Vec<i64> = stored_seqs(&server, "result_detail = 'payload_too_large'").await;
    assert!(refused.is_empty(), "{refused:?}");
}

#[tokio::test]
async fn a_body_above_the_limit_without_an_identity_is_not_recorded() {
    let server = common::start().await;
    let owner = server.owner().await;
    let before = audit_count(&server).await;

    // No credential at all.
    let res = server
        .client()
        .post(server.url("/api/v1/projects"))
        .header("Content-Type", "application/json")
        .body(project_body(LIMIT + 1))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (413, "payload_too_large"));

    // A public operation has no identity, even with a session alongside.
    let res = owner
        .post(server.url("/api/v1/auth/login"))
        .header("Content-Type", "application/json")
        .body(project_body(LIMIT + 1))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (413, "payload_too_large"));

    // A human's read is not recorded either.
    let res = owner
        .get(server.url("/api/v1/projects"))
        .header("Content-Type", "application/json")
        .body(project_body(LIMIT + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 413);

    assert_eq!(audit_count(&server).await, before);
}

#[tokio::test]
async fn an_agent_key_call_above_the_limit_is_recorded_for_its_agent() {
    let server = common::start().await;
    let owner = server.owner().await;
    let me = list(&owner, &server.url("/api/v1/auth/me")).await;
    let agent: Value = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({ "name": "operator", "model": "m", "owner_user_id": me["user"]["id"] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let key: Value = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Content-Type", "application/json")
        .body(project_body(LIMIT + 1))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (413, "payload_too_large"));
    let record = newest(&server, &owner).await;
    assert_too_large_record(&record, "POST /api/v1/auth/agent-token");
    assert_eq!(record["actor_type"], "agent");
    assert_eq!(record["actor_id"], agent_id.as_str());
}

#[tokio::test]
async fn a_float_is_audited_in_params_and_its_record_verifies() {
    let server = common::start().await;
    let owner = server.owner().await;

    // A human's approval request: refused after authorization, so the error
    // is audited with the body's parameters. Two of them are floats whose
    // RFC 8785 text an approximate parser reads back as another number;
    // the primary parses floats exactly (`audit-log-format.md` section 4.1,
    // ADR 0315), so they are kept.
    let body = r#"{"scope":"app:delete","target":{"type":"app","id":"app_01JC9Z6Q5W8N3K4T7R2M1XHB0F"},"operation":"DELETE /api/v1/apps/{app_id}","params":{"x":1548.2162860866162,"y":18983552425072.563},"justification":"j"}"#;
    let before = audit_count(&server).await;
    let res = owner
        .post(server.url("/api/v1/approvals"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    assert_eq!(audit_count(&server).await, before + 1);

    let record = newest(&server, &owner).await;
    assert_eq!(record["operation"], "POST /api/v1/approvals", "{record}");
    assert!(record["params_sha256"].is_null(), "{record}");
    let seq = record["seq"].as_i64().unwrap();
    let (stored,): (String,) = sqlx::query_as("SELECT params FROM audit_log WHERE seq = ?")
        .bind(seq)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        stored,
        r#"{"incident_id":null,"justification":"j","operation":"DELETE /api/v1/apps/{app_id}","params":{"x":1548.2162860866163,"y":18983552425072.562},"scope":"app:delete","target":{"id":"app_01JC9Z6Q5W8N3K4T7R2M1XHB0F","type":"app"}}"#
    );

    // Its change-log entry was written, and the chain verifies from the
    // stored text.
    let (entries,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM change_log WHERE entity_type = 'audit' AND entity_id = ?",
    )
    .bind(seq.to_string())
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(entries, 1);
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
    assert_eq!(verified["checked"].as_i64(), Some(seq));
}

#[tokio::test]
async fn a_whole_number_float_above_2_53_is_audited_with_a_digest() {
    let server = common::start().await;
    let owner = server.owner().await;

    // `1e16` is a float whose RFC 8785 text, `10000000000000000`, reads
    // back as an integer above 2^53, so the parameters stay a digest
    // (`audit-log-format.md` section 4.1). The text the digest covers holds
    // the other float as the primary read it.
    let body = r#"{"scope":"app:delete","target":{"type":"app","id":"app_01JC9Z6Q5W8N3K4T7R2M1XHB0F"},"operation":"DELETE /api/v1/apps/{app_id}","params":{"n":1e16,"x":1548.2162860866162},"justification":"j"}"#;
    let res = owner
        .post(server.url("/api/v1/approvals"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    let record = newest(&server, &owner).await;
    assert_eq!(record["operation"], "POST /api/v1/approvals", "{record}");
    assert_eq!(record["params"], json!({}), "{record}");
    let text = r#"{"incident_id":null,"justification":"j","operation":"DELETE /api/v1/apps/{app_id}","params":{"n":10000000000000000,"x":1548.2162860866163},"scope":"app:delete","target":{"id":"app_01JC9Z6Q5W8N3K4T7R2M1XHB0F","type":"app"}}"#;
    assert_eq!(record["params_sha256"], json!(sha256(text)), "{record}");
    assert_eq!(record["params_bytes"], json!(text.len()), "{record}");
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
}

#[tokio::test]
async fn a_humans_transcript_fetch_above_the_limit_is_recorded() {
    // Section 3.3 records every transcript fetch, a human's too, so its 413
    // is recorded like any refusal of it.
    let server = common::start().await;
    let owner = server.owner().await;
    let before = audit_count(&server).await;
    let res = owner
        .get(server.url("/api/v1/interactive-sessions/trm_01JC9Z6Q5W8N3K4T7R2M1XHB0D/transcript"))
        .header("Content-Type", "application/json")
        .body(project_body(LIMIT + 1))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (413, "payload_too_large"));
    assert_eq!(audit_count(&server).await, before + 1);
    let record = newest(&server, &owner).await;
    assert_too_large_record(
        &record,
        "GET /api/v1/interactive-sessions/{session_id}/transcript",
    );
    assert_eq!(record["actor_type"], "user");
}

#[tokio::test]
async fn a_request_that_matched_no_operation_is_not_recorded() {
    // An unknown path, and a method its path does not have, are no call the
    // audit log records, whatever the size of the body.
    let server = common::start().await;
    let owner = server.owner().await;
    let before = audit_count(&server).await;
    for (method, path) in [
        (reqwest::Method::POST, "/api/v1/no-such-thing"),
        (reqwest::Method::PUT, "/api/v1/projects"),
        (reqwest::Method::PATCH, "/api/v1/audit"),
    ] {
        let res = owner
            .request(method.clone(), server.url(path))
            .header("Content-Type", "application/json")
            .body(project_body(LIMIT + 1))
            .send()
            .await
            .unwrap();
        assert!(
            res.status().is_client_error(),
            "{method} {path}: {}",
            res.status()
        );
    }
    assert_eq!(audit_count(&server).await, before);
}

/// Sends `head` and then `body` exactly as given on a new connection, and
/// returns what the server sends back within ten seconds. A server that waits
/// for the rest of a body sends nothing more in that time.
async fn send_raw(server: &common::Server, head: &str, body: &[u8]) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut stream = tokio::net::TcpStream::connect(server.address())
        .await
        .unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut response),
    )
    .await;
    String::from_utf8_lossy(&response).to_string()
}

#[tokio::test]
async fn a_declared_length_above_the_limit_is_refused_before_the_body_is_read() {
    // A Content-Length of 262,145 with a two-byte body: the server refuses it
    // from the header, without waiting for bytes that never come, and before
    // it would send `100 Continue` to a client that waits for one.
    let server = common::start().await;
    let owner = server.owner().await;
    let cookie = session_cookie(&server).await;
    for expect in ["", "Expect: 100-continue\r\n"] {
        let before = audit_count(&server).await;
        let head = format!(
            "POST /api/v1/projects HTTP/1.1\r\nHost: {}\r\nCookie: {cookie}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n{expect}\
             Connection: close\r\n\r\n",
            server.address(),
            LIMIT + 1
        );
        let text = send_raw(&server, &head, b"{}").await;
        assert!(
            text.starts_with("HTTP/1.1 413 "),
            "{expect:?}: the first answer is the 413: {text:?}"
        );
        assert!(text.contains("payload_too_large"), "{text}");
        assert_eq!(audit_count(&server).await, before + 1, "{expect:?}");
        assert_too_large_record(&newest(&server, &owner).await, "POST /api/v1/projects");
    }
}

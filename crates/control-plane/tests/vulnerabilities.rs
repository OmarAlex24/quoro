// SPDX-License-Identifier: AGPL-3.0-only
//! The two vulnerability reads (ADR 0150) over real HTTP: an app's report,
//! the inventory states, ordering and filters, and the cluster summary with
//! its node data for callers that may read it.
//!
//! Findings, mirror rows, inventories and deployments go straight into the
//! test server's store and cache; the API is the only reader.

mod common;

use common::{Server, problem, start};
use quoro_control_plane::vulns::osv::Record;
use quoro_proto::inventory::{InventoryObject, sha256_hex};
use quoro_proto::wire::{InventoryOs, InventoryPackageWire};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

/// `sha256:` followed by 64 copies of one digit.
fn digest(digit: u8) -> String {
    format!("sha256:{}", char::from(digit).to_string().repeat(64))
}

/// A `vln_` id matching the contract pattern, distinct per index.
fn finding_id(index: u32) -> String {
    format!("vln_01JC9Z6Q5W8N3K4T7R2M1XHB{index:02}")
}

/// A `dep_` id matching the contract pattern, distinct per index.
fn deployment_id(index: u32) -> String {
    format!("dep_01JC9Z6Q5W8N3K4T7R2M1XHB{index:02}")
}

async fn create_project(owner: &reqwest::Client, server: &Server, slug: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": slug, "name": slug }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create project: {}",
        res.text().await.unwrap()
    );
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn create_app(
    owner: &reqwest::Client,
    server: &Server,
    project_id: &str,
    name: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": name,
            "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Gives the app a current deployment with `image_digest`, the way a finished
/// deploy would.
async fn set_current_image(server: &Server, app_id: &str, image_digest: &str, index: u32) {
    let id = deployment_id(index);
    let now = "2026-01-01T00:00:00Z";
    sqlx::query(
        "INSERT INTO deployments (id, app_id, image, image_digest, status, phase, error, \
         \"trigger\", created_by_type, created_by_id, created_at, finished_at) \
         VALUES (?, ?, 'ghcr.io/example/web@sha256:0000', ?, 'succeeded', NULL, NULL, 'api', \
         'user', 'usr_test', ?, ?)",
    )
    .bind(&id)
    .bind(app_id)
    .bind(image_digest)
    .bind(now)
    .bind(now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE apps SET current_deployment_id = ? WHERE id = ?")
        .bind(&id)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

/// One finding, with the defaults the tests do not care about.
struct Finding {
    id: String,
    subject_kind: &'static str,
    app_id: Option<String>,
    image_digest: Option<String>,
    node_id: Option<String>,
    component: Option<String>,
    vuln_id: String,
    package: String,
    severity: &'static str,
    kev: bool,
    resolved_at: Option<String>,
}

impl Finding {
    fn app(
        index: u32,
        app_id: &str,
        image_digest: &str,
        severity: &'static str,
        vuln_id: &str,
        package: &str,
        kev: bool,
    ) -> Self {
        Self {
            id: finding_id(index),
            subject_kind: "app",
            app_id: Some(app_id.to_string()),
            image_digest: Some(image_digest.to_string()),
            node_id: None,
            component: None,
            vuln_id: vuln_id.to_string(),
            package: package.to_string(),
            severity,
            kev,
            resolved_at: None,
        }
    }

    fn resolved(mut self, resolved_at: &str) -> Self {
        self.resolved_at = Some(resolved_at.to_string());
        self
    }
}

async fn insert_finding(server: &Server, finding: &Finding) {
    let (resolved_reason, resolved_at) = match &finding.resolved_at {
        Some(at) => (Some("image_changed"), Some(at.as_str())),
        None => (None, None),
    };
    sqlx::query(
        "INSERT INTO vulnerability_findings (id, subject_kind, app_id, project_id, image_digest, \
         node_id, component, vuln_id, aliases, summary, ecosystem, package, source_package, \
         installed_version, fixed_version, severity, cvss_score, cvss_vector, kev, epss, \
         first_seen_at, updated_at, resolved_at, resolved_reason) \
         VALUES (?, ?, ?, NULL, ?, ?, ?, ?, '[\"CVE-2026-9999\"]', 'A test finding', 'Debian:13', \
         ?, NULL, '1.0', NULL, ?, NULL, NULL, ?, NULL, '2026-01-01T00:00:00Z', \
         '2026-01-01T00:00:00Z', ?, ?)",
    )
    .bind(&finding.id)
    .bind(finding.subject_kind)
    .bind(&finding.app_id)
    .bind(&finding.image_digest)
    .bind(&finding.node_id)
    .bind(&finding.component)
    .bind(&finding.vuln_id)
    .bind(&finding.package)
    .bind(finding.severity)
    .bind(i64::from(finding.kev))
    .bind(resolved_at)
    .bind(resolved_reason)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// Keeps one inventory for `image_digest`.
async fn put_inventory(
    server: &Server,
    image_digest: &str,
    package_count: u64,
    ecosystems: &[&str],
) {
    let packages = if package_count == 0 {
        Vec::new()
    } else {
        vec![InventoryPackageWire {
            ecosystem: ecosystems.first().unwrap_or(&"Debian:13").to_string(),
            name: "bash".to_string(),
            source_name: None,
            version: "5.2.15-2".to_string(),
            purl: "pkg:deb/debian/bash@5.2.15-2".to_string(),
            path: None,
        }]
    };
    let object = InventoryObject {
        image_digest: image_digest.to_string(),
        inventoried_at: "2026-01-01T00:00:00Z".to_string(),
        os: Some(InventoryOs {
            id: "debian".to_string(),
            version_id: Some("13".to_string()),
        }),
        ecosystems: ecosystems.iter().map(|name| (*name).to_string()).collect(),
        not_inventoried: vec!["static_binary".to_string()],
        package_count,
        packages,
    };
    let sha = sha256_hex(&object);
    let json = serde_json::to_string(&object).unwrap();
    server
        .state
        .vulns
        .cache()
        .put_inventory(image_digest, &sha, &json)
        .await
        .unwrap();
}

/// Puts one record into one OSV bucket, which is what makes the cache hold it.
async fn put_bucket(server: &Server, bucket: &str) {
    let record = Record {
        id: "CVE-2026-0001".to_string(),
        modified: "2026-01-01T00:00:00Z".to_string(),
        ..Record::default()
    };
    server
        .state
        .vulns
        .cache()
        .replace_bucket(bucket, vec![record])
        .await
        .unwrap();
}

/// One `osv_mirror` row: the data's age for one ecosystem.
async fn insert_mirror(server: &Server, ecosystem: &str, source: &str) {
    sqlx::query(
        "INSERT INTO osv_mirror (ecosystem, source, record_count, data_modified_at, refreshed_at, \
         checked_at, last_error) \
         VALUES (?, ?, 1, '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z', \
         '2026-01-02T00:00:00Z', NULL) \
         ON CONFLICT(ecosystem) DO UPDATE SET source = excluded.source",
    )
    .bind(ecosystem)
    .bind(source)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// Inserts a connected node with the `node` role.
async fn insert_node(server: &Server, id: &str) {
    sqlx::query(
        "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
         labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
         log_seq, last_seen_at, created_at) \
         VALUES (?, ?, '[\"node\"]', NULL, 'connected', '', NULL, 1, '{}', '{}', '{}', \
         '{\"available\": true, \"kernel\": \"6.9.0\"}', NULL, NULL, 0, 0, 0, \
         '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    )
    .bind(id)
    .bind(id)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

async fn report(owner: &reqwest::Client, server: &Server, app_id: &str, query: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/vulnerabilities{query}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "report: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

// ---------------------------------------------------------------------------
// The app report
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_app_without_an_image_says_no_image_and_counts_nothing() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let body = report(&owner, &server, &app_id, "").await;
    assert_eq!(body["app_id"], app_id);
    assert_eq!(body["inventory"]["status"], "no_image");
    assert!(body["inventory"].get("image_digest").is_none());
    assert_eq!(body["items"], json!([]));
    assert_eq!(
        body["counts"],
        json!({"critical": 0, "high": 0, "medium": 0, "low": 0, "unknown": 0, "kev": 0})
    );
    assert_eq!(body["next_cursor"], Value::Null);
}

#[tokio::test]
async fn the_inventory_status_follows_the_digest_the_inventory_and_the_data() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;

    // A digest with no inventory yet.
    let pending_app = create_app(&owner, &server, &project_id, "pending").await;
    let pending = digest(b'2');
    set_current_image(&server, &pending_app, &pending, 1).await;
    assert_eq!(
        report(&owner, &server, &pending_app, "").await["inventory"]["status"],
        "pending"
    );

    // An inventory with no packages.
    let empty_app = create_app(&owner, &server, &project_id, "empty").await;
    let empty = digest(b'3');
    set_current_image(&server, &empty_app, &empty, 2).await;
    put_inventory(&server, &empty, 0, &[]).await;
    assert_eq!(
        report(&owner, &server, &empty_app, "").await["inventory"]["status"],
        "not_inventoried"
    );

    // A Debian:13 inventory with no Debian bucket in the cache.
    let waiting_app = create_app(&owner, &server, &project_id, "waiting").await;
    let waiting = digest(b'4');
    set_current_image(&server, &waiting_app, &waiting, 3).await;
    put_inventory(&server, &waiting, 1, &["Debian:13"]).await;
    assert_eq!(
        report(&owner, &server, &waiting_app, "").await["inventory"]["status"],
        "pending_data"
    );

    // With the bucket present the image is fully inventoried.
    let ready_app = create_app(&owner, &server, &project_id, "ready").await;
    let ready = digest(b'5');
    set_current_image(&server, &ready_app, &ready, 4).await;
    put_inventory(&server, &ready, 1, &["Debian:13"]).await;
    put_bucket(&server, "Debian").await;
    let inventory = report(&owner, &server, &ready_app, "").await["inventory"].clone();
    assert_eq!(inventory["status"], "inventoried");
    assert_eq!(inventory["os"], "debian 13");
    assert_eq!(inventory["image_digest"], ready);
    assert_eq!(inventory["package_count"], 1);
    assert_eq!(inventory["ecosystems"], json!(["Debian:13"]));
    assert_eq!(inventory["not_inventoried"], json!(["static_binary"]));
    assert_eq!(inventory["inventoried_at"], "2026-01-01T00:00:00Z");
}

#[tokio::test]
async fn findings_are_ordered_filtered_counted_and_paged() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let image = digest(b'7');
    set_current_image(&server, &app_id, &image, 1).await;
    put_inventory(&server, &image, 1, &["Debian:13"]).await;

    insert_finding(
        &server,
        &Finding::app(1, &app_id, &image, "critical", "CVE-2026-0001", "aaa", true),
    )
    .await;
    insert_finding(
        &server,
        &Finding::app(
            2,
            &app_id,
            &image,
            "critical",
            "CVE-2026-0002",
            "bbb",
            false,
        ),
    )
    .await;
    insert_finding(
        &server,
        &Finding::app(3, &app_id, &image, "high", "CVE-2026-0003", "ccc", false),
    )
    .await;
    insert_finding(
        &server,
        &Finding::app(4, &app_id, &image, "medium", "CVE-2026-0004", "ddd", false),
    )
    .await;
    insert_finding(
        &server,
        &Finding::app(5, &app_id, &image, "low", "CVE-2026-0005", "eee", false),
    )
    .await;
    insert_finding(
        &server,
        &Finding::app(6, &app_id, &image, "high", "CVE-2026-0006", "fff", false)
            .resolved("2026-01-05T00:00:00Z"),
    )
    .await;
    insert_finding(
        &server,
        &Finding::app(
            7,
            &app_id,
            &image,
            "critical",
            "CVE-2026-0007",
            "ggg",
            false,
        )
        .resolved("2026-01-06T00:00:00Z"),
    )
    .await;

    let ids = |body: &Value| -> Vec<String> {
        body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["vuln_id"].as_str().unwrap().to_string())
            .collect()
    };

    let body = report(&owner, &server, &app_id, "").await;
    assert_eq!(
        ids(&body),
        [
            "CVE-2026-0001",
            "CVE-2026-0002",
            "CVE-2026-0003",
            "CVE-2026-0004",
            "CVE-2026-0005"
        ],
        "open findings, KEV first within a severity"
    );
    assert_eq!(
        body["counts"],
        json!({"critical": 2, "high": 1, "medium": 1, "low": 1, "unknown": 0, "kev": 1})
    );

    let resolved = report(&owner, &server, &app_id, "?include_resolved=true").await;
    assert_eq!(
        ids(&resolved),
        [
            "CVE-2026-0001",
            "CVE-2026-0002",
            "CVE-2026-0003",
            "CVE-2026-0004",
            "CVE-2026-0005",
            "CVE-2026-0007",
            "CVE-2026-0006"
        ],
        "resolved findings come after the open ones, newest first"
    );

    let high = report(&owner, &server, &app_id, "?severity=high").await;
    assert_eq!(
        ids(&high),
        ["CVE-2026-0001", "CVE-2026-0002", "CVE-2026-0003"]
    );
    assert_eq!(high["counts"], body["counts"], "counts ignore the filters");

    let kev = report(&owner, &server, &app_id, "?kev=true").await;
    assert_eq!(ids(&kev), ["CVE-2026-0001"]);

    // One page of two, followed through its cursor.
    let first = report(&owner, &server, &app_id, "?limit=2").await;
    assert_eq!(ids(&first), ["CVE-2026-0001", "CVE-2026-0002"]);
    let cursor = first["next_cursor"].as_str().expect("a next cursor");
    let second = report(
        &owner,
        &server,
        &app_id,
        &format!("?limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(ids(&second), ["CVE-2026-0003", "CVE-2026-0004"]);
    let cursor = second["next_cursor"].as_str().expect("a next cursor");
    let third = report(
        &owner,
        &server,
        &app_id,
        &format!("?limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(ids(&third), ["CVE-2026-0005"]);
    assert_eq!(third["next_cursor"], Value::Null);

    let res = owner
        .get(server.url(&format!(
            "/api/v1/apps/{app_id}/vulnerabilities?cursor=not-a-cursor"
        )))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn the_report_is_not_found_unknown_and_readable_by_a_viewer() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let res = owner
        .get(server.url("/api/v1/apps/app_01JC9Z6Q5W8N3K4T7R2M1XHB00/vulnerabilities"))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    server
        .insert_user("viewer@example.com", "viewer password", "viewer")
        .await;
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", "viewer password")
        .await;
    let body: Value = viewer
        .get(server.url(&format!("/api/v1/apps/{app_id}/vulnerabilities")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["app_id"], app_id);
}

#[tokio::test]
async fn a_finding_serializes_with_the_contract_shape() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let image = digest(b'8');
    set_current_image(&server, &app_id, &image, 1).await;
    put_inventory(&server, &image, 1, &["Debian:13"]).await;
    put_bucket(&server, "Debian").await;
    insert_mirror(&server, "Debian", "download").await;
    insert_mirror(&server, "KEV", "download").await;
    insert_finding(
        &server,
        &Finding::app(
            1,
            &app_id,
            &image,
            "critical",
            "CVE-2026-0001",
            "bash",
            false,
        ),
    )
    .await;

    let body = report(&owner, &server, &app_id, "").await;
    assert_eq!(body["inventory"]["status"], "inventoried");
    assert_eq!(body["data"][0]["ecosystem"], "Debian");
    assert_eq!(body["data"][1]["ecosystem"], "KEV");
    assert_eq!(body["data"][0]["source"], "download");
    assert_eq!(body["data"][0]["data_modified_at"], "2026-01-01T00:00:00Z");
    assert_eq!(body["data"][0]["last_error"], Value::Null);

    let item = &body["items"][0];
    assert_eq!(item["id"], finding_id(1));
    assert_eq!(item["subject_kind"], "app");
    assert_eq!(item["app_id"], app_id);
    assert_eq!(item["image_digest"], image);
    assert_eq!(item["vuln_id"], "CVE-2026-0001");
    assert_eq!(item["aliases"], json!(["CVE-2026-9999"]));
    assert_eq!(item["summary"], "A test finding");
    assert_eq!(item["ecosystem"], "Debian:13");
    assert_eq!(item["package"], "bash");
    assert_eq!(item["installed_version"], "1.0");
    assert_eq!(item["severity"], "critical");
    assert_eq!(item["kev"], false);
    assert_eq!(item["first_seen_at"], "2026-01-01T00:00:00Z");
    // Required nullable properties are present as null.
    assert_eq!(item["fixed_version"], Value::Null);
    assert_eq!(item["cvss_score"], Value::Null);
    assert_eq!(item["epss"], Value::Null);
    assert_eq!(item["resolved_at"], Value::Null);
    assert_eq!(item["resolved_reason"], Value::Null);
    // Optional properties the row has no value for are absent.
    for absent in [
        "source_package",
        "cvss_vector",
        "node_id",
        "component",
        "upstream_version_only",
    ] {
        assert!(item.get(absent).is_none(), "{absent} must be absent");
    }
}

// ---------------------------------------------------------------------------
// The summary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_summary_orders_apps_and_filters_by_project() {
    let server = start().await;
    let owner = server.owner().await;
    let demo = create_project(&owner, &server, "demo").await;
    let other = create_project(&owner, &server, "other").await;
    let urgent = create_app(&owner, &server, &demo, "urgent").await;
    let quiet = create_app(&owner, &server, &demo, "quiet").await;
    let elsewhere = create_app(&owner, &server, &other, "elsewhere").await;

    let image = digest(b'9');
    set_current_image(&server, &urgent, &image, 1).await;
    insert_finding(
        &server,
        &Finding::app(
            1,
            &urgent,
            &image,
            "critical",
            "CVE-2026-0001",
            "aaa",
            false,
        ),
    )
    .await;

    let body: Value = owner
        .get(server.url("/api/v1/vulnerabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let apps: Vec<&str> = body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|app| app["app_id"].as_str().unwrap())
        .collect();
    assert_eq!(apps, [urgent.as_str(), quiet.as_str(), elsewhere.as_str()]);
    assert_eq!(body["apps"][0]["counts"]["critical"], 1);
    assert_eq!(body["apps"][0]["inventory_status"], "pending");
    assert_eq!(body["apps"][1]["inventory_status"], "no_image");
    assert_eq!(body["next_cursor"], Value::Null);

    let filtered: Value = owner
        .get(server.url(&format!("/api/v1/vulnerabilities?project_id={demo}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let apps: Vec<&str> = filtered["apps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|app| app["app_id"].as_str().unwrap())
        .collect();
    assert_eq!(apps, [urgent.as_str(), quiet.as_str()]);

    let res = owner
        .get(server.url("/api/v1/vulnerabilities?project_id=nope"))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn the_owner_sees_the_quoro_and_host_kernel_parts() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let node_id = "nod_01JC9Z6Q5W8N3K4T7R2M1XHB01";
    insert_node(&server, node_id).await;

    insert_finding(
        &server,
        &Finding {
            id: finding_id(1),
            subject_kind: "quoro",
            app_id: None,
            image_digest: None,
            node_id: None,
            component: Some("guest-kernel".to_string()),
            vuln_id: "CVE-2026-0010".to_string(),
            package: "linux".to_string(),
            severity: "high",
            kev: false,
            resolved_at: None,
        },
    )
    .await;
    insert_finding(
        &server,
        &Finding {
            id: finding_id(2),
            subject_kind: "host_kernel",
            app_id: None,
            image_digest: None,
            node_id: Some(node_id.to_string()),
            component: None,
            vuln_id: "CVE-2026-0011".to_string(),
            package: "linux".to_string(),
            severity: "critical",
            kev: true,
            resolved_at: None,
        },
    )
    .await;
    insert_mirror(&server, "KEV", "import").await;

    let body: Value = owner
        .get(server.url("/api/v1/vulnerabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["apps"][0]["app_id"], app_id);
    assert_eq!(body["data"][0]["ecosystem"], "KEV");
    assert_eq!(body["data"][0]["source"], "import");
    assert_eq!(body["quoro"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["quoro"]["counts"]["high"], 1);
    assert_eq!(body["quoro"]["items"][0]["component"], "guest-kernel");
    assert!(
        body["quoro"]["items"][0]
            .get("upstream_version_only")
            .is_none()
    );
    assert_eq!(body["host_kernels"][0]["node_id"], node_id);
    assert_eq!(body["host_kernels"][0]["kernel"], "6.9.0");
    assert_eq!(body["host_kernels"][0]["counts"]["critical"], 1);
    assert_eq!(body["host_kernels"][0]["counts"]["kev"], 1);
    assert_eq!(
        body["host_kernels"][0]["items"][0]["vuln_id"],
        "CVE-2026-0011"
    );
    assert_eq!(
        body["host_kernels"][0]["items"][0]["upstream_version_only"],
        true
    );
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

async fn owner_id(owner: &reqwest::Client, server: &Server) -> String {
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    me["user"]["id"].as_str().unwrap().to_string()
}

async fn create_agent(
    owner: &reqwest::Client,
    server: &Server,
    name: &str,
    owner_user_id: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": name,
            "model": "test-model",
            "owner_user_id": owner_user_id,
            "description": "a test agent",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create agent: {}",
        res.text().await.unwrap()
    );
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn agent_token(owner: &reqwest::Client, server: &Server, agent_id: &str) -> String {
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create key: {}",
        res.text().await.unwrap()
    );
    let key: Value = res.json().await.unwrap();
    let key = key["key"].as_str().unwrap().to_string();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"))
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "mint token: {}",
        res.text().await.unwrap()
    );
    let token: Value = res.json().await.unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

async fn grant(
    owner: &reqwest::Client,
    server: &Server,
    agent_id: &str,
    scopes: &[&str],
    selector: Value,
) {
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": scopes,
            "selector": selector,
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create grant: {}",
        res.text().await.unwrap()
    );
}

async fn agent_get(server: &Server, token: &str, path: &str) -> Value {
    let res = reqwest::Client::new()
        .get(server.url(path))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "agent read: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

#[tokio::test]
async fn an_agent_sees_its_projects_apps_and_no_node_data() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let demo = create_project(&owner, &server, "demo").await;
    let other = create_project(&owner, &server, "other").await;
    let mine = create_app(&owner, &server, &demo, "mine").await;
    let theirs = create_app(&owner, &server, &other, "theirs").await;
    let node_id = "nod_01JC9Z6Q5W8N3K4T7R2M1XHB01";
    insert_node(&server, node_id).await;
    insert_finding(
        &server,
        &Finding {
            id: finding_id(1),
            subject_kind: "host_kernel",
            app_id: None,
            image_digest: None,
            node_id: Some(node_id.to_string()),
            component: None,
            vuln_id: "CVE-2026-0011".to_string(),
            package: "linux".to_string(),
            severity: "high",
            kev: false,
            resolved_at: None,
        },
    )
    .await;

    let agent_id = create_agent(&owner, &server, "reader", &owner_user_id).await;
    grant(
        &owner,
        &server,
        &agent_id,
        &["app:read"],
        json!({"projects": ["demo"]}),
    )
    .await;
    let token = agent_token(&owner, &server, &agent_id).await;

    let body = agent_get(&server, &token, "/api/v1/vulnerabilities").await;
    let apps: Vec<&str> = body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|app| app["app_id"].as_str().unwrap())
        .collect();
    assert_eq!(apps, [mine.as_str()], "only the granted project's apps");
    assert!(apps.iter().all(|app| *app != theirs));
    assert!(body.get("quoro").is_none());
    assert!(body.get("host_kernels").is_none());

    // The summary is an agent read, recorded against the cluster.
    let row: (String, String, Option<String>) = sqlx::query_as(
        "SELECT target_type, target_id, result FROM audit_log \
         WHERE operation = 'GET /api/v1/vulnerabilities' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(row.0, "cluster");
    assert_eq!(row.1, server.state.cluster_id);
    assert_eq!(row.2.as_deref(), Some("ok"));
    // Leaving node data out is not a refusal.
    let (refusals,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM audit_log WHERE scope = 'node:read' AND result = 'refused'",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(refusals, 0);
}

#[tokio::test]
async fn an_agent_with_cluster_wide_node_read_sees_the_node_data() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let demo = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &demo, "web").await;
    let node_id = "nod_01JC9Z6Q5W8N3K4T7R2M1XHB01";
    insert_node(&server, node_id).await;
    insert_finding(
        &server,
        &Finding {
            id: finding_id(1),
            subject_kind: "quoro",
            app_id: None,
            image_digest: None,
            node_id: None,
            component: Some("cloud-hypervisor".to_string()),
            vuln_id: "CVE-2026-0010".to_string(),
            package: "cloud-hypervisor".to_string(),
            severity: "medium",
            kev: false,
            resolved_at: None,
        },
    )
    .await;
    insert_finding(
        &server,
        &Finding {
            id: finding_id(2),
            subject_kind: "host_kernel",
            app_id: None,
            image_digest: None,
            node_id: Some(node_id.to_string()),
            component: None,
            vuln_id: "CVE-2026-0011".to_string(),
            package: "linux".to_string(),
            severity: "low",
            kev: false,
            resolved_at: None,
        },
    )
    .await;

    let agent_id = create_agent(&owner, &server, "operator", &owner_user_id).await;
    grant(
        &owner,
        &server,
        &agent_id,
        &["app:read", "node:read"],
        json!({"all": true}),
    )
    .await;
    let token = agent_token(&owner, &server, &agent_id).await;

    let body = agent_get(&server, &token, "/api/v1/vulnerabilities").await;
    assert_eq!(body["apps"][0]["app_id"], app_id);
    assert_eq!(body["quoro"]["items"][0]["component"], "cloud-hypervisor");
    assert_eq!(body["host_kernels"][0]["node_id"], node_id);
    assert_eq!(
        body["host_kernels"][0]["items"][0]["vuln_id"],
        "CVE-2026-0011"
    );
}

//! Linked folders, projects, apps and `status`, end to end against an
//! in-process primary. No engine runs, so a start or a stop only changes the
//! desired state: the tests assert on what the API answers.

mod common;

use common::{Output, Primary, key_file, quoro, start, write_credentials};
use quoro_cli::{Environment, exit};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A primary with the project `demo` and its app `web`, a human's context
/// `lab` with the owner's token, and a work folder.
struct Lab {
    primary: Primary,
    owner_id: String,
    owner_token: String,
    app_id: String,
    project_id: String,
    home: PathBuf,
    work: PathBuf,
}

async fn lab() -> Lab {
    let primary = start().await;
    let (owner_id, owner_token) = primary.owner_token().await;
    let app_id = primary.app(&owner_token).await;
    let (_, app) = primary
        .fixture(
            reqwest::Method::GET,
            &format!("/api/v1/apps/{app_id}"),
            &owner_token,
            None,
        )
        .await;
    let project_id = app["project_id"].as_str().unwrap().to_string();
    let home = primary.config_home("home");
    write_credentials(&home, &owner_token, 0o600);
    let work = primary.dir.join("work");
    std::fs::create_dir_all(work.join("sub/deeper")).unwrap();
    Lab {
        primary,
        owner_id,
        owner_token,
        app_id,
        project_id,
        home,
        work,
    }
}

impl Lab {
    /// Rolls the app's image out once, as the owner, and marks the
    /// deployment current as a node would once it ran (no engine runs here):
    /// a start or a stop needs one.
    async fn deploy(&self) {
        let (status, body) = self
            .primary
            .fixture(
                reqwest::Method::POST,
                &format!("/api/v1/apps/{}/deployments", self.app_id),
                &self.owner_token,
                Some(json!({ "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000" })),
            )
            .await;
        assert!((200..300).contains(&status), "deploy: {status} {body}");
        sqlx::query("UPDATE apps SET current_deployment_id = ? WHERE id = ?")
            .bind(body["id"].as_str().unwrap())
            .bind(&self.app_id)
            .execute(self.primary.state.store.pool())
            .await
            .unwrap();
    }

    fn environment(&self, cwd: &Path) -> Environment {
        self.primary.context_environment(&self.home).with_cwd(cwd)
    }

    async fn run(&self, cwd: &Path, args: &[&str]) -> Output {
        quoro(args, &self.environment(cwd)).await
    }

    async fn ok(&self, cwd: &Path, args: &[&str]) -> Output {
        let output = self.run(cwd, args).await;
        assert_eq!(output.code, exit::OK, "{args:?}: {}", output.stderr);
        output
    }

    async fn json(&self, cwd: &Path, args: &[&str]) -> Value {
        let output = self.ok(cwd, &[args, &["--json"]].concat()).await;
        serde_json::from_str(&output.stdout)
            .unwrap_or_else(|e| panic!("{args:?}: {e}: {}", output.stdout))
    }

    fn links(&self) -> String {
        std::fs::read_to_string(self.home.join("quoro/links.toml")).unwrap_or_default()
    }

    /// A GET as the owner, on the fixture listener.
    async fn get(&self, path: &str) -> (u16, Value) {
        self.primary
            .fixture(reqwest::Method::GET, path, &self.owner_token, None)
            .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_link_is_found_from_below_and_removed_by_unlink() {
    let lab = lab().await;
    let deeper = lab.work.join("sub/deeper");

    // Without a link, status needs one (or --cluster).
    let output = lab.run(&deeper, &["status"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("not linked"), "{}", output.stderr);
    // Without a terminal, link needs the names.
    let output = lab.run(&lab.work, &["link"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);

    let output = lab.ok(&lab.work, &["link", "demo", "web"]).await;
    assert_eq!(
        output.stdout,
        format!(
            "Linked {} to the project demo and the app web on lab.\n",
            lab.work.display()
        )
    );
    let links = lab.links();
    assert!(
        links.contains(&format!("[links.\"{}\"]", lab.work.display()))
            && links.contains("context = \"lab\"")
            && links.contains(&format!("project_id = \"{}\"", lab.project_id))
            && links.contains(&format!("app_id = \"{}\"", lab.app_id)),
        "{links}"
    );
    assert!(
        !lab.work.join(".quoro").exists(),
        "nothing inside the folder"
    );

    // Two levels down, the link above applies.
    let status = lab.json(&deeper, &["status"]).await;
    assert_eq!(status["app"]["id"], lab.app_id.as_str());

    // Unlink from below removes the link above.
    let output = lab.ok(&deeper, &["unlink"]).await;
    assert!(
        output
            .stdout
            .starts_with(&format!("Unlinked {} ", lab.work.display())),
        "{}",
        output.stdout
    );
    assert!(!lab.links().contains("app_id"), "{}", lab.links());
    let output = lab.ok(&deeper, &["unlink"]).await;
    assert!(output.stdout.contains("is not linked"), "{}", output.stdout);
    assert_eq!(lab.run(&deeper, &["status"]).await.code, exit::USAGE);

    // Ids link too, and on a terminal the lists are offered by number.
    lab.ok(&lab.work, &["link", &lab.project_id, &lab.app_id])
        .await;
    assert_eq!(
        lab.json(&lab.work, &["status"]).await["app"]["id"],
        lab.app_id.as_str()
    );
    let picked = quoro(
        &["link"],
        &lab.environment(&deeper).with_stdin("1\n\n", true),
    )
    .await;
    assert_eq!(picked.code, exit::OK, "{}", picked.stderr);
    assert!(
        picked.stderr.contains("1) demo (Demo)"),
        "{}",
        picked.stderr
    );
    assert!(
        picked.stdout.contains("to the project demo on lab"),
        "{}",
        picked.stdout
    );
    // The nearer link now wins: the project, without an app.
    let status = lab.json(&deeper, &["status"]).await;
    assert!(status.get("app").is_none(), "{status}");
    assert_eq!(status["project"]["id"], lab.project_id.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_create_links_the_folder_and_app_create_fills_the_link() {
    let lab = lab().await;
    let output = lab.ok(&lab.work, &["project", "create", "My Blog"]).await;
    assert!(
        output
            .stdout
            .starts_with("Created the project my-blog (prj_"),
        "{}",
        output.stdout
    );
    assert!(
        output
            .stdout
            .contains(&format!("Linked {} to it", lab.work.display())),
        "{}",
        output.stdout
    );
    let projects = lab.json(&lab.work, &["project", "list"]).await;
    let blog = projects["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["slug"] == "my-blog")
        .expect("listed")
        .clone();
    let listed = lab.ok(&lab.work, &["project", "list"]).await;
    assert!(
        listed
            .stdout
            .lines()
            .any(|l| l.starts_with('*') && l.contains("my-blog")),
        "{}",
        listed.stdout
    );

    // A linked folder is not linked again.
    let output = lab
        .ok(
            &lab.work,
            &["project", "create", "Other", "--slug", "other"],
        )
        .await;
    assert!(!output.stdout.contains("Linked"), "{}", output.stdout);
    assert!(lab.links().contains(blog["id"].as_str().unwrap()));
    // A bad slug is refused before anything is sent.
    let output = lab
        .run(&lab.work, &["project", "create", "X", "--slug", "Not OK"])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);

    // The app lands in the linked project and fills the link.
    let image = "ghcr.io/example/api@sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let output = lab
        .ok(
            &lab.work,
            &["app", "create", "api", "--image", image, "--port", "3000"],
        )
        .await;
    assert!(
        output.stdout.contains("Created the app api (app_")
            && output.stdout.contains("in the project my-blog")
            && output.stdout.contains("Linked"),
        "{}",
        output.stdout
    );
    let status = lab.json(&lab.work, &["status"]).await;
    assert_eq!(status["app"]["name"], "api");
    assert_eq!(status["app"]["image"], image);
    assert_eq!(status["app"]["port"], 3000);
    assert_eq!(status["app"]["project_id"], blog["id"]);

    // A Git app; the link already has an app, so it stays.
    let created = lab
        .json(
            &lab.work,
            &[
                "app",
                "create",
                "site",
                "--repo",
                "https://github.com/example/site",
                "--ref",
                "v1",
                "--dockerfile",
                "docker/Dockerfile",
            ],
        )
        .await;
    assert_eq!(created["source"]["kind"], "git");
    assert_eq!(
        created["source"]["repo_url"],
        "https://github.com/example/site"
    );
    assert_eq!(created["source"]["ref"], "v1");
    assert_eq!(created["source"]["dockerfile_path"], "docker/Dockerfile");
    assert_eq!(created["port"], 8080);
    assert_eq!(lab.json(&lab.work, &["status"]).await["app"]["name"], "api");
    let apps = lab.json(&lab.work, &["app", "list"]).await;
    let names: Vec<&str> = apps["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|a| a["name"].as_str())
        .collect();
    assert_eq!(names.len(), 2, "{apps}");
    assert!(names.contains(&"api") && names.contains(&"site"), "{apps}");

    // --image and --repo are exclusive, and one is needed.
    for args in [
        &["app", "create", "x"][..],
        &[
            "app",
            "create",
            "x",
            "--image",
            image,
            "--repo",
            "https://g/x",
        ],
        &["app", "create", "x", "--image", image, "--ref", "main"],
    ] {
        let output = lab.run(&lab.work, args).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_delete_needs_a_confirmation() {
    let lab = lab().await;
    let requests = lab.primary.requests();
    let output = lab.run(&lab.work, &["project", "delete", "demo"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("pass --yes"), "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");

    // On a terminal, a wrong name stops it.
    let output = quoro(
        &["project", "delete", "demo"],
        &lab.environment(&lab.work).with_stdin("dmeo\n", true),
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("Type its name (demo)")
            && output.stderr.contains("nothing was deleted"),
        "{}",
        output.stderr
    );

    // With --yes the call is sent; this primary does not serve deleteProject
    // yet, and the client says so.
    let output = lab
        .run(&lab.work, &["project", "delete", "demo", "--yes"])
        .await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("does not implement deleteProject yet"),
        "{}",
        output.stderr
    );
    let (status, _) = lab
        .get(&format!("/api/v1/projects/{}", lab.project_id))
        .await;
    assert_eq!(status, 200, "the project is still there");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_human_updates_starts_stops_and_deletes_the_linked_app() {
    let lab = lab().await;
    lab.ok(&lab.work, &["link", "demo", "web"]).await;

    let output = lab.run(&lab.work, &["app", "update"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    let before = lab.json(&lab.work, &["status"]).await["app"].clone();
    let updated = lab
        .json(
            &lab.work,
            &[
                "app",
                "update",
                "--vcpus",
                "2",
                "--memory",
                "1024",
                "--port",
                "9090",
                "--health-path",
                "/healthz",
            ],
        )
        .await;
    assert_eq!(updated["resources"]["vcpus"], 2);
    assert_eq!(updated["resources"]["memory_mb"], 1024);
    assert_eq!(
        updated["resources"]["disk_gb"], before["resources"]["disk_gb"],
        "the disk keeps its size"
    );
    assert_eq!(updated["port"], 9090);
    assert_eq!(updated["health_check"]["path"], "/healthz");
    assert_eq!(
        updated["health_check"]["interval_s"],
        before["health_check"]["interval_s"]
    );
    let output = lab
        .ok(&lab.work, &["app", "update", "--port", "8081"])
        .await;
    assert!(
        output.stdout.contains("port 8081, health /healthz"),
        "{}",
        output.stdout
    );

    lab.deploy().await;
    let status = lab.json(&lab.work, &["status"]).await;
    assert!(
        status["deployments"]["items"][0]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("dep_")),
        "{status}"
    );
    let stopped = lab.json(&lab.work, &["app", "stop"]).await;
    assert_eq!(stopped["id"], lab.app_id.as_str());
    assert_eq!(stopped["status"], "stopped");
    let started = lab.json(&lab.work, &["app", "start"]).await;
    assert_eq!(started["id"], lab.app_id.as_str());
    assert_ne!(started["status"], "stopped", "{started}");

    // Deleting needs --yes without a terminal.
    let output = lab.run(&lab.work, &["app", "delete"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    let (status, _) = lab.get(&format!("/api/v1/apps/{}", lab.app_id)).await;
    assert_eq!(status, 200);
    let output = lab.ok(&lab.work, &["app", "delete", "--yes"]).await;
    assert!(
        output.stdout.starts_with("Deleting the app web"),
        "{}",
        output.stdout
    );
    let (status, _) = lab.get(&format!("/api/v1/apps/{}", lab.app_id)).await;
    assert_eq!(status, 404, "the app is gone");
    assert!(!lab.links().contains("app_id"), "the link forgot the app");
    let status = lab.json(&lab.work, &["status"]).await;
    assert_eq!(status["apps"]["items"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_shows_the_app_the_project_and_the_cluster() {
    let lab = lab().await;
    lab.ok(&lab.work, &["link", "demo", "web"]).await;

    let output = lab.ok(&lab.work, &["status"]).await;
    let text = &output.stdout;
    for expected in [
        format!("context   lab ({})", lab.primary.url),
        format!("project   demo ({})", lab.project_id),
        format!("app       web ({}), ", lab.app_id),
        "source    ghcr.io/example/web@sha256:".to_string(),
        "settings  replicas 1, ".to_string(),
        "domains   none".to_string(),
        "metrics   ".to_string(),
        "vulns     ".to_string(),
        format!("panel     {}/apps/{}", lab.primary.url, lab.app_id),
    ] {
        assert!(text.contains(&expected), "{expected:?} in:\n{text}");
    }

    let status = lab.json(&lab.work, &["status"]).await;
    for key in [
        "project",
        "app",
        "deployments",
        "metrics",
        "vulnerabilities",
        "panel_url",
    ] {
        assert!(status.get(key).is_some(), "{key}: {status}");
    }
    assert_eq!(status["app"]["id"], lab.app_id.as_str());
    assert!(status["app"]["domains"].is_array(), "getApp's detail");
    assert!(status["deployments"]["items"].is_array());
    assert!(status.get("cluster").is_none());
    // A section that could not be read is null and says why.
    for key in ["metrics", "vulnerabilities"] {
        if status[key].is_null() {
            assert!(status["errors"][key].is_string(), "{status}");
        }
    }

    // A project link shows the project and its apps.
    let project = lab.json(&lab.work, &["status", "--project", "Demo"]).await;
    assert_eq!(project["project"]["slug"], "demo");
    assert_eq!(project["apps"]["items"][0]["id"], lab.app_id.as_str());
    assert_eq!(project["panel_url"], format!("{}/", lab.primary.url));

    // --cluster works without a link and adds the cluster and its nodes.
    let elsewhere = lab.primary.dir.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let cluster = lab.json(&elsewhere, &["status", "--cluster"]).await;
    assert!(cluster.get("app").is_none(), "{cluster}");
    assert!(cluster["cluster"]["version"].is_string(), "{cluster}");
    assert_eq!(cluster["nodes"]["items"][0]["name"], "test-node");
    let output = lab.ok(&elsewhere, &["status", "--cluster"]).await;
    for expected in [
        "cluster   ",
        "primary   ",
        "votes     ",
        "nodes\n  test-node",
    ] {
        assert!(
            output.stdout.contains(expected),
            "{expected:?} in:\n{}",
            output.stdout
        );
    }
    let both = lab.json(&lab.work, &["status", "--cluster"]).await;
    assert!(both.get("app").is_some() && both.get("cluster").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn names_and_ids_resolve_and_an_ambiguous_name_names_its_candidates() {
    let lab = lab().await;
    let elsewhere = lab.primary.dir.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    // A second project with an app also called web.
    let other = lab
        .json(
            &elsewhere,
            &["project", "create", "Other", "--slug", "other"],
        )
        .await;
    let other_id = other["id"].as_str().unwrap().to_string();
    let image = "ghcr.io/example/web@sha256:2222222222222222222222222222222222222222222222222222222222222222";
    let other_web = lab
        .json(
            &elsewhere,
            &[
                "app",
                "create",
                "web",
                "--image",
                image,
                "--project",
                "other",
            ],
        )
        .await;
    let other_web_id = other_web["id"].as_str().unwrap().to_string();
    // project create linked the folder; the names below are resolved without
    // a link.
    lab.ok(&elsewhere, &["unlink"]).await;

    let output = lab.run(&elsewhere, &["status", "--app", "web"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("matches 2 apps")
            && output.stderr.contains(&lab.app_id)
            && output.stderr.contains(&other_web_id),
        "{}",
        output.stderr
    );
    for (args, expected) in [
        (&["--app", "web", "--project", "other"][..], &other_web_id),
        (&["--app", "web", "--project", &other_id][..], &other_web_id),
        (&["--app", "web", "--project", "Demo"][..], &lab.app_id),
        (&["--app", lab.app_id.as_str()][..], &lab.app_id),
    ] {
        let status = lab.json(&elsewhere, &[&["status"], args].concat()).await;
        assert_eq!(status["app"]["id"], expected.as_str(), "{args:?}");
    }
    let output = lab.run(&elsewhere, &["status", "--project", "nope"]).await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);

    // Two projects with the same display name are ambiguous by name.
    lab.ok(
        &elsewhere,
        &["project", "create", "Demo", "--slug", "demo-2"],
    )
    .await;
    let output = lab.run(&elsewhere, &["status", "--project", "Demo"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("matches 2 projects")
            && output.stderr.contains("demo (")
            && output.stderr.contains("demo-2 ("),
        "{}",
        output.stderr
    );
    // The slug still names one.
    let status = lab
        .json(&elsewhere, &["status", "--project", "demo-2"])
        .await;
    assert_eq!(status["project"]["slug"], "demo-2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_deletes_an_app_through_the_approval_flow() {
    let lab = lab().await;
    let (agent_id, key) = lab
        .primary
        .agent(&lab.owner_token, &lab.owner_id, "operator")
        .await;
    lab.primary
        .grant(
            &lab.owner_token,
            &agent_id,
            &[
                "project:read",
                "app:read",
                "app:stop",
                "approval:create",
                "approval:read",
                "event:read",
            ],
            json!({ "projects": ["demo"] }),
        )
        .await;
    let agent_home = lab.primary.dir.join("agent-home");
    let key_path = key_file(&agent_home.join("keys"), "operator.key", &key, 0o600);
    let environment = Environment::new(
        [
            (
                "XDG_CONFIG_HOME",
                agent_home.join("config").display().to_string(),
            ),
            (
                "XDG_RUNTIME_DIR",
                agent_home.join("run").display().to_string(),
            ),
        ],
        false,
    )
    .with_cwd(&lab.work);
    let key_path = key_path.display().to_string();
    let login = quoro(
        &[
            "--context",
            "lab",
            "login",
            "--agent",
            "--key-file",
            &key_path,
            "--url",
            &lab.primary.url,
            "--fingerprint",
            &lab.primary.fingerprint,
        ],
        &environment,
    )
    .await;
    assert_eq!(login.code, exit::OK, "{}", login.stderr);
    lab.deploy().await;
    let run = |args: &'static [&'static str]| quoro(args, &environment);
    assert_eq!(run(&["link", "demo", "web"]).await.code, exit::OK);

    // A change without a reason is refused before anything is sent.
    let mints = lab.primary.mints().await;
    let requests = lab.primary.requests();
    let output = run(&["app", "stop"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("--reason"), "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests);
    assert_eq!(lab.primary.mints().await, mints);
    let output = run(&["app", "stop", "--reason", "quiet hours"]).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let (justification, actor, operation) = lab.primary.last_actor().await;
    assert_eq!(
        (justification.as_deref(), actor.as_str(), operation.as_str()),
        (
            Some("quiet hours"),
            "agent",
            "POST /api/v1/apps/{app_id}/stop"
        )
    );

    // No --yes needed: the primary's approval is the agent's check.
    let output = run(&["app", "delete", "--reason", "retire it"]).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("scope app:delete on app {}", lab.app_id)),
        "{}",
        output.stderr
    );
    let output = run(&[
        "app",
        "delete",
        "--reason",
        "retire it",
        "--request-approval",
    ])
    .await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    let first = output.stdout.trim().to_string();
    assert!(first.starts_with("apr_"), "{}", output.stdout);

    let wait = run(&[
        "app",
        "delete",
        "--reason",
        "retire it",
        "--request-approval",
        "--wait",
    ]);
    let (output, approved) = tokio::join!(wait, approve(&lab, &first));
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("approval {approved} approved")),
        "{}",
        output.stderr
    );
    assert!(
        output.stdout.starts_with("Deleting the app web"),
        "{}",
        output.stdout
    );
    let (status, _) = lab.get(&format!("/api/v1/apps/{}", lab.app_id)).await;
    assert_eq!(status, 404, "the app is gone");
}

/// Approves the newest pending approval other than `except`, as the owner.
async fn approve(lab: &Lab, except: &str) -> String {
    let pool = lab.primary.state.store.pool();
    for _ in 0..200 {
        let pending: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM approvals WHERE status = 'pending' AND id <> ? \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(except)
        .fetch_optional(pool)
        .await
        .unwrap();
        if let Some((id,)) = pending {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let (status, body) = lab
                .primary
                .fixture(
                    reqwest::Method::POST,
                    &format!("/api/v1/approvals/{id}/approve"),
                    &lab.owner_token,
                    Some(json!({ "note": "approved by the test" })),
                )
                .await;
            assert_eq!(status, 200, "approve: {body}");
            return id;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no pending approval");
}

//! `up`, `deploy`, `redeploy`, `restart`, `build` and `logs`, end to end
//! against an in-process primary.
//!
//! No engine and no node agent run: a deployment only changes the desired
//! state, a build stays `queued` (no node takes it), and every read that goes
//! to a node (app, build and deployment logs and their streams) answers
//! `node_unreachable` unless the app has no instance to read from. The tests
//! assert on what the API answers and records.

mod common;

use common::{Output, Primary, key_file, quoro, start, write_credentials};
use quoro_cli::{Environment, exit};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A ref-pinned image the in-process primary accepts as it is.
const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";
const IMAGE_2: &str =
    "ghcr.io/example/web@sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// A primary with the project `demo`, its image app `web` and its Git app
/// `api`; a human's context `lab` with the owner's token; and two folders,
/// `web/` linked to `web` and `api/` linked to `api`.
struct Lab {
    primary: Primary,
    owner_id: String,
    owner_token: String,
    web_id: String,
    api_id: String,
    home: PathBuf,
    web: PathBuf,
    api: PathBuf,
}

async fn lab() -> Lab {
    let primary = start().await;
    let (owner_id, owner_token) = primary.owner_token().await;
    let web_id = primary.app(&owner_token).await;
    let (_, web) = primary
        .fixture(
            reqwest::Method::GET,
            &format!("/api/v1/apps/{web_id}"),
            &owner_token,
            None,
        )
        .await;
    let (status, api) = primary
        .fixture(
            reqwest::Method::POST,
            "/api/v1/apps",
            &owner_token,
            Some(json!({
                "project_id": web["project_id"],
                "name": "api",
                "source": {
                    "kind": "git",
                    "repo_url": "https://github.com/example/api",
                    "ref": "main",
                },
                "port": 8080,
            })),
        )
        .await;
    assert_eq!(status, 201, "create the Git app: {api}");
    let home = primary.config_home("home");
    write_credentials(&home, &owner_token, 0o600);
    let lab = Lab {
        web_id,
        api_id: api["id"].as_str().unwrap().to_string(),
        web: primary.dir.join("web"),
        api: primary.dir.join("api"),
        primary,
        owner_id,
        owner_token,
        home,
    };
    for (dir, app) in [(&lab.web, "web"), (&lab.api, "api")] {
        std::fs::create_dir_all(dir).unwrap();
        lab.ok(dir, &["link", "demo", app]).await;
    }
    lab
}

impl Lab {
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

    /// A GET as the owner, on the fixture listener.
    async fn get(&self, path: &str) -> Value {
        let (status, body) = self
            .primary
            .fixture(reqwest::Method::GET, path, &self.owner_token, None)
            .await;
        assert_eq!(status, 200, "GET {path}: {body}");
        body
    }

    async fn deployments(&self, app_id: &str) -> Vec<Value> {
        self.get(&format!("/api/v1/apps/{app_id}/deployments"))
            .await["items"]
            .as_array()
            .unwrap()
            .clone()
    }

    async fn builds(&self, app_id: &str) -> Vec<Value> {
        self.get(&format!("/api/v1/apps/{app_id}/builds")).await["items"]
            .as_array()
            .unwrap()
            .clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deploy_rolls_an_image_out_and_back() {
    let lab = lab().await;
    let output = lab.ok(&lab.web, &["deploy", "--image", IMAGE]).await;
    assert!(
        output.stdout.starts_with(&format!(
            "Deploying {IMAGE} to the app web: the deployment dep_"
        )),
        "{}",
        output.stdout
    );
    assert!(output.stdout.contains("quoro logs --deployment dep_"));
    let first = lab.deployments(&lab.web_id).await[0].clone();
    assert_eq!(first["image"], IMAGE);
    assert_eq!(first["trigger"], "api");

    let second = lab.json(&lab.web, &["deploy", "--image", IMAGE_2]).await;
    assert_eq!(second["image"], IMAGE_2);
    assert_eq!(second["app_id"], lab.web_id.as_str());

    // A rollback deploys the image the earlier deployment ran.
    let first_id = first["id"].as_str().unwrap();
    let output = lab.ok(&lab.web, &["deploy", "--rollback", first_id]).await;
    assert!(
        output.stdout.starts_with(&format!(
            "Rolling the app web back to {IMAGE} (from {first_id}): the deployment dep_"
        )),
        "{}",
        output.stdout
    );
    let newest = lab.deployments(&lab.web_id).await[0].clone();
    assert_eq!(newest["image"], IMAGE);
    assert_ne!(newest["id"], first["id"]);

    // A deployment of another app is not this app's to roll back to.
    let output = lab.run(&lab.api, &["deploy", "--rollback", first_id]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("belongs to the app"),
        "{}",
        output.stderr
    );

    let output = lab.run(&lab.web, &["deploy", "--rollback", "v1"]).await;
    assert_eq!(output.code, exit::USAGE);
    assert!(
        output.stderr.contains("not a deployment id"),
        "{}",
        output.stderr
    );
    let output = lab.run(&lab.web, &["deploy"]).await;
    assert_eq!(
        output.code,
        exit::USAGE,
        "--image or --rollback is required"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_git_app_refuses_an_image_and_up_refuses_an_image_app() {
    let lab = lab().await;
    let output = lab.run(&lab.api, &["deploy", "--image", IMAGE]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    assert!(
        output.stderr.contains("builds its image from a Git source")
            && output.stderr.contains("run quoro up"),
        "{}",
        output.stderr
    );
    assert!(output.stderr.contains("request id: "), "{}", output.stderr);
    assert!(lab.deployments(&lab.api_id).await.is_empty());

    let before = lab.primary.requests();
    let output = lab.run(&lab.web, &["up"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    assert!(
        output.stderr.contains(
            "the app web deploys an image and has nothing to build; use quoro deploy --image REF"
        ),
        "{}",
        output.stderr
    );
    assert!(lab.builds(&lab.web_id).await.is_empty());
    // Only the read of the linked app was sent.
    assert_eq!(lab.primary.requests(), before + 1);

    // A Git app has nothing to redeploy before its first build.
    let output = lab.run(&lab.api, &["redeploy"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    assert!(
        output.stderr.contains("run quoro up first"),
        "{}",
        output.stderr
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redeploy_and_restart_the_linked_app() {
    let lab = lab().await;
    lab.ok(&lab.web, &["deploy", "--image", IMAGE]).await;
    let output = lab.ok(&lab.web, &["redeploy"]).await;
    assert!(
        output.stdout.starts_with(&format!(
            "Redeploying {IMAGE} to the app web: the deployment dep_"
        )),
        "{}",
        output.stdout
    );
    let newest = lab.deployments(&lab.web_id).await[0].clone();
    assert_eq!(newest["trigger"], "redeploy");
    assert_eq!(newest["image"], IMAGE);

    let output = lab.ok(&lab.web, &["restart"]).await;
    assert_eq!(
        output.stdout,
        "Restarting the instances of the app web (restart generation 1); the nodes restart them.\n"
    );
    let app = lab.json(&lab.web, &["restart"]).await;
    assert_eq!(app["restart_generation"], 2);
    // `--app` names another app than the link's.
    let app = lab.json(&lab.web, &["restart", "--app", "api"]).await;
    assert_eq!(app["id"], lab.api_id.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn up_starts_a_build_and_build_cancel_stops_it() {
    let lab = lab().await;
    let output = lab.ok(&lab.api, &["build", "list"]).await;
    assert_eq!(
        output.stdout,
        "No builds of the app api yet; start one with quoro up.\n"
    );
    let output = lab.ok(&lab.web, &["build", "list"]).await;
    assert_eq!(
        output.stdout,
        "The app web deploys an image; it has no builds.\n"
    );

    let output = lab.ok(&lab.api, &["up", "--detach", "--ref", "v2"]).await;
    assert!(
        output.stdout.starts_with(
            "Building the app api from https://github.com/example/api @ v2: the build bld_"
        ) && output.stdout.contains(" is queued.\n")
            && output
                .stdout
                .contains("Follow it with quoro logs --build bld_"),
        "{}",
        output.stdout
    );
    let builds = lab.builds(&lab.api_id).await;
    assert_eq!(builds.len(), 1);
    let build_id = builds[0]["id"].as_str().unwrap().to_string();
    assert_eq!(builds[0]["status"], "queued");
    assert_eq!(builds[0]["source"]["ref"], "v2");

    let output = lab.ok(&lab.api, &["build", "list"]).await;
    let lines: Vec<&str> = output.stdout.lines().collect();
    assert!(lines[0].starts_with("ID "), "{}", output.stdout);
    assert!(
        lines[1].starts_with(&build_id) && lines[1].contains("queued") && lines[1].contains("v2"),
        "{}",
        output.stdout
    );
    let page = lab.json(&lab.api, &["build", "list"]).await;
    assert_eq!(page["items"][0]["id"], build_id.as_str());

    // One build at a time.
    let output = lab.run(&lab.api, &["up", "--detach"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    assert!(
        output.stderr.contains("quoro build cancel stops it"),
        "{}",
        output.stderr
    );

    let output = lab.ok(&lab.api, &["build", "cancel"]).await;
    assert_eq!(output.stdout, format!("Cancelled the build {build_id}.\n"));
    assert_eq!(lab.builds(&lab.api_id).await[0]["status"], "cancelled");

    let output = lab.run(&lab.api, &["build", "cancel"]).await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("the app api has no build in progress"),
        "{}",
        output.stderr
    );
    let output = lab.run(&lab.api, &["build", "cancel", "nope"]).await;
    assert_eq!(output.code, exit::USAGE);

    // A JSON start answers the build.
    let build = lab.json(&lab.api, &["up", "--detach"]).await;
    assert_eq!(build["status"], "queued");
    assert_eq!(build["source"]["ref"], "main");
    let id = build["id"].as_str().unwrap();
    let cancelled = lab.json(&lab.api, &["build", "cancel", id]).await;
    assert_eq!(cancelled["status"], "cancelled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn up_follows_the_build_and_fails_when_it_is_cancelled() {
    let lab = lab().await;
    let environment = lab.environment(&lab.api);
    let canceller = async {
        // The build is queued as soon as `up` started it.
        let build_id = loop {
            if let Some(build) = lab.builds(&lab.api_id).await.first() {
                break build["id"].as_str().unwrap().to_string();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (status, body) = lab
            .primary
            .fixture(
                reqwest::Method::POST,
                &format!("/api/v1/builds/{build_id}/cancel"),
                &lab.owner_token,
                None,
            )
            .await;
        assert_eq!(status, 202, "cancel: {body}");
        build_id
    };
    let (output, build_id) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(quoro(&["up"], &environment), canceller)
    })
    .await
    .expect("up ends once its build is cancelled");
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    // No node runs the build: its log cannot be streamed, so `up` follows the
    // status.
    assert!(
        output
            .stderr
            .contains("the build log cannot be streamed now")
            && output.stderr.contains("following the build's status"),
        "{}",
        output.stderr
    );
    assert!(
        output
            .stderr
            .contains(&format!("quoro: build {build_id} is cancelled")),
        "{}",
        output.stderr
    );
    assert!(
        output
            .stderr
            .contains(&format!("quoro: the build {build_id} ended cancelled")),
        "{}",
        output.stderr
    );
    assert!(
        output.stdout.starts_with("Building the app api from"),
        "{}",
        output.stdout
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_git_rollback_rebuilds_the_commit_of_that_deployment() {
    let lab = lab().await;
    let build = lab.json(&lab.api, &["up", "--detach"]).await;
    let build_id = build["id"].as_str().unwrap();
    // As if a node ran the build and its deployment (no node runs here).
    let commit = "0123456789abcdef0123456789abcdef01234567";
    let deployment_id = format!("dep_{}", "1".repeat(26));
    let pool = lab.primary.state.store.pool();
    sqlx::query(
        "INSERT INTO deployments (id, app_id, image, image_digest, status, phase, error, \"trigger\", \
         created_by_type, created_by_id, created_at, finished_at) \
         VALUES (?, ?, ?, NULL, 'succeeded', NULL, NULL, 'build', 'system', 'system', \
         '2026-11-06T10:00:00.000Z', '2026-11-06T10:01:00.000Z')",
    )
    .bind(&deployment_id)
    .bind(&lab.api_id)
    .bind(format!("build.quoro.invalid/{}:{build_id}", lab.api_id))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE builds SET status = 'succeeded', \"commit\" = ?, deployment_id = ? WHERE id = ?",
    )
    .bind(commit)
    .bind(&deployment_id)
    .bind(build_id)
    .execute(pool)
    .await
    .unwrap();

    let output = lab
        .ok(&lab.api, &["deploy", "--rollback", &deployment_id])
        .await;
    assert!(
        output.stdout.starts_with(&format!(
            "Building the app api from https://github.com/example/api @ {commit}: the build bld_"
        )) && output.stdout.contains(&format!(
            "It rebuilds the commit the deployment {deployment_id} ran"
        )),
        "{}",
        output.stdout
    );
    let newest = lab.builds(&lab.api_id).await[0].clone();
    assert_ne!(newest["id"], build_id);
    assert_eq!(newest["source"]["ref"], commit);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_read_the_app_build_and_deployment_logs() {
    let lab = lab().await;
    // The Git app has no instance yet: nothing to read, from no node.
    let output = lab.ok(&lab.api, &["logs", "-n", "3"]).await;
    assert_eq!(
        output.stdout,
        "No log lines from the app api; its instances may not run yet.\n"
    );
    let body = lab.json(&lab.api, &["logs", "--since", "10m"]).await;
    assert_eq!(body, json!({ "items": [] }));
    // With no instance the stream ends at once.
    let output = lab.ok(&lab.api, &["logs", "-f"]).await;
    assert_eq!(output.stdout, "");

    for bad in [
        &["logs", "-n", "6000"][..],
        &["logs", "-n", "0"],
        &["logs", "--since", "yesterday"],
        &["logs", "--since", "10m", "-f"],
        &["logs", "--build", "--deployment", "dep_x"],
        &["logs", "--deployment", "nope"],
        &["logs", "--build", "nope"],
    ] {
        let output = lab.run(&lab.api, bad).await;
        assert_eq!(output.code, exit::USAGE, "{bad:?}: {}", output.stderr);
    }

    // The image app runs an instance on a node that is not connected.
    lab.ok(&lab.web, &["deploy", "--image", IMAGE]).await;
    let output = lab.run(&lab.web, &["logs", "-n", "5"]).await;
    assert_eq!(output.code, exit::UNREACHABLE, "{}", output.stderr);
    assert!(
        output.stderr.contains("node_unreachable"),
        "{}",
        output.stderr
    );
    let output = lab.run(&lab.web, &["logs", "-f"]).await;
    assert_eq!(output.code, exit::UNREACHABLE, "{}", output.stderr);

    // A deployment's progress lines come from its instance's node.
    let deployment = lab.deployments(&lab.web_id).await[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let output = lab
        .run(&lab.web, &["logs", "--deployment", &deployment])
        .await;
    assert_eq!(output.code, exit::UNREACHABLE, "{}", output.stderr);
    sqlx::query("DELETE FROM vms WHERE app_id = ?")
        .bind(&lab.web_id)
        .execute(lab.primary.state.store.pool())
        .await
        .unwrap();
    let output = lab
        .ok(&lab.web, &["logs", "--deployment", &deployment, "-n", "2"])
        .await;
    assert_eq!(
        output.stdout,
        format!("No progress lines for the deployment {deployment} yet.\n")
    );
    let body = lab
        .json(&lab.web, &["logs", "--deployment", &deployment])
        .await;
    assert_eq!(body, json!({ "items": [] }));

    // A build's log stays on the node that ran it.
    let output = lab.run(&lab.api, &["logs", "--build"]).await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("the app api has no builds; start one with quoro up"),
        "{}",
        output.stderr
    );
    lab.ok(&lab.api, &["up", "--detach"]).await;
    let output = lab.run(&lab.api, &["logs", "--build"]).await;
    assert_eq!(output.code, exit::UNREACHABLE, "{}", output.stderr);
    let output = lab.run(&lab.api, &["logs", "--build", "-f"]).await;
    assert_eq!(output.code, exit::UNREACHABLE, "{}", output.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_needs_a_reason_to_change_and_its_reads_are_audited() {
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
                "app:logs",
                "app:deploy",
                "app:restart",
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
    .with_cwd(&lab.primary.dir);
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

    let before = lab.primary.requests();
    let mints = lab.primary.mints().await;
    for args in [
        &["up", "--app", "api"][..],
        &["deploy", "--app", "web", "--image", IMAGE],
        &[
            "deploy",
            "--app",
            "web",
            "--rollback",
            &format!("dep_{}", "1".repeat(26)),
        ],
        &["redeploy", "--app", "web"],
        &["restart", "--app", "web"],
        &["build", "cancel", "--app", "api"],
    ] {
        let output = quoro(args, &environment).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
        assert!(
            output
                .stderr
                .contains("an agent must say why it runs quoro")
                && output.stderr.contains("nothing was sent"),
            "{args:?}: {}",
            output.stderr
        );
    }
    assert_eq!(
        lab.primary.requests(),
        before,
        "nothing reached the primary"
    );
    assert_eq!(lab.primary.mints().await, mints, "not even a mint");

    // A read needs no reason, and the primary records what it asked for.
    let output = quoro(&["logs", "--app", "api", "-n", "3"], &environment).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let (params, operation): (String, String) =
        sqlx::query_as("SELECT params, operation FROM audit_log ORDER BY seq DESC LIMIT 1")
            .fetch_one(lab.primary.state.store.pool())
            .await
            .unwrap();
    assert_eq!(operation, "GET /api/v1/apps/{app_id}/logs");
    let params: Value = serde_json::from_str(&params).unwrap();
    assert_eq!(params["tail"], 3, "{params}");

    // With a reason the change is made and audited with it.
    let output = quoro(
        &[
            "restart",
            "--app",
            "web",
            "--reason",
            "clear a stuck worker",
        ],
        &environment,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let (justification, actor_type, operation) = lab.primary.last_actor().await;
    assert_eq!(justification.as_deref(), Some("clear a stuck worker"));
    assert_eq!(actor_type, "agent");
    assert_eq!(operation, "POST /api/v1/apps/{app_id}/restart");

    let output = quoro(
        &["up", "--app", "api", "--detach", "--reason", "ship the fix"],
        &environment,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let builds = lab.builds(&lab.api_id).await;
    assert_eq!(builds[0]["created_by_type"], "agent");
}

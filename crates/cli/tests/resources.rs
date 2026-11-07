//! `env`, `domain`, `volume` and `backup`, end to end against an in-process
//! primary.
//!
//! No engine and no node agent run: a volume stays `creating` (the test marks
//! it `ready` as its node would), a backup stays `queued` (the test marks it
//! `succeeded` with a manifest as the primary's seal would), and a restore
//! that the primary accepts only records the new volume. The tests assert on
//! what the API answers and records.

mod common;

use common::{Output, Primary, key_file, quoro, start, write_credentials};
use quoro_cli::{Environment, exit};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::Duration;

/// A value that must never be printed.
const SECRET: &str = "s3cr3t-value-never-printed";

/// A primary with the project `demo` and its app `web` on the connected node
/// `test-node`, one `dns` provider named `dns-main`, a human's context `lab`
/// with the owner's token, and the folder `web/` linked to the app.
struct Lab {
    primary: Primary,
    owner_id: String,
    owner_token: String,
    app_id: String,
    home: PathBuf,
    web: PathBuf,
}

async fn lab() -> Lab {
    let primary = start().await;
    let (owner_id, owner_token) = primary.owner_token().await;
    let app_id = primary.app(&owner_token).await;
    let (status, body) = primary
        .fixture(
            reqwest::Method::POST,
            "/api/v1/providers",
            &owner_token,
            Some(json!({
                "kind": "dns",
                "name": "dns-main",
                "config": { "account_id": "a1" },
                "secret": "provider-secret",
            })),
        )
        .await;
    assert_eq!(status, 201, "provider: {body}");
    let home = primary.config_home("home");
    write_credentials(&home, &owner_token, 0o600);
    let lab = Lab {
        web: primary.dir.join("web"),
        primary,
        owner_id,
        owner_token,
        app_id,
        home,
    };
    std::fs::create_dir_all(&lab.web).unwrap();
    lab.ok(&["link", "demo", "web"]).await;
    lab
}

impl Lab {
    fn environment(&self) -> Environment {
        self.primary
            .context_environment(&self.home)
            .with_cwd(&self.web)
    }

    async fn run(&self, args: &[&str]) -> Output {
        quoro(args, &self.environment()).await
    }

    async fn ok(&self, args: &[&str]) -> Output {
        let output = self.run(args).await;
        assert_eq!(output.code, exit::OK, "{args:?}: {}", output.stderr);
        output
    }

    async fn json(&self, args: &[&str]) -> Value {
        let output = self.ok(&[args, &["--json"]].concat()).await;
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

    async fn env_names(&self) -> Vec<String> {
        self.get(&format!("/api/v1/apps/{}/env", self.app_id)).await["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["name"].as_str().unwrap().to_string())
            .collect()
    }

    /// Writes `text` to `name` in the scratch directory; returns its path.
    fn file(&self, name: &str, text: &str) -> String {
        let path = self.primary.dir.join(name);
        std::fs::write(&path, text).unwrap();
        path.display().to_string()
    }

    /// An agent with `scopes` on the project `demo`, logged in to its own
    /// context `lab` and linked to `web` in `cwd`.
    async fn agent(&self, scopes: &[&str]) -> Environment {
        let (agent_id, key) = self
            .primary
            .agent(&self.owner_token, &self.owner_id, "operator")
            .await;
        self.primary
            .grant(
                &self.owner_token,
                &agent_id,
                scopes,
                json!({ "projects": ["demo"] }),
            )
            .await;
        let agent_home = self.primary.dir.join("agent-home");
        let key_path = key_file(&agent_home.join("keys"), "operator.key", &key, 0o600);
        let cwd = self.primary.dir.join("agent-work");
        std::fs::create_dir_all(&cwd).unwrap();
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
        .with_cwd(&cwd);
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
                &self.primary.url,
                "--fingerprint",
                &self.primary.fingerprint,
            ],
            &environment,
        )
        .await;
        assert_eq!(login.code, exit::OK, "{}", login.stderr);
        let link = quoro(&["link", "demo", "web"], &environment).await;
        assert_eq!(link.code, exit::OK, "{}", link.stderr);
        environment
    }

    /// Creates the volume `/data` as the human and marks it `ready`, as its
    /// node would once it made the file; returns its id.
    async fn ready_volume(&self) -> String {
        let volume = self
            .json(&["volume", "create", "--size", "5", "--mount", "/data"])
            .await;
        let id = volume["id"].as_str().unwrap().to_string();
        sqlx::query("UPDATE volumes SET state = 'ready' WHERE id = ?")
            .bind(&id)
            .execute(self.primary.state.store.pool())
            .await
            .unwrap();
        id
    }

    /// An `object_store` provider and the cluster's backup repository. The
    /// API's create probes the bucket with a write, which no bucket here can
    /// answer, so the repository row is recorded directly, as the create
    /// would record it after its probe.
    async fn repository(&self) {
        let (status, provider) = self
            .primary
            .fixture(
                reqwest::Method::POST,
                "/api/v1/providers",
                &self.owner_token,
                Some(json!({
                    "kind": "object_store",
                    "name": "backups",
                    "config": {
                        "endpoint": "https://s3.example.test",
                        "bucket": "quoro",
                        "access_key_id": "AKIATEST",
                        "region": "auto",
                    },
                    "secret": "secret-access-key",
                })),
            )
            .await;
        assert_eq!(status, 201, "provider: {provider}");
        let id = format!("brp_{}", "1".repeat(26));
        let key =
            self.primary
                .state
                .data_key
                .encrypt("backup_repositories.repo_key_enc", &id, &[7; 32]);
        sqlx::query(
            "INSERT INTO backup_repositories (id, name, provider_id, prefix, repo_key_enc, \
             recovery_key_fingerprint, recovery_key_rotated_at, control_plane_schedule, \
             created_by, created_at) VALUES (?, 'primary', ?, 'cli-test', ?, ?, NULL, \
             '{\"interval_h\":24,\"keep\":7}', ?, '2026-01-01T00:00:00.000Z')",
        )
        .bind(&id)
        .bind(provider["id"].as_str().unwrap())
        .bind(key)
        .bind("c".repeat(16))
        .bind(&self.owner_id)
        .execute(self.primary.state.store.pool())
        .await
        .unwrap();
    }
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
            // Let the client open its event stream first.
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

fn assert_no_secret(output: &Output) {
    assert!(!output.stdout.contains(SECRET), "stdout: {}", output.stdout);
    assert!(!output.stderr.contains(SECRET), "stderr: {}", output.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn env_list_shows_names_and_never_a_value() {
    let lab = lab().await;
    let output = lab.ok(&["env", "list"]).await;
    assert_eq!(
        output.stdout,
        "The app web has no environment variables; set them with quoro env replace --file FILE.\n"
    );
    let (status, body) = lab
        .primary
        .fixture(
            reqwest::Method::PUT,
            &format!("/api/v1/apps/{}/env", lab.app_id),
            &lab.owner_token,
            Some(json!({ "items": [
                { "name": "DATABASE_URL", "value": SECRET },
                { "name": "API_KEY", "value": SECRET },
            ] })),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    let output = lab.ok(&["env", "list"]).await;
    assert_no_secret(&output);
    let lines: Vec<&str> = output.stdout.lines().collect();
    assert!(lines[0].starts_with("NAME") && lines[0].ends_with("UPDATED"));
    assert!(lines[1].starts_with("API_KEY "), "{}", output.stdout);
    assert!(lines[2].starts_with("DATABASE_URL "), "{}", output.stdout);

    let output = lab.ok(&["env", "list", "--json"]).await;
    assert_no_secret(&output);
    let body: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    for item in body["items"].as_array().unwrap() {
        let mut keys: Vec<&str> = item
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, ["app_id", "name", "updated_at"]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn env_replace_reads_a_file_or_stdin_and_never_echoes_a_value() {
    let lab = lab().await;
    let file = lab.file(
        "first.env",
        &format!("# the app's settings\nexport A=1\nB=\"{SECRET}\"\n"),
    );
    let output = lab.ok(&["env", "replace", "--file", &file, "--yes"]).await;
    assert_no_secret(&output);
    assert!(
        output.stderr.contains("  adds: A, B\n"),
        "{}",
        output.stderr
    );
    assert!(!output.stderr.contains("removes:"), "{}", output.stderr);
    assert!(
        output
            .stdout
            .starts_with("Replaced the environment of the app web: 2 variables."),
        "{}",
        output.stdout
    );
    assert_eq!(lab.env_names().await, ["A", "B"]);

    // From stdin: B is set again, A removed and C added.
    let environment = lab
        .environment()
        .with_stdin(format!("B='{SECRET}'\nC=3\n"), false);
    let output = quoro(
        &["env", "replace", "--file", "-", "--yes", "--json"],
        &environment,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_no_secret(&output);
    assert!(
        output.stderr.contains("  removes: A\n")
            && output.stderr.contains("  adds: C\n")
            && output.stderr.contains("  sets again: B\n"),
        "{}",
        output.stderr
    );
    let body: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(body["items"][0]["name"], "B");
    assert_eq!(lab.env_names().await, ["B", "C"]);

    // Without --yes and without a terminal nothing is sent.
    let requests = lab.primary.requests();
    let output = lab.run(&["env", "replace", "--file", &file]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("pass --yes") && output.stderr.contains("nothing was sent"),
        "{}",
        output.stderr
    );
    // stdin holds the file, so it cannot answer the prompt either.
    let terminal = lab.environment().with_stdin("A=1\n", true);
    let output = quoro(&["env", "replace", "--file", "-"], &terminal).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("--file - reads"),
        "{}",
        output.stderr
    );
    // A bad file is refused before any request, by line number only.
    let bad = lab.file("bad.env", &format!("A=1\nBAD-NAME={SECRET}\n"));
    let output = lab.run(&["env", "replace", "--file", &bad, "--yes"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert_no_secret(&output);
    assert!(
        output
            .stderr
            .contains("bad.env: line 2: the name must match"),
        "{}",
        output.stderr
    );
    let reserved = lab.file("reserved.env", "QUORO_NODE_ID=x\n");
    let output = lab
        .run(&["env", "replace", "--file", &reserved, "--yes"])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    let output = lab
        .run(&["env", "replace", "--file", "/nonexistent/x.env", "--yes"])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    assert_eq!(lab.env_names().await, ["B", "C"]);

    // On a terminal a human types the app's name; anything else changes
    // nothing.
    let output = quoro(
        &["env", "replace", "--file", &file],
        &lab.environment().with_stdin("nope\n", true),
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("Type the app's name (web) to confirm")
            && output.stderr.contains("nothing was changed"),
        "{}",
        output.stderr
    );
    assert_eq!(lab.env_names().await, ["B", "C"]);
    let output = quoro(
        &["env", "replace", "--file", &file],
        &lab.environment().with_stdin("web\n", true),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_no_secret(&output);
    assert_eq!(lab.env_names().await, ["A", "B"]);

    // An empty file clears the environment.
    let empty = lab.file("empty.env", "# nothing\n");
    let output = lab.ok(&["env", "replace", "--file", &empty, "--yes"]).await;
    assert!(
        output.stderr.contains("  removes: A, B\n"),
        "{}",
        output.stderr
    );
    assert!(lab.env_names().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_add_list_and_remove() {
    let lab = lab().await;
    let output = lab.ok(&["domain", "list"]).await;
    assert_eq!(
        output.stdout,
        "The app web has no domains; add one with quoro domain add HOST.\n"
    );
    let output = lab.ok(&["domain", "add", "App.Example.com"]).await;
    assert!(
        output
            .stdout
            .starts_with("Routing app.example.com to the app web (dom_"),
        "{}",
        output.stdout
    );
    assert!(output.stdout.contains("its certificate is pending"));
    // The provider by name, of the mode's kind.
    let domain = lab
        .json(&["domain", "add", "api.example.com", "--provider", "dns-main"])
        .await;
    assert_eq!(domain["hostname"], "api.example.com");
    assert_eq!(domain["tls_mode"], "acme");
    assert!(domain["provider_id"].as_str().unwrap().starts_with("prv_"));
    let output = lab
        .run(&["domain", "add", "x.example.com", "--provider", "nope"])
        .await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    // No tunnel provider exists.
    let output = lab
        .run(&["domain", "add", "t.example.com", "--tls", "tunnel"])
        .await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output.stderr.contains("validation_failed"),
        "{}",
        output.stderr
    );
    // A hostname routes to one app.
    let output = lab.run(&["domain", "add", "app.example.com"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    let requests = lab.primary.requests();
    let output = lab.run(&["domain", "add", "bad_host"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    let output = lab
        .run(&["domain", "add", "a.example.com", "--tls", "x"])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");

    let output = lab.ok(&["domain", "list"]).await;
    let lines: Vec<&str> = output.stdout.lines().collect();
    assert!(lines[0].starts_with("HOSTNAME"), "{}", output.stdout);
    assert_eq!(lines.len(), 3, "{}", output.stdout);
    assert!(
        output.stdout.contains("app.example.com  acme"),
        "{}",
        output.stdout
    );
    let page = lab.json(&["domain", "list"]).await;
    assert_eq!(page["items"].as_array().unwrap().len(), 2);

    let requests = lab.primary.requests();
    let output = lab.run(&["domain", "remove", "app.example.com"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    let output = quoro(
        &["domain", "remove", "app.example.com"],
        &lab.environment().with_stdin("app.example.com\n", true),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("Type the hostname (app.example.com) to confirm"),
        "{}",
        output.stderr
    );
    assert!(
        output.stdout.starts_with("Removed app.example.com (dom_"),
        "{}",
        output.stdout
    );
    let removed = lab
        .json(&["domain", "remove", "api.example.com", "--yes"])
        .await;
    assert_eq!(removed["hostname"], "api.example.com");
    let page = lab.json(&["domain", "list"]).await;
    assert!(page["items"].as_array().unwrap().is_empty());
    let output = lab
        .run(&["domain", "remove", "app.example.com", "--yes"])
        .await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    assert!(
        output.stderr.contains("quoro domain list"),
        "{}",
        output.stderr
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_human_creates_lists_and_deletes_a_volume() {
    let lab = lab().await;
    let output = lab.ok(&["volume", "list"]).await;
    assert!(output.stdout.starts_with("The app web has no volumes"));
    let output = lab
        .ok(&["volume", "create", "--size", "5", "--mount", "/data"])
        .await;
    assert!(
        output.stdout.starts_with("Created the volume vol_")
            && output
                .stdout
                .contains("(5 GiB at /data) of the app web on the node nod_")
            && output.stdout.contains("it is creating"),
        "{}",
        output.stdout
    );
    // One volume per app.
    let output = lab
        .run(&["volume", "create", "--size", "1", "--mount", "/other"])
        .await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    let requests = lab.primary.requests();
    for args in [
        &["volume", "create", "--size", "1", "--mount", "/proc/x"][..],
        &["volume", "create", "--size", "0", "--mount", "/data"],
        &["volume", "create", "--size", "1025", "--mount", "/data"],
        &["volume", "delete", "data"],
    ] {
        let output = lab.run(args).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
    }
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");

    let output = lab.ok(&["volume", "list"]).await;
    let lines: Vec<&str> = output.stdout.lines().collect();
    assert!(lines[0].starts_with("ID "), "{}", output.stdout);
    assert!(
        lines[1].starts_with("vol_") && lines[1].contains("creating  5 GiB  /data  nod_"),
        "{}",
        output.stdout
    );
    let page = lab.json(&["volume", "list"]).await;
    let id = page["items"][0]["id"].as_str().unwrap().to_string();

    let requests = lab.primary.requests();
    let output = lab.run(&["volume", "delete", "/data"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    let output = quoro(
        &["volume", "delete", "/data"],
        &lab.environment().with_stdin("/wrong\n", true),
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("nothing was deleted"),
        "{}",
        output.stderr
    );
    let output = quoro(
        &["volume", "delete", "/data"],
        &lab.environment().with_stdin("/data\n", true),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("Type its mount path (/data) to confirm"),
        "{}",
        output.stderr
    );
    assert_eq!(
        output.stdout,
        format!(
            "Deleting the volume {id} (/data): it is deleting until its node removes the file. \
             Its backups stay until retention prunes them.\n"
        )
    );
    let volume = lab.get(&format!("/api/v1/volumes/{id}")).await;
    assert_eq!(volume["state"], "deleting");
    let output = lab.run(&["volume", "delete", &id, "--yes"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_needs_a_reason_and_an_approval() {
    let lab = lab().await;
    let volume_id = lab.ready_volume().await;
    let agent = lab
        .agent(&[
            "project:read",
            "app:read",
            "app:update",
            "backup:read",
            "approval:create",
            "approval:read",
            "event:read",
        ])
        .await;
    let file = lab.file("agent.env", &format!("TOKEN={SECRET}\n"));

    // Every change without a reason is refused before anything is sent, not
    // even a mint.
    let requests = lab.primary.requests();
    let mints = lab.primary.mints().await;
    let backup = format!("bkp_{}", "0".repeat(26));
    for args in [
        &["env", "replace", "--file", &file][..],
        &["domain", "add", "agent.example.com"],
        &["domain", "remove", "agent.example.com"],
        &["volume", "create", "--size", "1", "--mount", "/x"],
        &["volume", "delete", &volume_id],
        &["backup", "create"],
        &["backup", "restore", &backup, "--node", "test-node"],
    ] {
        let output = quoro(args, &agent).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
        assert!(
            output.stderr.contains("--reason"),
            "{args:?}: {}",
            output.stderr
        );
    }
    // A provider by name needs a human-only read: refused locally too.
    let output = quoro(
        &[
            "domain",
            "add",
            "agent.example.com",
            "--provider",
            "dns-main",
            "--reason",
            "r",
        ],
        &agent,
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("by its id (prv_...)"),
        "{}",
        output.stderr
    );
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    assert_eq!(lab.primary.mints().await, mints, "not even a mint");

    // An agent is not asked: no --yes, and the reason is audited.
    let output = quoro(
        &[
            "env",
            "replace",
            "--file",
            &file,
            "--reason",
            "rotate the token",
        ],
        &agent,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_no_secret(&output);
    let (justification, actor, operation) = lab.primary.last_actor().await;
    assert_eq!(
        (justification.as_deref(), actor.as_str(), operation.as_str()),
        (
            Some("rotate the token"),
            "agent",
            "PUT /api/v1/apps/{app_id}/env"
        )
    );
    let output = quoro(
        &["domain", "add", "agent.example.com", "--reason", "serve it"],
        &agent,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let output = quoro(
        &[
            "domain",
            "remove",
            "agent.example.com",
            "--reason",
            "retire it",
        ],
        &agent,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);

    // A volume delete is destructive: exit 3, then an approval request.
    let delete = [
        "volume",
        "delete",
        volume_id.as_str(),
        "--reason",
        "drop the data",
    ];
    let output = quoro(&delete, &agent).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("scope app:delete on volume {volume_id}")),
        "{}",
        output.stderr
    );
    let output = quoro(&[&delete[..], &["--request-approval"]].concat(), &agent).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    let first = output.stdout.trim().to_string();
    assert!(first.starts_with("apr_"), "{}", output.stdout);

    let wait = [&delete[..], &["--request-approval", "--wait"]].concat();
    let (output, approved) = tokio::join!(quoro(&wait, &agent), approve(&lab, &first));
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("approval {approved} approved")),
        "{}",
        output.stderr
    );
    assert!(
        output
            .stdout
            .starts_with(&format!("Deleting the volume {volume_id} (/data)")),
        "{}",
        output.stdout
    );
    let volume = lab.get(&format!("/api/v1/volumes/{volume_id}")).await;
    assert_eq!(volume["state"], "deleting");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backups_are_listed_taken_and_restored() {
    let lab = lab().await;
    let output = lab.ok(&["backup", "list"]).await;
    assert_eq!(
        output.stdout,
        "The app web has no backups; take one with quoro backup create.\n"
    );
    // Without a ready volume and a repository there is nothing to back up.
    let output = lab.run(&["backup", "create"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    assert!(output.stderr.contains("a backup needs a ready volume"));

    let volume_id = lab.ready_volume().await;
    lab.repository().await;
    let output = lab.ok(&["backup", "create"]).await;
    assert!(
        output
            .stdout
            .starts_with("Backing up the volume of the app web: the backup bkp_")
            && output.stdout.contains(" is queued;"),
        "{}",
        output.stdout
    );
    // One backup of a volume at a time.
    let output = lab.run(&["backup", "create"]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);

    let output = lab.ok(&["backup", "list"]).await;
    let lines: Vec<&str> = output.stdout.lines().collect();
    assert!(lines[0].starts_with("ID "), "{}", output.stdout);
    assert!(
        lines[1].starts_with("bkp_") && lines[1].contains("queued  api"),
        "{}",
        output.stdout
    );
    let page = lab.json(&["backup", "list"]).await;
    let backup_id = page["items"][0]["id"].as_str().unwrap().to_string();
    assert_eq!(page["items"][0]["volume_id"], volume_id.as_str());

    // Refusals before anything is sent.
    let requests = lab.primary.requests();
    let output = lab
        .run(&[
            "backup",
            "restore",
            "latest",
            "--node",
            "test-node",
            "--yes",
        ])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    let output = lab
        .run(&["backup", "restore", &backup_id, "--node", "test-node"])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("pass --yes"), "{}", output.stderr);
    let output = lab.run(&["backup", "restore", &backup_id]).await;
    assert_eq!(output.code, exit::USAGE, "--node is required");
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");

    let output = lab
        .run(&["backup", "restore", &backup_id, "--node", "nope", "--yes"])
        .await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    assert!(output.stderr.contains("no node has the id or name 'nope'"));
    let missing = format!("bkp_{}", "0".repeat(26));
    let output = lab
        .run(&[
            "backup",
            "restore",
            &missing,
            "--node",
            "test-node",
            "--yes",
        ])
        .await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    // The node by name resolves; a queued backup cannot be restored yet.
    let output = lab
        .run(&[
            "backup",
            "restore",
            &backup_id,
            "--node",
            "test-node",
            "--yes",
        ])
        .await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);
    assert!(output.stderr.contains("not succeeded"), "{}", output.stderr);

    // Sealed as the primary would; the old volume failed, so no node session
    // has to hold it.
    let pool = lab.primary.state.store.pool();
    sqlx::query(
        "UPDATE backups SET status = 'succeeded', manifest_key = 'manifests/x.json', \
         manifest_digest = ?, logical_bytes = 2048, finished_at = created_at WHERE id = ?",
    )
    .bind("b".repeat(64))
    .bind(&backup_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE volumes SET state = 'failed' WHERE id = ?")
        .bind(&volume_id)
        .execute(pool)
        .await
        .unwrap();
    let output = lab.ok(&["backup", "list"]).await;
    assert!(
        output.stdout.contains("succeeded  api"),
        "{}",
        output.stdout
    );
    assert!(output.stdout.contains("2.0 KiB"), "{}", output.stdout);

    // An agent's restore is destructive: exit 3, and the approval request
    // names the node.
    let agent = lab
        .agent(&[
            "project:read",
            "app:read",
            "backup:read",
            "node:read",
            "approval:create",
        ])
        .await;
    // A grant on a project reads no node, so the agent names it by its id.
    let output = quoro(
        &[
            "backup",
            "restore",
            &backup_id,
            "--node",
            "test-node",
            "--reason",
            "r",
        ],
        &agent,
    )
    .await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    assert!(
        output.stderr.contains("by its id (nod_...)"),
        "{}",
        output.stderr
    );
    let node_id: String = sqlx::query_scalar("SELECT id FROM nodes WHERE name = 'test-node'")
        .fetch_one(pool)
        .await
        .unwrap();
    let restore = [
        "backup",
        "restore",
        backup_id.as_str(),
        "--node",
        node_id.as_str(),
        "--reason",
        "bring the data back",
    ];
    let output = quoro(&restore, &agent).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("scope backup:restore on backup {backup_id}")),
        "{}",
        output.stderr
    );
    let output = quoro(&[&restore[..], &["--request-approval"]].concat(), &agent).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    let approval_id = output.stdout.trim().to_string();
    assert!(approval_id.starts_with("apr_"), "{}", output.stdout);
    let approval = lab.get(&format!("/api/v1/approvals/{approval_id}")).await;
    assert_eq!(
        approval["operation"],
        "POST /api/v1/backups/{backup_id}/restore"
    );

    // A human restores on a terminal by typing the app's name.
    let output = quoro(
        &["backup", "restore", &backup_id, "--node", "test-node"],
        &lab.environment().with_stdin("web\n", true),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("This stops the app web, replaces its data with the backup")
            && output
                .stderr
                .contains("Type the app's name (web) to confirm"),
        "{}",
        output.stderr
    );
    assert!(
        output.stdout.starts_with(&format!(
            "Restoring the backup {backup_id} into the volume vol_"
        )),
        "{}",
        output.stdout
    );
    let volumes = lab
        .get(&format!("/api/v1/apps/{}/volumes", lab.app_id))
        .await;
    let restoring = volumes["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["state"] == "restoring")
        .expect("a restoring volume");
    assert_eq!(restoring["restored_from_backup_id"], backup_id.as_str());

    // The failed and the restoring volume share /data: it names neither.
    let output = lab.run(&["volume", "delete", "/data", "--yes"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains(&format!("{volume_id} (failed)"))
            && output.stderr.contains(restoring["id"].as_str().unwrap())
            && output.stderr.contains("name one by its id"),
        "{}",
        output.stderr
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_new_commands_need_a_context_and_an_app() {
    let primary = start().await;
    let empty = primary.dir.join("nowhere");
    std::fs::create_dir_all(&empty).unwrap();
    let environment = Environment::new([("XDG_CONFIG_HOME", empty.display().to_string())], false)
        .with_cwd(&empty);
    for args in [
        &["env", "list"][..],
        &["domain", "list"],
        &["volume", "list"],
        &["backup", "list"],
    ] {
        let output = quoro(args, &environment).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
        assert!(
            output.stderr.contains("no context is selected"),
            "{args:?}: {}",
            output.stderr
        );
    }
    assert_eq!(primary.requests(), 0);
}

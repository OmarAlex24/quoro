//! An agent through the client, end to end against an in-process primary:
//! `quoro login --agent`, the cached access token, `--reason`, the approval
//! flags and `quoro whoami`.

mod common;

use common::{Output, Primary, key_file, mode, quoro, start, write_credentials};
use quoro_cli::{Environment, exit};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A primary with an owner, the app `web` in the project `demo`, and the
/// agent `operator` holding `scopes` on that project, with its key in a
/// private file.
struct Lab {
    primary: Primary,
    owner_token: String,
    owner_id: String,
    app_id: String,
    agent_id: String,
    key: String,
    key_path: PathBuf,
    home: PathBuf,
}

async fn lab(scopes: &[&str]) -> Lab {
    let primary = start().await;
    let (owner_id, owner_token) = primary.owner_token().await;
    let app_id = primary.app(&owner_token).await;
    let (agent_id, key) = primary.agent(&owner_token, &owner_id, "operator").await;
    primary
        .grant(
            &owner_token,
            &agent_id,
            scopes,
            json!({ "projects": ["demo"] }),
        )
        .await;
    let home = primary.dir.join("agent-home");
    let key_path = key_file(&home.join("keys"), "operator.key", &key, 0o600);
    Lab {
        primary,
        owner_token,
        owner_id,
        app_id,
        agent_id,
        key,
        key_path,
        home,
    }
}

impl Lab {
    fn config_dir(&self) -> PathBuf {
        self.home.join("config/quoro")
    }

    fn cache_file(&self) -> PathBuf {
        self.home.join("run/quoro/lab.token")
    }

    /// The agent's environment: its own config and runtime directories.
    fn environment(&self, extra: &[(&str, &str)]) -> Environment {
        let mut vars = vec![
            (
                "XDG_CONFIG_HOME".to_string(),
                self.home.join("config").display().to_string(),
            ),
            (
                "XDG_RUNTIME_DIR".to_string(),
                self.home.join("run").display().to_string(),
            ),
        ];
        vars.extend(
            extra
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
        );
        Environment::new(vars, false)
    }

    async fn login_with(&self, key_path: &Path) -> Output {
        let key_path = key_path.display().to_string();
        quoro(
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
            &self.environment(&[]),
        )
        .await
    }

    async fn login(&self) -> Output {
        let output = self.login_with(&self.key_path).await;
        assert_eq!(output.code, exit::OK, "login: {}", output.stderr);
        output
    }

    async fn run(&self, args: &[&str]) -> Output {
        quoro(args, &self.environment(&[])).await
    }

    /// Asserts that neither the key nor any token was printed.
    fn assert_quiet(&self, output: &Output) {
        for text in [&output.stdout, &output.stderr] {
            assert!(!text.contains(&self.key), "the key was printed: {text}");
            assert!(!text.contains("atk_"), "a token was printed: {text}");
        }
    }

    /// Rewrites the cached token's fields.
    fn edit_cache(&self, change: impl FnOnce(&mut Value)) {
        let path = self.cache_file();
        let mut entry: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        change(&mut entry);
        std::fs::write(&path, entry.to_string()).unwrap();
    }

    /// An approval body for deleting the app.
    fn approval_body(&self) -> String {
        let path = self.home.join("approval.json");
        std::fs::write(
            &path,
            json!({
                "scope": "app:delete",
                "target": { "type": "app", "id": self.app_id },
                "operation": "DELETE /api/v1/apps/{app_id}",
                "params": { "app_id": self.app_id },
                "justification": "by hand",
            })
            .to_string(),
        )
        .unwrap();
        format!("@{}", path.display())
    }

    /// Waits for a pending approval other than `except`, then decides it as
    /// the owner with `decision` (`approve` or `deny`).
    async fn decide(&self, except: Option<&str>, decision: &str) -> String {
        let pool = self.primary.state.store.pool();
        for _ in 0..200 {
            let pending: Option<(String,)> = sqlx::query_as(
                "SELECT id FROM approvals WHERE status = 'pending' AND id <> ? \
                 ORDER BY created_at DESC LIMIT 1",
            )
            .bind(except.unwrap_or(""))
            .fetch_optional(pool)
            .await
            .unwrap();
            if let Some((id,)) = pending {
                // Let the client open its event stream first.
                tokio::time::sleep(Duration::from_millis(300)).await;
                let (status, body) = self
                    .primary
                    .fixture(
                        reqwest::Method::POST,
                        &format!("/api/v1/approvals/{id}/{decision}"),
                        &self.owner_token,
                        Some(json!({ "note": "decided by the test" })),
                    )
                    .await;
                assert_eq!(status, 200, "{decision}: {body}");
                return id;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("no pending approval to {decision}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_login_saves_the_key_path_and_never_the_key() {
    let lab = lab(&["app:read"]).await;
    let output = lab.login().await;
    assert!(
        output.stdout.contains("Logged in to lab") && output.stdout.contains("as agent operator"),
        "{}",
        output.stdout
    );
    lab.assert_quiet(&output);

    let config = std::fs::read_to_string(lab.config_dir().join("config.toml")).unwrap();
    assert!(config.contains("current = \"lab\""), "{config}");
    assert!(
        config.contains(&format!("url = \"{}\"", lab.primary.url)),
        "{config}"
    );
    assert!(
        config.contains(&format!("fingerprint = \"{}\"", lab.primary.fingerprint)),
        "{config}"
    );
    let credentials_path = lab.config_dir().join("credentials.toml");
    assert_eq!(mode(&credentials_path), 0o600);
    let credentials = std::fs::read_to_string(&credentials_path).unwrap();
    assert!(
        credentials.contains(&format!("key_file = \"{}\"", lab.key_path.display())),
        "{credentials}"
    );
    assert!(
        !credentials.contains(&lab.key) && !credentials.contains("token"),
        "{credentials}"
    );

    // The token minted at login is cached for the next command, privately.
    assert_eq!(mode(&lab.cache_file()), 0o600);
    assert_eq!(mode(lab.cache_file().parent().unwrap()), 0o700);
    let cached = std::fs::read_to_string(lab.cache_file()).unwrap();
    assert!(!cached.contains(&lab.key), "the key is not cached");
    assert_eq!(lab.primary.mints().await, 1);

    let whoami = lab.run(&["whoami"]).await;
    assert_eq!(whoami.code, exit::OK, "{}", whoami.stderr);
    assert!(
        whoami
            .stdout
            .starts_with(&format!("context  lab ({})\n", lab.primary.url)),
        "{}",
        whoami.stdout
    );
    assert!(
        whoami.stdout.contains(&format!(
            "agent    operator ({}), model test-model, owner {}",
            lab.agent_id, lab.owner_id
        )),
        "{}",
        whoami.stdout
    );
    assert!(
        whoami.stdout.contains("app:read  on projects=demo  until "),
        "{}",
        whoami.stdout
    );
    lab.assert_quiet(&whoami);
    assert_eq!(
        lab.primary.mints().await,
        1,
        "whoami reused the cached token"
    );

    let json_output = lab.run(&["whoami", "--json"]).await;
    let me: Value = serde_json::from_str(&json_output.stdout).unwrap();
    assert_eq!(me["type"], "agent");
    assert_eq!(me["agent"]["id"], lab.agent_id.as_str());
    assert_eq!(me["grants"][0]["scopes"], json!(["app:read"]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_login_writes_nothing() {
    let lab = lab(&["app:read"]).await;
    let nothing_written = || {
        assert!(!lab.config_dir().join("config.toml").exists());
        assert!(!lab.config_dir().join("credentials.toml").exists());
    };

    // A key file the group can read is refused before anything is sent.
    let shared = key_file(&lab.home.join("keys"), "shared.key", &lab.key, 0o640);
    let output = lab.login_with(&shared).await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output.stderr.contains(&format!(
            "the agent key file {} is accessible by other users (mode 640); restrict it with 'chmod 600 {}'",
            shared.display(),
            shared.display()
        )),
        "{}",
        output.stderr
    );
    lab.assert_quiet(&output);
    assert_eq!(lab.primary.requests(), 0);
    nothing_written();

    // Without a pin and without a terminal the certificate is shown, not
    // trusted, and nothing reaches the router: the handshake is stopped.
    let key_path = lab.key_path.display().to_string();
    let output = quoro(
        &[
            "login",
            "--agent",
            "--key-file",
            &key_path,
            "--url",
            &lab.primary.url,
        ],
        &lab.environment(&[]),
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("pass --fingerprint {}", lab.primary.fingerprint)),
        "{}",
        output.stderr
    );
    assert_eq!(lab.primary.requests(), 0);
    nothing_written();

    // A key the primary does not know mints nothing.
    let unknown = key_file(
        &lab.home.join("keys"),
        "unknown.key",
        &format!("akey_{}", "Z".repeat(40)),
        0o600,
    );
    let output = lab.login_with(&unknown).await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output.stderr.contains("refused to mint an access token"),
        "{}",
        output.stderr
    );
    nothing_written();
    assert_eq!(lab.primary.mints().await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cached_token_is_reused_until_due_and_minted_again_on_a_401() {
    let lab = lab(&["app:read"]).await;
    lab.login().await;
    let me = || lab.run(&["api", "GET", "/api/v1/auth/me"]);
    for _ in 0..2 {
        let output = me().await;
        assert_eq!(output.code, exit::OK, "{}", output.stderr);
    }
    assert_eq!(lab.primary.mints().await, 1, "the login's token is reused");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    // Expired.
    lab.edit_cache(|entry| entry["expires_at"] = json!(now - 10));
    assert_eq!(me().await.code, exit::OK);
    assert_eq!(lab.primary.mints().await, 2);
    // Within 30 s of its expiry.
    lab.edit_cache(|entry| entry["expires_at"] = json!(now + 20));
    assert_eq!(me().await.code, exit::OK);
    assert_eq!(lab.primary.mints().await, 3);
    // Unknown to the primary: answered 401, minted once more, sent again.
    lab.edit_cache(|entry| entry["access_token"] = json!(format!("atk_{}", "0".repeat(26))));
    let output = me().await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(lab.primary.mints().await, 4);
    assert_eq!(me().await.code, exit::OK);
    assert_eq!(lab.primary.mints().await, 4, "the new token is cached");

    // QUORO_AGENT_KEY_FILE overrides the stored key, and the token cached for
    // the first key is not reused for it.
    let (other_id, other_key) = lab
        .primary
        .agent(&lab.owner_token, &lab.owner_id, "second")
        .await;
    let other_path = key_file(&lab.home.join("keys"), "second.key", &other_key, 0o600);
    let other_path = other_path.display().to_string();
    let output = quoro(
        &["whoami", "--json"],
        &lab.environment(&[("QUORO_AGENT_KEY_FILE", &other_path)]),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let me_json: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(me_json["agent"]["id"], other_id.as_str());
    assert_eq!(lab.primary.mints().await, 5);
    let output = lab.run(&["whoami", "--json"]).await;
    let me_json: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(me_json["agent"]["id"], lab.agent_id.as_str());
    assert_eq!(lab.primary.mints().await, 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_change_needs_a_reason() {
    let lab = lab(&["app:read", "approval:create", "approval:read"]).await;
    lab.login().await;
    let data = lab.approval_body();
    let requests = lab.primary.requests();
    let mints = lab.primary.mints().await;

    // Expire the cache too, so a mint would have to be sent.
    lab.edit_cache(|entry| entry["expires_at"] = json!(0));
    let output = lab
        .run(&["api", "POST", "/api/v1/approvals", "--data", &data])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains("pass --reason TEXT or set QUORO_JUSTIFICATION; nothing was sent"),
        "{}",
        output.stderr
    );
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    assert_eq!(lab.primary.mints().await, mints, "not even a mint");

    let output = lab
        .run(&[
            "api",
            "POST",
            "/api/v1/approvals",
            "--data",
            &data,
            "--reason",
            "the app is retired",
        ])
        .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let (justification, actor_type, operation) = lab.primary.last_actor().await;
    assert_eq!(operation, "POST /api/v1/approvals");
    assert_eq!(actor_type, "agent");
    assert_eq!(justification.as_deref(), Some("the app is retired"));

    let output = quoro(
        &["api", "POST", "/api/v1/approvals", "--data", &data],
        &lab.environment(&[("QUORO_JUSTIFICATION", "from the environment")]),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let (justification, actor_type, _) = lab.primary.last_actor().await;
    assert_eq!(actor_type, "agent");
    assert_eq!(justification.as_deref(), Some("from the environment"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_destructive_call_files_an_approval_and_runs_once_approved() {
    let lab = lab(&["app:read", "approval:create", "approval:read", "event:read"]).await;
    lab.login().await;
    let path = format!("/api/v1/apps/{}", lab.app_id);
    let delete = ["api", "DELETE", path.as_str(), "--reason", "retire the app"];

    let output = lab.run(&delete).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("scope app:delete on app {}", lab.app_id)),
        "{}",
        output.stderr
    );

    let output = lab
        .run(&[&delete[..], &["--request-approval"]].concat())
        .await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    let first = output.stdout.trim().to_string();
    assert!(first.starts_with("apr_"), "{}", output.stdout);
    assert!(output.stderr.contains("--wait"), "{}", output.stderr);
    let (status, approval) = lab
        .primary
        .fixture(
            reqwest::Method::GET,
            &format!("/api/v1/approvals/{first}"),
            &lab.owner_token,
            None,
        )
        .await;
    assert_eq!(status, 200, "{approval}");
    assert_eq!(approval["status"], "pending");
    assert_eq!(approval["operation"], "DELETE /api/v1/apps/{app_id}");
    assert_eq!(approval["params"], json!({ "app_id": lab.app_id }));
    assert_eq!(approval["justification"], "retire the app");

    let wait = [&delete[..], &["--request-approval", "--wait"]].concat();
    let (output, approved) = tokio::join!(lab.run(&wait), lab.decide(Some(&first), "approve"));
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let app: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(app["id"], lab.app_id.as_str());
    assert!(
        output
            .stderr
            .contains(&format!("approval {approved} approved")),
        "{}",
        output.stderr
    );
    lab.assert_quiet(&output);
    let apps = lab
        .primary
        .count(&format!(
            "SELECT COUNT(*) FROM apps WHERE id = '{}'",
            lab.app_id
        ))
        .await;
    assert_eq!(apps, 0, "the app is deleted");
    let (approval_id,): (Option<String>,) = sqlx::query_as(
        "SELECT approval_id FROM audit_log WHERE operation = 'DELETE /api/v1/apps/{app_id}' \
         AND result = 'ok'",
    )
    .fetch_one(lab.primary.state.store.pool())
    .await
    .unwrap();
    assert_eq!(approval_id.as_deref(), Some(approved.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_denied_approval_exits_3_and_nothing_is_deleted() {
    // Without event:read the wait reads the approval only.
    let lab = lab(&["app:read", "approval:create", "approval:read"]).await;
    lab.login().await;
    let path = format!("/api/v1/apps/{}", lab.app_id);
    let wait = [
        "api",
        "DELETE",
        path.as_str(),
        "--reason",
        "retire the app",
        "--request-approval",
        "--wait",
    ];
    let (output, denied) = tokio::join!(lab.run(&wait), lab.decide(None, "deny"));
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    assert!(
        output.stderr.contains(&format!(
            "approval {denied} was denied: decided by the test"
        )),
        "{}",
        output.stderr
    );
    let apps = lab
        .primary
        .count(&format!(
            "SELECT COUNT(*) FROM apps WHERE id = '{}'",
            lab.app_id
        ))
        .await;
    assert_eq!(apps, 1, "the app is still there");

    // --wait without --request-approval is a usage error.
    let output = lab
        .run(&["api", "DELETE", path.as_str(), "--reason", "r", "--wait"])
        .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whoami_shows_a_user_and_needs_a_context() {
    let primary = start().await;
    let (owner_id, token) = primary.owner_token().await;
    let home = primary.config_home("user");
    write_credentials(&home, &token, 0o600);
    let environment = primary.context_environment(&home);

    let output = quoro(&["whoami"], &environment).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(
        output.stdout,
        format!(
            "context  lab ({})\nuser     Owner <owner@example.com> ({owner_id}), role owner\n",
            primary.url
        )
    );
    assert!(!output.stdout.contains(&token));

    let output = quoro(&["whoami", "--json"], &environment).await;
    let me: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(me["type"], "user");
    assert_eq!(me["user"]["id"], owner_id.as_str());

    // A human may change something without a reason.
    let body = primary.dir.join("project.json");
    std::fs::write(&body, r#"{"slug":"ops","name":"Ops"}"#).unwrap();
    let data = format!("@{}", body.display());
    let output = quoro(
        &["api", "POST", "/api/v1/projects", "--data", &data],
        &environment,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);

    let empty = Environment::new(
        [(
            "XDG_CONFIG_HOME",
            primary.dir.join("nowhere").display().to_string(),
        )],
        false,
    );
    let output = quoro(&["whoami"], &empty).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("no context is selected"),
        "{}",
        output.stderr
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_arguments_show_the_client_help() {
    let output = quoro(&[], &Environment::default()).await;
    assert_eq!(output.code, exit::USAGE);
    assert!(output.stderr.contains("Usage: quoro"), "{}", output.stderr);
    assert!(output.stderr.contains("whoami"), "{}", output.stderr);
    assert!(
        output.stderr.contains("quoro help host"),
        "{}",
        output.stderr
    );
    assert!(output.stdout.is_empty());
}

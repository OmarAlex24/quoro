//! A human through the client, end to end against an in-process primary:
//! `quoro login` with a personal API token, and `quoro logout`.

mod common;

use common::{Primary, key_file, mode, quoro, start};
use quoro_cli::{Environment, exit};
use serde_json::json;
use std::path::{Path, PathBuf};

/// A primary with an owner and a personal API token created through the
/// tokens API, and an empty home for the client.
struct Lab {
    primary: Primary,
    owner_id: String,
    token: String,
    home: PathBuf,
}

async fn lab() -> Lab {
    let primary = start().await;
    let (owner_id, cookie) = primary.owner_session().await;
    let (status, created) = primary
        .with_session(
            reqwest::Method::POST,
            "/api/v1/tokens",
            &cookie,
            Some(json!({ "name": "cli" })),
        )
        .await;
    assert_eq!(status, 201, "create token: {created}");
    let token = created["token"].as_str().unwrap().to_string();
    let home = primary.dir.join("human-home");
    Lab {
        primary,
        owner_id,
        token,
        home,
    }
}

impl Lab {
    fn config_dir(&self) -> PathBuf {
        self.home.join("config/quoro")
    }

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

    /// `quoro login --url --fingerprint --token-stdin` with `stdin`.
    async fn login_piped(&self, stdin: &str) -> common::Output {
        quoro(
            &[
                "--context",
                "lab",
                "login",
                "--url",
                &self.primary.url,
                "--fingerprint",
                &self.primary.fingerprint,
                "--token-stdin",
            ],
            &self.environment(&[]).with_stdin(stdin, false),
        )
        .await
    }

    fn nothing_written(&self) {
        assert!(!self.config_dir().join("config.toml").exists());
        assert!(!self.config_dir().join("credentials.toml").exists());
    }

    fn assert_quiet(&self, output: &common::Output) {
        for text in [&output.stdout, &output.stderr] {
            assert!(!text.contains(&self.token), "the token was printed: {text}");
        }
    }
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_piped_token_saves_the_context_and_a_private_credential() {
    let lab = lab().await;
    let output = lab.login_piped(&format!("{}\n", lab.token)).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(
        output.stdout,
        format!(
            "Logged in to lab ({}) as Owner <owner@example.com>, role owner; lab is the current context.\n",
            lab.primary.url
        )
    );
    lab.assert_quiet(&output);

    let config = read(&lab.config_dir().join("config.toml"));
    assert!(config.contains("current = \"lab\""), "{config}");
    assert!(
        config.contains(&format!("url = \"{}\"", lab.primary.url)),
        "{config}"
    );
    assert!(
        config.contains(&format!("fingerprint = \"{}\"", lab.primary.fingerprint)),
        "{config}"
    );
    assert!(!config.contains(&lab.token), "{config}");
    let credentials_path = lab.config_dir().join("credentials.toml");
    assert_eq!(mode(&credentials_path), 0o600);
    assert_eq!(
        read(&credentials_path),
        format!("[contexts.\"lab\"]\ntoken = \"{}\"\n", lab.token)
    );

    // The next command uses the stored token.
    let whoami = quoro(&["whoami", "--json"], &lab.environment(&[])).await;
    assert_eq!(whoami.code, exit::OK, "{}", whoami.stderr);
    let me: serde_json::Value = serde_json::from_str(&whoami.stdout).unwrap();
    assert_eq!(me["user"]["id"], lab.owner_id.as_str());

    // A second login on the same context keeps its pin without --fingerprint.
    let output = quoro(
        &["login", "--token-stdin"],
        &lab.environment(&[]).with_stdin(lab.token.clone(), false),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_the_primary_refuses_writes_nothing() {
    let lab = lab().await;
    let output = lab.login_piped(&format!("tok_{}", "Z".repeat(40))).await;
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output.stderr.contains("the primary refused the token")
            && output.stderr.contains("(unauthenticated)")
            && output.stderr.contains("nothing was written"),
        "{}",
        output.stderr
    );
    assert!(!output.stderr.contains("ZZZZ"), "{}", output.stderr);
    lab.nothing_written();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_credential_or_no_token_is_refused_before_anything_is_sent() {
    let lab = lab().await;
    for (stdin, expected) in [
        (format!("atk_{}", "A".repeat(26)), "an agent's credential"),
        (format!("akey_{}", "A".repeat(40)), "an agent's credential"),
        ("not-a-token".to_string(), "starts with tok_"),
        (String::new(), "is not a token"),
        ("tok_ two words".to_string(), "is not a token"),
    ] {
        let output = lab.login_piped(&stdin).await;
        assert_eq!(output.code, exit::USAGE, "{stdin}: {}", output.stderr);
        assert!(
            output.stderr.contains(expected),
            "{stdin}: {}",
            output.stderr
        );
        assert!(
            stdin.len() < 5 || !output.stderr.contains(&stdin[5..]),
            "{}",
            output.stderr
        );
    }
    assert_eq!(lab.primary.requests(), 0, "nothing reached the primary");
    lab.nothing_written();

    // No terminal and no --token-stdin: the two ways are named.
    let output = quoro(
        &[
            "login",
            "--url",
            &lab.primary.url,
            "--fingerprint",
            &lab.primary.fingerprint,
        ],
        &lab.environment(&[]),
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("--token-stdin")
            && output.stderr.contains("on a terminal")
            && output.stderr.contains("Your account → API tokens"),
        "{}",
        output.stderr
    );
    assert_eq!(lab.primary.requests(), 0);
    lab.nothing_written();

    // A token is never an argument.
    let output = quoro(&["login", "--token", &lab.token], &lab.environment(&[])).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    lab.nothing_written();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_a_terminal_the_certificate_is_confirmed_then_the_token_asked_for() {
    let lab = lab().await;
    let typed = format!("y\n{}\n", lab.token);
    let output = quoro(
        &["login", "--url", &lab.primary.url],
        &lab.environment(&[]).with_stdin(typed, true),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let trust = output.stderr.find("Trust it? [y/N]").expect("trust asked");
    let token = output
        .stderr
        .find("Create one in the panel: Your account → API tokens")
        .expect("token asked");
    assert!(trust < token, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("({}/account)", lab.primary.url)),
        "{}",
        output.stderr
    );
    lab.assert_quiet(&output);
    let config = read(&lab.config_dir().join("config.toml"));
    assert!(
        config.contains("current = \"default\"")
            && config.contains(&format!("fingerprint = \"{}\"", lab.primary.fingerprint)),
        "{config}"
    );

    // An untrusted certificate stops the login before the token is asked for.
    let other = lab.home.join("other");
    let output = quoro(
        &["login", "--url", &lab.primary.url],
        &Environment::new([("XDG_CONFIG_HOME", other.display().to_string())], false)
            .with_stdin(format!("n\n{}\n", lab.token), true),
    )
    .await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(!output.stderr.contains("Token:"), "{}", output.stderr);
    assert!(!other.join("quoro/config.toml").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logout_forgets_the_token_and_points_to_the_panel_to_revoke_it() {
    let lab = lab().await;
    assert_eq!(lab.login_piped(&lab.token).await.code, exit::OK);
    let credentials_path = lab.config_dir().join("credentials.toml");

    // There is no --revoke: the client cannot tell which token it holds.
    let output = quoro(&["logout", "--revoke"], &lab.environment(&[])).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(read(&credentials_path).contains(&lab.token));

    let output = quoro(
        &["logout"],
        &lab.environment(&[("QUORO_TOKEN", "tok_from_ci")]),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stdout
            .starts_with("Logged out of lab: its token was removed from ")
            && output.stdout.contains("QUORO_TOKEN is set"),
        "{}",
        output.stdout
    );
    assert_eq!(
        output.stdout.lines().last(),
        Some(
            format!(
                "The token still works until it is revoked: revoke it in the panel, \
                 Your account → API tokens ({}/account).",
                lab.primary.url
            )
            .as_str()
        ),
        "{}",
        output.stdout
    );
    lab.assert_quiet(&output);
    assert_eq!(read(&credentials_path), "");
    assert_eq!(mode(&credentials_path), 0o600);
    let config = read(&lab.config_dir().join("config.toml"));
    assert!(config.contains("[contexts.\"lab\"]"), "the context stays");

    // Without a credential the primary answers unauthenticated.
    let whoami = quoro(&["whoami"], &lab.environment(&[])).await;
    assert_eq!(whoami.code, exit::OTHER, "{}", whoami.stderr);
    let output = quoro(&["logout"], &lab.environment(&[])).await;
    assert_eq!(output.code, exit::OK);
    assert_eq!(
        output.stdout,
        "No credential is stored for lab; nothing to remove.\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_logout_forgets_the_key_path_and_the_cached_token() {
    let lab = lab().await;
    let (_, key) = lab
        .primary
        .agent(&lab.token, &lab.owner_id, "operator")
        .await;
    let key_path = key_file(&lab.home.join("keys"), "operator.key", &key, 0o600);
    let key_path_text = key_path.display().to_string();
    let output = quoro(
        &[
            "--context",
            "lab",
            "login",
            "--agent",
            "--key-file",
            &key_path_text,
            "--url",
            &lab.primary.url,
            "--fingerprint",
            &lab.primary.fingerprint,
        ],
        &lab.environment(&[]),
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let cache = lab.home.join("run/quoro/lab.token");
    assert!(cache.exists());

    let output = quoro(&["logout"], &lab.environment(&[])).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stdout
            .contains("its agent key file path was removed"),
        "{}",
        output.stdout
    );
    assert_eq!(
        output.stdout.lines().last(),
        Some(
            format!(
                "The agent key still works until it is revoked: a human revokes it on the \
                 agent's page in the panel, Agents ({}/agents).",
                lab.primary.url
            )
            .as_str()
        ),
        "{}",
        output.stdout
    );
    assert!(!cache.exists(), "the cached token is gone");
    let credentials = read(&lab.config_dir().join("credentials.toml"));
    assert!(!credentials.contains(&key_path_text), "{credentials}");
    assert!(key_path.exists(), "the key file itself is the user's");
}

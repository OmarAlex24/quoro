//! `quoro mcp install` into temporary home and project folders: each
//! client's configuration file and skills directory, a merge that keeps what
//! else the file holds, re-runs that change nothing, and the contexts and
//! folders it refuses before writing anything.

use quoro_cli::Environment;
use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

const PIN: &str = "sha256:a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
const EXECUTABLE: &str = "/opt/quoro/bin/quoro";

/// A temporary tree: `home/`, `xdg/quoro/` with the contexts, `project/`.
struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "quoro-mcp-install-{label}-{}-{unique}",
            std::process::id()
        ));
        for directory in ["home", "xdg/quoro", "project", "keys"] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
        }
        let key = root.join("keys/agent.key");
        write(&key, "akey_0123456789abcdef\n", 0o600);
        let shared = root.join("keys/shared.key");
        write(&shared, "akey_0123456789abcdef\n", 0o644);
        write(
            &root.join("xdg/quoro/config.toml"),
            &format!(
                "current = \"lab\"\n\n\
                 [contexts.lab]\nurl = \"https://lab.example:8443\"\nfingerprint = \"{PIN}\"\n\n\
                 [contexts.prod]\nurl = \"https://prod.example\"\nfingerprint = \"{PIN}\"\n\n\
                 [contexts.human]\nurl = \"https://lab.example:8443\"\nfingerprint = \"{PIN}\"\n\n\
                 [contexts.system]\nurl = \"https://lab.example\"\ntls = \"system\"\n\n\
                 [contexts.shared]\nurl = \"https://lab.example\"\nfingerprint = \"{PIN}\"\n"
            ),
            0o644,
        );
        write(
            &root.join("xdg/quoro/credentials.toml"),
            &format!(
                "[contexts.lab]\nkey_file = \"{0}\"\n\n[contexts.prod]\nkey_file = \"{0}\"\n\n\
                 [contexts.human]\ntoken = \"tok_secret\"\n\n\
                 [contexts.system]\nkey_file = \"{0}\"\n\n[contexts.shared]\nkey_file = \"{1}\"\n",
                key.display(),
                shared.display()
            ),
            0o600,
        );
        Self { root }
    }

    fn home(&self) -> PathBuf {
        self.root.join("home")
    }

    fn project(&self) -> PathBuf {
        self.root.join("project")
    }

    fn environment(&self, extra: &[(&str, &str)]) -> Environment {
        let mut vars = vec![
            ("HOME".to_string(), self.home().display().to_string()),
            (
                "XDG_CONFIG_HOME".to_string(),
                self.root.join("xdg").display().to_string(),
            ),
        ];
        vars.extend(
            extra
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
        );
        Environment::new(vars, false)
            .with_cwd(self.project())
            .with_executable(EXECUTABLE)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn write(path: &Path, text: &str, mode: u32) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

struct Output {
    code: u8,
    stdout: String,
    stderr: String,
}

async fn quoro(args: &[&str], environment: &Environment) -> Output {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let code = quoro_cli::run_with(
        args.iter().map(ToString::to_string).collect(),
        environment,
        &mut stdout,
        &mut stderr,
    )
    .await;
    Output {
        code,
        stdout: String::from_utf8(stdout).unwrap(),
        stderr: String::from_utf8(stderr).unwrap(),
    }
}

fn entry(context: &str) -> Value {
    json!({
        "type": "stdio",
        "command": EXECUTABLE,
        "args": ["mcp", "stdio", "--context", context],
    })
}

/// The skill directory holds exactly the embedded files.
fn assert_skill(directory: &Path) {
    for (path, text) in quoro_cli::skill::FILES {
        let written = std::fs::read_to_string(directory.join(path))
            .unwrap_or_else(|e| panic!("{}/{path}: {e}", directory.display()));
        assert_eq!(written, *text, "{path}");
    }
    let count = std::fs::read_dir(directory.join("references"))
        .unwrap()
        .count()
        + 1;
    assert_eq!(count, quoro_cli::skill::FILES.len());
}

#[tokio::test]
async fn claude_user_scope_merges_into_claude_json_and_installs_the_skill() {
    let fixture = Fixture::new("claude");
    let environment = fixture.environment(&[]);
    let config = fixture.home().join(".claude.json");
    // A file Claude Code wrote, in an order serde_json would not keep.
    let original = "{\n  \"numStartups\": 7,\n  \"projects\": {\"/x\": {\"mcpServers\": {}}},\n  \"autoUpdates\": false\n}\n";
    write(&config, original, 0o644);

    let output = quoro(&["mcp", "install"], &environment).await;
    assert_eq!(output.code, 0, "{}", output.stderr);
    let config_text = std::fs::read_to_string(&config).unwrap();
    assert!(
        config_text.starts_with(&original[..original.len() - 3]),
        "the other members keep their bytes:\n{config_text}"
    );
    let document: Value = serde_json::from_str(&config_text).unwrap();
    assert_eq!(document["mcpServers"]["quoro"], entry("lab"));
    assert_eq!(document["numStartups"], 7);
    assert_eq!(mode(&config), 0o644, "the file keeps its mode");
    let skill = fixture.home().join(".claude/skills/quoro");
    assert_skill(&skill);
    assert!(
        output.stdout.contains(&format!(
            "MCP server 'quoro' for Claude Code (user scope): added to {}",
            config.display()
        )),
        "{}",
        output.stdout
    );
    assert!(
        output
            .stdout
            .contains(&format!("runs {EXECUTABLE} mcp stdio --context lab")),
        "{}",
        output.stdout
    );
    assert!(output.stdout.contains("installed in"), "{}", output.stdout);

    // Again: nothing changes.
    let again = quoro(&["mcp", "install", "--client", "claude"], &environment).await;
    assert_eq!(again.code, 0, "{}", again.stderr);
    assert_eq!(std::fs::read_to_string(&config).unwrap(), config_text);
    assert!(again.stdout.contains("unchanged in"), "{}", again.stdout);
    assert!(
        again.stdout.contains("Skill 'quoro': unchanged"),
        "{}",
        again.stdout
    );

    // Another context replaces the entry and says what it ran; a stale
    // skill copy is replaced.
    std::fs::write(skill.join("references/old.md"), "old").unwrap();
    let other = quoro(&["mcp", "install", "--context", "prod"], &environment).await;
    assert_eq!(other.code, 0, "{}", other.stderr);
    assert!(
        other
            .stdout
            .contains(&format!("it ran {EXECUTABLE} mcp stdio --context lab")),
        "{}",
        other.stdout
    );
    assert!(
        other.stdout.contains("Skill 'quoro': replaced"),
        "{}",
        other.stdout
    );
    assert!(!skill.join("references/old.md").exists());
    assert_skill(&skill);
    let document: Value = serde_json::from_str(&std::fs::read_to_string(&config).unwrap()).unwrap();
    assert_eq!(document["mcpServers"]["quoro"], entry("prod"));
}

#[tokio::test]
async fn each_client_and_scope_writes_its_own_files() {
    let fixture = Fixture::new("clients");
    let environment = fixture.environment(&[]);

    let claude = quoro(&["mcp", "install", "--scope", "project"], &environment).await;
    assert_eq!(claude.code, 0, "{}", claude.stderr);
    let mcp_json = fixture.project().join(".mcp.json");
    let document: Value =
        serde_json::from_str(&std::fs::read_to_string(&mcp_json).unwrap()).unwrap();
    assert_eq!(document, json!({ "mcpServers": { "quoro": entry("lab") } }));
    assert_eq!(
        mode(&mcp_json),
        0o644,
        "a project file is meant to be committed"
    );
    assert_skill(&fixture.project().join(".claude/skills/quoro"));

    let cursor = quoro(&["mcp", "install", "--client", "cursor"], &environment).await;
    assert_eq!(cursor.code, 0, "{}", cursor.stderr);
    let cursor_json = fixture.home().join(".cursor/mcp.json");
    let document: Value =
        serde_json::from_str(&std::fs::read_to_string(&cursor_json).unwrap()).unwrap();
    assert_eq!(document["mcpServers"]["quoro"], entry("lab"));
    assert_eq!(
        mode(&cursor_json),
        0o600,
        "a new file in the home is private"
    );
    assert_skill(&fixture.home().join(".cursor/skills/quoro"));

    let codex_config = fixture.home().join(".codex/config.toml");
    write(
        &codex_config,
        "# mine\nmodel = \"o3\"\n\n[mcp_servers.other]\ncommand = \"x\"\n",
        0o600,
    );
    let codex = quoro(&["mcp", "install", "--client", "codex"], &environment).await;
    assert_eq!(codex.code, 0, "{}", codex.stderr);
    assert_eq!(
        std::fs::read_to_string(&codex_config).unwrap(),
        format!(
            "# mine\nmodel = \"o3\"\n\n[mcp_servers.other]\ncommand = \"x\"\n\n\
             [mcp_servers.quoro]\ncommand = \"{EXECUTABLE}\"\nargs = [\"mcp\", \"stdio\", \"--context\", \"lab\"]\n"
        )
    );
    assert_skill(&fixture.home().join(".agents/skills/quoro"));

    let project = quoro(
        &["mcp", "install", "--client", "codex", "--scope", "project"],
        &environment,
    )
    .await;
    assert_eq!(project.code, 0, "{}", project.stderr);
    assert!(fixture.project().join(".codex/config.toml").is_file());
    assert_skill(&fixture.project().join(".agents/skills/quoro"));
    assert!(project.stdout.contains("trust"), "{}", project.stdout);
}

#[tokio::test]
async fn contexts_and_folders_that_cannot_run_the_relay_are_refused() {
    let fixture = Fixture::new("refused");
    let environment = fixture.environment(&[]);
    let config = fixture.home().join(".claude.json");

    for (context, code, expected) in [
        (
            "human",
            2,
            "quoro login --agent --key-file PATH --context human",
        ),
        ("system", 2, "--fingerprint"),
        ("shared", 1, "chmod 600"),
        ("missing", 2, "'missing' is not defined"),
    ] {
        let output = quoro(&["mcp", "install", "--context", context], &environment).await;
        assert_eq!(output.code, code, "{context}: {}", output.stderr);
        assert!(
            output.stderr.contains(expected),
            "{context}: {}",
            output.stderr
        );
        assert!(!output.stderr.contains("tok_secret"));
        assert!(!config.exists(), "{context}: nothing is written");
    }

    let ad_hoc = fixture.environment(&[
        ("QUORO_URL", "https://ci.example"),
        ("QUORO_FINGERPRINT", PIN),
    ]);
    let output = quoro(&["mcp", "install"], &ad_hoc).await;
    assert_eq!(output.code, 2, "{}", output.stderr);
    assert!(output.stderr.contains("ad-hoc"), "{}", output.stderr);

    // A skill directory this command did not write is left alone, and so is
    // the configuration.
    let foreign = fixture.home().join(".claude/skills/quoro");
    write(
        &foreign.join("SKILL.md"),
        "---\nname: someone-else\n---\n",
        0o644,
    );
    let output = quoro(&["mcp", "install"], &environment).await;
    assert_eq!(output.code, 1, "{}", output.stderr);
    assert!(output.stderr.contains("move it away"), "{}", output.stderr);
    assert!(!config.exists());
    assert_eq!(
        std::fs::read_to_string(foreign.join("SKILL.md")).unwrap(),
        "---\nname: someone-else\n---\n"
    );

    // A configuration that is not JSON is never overwritten.
    std::fs::remove_dir_all(&foreign).unwrap();
    write(&config, "{ not json", 0o600);
    let output = quoro(&["mcp", "install"], &environment).await;
    assert_eq!(output.code, 1, "{}", output.stderr);
    assert!(
        output.stderr.contains("nothing was written"),
        "{}",
        output.stderr
    );
    assert_eq!(std::fs::read_to_string(&config).unwrap(), "{ not json");

    let output = quoro(&["mcp", "install", "--client", "vscode"], &environment).await;
    assert_eq!(output.code, 2);
}

/// A client that rewrote the file in its own layout (key order, spacing)
/// still holds the same entry: a re-run leaves the file alone.
#[tokio::test]
async fn an_entry_the_client_reformatted_is_unchanged() {
    let fixture = Fixture::new("reformatted");
    let environment = fixture.environment(&[]);
    let config = fixture.home().join(".cursor/mcp.json");
    let original = format!(
        "{{\"mcpServers\":{{\"quoro\":{{\"command\":\"{EXECUTABLE}\",\"args\":[\"mcp\",\"stdio\",\"--context\",\"lab\"],\"type\":\"stdio\"}}}}}}"
    );
    write(&config, &original, 0o600);
    let output = quoro(&["mcp", "install", "--client", "cursor"], &environment).await;
    assert_eq!(output.code, 0, "{}", output.stderr);
    assert!(output.stdout.contains("unchanged in"), "{}", output.stdout);
    assert_eq!(std::fs::read_to_string(&config).unwrap(), original);
}

#[tokio::test]
async fn mcp_help_points_to_the_relay() {
    let fixture = Fixture::new("help");
    let output = quoro(&["mcp", "--help"], &fixture.environment(&[])).await;
    assert_eq!(output.code, 0);
    assert!(output.stdout.contains("install"), "{}", output.stdout);
    assert!(
        output.stdout.contains("quoro mcp stdio --context NAME"),
        "{}",
        output.stdout
    );
}

//! The `quoro` binary (D-0054).
//!
//! One binary serves the HTTPS listener, issues the first-run setup token,
//! joins a cluster and runs the node agent (`join`, `node`), and relays MCP
//! over stdio to a primary for a local MCP client (`mcp stdio`). Host command
//! lines are parsed by hand: they have a few commands and a handful of flags,
//! so the `match` below is less code than a parser. Every other command line
//! goes to the client CLI (`quoro_cli`), which talks to a primary's API; the
//! hand-offs are the unknown-command arm, `mcp` other than `mcp stdio`, `audit`
//! other than `verify`, and `--help`.

mod advertise;
mod audit_cli;
mod config;
mod mcp_stdio;
mod node;
mod null_engine;
mod panel;
mod privilege;
mod restore;
mod serve;
mod tls;
mod vuln_cli;

use quoro_control_plane::auth::setup_token;
use quoro_control_plane::store::Store;
use std::path::PathBuf;
use std::process::ExitCode;

/// The host commands: printed by `quoro help host` and on a host command's
/// usage error.
const USAGE: &str = "\
usage: quoro <command> [options]

commands:
  serve [--config PATH] [--role node] [--skip-upgrade-backup]
                                        run the HTTPS listener (D-0054)
  setup-token [--config PATH]           print a one-time first-run setup token
  version                               print the version
  join <url> --token TOKEN --fingerprint FINGERPRINT [--config PATH]
            [--role node|edge|witness]
                                        enroll this machine as a node (the
                                        default, runs microVMs), as an edge
                                        (runs the proxy, no engine, no KVM)
                                        or as a witness (only votes, no
                                        engine, no KVM)
  node [--config PATH]                  run the node agent with the role joined
  node set-primary <url> [--config PATH] [--no-restart]
                                        point this enrolled node at its
                                        primary's new address
  mcp stdio --context NAME [--url URL] [--fingerprint FINGERPRINT]
            [--key-file PATH] [--justification TEXT]
  mcp stdio --url URL --fingerprint FINGERPRINT --key-file PATH
            [--justification TEXT]
                                        serve MCP over stdio for a local MCP
                                        client, relayed to the primary's /mcp
  audit verify --file PATH [--prev-hash HEX]
                                        verify an exported JSON Lines segment
  audit verify --db PATH                verify a database's audit_log, offline
  vuln import <dir> [--config PATH]     import OSV ecosystem archives and the
                                        KEV catalog from a directory, offline
  restore --endpoint URL --region REGION --bucket NAME --prefix PREFIX
          --cluster-id CLS --credentials-file PATH --recovery-key-file PATH
          --signing-key-file PATH --data-key-file PATH --data-dir DIR
          [--backup BKP] [--node-id NOD] [--dry-run]
                                        restore a control-plane database from
                                        its backup repository, offline (ADR 0148)

The configuration file defaults to /etc/quoro/quoro.toml; a missing file means
the built-in defaults.

`quoro serve` takes the pre-upgrade backup itself when a migration is pending
(backup-format.md section 7.2). If that backup fails the start stops without
migrating; `--skip-upgrade-backup` starts once without it, records the skip in
the audit log and migrates.

`quoro restore` names the node on this host that takes the primary status at
the primary's next start: the one --node-id gives, otherwise the one in
/var/lib/quoro-node/identity.json when that file exists (backup-format.md
section 8.2).

quoro mcp stdio speaks MCP on stdin/stdout as one agent. The agent key
(akey_...) is read from --key-file, a file only its owner may read (chmod 600);
it is never accepted on the command line. The key is exchanged for a
five-minute access token, renewed before it expires, and every message goes
to the primary at --url over TLS pinned to --fingerprint (the sha256: value a
join command carries). The primary authorizes and audits every tool call;
--justification is recorded with each token the session mints.

With --context NAME the URL, the fingerprint and the key file come from that
context of the client's config.toml and credentials.toml (quoro login --agent
saves them; quoro mcp install writes this command into an MCP client). A flag
given as well overrides the context, and QUORO_AGENT_KEY_FILE overrides the
context's key file, as it does for every client command.";

/// What `quoro version` adds when the control plane was compiled with its
/// dev-only feature, however it was turned on (ADR 0223). CI fails a default
/// or a release binary that prints it.
const DEV_BUILD: &str = if quoro_control_plane::DEV_WARM_POLICY {
    " (dev build: dev-warm-policy, ADR 0223; not a release)"
} else {
    ""
};

#[tokio::main]
async fn main() -> ExitCode {
    // Logs to stderr, which systemd forwards to journald (D-0054).
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else {
        return quoro_cli::run(Vec::new()).await;
    };

    match command.as_str() {
        "version" | "--version" | "-V" => {
            println!("quoro {}{DEV_BUILD}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        "serve" => serve_command(rest).await,
        "setup-token" => issue_token(rest).await,
        "join" => join_command(rest).await,
        "node" => node_command(rest).await,
        "audit" => audit_cli::run(rest).await,
        "vuln" => vuln_cli::run(rest).await,
        "restore" => {
            match restore::run(rest, std::path::Path::new(config::DEFAULT_NODE_DATA_DIR)).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(restore::Error::Usage(message)) => usage_error(&message),
                Err(restore::Error::Failed(message)) => {
                    eprintln!("quoro restore: {message}");
                    ExitCode::from(1)
                }
            }
        }
        "mcp" => mcp_command(rest).await,
        "help" if rest == ["host"] => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        // The client's help, which ends with a line pointing to
        // `quoro help host`.
        "--help" | "-h" | "help" => quoro_cli::run(args.clone()).await,
        // Not a host command: the client CLI's.
        _ => quoro_cli::run(args.clone()).await,
    }
}

/// Hands `quoro <word> <args...>` to the client CLI.
pub(crate) async fn client(word: &str, args: &[String]) -> ExitCode {
    quoro_cli::run(
        std::iter::once(word.to_string())
            .chain(args.iter().cloned())
            .collect(),
    )
    .await
}

/// Prints the usage text on stderr, the way a usage error does.
fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

/// `quoro serve [--config PATH] [--role node] [--skip-upgrade-backup]`.
async fn serve_command(args: &[String]) -> ExitCode {
    let flags = match parse_serve_flags(args) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let path = config_path(&flags);
    let role = flag(&flags, "role").unwrap_or("node");
    let skip_upgrade_backup = flag_boolean(&flags, "skip-upgrade-backup");

    // D-0054 defines three roles; M1 runs the primary, which is the node role.
    match role {
        "node" => {}
        "witness" => {
            // A witness is its own agent on its own machine (ADR 0170).
            eprintln!(
                "a witness is not a serve role; enroll the machine with 'quoro join --role witness' and run 'quoro node'"
            );
            return ExitCode::from(2);
        }
        "edge" => {
            // An edge is its own agent on its own machine, not a primary role.
            eprintln!(
                "an edge is not a serve role; enroll the machine with 'quoro join --role edge' and run 'quoro node'"
            );
            return ExitCode::from(2);
        }
        other => {
            eprintln!("unknown role '{other}'; expected node, witness or edge");
            return ExitCode::from(2);
        }
    }

    let config = match config::Config::load(&path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("quoro serve: cannot read {}: {e}", path.display());
            return ExitCode::from(1);
        }
    };

    match serve::run(config, skip_upgrade_backup).await {
        Ok(code) => exit_code(code),
        Err(e) => {
            eprintln!("quoro serve: {e}");
            ExitCode::from(1)
        }
    }
}

/// `quoro join <url> --token TOKEN --fingerprint FINGERPRINT [--config PATH]
/// [--role node|edge|witness]`.
async fn join_command(args: &[String]) -> ExitCode {
    let Some((url, rest)) = args.split_first().filter(|(url, _)| !url.starts_with("--")) else {
        return usage_error("join needs the primary's URL");
    };
    let flags = match parse_flags(rest, &["config", "token", "fingerprint", "role"]) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let (Some(token), Some(fingerprint)) = (flag(&flags, "token"), flag(&flags, "fingerprint"))
    else {
        return usage_error("join needs --token and --fingerprint");
    };
    let role = match join_role(&flags) {
        Ok(role) => role,
        Err(e) => return usage_error(&e),
    };
    let Some(config) = load_config("join", &config_path(&flags)) else {
        return ExitCode::from(1);
    };
    let target = quoro_node_agent::JoinTarget {
        url: url.clone(),
        token: token.to_string(),
        fingerprint: fingerprint.to_string(),
    };
    exit_code(node::join(&config, &target, role).await)
}

/// The `--role` of a join command: `node` when it was not given (ADR 0143).
fn join_role(flags: &[(String, String)]) -> Result<node::Role, String> {
    match flag(flags, "role") {
        Some(text) => node::Role::parse(text),
        None => Ok(node::Role::default()),
    }
}

/// `quoro mcp stdio [--context NAME] [--url URL] [--fingerprint FINGERPRINT]
/// [--key-file PATH] [--justification TEXT]`; `quoro mcp` and any other
/// `mcp` command line (`mcp install`) are the client's.
async fn mcp_command(args: &[String]) -> ExitCode {
    let Some((_, rest)) = args.split_first().filter(|(word, _)| *word == "stdio") else {
        return client("mcp", args).await;
    };
    let flags = match parse_flags(
        rest,
        &["context", "url", "fingerprint", "key-file", "justification"],
    ) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let context = match flag(&flags, "context") {
        Some(name) => match quoro_cli::stdio_context(name) {
            Ok(context) => Some((name, context)),
            Err(message) => {
                eprintln!("quoro mcp stdio: {message}");
                return ExitCode::from(1);
            }
        },
        None => None,
    };
    let key_variable = std::env::var("QUORO_AGENT_KEY_FILE")
        .ok()
        .filter(|path| !path.is_empty());
    let options = match stdio_options(&flags, key_variable.as_deref(), context) {
        Ok(options) => options,
        Err(StdioError::Usage(message)) => return usage_error(&message),
        Err(StdioError::Context(message)) => {
            eprintln!("quoro mcp stdio: {message}");
            return ExitCode::from(1);
        }
    };
    mcp_stdio::run(options).await
}

/// Why `quoro mcp stdio`'s options could not be put together.
#[derive(Debug)]
enum StdioError {
    /// The command line is incomplete.
    Usage(String),
    /// The context lacks what the relay needs.
    Context(String),
}

/// The relay's options: each flag, else for the key `QUORO_AGENT_KEY_FILE`,
/// else the context's value.
fn stdio_options(
    flags: &[(String, String)],
    key_variable: Option<&str>,
    context: Option<(&str, quoro_cli::StdioContext)>,
) -> Result<mcp_stdio::Options, StdioError> {
    let justification = flag(flags, "justification")
        .unwrap_or(mcp_stdio::DEFAULT_JUSTIFICATION)
        .to_string();
    let key_override = flag(flags, "key-file").or(key_variable).map(PathBuf::from);
    let Some((name, context)) = context else {
        let (Some(url), Some(fingerprint), Some(key_file)) =
            (flag(flags, "url"), flag(flags, "fingerprint"), key_override)
        else {
            return Err(StdioError::Usage(
                "mcp stdio needs --context NAME, or --url, --fingerprint and --key-file"
                    .to_string(),
            ));
        };
        return Ok(mcp_stdio::Options {
            url: url.to_string(),
            fingerprint: fingerprint.to_string(),
            key_file,
            justification,
        });
    };
    let fingerprint = flag(flags, "fingerprint")
        .map(str::to_string)
        .or(context.fingerprint)
        .ok_or_else(|| {
            StdioError::Context(format!(
                "the context '{name}' trusts the system's CA store and the relay pins the \
                 primary's certificate: pass --fingerprint sha256:..."
            ))
        })?;
    let key_file = key_override.or(context.key_file).ok_or_else(|| {
        StdioError::Context(format!(
            "the context '{name}' has no agent key file, and MCP runs as an agent: log in with \
             quoro login --agent --key-file PATH --context {name}, or pass --key-file"
        ))
    })?;
    Ok(mcp_stdio::Options {
        url: flag(flags, "url").map_or(context.url, str::to_string),
        fingerprint,
        key_file,
        justification,
    })
}

/// `quoro node [--config PATH]`: the node agent alone, until SIGTERM.
/// `quoro node set-primary <url> ...` points an enrolled node at a new
/// primary address instead of running the agent.
async fn node_command(args: &[String]) -> ExitCode {
    if args
        .first()
        .is_some_and(|argument| argument == "set-primary")
    {
        return set_primary_command(&args[1..]).await;
    }
    let flags = match parse_flags(args, &["config"]) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let Some(config) = load_config("node", &config_path(&flags)) else {
        return ExitCode::from(1);
    };
    match node::run(&config, serve::signal()).await {
        Ok(code) => exit_code(code),
        Err(e) => {
            eprintln!("quoro node: {e}");
            ExitCode::from(1)
        }
    }
}

/// `quoro node set-primary <url> [--config PATH] [--no-restart]`: hand an
/// enrolled node its primary's new address (ADR 0139).
async fn set_primary_command(args: &[String]) -> ExitCode {
    let Some((url, rest)) = args.split_first().filter(|(url, _)| !url.starts_with("--")) else {
        return usage_error("node set-primary needs the primary's new URL");
    };
    let flags = match parse_set_primary_flags(rest) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let Some(config) = load_config("node set-primary", &config_path(&flags)) else {
        return ExitCode::from(1);
    };
    exit_code(node::set_primary(&config, url, !flag_boolean(&flags, "no-restart")).await)
}

/// Reads `serve`'s flags: `--config PATH`, `--role NAME` and the value-less
/// `--skip-upgrade-backup` (`backup-format.md` section 7.2).
fn parse_serve_flags(args: &[String]) -> Result<Vec<(String, String)>, String> {
    let mut flags = Vec::with_capacity(args.len() / 2);
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(name) = argument.strip_prefix("--") else {
            return Err(format!("unexpected argument '{argument}'"));
        };
        match name {
            "skip-upgrade-backup" => flags.push((name.to_string(), String::new())),
            "config" | "role" => {
                let Some(value) = args.next() else {
                    return Err(format!("'{argument}' needs a value"));
                };
                flags.push((name.to_string(), value.clone()));
            }
            _ => return Err(format!("unknown option '{argument}'")),
        }
    }
    Ok(flags)
}

/// Reads `node set-primary`'s flags: `--config PATH` and the value-less
/// `--no-restart`.
fn parse_set_primary_flags(args: &[String]) -> Result<Vec<(String, String)>, String> {
    let mut flags = Vec::with_capacity(args.len());
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(name) = argument.strip_prefix("--") else {
            return Err(format!("unexpected argument '{argument}'"));
        };
        match name {
            "no-restart" => flags.push((name.to_string(), String::new())),
            "config" => {
                let Some(value) = args.next() else {
                    return Err(format!("'{argument}' needs a value"));
                };
                flags.push((name.to_string(), value.clone()));
            }
            _ => return Err(format!("unknown option '{argument}'")),
        }
    }
    Ok(flags)
}

/// Whether a value-less flag was given.
fn flag_boolean(flags: &[(String, String)], name: &str) -> bool {
    flags.iter().any(|(key, _)| key == name)
}

/// Loads `quoro.toml`, printing the failure under `command`.
fn load_config(command: &str, path: &std::path::Path) -> Option<config::Config> {
    match config::Config::load(path) {
        Ok(config) => Some(config),
        Err(e) => {
            eprintln!("quoro {command}: cannot read {}: {e}", path.display());
            None
        }
    }
}

fn exit_code(code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

/// `quoro setup-token [--config PATH]`: a fresh token, unless an owner exists.
async fn issue_token(args: &[String]) -> ExitCode {
    let flags = match parse_flags(args, &["config"]) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let path = config_path(&flags);

    let config = match config::Config::load(&path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("quoro setup-token: cannot read {}: {e}", path.display());
            return ExitCode::from(1);
        }
    };
    // Run as root on a host where the control plane has its own user, the
    // token and SQLite's side files must still belong to that user.
    if let Some(dropped) = privilege::run_as_data_dir_owner(&config.data_dir) {
        return match dropped {
            Ok(code) => exit_code(code),
            Err(e) => {
                eprintln!(
                    "quoro setup-token: cannot run as the owner of {}: {e}",
                    config.data_dir.display()
                );
                ExitCode::from(1)
            }
        };
    }
    if let Err(e) = config::ensure_data_dir(&config.data_dir) {
        eprintln!(
            "quoro setup-token: cannot create {}: {e}",
            config.data_dir.display()
        );
        return ExitCode::from(1);
    }

    let database = config.data_dir.join("quoro.db");
    // A demoted primary writes nothing more (`node-agent-protocol.md`
    // section 7.4): read before `Store::open` migrates.
    match serve::demoted_by_term(&database).await {
        Ok(None) => {}
        Ok(Some(term)) => {
            eprintln!(
                "quoro setup-token: this host is a demoted primary at term {term}; it issues no setup token"
            );
            return ExitCode::from(1);
        }
        Err(e) => {
            eprintln!("quoro setup-token: {e}");
            return ExitCode::from(1);
        }
    }
    let store = match Store::open(&database).await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("quoro setup-token: cannot open {}: {e}", database.display());
            return ExitCode::from(1);
        }
    };
    match serve::owner_exists(&store).await {
        Ok(true) => {
            // A second token could not be used: setup is refused once a user
            // exists.
            eprintln!("an owner already exists");
            return ExitCode::from(1);
        }
        Ok(false) => {}
        Err(e) => {
            eprintln!("quoro setup-token: cannot read the user table: {e}");
            return ExitCode::from(1);
        }
    }

    match setup_token::issue(&config.data_dir) {
        Ok(token) => {
            // The secret goes to stdout alone, so a script can capture it.
            println!("{token}");
            eprintln!("this token is single use and valid for one hour");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!(
                "quoro setup-token: cannot write the token into {}: {e}",
                config.data_dir.display()
            );
            ExitCode::from(1)
        }
    }
}

pub(crate) fn usage_error(message: &str) -> ExitCode {
    eprintln!("quoro: {message}");
    usage()
}

/// The `--config` path, or the installer's default (D-0054).
fn config_path(flags: &[(String, String)]) -> PathBuf {
    flag(flags, "config").map_or_else(|| PathBuf::from(config::DEFAULT_PATH), PathBuf::from)
}

/// The value of `--name`, when it was given.
pub(crate) fn flag<'a>(flags: &'a [(String, String)], name: &str) -> Option<&'a str> {
    flags
        .iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

/// Reads `--name value` pairs, refusing an unknown option, a flag without a
/// value and a positional argument.
pub(crate) fn parse_flags(
    args: &[String],
    names: &[&str],
) -> Result<Vec<(String, String)>, String> {
    let mut flags = Vec::with_capacity(args.len() / 2);
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(name) = argument.strip_prefix("--") else {
            return Err(format!("unexpected argument '{argument}'"));
        };
        if !names.contains(&name) {
            return Err(format!("unknown option '{argument}'"));
        }
        let Some(value) = args.next() else {
            return Err(format!("'{argument}' needs a value"));
        };
        flags.push((name.to_string(), value.clone()));
    }
    Ok(flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|a| (*a).to_string()).collect()
    }

    #[test]
    fn flags_are_read_by_name_and_repeat_last() {
        let flags = parse_flags(
            &args(&["--config", "/tmp/a", "--role", "edge"]),
            &["config", "role"],
        )
        .unwrap();
        assert_eq!(flag(&flags, "config"), Some("/tmp/a"));
        assert_eq!(flag(&flags, "role"), Some("edge"));
        assert_eq!(flag(&flags, "missing"), None);

        let flags = parse_flags(&args(&["--config", "/a", "--config", "/b"]), &["config"]).unwrap();
        assert_eq!(flag(&flags, "config"), Some("/b"));
    }

    #[test]
    fn bad_arguments_are_refused() {
        assert!(parse_flags(&args(&["--nope", "1"]), &["config"]).is_err());
        assert!(parse_flags(&args(&["--config"]), &["config"]).is_err());
        assert!(parse_flags(&args(&["serve"]), &["config"]).is_err());
        assert!(parse_flags(&[], &["config"]).unwrap().is_empty());
    }

    #[test]
    fn set_primary_flags_carry_config_and_the_boolean() {
        let flags =
            parse_set_primary_flags(&args(&["--no-restart", "--config", "/etc/quoro/x.toml"]))
                .unwrap();
        assert_eq!(flag(&flags, "config"), Some("/etc/quoro/x.toml"));
        assert!(flag_boolean(&flags, "no-restart"));
        assert!(!flag_boolean(&flags, "restart"));

        assert!(
            parse_set_primary_flags(&args(&["--no-restart=yes"])).is_err(),
            "a value after --no-restart is a typo, not a flag"
        );
        assert!(parse_set_primary_flags(&args(&["--config"])).is_err());
        assert!(parse_set_primary_flags(&args(&["https://example.com"])).is_err());
    }

    #[test]
    fn join_role_defaults_to_node_and_refuses_unknown_roles() {
        assert_eq!(
            join_role(&[]).unwrap(),
            node::Role::Node,
            "a join without --role enrolls a node"
        );
        let flags = parse_flags(&args(&["--role", "edge"]), &["role"]).unwrap();
        assert_eq!(join_role(&flags).unwrap(), node::Role::Edge);
        let flags = parse_flags(&args(&["--role", "node"]), &["role"]).unwrap();
        assert_eq!(join_role(&flags).unwrap(), node::Role::Node);
        let flags = parse_flags(&args(&["--role", "witness"]), &["role"]).unwrap();
        assert_eq!(join_role(&flags).unwrap(), node::Role::Witness);

        let flags = parse_flags(&args(&["--role", "primary"]), &["role"]).unwrap();
        let err = join_role(&flags).expect_err("a primary does not join");
        assert!(err.contains("expected node, edge or witness"), "{err}");
    }

    #[test]
    fn serve_flags_carry_the_skip_and_refuse_a_value_after_it() {
        let flags = parse_serve_flags(&args(&["--skip-upgrade-backup", "--config", "/etc/q.toml"]))
            .unwrap();
        assert!(flag_boolean(&flags, "skip-upgrade-backup"));
        assert_eq!(flag(&flags, "config"), Some("/etc/q.toml"));
        assert_eq!(flag(&flags, "role"), None);

        assert!(!flag_boolean(
            &parse_serve_flags(&args(&["--role", "node"])).unwrap(),
            "skip-upgrade-backup"
        ));
        // A value after it is a typo, not a flag, and an unknown option is
        // refused as everywhere else.
        assert!(parse_serve_flags(&args(&["--skip-upgrade-backup=yes"])).is_err());
        assert!(parse_serve_flags(&args(&["--config"])).is_err());
        assert!(parse_serve_flags(&args(&["--nope", "1"])).is_err());
    }

    #[test]
    fn stdio_options_take_flags_then_the_key_variable_then_the_context() {
        let flags = |list: &[&str]| {
            parse_flags(
                &args(list),
                &["context", "url", "fingerprint", "key-file", "justification"],
            )
            .unwrap()
        };
        let context = |fingerprint: Option<&str>, key_file: Option<&str>| quoro_cli::StdioContext {
            url: "https://lab.example:8443".to_string(),
            fingerprint: fingerprint.map(str::to_string),
            key_file: key_file.map(PathBuf::from),
        };
        let pin = "sha256:aa";

        let options = stdio_options(
            &flags(&["--context", "lab"]),
            None,
            Some(("lab", context(Some(pin), Some("/k/agent.key")))),
        )
        .unwrap();
        assert_eq!(options.url, "https://lab.example:8443");
        assert_eq!(options.fingerprint, pin);
        assert_eq!(options.key_file, PathBuf::from("/k/agent.key"));
        assert_eq!(options.justification, mcp_stdio::DEFAULT_JUSTIFICATION);

        let options = stdio_options(
            &flags(&[
                "--context",
                "lab",
                "--url",
                "https://other:1",
                "--justification",
                "j",
            ]),
            Some("/env/agent.key"),
            Some(("lab", context(Some(pin), Some("/k/agent.key")))),
        )
        .unwrap();
        assert_eq!(
            options.url, "https://other:1",
            "a flag overrides the context"
        );
        assert_eq!(options.key_file, PathBuf::from("/env/agent.key"));
        assert_eq!(options.justification, "j");

        let options = stdio_options(
            &flags(&["--context", "lab", "--key-file", "/flag.key"]),
            Some("/env/agent.key"),
            Some(("lab", context(Some(pin), None))),
        )
        .unwrap();
        assert_eq!(
            options.key_file,
            PathBuf::from("/flag.key"),
            "the flag wins"
        );

        let error = stdio_options(
            &flags(&["--context", "lab"]),
            None,
            Some(("lab", context(Some(pin), None))),
        )
        .unwrap_err();
        assert!(
            matches!(&error, StdioError::Context(m) if m.contains("quoro login --agent")),
            "{error:?}"
        );
        let error = stdio_options(
            &flags(&["--context", "lab"]),
            None,
            Some(("lab", context(None, Some("/k")))),
        )
        .unwrap_err();
        assert!(
            matches!(&error, StdioError::Context(m) if m.contains("--fingerprint")),
            "{error:?}"
        );

        // Without a context every value is a flag, and the key may be the
        // variable's.
        assert!(matches!(
            stdio_options(&flags(&["--url", "https://x"]), None, None),
            Err(StdioError::Usage(_))
        ));
        let options = stdio_options(
            &flags(&["--url", "https://x", "--fingerprint", pin]),
            Some("/env/agent.key"),
            None,
        )
        .unwrap();
        assert_eq!(options.key_file, PathBuf::from("/env/agent.key"));
    }

    #[test]
    fn default_config_path_is_the_installer_location() {
        assert_eq!(
            config_path(&[]),
            PathBuf::from(config::DEFAULT_PATH),
            "a bare `quoro serve` reads /etc/quoro/quoro.toml"
        );
    }
}

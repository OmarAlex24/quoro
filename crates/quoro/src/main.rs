//! The `quoro` binary (D-0054).
//!
//! One binary serves the HTTPS listener, issues the first-run setup token,
//! joins a cluster and runs the node agent (`join`, `node`), and, from a later
//! milestone, serves MCP over stdio (`mcp`, M3). The command line is parsed by hand: it has five
//! commands and a handful of flags, so a parser dependency would be more code
//! and one more licence than the `match` below.

mod config;
mod node;
mod panel;
mod privilege;
mod serve;
mod tls;

use quoro_control_plane::auth::setup_token;
use quoro_control_plane::store::Store;
use std::path::PathBuf;
use std::process::ExitCode;

/// Printed on `--help` and on a command that does not exist.
const USAGE: &str = "\
usage: quoro <command> [options]

commands:
  serve [--config PATH] [--role node]   run the HTTPS listener (D-0054)
  setup-token [--config PATH]           print a one-time first-run setup token
  version                               print the version
  join <url> --token TOKEN --fingerprint FINGERPRINT [--config PATH]
                                        enroll this machine as a node
  node [--config PATH]                  run the node agent
  mcp stdio                             serve MCP over stdio (arrives in M3)

The configuration file defaults to /etc/quoro/quoro.toml; a missing file means
the built-in defaults.";

#[tokio::main]
async fn main() -> ExitCode {
    // Logs to stderr, which systemd forwards to journald (D-0054).
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else {
        return usage();
    };

    match command.as_str() {
        "version" | "--version" | "-V" => {
            println!("quoro {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        "serve" => serve_command(rest).await,
        "setup-token" => issue_token(rest).await,
        "join" => join_command(rest).await,
        "node" => node_command(rest).await,
        "mcp" => {
            eprintln!("quoro mcp arrives in M3");
            ExitCode::from(2)
        }
        "--help" | "-h" | "help" => usage(),
        other => {
            eprintln!("quoro: unknown command '{other}'");
            usage()
        }
    }
}

/// Prints the usage text on stderr, the way a usage error does.
fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

/// `quoro serve [--config PATH] [--role node]`.
async fn serve_command(args: &[String]) -> ExitCode {
    let flags = match parse_flags(args, &["config", "role"]) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let path = config_path(&flags);
    let role = flag(&flags, "role").unwrap_or("node");

    // D-0054 defines three roles; M1 runs the primary, which is the node role.
    match role {
        "node" => {}
        "witness" | "edge" => {
            eprintln!("role {role} arrives in M5");
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

    match serve::run(config).await {
        Ok(code) => exit_code(code),
        Err(e) => {
            eprintln!("quoro serve: {e}");
            ExitCode::from(1)
        }
    }
}

/// `quoro join <url> --token TOKEN --fingerprint FINGERPRINT [--config PATH]`.
async fn join_command(args: &[String]) -> ExitCode {
    let Some((url, rest)) = args.split_first().filter(|(url, _)| !url.starts_with("--")) else {
        return usage_error("join needs the primary's URL");
    };
    let flags = match parse_flags(rest, &["config", "token", "fingerprint"]) {
        Ok(flags) => flags,
        Err(e) => return usage_error(&e),
    };
    let (Some(token), Some(fingerprint)) = (flag(&flags, "token"), flag(&flags, "fingerprint"))
    else {
        return usage_error("join needs --token and --fingerprint");
    };
    let Some(config) = load_config("join", &config_path(&flags)) else {
        return ExitCode::from(1);
    };
    let target = quoro_node_agent::JoinTarget {
        url: url.clone(),
        token: token.to_string(),
        fingerprint: fingerprint.to_string(),
    };
    exit_code(node::join(&config, &target).await)
}

/// `quoro node [--config PATH]`: the node agent alone, until SIGTERM.
async fn node_command(args: &[String]) -> ExitCode {
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

fn usage_error(message: &str) -> ExitCode {
    eprintln!("quoro: {message}");
    usage()
}

/// The `--config` path, or the installer's default (D-0054).
fn config_path(flags: &[(String, String)]) -> PathBuf {
    flag(flags, "config").map_or_else(|| PathBuf::from(config::DEFAULT_PATH), PathBuf::from)
}

/// The value of `--name`, when it was given.
fn flag<'a>(flags: &'a [(String, String)], name: &str) -> Option<&'a str> {
    flags
        .iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

/// Reads `--name value` pairs, refusing an unknown option, a flag without a
/// value and a positional argument.
fn parse_flags(args: &[String], names: &[&str]) -> Result<Vec<(String, String)>, String> {
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
    fn default_config_path_is_the_installer_location() {
        assert_eq!(
            config_path(&[]),
            PathBuf::from(config::DEFAULT_PATH),
            "a bare `quoro serve` reads /etc/quoro/quoro.toml"
        );
    }
}

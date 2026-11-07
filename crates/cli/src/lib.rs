//! The client CLI of the `quoro` binary (ADR 0054).
//!
//! Host commands (`serve`, `join`, `node`, `restore`, ...) keep their own
//! hand-written parser in `crates/quoro`; every other command line is handed
//! to [`run`]. The client talks to a primary's HTTP API and to nothing else,
//! so everything it does is authorized and audited by the primary like any
//! API call.
//!
//! The command tree is small on purpose and a test holds it to fixed budgets
//! (`tests/budget.rs`): a command exists because a task needs it, and every
//! other operation stays reachable through `quoro api`.
//!
//! The primary is reached through a context (`context.rs`): a URL and either
//! the SHA-256 pin of its self-signed certificate or the host's CA store.
//! Errors from the API are mapped to fixed exit codes ([`exit`]) so a script
//! or an agent can branch on them without reading text.

mod api;
mod cli;
mod context;
mod error;
pub mod operations;
mod tls;

use clap::Parser as _;
use std::collections::BTreeMap;
use std::io::{IsTerminal as _, Write};
use std::process::ExitCode;

pub use error::exit;

/// The environment variables the client reads; nothing else is looked at.
const VARIABLES: &[&str] = &[
    "HOME",
    "XDG_CONFIG_HOME",
    "QUORO_CONTEXT",
    "QUORO_URL",
    "QUORO_FINGERPRINT",
    "QUORO_TOKEN",
];

/// What the client reads from its process: environment variables and whether
/// stdout is a terminal. Tests build one by hand, so they never change the
/// test process's environment.
#[derive(Clone, Debug, Default)]
pub struct Environment {
    vars: BTreeMap<String, String>,
    stdout_is_terminal: bool,
}

impl Environment {
    /// The current process's environment.
    #[must_use]
    pub fn from_process() -> Self {
        let vars = VARIABLES
            .iter()
            .filter_map(|name| Some(((*name).to_string(), std::env::var(name).ok()?)))
            .collect();
        Self {
            vars,
            stdout_is_terminal: std::io::stdout().is_terminal(),
        }
    }

    /// An environment with exactly these variables.
    #[must_use]
    pub fn new<K, V>(vars: impl IntoIterator<Item = (K, V)>, stdout_is_terminal: bool) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            vars: vars
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            stdout_is_terminal,
        }
    }

    /// A variable's value; an empty one counts as unset.
    fn var(&self, name: &str) -> Option<&str> {
        self.vars
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }
}

/// The client command tree, for the budget test and shell completions.
#[must_use]
pub fn command() -> clap::Command {
    <cli::Cli as clap::CommandFactory>::command()
}

/// Runs a client command line (`args` without the program name) against the
/// process's environment, stdout and stderr.
pub async fn run(args: Vec<String>) -> ExitCode {
    let environment = Environment::from_process();
    let code = run_with(
        args,
        &environment,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    )
    .await;
    ExitCode::from(code)
}

/// [`run`] with an explicit environment and output streams; returns the exit
/// code ([`exit`]).
pub async fn run_with(
    args: Vec<String>,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let parsed = cli::Cli::try_parse_from(std::iter::once("quoro".to_string()).chain(args));
    let cli = match parsed {
        Ok(cli) => cli,
        Err(error) => {
            // `--help` is an answer, not an error: stdout and exit 0.
            if !error.use_stderr() {
                let _ = write!(stdout, "{}", error.render());
                return exit::OK;
            }
            let _ = write!(stderr, "{}", error.render());
            return exit::USAGE;
        }
    };
    let result = match cli.command {
        cli::Command::Api(args) => api::run(&cli.global, args, environment, stdout).await,
        cli::Command::Completion { shell } => {
            clap_complete::generate(shell, &mut command(), "quoro", stdout);
            Ok(())
        }
    };
    match result {
        Ok(()) => exit::OK,
        Err(failure) => failure.report(stderr),
    }
}

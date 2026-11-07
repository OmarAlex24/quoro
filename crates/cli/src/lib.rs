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
//!
//! A human logs in with a personal API token typed at a hidden prompt or
//! piped with `--token-stdin` (`login.rs`), and `logout` forgets it
//! (`logout.rs`). An agent logs in with its key file; every command then mints
//! a short-lived access token from it, cached between commands (`agent.rs`).
//! An agent must say why it changes something (`--reason`), and a destructive
//! call it may not make yet can file an approval request and wait for a
//! human's decision (`approval.rs`).
//!
//! A folder is linked to a project and an app (`links.rs`), so `status` and
//! the `project` and `app` commands know what they act on; a resource is
//! named by name or id, never by an id flag (`scope.rs`).

mod agent;
mod api;
mod app;
mod approval;
mod cli;
mod client;
mod context;
mod error;
mod link;
mod links;
mod login;
mod logout;
pub mod operations;
mod project;
mod scope;
mod status;
mod terminal;
mod tls;
mod whoami;

use clap::Parser as _;
use std::collections::BTreeMap;
use std::io::{BufRead as _, IsTerminal as _, Read as _, Write};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

pub use error::exit;

/// The environment variables the client reads; nothing else is looked at.
const VARIABLES: &[&str] = &[
    "HOME",
    "XDG_CONFIG_HOME",
    "QUORO_CONTEXT",
    "QUORO_URL",
    "QUORO_FINGERPRINT",
    "QUORO_TOKEN",
    "QUORO_AGENT_KEY_FILE",
    "QUORO_JUSTIFICATION",
    "XDG_RUNTIME_DIR",
    "XDG_CACHE_HOME",
];

/// What the client reads from its process: environment variables, whether
/// stdout and stdin are terminals, and stdin itself. Tests build one by hand,
/// so they never change the test process's environment or read its stdin.
#[derive(Clone, Debug, Default)]
pub struct Environment {
    vars: BTreeMap<String, String>,
    stdout_is_terminal: bool,
    /// Whether a question can be asked (`quoro login`'s first contact and
    /// token prompt).
    stdin_is_terminal: bool,
    /// What stdin holds, for a test; `None` reads the process's stdin.
    input: Option<Input>,
    /// The directory the command runs in, where a link is looked up.
    cwd: Option<std::path::PathBuf>,
}

/// Bytes standing in for stdin. `Debug` never shows them: they may hold a
/// token.
#[derive(Clone)]
struct Input(Arc<Mutex<std::io::Cursor<Vec<u8>>>>);

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Input(..)")
    }
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
            stdin_is_terminal: std::io::stdin().is_terminal(),
            input: None,
            cwd: std::env::current_dir().ok(),
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
            stdin_is_terminal: false,
            input: None,
            cwd: None,
        }
    }

    /// This environment running in the directory `cwd`.
    #[must_use]
    pub fn with_cwd(mut self, cwd: impl Into<std::path::PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// This environment with `bytes` as stdin, a terminal when
    /// `is_terminal` (what a user would type at the prompts).
    #[must_use]
    pub fn with_stdin(mut self, bytes: impl Into<Vec<u8>>, is_terminal: bool) -> Self {
        self.input = Some(Input(Arc::new(Mutex::new(std::io::Cursor::new(
            bytes.into(),
        )))));
        self.stdin_is_terminal = is_terminal;
        self
    }

    /// One line from stdin, without its line ending.
    fn read_line(&self) -> std::io::Result<String> {
        let mut line = String::new();
        match &self.input {
            Some(Input(input)) => {
                let mut input = input
                    .lock()
                    .map_err(|_| std::io::Error::other("stdin is poisoned"))?;
                input.read_line(&mut line)?;
            }
            None => {
                std::io::stdin().lock().read_line(&mut line)?;
            }
        }
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }

    /// All of stdin.
    fn read_to_end(&self) -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        match &self.input {
            Some(Input(input)) => {
                let mut input = input
                    .lock()
                    .map_err(|_| std::io::Error::other("stdin is poisoned"))?;
                input.read_to_end(&mut bytes)?;
            }
            None => {
                std::io::stdin().lock().read_to_end(&mut bytes)?;
            }
        }
        Ok(bytes)
    }

    /// One line typed on the terminal with its echo off, so a secret never
    /// shows on screen. Stdin standing in for a test is read as it is.
    fn read_hidden_line(&self) -> std::io::Result<String> {
        if self.input.is_some() {
            return self.read_line();
        }
        terminal::read_hidden_line()
    }

    /// A variable's value; an empty one counts as unset.
    fn var(&self, name: &str) -> Option<&str> {
        self.vars
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    /// The command's justification: `--reason`, else `QUORO_JUSTIFICATION`.
    fn justification(&self, global: &cli::Global) -> Option<String> {
        global
            .reason
            .as_deref()
            .or(self.var("QUORO_JUSTIFICATION"))
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
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
        cli::Command::Login(args) => {
            login::run(&cli.global, args, environment, stdout, stderr).await
        }
        cli::Command::Logout => logout::run(&cli.global, environment, stdout),
        cli::Command::Whoami => whoami::run(&cli.global, environment, stdout).await,
        cli::Command::Link(args) => {
            link::link(&cli.global, args, environment, stdout, stderr).await
        }
        cli::Command::Unlink => link::unlink(environment, stdout),
        cli::Command::Status(args) => status::run(&cli.global, args, environment, stdout).await,
        cli::Command::Project(command) => {
            project::run(&cli.global, command, environment, stdout, stderr).await
        }
        cli::Command::App(command) => {
            app::run(&cli.global, command, environment, stdout, stderr).await
        }
        cli::Command::Api(args) => api::run(&cli.global, args, environment, stdout, stderr).await,
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

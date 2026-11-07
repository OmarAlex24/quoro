//! The client command tree.
//!
//! Every word here is held to the budgets in `tests/budget.rs`. A command is
//! added when a task needs it; an operation that only exists in the API is
//! reached with `quoro api`.

use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "quoro",
    about = "Operate a Quoro cluster through its primary's API",
    arg_required_else_help = true,
    after_help = "Host commands (serve, join, node, mcp stdio, audit verify, ...): quoro help host"
)]
pub struct Cli {
    #[command(flatten)]
    pub global: Global,
    #[command(subcommand)]
    pub command: Command,
}

/// The options every client command accepts.
#[derive(Debug, Args)]
pub struct Global {
    /// The context to use (default: QUORO_CONTEXT, then `current` in config.toml)
    #[arg(long, global = true, value_name = "NAME")]
    pub context: Option<String>,
    /// The project a command acts on, by name or id
    #[arg(long, global = true, value_name = "PROJECT")]
    #[expect(dead_code, reason = "no command in the tree targets a project yet")]
    pub project: Option<String>,
    /// The app a command acts on, by name or id
    #[arg(long, global = true, value_name = "APP")]
    #[expect(dead_code, reason = "no command in the tree targets an app yet")]
    pub app: Option<String>,
    /// Print the API's JSON body unchanged
    #[arg(long, global = true)]
    pub json: bool,
    /// Confirm a destructive command without a prompt
    #[arg(long, global = true)]
    #[expect(
        dead_code,
        reason = "no command in the tree asks for a confirmation yet"
    )]
    pub yes: bool,
    /// Why you make a change; sent as Quoro-Justification and audited
    #[arg(long, global = true, value_name = "TEXT")]
    pub reason: Option<String>,
    /// The incident a change belongs to; sent as Quoro-Incident
    #[arg(long, global = true, value_name = "INCIDENT")]
    pub incident: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Save a context and its credential: a personal API token or an agent key
    Login(LoginArgs),
    /// Forget the context's stored credential
    Logout,
    /// Show the context and the identity behind its credential
    Whoami,
    /// Call any API operation and print the response body
    Api(ApiArgs),
    /// Print a shell completion script
    Completion {
        /// The shell to complete for
        shell: Shell,
    },
}

#[derive(Debug, Args)]
pub struct ApiArgs {
    /// The HTTP method, such as GET or POST
    pub method: String,
    /// The path, starting with /api/v1/
    pub path: String,
    /// The JSON request body: @FILE, or - to read it from stdin
    #[arg(long, value_name = "@FILE|-")]
    pub data: Option<String>,
    /// When the call needs approval, file the approval request
    #[arg(long)]
    pub request_approval: bool,
    /// With --request-approval: wait for the decision and repeat the call once approved
    #[arg(long, requires = "request_approval")]
    pub wait: bool,
}

#[derive(Debug, Args)]
pub struct LoginArgs {
    /// Read the personal API token from stdin instead of a prompt
    #[arg(long, conflicts_with = "agent")]
    pub token_stdin: bool,
    /// Log in as an agent, with --key-file
    #[arg(long, requires = "key_file")]
    pub agent: bool,
    /// The file holding the agent key (mode 0600); its path is saved, never the key
    #[arg(long, value_name = "PATH", requires = "agent")]
    pub key_file: Option<PathBuf>,
    /// The primary, https://host[:port] (default: the context's)
    #[arg(long, value_name = "URL")]
    pub url: Option<String>,
    /// The SHA-256 of the primary's certificate, sha256:<hex>
    #[arg(long, value_name = "FP")]
    pub fingerprint: Option<String>,
}

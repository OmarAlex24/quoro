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
    /// The project a command acts on, by name or id (default: the folder's link)
    #[arg(long, global = true, value_name = "PROJECT")]
    pub project: Option<String>,
    /// The app a command acts on, by name or id (default: the folder's link)
    #[arg(long, global = true, value_name = "APP")]
    pub app: Option<String>,
    /// Print the API's JSON body unchanged
    #[arg(long, global = true)]
    pub json: bool,
    /// Confirm a destructive command without a prompt
    #[arg(long, global = true)]
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
    /// Link this folder to a project and, optionally, an app
    Link(LinkArgs),
    /// Remove this folder's link
    Unlink,
    /// Show the linked app or project; --cluster adds the cluster
    Status(StatusArgs),
    /// List, create and delete projects
    #[command(subcommand)]
    Project(ProjectCommand),
    /// List, create, change, start, stop and delete apps
    #[command(subcommand)]
    App(AppCommand),
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
    #[command(flatten)]
    pub approval: ApprovalArgs,
}

/// The approval flags of a destructive call (`approval.rs`).
#[derive(Debug, Args)]
pub struct ApprovalArgs {
    /// When the call needs approval, file the approval request
    #[arg(long)]
    pub request_approval: bool,
    /// With --request-approval: wait for the decision and repeat the call once approved
    #[arg(long, requires = "request_approval")]
    pub wait: bool,
}

#[derive(Debug, Args)]
pub struct LinkArgs {
    /// The project, by slug, name or id (default: --project, else pick one on a terminal)
    #[arg(value_name = "PROJECT")]
    pub project_ref: Option<String>,
    /// The app, by name or id
    #[arg(value_name = "APP")]
    pub app_ref: Option<String>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Also show the cluster, its primary and its nodes
    #[arg(long)]
    pub cluster: bool,
}

#[derive(Debug, Subcommand)]
pub enum ProjectCommand {
    /// List the projects
    List,
    /// Create a project; links this folder when it is not linked yet
    Create {
        /// The project's display name
        name: String,
        /// Its slug, a-z, 0-9 and - (default: made from the name)
        #[arg(long)]
        slug: Option<String>,
    },
    /// Delete an empty project
    Delete {
        /// The project, by slug, name or id
        #[arg(value_name = "PROJECT")]
        project_ref: String,
        #[command(flatten)]
        approval: ApprovalArgs,
    },
}

#[derive(Debug, Subcommand)]
pub enum AppCommand {
    /// List the apps of the project (all apps when none is linked)
    List,
    /// Create an app from an image or a Git repository
    Create(AppCreateArgs),
    /// Change the app's replicas, resources, port or health check path
    Update(AppUpdateArgs),
    /// Start the app's instances
    Start,
    /// Stop the app's instances
    Stop,
    /// Delete the app, its instances, domains and volumes
    Delete {
        #[command(flatten)]
        approval: ApprovalArgs,
    },
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("source").required(true).args(["image", "repo"])))]
pub struct AppCreateArgs {
    /// The app's name, a-z, 0-9 and -
    pub name: String,
    /// The OCI image to deploy
    #[arg(long, value_name = "REF")]
    pub image: Option<String>,
    /// A public https:// Git repository to build with its Dockerfile
    #[arg(long, value_name = "URL")]
    pub repo: Option<String>,
    /// The branch, tag or commit to build (default: main)
    #[arg(long = "ref", value_name = "REF", requires = "repo")]
    pub git_ref: Option<String>,
    /// The Dockerfile's path in the repository (default: Dockerfile)
    #[arg(long, value_name = "PATH", requires = "repo")]
    pub dockerfile: Option<String>,
    /// The port the app listens on
    #[arg(long, default_value_t = 8080, value_parser = clap::value_parser!(u16).range(1..))]
    pub port: u16,
}

#[derive(Debug, Args)]
pub struct AppUpdateArgs {
    /// Instances, one per node (1 to 64)
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=64))]
    pub replicas: Option<u32>,
    /// Virtual CPUs of each instance
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub vcpus: Option<u32>,
    /// Memory of each instance, in MiB
    #[arg(long, value_name = "MIB", value_parser = clap::value_parser!(u32).range(1..))]
    pub memory: Option<u32>,
    /// The port the app listens on
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    pub port: Option<u16>,
    /// The HTTP path the health check probes, starting with /
    #[arg(long, value_name = "PATH")]
    pub health_path: Option<String>,
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

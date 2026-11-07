//! `quoro ssh`: a shell in the app's running instance (ADR 0113, ADR 0114).
//!
//! It acts on the folder's linked app, or on `--app`. The open
//! (`createTerminalSession`) runs `/bin/sh` at the terminal's size and is
//! audited with its session id; the session's timed transcript stays on the
//! node (ADR 0115). The shell then runs over the session's WebSocket
//! (`session.rs`) in raw mode: every key goes to it, and Ctrl-] closes the
//! session from this side. A window change is sent as `resize`.
//!
//! `app:terminal` is human-only: an agent is refused here, with the exit code
//! of `forbidden` (`human_only`), before anything is sent, not even a mint.
//! A shell needs a terminal on stdin and stdout, so without one the command
//! is refused before anything is sent too. Running one command without a
//! shell (`ssh -- CMD`) is a later, separate decision (ADR 0114).
//!
//! The exit code is 0 when the shell exits with 0 or the user leaves, 1 when
//! it exits with another code (printed) or the primary closes the session
//! (its reason printed), and 7 when the connection drops.

use crate::Environment;
use crate::cli::Global;
use crate::error::{Failure, exit};
use crate::operations::CREATE_TERMINAL_SESSION;
use crate::scope::{Scope, text};
use crate::session::{self, Kind};
use reqwest::Method;
use serde_json::json;
use std::io::Write;

pub async fn run(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "ssh", false)?;
    if scope.is_agent {
        return Err(Failure::new(
            exit::FORBIDDEN,
            "a shell in an app is for humans only (forbidden, human_only): app:terminal cannot be \
             granted to an agent; nothing was sent",
        ));
    }
    if !(environment.stdin_is_terminal && environment.stdout_is_terminal) {
        return Err(Failure::usage(
            "quoro ssh needs a terminal on stdin and stdout; nothing was sent",
        ));
    }
    let app = scope.required_app(global).await?;
    let (rows, cols) = session::size(environment);
    let call = scope.change(
        Method::POST,
        CREATE_TERMINAL_SESSION
            .path
            .replace("{app_id}", &text(&app, "id")),
        Some(&json!({ "rows": rows, "cols": cols })),
    )?;
    let ticket = scope.send(&call).await?.json().map_err(|mut failure| {
        if failure.code == exit::CONFLICT {
            failure.message = format!(
                "{}; quoro status shows the app's instances, and an instance created before the \
                 terminal existed gets one at the next deployment: quoro redeploy",
                failure.message
            );
        }
        failure
    })?;
    let _ = write!(
        stderr,
        "quoro: a shell in {} ({}); Ctrl-] closes it\r\n",
        text(&app, "name"),
        text(&ticket, "vm_id")
    );
    session::attach(
        &scope.session.context,
        &ticket,
        Kind::Terminal,
        environment,
        stdout,
        stderr,
    )
    .await
}

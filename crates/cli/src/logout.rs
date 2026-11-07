//! `quoro logout [--revoke]`: forgets the context's stored credential.
//!
//! Removes the context's entry from `credentials.toml` (a human's token, or
//! an agent's key file path) and the agent's cached access token. The context
//! itself (URL and pin in `config.toml`) stays, so the next `quoro login`
//! needs no `--url`. The context is `--context`, else `QUORO_CONTEXT`, else
//! the current one; `QUORO_TOKEN` and `QUORO_AGENT_KEY_FILE` are not stored,
//! so logging out does not touch them.
//!
//! `--revoke` is refused, and nothing is removed:
//!
//! - for a token, because the client cannot tell which token it is. The
//!   token list (`listTokens`) never returns a secret and `getMe` does not
//!   name the token in use, so the only id the client could send would be a
//!   guess. The human revokes it in the panel (Your account → API tokens) and
//!   then logs out.
//! - for an agent key, because revoking a key is a human's decision, taken in
//!   the panel.

use crate::Environment;
use crate::agent;
use crate::cli::{Global, LogoutArgs};
use crate::context::{self, Context, Trust};
use crate::error::Failure;
use crate::login::TOKEN_HOME;
use std::io::Write;

pub fn run(
    global: &Global,
    args: &LogoutArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let directory = context::config_dir(environment)?;
    let (config_path, config) = context::read_config(&directory)?;
    let Some(name) = global
        .context
        .clone()
        .or_else(|| environment.var("QUORO_CONTEXT").map(str::to_string))
        .or_else(|| config.current.clone())
    else {
        return Err(Failure::usage(format!(
            "no context is selected: pass --context NAME or set `current` in {}",
            config_path.display()
        )));
    };
    let url = config.contexts.get(&name).map(|entry| entry.url.clone());
    let credentials_path = directory.join("credentials.toml");
    let mut credentials = context::read_credentials(&credentials_path)?.unwrap_or_default();
    let stored = credentials.contexts.get(&name);
    let has_token = stored.is_some_and(|entry| entry.token.is_some());
    let has_key = stored.is_some_and(|entry| entry.key_file.is_some());

    if args.revoke && has_token {
        let panel = url
            .as_deref()
            .map_or_else(String::new, |url| format!(" ({url}/account)"));
        return Err(Failure::usage(format!(
            "--revoke cannot tell which of your tokens this is: the API never shows a token's \
             secret and getMe does not name the token in use. Revoke it in the panel: \
             {TOKEN_HOME}{panel}, then run quoro logout. Nothing was removed"
        )));
    }
    if args.revoke && has_key {
        return Err(Failure::usage(
            "an agent cannot revoke its key: a human revokes it in the panel (Agents). \
             quoro logout without --revoke forgets the key file path. Nothing was removed",
        ));
    }

    // The cached access token goes too, whatever the credential was.
    if let Some(cache) = agent::cache_path(
        environment,
        &Context {
            name: Some(name.clone()),
            url: url.clone().unwrap_or_default(),
            trust: Trust::System,
            credential: None,
        },
    ) {
        let _ = std::fs::remove_file(cache);
    }

    let mut message = if credentials.contexts.remove(&name).is_some() && (has_token || has_key) {
        context::write_credentials(&directory, &credentials)?;
        let what = if has_token {
            "its token"
        } else {
            "its agent key file path"
        };
        format!(
            "Logged out of {name}: {what} was removed from {}.\n",
            credentials_path.display()
        )
    } else {
        format!("No credential is stored for {name}; nothing to remove.\n")
    };
    for variable in ["QUORO_TOKEN", "QUORO_AGENT_KEY_FILE"] {
        if environment.var(variable).is_some() {
            message.push_str(&format!(
                "{variable} is set in this shell and still overrides the stored credential.\n"
            ));
        }
    }
    crate::api::write(stdout, message.as_bytes())
}

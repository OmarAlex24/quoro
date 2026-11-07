//! `quoro domain list | add HOST [--tls acme|tunnel] [--provider P] | remove
//! HOST`.
//!
//! Each acts on the folder's linked app, or on `--app` (in `--project` or the
//! linked project).
//!
//! - `add` routes a hostname to the app (`createAppDomain`) with a fresh
//!   `Idempotency-Key`. TLS comes from ACME (the default) or a Cloudflare
//!   Tunnel (ADR 0024). `--provider` names the DNS or tunnel provider that
//!   manages the hostname's records, by id (`prv_...`) or by name; without
//!   it the primary uses the only provider of that kind. Reading providers
//!   by name needs `provider:manage`, which is human-only, so an agent names
//!   one by its id.
//! - `remove` stops routing a hostname. The contract deletes a domain by its
//!   id (`deleteDomain`), so the client finds the id among the app's domains
//!   first. A human types the hostname on a terminal or passes `--yes`.

use crate::Environment;
use crate::cli::{DomainCommand, Global};
use crate::client::idempotency_key;
use crate::error::{Failure, exit};
use crate::operations::{CREATE_APP_DOMAIN, DELETE_DOMAIN, LIST_APP_DOMAINS, LIST_PROVIDERS};
use crate::scope::{self, Confirm, Scope, is_id, text};
use reqwest::Method;
use serde_json::{Value, json};
use std::io::Write;

pub async fn run(
    global: &Global,
    command: DomainCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        DomainCommand::List => list(global, environment, stdout).await,
        DomainCommand::Add {
            host,
            tls,
            provider,
        } => {
            add(
                global,
                environment,
                &host,
                &tls,
                provider.as_deref(),
                stdout,
            )
            .await
        }
        DomainCommand::Remove { host } => remove(global, environment, &host, stdout, stderr).await,
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "domain list", false)?;
    let app = scope.required_app(global).await?;
    let domains = domains(&mut scope, &app).await?;
    if global.json {
        let body = json!({ "items": domains, "next_cursor": null });
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    if domains.is_empty() {
        let line = format!(
            "The app {} has no domains; add one with quoro domain add HOST.\n",
            text(&app, "name")
        );
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        ["HOSTNAME", "TLS", "CERTIFICATE", "SERVING", "ID"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    ];
    let mut errors = String::new();
    for domain in &domains {
        rows.push(vec![
            text(domain, "hostname"),
            text(domain, "tls_mode"),
            text(domain, "cert_status"),
            text(domain, "serving_status"),
            text(domain, "id"),
        ]);
        if let Some(error) = domain["last_error"].as_str() {
            errors.push_str(&format!("{}: {error}\n", text(domain, "hostname")));
        }
    }
    let mut out = scope::table(&rows);
    out.push_str(&errors);
    crate::api::write(stdout, out.as_bytes())
}

async fn add(
    global: &Global,
    environment: &Environment,
    host: &str,
    tls: &str,
    provider: Option<&str>,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if !valid_hostname(&host) {
        return Err(Failure::usage(format!(
            "'{host}' is not a hostname: a DNS name such as app.example.com, at most 253 characters"
        )));
    }
    let mut scope = Scope::open(global, environment, "domain add", true)?;
    let mut body = json!({ "hostname": host, "tls_mode": tls });
    if let Some(provider) = provider {
        body["provider_id"] = json!(provider_id(&mut scope, provider, tls).await?);
    }
    let app = scope.required_app(global).await?;
    let name = text(&app, "name");
    let mut call = scope.change(
        Method::POST,
        CREATE_APP_DOMAIN
            .path
            .replace("{app_id}", &text(&app, "id")),
        Some(&body),
    )?;
    call.headers.insert("idempotency-key", idempotency_key());
    let answer = scope.send(&call).await?;
    scope::report(&answer, global.json, stdout, |domain| {
        let id = text(domain, "id");
        let tls = match text(domain, "tls_mode").as_str() {
            "tunnel" => "TLS comes from the tunnel".to_string(),
            _ => format!(
                "its certificate is {}; ACME issues it",
                text(domain, "cert_status")
            ),
        };
        format!(
            "Routing {host} to the app {name} ({id}): {tls}. quoro domain list shows its state.\n"
        )
    })
    .map_err(|mut failure| {
        if failure.code == exit::CONFLICT {
            failure.message = format!("{}; a hostname routes to one app", failure.message);
        }
        failure
    })
}

/// The provider `reference` names for a domain of `tls` mode: an id as it
/// is, or the provider of that name and of the mode's kind.
async fn provider_id(scope: &mut Scope, reference: &str, tls: &str) -> Result<String, Failure> {
    if is_id("prv_", reference) {
        return Ok(reference.to_string());
    }
    if scope.is_agent {
        return Err(Failure::usage(format!(
            "an agent names the provider by its id (prv_...), not '{reference}': reading \
             providers is human-only; nothing was sent"
        )));
    }
    let kind = if tls == "tunnel" { "tunnel" } else { "dns" };
    let providers = scope.list_all(LIST_PROVIDERS.path).await?;
    let named: Vec<&Value> = providers
        .iter()
        .filter(|p| p["name"] == reference && p["kind"] == kind)
        .collect();
    match named.as_slice() {
        [provider] => Ok(text(provider, "id")),
        [] => Err(Failure::new(
            exit::NOT_FOUND,
            format!(
                "no {kind} provider has the id or name '{reference}'; the panel's Settings lists them"
            ),
        )),
        several => Err(Failure::usage(format!(
            "the name '{reference}' matches {} {kind} providers; name one by its id: {}",
            several.len(),
            several
                .iter()
                .map(|p| text(p, "id"))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

async fn remove(
    global: &Global,
    environment: &Environment,
    host: &str,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    let mut scope = Scope::open(global, environment, "domain remove", true)?;
    scope::can_confirm_change(global, environment, scope.is_agent, "removing a domain")?;
    let app = scope.required_app(global).await?;
    let name = text(&app, "name");
    let domain = domains(&mut scope, &app)
        .await?
        .into_iter()
        .find(|domain| domain["hostname"] == host.as_str())
        .ok_or_else(|| {
            Failure::new(
                exit::NOT_FOUND,
                format!("the app {name} has no domain {host}; quoro domain list shows its domains"),
            )
        })?;
    let id = text(&domain, "id");
    scope::confirm(
        global,
        environment,
        scope.is_agent,
        &Confirm {
            change: "removing a domain",
            warning: &format!("This stops serving {host} for the app {name}"),
            label: "the hostname",
            expected: &host,
            nothing: "nothing was removed",
        },
        stderr,
    )?;
    let call = scope.change(
        Method::DELETE,
        DELETE_DOMAIN.path.replace("{domain_id}", &id),
        None,
    )?;
    let answer = scope.send(&call).await?;
    if global.json {
        // `deleteDomain` answers 204 with no body; the removed domain stands
        // for it.
        if answer.is_success() {
            return crate::api::write(stdout, format!("{domain}\n").as_bytes());
        }
        return scope::report(&answer, true, stdout, |_| String::new());
    }
    scope::report(&answer, false, stdout, |_| {
        format!("Removed {host} ({id}) from the app {name}; it is no longer served.\n")
    })
}

/// Every domain of `app`.
async fn domains(scope: &mut Scope, app: &Value) -> Result<Vec<Value>, Failure> {
    scope
        .list_all(&LIST_APP_DOMAINS.path.replace("{app_id}", &text(app, "id")))
        .await
}

/// A lowercase DNS name of 1 to 253 characters, labels of 1 to 63 of a-z,
/// 0-9 and `-`, not starting or ending with `-`; what the primary accepts.
fn valid_hostname(host: &str) -> bool {
    (1..=253).contains(&host.len())
        && host.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostnames_are_dns_names() {
        for good in ["app.example.com", "a", "x-1.example.io"] {
            assert!(valid_hostname(good), "{good}");
        }
        for bad in [
            "",
            "-a.example.com",
            "a..b",
            "a_b.com",
            "App.com",
            &"a".repeat(64),
        ] {
            assert!(!valid_hostname(bad), "{bad}");
        }
    }
}

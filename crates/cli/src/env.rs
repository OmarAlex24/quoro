//! `quoro env list | replace --file FILE`.
//!
//! Both act on the folder's linked app, or on `--app` (in `--project` or the
//! linked project).
//!
//! The contract never returns a value (`getAppEnv`, `AppEnvVar`): only the
//! names and when each changed. The client holds to that on its side too:
//! every answer it prints, `--json` included, keeps only `app_id`, `name` and
//! `updated_at` of each variable ([`without_values`]), and a value read from
//! a file is never printed, not even in an error. Setting one variable needs
//! an operation the contract does not have yet; `replace` sends the whole
//! environment (`replaceAppEnv`), and every variable the file does not list
//! is removed.
//!
//! The file is dotenv-style ([`parse`]):
//!
//! - one `NAME=VALUE` per line, optionally after `export `; blank lines and
//!   lines starting with `#` are skipped;
//! - `NAME` matches `[A-Za-z_][A-Za-z0-9_]*`, appears once, and is not one of
//!   the names the node sets (`QUORO_INSTANCE_ID`, `QUORO_NODE_ID`);
//! - `VALUE` is the rest of the line with the spaces around it trimmed (a `#`
//!   in it is part of the value), or a quoted string: `'...'` as written, or
//!   `"..."` with `\n`, `\t`, `\"` and `\\` escapes. A value spans one line;
//! - at most 200 variables, each value at most 32 KiB (the primary's limits).
//!
//! The file is read and checked before anything is sent; a bad line is named
//! by its number, never by its text. `--file -` reads it from stdin.
//!
//! A replace cannot be undone: the values it overwrites or removes cannot be
//! read back. So it prints the names it removes, adds and sets again, then a
//! human types the app's name on a terminal, or passes `--yes`; without either
//! it exits 2 before anything is sent. With `--file -` stdin holds the file,
//! so `--yes` is needed. An agent is not asked, and like every change it
//! needs `--reason`.

use crate::Environment;
use crate::cli::{EnvCommand, Global};
use crate::error::Failure;
use crate::operations::{GET_APP_ENV, REPLACE_APP_ENV};
use crate::scope::{self, Confirm, Scope, text};
use reqwest::Method;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;

/// The names the node sets in every instance (ADR 0142).
const RESERVED: [&str; 2] = ["QUORO_INSTANCE_ID", "QUORO_NODE_ID"];
/// The primary's limits on a replacement.
const MAX_VARIABLES: usize = 200;
const MAX_VALUE_BYTES: usize = 32 * 1024;
/// The fields of a variable that may be printed.
const PRINTED: [&str; 3] = ["app_id", "name", "updated_at"];

pub async fn run(
    global: &Global,
    command: EnvCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        EnvCommand::List => list(global, environment, stdout).await,
        EnvCommand::Replace { file } => replace(global, environment, &file, stdout, stderr).await,
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "env list", false)?;
    let app = scope.required_app(global).await?;
    let body = scope
        .get(&GET_APP_ENV.path.replace("{app_id}", &text(&app, "id")))
        .await?;
    let body = without_values(body);
    if global.json {
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    let items = body["items"].as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        let line = format!(
            "The app {} has no environment variables; set them with quoro env replace --file FILE.\n",
            text(&app, "name")
        );
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![vec!["NAME".to_string(), "UPDATED".to_string()]];
    for item in &items {
        rows.push(vec![text(item, "name"), text(item, "updated_at")]);
    }
    crate::api::write(stdout, scope::table(&rows).as_bytes())
}

async fn replace(
    global: &Global,
    environment: &Environment,
    file: &Path,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let from_stdin = file.as_os_str() == "-";
    let mut scope = Scope::open(global, environment, "env replace", true)?;
    if from_stdin && !global.yes && !scope.is_agent {
        return Err(Failure::usage(
            "--file - reads the variables from stdin, so the replace cannot be confirmed \
             there: pass --yes; nothing was sent",
        ));
    }
    scope::can_confirm_change(
        global,
        environment,
        scope.is_agent,
        "replacing an app's environment",
    )?;
    let bytes = if from_stdin {
        environment
            .read_to_end()
            .map_err(|e| Failure::usage(format!("cannot read stdin: {e}")))?
    } else {
        std::fs::read(file)
            .map_err(|e| Failure::usage(format!("cannot read {}: {e}", file.display())))?
    };
    let shown = if from_stdin {
        "stdin".to_string()
    } else {
        file.display().to_string()
    };
    let variables = parse(&bytes)
        .map_err(|message| Failure::usage(format!("{shown}: {message}; nothing was sent")))?;

    let app = scope.required_app(global).await?;
    let (name, id) = (text(&app, "name"), text(&app, "id"));
    let current = scope
        .get(&GET_APP_ENV.path.replace("{app_id}", &id))
        .await?;
    let before: BTreeSet<String> = current["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| text(item, "name"))
        .collect();
    let after: BTreeSet<String> = variables.iter().map(|(n, _)| n.clone()).collect();
    let plan = Plan {
        removed: before.difference(&after).cloned().collect(),
        added: after.difference(&before).cloned().collect(),
        kept: after.intersection(&before).cloned().collect(),
    };
    let _ = write!(stderr, "{}", plan.describe(&name));
    scope::confirm(
        global,
        environment,
        scope.is_agent,
        &Confirm {
            change: "replacing an app's environment",
            warning: &format!(
                "This replaces the environment of the app {name}; the values it overwrites \
                 or removes cannot be read back"
            ),
            label: "the app's name",
            expected: &name,
            nothing: "nothing was changed",
        },
        stderr,
    )?;

    let items: Vec<Value> = variables
        .iter()
        .map(|(name, value)| json!({ "name": name, "value": value }))
        .collect();
    let call = scope.change(
        Method::PUT,
        REPLACE_APP_ENV.path.replace("{app_id}", &id),
        Some(&json!({ "items": items })),
    )?;
    let answer = scope.send(&call).await?;
    if !answer.is_success() {
        // A problem never carries a value; it is printed as any other.
        return scope::report(&answer, global.json, stdout, |_| String::new());
    }
    let body = without_values(answer.json()?);
    if global.json {
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    let count = body["items"].as_array().map_or(0, Vec::len);
    crate::api::write(
        stdout,
        format!(
            "Replaced the environment of the app {name}: {count} variable{}. Running \
             instances keep the old one until the app is deployed again: quoro redeploy.\n",
            if count == 1 { "" } else { "s" }
        )
        .as_bytes(),
    )
}

/// What a replace does to the names.
struct Plan {
    removed: Vec<String>,
    added: Vec<String>,
    kept: Vec<String>,
}

impl Plan {
    fn describe(&self, app: &str) -> String {
        let mut out = format!("The app {app}'s environment:\n");
        for (label, names) in [
            ("removes", &self.removed),
            ("adds", &self.added),
            ("sets again", &self.kept),
        ] {
            if !names.is_empty() {
                out.push_str(&format!("  {label}: {}\n", names.join(", ")));
            }
        }
        if self.removed.is_empty() && self.added.is_empty() && self.kept.is_empty() {
            out.push_str("  removes nothing and adds nothing: it stays empty\n");
        }
        out
    }
}

/// An env answer with only the printable fields of each variable. The
/// contract returns no value, so this changes nothing on a primary that
/// keeps to it; it stands so a value can never reach the output.
fn without_values(mut body: Value) -> Value {
    let items = body["items"].as_array().cloned().unwrap_or_default();
    let items: Vec<Value> = items
        .into_iter()
        .map(|item| {
            let kept: Map<String, Value> = PRINTED
                .iter()
                .filter_map(|key| Some(((*key).to_string(), item.get(*key)?.clone())))
                .collect();
            Value::Object(kept)
        })
        .collect();
    match body.as_object_mut() {
        Some(object) => {
            object.retain(|key, _| key != "items" && !key.contains("value"));
            object.insert("items".to_string(), Value::Array(items));
            body
        }
        None => json!({ "items": items }),
    }
}

/// The variables of a dotenv-style file, in its order (see the module
/// documentation). An error names the line by number, never its text.
fn parse(bytes: &[u8]) -> Result<Vec<(String, String)>, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "the file is not UTF-8".to_string())?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut variables: Vec<(String, String)> = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, line) in text.lines().enumerate() {
        let number = index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").map_or(line, str::trim_start);
        let Some((name, value)) = line.split_once('=') else {
            return Err(format!("line {number} is not NAME=VALUE"));
        };
        let name = name.trim();
        if !valid_name(name) {
            return Err(format!(
                "line {number}: the name must match [A-Za-z_][A-Za-z0-9_]*"
            ));
        }
        if RESERVED.contains(&name) {
            return Err(format!(
                "line {number}: {name} is set by the node in every instance and cannot be set"
            ));
        }
        if !seen.insert(name.to_string()) {
            return Err(format!("line {number}: {name} is set twice"));
        }
        let value = unquote(value.trim())
            .map_err(|problem| format!("line {number}: the value of {name} {problem}"))?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(format!("line {number}: the value of {name} is over 32 KiB"));
        }
        variables.push((name.to_string(), value));
    }
    if variables.len() > MAX_VARIABLES {
        return Err(format!(
            "{} variables; an app may have at most {MAX_VARIABLES}",
            variables.len()
        ));
    }
    Ok(variables)
}

fn valid_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b) if b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// A value as written: `'...'` literally, `"..."` with its escapes, anything
/// else as it is.
fn unquote(value: &str) -> Result<String, &'static str> {
    if let Some(rest) = value.strip_prefix('\'') {
        return rest
            .strip_suffix('\'')
            .filter(|inner| !inner.contains('\''))
            .map(str::to_string)
            .ok_or("has an unclosed or stray ' quote");
    }
    let Some(rest) = value.strip_prefix('"') else {
        return Ok(value.to_string());
    };
    let mut out = String::new();
    let mut chars = rest.chars();
    loop {
        match chars.next() {
            None => return Err("has an unclosed \" quote"),
            Some('"') => {
                return if chars.as_str().is_empty() {
                    Ok(out)
                } else {
                    Err("goes on after its closing \" quote")
                };
            }
            Some('\\') => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                _ => return Err("has an unknown \\ escape (\\n, \\t, \\\" and \\\\ are known)"),
            },
            Some(c) => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dotenv_file_reads_as_names_and_values() {
        let file = "# a comment\n\nexport A=1\nB = two words # kept\nC='it''s'\nD=\"x\\ny \\\"q\\\"\"\nE=\n_F9='a b'\n";
        let parsed = parse(file.as_bytes());
        assert!(parsed.is_err(), "C has a stray quote");
        let file = "# a comment\n\nexport A=1\nB = two words # kept\nD=\"x\\ny \\\"q\\\"\"\nE=\n_F9='a b'\r\n";
        assert_eq!(
            parse(file.as_bytes()).unwrap(),
            vec![
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "two words # kept".to_string()),
                ("D".to_string(), "x\ny \"q\"".to_string()),
                ("E".to_string(), String::new()),
                ("_F9".to_string(), "a b".to_string()),
            ]
        );
        assert_eq!(parse(b"").unwrap(), vec![]);
    }

    #[test]
    fn a_bad_line_is_named_by_its_number_and_never_shows_a_value() {
        let secret = "s3cr3t-value";
        for (file, expected) in [
            (format!("A=1\n{secret}\n"), "line 2 is not NAME=VALUE"),
            (format!("9A={secret}\n"), "line 1: the name must match"),
            (format!("A-B={secret}\n"), "line 1: the name must match"),
            (format!("A=1\nA={secret}\n"), "line 2: A is set twice"),
            (
                format!("QUORO_NODE_ID={secret}\n"),
                "line 1: QUORO_NODE_ID is set by the node",
            ),
            (
                format!("A=\"{secret}\n"),
                "line 1: the value of A has an unclosed",
            ),
            (
                format!("A=\"{secret}\" x\n"),
                "line 1: the value of A goes on",
            ),
        ] {
            let error = parse(file.as_bytes()).unwrap_err();
            assert!(error.starts_with(expected), "{error}");
            assert!(!error.contains(secret), "{error}");
        }
        assert!(parse(&[0xff, b'=']).unwrap_err().contains("UTF-8"));
        let many: String = (0..201).map(|i| format!("V{i}=x\n")).collect();
        assert!(parse(many.as_bytes()).unwrap_err().contains("at most 200"));
        let big = format!("A={}\n", "x".repeat(MAX_VALUE_BYTES + 1));
        assert!(parse(big.as_bytes()).unwrap_err().contains("over 32 KiB"));
    }

    #[test]
    fn a_value_never_survives_the_printed_answer() {
        let body = json!({
            "items": [
                { "app_id": "app_1", "name": "A", "updated_at": "t", "value": "secret" },
                { "name": "B", "updated_at": "t", "secret_value": "secret" },
            ],
            "values": ["secret"],
        });
        let printed = without_values(body).to_string();
        assert!(!printed.contains("secret"), "{printed}");
        assert_eq!(
            printed,
            r#"{"items":[{"app_id":"app_1","name":"A","updated_at":"t"},{"name":"B","updated_at":"t"}]}"#
        );
    }
}

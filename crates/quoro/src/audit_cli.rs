//! `quoro audit verify`: offline verification of the hash chain
//! (`audit-log-format.md` section 6.1).
//!
//! Two sources, both local and with no server involved: an exported JSON Lines
//! segment (`--file`) and a control-plane database opened read-only (`--db`).
//! The same pure verifier the HTTP service uses walks either one.

use quoro_control_plane::audit::{self, Verification};
use quoro_control_plane::store::GENESIS_HASH;
use serde_json::Value;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use std::path::Path;
use std::process::ExitCode;

/// `quoro audit verify (--file PATH | --db PATH) [--prev-hash HEX]`.
///
/// # Errors
///
/// Nothing: a bad command line, an unreadable file or database, and a broken
/// chain are all reported through the exit code and stderr.
pub async fn run(args: &[String]) -> ExitCode {
    let Some((subcommand, rest)) = args.split_first() else {
        return crate::usage_error("audit needs a subcommand: verify");
    };
    if subcommand != "verify" {
        return crate::usage_error(&format!("unknown audit subcommand '{subcommand}'"));
    }
    let flags = match crate::parse_flags(rest, &["file", "db", "prev-hash"]) {
        Ok(flags) => flags,
        Err(error) => return crate::usage_error(&error),
    };
    let file = crate::flag(&flags, "file");
    let database = crate::flag(&flags, "db");
    match (file, database) {
        (Some(_), Some(_)) => crate::usage_error("audit verify takes --file or --db, not both"),
        (None, None) => crate::usage_error("audit verify needs --file or --db"),
        (Some(path), None) => verify_file(Path::new(path), crate::flag(&flags, "prev-hash")),
        (None, Some(path)) => verify_database(Path::new(path)).await,
    }
}

/// Prints the contract's single result line and maps the outcome to the exit
/// code: `0` when the chain verified, `1` when it did not.
fn report(verification: &Verification) -> ExitCode {
    if verification.ok {
        println!(
            "ok: checked {} records, head {}",
            verification.checked, verification.head_hash
        );
        ExitCode::SUCCESS
    } else {
        println!(
            "broken at seq {}: {}",
            verification.broken_at_seq.unwrap_or_default(),
            verification.reason.unwrap_or("unknown")
        );
        ExitCode::from(1)
    }
}

fn read_error(path: &Path, error: &dyn std::fmt::Display) -> ExitCode {
    eprintln!(
        "quoro audit verify: cannot read {}: {error}",
        path.display()
    );
    ExitCode::from(2)
}

/// JSON Lines, one complete record per line, in `seq` order (section 6.1).
fn verify_file(path: &Path, prev_hash: Option<&str>) -> ExitCode {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => return read_error(path, &error),
    };
    let mut records = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(record) => records.push(record),
            Err(error) => {
                return read_error(path, &format!("line {}: {error}", number + 1));
            }
        }
    }
    let first_seq = records
        .first()
        .and_then(|record| record.get("seq"))
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let seed = prev_hash.unwrap_or(GENESIS_HASH);
    report(&audit::verify_records(records, first_seq, seed))
}

/// The whole `audit_log` of a database file, opened read-only: no migration
/// runs and nothing is written (section 6.1).
async fn verify_database(path: &Path) -> ExitCode {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = match SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
    {
        Ok(pool) => pool,
        Err(error) => return read_error(path, &error),
    };
    let rows = match sqlx::query(&format!(
        "SELECT {} FROM audit_log ORDER BY seq",
        audit::COLUMNS
    ))
    .fetch_all(&pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => return read_error(path, &error),
    };
    let records = match rows
        .iter()
        .map(audit::record_value_from_row)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(records) => records,
        Err(error) => return read_error(path, &error),
    };
    let first_seq = records
        .first()
        .and_then(|record| record.get("seq"))
        .and_then(Value::as_i64)
        .unwrap_or(1);
    report(&audit::verify_records(records, first_seq, GENESIS_HASH))
}

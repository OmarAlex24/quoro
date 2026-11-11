// SPDX-License-Identifier: AGPL-3.0-only
//! `quoro vuln import <dir>`: feeding the vulnerability data offline.
//!
//! The command runs on the primary's host, as the owner of its data directory.
//! It opens the control-plane database without migrating it, refuses when the
//! server still has a migration to apply, imports every `<Bucket>/all.zip` and
//! the KEV catalog the directory holds into the cache, then records the
//! `osv_mirror` rows and one `vulnerability_data.import` audit record in one
//! transaction.
//!
//! The work is [`import`], which takes a data directory and a directory of
//! data; [`run`] only parses the command line, loads the configuration and
//! drops privileges.

use quoro_control_plane::audit::{self, Channel, Outcome, Record};
use quoro_control_plane::changelog::Actor;
use quoro_control_plane::store::Store;
use quoro_control_plane::vulns::{Cache, import_dir};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::SqliteConnection;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// The audit actor id of the vulnerability data subsystem.
const ACTOR: &str = "vulnerability_data";
/// The audit operation of an import run.
const OPERATION: &str = "vulnerability_data.import";

/// What one import brought in: bucket (or `KEV`), record count, data age.
pub type Imported = Vec<(String, u64, Option<String>)>;

/// `quoro vuln <subcommand>`.
pub async fn run(args: &[String]) -> ExitCode {
    match args.first().map(String::as_str) {
        Some("import") => import_command(&args[1..]).await,
        Some(other) => {
            eprintln!("quoro vuln: unknown subcommand '{other}'");
            usage()
        }
        None => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: quoro vuln import <dir> [--config PATH]");
    ExitCode::from(2)
}

async fn import_command(args: &[String]) -> ExitCode {
    let (dir, flags) = match parse_import_args(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("quoro vuln import: {message}");
            return usage();
        }
    };
    let path = crate::config_path(&flags);
    let config = match crate::config::Config::load(&path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("quoro vuln import: cannot read {}: {error}", path.display());
            return ExitCode::from(1);
        }
    };
    // On a host where the control plane has its own user, running as root
    // here would leave root-owned cache and SQLite side files behind.
    if let Some(dropped) = crate::privilege::run_as_data_dir_owner(&config.data_dir) {
        return match dropped {
            Ok(code) => crate::exit_code(code),
            Err(error) => {
                eprintln!(
                    "quoro vuln import: cannot run as the owner of {}: {error}",
                    config.data_dir.display()
                );
                ExitCode::from(1)
            }
        };
    }
    match import(&config.data_dir, &dir).await {
        Ok(imported) => {
            for (name, count, modified) in imported {
                match modified {
                    Some(modified) => {
                        println!("{name}: {count} records, data from {modified}");
                    }
                    None => println!("{name}: {count} records, data from unknown"),
                }
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("quoro vuln import: {error}");
            ExitCode::from(1)
        }
    }
}

/// Reads `import <dir> [--config PATH]`.
///
/// Unlike the control plane's `--flag value` parser, one positional argument
/// is allowed here: the directory to import.
fn parse_import_args(args: &[String]) -> Result<(PathBuf, Vec<(String, String)>), String> {
    let mut dir: Option<PathBuf> = None;
    let mut flags = Vec::new();
    let mut rest = args.iter();
    while let Some(argument) = rest.next() {
        if let Some(name) = argument.strip_prefix("--") {
            if name != "config" {
                return Err(format!("unknown option '{argument}'"));
            }
            let Some(value) = rest.next() else {
                return Err(format!("'{argument}' needs a value"));
            };
            flags.push((name.to_string(), value.clone()));
            continue;
        }
        if dir.is_some() {
            return Err(format!("unexpected argument '{argument}'"));
        }
        dir = Some(PathBuf::from(argument));
    }
    let dir = dir.ok_or("import needs the directory to read")?;
    Ok((dir, flags))
}

/// The import itself: open the database without migrating, import into the
/// cache, then record the rows and one audit record.
///
/// # Errors
///
/// The database or cache cannot be opened, the database still needs the server
/// to migrate it, the directory holds nothing usable, or the rows cannot be
/// written. A failure after the database opened also appends an `error` audit
/// record, best effort.
pub async fn import(data_dir: &Path, dir: &Path) -> Result<Imported, String> {
    let database = data_dir.join("quoro.db");
    // Opening would create an empty database, so a mistyped data directory
    // would leave one behind that no server can use.
    if !database.is_file() {
        return Err(format!(
            "there is no control-plane database at {}",
            database.display()
        ));
    }
    // A demoted primary writes nothing more, not even the error audit record
    // (`node-agent-protocol.md` section 7.4).
    if let Some(term) = crate::serve::demoted_by_term(&database).await? {
        return Err(format!(
            "this host is a demoted primary at term {term}; it imports nothing"
        ));
    }
    let store = Store::open_unmigrated(&database)
        .await
        .map_err(|error| format!("cannot open {}: {error}", database.display()))?;
    match store.pending_migrations().await {
        Ok(None) => {}
        Ok(Some(_)) => {
            let reason = "the database needs the server to migrate it first".to_string();
            record_error(&store, &empty_params(), &reason).await;
            return Err(reason);
        }
        Err(error) => {
            let reason = format!("cannot read {}: {error}", database.display());
            record_error(&store, &empty_params(), &reason).await;
            return Err(reason);
        }
    }
    let (cache, _) = Cache::open(&data_dir.join("vulns-cache.db"))
        .await
        .map_err(|error| format!("cannot open the vulnerability cache: {error}"))?;
    let imported = match import_dir(&cache, dir).await {
        Ok(imported) => imported,
        Err(reason) => {
            record_error(&store, &empty_params(), &reason).await;
            return Err(reason);
        }
    };
    let params = params(&imported);
    if let Err(reason) = record_success(&store, &imported, &params).await {
        record_error(&store, &params, &reason).await;
        return Err(reason);
    }
    Ok(imported)
}

/// The audit params of a run: the names imported and their record counts.
fn params(imported: &Imported) -> Value {
    let ecosystems: Vec<&str> = imported.iter().map(|(name, _, _)| name.as_str()).collect();
    let mut counts = serde_json::Map::new();
    for (name, count, _) in imported {
        counts.insert(name.clone(), json!(count));
    }
    json!({ "ecosystems": ecosystems, "record_counts": Value::Object(counts) })
}

fn empty_params() -> Value {
    json!({ "ecosystems": [], "record_counts": {} })
}

/// Writes the `osv_mirror` rows and the `ok` audit record in one transaction.
async fn record_success(store: &Store, imported: &Imported, params: &Value) -> Result<(), String> {
    let mut write = store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|error| format!("cannot write osv_mirror: {error}"))?;
    let stamp = quoro_control_plane::store::now_rfc3339();
    for (name, count, modified) in imported {
        sqlx::query(
            "INSERT INTO osv_mirror (ecosystem, source, record_count, data_modified_at, \
             refreshed_at, checked_at, last_error) VALUES (?, 'import', ?, ?, ?, ?, NULL) \
             ON CONFLICT(ecosystem) DO UPDATE SET source = excluded.source, \
             record_count = excluded.record_count, data_modified_at = excluded.data_modified_at, \
             refreshed_at = excluded.refreshed_at, checked_at = excluded.checked_at, \
             last_error = NULL",
        )
        .bind(name)
        .bind(i64::try_from(*count).unwrap_or(i64::MAX))
        .bind(modified)
        .bind(&stamp)
        .bind(&stamp)
        .execute(&mut *write)
        .await
        .map_err(|error| format!("cannot write osv_mirror: {error}"))?;
    }
    let cluster_id = cluster_id(&mut write).await.ok();
    let record = audit_record(cluster_id, params, Outcome::Ok, None);
    audit::append(&mut write, &record)
        .await
        .map_err(|error| format!("cannot write the audit log: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("cannot write osv_mirror: {error}"))
}

/// Appends the `error` record of a failed run in its own transaction.
///
/// This is best effort: the run already failed, and losing the record must not
/// turn a clear error message into a second, unrelated one.
async fn record_error(store: &Store, params: &Value, reason: &str) {
    let result: Result<(), String> = async {
        let mut write = store
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| error.to_string())?;
        let cluster_id = cluster_id(&mut write).await.ok();
        let record = audit_record(cluster_id, params, Outcome::Error, Some(reason.to_string()));
        audit::append(&mut write, &record)
            .await
            .map_err(|error| error.to_string())?;
        write.commit().await.map_err(|error| error.to_string())
    }
    .await;
    if let Err(error) = result {
        eprintln!("quoro vuln import: cannot write the audit log: {error}");
    }
}

/// Builds the one audit record of a run.
fn audit_record(
    cluster_id: Option<String>,
    params: &Value,
    result: Outcome,
    result_detail: Option<String>,
) -> Record {
    Record {
        actor: Actor::system(ACTOR),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::Cli,
        operation: OPERATION.to_string(),
        scope: None,
        target_type: Some("cluster".to_string()),
        target_id: cluster_id,
        project_id: None,
        params: params.clone(),
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: new_id(Prefix::Request),
        result,
        result_detail,
        change_seq: None,
    }
}

/// The cluster id this database belongs to, never inserted here.
async fn cluster_id(conn: &mut SqliteConnection) -> Result<String, String> {
    let row = sqlx::query_as::<_, (String,)>("SELECT value FROM meta WHERE key = 'cluster_id'")
        .fetch_optional(conn)
        .await
        .map_err(|error| format!("cannot read the cluster id: {error}"))?;
    row.map(|(value,)| value)
        .ok_or_else(|| "the database has no cluster id".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEV_BODY: &str = r#"{
        "dateReleased": "2026-11-04T20:00:05.774Z",
        "vulnerabilities": [
            {"cveID": "CVE-2024-27834", "dateAdded": "2026-11-04"},
            {"cveID": "CVE-2023-0001", "dateAdded": "2026-10-01"}
        ]
    }"#;

    async fn migrated_store(dir: &Path) -> Store {
        let store = Store::open(&dir.join("quoro.db")).await.unwrap();
        sqlx::query("INSERT OR REPLACE INTO meta (key, value) VALUES ('cluster_id', 'cls_test')")
            .execute(store.pool())
            .await
            .unwrap();
        store
    }

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quoro-vulns-{}",
            quoro_control_plane::store::random_secret()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn a_run_imports_and_records_one_ok_audit_record() {
        let dir = temp_dir();
        let store = migrated_store(&dir).await;
        drop(store);
        let source = dir.join("import");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(
            source.join("known_exploited_vulnerabilities.json"),
            KEV_BODY,
        )
        .unwrap();

        let imported = import(&dir, &source).await.unwrap();
        assert_eq!(
            imported,
            vec![(
                "KEV".to_string(),
                2,
                Some("2026-11-04T20:00:05.774Z".to_string())
            )]
        );

        let store = Store::open(&dir.join("quoro.db")).await.unwrap();
        let rows = sqlx::query_as::<_, (String, String, i64, Option<String>, Option<String>)>(
            "SELECT ecosystem, source, record_count, data_modified_at, last_error FROM osv_mirror",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "KEV");
        assert_eq!(rows[0].1, "import");
        assert_eq!(rows[0].2, 2);
        assert_eq!(rows[0].3.as_deref(), Some("2026-11-04T20:00:05.774Z"));
        assert!(rows[0].4.is_none());

        let audits = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                Option<String>,
                String,
                String,
            ),
        >(
            "SELECT actor_type, actor_id, channel, operation, target_id, result, params \
             FROM audit_log ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(audits.len(), 1);
        let (actor_type, actor_id, channel, operation, target_id, result, params) = &audits[0];
        assert_eq!(actor_type, "system");
        assert_eq!(actor_id, "vulnerability_data");
        assert_eq!(channel, "cli");
        assert_eq!(operation, "vulnerability_data.import");
        assert_eq!(target_id.as_deref(), Some("cls_test"));
        assert_eq!(result, "ok");
        let params: Value = serde_json::from_str(params).unwrap();
        assert_eq!(params["ecosystems"], json!(["KEV"]));
        assert_eq!(params["record_counts"]["KEV"], json!(2));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_failing_run_records_one_error_audit_record() {
        let dir = temp_dir();
        let store = migrated_store(&dir).await;
        drop(store);
        let source = dir.join("import");
        std::fs::create_dir_all(&source).unwrap();

        let error = import(&dir, &source).await.unwrap_err();
        assert!(error.contains("no OSV data found"), "{error}");

        let store = Store::open(&dir.join("quoro.db")).await.unwrap();
        let audits = sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT operation, result, actor_id, result_detail FROM audit_log ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].0, "vulnerability_data.import");
        assert_eq!(audits[0].1, "error");
        assert_eq!(audits[0].2, "vulnerability_data");
        assert!(
            audits[0]
                .3
                .as_deref()
                .is_some_and(|detail| detail.contains("no OSV data found"))
        );
        let rows = sqlx::query_as::<_, (i64,)>("SELECT count(*) FROM osv_mirror")
            .fetch_all(store.pool())
            .await
            .unwrap();
        assert_eq!(rows[0].0, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_directory_and_a_config_are_parsed() {
        let args = vec![
            "/data/osv".to_string(),
            "--config".to_string(),
            "/etc/quoro/quoro.toml".to_string(),
        ];
        let (dir, flags) = parse_import_args(&args).unwrap();
        assert_eq!(dir, PathBuf::from("/data/osv"));
        assert_eq!(flags.len(), 1);
        assert_eq!(
            crate::config_path(&flags),
            PathBuf::from("/etc/quoro/quoro.toml")
        );

        assert!(parse_import_args(&[]).is_err());
        assert!(parse_import_args(&["a".into(), "b".into()]).is_err());
        assert!(parse_import_args(&["--nope".into(), "x".into()]).is_err());
    }
}

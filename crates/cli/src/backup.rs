//! `quoro backup list | create | restore BACKUP --node NODE`.
//!
//! A backup belongs to an app: it is a copy of the app's volume (ADR 0147).
//! `list` and `create` act on the folder's linked app, or on `--app` (in
//! `--project` or the linked project); `restore` names its backup by id and
//! needs no app.
//!
//! - `list` shows the app's newest backups (`listAppBackups`).
//! - `create` queues a backup of the app's volume now (`createBackup`), with a
//!   fresh `Idempotency-Key`; the volume's node takes it.
//! - `restore BACKUP --node NODE` restores a succeeded backup into a new
//!   volume on `NODE` and moves the app there (`restoreBackup`): the app stops
//!   and its current volume is detached. `NODE` is a node id (`nod_...`) or a
//!   node name, read with `listNodes` (`node:read`). It is destructive
//!   (`backup:restore`): a human types the app's name on a terminal or passes
//!   `--yes`, and an agent's call goes through the approval flags. The
//!   approval grant matches the node the call names, and the repeated call
//!   carries the same `Idempotency-Key`: the primary checks the approval
//!   before the key, so the refused first call never claimed it.

use crate::Environment;
use crate::cli::{BackupCommand, Global};
use crate::client::idempotency_key;
use crate::error::{Failure, exit};
use crate::operations::{CREATE_BACKUP, GET_BACKUP, LIST_APP_BACKUPS, RESTORE_BACKUP};
use crate::scope::{self, Confirm, Scope, is_id, text};
use reqwest::Method;
use serde_json::json;
use std::io::Write;

/// How many backups `backup list` shows.
const BACKUPS: usize = 20;

pub async fn run(
    global: &Global,
    command: BackupCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        BackupCommand::List => list(global, environment, stdout).await,
        BackupCommand::Create => create(global, environment, stdout).await,
        BackupCommand::Restore {
            backup,
            node,
            approval,
        } => {
            if !is_id("bkp_", &backup) {
                return Err(Failure::usage(format!(
                    "'{backup}' is not a backup id (bkp_...); quoro backup list shows them"
                )));
            }
            let mut scope = Scope::open(global, environment, "backup restore", true)?;
            scope::can_confirm_change(
                global,
                environment,
                scope.is_agent,
                "restoring a backup over an app's data",
            )?;
            let found = scope
                .get(&GET_BACKUP.path.replace("{backup_id}", &backup))
                .await?;
            let app_id = found["app_id"].as_str().map(str::to_string).ok_or_else(|| {
                Failure::new(
                    exit::CONFLICT,
                    format!(
                        "the backup {backup} is of the control-plane database, which is restored \
                         offline with quoro restore on the host"
                    ),
                )
            })?;
            let node_id = scope.node_id(&node).await?;
            // The name reads better in the prompt; the id stands in when the
            // caller may not read the app.
            let app_name = match scope.get(&crate::app::app_path(&app_id)).await {
                Ok(app) => text(&app, "name"),
                Err(_) => app_id.clone(),
            };
            scope::confirm(
                global,
                environment,
                scope.is_agent,
                &Confirm {
                    change: "restoring a backup over an app's data",
                    warning: &format!(
                        "This stops the app {app_name}, replaces its data with the backup \
                         {backup} of {} and moves the app to the node {node_id}",
                        text(&found, "created_at")
                    ),
                    label: "the app's name",
                    expected: &app_name,
                    nothing: "nothing was restored",
                },
                stderr,
            )?;
            let mut call = scope.change(
                Method::POST,
                RESTORE_BACKUP.path.replace("{backup_id}", &backup),
                Some(&json!({ "node_id": node_id })),
            )?;
            call.headers.insert("idempotency-key", idempotency_key());
            let answer = scope
                .send_destructive(&call, &approval, global.json, stdout, stderr)
                .await?;
            scope::report(&answer, global.json, stdout, |volume| {
                format!(
                    "Restoring the backup {backup} into the volume {} on the node {node_id}; the \
                     app {app_name} starts there once the volume is ready: quoro volume list \
                     --app {app_id} follows it.\n",
                    text(volume, "id")
                )
            })
        }
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "backup list", false)?;
    let app = scope.required_app(global).await?;
    let (name, id) = (text(&app, "name"), text(&app, "id"));
    let path = format!(
        "{}?limit={BACKUPS}",
        LIST_APP_BACKUPS.path.replace("{app_id}", &id)
    );
    let answer = scope.answer(&path).await?;
    if global.json {
        return scope::report(&answer, true, stdout, |_| String::new());
    }
    let page = answer.json()?;
    let backups = page["items"].as_array().cloned().unwrap_or_default();
    if backups.is_empty() {
        let line = format!("The app {name} has no backups; take one with quoro backup create.\n");
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        ["ID", "STATUS", "TRIGGER", "CREATED", "SIZE", "VOLUME"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    ];
    let mut errors = String::new();
    for backup in &backups {
        let status = if backup["pruned_at"].is_string() {
            format!("{} (pruned)", text(backup, "status"))
        } else {
            text(backup, "status")
        };
        rows.push(vec![
            text(backup, "id"),
            status,
            text(backup, "trigger"),
            text(backup, "created_at"),
            backup["logical_bytes"]
                .as_u64()
                .map_or_else(|| "-".to_string(), size),
            text(backup, "volume_id"),
        ]);
        if let Some(error) = backup["error"].as_str() {
            errors.push_str(&format!("{}: {error}\n", text(backup, "id")));
        }
    }
    let mut out = scope::table(&rows);
    out.push_str(&errors);
    if page["next_cursor"].as_str().is_some_and(|c| !c.is_empty()) {
        out.push_str(&format!(
            "The {BACKUPS} newest backups; quoro api GET {}?cursor=... pages further.\n",
            LIST_APP_BACKUPS.path.replace("{app_id}", &id)
        ));
    }
    crate::api::write(stdout, out.as_bytes())
}

async fn create(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "backup create", true)?;
    let app = scope.required_app(global).await?;
    let name = text(&app, "name");
    let mut call = scope.change(
        Method::POST,
        CREATE_BACKUP.path.replace("{app_id}", &text(&app, "id")),
        None,
    )?;
    call.headers.insert("idempotency-key", idempotency_key());
    let answer = scope.send(&call).await?;
    scope::report(&answer, global.json, stdout, |backup| {
        format!(
            "Backing up the volume of the app {name}: the backup {} is {}; quoro backup list \
             shows how it ends.\n",
            text(backup, "id"),
            text(backup, "status")
        )
    })
    .map_err(|mut failure| {
        if failure.code == exit::CONFLICT {
            failure.message = format!(
                "{}; a backup needs a ready volume (quoro volume list), a backup repository \
                 and no other backup in progress (quoro backup list)",
                failure.message
            );
        }
        failure
    })
}

/// Bytes in binary units, such as `2.0 MiB`.
fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_read_in_binary_units() {
        assert_eq!(size(0), "0 B");
        assert_eq!(size(1023), "1023 B");
        assert_eq!(size(2048), "2.0 KiB");
        assert_eq!(size(5 * 1024 * 1024 * 1024), "5.0 GiB");
    }
}

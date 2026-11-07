//! `quoro volume list | create --size GB --mount PATH | delete VOLUME`.
//!
//! Each acts on the folder's linked app, or on `--app` (in `--project` or the
//! linked project); `delete` given a volume id needs no app.
//!
//! - `create` makes a sparse local disk for the app on one node and makes the
//!   app stateful (`createVolume`, ADR 0145), with a fresh `Idempotency-Key`.
//!   It attaches at the app's next start, which the call does not trigger.
//! - `delete VOLUME` deletes a volume and its data (`deleteVolume`); the
//!   volume is named by its id (`vol_...`) or by its mount path among the
//!   app's volumes; a mount path two volumes share (after a restore) is
//!   refused with their ids. It is destructive (`app:delete`): a human types
//!   the mount path on a terminal or passes `--yes`, and an agent's call goes
//!   through the approval flags (`--request-approval`, `--wait`).

use crate::Environment;
use crate::cli::{Global, VolumeCommand};
use crate::client::idempotency_key;
use crate::error::{Failure, exit};
use crate::operations::{CREATE_VOLUME, DELETE_VOLUME, GET_VOLUME, LIST_APP_VOLUMES};
use crate::scope::{self, Confirm, Scope, is_id, text};
use reqwest::Method;
use serde_json::{Value, json};
use std::io::Write;

pub async fn run(
    global: &Global,
    command: VolumeCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        VolumeCommand::List => list(global, environment, stdout).await,
        VolumeCommand::Create { size, mount } => {
            create(global, environment, size, &mount, stdout).await
        }
        VolumeCommand::Delete { volume, approval } => {
            if !is_id("vol_", &volume) && !volume.starts_with('/') {
                return Err(Failure::usage(format!(
                    "'{volume}' is neither a volume id (vol_...) nor a mount path; \
                     quoro volume list shows them"
                )));
            }
            let mut scope = Scope::open(global, environment, "volume delete", true)?;
            scope::can_confirm(global, environment, scope.is_agent, "a volume")?;
            let volume = if is_id("vol_", &volume) {
                scope
                    .get(&GET_VOLUME.path.replace("{volume_id}", &volume))
                    .await?
            } else {
                let app = scope.required_app(global).await?;
                // After a restore the old and the new volume share the mount
                // path: more than one match is refused, never guessed.
                let mut matches: Vec<Value> = volumes(&mut scope, &app)
                    .await?
                    .into_iter()
                    .filter(|v| v["mount_path"] == volume.as_str() && v["state"] != "deleting")
                    .collect();
                match matches.len() {
                    1 => matches.remove(0),
                    0 => {
                        return Err(Failure::new(
                            exit::NOT_FOUND,
                            format!(
                                "the app {} has no volume at {volume}; quoro volume list shows its volumes",
                                text(&app, "name")
                            ),
                        ));
                    }
                    n => {
                        return Err(Failure::usage(format!(
                            "{n} volumes of the app {} are at {volume}: {}; name one by its id",
                            text(&app, "name"),
                            matches
                                .iter()
                                .map(|v| format!("{} ({})", text(v, "id"), text(v, "state")))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    }
                }
            };
            let (id, mount) = (text(&volume, "id"), text(&volume, "mount_path"));
            scope::confirm(
                global,
                environment,
                scope.is_agent,
                &Confirm {
                    change: "deleting a volume",
                    warning: &format!(
                        "This deletes the volume {id} ({} GiB at {mount}) of the app {} and its \
                         data for good",
                        text(&volume, "size_gb"),
                        text(&volume, "app_id")
                    ),
                    label: "its mount path",
                    expected: &mount,
                    nothing: "nothing was deleted",
                },
                stderr,
            )?;
            let call = scope.change(
                Method::DELETE,
                DELETE_VOLUME.path.replace("{volume_id}", &id),
                None,
            )?;
            let answer = scope
                .send_destructive(&call, &approval, global.json, stdout, stderr)
                .await?;
            scope::report(&answer, global.json, stdout, |volume| {
                format!(
                    "Deleting the volume {id} ({mount}): it is {} until its node removes the file. \
                     Its backups stay until retention prunes them.\n",
                    text(volume, "state")
                )
            })
            .map_err(|mut failure| {
                if failure.code == exit::CONFLICT {
                    failure.message = format!(
                        "{}; a running app's volume is deleted once the app is stopped: quoro app stop",
                        failure.message
                    );
                }
                failure
            })
        }
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "volume list", false)?;
    let app = scope.required_app(global).await?;
    let path = LIST_APP_VOLUMES.path.replace("{app_id}", &text(&app, "id"));
    let answer = scope.answer(&path).await?;
    if global.json {
        return scope::report(&answer, true, stdout, |_| String::new());
    }
    let page = answer.json()?;
    let volumes = page["items"].as_array().cloned().unwrap_or_default();
    if volumes.is_empty() {
        let line = format!(
            "The app {} has no volumes; create one with quoro volume create --size GB --mount PATH.\n",
            text(&app, "name")
        );
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        ["ID", "STATE", "SIZE", "MOUNT", "NODE", "CREATED"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    ];
    let mut errors = String::new();
    for volume in &volumes {
        rows.push(vec![
            text(volume, "id"),
            text(volume, "state"),
            format!("{} GiB", text(volume, "size_gb")),
            text(volume, "mount_path"),
            text(volume, "node_id"),
            text(volume, "created_at"),
        ]);
        if let Some(error) = volume["error"].as_str() {
            errors.push_str(&format!("{}: {error}\n", text(volume, "id")));
        }
    }
    let mut out = scope::table(&rows);
    out.push_str(&errors);
    crate::api::write(stdout, out.as_bytes())
}

async fn create(
    global: &Global,
    environment: &Environment,
    size: u32,
    mount: &str,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    if !valid_mount(mount) {
        return Err(Failure::usage(format!(
            "--mount '{mount}' must be an absolute path such as /data, of A-Z, a-z, 0-9, ., _, - \
             and /, with no . or .. segment, not / and not under /proc, /sys or /dev"
        )));
    }
    let mut scope = Scope::open(global, environment, "volume create", true)?;
    let app = scope.required_app(global).await?;
    let name = text(&app, "name");
    let mut call = scope.change(
        Method::POST,
        CREATE_VOLUME.path.replace("{app_id}", &text(&app, "id")),
        Some(&json!({ "size_gb": size, "mount_path": mount })),
    )?;
    call.headers.insert("idempotency-key", idempotency_key());
    let answer = scope.send(&call).await?;
    scope::report(&answer, global.json, stdout, |volume| {
        format!(
            "Created the volume {} ({size} GiB at {mount}) of the app {name} on the node {}; \
             it is {}. It attaches at the app's next start: quoro restart.\n",
            text(volume, "id"),
            text(volume, "node_id"),
            text(volume, "state")
        )
    })
    .map_err(|mut failure| {
        if failure.code == exit::CONFLICT {
            failure.message = format!(
                "{}; an app has one volume and one replica: quoro volume list shows it",
                failure.message
            );
        }
        failure
    })
}

/// Every volume of `app` (`listAppVolumes` is not paged).
async fn volumes(scope: &mut Scope, app: &Value) -> Result<Vec<Value>, Failure> {
    let page = scope
        .get(&LIST_APP_VOLUMES.path.replace("{app_id}", &text(app, "id")))
        .await?;
    Ok(page["items"].as_array().cloned().unwrap_or_default())
}

/// A mount path the contract accepts (`CreateVolumeRequest.mount_path`).
fn valid_mount(path: &str) -> bool {
    let Some(rest) = path.strip_prefix('/') else {
        return false;
    };
    (2..=255).contains(&path.len())
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/' | b'-'))
        && rest
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
        && !["proc", "sys", "dev"].contains(&rest.split('/').next().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_paths_are_what_the_contract_accepts() {
        for good in ["/data", "/var/lib/pg-data", "/a.b/c_d"] {
            assert!(valid_mount(good), "{good}");
        }
        for bad in [
            "", "/", "data", "/data/", "/a//b", "/a/./b", "/a/../b", "/proc", "/sys/x", "/dev",
            "/a b",
        ] {
            assert!(!valid_mount(bad), "{bad}");
        }
    }
}

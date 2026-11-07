//! `quoro up | deploy | redeploy | restart | build list | build cancel`.
//!
//! Each acts on the folder's linked app, or on `--app` (in `--project` or the
//! linked project).
//!
//! - `up [--ref REF] [--detach]` builds a Git app (`startBuild`) and follows
//!   its log until the build is terminal: exit 0 when it succeeded, 1
//!   ([`exit::OTHER`]) when it failed or was cancelled. Ctrl-C stops following
//!   and leaves the build running (exit 0). When the log cannot be streamed
//!   (the node unreachable) the build's status is followed instead. An image
//!   app has nothing to build: `up` refuses it with exit 6 and points to
//!   `deploy --image`. Building a local folder is not in the contract yet.
//! - `deploy --image REF` rolls a new image out to an image app
//!   (`createDeployment`); a Git app answers `conflict` (ADR 0117), exit 6,
//!   with a pointer to `up`.
//! - `deploy --rollback DEPLOYMENT` deploys what an earlier deployment of the
//!   app ran. For an image app that is its image, pinned to the digest it
//!   resolved to when it is known. A Git app cannot be given an image, so it
//!   rebuilds the commit of the build that made that deployment
//!   (`startBuild` with that commit as `ref`). The contract has no rollback
//!   request: the new deployment records `trigger: api` (or `build`), not
//!   `rollback`.
//! - `redeploy` rolls the current image out again (`redeployApp`); `restart`
//!   restarts the instances (`restartApp`).
//! - `build list` shows the newest builds; `build cancel [BUILD]` cancels one,
//!   by default the app's build in progress (exit 5 when there is none).
//!
//! `startBuild` and `createDeployment` carry a fresh `Idempotency-Key`, so a
//! request that reaches the primary twice starts one build or deployment. An
//! agent's change needs `--reason` (`scope.rs`).

use crate::Environment;
use crate::cli::{BuildCommand, DeployArgs, Global, UpArgs};
use crate::client::{Answer, idempotency_key};
use crate::error::{Failure, exit};
use crate::logs::{self, Follow, Followed, Status, terminal};
use crate::operations::{
    CANCEL_BUILD, CREATE_DEPLOYMENT, GET_BUILD, GET_DEPLOYMENT, LIST_APP_BUILDS, REDEPLOY_APP,
    RESTART_APP, START_BUILD,
};
use crate::scope::{self, Scope, is_id, text};
use reqwest::Method;
use serde_json::{Value, json};
use std::io::Write;

/// How many builds `build list` shows, and how far `build cancel` looks for
/// the one in progress.
const BUILDS: usize = 20;
/// The registry of the images builds produce, which no node pulls.
const BUILT_IMAGE_PREFIX: &str = "build.quoro.invalid/";

pub async fn up(
    global: &Global,
    args: UpArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "up", true)?;
    let app = scope.required_app(global).await?;
    if !is_git(&app) {
        return Err(Failure::new(
            exit::CONFLICT,
            format!(
                "the app {} deploys an image and has nothing to build; use quoro deploy --image REF",
                text(&app, "name")
            ),
        ));
    }
    let body = match &args.git_ref {
        Some(git_ref) => json!({ "ref": git_ref }),
        None => json!({}),
    };
    let build = start_build(&mut scope, &app, &body, global.json, stdout).await?;
    let id = text(&build, "id");
    if args.detach {
        if !global.json {
            crate::api::write(
                stdout,
                format!(
                    "Follow it with quoro logs --build {id} -f; stop it with quoro build cancel.\n"
                )
                .as_bytes(),
            )?;
        }
        return Ok(());
    }
    let followed = if terminal(&text(&build, "status")) {
        Followed::Done(build.clone())
    } else {
        let follow = Follow {
            tail: None,
            wait_without_log: true,
            status: Status::seen(&build),
            json: global.json,
        };
        logs::follow_build(&mut scope, &id, follow, stdout, stderr).await?
    };
    match followed {
        Followed::Done(build) => finished(&build, global.json, stdout),
        Followed::Interrupted => {
            let _ = writeln!(
                stderr,
                "quoro: stopped following; the build {id} goes on: quoro logs --build {id} -f \
                 follows it again, quoro build cancel stops it"
            );
            Ok(())
        }
    }
}

/// The end of a followed build: exit 0 when it succeeded, else 1.
fn finished(build: &Value, json: bool, stdout: &mut dyn Write) -> Result<(), Failure> {
    let id = text(build, "id");
    match text(build, "status").as_str() {
        "succeeded" => {
            if json {
                return Ok(());
            }
            let commit = text(build, "commit");
            let deployment = text(build, "deployment_id");
            crate::api::write(
                stdout,
                format!(
                    "Built commit {} as {}; the deployment {deployment} rolls it out: \
                     quoro logs --deployment {deployment} shows its progress.\n",
                    &commit[..commit.len().min(12)],
                    text(build, "image_digest"),
                )
                .as_bytes(),
            )
        }
        "failed" => {
            let phase = build["failed_phase"]
                .as_str()
                .map(|phase| format!(" while {phase}"))
                .unwrap_or_default();
            Err(Failure::other(format!(
                "the build {id} failed{phase}: {}; quoro logs --build {id} shows its log",
                text(build, "error")
            )))
        }
        status => Err(Failure::other(format!("the build {id} ended {status}"))),
    }
}

/// Starts a build of `app` with `body` (`StartBuildRequest`) and prints it.
async fn start_build(
    scope: &mut Scope,
    app: &Value,
    body: &Value,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Value, Failure> {
    let name = text(app, "name");
    let mut call = scope.change(
        Method::POST,
        START_BUILD.path.replace("{app_id}", &text(app, "id")),
        Some(body),
    )?;
    call.headers.insert("idempotency-key", idempotency_key());
    let answer = scope.send(&call).await?;
    let git_ref = body["ref"]
        .as_str()
        .map_or_else(|| text(&app["source"], "ref"), str::to_string);
    report(
        &answer,
        json,
        stdout,
        "the app may already have a build in progress: quoro build list shows it, \
         quoro build cancel stops it",
        |build| {
            format!(
                "Building the app {name} from {} @ {git_ref}: the build {} is {}.\n",
                text(&app["source"], "repo_url"),
                text(build, "id"),
                text(build, "status")
            )
        },
    )?;
    Ok(serde_json::from_slice(&answer.body).unwrap_or(Value::Null))
}

pub async fn deploy(
    global: &Global,
    args: DeployArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    if let Some(rollback) = &args.rollback
        && !is_id("dep_", rollback)
    {
        return Err(Failure::usage(format!(
            "'{rollback}' is not a deployment id (dep_...); quoro status shows the last ones"
        )));
    }
    if args
        .image
        .as_deref()
        .is_some_and(|image| image.trim().is_empty())
    {
        return Err(Failure::usage("--image needs an image reference"));
    }
    let mut scope = Scope::open(global, environment, "deploy", true)?;
    let app = scope.required_app(global).await?;
    if let Some(image) = &args.image {
        return deploy_image(&mut scope, &app, image, None, global.json, stdout).await;
    }
    let Some(rollback) = &args.rollback else {
        return Err(Failure::usage("pass --image REF or --rollback DEPLOYMENT"));
    };
    let deployment = scope
        .get(&GET_DEPLOYMENT.path.replace("{deployment_id}", rollback))
        .await?;
    if deployment["app_id"] != app["id"] {
        return Err(Failure::usage(format!(
            "the deployment {rollback} belongs to the app {}, not to {}",
            text(&deployment, "app_id"),
            text(&app, "name")
        )));
    }
    if !is_git(&app) {
        let image = pinned(
            &text(&deployment, "image"),
            deployment["image_digest"].as_str(),
        );
        return deploy_image(
            &mut scope,
            &app,
            &image,
            Some(rollback),
            global.json,
            stdout,
        )
        .await;
    }
    let commit = rollback_commit(&mut scope, &app, &deployment).await?;
    let build = start_build(
        &mut scope,
        &app,
        &json!({ "ref": commit }),
        global.json,
        stdout,
    )
    .await?;
    if !global.json {
        let id = text(&build, "id");
        crate::api::write(
            stdout,
            format!(
                "It rebuilds the commit the deployment {rollback} ran; follow it with \
                 quoro logs --build {id} -f.\n"
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

/// Rolls `image` out to `app`; `rollback` names the deployment it came from.
async fn deploy_image(
    scope: &mut Scope,
    app: &Value,
    image: &str,
    rollback: Option<&str>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let name = text(app, "name");
    let mut call = scope.change(
        Method::POST,
        CREATE_DEPLOYMENT.path.replace("{app_id}", &text(app, "id")),
        Some(&json!({ "image": image })),
    )?;
    call.headers.insert("idempotency-key", idempotency_key());
    let answer = scope.send(&call).await?;
    let hint = if is_git(app) {
        format!(
            "the app {name} builds its image from its Git source: run quoro up, or quoro up --ref REF"
        )
    } else {
        "quoro status shows the app".to_string()
    };
    report(&answer, json, stdout, &hint, |deployment| {
        let id = text(deployment, "id");
        let what = match rollback {
            Some(from) => format!("Rolling the app {name} back to {image} (from {from})"),
            None => format!("Deploying {image} to the app {name}"),
        };
        format!(
            "{what}: the deployment {id} is {}; quoro logs --deployment {id} shows its progress.\n",
            text(deployment, "status")
        )
    })
}

/// The commit a Git app's deployment ran: that of the build named in its
/// image (`build.quoro.invalid/<app_id>:<build_id>`), else of the build that
/// started it.
async fn rollback_commit(
    scope: &mut Scope,
    app: &Value,
    deployment: &Value,
) -> Result<String, Failure> {
    let deployment_id = text(deployment, "id");
    let image = text(deployment, "image");
    let named = image
        .strip_prefix(BUILT_IMAGE_PREFIX)
        .map(|rest| rest.split('@').next().unwrap_or(rest))
        .and_then(|rest| rest.rsplit_once(':'))
        .map(|(_, build)| build.to_string())
        .filter(|build| is_id("bld_", build));
    let build = match named {
        Some(id) => Some(
            scope
                .get(&GET_BUILD.path.replace("{build_id}", &id))
                .await?,
        ),
        None => scope
            .list_all(&LIST_APP_BUILDS.path.replace("{app_id}", &text(app, "id")))
            .await?
            .into_iter()
            .find(|build| build["deployment_id"] == deployment["id"]),
    };
    let commit = build
        .as_ref()
        .and_then(|build| build["commit"].as_str())
        .filter(|commit| !commit.is_empty());
    match commit {
        Some(commit) => Ok(commit.to_string()),
        None => Err(Failure::new(
            exit::NOT_FOUND,
            format!(
                "no build of the app {} records the commit the deployment {deployment_id} ran, \
                 so there is nothing to rebuild; build a ref with quoro up --ref REF",
                text(app, "name")
            ),
        )),
    }
}

/// `image` pinned to `digest` when it names none yet: `repo@sha256:...`.
fn pinned(image: &str, digest: Option<&str>) -> String {
    match digest {
        Some(digest) if !image.contains('@') && digest.starts_with("sha256:") => {
            format!("{}@{digest}", repository(image))
        }
        _ => image.to_string(),
    }
}

/// An image reference without its tag. A `:` before the last `/` is a
/// registry port.
fn repository(image: &str) -> &str {
    let name = image.rfind('/').map_or(0, |slash| slash + 1);
    match image[name..].rfind(':') {
        Some(colon) => &image[..name + colon],
        None => image,
    }
}

pub async fn redeploy(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "redeploy", true)?;
    let app = scope.required_app(global).await?;
    let name = text(&app, "name");
    let call = scope.change(
        Method::POST,
        REDEPLOY_APP.path.replace("{app_id}", &text(&app, "id")),
        None,
    )?;
    let answer = scope.send(&call).await?;
    let hint = if is_git(&app) {
        "a Git app redeploys what it built: run quoro up first".to_string()
    } else {
        "quoro status shows the app".to_string()
    };
    report(&answer, global.json, stdout, &hint, |deployment| {
        let id = text(deployment, "id");
        format!(
            "Redeploying {} to the app {name}: the deployment {id} is {}; \
             quoro logs --deployment {id} shows its progress.\n",
            text(deployment, "image"),
            text(deployment, "status")
        )
    })
}

pub async fn restart(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "restart", true)?;
    let app = scope.required_app(global).await?;
    let call = scope.change(
        Method::POST,
        RESTART_APP.path.replace("{app_id}", &text(&app, "id")),
        None,
    )?;
    let answer = scope.send(&call).await?;
    report(
        &answer,
        global.json,
        stdout,
        "quoro status shows the app",
        |app| {
            format!(
                "Restarting the instances of the app {} (restart generation {}); \
                 the nodes restart them.\n",
                text(app, "name"),
                text(app, "restart_generation")
            )
        },
    )
}

pub async fn build(
    global: &Global,
    command: BuildCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        BuildCommand::List => list(global, environment, stdout).await,
        BuildCommand::Cancel { build } => cancel(global, environment, build, stdout).await,
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "build list", false)?;
    let app = scope.required_app(global).await?;
    let (name, id) = (text(&app, "name"), text(&app, "id"));
    let path = format!(
        "{}?limit={BUILDS}",
        LIST_APP_BUILDS.path.replace("{app_id}", &id)
    );
    let answer = scope.answer(&path).await?;
    if global.json {
        return scope::report(&answer, true, stdout, |_| String::new());
    }
    let page = answer.json()?;
    let builds = page["items"].as_array().cloned().unwrap_or_default();
    if builds.is_empty() {
        let line = if is_git(&app) {
            format!("No builds of the app {name} yet; start one with quoro up.\n")
        } else {
            format!("The app {name} deploys an image; it has no builds.\n")
        };
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        ["ID", "STATUS", "REF", "COMMIT", "CREATED", "DEPLOYMENT"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    ];
    for build in &builds {
        let commit = text(build, "commit");
        rows.push(vec![
            text(build, "id"),
            text(build, "status"),
            text(&build["source"], "ref"),
            commit[..commit.len().min(12)].to_string(),
            text(build, "created_at"),
            text(build, "deployment_id"),
        ]);
    }
    let mut out = scope::table(&rows);
    if page["next_cursor"].as_str().is_some_and(|c| !c.is_empty()) {
        out.push_str(&format!(
            "The {BUILDS} newest builds; quoro api GET {}?cursor=... pages further.\n",
            LIST_APP_BUILDS.path.replace("{app_id}", &id)
        ));
    }
    crate::api::write(stdout, out.as_bytes())
}

async fn cancel(
    global: &Global,
    environment: &Environment,
    build: Option<String>,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    if let Some(build) = &build
        && !is_id("bld_", build)
    {
        return Err(Failure::usage(format!(
            "'{build}' is not a build id (bld_...); quoro build list shows them"
        )));
    }
    let mut scope = Scope::open(global, environment, "build cancel", true)?;
    let id = match build {
        Some(id) => id,
        None => {
            let app = scope.required_app(global).await?;
            let page = scope
                .get(&format!(
                    "{}?limit={BUILDS}",
                    LIST_APP_BUILDS.path.replace("{app_id}", &text(&app, "id"))
                ))
                .await?;
            page["items"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|build| !terminal(&text(build, "status")))
                .map(|build| text(build, "id"))
                .ok_or_else(|| {
                    Failure::new(
                        exit::NOT_FOUND,
                        format!(
                            "the app {} has no build in progress; quoro build list shows its builds",
                            text(&app, "name")
                        ),
                    )
                })?
        }
    };
    let call = scope.change(
        Method::POST,
        CANCEL_BUILD.path.replace("{build_id}", &id),
        None,
    )?;
    let answer = scope.send(&call).await?;
    report(
        &answer,
        global.json,
        stdout,
        "the build already finished: quoro build list shows how it ended",
        |build| match text(build, "status").as_str() {
            "cancelled" => format!("Cancelled the build {id}.\n"),
            status => format!(
                "Cancelling the build {id} ({status}); the node stops it and it ends cancelled.\n"
            ),
        },
    )
}

/// Whether the app builds from a Git source.
fn is_git(app: &Value) -> bool {
    app["source"].is_object()
}

/// [`scope::report`], with `hint` after a `conflict`.
fn report(
    answer: &Answer,
    json: bool,
    stdout: &mut dyn Write,
    hint: &str,
    line: impl FnOnce(&Value) -> String,
) -> Result<(), Failure> {
    scope::report(answer, json, stdout, line).map_err(|mut failure| {
        if failure.code == exit::CONFLICT {
            failure.message = format!("{}; {hint}", failure.message);
        }
        failure
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rollback_image_is_pinned_to_its_digest() {
        let digest = "sha256:abc";
        assert_eq!(
            pinned("ghcr.io/x/web:1", Some(digest)),
            "ghcr.io/x/web@sha256:abc"
        );
        assert_eq!(
            pinned("localhost:5000/web:1", Some(digest)),
            "localhost:5000/web@sha256:abc"
        );
        assert_eq!(
            pinned("localhost:5000/web", Some(digest)),
            "localhost:5000/web@sha256:abc"
        );
        assert_eq!(pinned("web@sha256:def", Some(digest)), "web@sha256:def");
        assert_eq!(pinned("ghcr.io/x/web:1", None), "ghcr.io/x/web:1");
        assert_eq!(
            pinned("ghcr.io/x/web:1", Some("ghcr.io/x/web:1")),
            "ghcr.io/x/web:1"
        );
    }
}

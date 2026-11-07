//! `quoro app list | create | update | start | stop | delete`.
//!
//! Every command but `list` and `create` acts on the folder's linked app, or
//! on `--app` (in `--project` or the linked project). `create` adds the app
//! to the project of `--project` or the link and, when the folder is linked
//! to that project without an app, links the new app too.
//!
//! Only the settings a task needs have flags: the image or the Git source,
//! the port, replicas, vCPUs, memory and the health check path. Everything
//! else `createApp` and `updateApp` accept (health check timings, placement,
//! HA policy, backup schedule) is set in the panel or with `quoro api`.
//!
//! `delete` is destructive: a human types the app's name on a terminal or
//! passes `--yes`, and an agent's call goes through the approval flags
//! (`--request-approval`, `--wait`).

use crate::Environment;
use crate::cli::{AppCommand, AppCreateArgs, AppUpdateArgs, Global};
use crate::error::Failure;
use crate::links::Links;
use crate::operations::{
    CREATE_APP, DELETE_APP, GET_APP, LIST_APPS, LIST_PROJECTS, START_APP, STOP_APP, UPDATE_APP,
};
use crate::scope::{self, Scope, text};
use reqwest::Method;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::io::Write;

/// The default `--ref` of a Git source.
const DEFAULT_REF: &str = "main";

pub async fn run(
    global: &Global,
    command: AppCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        AppCommand::List => list(global, environment, stdout).await,
        AppCommand::Create(args) => create(global, environment, args, stdout, stderr).await,
        AppCommand::Update(args) => update(global, environment, args, stdout).await,
        AppCommand::Start => lifecycle(global, environment, "start", stdout).await,
        AppCommand::Stop => lifecycle(global, environment, "stop", stdout).await,
        AppCommand::Delete { approval } => {
            let mut scope = Scope::open(global, environment, "app delete", true)?;
            scope::can_confirm(global, environment, scope.is_agent, "an app")?;
            let app = scope.required_app(global).await?;
            let (name, id) = (text(&app, "name"), text(&app, "id"));
            scope::confirm_name(
                global,
                environment,
                scope.is_agent,
                &format!("the app {name}, its instances, domains and volumes"),
                &name,
                stderr,
            )?;
            let call = scope.change(
                Method::DELETE,
                DELETE_APP.path.replace("{app_id}", &id),
                None,
            )?;
            let answer = scope
                .send_destructive(&call, &approval, global.json, stdout, stderr)
                .await?;
            scope::report(&answer, global.json, stdout, |_| {
                format!("Deleting the app {name} ({id}): its instances stop and are removed.\n")
            })?;
            if let Some(context) = &scope.context_name {
                let mut links = Links::read(environment)?;
                if links.forget_app(context, &id) > 0 {
                    links.write()?;
                }
            }
            Ok(())
        }
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "app list", false)?;
    let project = scope.project_in_scope(global).await?;
    let path = match &project {
        Some(project) => format!("{}?project_id={}", LIST_APPS.path, text(project, "id")),
        None => LIST_APPS.path.to_string(),
    };
    let apps = scope.list_all(&path).await?;
    if global.json {
        let body = json!({ "items": apps, "next_cursor": null });
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    if apps.is_empty() {
        return crate::api::write(stdout, b"No apps. Create one with quoro app create NAME.\n");
    }
    // Without a project the apps of every project are listed, by slug.
    let slugs: Option<BTreeMap<String, String>> = if project.is_none() {
        let projects = scope.list_all(LIST_PROJECTS.path).await?;
        Some(
            projects
                .iter()
                .map(|p| (text(p, "id"), text(p, "slug")))
                .collect(),
        )
    } else {
        None
    };
    let mut header = vec!["NAME", "ID", "STATUS", "REPLICAS", "SOURCE"];
    if slugs.is_some() {
        header.insert(0, "PROJECT");
    }
    let mut rows = vec![header.into_iter().map(str::to_string).collect::<Vec<_>>()];
    for app in &apps {
        let mut row = vec![
            text(app, "name"),
            text(app, "id"),
            text(app, "status"),
            text(app, "replicas"),
            source(app),
        ];
        if let Some(slugs) = &slugs {
            let project_id = text(app, "project_id");
            row.insert(0, slugs.get(&project_id).cloned().unwrap_or(project_id));
        }
        rows.push(row);
    }
    crate::api::write(stdout, scope::table(&rows).as_bytes())
}

/// `ghcr.io/x/web:1`, or `git https://... @ main`.
pub fn source(app: &Value) -> String {
    match app.get("source").filter(|s| s.is_object()) {
        Some(source) => format!("git {} @ {}", text(source, "repo_url"), text(source, "ref")),
        None => {
            let image = text(app, "image");
            if image.is_empty() {
                "-".to_string()
            } else {
                image
            }
        }
    }
}

async fn create(
    global: &Global,
    environment: &Environment,
    args: AppCreateArgs,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    if !valid_name(&args.name) {
        return Err(Failure::usage(format!(
            "the app name {:?} must be 1 to 40 of a-z, 0-9 and -",
            args.name
        )));
    }
    if args.image.is_some() && (args.git_ref.is_some() || args.dockerfile.is_some()) {
        return Err(Failure::usage(
            "--ref and --dockerfile go with --repo, not --image",
        ));
    }
    let mut body = json!({ "name": args.name, "port": args.port });
    match (&args.image, &args.repo) {
        (Some(image), None) => body["image"] = json!(image),
        (None, Some(repo)) => {
            let mut source = json!({
                "kind": "git",
                "repo_url": repo,
                "ref": args.git_ref.as_deref().unwrap_or(DEFAULT_REF),
            });
            if let Some(dockerfile) = &args.dockerfile {
                source["dockerfile_path"] = json!(dockerfile);
            }
            body["source"] = source;
        }
        _ => return Err(Failure::usage("pass exactly one of --image and --repo")),
    }
    let mut scope = Scope::open(global, environment, "app create", true)?;
    let project = scope.required_project(global).await?;
    let project_id = text(&project, "id");
    body["project_id"] = json!(project_id);
    let call = scope.change(Method::POST, CREATE_APP.path.to_string(), Some(&body))?;
    let answer = scope.send(&call).await?;
    let slug = text(&project, "slug");
    scope::report(&answer, global.json, stdout, |app| {
        format!(
            "Created the app {} ({}) in the project {slug}.\n",
            text(app, "name"),
            text(app, "id")
        )
    })?;
    let app: Value = serde_json::from_slice(&answer.body).unwrap_or(Value::Null);
    // The folder linked to this project without an app gets the new app.
    let Some((dir, mut link)) = scope.link.clone() else {
        return Ok(());
    };
    if link.project_id != project_id || link.app_id.is_some() {
        return Ok(());
    }
    link.app_id = Some(text(&app, "id"));
    let mut links = Links::read(environment)?;
    links.set(&dir, link)?;
    links.write()?;
    let note = format!("Linked {} to it.\n", dir.display());
    if global.json {
        let _ = write!(stderr, "quoro: {note}");
        Ok(())
    } else {
        crate::api::write(stdout, note.as_bytes())
    }
}

async fn update(
    global: &Global,
    environment: &Environment,
    args: AppUpdateArgs,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    if args.replicas.is_none()
        && args.vcpus.is_none()
        && args.memory.is_none()
        && args.port.is_none()
        && args.health_path.is_none()
    {
        return Err(Failure::usage(
            "nothing to change: pass --replicas, --vcpus, --memory, --port or --health-path",
        ));
    }
    if let Some(path) = &args.health_path
        && !path.starts_with('/')
    {
        return Err(Failure::usage("--health-path must start with /"));
    }
    let mut scope = Scope::open(global, environment, "app update", true)?;
    let app = scope.required_app(global).await?;
    let id = text(&app, "id");
    let mut body = Map::new();
    if let Some(replicas) = args.replicas {
        body.insert("replicas".to_string(), json!(replicas));
    }
    if let Some(port) = args.port {
        body.insert("port".to_string(), json!(port));
    }
    // The contract replaces `resources` and `health_check` whole, so the
    // fields not given keep the app's current values.
    if args.vcpus.is_some() || args.memory.is_some() {
        let mut resources = app["resources"].clone();
        if let Some(vcpus) = args.vcpus {
            resources["vcpus"] = json!(vcpus);
        }
        if let Some(memory) = args.memory {
            resources["memory_mb"] = json!(memory);
        }
        body.insert("resources".to_string(), resources);
    }
    if let Some(path) = &args.health_path {
        let mut health = app["health_check"].clone();
        health["path"] = json!(path);
        body.insert("health_check".to_string(), health);
    }
    let call = scope.change(
        Method::PATCH,
        UPDATE_APP.path.replace("{app_id}", &id),
        Some(&Value::Object(body)),
    )?;
    let answer = scope.send(&call).await?;
    scope::report(&answer, global.json, stdout, |app| {
        format!(
            "Updated the app {} ({}): {}\n",
            text(app, "name"),
            text(app, "id"),
            settings(app)
        )
    })
}

/// `replicas 2, 2 vCPU, 1024 MiB, port 8080, health /healthz`.
pub fn settings(app: &Value) -> String {
    format!(
        "replicas {}, {} vCPU, {} MiB, port {}, health {}",
        text(app, "replicas"),
        text(&app["resources"], "vcpus"),
        text(&app["resources"], "memory_mb"),
        text(app, "port"),
        text(&app["health_check"], "path")
    )
}

async fn lifecycle(
    global: &Global,
    environment: &Environment,
    verb: &str,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, &format!("app {verb}"), true)?;
    let app = scope.required_app(global).await?;
    let id = text(&app, "id");
    let operation = if verb == "start" { START_APP } else { STOP_APP };
    let call = scope.change(Method::POST, operation.path.replace("{app_id}", &id), None)?;
    let answer = scope.send(&call).await?;
    let doing = if verb == "start" {
        "Starting"
    } else {
        "Stopping"
    };
    scope::report(&answer, global.json, stdout, |app| {
        format!(
            "{doing} the app {} ({}); its status is {} and follows on the node.\n",
            text(app, "name"),
            text(app, "id"),
            text(app, "status")
        )
    })
}

fn valid_name(name: &str) -> bool {
    (1..=40).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// The read of one app that `status` and the resolution use.
pub fn app_path(id: &str) -> String {
    GET_APP.path.replace("{app_id}", id)
}

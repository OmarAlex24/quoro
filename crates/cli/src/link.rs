//! `quoro link [PROJECT] [APP]` and `quoro unlink`.
//!
//! `link` records the current folder in `links.toml` (`links.rs`) with the
//! context, the project and, optionally, the app, each named by name or id.
//! With no argument on a terminal it offers numbered lists to pick from;
//! without a terminal it needs the names. `unlink` removes the nearest link
//! at or above the current folder and sends nothing.

use crate::Environment;
use crate::cli::{Global, LinkArgs};
use crate::error::Failure;
use crate::links::{self, Link, Links};
use crate::operations::{LIST_APPS, LIST_PROJECTS};
use crate::scope::{Scope, text};
use serde_json::Value;
use std::io::Write;

pub async fn link(
    global: &Global,
    args: LinkArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let cwd = links::current_dir(environment)?;
    let project_ref = args.project_ref.or_else(|| global.project.clone());
    let app_ref = args.app_ref.or_else(|| global.app.clone());
    if project_ref.is_none() && !environment.stdin_is_terminal {
        return Err(Failure::usage(
            "name the project to link: quoro link PROJECT [APP] (there is no terminal to pick from)",
        ));
    }
    let mut scope = Scope::open(global, environment, "link", false)?;
    let Some(context) = scope.context_name.clone() else {
        return Err(Failure::usage(
            "an ad-hoc context from QUORO_URL cannot be linked: log in to a named context first",
        ));
    };
    let project = match &project_ref {
        Some(reference) => scope.project(reference).await?,
        None => {
            let projects = scope.list_all(LIST_PROJECTS.path).await?;
            if projects.is_empty() {
                return Err(Failure::usage(
                    "there is no project to link: create one with quoro project create NAME",
                ));
            }
            pick(environment, stderr, "project", &projects, false, |p| {
                format!("{} ({})", text(p, "slug"), text(p, "name"))
            })?
            .ok_or_else(|| Failure::usage("no project was picked; nothing was linked"))?
        }
    };
    let project_id = text(&project, "id");
    let app = match &app_ref {
        Some(reference) => Some(scope.app(reference, Some(&project_id)).await?),
        None if project_ref.is_none() => {
            let apps = scope
                .list_all(&format!("{}?project_id={project_id}", LIST_APPS.path))
                .await?;
            if apps.is_empty() {
                None
            } else {
                pick(environment, stderr, "app", &apps, true, |a| text(a, "name"))?
            }
        }
        None => None,
    };

    let mut file = Links::read(environment)?;
    file.set(
        &cwd,
        Link {
            context: context.clone(),
            project_id,
            app_id: app.as_ref().map(|a| text(a, "id")),
        },
    )?;
    file.write()?;
    let what = match &app {
        Some(app) => format!(
            "the project {} and the app {}",
            text(&project, "slug"),
            text(app, "name")
        ),
        None => format!("the project {}", text(&project, "slug")),
    };
    crate::api::write(
        stdout,
        format!("Linked {} to {what} on {context}.\n", cwd.display()).as_bytes(),
    )
}

/// Offers `items` by number on the terminal; `optional` lets Enter pick none.
fn pick(
    environment: &Environment,
    stderr: &mut dyn Write,
    what: &str,
    items: &[Value],
    optional: bool,
    label: impl Fn(&Value) -> String,
) -> Result<Option<Value>, Failure> {
    for (index, item) in items.iter().enumerate() {
        let _ = writeln!(stderr, "  {}) {}", index + 1, label(item));
    }
    let _ = if optional {
        write!(
            stderr,
            "Pick an {what} (1-{}, Enter for none): ",
            items.len()
        )
    } else {
        write!(stderr, "Pick a {what} (1-{}): ", items.len())
    };
    let _ = stderr.flush();
    let answer = environment
        .read_line()
        .map_err(|e| Failure::usage(format!("cannot read the answer: {e}")))?;
    let answer = answer.trim();
    if answer.is_empty() && optional {
        return Ok(None);
    }
    match answer.parse::<usize>() {
        Ok(number) if (1..=items.len()).contains(&number) => Ok(Some(items[number - 1].clone())),
        _ => Err(Failure::usage(format!(
            "{answer:?} is not a number from the list; nothing was linked"
        ))),
    }
}

pub fn unlink(environment: &Environment, stdout: &mut dyn Write) -> Result<(), Failure> {
    let cwd = links::current_dir(environment)?;
    let mut file = Links::read(environment)?;
    let Some((dir, _)) = file.nearest(&cwd) else {
        return crate::api::write(
            stdout,
            format!("{} is not linked; nothing to remove.\n", cwd.display()).as_bytes(),
        );
    };
    let link = file.remove(&dir);
    file.write()?;
    let project = link.map(|link| link.project_id).unwrap_or_default();
    crate::api::write(
        stdout,
        format!("Unlinked {} (it was linked to {project}).\n", dir.display()).as_bytes(),
    )
}

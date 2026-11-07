//! `quoro project list | create NAME [--slug SLUG] | delete PROJECT`.
//!
//! `create` links the current folder to the new project when the folder is
//! not linked yet, so `quoro app create` and `quoro status` work in it at
//! once. `delete` is destructive: a human types the project's slug on a
//! terminal or passes `--yes`, and an agent's call goes through the approval
//! flags. The primary refuses a project that still holds apps.

use crate::Environment;
use crate::cli::{Global, ProjectCommand};
use crate::error::{Failure, exit};
use crate::links::{self, Link, Links};
use crate::operations::{CREATE_PROJECT, DELETE_PROJECT, LIST_PROJECTS};
use crate::scope::{self, Scope, text};
use reqwest::Method;
use serde_json::{Value, json};
use std::io::Write;

pub async fn run(
    global: &Global,
    command: ProjectCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        ProjectCommand::List => list(global, environment, stdout).await,
        ProjectCommand::Create { name, slug } => {
            create(global, environment, &name, slug, stdout, stderr).await
        }
        ProjectCommand::Delete {
            project_ref,
            approval,
        } => {
            let mut scope = Scope::open(global, environment, "project delete", true)?;
            scope::can_confirm(global, environment, scope.is_agent, "a project")?;
            let project = scope.project(&project_ref).await?;
            let slug = text(&project, "slug");
            let id = text(&project, "id");
            scope::confirm_name(
                global,
                environment,
                scope.is_agent,
                &format!("the project {slug}"),
                &slug,
                stderr,
            )?;
            let call = scope.change(
                Method::DELETE,
                DELETE_PROJECT.path.replace("{project_id}", &id),
                None,
            )?;
            let answer = scope
                .send_destructive(&call, &approval, global.json, stdout, stderr)
                .await?;
            if answer.status == 405 {
                return Err(Failure {
                    request_id: answer.request_id.clone(),
                    ..Failure::other(
                        "this primary does not implement deleteProject yet (HTTP 405); \
                         nothing was deleted",
                    )
                });
            }
            scope::report(&answer, global.json, stdout, |_| {
                format!("Deleted the project {slug} ({id}).\n")
            })?;
            if let Some(context) = &scope.context_name {
                let mut links = Links::read(environment)?;
                if links.forget_project(context, &id) > 0 {
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
    let mut scope = Scope::open(global, environment, "project list", false)?;
    let projects = scope.list_all(LIST_PROJECTS.path).await?;
    if global.json {
        let body = json!({ "items": projects, "next_cursor": null });
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    if projects.is_empty() {
        return crate::api::write(
            stdout,
            b"No projects. Create one with quoro project create NAME.\n",
        );
    }
    let linked = scope.link.as_ref().map(|(_, link)| link.project_id.clone());
    let mut rows = vec![vec![
        String::new(),
        "SLUG".to_string(),
        "NAME".to_string(),
        "ID".to_string(),
    ]];
    for project in &projects {
        let id = text(project, "id");
        rows.push(vec![
            if linked.as_deref() == Some(id.as_str()) {
                "*".to_string()
            } else {
                String::new()
            },
            text(project, "slug"),
            text(project, "name"),
            id,
        ]);
    }
    let mut out = scope::table(&rows);
    if linked.is_some() {
        out.push_str("* linked to this folder\n");
    }
    crate::api::write(stdout, out.as_bytes())
}

async fn create(
    global: &Global,
    environment: &Environment,
    name: &str,
    slug: Option<String>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 200 {
        return Err(Failure::usage("a project's name has 1 to 200 characters"));
    }
    let slug = slug.unwrap_or_else(|| slugify(name));
    if !valid_slug(&slug) {
        return Err(Failure::usage(format!(
            "the slug {slug:?} must be 1 to 40 of a-z, 0-9 and -; pass --slug"
        )));
    }
    let mut scope = Scope::open(global, environment, "project create", true)?;
    let call = scope.change(
        Method::POST,
        CREATE_PROJECT.path.to_string(),
        Some(&json!({ "slug": slug, "name": name })),
    )?;
    let answer = scope.send(&call).await?;
    scope::report(&answer, global.json, stdout, |project: &Value| {
        format!(
            "Created the project {} ({}).\n",
            text(project, "slug"),
            text(project, "id")
        )
    })?;
    let project: Value = serde_json::from_slice(&answer.body).unwrap_or(Value::Null);
    let note = link_folder(&scope, environment, &text(&project, "id"))?;
    if let Some(note) = note {
        if global.json {
            let _ = write!(stderr, "quoro: {note}");
        } else {
            crate::api::write(stdout, note.as_bytes())?;
        }
    }
    Ok(())
}

/// Links the current folder to the new project when it is not linked yet,
/// on a named context.
fn link_folder(
    scope: &Scope,
    environment: &Environment,
    project_id: &str,
) -> Result<Option<String>, Failure> {
    let (Some(context), Ok(cwd)) = (&scope.context_name, links::current_dir(environment)) else {
        return Ok(None);
    };
    let mut links = Links::read(environment)?;
    if links.nearest(&cwd).is_some() {
        return Ok(None);
    }
    links.set(
        &cwd,
        Link {
            context: context.clone(),
            project_id: project_id.to_string(),
            app_id: None,
        },
    )?;
    links.write().map_err(|mut failure| {
        failure.code = exit::OTHER;
        failure.message = format!("the project was created, but {}", failure.message);
        failure
    })?;
    Ok(Some(format!(
        "Linked {} to it; quoro app create NAME adds an app.\n",
        cwd.display()
    )))
}

/// `My Blog!` → `my-blog`.
fn slugify(name: &str) -> String {
    let mut slug = String::new();
    for c in name.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug: String = slug.trim_end_matches('-').chars().take(40).collect();
    slug.trim_end_matches('-').to_string()
}

fn valid_slug(slug: &str) -> bool {
    (1..=40).contains(&slug.len())
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slug_comes_from_the_name() {
        assert_eq!(slugify("My Blog!"), "my-blog");
        assert_eq!(slugify("  Ops -- Team 2 "), "ops-team-2");
        assert_eq!(slugify("Ünïcode"), "n-code");
        assert_eq!(slugify("!!!"), "");
        assert!(valid_slug(&slugify(&"a b".repeat(30))));
        assert!(!valid_slug(""));
        assert!(!valid_slug("Upper"));
    }
}

//! `quoro status [--cluster]`: what the linked folder runs, at a glance.
//!
//! For an app (the link's, or `--app`): its project; its source (image or Git
//! repository and ref), replicas and resources; its aggregated status and
//! each instance's desired and observed state; the last five deployments with
//! their ids; its domains with their certificate and serving status; the
//! newest metrics sample of each instance; the count of open vulnerabilities
//! of its image; and the panel URL of the app (`/apps/{app_id}`).
//!
//! For a project (a link without an app, or `--project`): the project, its
//! apps and the panel URL (the panel lists apps at `/`).
//!
//! `--cluster` adds the cluster (name, version, primary, term, voters) and its
//! nodes with their roles and connection state, and works in a folder that is
//! not linked. The N+1 capacity check (`getClusterCapacity`) is left out: the
//! primary does not implement it yet; it joins this view once it does.
//!
//! Metrics and vulnerabilities are read from the nodes and the primary's
//! vulnerability data, which may be unavailable (an unreachable node, no
//! inventory yet). Such a section says why and the rest is still shown.
//! `--json` prints one object holding the API answers unchanged under
//! `project`, `app`, `apps`, `deployments`, `metrics`, `vulnerabilities`,
//! `cluster` and `nodes`, with `panel_url` and, for a section that could not
//! be read, its error under `errors`.

use crate::Environment;
use crate::app::{self, settings, source};
use crate::cli::{Global, StatusArgs};
use crate::error::Failure;
use crate::operations::{
    GET_APP_METRICS, GET_APP_VULNERABILITIES, GET_CLUSTER, GET_PROJECT, LIST_APP_DEPLOYMENTS,
    LIST_APPS, LIST_NODES,
};
use crate::scope::{self, Scope, text};
use serde_json::{Map, Value, json};
use std::fmt::Write as _;
use std::io::Write;

/// How many deployments are shown.
const DEPLOYMENTS: usize = 5;

pub async fn run(
    global: &Global,
    args: StatusArgs,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "status", false)?;
    let linked_app = scope
        .link
        .as_ref()
        .is_some_and(|(_, link)| link.app_id.is_some());
    let wants_app = global.app.is_some() || (global.project.is_none() && linked_app);
    let wants_project = !wants_app && (global.project.is_some() || scope.link.is_some());
    if !wants_app && !wants_project && !args.cluster {
        return Err(Failure::usage(
            "this folder is not linked: run quoro link PROJECT [APP], pass --project or --app, \
             or pass --cluster",
        ));
    }
    let mut view = View {
        json: Map::new(),
        errors: Map::new(),
        text: format!(
            "context   {} ({})\n",
            scope
                .context_name
                .clone()
                .unwrap_or_else(|| "(ad hoc, from QUORO_URL)".to_string()),
            scope.url()
        ),
    };
    if wants_app {
        app_status(&mut scope, global, &mut view).await?;
    } else if wants_project {
        project_status(&mut scope, global, &mut view).await?;
    }
    if args.cluster {
        cluster_status(&mut scope, &mut view).await?;
    }
    if global.json {
        if !view.errors.is_empty() {
            view.json
                .insert("errors".to_string(), Value::Object(view.errors));
        }
        let body = Value::Object(view.json);
        return crate::api::write(stdout, format!("{body}\n").as_bytes());
    }
    crate::api::write(stdout, view.text.as_bytes())
}

struct View {
    json: Map<String, Value>,
    errors: Map<String, Value>,
    text: String,
}

impl View {
    fn line(&mut self, label: &str, value: impl std::fmt::Display) {
        let _ = writeln!(self.text, "{label:<10}{value}");
    }
}

/// A read that may fail without failing the command: the body, or the
/// failure's message. An unreachable primary still fails it.
async fn optional(scope: &mut Scope, path: &str) -> Result<Result<Value, String>, Failure> {
    let answer = scope.answer(path).await?;
    if !answer.is_success() {
        return Ok(Err(answer.failure().message));
    }
    Ok(answer.json().map_err(|failure| failure.message))
}

async fn app_status(scope: &mut Scope, global: &Global, view: &mut View) -> Result<(), Failure> {
    let found = scope.required_app(global).await?;
    let id = text(&found, "id");
    // The read of one app carries its instances and domains.
    let app = scope.get(&app::app_path(&id)).await?;
    let project = scope
        .get(
            &GET_PROJECT
                .path
                .replace("{project_id}", &text(&app, "project_id")),
        )
        .await?;
    let deployments = scope
        .get(&format!(
            "{}?limit={DEPLOYMENTS}",
            LIST_APP_DEPLOYMENTS.path.replace("{app_id}", &id)
        ))
        .await?;
    let metrics = optional(scope, &GET_APP_METRICS.path.replace("{app_id}", &id)).await?;
    let vulnerabilities = optional(
        scope,
        &format!(
            "{}?limit=1",
            GET_APP_VULNERABILITIES.path.replace("{app_id}", &id)
        ),
    )
    .await?;
    let panel = format!("{}/apps/{id}", scope.url());

    view.line(
        "project",
        format!("{} ({})", text(&project, "slug"), text(&project, "id")),
    );
    view.line(
        "app",
        format!("{} ({}), {}", text(&app, "name"), id, text(&app, "status")),
    );
    view.line("source", source(&app));
    view.line("settings", settings(&app));
    let instances = items(&app["instances"]);
    if instances.is_empty() {
        view.line("instances", "none");
    }
    for (index, vm) in instances.iter().enumerate() {
        view.line(
            if index == 0 { "instances" } else { "" },
            format!(
                "{} on {}: desired {}, observed {}",
                text(vm, "id"),
                text(vm, "node_id"),
                text(vm, "desired_status"),
                text(vm, "observed_status")
            ),
        );
    }

    let deployment_items = items(&deployments["items"]);
    if deployment_items.is_empty() {
        view.line("deploys", "none yet");
    } else {
        view.text.push_str("deploys\n");
        let rows: Vec<Vec<String>> = deployment_items
            .iter()
            .map(|d| {
                vec![
                    format!("  {}", text(d, "id")),
                    text(d, "status"),
                    text(d, "trigger"),
                    text(d, "created_at"),
                    text(d, "image"),
                ]
            })
            .collect();
        view.text.push_str(&scope::table(&rows));
    }

    let domains = items(&app["domains"]);
    if domains.is_empty() {
        view.line("domains", "none");
    } else {
        view.text.push_str("domains\n");
        let rows: Vec<Vec<String>> = domains
            .iter()
            .map(|d| {
                vec![
                    format!("  {}", text(d, "hostname")),
                    format!("cert {}", text(d, "cert_status")),
                    text(d, "serving_status"),
                ]
            })
            .collect();
        view.text.push_str(&scope::table(&rows));
    }

    match &metrics {
        Ok(metrics) => {
            let lines = metric_lines(metrics);
            if lines.is_empty() {
                view.line("metrics", "no sample: no instance is running");
            }
            for (index, line) in lines.into_iter().enumerate() {
                view.line(if index == 0 { "metrics" } else { "" }, line);
            }
        }
        Err(message) => view.line("metrics", format!("unavailable: {message}")),
    }
    match &vulnerabilities {
        Ok(report) => view.line("vulns", vulnerability_line(report)),
        Err(message) => view.line("vulns", format!("unavailable: {message}")),
    }
    view.line("panel", &panel);

    view.json.insert("project".to_string(), project);
    view.json.insert("app".to_string(), app);
    view.json.insert("deployments".to_string(), deployments);
    for (key, section) in [("metrics", metrics), ("vulnerabilities", vulnerabilities)] {
        match section {
            Ok(body) => {
                view.json.insert(key.to_string(), body);
            }
            Err(message) => {
                view.json.insert(key.to_string(), Value::Null);
                view.errors.insert(key.to_string(), json!(message));
            }
        }
    }
    view.json.insert("panel_url".to_string(), json!(panel));
    Ok(())
}

/// The newest sample of each instance.
fn metric_lines(metrics: &Value) -> Vec<String> {
    items(&metrics["instances"])
        .iter()
        .filter_map(|instance| {
            let sample = items(&instance["samples"]).last()?.clone();
            let cpu = sample["cpu_usage"].as_f64().unwrap_or(0.0) * 100.0;
            let mib = |key: &str| sample[key].as_u64().unwrap_or(0) / (1024 * 1024);
            Some(format!(
                "{}: cpu {cpu:.0}% of {} vCPU, memory {} MiB of {} MiB at {}",
                text(instance, "vm_id"),
                text(&sample, "vcpus"),
                mib("memory_rss_bytes"),
                mib("memory_bytes"),
                text(&sample, "time")
            ))
        })
        .collect()
}

/// `3 open (1 critical, 2 high), 0 in KEV`, or why there is no count.
fn vulnerability_line(report: &Value) -> String {
    let status = text(&report["inventory"], "status");
    if status != "inventoried" {
        return format!("no count: the image inventory is {status}");
    }
    let counts = &report["counts"];
    let severities = ["critical", "high", "medium", "low", "unknown"];
    let open: u64 = severities
        .iter()
        .map(|s| counts[*s].as_u64().unwrap_or(0))
        .sum();
    let parts: Vec<String> = severities
        .iter()
        .filter_map(|s| {
            let n = counts[*s].as_u64().unwrap_or(0);
            (n > 0).then(|| format!("{n} {s}"))
        })
        .collect();
    let kev = counts["kev"].as_u64().unwrap_or(0);
    if parts.is_empty() {
        format!("{open} open, {kev} in KEV")
    } else {
        format!("{open} open ({}), {kev} in KEV", parts.join(", "))
    }
}

async fn project_status(
    scope: &mut Scope,
    global: &Global,
    view: &mut View,
) -> Result<(), Failure> {
    let project = scope.required_project(global).await?;
    let id = text(&project, "id");
    let apps = scope
        .list_all(&format!("{}?project_id={id}", LIST_APPS.path))
        .await?;
    let panel = format!("{}/", scope.url());
    view.line(
        "project",
        format!(
            "{} ({}), {}",
            text(&project, "slug"),
            id,
            text(&project, "name")
        ),
    );
    if apps.is_empty() {
        view.line("apps", "none yet: quoro app create NAME --image REF");
    } else {
        view.text.push_str("apps\n");
        let rows: Vec<Vec<String>> = apps
            .iter()
            .map(|a| {
                vec![
                    format!("  {}", text(a, "name")),
                    text(a, "id"),
                    text(a, "status"),
                    format!("replicas {}", text(a, "replicas")),
                    source(a),
                ]
            })
            .collect();
        view.text.push_str(&scope::table(&rows));
    }
    view.line("panel", &panel);
    view.json.insert("project".to_string(), project);
    view.json.insert(
        "apps".to_string(),
        json!({ "items": apps, "next_cursor": null }),
    );
    view.json.insert("panel_url".to_string(), json!(panel));
    Ok(())
}

async fn cluster_status(scope: &mut Scope, view: &mut View) -> Result<(), Failure> {
    let cluster = scope.get(GET_CLUSTER.path).await?;
    let nodes = scope.list_all(LIST_NODES.path).await?;
    let primary_id = text(&cluster, "primary_node_id");
    let primary_name = nodes
        .iter()
        .find(|n| n["id"] == primary_id.as_str())
        .map_or_else(|| "?".to_string(), |n| text(n, "name"));
    view.line(
        "cluster",
        format!(
            "{} ({}), version {}",
            text(&cluster, "name"),
            text(&cluster, "id"),
            text(&cluster, "version")
        ),
    );
    view.line(
        "primary",
        format!(
            "{primary_name} ({primary_id}), term {}",
            text(&cluster, "term")
        ),
    );
    let votes = &cluster["votes"];
    view.line(
        "votes",
        format!(
            "{} of {} needed, HA {}",
            text(votes, "count"),
            text(votes, "required"),
            if votes["ha_ready"] == true {
                "ready"
            } else {
                "not ready"
            }
        ),
    );
    if nodes.is_empty() {
        view.line("nodes", "none");
    } else {
        view.text.push_str("nodes\n");
        let rows: Vec<Vec<String>> = nodes
            .iter()
            .map(|n| {
                let roles: Vec<&str> = items(&n["roles"])
                    .iter()
                    .filter_map(Value::as_str)
                    .collect();
                vec![
                    format!("  {}", text(n, "name")),
                    text(n, "id"),
                    roles.join(","),
                    text(n, "status"),
                    n["cp_status"].as_str().unwrap_or("").to_string(),
                ]
            })
            .collect();
        view.text.push_str(&scope::table(&rows));
    }
    view.json.insert("cluster".to_string(), cluster);
    view.json.insert(
        "nodes".to_string(),
        json!({ "items": nodes, "next_cursor": null }),
    );
    Ok(())
}

fn items(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_vulnerability_count_needs_an_inventory() {
        let pending = json!({ "inventory": { "status": "pending" }, "counts": {} });
        assert_eq!(
            vulnerability_line(&pending),
            "no count: the image inventory is pending"
        );
        let report = json!({
            "inventory": { "status": "inventoried" },
            "counts": { "critical": 1, "high": 2, "medium": 0, "low": 0, "unknown": 0, "kev": 1 },
        });
        assert_eq!(
            vulnerability_line(&report),
            "3 open (1 critical, 2 high), 1 in KEV"
        );
    }

    #[test]
    fn a_metrics_line_is_the_newest_sample() {
        let metrics = json!({ "instances": [
            { "vm_id": "vm_1", "node_id": "nod_1", "samples": [
                { "time": "t1", "cpu_usage": 0.5, "vcpus": 2, "memory_rss_bytes": 0, "memory_bytes": 0 },
                { "time": "t2", "cpu_usage": 0.25, "vcpus": 2,
                  "memory_rss_bytes": 104_857_600, "memory_bytes": 536_870_912 },
            ] },
            { "vm_id": "vm_2", "node_id": "nod_1", "samples": [] },
        ] });
        assert_eq!(
            metric_lines(&metrics),
            ["vm_1: cpu 25% of 2 vCPU, memory 100 MiB of 512 MiB at t2"]
        );
    }
}

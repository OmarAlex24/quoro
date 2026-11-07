//! `quoro vm list | show VM | create NAME --image --node --vcpus --memory
//! [--disk] | start VM | stop VM | delete VM | console VM`.
//!
//! A VM belongs to a project (`Vm.project_id`): `list` shows the VMs of
//! `--project` or the linked project, else every VM the caller may read, and
//! `create` needs a project. A VM is named by its id (`vm_...`) or its name,
//! looked up in that project when one is known; names are not unique, so a
//! name that matches more than one VM is refused with their ids. A deleted VM
//! is never matched by name. An app's instances are VMs too.
//!
//! - `create` places the VM on `--node` (a node id, or a name read with
//!   `listNodes`) with a fresh `Idempotency-Key`; `observed_status` then
//!   follows through `creating` as the node reports it.
//! - `start` and `stop` set the desired status; the node follows.
//! - `delete` deletes the VM and its disks (`vm:delete`, destructive): a human
//!   types the VM's name on a terminal or passes `--yes`, and an agent's call
//!   goes through the approval flags (`--request-approval`, `--wait`).
//! - `console` follows the VM's serial console, read-only, after replaying
//!   its recent output (`createConsoleSession`, ADR 0116), until Ctrl-C. It
//!   sends nothing, so the primary closes it after 15 minutes
//!   (`idle_timeout`); an agent's session also ends with its 5-minute access
//!   token (permission-model.md section 7.5). Its output may go to a file or a
//!   pipe.

use crate::Environment;
use crate::cli::{Global, VmCommand, VmCreateArgs};
use crate::client::idempotency_key;
use crate::error::{Failure, exit};
use crate::operations::{
    CREATE_CONSOLE_SESSION, CREATE_VM, DELETE_VM, GET_VM, LIST_VMS, START_VM, STOP_VM,
};
use crate::scope::{self, Confirm, Scope, is_id, text};
use crate::session::{self, Kind};
use reqwest::Method;
use serde_json::{Value, json};
use std::io::Write;

/// The VMs `list` shows; more are paged with `quoro api`.
const PAGE: usize = 100;

pub async fn run(
    global: &Global,
    command: VmCommand,
    environment: &Environment,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    match command {
        VmCommand::List => list(global, environment, stdout).await,
        VmCommand::Show { vm } => {
            let mut scope = Scope::open(global, environment, "vm show", false)?;
            let vm = resolve(&mut scope, global, &vm).await?;
            if global.json {
                return crate::api::write(stdout, format!("{vm}\n").as_bytes());
            }
            crate::api::write(stdout, describe(&vm).as_bytes())
        }
        VmCommand::Create(args) => create(global, environment, args, stdout).await,
        VmCommand::Start { vm } => set_status(global, environment, &vm, "start", stdout).await,
        VmCommand::Stop { vm } => set_status(global, environment, &vm, "stop", stdout).await,
        VmCommand::Delete { vm, approval } => {
            let mut scope = Scope::open(global, environment, "vm delete", true)?;
            scope::can_confirm(global, environment, scope.is_agent, "a VM")?;
            let vm = resolve(&mut scope, global, &vm).await?;
            let (id, name) = (text(&vm, "id"), text(&vm, "name"));
            scope::confirm(
                global,
                environment,
                scope.is_agent,
                &Confirm {
                    change: "deleting a VM",
                    warning: &format!(
                        "This deletes the VM {name} ({id}) on the node {} and its disks for good",
                        text(&vm, "node_id")
                    ),
                    label: "its name",
                    expected: &name,
                    nothing: "nothing was deleted",
                },
                stderr,
            )?;
            let call =
                scope.change(Method::DELETE, DELETE_VM.path.replace("{vm_id}", &id), None)?;
            let answer = scope
                .send_destructive(&call, &approval, global.json, stdout, stderr)
                .await?;
            scope::report(&answer, global.json, stdout, |_| {
                format!("Deleted the VM {name} ({id}); its node stops it and removes its disks.\n")
            })
        }
        VmCommand::Console { vm } => console(global, environment, &vm, stdout, stderr).await,
    }
}

async fn list(
    global: &Global,
    environment: &Environment,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, "vm list", false)?;
    let project = scope.project_in_scope(global).await?;
    let mut path = format!("{}?limit={PAGE}", LIST_VMS.path);
    if let Some(project) = &project {
        path.push_str(&format!("&project_id={}", text(project, "id")));
    }
    let answer = scope.answer(&path).await?;
    if global.json {
        return scope::report(&answer, true, stdout, |_| String::new());
    }
    let page = answer.json()?;
    let vms = page["items"].as_array().cloned().unwrap_or_default();
    if vms.is_empty() {
        let line = match &project {
            Some(project) => format!(
                "The project {} has no VMs; create one with quoro vm create NAME --image REF \
                 --node NODE --vcpus N --memory MIB.\n",
                text(project, "name")
            ),
            None => "No VMs.\n".to_string(),
        };
        return crate::api::write(stdout, line.as_bytes());
    }
    let mut rows = vec![
        [
            "NAME", "ID", "STATUS", "DESIRED", "NODE", "VCPUS", "MEMORY", "APP",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
    ];
    for vm in &vms {
        rows.push(vec![
            text(vm, "name"),
            text(vm, "id"),
            text(vm, "observed_status"),
            text(vm, "desired_status"),
            text(vm, "node_id"),
            text(vm, "vcpus"),
            format!("{} MiB", text(vm, "memory_mb")),
            text(vm, "app_id"),
        ]);
    }
    let mut out = scope::table(&rows);
    if page["next_cursor"].as_str().is_some_and(|c| !c.is_empty()) {
        out.push_str(&format!(
            "The first {PAGE} VMs; quoro api GET '{path}&cursor=...' pages further.\n"
        ));
    }
    crate::api::write(stdout, out.as_bytes())
}

/// `vm show`: the VM, one field a line.
fn describe(vm: &Value) -> String {
    let mut out = String::new();
    let fields = [
        ("Name", text(vm, "name")),
        ("ID", text(vm, "id")),
        (
            "Status",
            format!(
                "{} (desired {})",
                text(vm, "observed_status"),
                text(vm, "desired_status")
            ),
        ),
        ("Project", text(vm, "project_id")),
        ("App", text(vm, "app_id")),
        ("Node", text(vm, "node_id")),
        (
            "Size",
            format!(
                "{} vCPUs, {} MiB memory, {} GiB disk",
                text(vm, "vcpus"),
                text(vm, "memory_mb"),
                text(vm, "disk_gb")
            ),
        ),
        ("Image", text(vm, "image")),
        ("IP", text(vm, "ip")),
        (
            "Restarts",
            format!(
                "{} (last exit code {})",
                text(vm, "restart_count"),
                text(vm, "last_exit_code")
            ),
        ),
        ("Created", text(vm, "created_at")),
        ("Updated", text(vm, "updated_at")),
    ];
    for (label, value) in fields {
        out.push_str(&format!("{:<10}{value}\n", format!("{label}:")));
    }
    out
}

async fn create(
    global: &Global,
    environment: &Environment,
    args: VmCreateArgs,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let name_len = args.name.chars().count();
    if !(1..=200).contains(&name_len) || args.name.trim().is_empty() {
        return Err(Failure::usage(
            "a VM's name has 1 to 200 characters".to_string(),
        ));
    }
    if args.image.trim().is_empty() {
        return Err(Failure::usage("--image must name an OCI image"));
    }
    let mut scope = Scope::open(global, environment, "vm create", true)?;
    let project = scope.required_project(global).await?;
    let node_id = scope.node_id(&args.node).await?;
    let body = json!({
        "project_id": text(&project, "id"),
        "node_id": node_id,
        "name": args.name,
        "vcpus": args.vcpus,
        "memory_mb": args.memory,
        "disk_gb": args.disk,
        "image": args.image,
    });
    let mut call = scope.change(Method::POST, CREATE_VM.path.to_string(), Some(&body))?;
    call.headers.insert("idempotency-key", idempotency_key());
    let answer = scope.send(&call).await?;
    let project_name = text(&project, "name");
    scope::report(&answer, global.json, stdout, |vm| {
        format!(
            "Created the VM {} ({}) in the project {project_name} on the node {}; it is {}.\n",
            text(vm, "name"),
            text(vm, "id"),
            text(vm, "node_id"),
            text(vm, "observed_status")
        )
    })
}

/// `vm start` and `vm stop`.
async fn set_status(
    global: &Global,
    environment: &Environment,
    reference: &str,
    verb: &str,
    stdout: &mut dyn Write,
) -> Result<(), Failure> {
    let mut scope = Scope::open(global, environment, &format!("vm {verb}"), true)?;
    let vm = resolve(&mut scope, global, reference).await?;
    let (id, name) = (text(&vm, "id"), text(&vm, "name"));
    let operation = if verb == "start" { START_VM } else { STOP_VM };
    let call = scope.change(Method::POST, operation.path.replace("{vm_id}", &id), None)?;
    let answer = scope.send(&call).await?;
    scope::report(&answer, global.json, stdout, |vm| {
        format!(
            "{} the VM {name} ({id}): it should be {}, and it is {}.\n",
            if verb == "start" {
                "Starting"
            } else {
                "Stopping"
            },
            text(vm, "desired_status"),
            text(vm, "observed_status")
        )
    })
}

async fn console(
    global: &Global,
    environment: &Environment,
    reference: &str,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Failure> {
    // The open is audited and, for an agent, carries its reason.
    let mut scope = Scope::open(global, environment, "vm console", true)?;
    let vm = resolve(&mut scope, global, reference).await?;
    let id = text(&vm, "id");
    let call = scope.change(
        Method::POST,
        CREATE_CONSOLE_SESSION.path.replace("{vm_id}", &id),
        Some(&json!({})),
    )?;
    let ticket = scope.send(&call).await?.json().map_err(|mut failure| {
        if failure.code == exit::CONFLICT {
            failure.message = format!(
                "{}; a VM created before the console existed gets one when it is created again \
                 (an app: quoro redeploy)",
                failure.message
            );
        }
        failure
    })?;
    if environment.stdout_is_terminal {
        let _ = writeln!(
            stderr,
            "quoro: the console of {} ({id}), read-only; Ctrl-C leaves",
            text(&vm, "name")
        );
    }
    session::attach(
        &scope.session.context,
        &ticket,
        Kind::Console,
        environment,
        stdout,
        stderr,
    )
    .await
}

/// The VM `reference` names: its id, or its name in the project in scope
/// (every VM the caller may read when there is none).
pub async fn resolve(
    scope: &mut Scope,
    global: &Global,
    reference: &str,
) -> Result<Value, Failure> {
    if is_id("vm_", reference) {
        return scope.get(&GET_VM.path.replace("{vm_id}", reference)).await;
    }
    let project = scope.project_in_scope(global).await?;
    let path = match &project {
        Some(project) => format!("{}?project_id={}", LIST_VMS.path, text(project, "id")),
        None => LIST_VMS.path.to_string(),
    };
    let vms = scope.list_all(&path).await?;
    let named: Vec<&Value> = vms
        .iter()
        .filter(|vm| vm["name"] == reference && vm["observed_status"] != "deleted")
        .collect();
    match named.as_slice() {
        [vm] => Ok((*vm).clone()),
        [] => Err(Failure::new(
            exit::NOT_FOUND,
            match &project {
                Some(project) => format!(
                    "no VM has the id or name '{reference}' in the project {}; quoro vm list shows them",
                    text(project, "name")
                ),
                None => format!("no VM has the id or name '{reference}'; quoro vm list shows them"),
            },
        )),
        several => Err(Failure::usage(format!(
            "the name '{reference}' matches {} VMs: {}; name one by its id",
            several.len(),
            several
                .iter()
                .map(|vm| format!(
                    "{} ({}, project {})",
                    text(vm, "id"),
                    text(vm, "observed_status"),
                    text(vm, "project_id")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

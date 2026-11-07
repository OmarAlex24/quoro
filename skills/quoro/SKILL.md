---
name: quoro
description: Operate a Quoro cluster as an AI agent with the quoro CLI and the quoro MCP server. Deploy images and Git apps, roll back, read logs, debug failed deploys, scale, run VMs and act on incidents under scoped, time-limited grants and human approval. Use when a task mentions Quoro or a Quoro project, app, VM, deployment, build, incident or approval, or when the quoro MCP tools are available.
---

# Quoro

Quoro runs one microVM per app instance on a cluster of machines. You act on it as an agent: a named identity with a human owner that holds only the grants a human gave it. Every call is checked against those grants on the primary and written to an append-only audit log, which humans read.

## The model

- A **cluster** is a set of **nodes** (machines). One node is the primary: the CLI and the MCP server talk to it, and it tells the other nodes what to run.
- A **project** groups apps and VMs.
- An **app** runs as one microVM per instance, booted from an OCI image. It is not a Docker container: there is no Docker daemon, no compose file and no host to log in to.
- An **image app** runs an image you name: `quoro deploy --image REF`.
- A **Git app** builds the Dockerfile of a public Git repository on a node and deploys only what it built: `quoro up`. It cannot be given an image.
- A **deployment** (`dep_...`) is one rollout; a **build** (`bld_...`) is one image build of a Git app.
- A **VM** (`vm_...`) is a microVM you manage yourself. An app's instances are VMs too.
- An **incident** (`inc_...`) is opened by a detection. While it is open, incident grants give you the scopes to fix it.

## Start every task the same way

1. Run `quoro whoami`. It shows the context, your agent identity and your active grants: each scope, what it covers, when it expires and the uses left. Plan only calls those grants cover.
2. Find what you act on: `quoro status` in a linked folder, else `quoro project list` and `quoro app list`, then pass `--project` and `--app` (a name or an id) on each command.
3. Read before you write: look at the current state before a change and again after it.

Over MCP there is no identity tool. Start with `get_cluster` and `list_apps`; a refusal names the missing grant (`forbidden` with a reason, see [errors](references/errors.md)).

## Rules

1. **Every change carries a reason.** Pass `--reason "TEXT"` on every command that changes something; the CLI refuses an agent's change without one, before anything is sent. Over MCP, every mutating tool takes a required `justification`. Write one sentence for the human who reads the audit log: what you change and why.
2. **Name the incident.** When you act on an incident, pass `--incident inc_...` on every call for it (MCP: `incident_id`), so the call is tied to the incident and its grants apply.
3. **Destructive calls wait for a human.** Use the approval loop below. Never try to approve anything yourself.
4. **Never handle credentials.** Never ask a human for a token, an agent key or a password, and never put one on a command line. The context holds the path of your key file; the CLI mints short-lived tokens from it.
5. **Environment values are write-only.** See below.
6. **Branch on exit codes, not on text.** `0` is success; the others are in [errors](references/errors.md).
7. **Stop at human-only work** and say what the human should do in the panel.

## Destructive calls: the approval loop

Destructive scopes remove data or capacity and cannot be undone: `project:delete`, `vm:delete`, `app:delete` (it also covers deleting a volume), `backup:restore` and `app:failover`. No standing grant or incident grant can hold them. The commands are `quoro project delete`, `quoro app delete`, `quoro volume delete`, `quoro backup restore` and `quoro vm delete`.

Run the command with `--request-approval --wait`:

```sh
quoro app delete --app web --reason "Retire web; replaced by web-v2 (ticket 412)" --request-approval --wait
```

1. The primary answers `approval_required`. The CLI files an approval request for exactly this call and prints its id (`apr_...`).
2. It waits, up to an hour, while a human decides in the panel.
3. Approved: the CLI repeats the exact same call once. The approval grant matches only this call with these parameters, once, within ten minutes.
4. Denied or expired: the command exits `3` with the note. Do not retry the same call; report to the human.

Without `--wait`, the request is filed and the command exits `3`; repeat the exact same call once it is approved. Over MCP: call the tool (`delete_app`); on `approval_required` call `request_approval` with the `scope` and `target` of the answer and your justification, read `get_approval` until its status is `approved`, then repeat the exact same call with the same arguments.

Never ask the human for a code or a token to get past an approval, never change the parameters after approval (it is then a different call), and never look for another way to approve: `approval:decide` is human-only and has no command and no MCP tool.

## Human-only: it stays in the panel

No grant can give an agent these scopes; the call answers `forbidden` with reason `human_only` (exit `4`). Ask a human to do it in the panel:

- deciding approval requests (`approval:decide`);
- agents, agent keys, grants and webhooks (`agent:write`, `grant:write`, `webhook:write`): you cannot widen your own power;
- users and their roles (`user:read`, `user:write`);
- enrolling and removing nodes and changing their roles, acknowledging a fence, declaring a node lost (`node:enroll`, `node:remove`, `node:roles`, `node:fence_ack`, `node:declare_lost`);
- provider credentials, such as DNS, tunnel and object storage (`provider:manage`);
- backup repositories and their recovery key (`backup:manage`);
- cluster settings and the signing key (`cluster:manage`);
- deleting a quarantined copy of a volume (`app:delete_quarantine`);
- a shell in an app's instance (`app:terminal`): `quoro ssh` and the panel's terminal are for humans; an agent exits `4`.

## Environment variables

Values are write-only. `quoro env list` shows each variable's name and when it changed, never its value, and no command or tool reads one back. Never print, log or repeat a value, and never ask a human to paste a secret into the conversation.

`quoro env replace --file FILE` replaces the whole environment: every variable the file does not list is removed, and the old values cannot be read back. Since you cannot read the current values, do not write a file yourself unless the human gave you the complete file; otherwise ask the human to run `quoro env replace` and then roll the app out with `quoro redeploy`.

## CLI or MCP

Both reach the same service with the same grants and the same audit log.

- **MCP** for structured single calls: read the cluster, apps, deployments, logs, events, incidents and the audit log; restart and redeploy an app; resolve an incident; file and read approvals. The tools are listed in [mcp-tools](references/mcp-tools.md).
- **CLI** for everything else: deploying (`quoro deploy`, `quoro up`), following a stream (`quoro logs -f`, `quoro up`), VMs, volumes, backups, domains, environment variables, and scripts. Add `--json` to print the API's JSON. `quoro api METHOD PATH` calls any API operation that has no command.

## References

- [commands](references/commands.md): every command and flag, generated from the CLI.
- [mcp-tools](references/mcp-tools.md): every MCP tool with its endpoint and scope, generated from the contract.
- [errors](references/errors.md): exit codes, problem codes and refusal reasons, each with the next step.
- [recipes](references/recipes.md): deploy an image; deploy from Git and roll back; debug a failed deploy; act on an incident; create and operate a VM; scale an app.

# Commands

Generated from the `quoro` command tree by `crates/cli/tests/skill.rs`; do not edit. Regenerate with `QUORO_BLESS=1 cargo test -p quoro-cli --test skill`.

Host commands, which run on a cluster machine, are not listed: `quoro help host` prints them.

## Global flags

Every command accepts these.

- `--context NAME`: The context to use (default: QUORO_CONTEXT, then `current` in config.toml)
- `--project PROJECT`: The project a command acts on, by name or id (default: the folder's link)
- `--app APP`: The app a command acts on, by name or id (default: the folder's link)
- `--json`: Print the API's JSON body unchanged
- `--yes`: Confirm a destructive command without a prompt
- `--reason TEXT`: Why you make a change; sent as Quoro-Justification and audited
- `--incident INCIDENT`: The incident a change belongs to; sent as Quoro-Incident

## quoro login

Save a context and its credential: a personal API token or an agent key.

- `--token-stdin`: Read the personal API token from stdin instead of a prompt
- `--agent`: Log in as an agent, with --key-file
- `--key-file PATH`: The file holding the agent key (mode 0600); its path is saved, never the key
- `--url URL`: The primary, https://host[:port] (default: the context's)
- `--fingerprint FP`: The SHA-256 of the primary's certificate, sha256:<hex>

## quoro logout

Forget the context's stored credential.

## quoro whoami

Show the context and the identity behind its credential.

## quoro link

Link this folder to a project and, optionally, an app.

- `[PROJECT]`: The project, by slug, name or id (default: --project, else pick one on a terminal)
- `[APP]`: The app, by name or id

## quoro unlink

Remove this folder's link.

## quoro status

Show the linked app or project; --cluster adds the cluster.

- `--cluster`: Also show the cluster, its primary and its nodes

## quoro project

List, create and delete projects.

Subcommands: list, create, delete.

## quoro project list

List the projects.

## quoro project create

Create a project; links this folder when it is not linked yet.

- `NAME`: The project's display name
- `--slug SLUG`: Its slug, a-z, 0-9 and - (default: made from the name)

## quoro project delete

Delete an empty project.

- `PROJECT`: The project, by slug, name or id
- `--request-approval`: When the call needs approval, file the approval request
- `--wait`: With --request-approval: wait for the decision and repeat the call once approved

## quoro app

List, create, change, start, stop and delete apps.

Subcommands: list, create, update, start, stop, delete.

## quoro app list

List the apps of the project (all apps when none is linked).

## quoro app create

Create an app from an image or a Git repository.

- `NAME`: The app's name, a-z, 0-9 and -
- `--image REF`: The OCI image to deploy
- `--repo URL`: A public https:// Git repository to build with its Dockerfile
- `--ref REF`: The branch, tag or commit to build (default: main)
- `--dockerfile PATH`: The Dockerfile's path in the repository (default: Dockerfile)
- `--port PORT`: The port the app listens on. Default: 8080

## quoro app update

Change the app's replicas, resources, port or health check path.

- `--replicas REPLICAS`: Instances, one per node (1 to 64)
- `--vcpus VCPUS`: Virtual CPUs of each instance
- `--memory MIB`: Memory of each instance, in MiB
- `--port PORT`: The port the app listens on
- `--health-path PATH`: The HTTP path the health check probes, starting with /

## quoro app start

Start the app's instances.

## quoro app stop

Stop the app's instances.

## quoro app delete

Delete the app, its instances, domains and volumes.

- `--request-approval`: When the call needs approval, file the approval request
- `--wait`: With --request-approval: wait for the decision and repeat the call once approved

## quoro up

Build the linked Git app and follow its build log.

- `--ref REF`: Build this branch, tag or commit instead of the source's ref
- `--detach`: Start the build and return without following it

## quoro deploy

Deploy an image, or roll back to an earlier deployment.

- `--image REF`: The OCI image to deploy to an image app
- `--rollback DEPLOYMENT`: Deploy what an earlier deployment ran; a Git app rebuilds its commit

## quoro redeploy

Roll out the app's current image again.

## quoro restart

Restart the app's instances.

## quoro build

List the app's builds and cancel one.

Subcommands: list, cancel.

## quoro build list

List the app's newest builds.

## quoro build cancel

Cancel a build (default: the app's build in progress).

- `[BUILD]`: The build, by id (bld_...)

## quoro logs

Read or follow the app's, a build's or a deployment's logs.

- `-n, --lines N`: Show the last N lines (app logs: 200 by default)
- `--since T`: Only lines since a time (RFC 3339) or a duration ago (30s, 10m, 2h, 1d)
- `-f, --follow`: Follow new lines until Ctrl-C (a build: until it finishes)
- `--build [BUILD]`: A build's log (default: the app's newest build)
- `--deployment DEPLOYMENT`: A deployment's progress lines

## quoro ssh

Open a shell in the app's running instance; Ctrl-] closes it (humans only).

## quoro env

List or replace the app's environment variables.

Subcommands: list, replace.

## quoro env list

List the names of the app's variables and when they changed; never a value.

## quoro env replace

Replace all of the app's variables with those of a dotenv file.

- `--file FILE`: The file, NAME=VALUE per line (# comments, quotes), or - for stdin. Required

## quoro domain

List, add and remove the app's domains.

Subcommands: list, add, remove.

## quoro domain list

List the app's domains with their TLS and serving state.

## quoro domain add

Route a hostname to the app.

- `HOST`: The hostname, such as app.example.com
- `--tls MODE`: How TLS is provided. One of: acme, tunnel. Default: acme
- `--provider PROVIDER`: The DNS or tunnel provider, by name or id (default: the only one of its kind)

## quoro domain remove

Stop routing a hostname to the app.

- `HOST`: The hostname

## quoro volume

List, create and delete the app's volume.

Subcommands: list, create, delete.

## quoro volume list

List the app's volumes.

## quoro volume create

Create a local volume for the app; it attaches at the next start.

- `--size GB`: The size in GiB (1 to 1024). Required
- `--mount PATH`: The absolute path the app sees it at, such as /data. Required

## quoro volume delete

Delete a volume and its data.

- `VOLUME`: The volume, by id (vol_...) or by the app's mount path
- `--request-approval`: When the call needs approval, file the approval request
- `--wait`: With --request-approval: wait for the decision and repeat the call once approved

## quoro backup

List, take and restore backups of the app's volume.

Subcommands: list, create, restore.

## quoro backup list

List the newest backups of the app's volume.

## quoro backup create

Back the app's volume up now.

## quoro backup restore

Restore a backup onto a node; it replaces the app's data and moves the app.

- `BACKUP`: The backup, by id (bkp_...)
- `--node NODE`: The node to restore onto, by name or id. Required
- `--request-approval`: When the call needs approval, file the approval request
- `--wait`: With --request-approval: wait for the decision and repeat the call once approved

## quoro vm

List, create, start, stop and delete VMs, and read a VM's console.

Subcommands: list, show, create, start, stop, delete, console.

## quoro vm list

List the VMs of the project (all VMs when none is linked).

## quoro vm show

Show one VM.

- `VM`: The VM, by name or id (vm_...)

## quoro vm create

Create a VM in the project on a node.

- `NAME`: The VM's name
- `--image REF`: The OCI image the VM boots. Required
- `--node NODE`: The node to place it on, by name or id. Required
- `--vcpus VCPUS`: Virtual CPUs (1 to 255). Required
- `--memory MIB`: Memory, in MiB. Required
- `--disk GB`: The root disk, in GiB. Default: 1

## quoro vm start

Start a VM.

- `VM`: The VM, by name or id (vm_...)

## quoro vm stop

Stop a VM.

- `VM`: The VM, by name or id (vm_...)

## quoro vm delete

Delete a VM and its disks.

- `VM`: The VM, by name or id (vm_...)
- `--request-approval`: When the call needs approval, file the approval request
- `--wait`: With --request-approval: wait for the decision and repeat the call once approved

## quoro vm console

Follow a VM's serial console, read-only, until Ctrl-C.

- `VM`: The VM, by name or id (vm_...)

## quoro incident

List, read and resolve incidents.

Subcommands: list, show, resolve.

## quoro incident list

List the newest incidents.

- `--status STATUS`: Only incidents in this status. One of: open, resolved

## quoro incident show

Show an incident with its trigger event and grants.

- `INCIDENT`: The incident, by id (inc_...)

## quoro incident resolve

Resolve an incident; its incident grants stop working.

- `INCIDENT`: The incident, by id (inc_...)
- `--note TEXT`: What was done to resolve it; recorded with the incident

## quoro audit

Read the audit log.

Subcommands: list.

The offline check of an exported log or a database is the host command quoro audit verify --file PATH | --db PATH (quoro help host).

## quoro audit list

Show the 100 newest audit records, the newest at the bottom.

- `--actor ACTOR`: Only records of this actor: a user, agent or node id, or a name; without --since, its newest among the log's newest 1000 records
- `--since T`: The first 100 records from a time (RFC 3339) or a duration ago (30s, 10m, 2h, 1d)

The start of the page is found by reading single records (getAuditRecord), a few dozen at most; an agent's reads are audited, so each is a record and, under a grant with a use limit, a use.

## quoro api

Call any API operation and print the response body.

- `METHOD`: The HTTP method, such as GET or POST
- `PATH`: The path, starting with /api/v1/
- `--data @FILE|-`: The JSON request body: @FILE, or - to read it from stdin
- `--request-approval`: When the call needs approval, file the approval request
- `--wait`: With --request-approval: wait for the decision and repeat the call once approved

## quoro mcp

Connect an MCP client (Claude Code, Codex, Cursor) and teach it Quoro.

Subcommands: install.

The relay the client runs is the host command quoro mcp stdio --context NAME (quoro help host).

## quoro mcp install

Make an MCP client run quoro mcp stdio with this context, and install the skill.

- `--client CLIENT`: The MCP client to configure. One of: claude, codex, cursor. Default: claude
- `--scope SCOPE`: user: your home directory, for every project; project: this folder. One of: user, project. Default: user

## quoro completion

Print a shell completion script.

- `SHELL`: The shell to complete for. One of: bash, elvish, fish, powershell, zsh

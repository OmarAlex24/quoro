# Command-line client

The `quoro` command line operates a Quoro cluster through its primary's HTTP API. This guide is for two readers: a human operator, and someone who sets up an AI agent to operate the cluster. It covers installing the client, connecting it to a primary, logging in, linking a folder, the commands, scripting, approvals, and what stays in the panel.

Every command and flag named here exists in the client. For the full list of flags, run `quoro app --help` (or `--help` on any command), or read the generated [command reference](https://github.com/OmarAlex24/quoro/blob/main/skills/quoro/references/commands.md).

## What the client is

The client is not a separate program. It is the same `quoro` binary that runs on every cluster machine ([ADR 0054](../adr/0054-host-layout-and-cli-one-tls-listener-for-api-panel-mcp-on.md)). The binary has two kinds of commands:

- **Host commands** run on a cluster machine: `serve`, `setup-token`, `join`, `node`, `restore`, `vuln import`, `audit verify` and `mcp stdio`, and `version`. `quoro help host` lists them with their flags.
- **Client commands** are every other command line. They talk to a primary's API and to nothing else, so the primary authorizes and audits each one like any API call.

`quoro` with no arguments, or with `--help`, prints the client's help. Its last line points to `quoro help host`.

## Get the binary

Today a release publishes one build: a static `x86_64-unknown-linux-musl` binary in the release tarball, signed with a GitHub artifact attestation ([ADR 0027](../adr/0027-signed-binaries-and-reproducible-builds-are-mandatory.md)). There is no published build for macOS, for Windows or for ARM.

The client commands arrived after v0.2.0. A binary from v0.2.0 or earlier does not have them. Until a release carries them, build the binary from source.

- **On a cluster machine.** The [installer](../site/install.md) puts the binary at `/usr/local/bin/quoro`. Once the host runs a release that includes the client, the client is already there.
- **On another Linux x86_64 machine.** Download the release tarball, check it with [`packaging/verify-release.sh`](https://github.com/OmarAlex24/quoro/blob/main/packaging/verify-release.sh), and copy the `quoro` file from the tarball onto your `PATH`. It is static, so it needs no other files.
- **From source.** Clone the repository and build with the pinned toolchain:

  ```sh
  cargo build --release --locked -p quoro
  ```

  The binary is `target/release/quoro`. The client needs neither the panel nor KVM. The workspace also compiles on macOS, but CI tests only on Linux and no macOS binary is released, so a macOS build is untested.

Check the build with `quoro version`.

## Contexts and the certificate pin

A **context** names one primary and how to reach it: its URL, how its certificate is trusted, and the credential the client presents. `quoro login` writes contexts for you. You rarely edit the files by hand.

### Where the files live

The client keeps three files in `$XDG_CONFIG_HOME/quoro/`, or `~/.config/quoro/` when `XDG_CONFIG_HOME` is unset. That path is the same on macOS.

| File | Holds | Mode |
| --- | --- | --- |
| `config.toml` | The contexts and the current one. No secret. | `0644` |
| `credentials.toml` | Each context's credential: a personal API token, or the path of an agent key file. | `0600`; refused if any other user can read or write it |
| `links.toml` | Linked folders: a directory, its context, a project and, optionally, an app. No secret. | `0644` |

`config.toml` looks like this:

```toml
current = "lab"

[contexts.lab]
url = "https://primary.example.com:8443"
fingerprint = "sha256:3b1f..."
```

`credentials.toml` holds either `token = "tok_..."` or `key_file = "/home/me/.config/quoro/agent.key"` under `[contexts.lab]`, never both. It never holds an agent key itself.

### The certificate pin

A primary serves a self-signed certificate ([ADR 0054](../adr/0054-host-layout-and-cli-one-tls-listener-for-api-panel-mcp-on.md)). So a context **pins** the SHA-256 of that certificate, `sha256:<hex>`, and the client checks it before it sends anything. Names and validity dates are not checked. Only the digest is.

You find the fingerprint in three places: the installer prints it at the end of a primary install, `journalctl -u quoro` on the primary shows it, and every join command carries it.

- **With `--fingerprint`.** `quoro login --fingerprint sha256:...` pins that value.
- **First contact.** Without `--fingerprint`, a login for a new URL connects once, reads the certificate the primary presents, and stops at the handshake, so nothing is sent on that connection. On a terminal it shows the digest and asks `Trust it? [y/N]`. Compare it with the value you know. Without a terminal, the login exits 2 and prints the digest so that you can pass it with `--fingerprint`.
- **An existing context** with the same URL keeps its pin.

When the primary's certificate no longer matches the pin, every command exits 7 and names both digests. That is not a transient error. Find out why the certificate changed before you trust the new one. Then log in again with `--fingerprint sha256:...` and the new value.

A primary behind a publicly trusted certificate can use the host's CA store instead. Replace the `fingerprint` line with `tls = "system"` in `config.toml`. Only a hand edit writes that setting: `quoro login` pins every new context, and an existing context keeps the trust it has.

### Which context a command uses

1. `--context NAME`.
2. `QUORO_CONTEXT`.
3. An ad-hoc context from `QUORO_URL` and `QUORO_FINGERPRINT`, for CI. It is always pinned: `QUORO_URL` without `QUORO_FINGERPRINT` exits 2.
4. For commands that act on a project or an app, the context of the folder's link (see [Link a folder](#link-a-folder)).
5. `current` in `config.toml`.

The credential is `QUORO_TOKEN` when it is set, else the key file in `QUORO_AGENT_KEY_FILE`, else what `credentials.toml` holds for the context. An ad-hoc context has no stored credential, so it needs one of the two variables.

| Variable | Effect |
| --- | --- |
| `QUORO_CONTEXT` | The context, unless `--context` is given. |
| `QUORO_URL`, `QUORO_FINGERPRINT` | An ad-hoc context that is not saved. |
| `QUORO_TOKEN` | A bearer token that overrides the stored credential. |
| `QUORO_AGENT_KEY_FILE` | An agent key file that overrides the stored credential. |
| `QUORO_JUSTIFICATION` | The reason for a change when `--reason` is not given. |

Besides these, the client reads only `HOME`, `XDG_CONFIG_HOME`, `XDG_RUNTIME_DIR` and `XDG_CACHE_HOME`.

## Log in as a human

A human logs in with a **personal API token** (`tok_...`). Create one in the panel under Settings → API tokens. The token is shown once. The client's own messages still call that place "Your account → API tokens", and the old `/account` address leads to Settings → Account, which links to API tokens.

Personal API tokens need a primary that serves the tokens API (`/api/v1/tokens`). That API arrived after v0.2.0, so a v0.2.0 primary cannot create one.

Log in to a new primary:

```sh
quoro login --context lab --url https://primary.example.com:8443
```

The client confirms the certificate first, so you type a token only for a primary you trust. Then it asks for the token at a prompt with the terminal's echo off. Without `--context`, the login uses `QUORO_CONTEXT`, else the current context, else a context named `default`. A new context needs `--url`.

The token is never read from the command line, where any local user could see it in the process list. Without a terminal, pipe it with `--token-stdin`:

```sh
pass show quoro/lab | quoro login --context lab --url https://primary.example.com:8443 --fingerprint sha256:3b1f... --token-stdin
```

Without a terminal and without `--token-stdin`, the login exits 2 before it sends anything. An agent's credential (`atk_...` or `akey_...`) given as a token is refused the same way.

Nothing is written until the primary reads the token back as a user. The token then goes to `credentials.toml`, and the context becomes the current one. `quoro whoami` shows the context, your name, email and role.

`quoro logout` removes the context's stored credential and keeps the context, so the next `quoro login` needs no `--url`. Logging out does not revoke the token: it still works until you revoke it in the panel, and the last line of the output says where. `quoro logout` also leaves `QUORO_TOKEN` and `QUORO_AGENT_KEY_FILE` alone, since it never stored them.

## Log in as an agent

An agent is its own identity, separate from any human ([ADR 0029](../adr/0029-agents-are-first-class-identities-separate-from-humans.md)). A human creates the agent and its **agent key** (`akey_...`) in the panel under Agents. The key is shown once. The key authorizes nothing by itself: it only mints five-minute access tokens (`atk_...`), and the agent's grants decide what those tokens may do ([ADR 0030](../adr/0030-grants-scope-resource-selector-ttl-optional-max-uses.md)).

Save the key in a file that only its owner can read:

```sh
umask 077
cat > ~/.config/quoro/agent.key
chmod 600 ~/.config/quoro/agent.key
```

Paste the key, press Enter, then Ctrl-D. The file must be a regular file with mode `0600` or stricter, and hold one line that starts with `akey_`. Any other file is refused before anything is read or sent.

Then log in:

```sh
quoro login --agent --key-file ~/.config/quoro/agent.key --context lab-agent --url https://primary.example.com:8443 --fingerprint sha256:3b1f...
```

The client checks the key file, mints a token from it, and reads the agent back. Only then does it save the context and the key file's absolute path. It never saves the key itself. `QUORO_AGENT_KEY_FILE` overrides the saved path.

What happens on every later command:

- **Cached tokens.** A command mints a five-minute access token and caches it in `$XDG_RUNTIME_DIR/quoro/`, else `$XDG_CACHE_HOME/quoro/`, else `~/.cache/quoro/`. That is one file per context, mode `0600` in a `0700` directory. The token is reused until 30 seconds before it expires, and only for the same primary and the same key. When the primary answers 401 to a cached token, the client mints once more. Neither the key nor a token is ever printed.
- **A reason for every change.** An agent's command that changes something needs `--reason "TEXT"` or `QUORO_JUSTIFICATION`. Without one, it exits 2 before anything is sent, not even a mint. The reason is sent as `Quoro-Justification` and recorded in the audit log ([ADR 0031](../adr/0031-audit-log-append-only-hash-chained-optionally-copied-to-s3.md)), so write one sentence for the human who reads it.
- **Incidents.** When the agent acts on an incident, `--incident inc_...` ties each call to it, and the incident's grants apply.

`quoro whoami` shows the agent's name, model and owner, and its active grants: each grant's scopes, what it covers, when it expires and the uses left.

## Link a folder

A link tells the client which project and app a folder works on, so you do not repeat `--project` and `--app` on every command.

```sh
cd ~/src/shop-web
quoro link shop web
quoro status
```

- `quoro link PROJECT [APP]` records the current folder in `links.toml` with the context in use, the project and, optionally, the app. Nothing is written inside the folder, so a repository never carries a link.
- With no argument on a terminal, `quoro link` offers numbered lists to pick from. Without a terminal it needs the names.
- A command looks for the nearest linked folder at or above where it runs.
- `quoro project create` links the current folder to the new project when the folder is not linked yet. `quoro app create` links the new app when the folder is linked to that project without an app.
- `quoro unlink` removes the nearest link and sends nothing.
- A link belongs to its context. Its project and app apply only on that context, so ids from one cluster are never sent to another. An ad-hoc context from `QUORO_URL` cannot be linked.

### How resources are named

You name a resource by its name or by its id, never with an id flag.

| Resource | Named by |
| --- | --- |
| Project | Its id (`prj_...`), its slug or its name. |
| App | Its id (`app_...`) or its name, looked up in `--project` or the linked project when one is known. |
| VM | Its id (`vm_...`) or its name, looked up in the project when one is known. A deleted VM never matches a name. |
| Node | Its id (`nod_...`) or its name. A name needs `node:read`. |
| Volume | Its id (`vol_...`) or its mount path among the app's volumes. |
| Build, deployment, backup, incident | Their ids: `bld_...`, `dep_...`, `bkp_...`, `inc_...`. |

`--project` and `--app` override the link. `--project` without `--app` also stops the link's app from applying. A name that matches more than one resource is refused with the candidates (exit 2), never resolved by a guess: pass `--project` or the id. A name that matches nothing exits 5.

## The command tree

`quoro --help` groups the commands by topic. Each command acts on the linked app or project unless the table says otherwise.

| Topic | Command | Subcommands | What it does |
| --- | --- | --- | --- |
| Account | `quoro login` | | Save a context and its credential: a personal API token or an agent key. |
| | `quoro logout` | | Forget the context's stored credential. |
| | `quoro whoami` | | Show the context and the identity behind its credential. |
| Folder | `quoro link` | | Link this folder to a project and, optionally, an app. |
| | `quoro unlink` | | Remove this folder's link. |
| | `quoro status` | | Show the linked app or project. `--cluster` adds the cluster and its nodes. |
| Ship | `quoro up` | | Build the linked Git app and follow its build log. |
| | `quoro deploy` | | Deploy an image, or roll back to an earlier deployment. |
| | `quoro redeploy` | | Roll out the app's current image again. |
| | `quoro restart` | | Restart the app's instances. |
| | `quoro logs` | | Read or follow the app's, a build's or a deployment's logs. |
| | `quoro ssh` | | Open a shell in the app's running instance (humans only). |
| Resources | `quoro project` | list, create, delete | Manage projects. |
| | `quoro app` | list, create, update, start, stop, delete | Manage apps. |
| | `quoro env` | list, replace | List the app's variable names, or replace its environment. |
| | `quoro domain` | list, add, remove | Route hostnames to the app. |
| | `quoro volume` | list, create, delete | Manage the app's local volume. |
| | `quoro backup` | list, create, restore | Back up the app's volume and restore it. |
| | `quoro build` | list, cancel | List the app's builds and cancel one. |
| Machines | `quoro vm` | list, show, create, start, stop, delete, console | Manage VMs and read a VM's console. |
| Operate | `quoro incident` | list, show, resolve | Read and resolve incidents. |
| | `quoro audit` | list | Read the audit log. |
| Tools | `quoro api` | | Call any API operation and print the response body. |
| | `quoro completion` | | Print a shell completion script. |
| | `quoro mcp` | install | Connect an MCP client and install the agent skill. |

Every command accepts the global flags `--context`, `--project`, `--app`, `--json`, `--yes`, `--reason` and `--incident`. Run `quoro vm --help` or `quoro vm create --help` for a command's own flags.

What the client does not do yet:

- `quoro project delete` is in the tree, but the primary does not serve `deleteProject` yet ([#515](https://github.com/OmarAlex24/quoro/issues/515)). It answers HTTP 405, and the command exits 1 with nothing deleted.
- There is no command to set one environment variable. `quoro env replace` replaces the whole environment.
- `quoro up` builds the Git source the app was created with. It does not build a local folder.
- `quoro deploy --rollback` of a Git app rebuilds the commit of that deployment. It does not reuse the old image.
- `quoro status --cluster` does not show the N+1 capacity check yet, because the primary does not serve it.

## Everyday tasks

The examples run in a folder linked to the project `shop`. Elsewhere, add `--project` and `--app`. An agent adds `--reason` to every command that changes something.

### Deploy an image

Creating an app records it. Nothing runs until a deployment.

```sh
quoro app create web --image ghcr.io/acme/web:1.4.2 --port 8080
quoro deploy --image ghcr.io/acme/web:1.4.2
quoro status
```

`quoro deploy` prints the deployment's id (`dep_...`) and returns. Read the rollout's progress with `quoro logs --deployment dep_...`. Prefer an immutable tag or a digest. To go back, deploy what an earlier deployment ran:

```sh
quoro deploy --rollback dep_01JC9Z6Q5W8N3K4T7R2M1XHB0D
```

For an image app that is its image, pinned to the digest it resolved to when that is known.

### Deploy from Git and follow the build

A Git app builds the Dockerfile of a public `https://` repository on a node ([ADR 0117](../adr/0117-builder-interface-a-node-agent-builder-trait-dockerfile.md)). It cannot be given an image: `quoro deploy --image` on a Git app exits 6 and points to `quoro up`.

```sh
quoro app create api --repo https://github.com/acme/api --ref main --port 8080
quoro up
```

`quoro up` starts a build and follows its log until the build ends. It exits 0 when the build succeeded, which deploys it, and 1 when it failed or was cancelled. Ctrl-C stops following and leaves the build running. `--ref` builds another branch, tag or commit, and `--detach` returns at once. On an image app, `up` exits 6 and points to `quoro deploy --image`.

`quoro build list` shows the newest builds, and `quoro build cancel` cancels the one in progress.

### Logs

Logs stay on the node that produced them and are read on demand ([ADR 0014](../adr/0014-telemetry-app-logs-metrics-stays-on-each-node-only-state-is.md)), so an unreachable node exits 7.

```sh
quoro logs                  # the app's last 200 lines
quoro logs -n 50 --since 10m
quoro logs -f               # follow until Ctrl-C
quoro logs --build          # the newest build's log
quoro logs --build -f       # follow it until the build ends
quoro logs --deployment dep_01JC9Z6Q5W8N3K4T7R2M1XHB0D
```

`--since` takes an RFC 3339 time or a duration ago (`30s`, `10m`, `2h`, `1d`). It reads the app's output only and does not combine with `-f`.

### Environment variables

Values are write-only. `quoro env list` shows each variable's name and when it changed, never its value, and no command reads one back, `--json` included.

```sh
quoro env list
quoro env replace --file .env.production
quoro redeploy
```

`quoro env replace` replaces the **whole** environment with the variables of a dotenv file. Every variable the file does not list is removed. The format is `NAME=VALUE` per line, with optional `export`, `#` comments and single or double quotes. `--file -` reads the file from stdin. The file is checked before anything is sent, and a bad line is named by its number, never by its text. The command prints the names it removes, adds and sets again, then asks you to type the app's name. Running instances keep the old environment until the app is deployed again, so run `quoro redeploy`.

### Domains

```sh
quoro domain add shop.example.com
quoro domain add shop.example.com --tls tunnel --provider prv_01JC9Z6Q5W8N3K4T7R2M1XHB0D
quoro domain list
quoro domain remove shop.example.com
```

TLS comes from ACME by default, or from a Cloudflare Tunnel with `--tls tunnel` ([ADR 0024](../adr/0024-ingress-reverse-proxy-with-acme-for-public-ip-hosts.md)). Without `--provider`, the primary uses the only DNS or tunnel provider of that kind. A human may name a provider by its name. An agent names it by its id, because reading providers by name needs `provider:manage`, which is human-only. `quoro domain list` shows each hostname's TLS and serving state.

### Volumes and backups

```sh
quoro volume create --size 10 --mount /data
quoro backup create
quoro backup list
quoro backup restore bkp_01JC9Z6Q5W8N3K4T7R2M1XHB0D --node lab-2
```

A volume is a local disk on one node, and it makes the app stateful ([ADR 0145](../adr/0145-local-volumes-api-post-apps-app-id-volumes-size-gb-mount.md)). It attaches the next time the app starts, which `volume create` does not trigger. A backup is a copy of the app's volume in the cluster's backup repository ([ADR 0147](../adr/0147-backup-repository-format-and-keys-fastcdc-2020-chunks-256k.md)). A human sets up that repository in the panel. `quoro backup restore` restores a backup into a new volume on the node you name and moves the app there: the app stops and its current volume is detached. `volume delete` and `backup restore` cannot be undone (see [Destructive commands and approvals](#destructive-commands-and-approvals)).

### VMs and the console

A VM belongs to a project. An app's instances are VMs too.

```sh
quoro vm create worker --image ghcr.io/acme/worker:1 --node lab-1 --vcpus 2 --memory 2048 --disk 10
quoro vm list
quoro vm show worker
quoro vm stop worker
quoro vm start worker
quoro vm console worker
```

`quoro vm console` follows the VM's serial console, read-only, after it replays the recent output ([ADR 0116](../adr/0116-m4-vm-console-read-only-serial-in-socket-mode-with-the-node.md)). Ctrl-C leaves. It sends nothing, so the primary closes it after 15 minutes. An agent's console also ends with its five-minute access token.

### A shell in an app

```sh
quoro ssh --app web
```

`quoro ssh` opens `/bin/sh` in the app's running instance, at your terminal's size, in raw mode ([ADR 0113](../adr/0113-m4-web-terminal-path-exec-through-quoro-init-over-virtio.md)). Every key goes to the shell, Ctrl-C included, and Ctrl-] closes the session. The session is audited, and its timed transcript stays on the node ([ADR 0115](../adr/0115-m4-session-audit-an-open-record-plus-a-session-end-record.md)). It needs a terminal, and it is for humans only: an agent exits 4 before anything is sent ([ADR 0114](../adr/0114-m4-terminal-and-console-access-app-terminal-human-only.md)). The command exits 0 when the shell exits with 0 or you leave, 1 when the shell exits with another code or the primary closes the session, and 7 when the connection drops.

### Incidents

A detection opens an incident. While it is open, its incident grants give an agent the scopes to fix it.

```sh
quoro incident list --status open
quoro incident show inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D
quoro restart --incident inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D --reason "web stopped answering its health check"
quoro incident resolve inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D --note "Restarted web; health check passes again"
```

`show` prints the incident, the event that opened it and the grants issued for it. Resolving an incident ends its incident grants.

### The audit log

```sh
quoro audit list
quoro audit list --actor deploy-bot
quoro audit list --since 2h
```

`quoro audit list` shows the 100 newest records, the newest at the bottom. `--since` shows the first 100 from a time or a duration ago. `--actor` takes a user, agent or node id, or a name. Without `--since`, it shows that actor's newest records among the log's newest 1000. The API has no time filter, so the client finds where the page starts by reading a few dozen single records. An agent's reads are audited, so each of those reads is a record too.

`quoro audit verify --file PATH` and `quoro audit verify --db PATH` check the hash chain offline. They are host commands.

### Anything else: `quoro api`

The client keeps a small, fixed set of commands. Any other operation is one `quoro api` call:

```sh
quoro api GET /api/v1/nodes
quoro api POST /api/v1/apps/app_01JC9Z6Q5W8N3K4T7R2M1XHB0D/redeploy --reason "Pick up the new environment"
quoro api PATCH /api/v1/apps/app_01JC9Z6Q5W8N3K4T7R2M1XHB0D --data @patch.json
```

The path starts with `/api/v1/`. A request body comes from a file (`--data @FILE`) or stdin (`--data -`), never from the command line. The response body goes to stdout as the primary sent it. It is pretty-printed only on a terminal without `--json`. The operations are documented in the [API contract](../contracts/README.md).

## Output and scripting

- **stdout** carries the result: a table, a line, or with `--json` the API's JSON body unchanged. `quoro status --json` prints one object that holds each API answer under its own key. A follow (`logs -f`, `logs --build -f`) prints one JSON object per line.
- **stderr** carries prompts, progress and errors. An error is one line, `quoro: <what failed> (<code>[, <reason>])`, followed by `request id: req_...` when the primary answered. Quote the request id when you report a failure.
- **Help** (`--help`) goes to stdout and exits 0. A command line the client cannot parse exits 2.

Branch on the exit code, not on the text. The codes never change meaning:

| Exit | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | Any other failure: `unauthenticated`, `validation_failed`, `rate_limited`, `not_primary`, `internal`, a local file, or a build that `quoro up` followed and that failed or was cancelled. |
| 2 | The client refused the command line, the context or a request body before it sent anything. |
| 3 | `approval_required`: a human must approve the call first, or the approval waited for was denied or expired. |
| 4 | `forbidden`, whatever the reason, `human_only` included. Retrying the same call cannot succeed. |
| 5 | `not_found`. |
| 6 | `conflict`: the resource is not in a state that allows the call. |
| 7 | The primary could not be reached (connection, certificate pin, timeout), or the primary could not reach the node (`node_unreachable`). |

The [error reference](https://github.com/OmarAlex24/quoro/blob/main/skills/quoro/references/errors.md) lists every problem code and refusal reason with the next step.

In CI, an ad-hoc context needs no files:

```sh
export QUORO_URL=https://primary.example.com:8443
export QUORO_FINGERPRINT=sha256:3b1f...
export QUORO_TOKEN="$QUORO_CI_TOKEN"
quoro deploy --project shop --app web --image "ghcr.io/acme/web:$GIT_SHA" --json
```

Keep the token in the CI's secret store. `quoro completion bash` (or `zsh`, `fish`, `elvish`, `powershell`) prints a completion script for your shell.

## Destructive commands and approvals

Some changes cannot be undone. The client asks for confirmation before it sends them, and the primary makes an agent ask a human first.

### Humans: type the name, or `--yes`

On a terminal, the client tells you what will be lost and asks you to type a name:

| Command | You type |
| --- | --- |
| `quoro project delete` | The project's slug. |
| `quoro app delete` | The app's name. |
| `quoro volume delete` | The volume's mount path. |
| `quoro backup restore` | The app's name. |
| `quoro vm delete` | The VM's name. |
| `quoro env replace` | The app's name. |
| `quoro domain remove` | The hostname. |

`--yes` confirms without the prompt. Without a terminal and without `--yes`, the command exits 2 before it sends anything. `quoro env replace --file -` reads the file from stdin, so it always needs `--yes`.

### Agents: request an approval and wait

Destructive scopes remove data or capacity: `project:delete`, `app:delete` (which also covers deleting a volume), `vm:delete` and `backup:restore`. No standing grant and no incident grant can hold them ([ADR 0030](../adr/0030-grants-scope-resource-selector-ttl-optional-max-uses.md), [permission model](../contracts/permission-model.md), section 4.1). An agent is never asked to type a name. The primary's approval is its check instead.

The commands that can be destructive take two flags: `quoro project delete`, `quoro app delete`, `quoro volume delete`, `quoro backup restore`, `quoro vm delete` and `quoro api`.

```sh
quoro app delete --app web --reason "Retire web; replaced by web-v2" --request-approval --wait
```

1. The primary answers `approval_required` with the scope and the target the call needs.
2. With `--request-approval`, the client files an approval request for exactly this call, with these parameters, and prints its id (`apr_...`).
3. With `--wait`, it waits while a human decides in the panel, until the request expires, which is one hour after it was filed.
4. **Approved:** the client sends the exact same call again, once. The approval grant matches only this call with these parameters, once, within ten minutes.
5. **Denied or expired:** the command exits 3, with the human's note when there is one. Do not retry the same call. Report to the human.

Without `--wait`, the client prints the approval id on stdout and exits 3. Repeat the exact same command once a human has approved it. Changing a parameter makes it a different call, which the approval does not cover. `--wait` needs `--request-approval`. Without either flag, an `approval_required` answer exits 3.

The client files a request only for the operations whose parameters it can rebuild: deleting an app, a VM or a volume, and restoring a backup. For any other operation, such as one sent with `quoro api`, it exits 3 and asks you to file the request yourself with `quoro api POST /api/v1/approvals`.

No command decides an approval. Deciding needs `approval:decide`, which is human-only.

## Set up an AI agent

An agent can use the client directly, or through MCP. Both reach the same service with the same grants and the same audit log.

1. **A human creates the agent** in the panel under Agents, with its owner and model, and creates a key for it. The key is shown once.
2. **A human gives it grants** in the panel: the scopes it needs, on the resources it needs, for a limited time. Plan for the approval loop for anything destructive.
3. **On the agent's machine**, save the key file and log in with `quoro login --agent` (see [Log in as an agent](#log-in-as-an-agent)). Run `quoro whoami` to check the identity and the grants.
4. **Connect the MCP client** and install the skill:

   ```sh
   quoro mcp install --context lab-agent
   ```

### `quoro mcp install`

`quoro mcp install` makes an MCP client run `quoro mcp stdio --context NAME` for the context in use, or for `--context`, and installs the agent skill where that client looks for skills.

| `--client` | `--scope user` (default) | `--scope project` | Skill |
| --- | --- | --- | --- |
| `claude` (default) | `mcpServers` in `~/.claude.json` | `mcpServers` in `.mcp.json` | `~/.claude/skills/quoro/` or `.claude/skills/quoro/` |
| `codex` | `[mcp_servers.quoro]` in `~/.codex/config.toml` | `.codex/config.toml` | `~/.agents/skills/quoro/` or `.agents/skills/quoro/` |
| `cursor` | `mcpServers` in `~/.cursor/mcp.json` | `.cursor/mcp.json` | `~/.cursor/skills/quoro/` or `.cursor/skills/quoro/` |

The user scope writes under your home directory, for every project. The project scope writes in the current folder, in a file meant to be committed. Codex reads a project's `.codex/config.toml` only in a project you trust.

- The server entry is named `quoro`, and its command is the absolute path of the running binary. Only that entry changes. A JSON file keeps every other byte, and a TOML file keeps its comments and order. Running the command again changes nothing and says so.
- Installing another context replaces the entry and says what it ran before.
- The context must hold an agent key file and pin the primary's certificate. A context with a personal API token, with `tls = "system"`, or an ad-hoc `QUORO_URL` is refused, with the `quoro login --agent` command to run.
- A skill directory that the command did not write is refused before anything is written.

Start a new session of the MCP client to load the server and the skill.

### The relay: `quoro mcp stdio`

The MCP client runs `quoro mcp stdio --context NAME`, a host command. It speaks MCP on stdin and stdout as one agent and relays every message to the primary's `/mcp` endpoint over the pinned connection. It reads the URL, the fingerprint and the key file from the context. `--url`, `--fingerprint` and `--key-file` override them, and `QUORO_AGENT_KEY_FILE` overrides the key file. It mints and renews the five-minute access tokens itself. `quoro help host` shows its flags.

The MCP server exposes a fixed set of tools, such as `get_cluster`, `list_apps`, `restart_app`, `delete_app`, `request_approval` and `get_approval`. The [MCP tool list](https://github.com/OmarAlex24/quoro/blob/main/skills/quoro/references/mcp-tools.md) gives each tool with its endpoint and scope. Every mutating tool takes a required `justification`. Deploying, following a build, VMs, volumes, backups, domains and environment variables go through the client.

### The skill

The skill is [`skills/quoro/`](https://github.com/OmarAlex24/quoro/tree/main/skills/quoro) in the Agent Skills format, compiled into the binary, so the installed copy always matches the binary that runs the relay. It teaches an agent:

- the model: clusters, projects, apps, VMs, deployments, builds and incidents;
- to start with `quoro whoami` and plan only the calls its grants cover;
- to pass `--reason` on every change and `--incident` for an incident;
- the approval loop, and never to look for another way past it;
- what stays human-only;
- that environment values are write-only;
- when to use the client and when to use MCP.

It also carries recipes: deploy an image, deploy from Git and roll back, debug a failed deploy, act on an incident, operate a VM, and scale an app. Its command and MCP tool references are generated from the command tree and the contract, and a test fails when the skill or this guide names a command, a flag or a tool that does not exist.

## What stays in the panel

Some scopes are **human-only** ([permission model](../contracts/permission-model.md), section 4.2). No grant can give them to an agent, and an agent's call answers `forbidden` with reason `human_only` (exit 4) on every channel: the client, `quoro api` and MCP. The client has no command for them, so these tasks stay in the panel:

- deciding approval requests (`approval:decide`);
- agents, agent keys, grants and webhooks (`agent:write`, `grant:write`, `webhook:write`), so an agent can never widen its own power;
- users and their roles (`user:read`, `user:write`);
- enrolling and removing nodes, changing their roles, acknowledging a fence and declaring a node lost (`node:enroll`, `node:remove`, `node:roles`, `node:fence_ack`, `node:declare_lost`);
- provider credentials, such as DNS, tunnel and object storage (`provider:manage`);
- backup repositories and their recovery key (`backup:manage`);
- cluster settings and the signing key (`cluster:manage`);
- deleting a quarantined copy of a volume (`app:delete_quarantine`).

Personal API tokens are also managed only by humans: an agent is refused on all three token calls. A shell in an app (`app:terminal`) is human-only too, but a human has `quoro ssh` for it.

A human can still reach any of these operations with `quoro api` and a personal API token, under the human's role. A panel is better for them: they are rare, they need a human's judgement, and several of them show a secret once.

The client also leaves out app settings that a task seldom needs, such as health check timings, placement, the HA policy and the backup schedule. Set those in the panel or with `quoro api`.

This is a rule, not a gap: the client keeps a small, fixed command set, small enough to learn and for an agent to hold in its context. A test holds the command tree to fixed budgets: at most 25 top-level commands, 50 leaf commands, 7 global flags, 5 flags per command and 40 lines of `quoro --help`. A command is added when a task needs it. Every other operation stays one `quoro api` call away.

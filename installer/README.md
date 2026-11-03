# Quoro installer

One command installs Quoro on a Linux host: the static binary, the Cloud
Hypervisor runtime it runs apps on, the `cloudflared` connector it serves app
hostnames through, two systemd units, the data directories and the config file.

```sh
curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo sh
```

Requirements: Linux on x86_64 (`x86_64-unknown-linux-musl` is the only published
target for now), root, systemd running, and `curl`, `tar`, `gzip`, `sha256sum`,
`install`, `mktemp`, `cmp`, `systemctl` and `grep` on the host; a primary also
needs `useradd` for its system account. The engine also needs `mkfs.ext4`
(e2fsprogs), `ip` (iproute2) and `nft` (nftables): when one is missing the
installer runs `apt-get update` and then installs the three packages from apt
without recommends; without `apt-get` it stops and names the missing tool.
`/dev/kvm` is required at run time: the installer warns when it is absent (the
node agent then refuses to start the engine), so the files can still be
inspected on a machine without virtualisation. The installer stops and lists
anything else missing.

The script is POSIX `sh`, and every statement that touches the host lives inside
a function with a single `main "$@"` at the end: a truncated download does
nothing.

## Environment variables

| Variable | Meaning |
| --- | --- |
| `QUORO_VERSION` | Install this version instead of the latest release. `v0.1.0` and `0.1.0` are both accepted. |
| `QUORO_INSTALL_FROM=<dir>` | Install from local artifacts in `<dir>` instead of downloading. For testing before a release exists. The version comes from `QUORO_VERSION`, or from the single `quoro-*-x86_64-unknown-linux-musl.tar.gz` in that directory. |
| `QUORO_INSTALL_DRY_RUN_REF=refs/heads/<branch>` | Verify a `workflow_dispatch` dry-run build from that branch instead of a release tag. Only honored together with `QUORO_INSTALL_FROM`, refused together with the insecure flag, and it prints a loud warning. Never use it for a real installation. |
| `QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1` | Skip Sigstore verification. Only honored together with `QUORO_INSTALL_FROM`, prints a loud warning, and the sha256 check still runs. Never use it for a real installation. |
| `QUORO_NO_START=1` | Install everything but do not start the units, and do not issue a setup token. On a node the enrollment still runs: it writes identity files and starts nothing. |
| `QUORO_ROLE=primary\|node` | What this host runs. `primary` (the default) installs the control plane and its own node agent; `node` installs only the node agent and enrolls it with a primary. See [Roles](#roles). |
| `QUORO_JOIN_URL` | Node only: the primary's HTTPS URL, for example `https://quoro.example.com:8443`. |
| `QUORO_JOIN_TOKEN` | Node only: a join token from the primary. It is single use, expires, and the installer never prints it. |
| `QUORO_JOIN_FINGERPRINT` | Node only: the primary's TLS fingerprint, `sha256:<hex>`. The installer prints it at the end of a primary install and it is in `journalctl -u quoro`. |

Example, pinning a version:

```sh
curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh \
  | sudo QUORO_VERSION=0.1.0 sh
```

## Roles

`QUORO_ROLE` decides what the host is (ADR 0089):

- **`primary`** (default) runs the control plane, `quoro.service`: the API, the
  panel and the node sessions, as the unprivileged system user `quoro-api` with
  an empty capability bounding set. The same host also runs its own node agent,
  so it deploys apps too. Nothing else is needed: the primary's agent enrolls
  itself through a join file the control plane writes, so there is no token.
- **`node`** runs only the node agent, `quoro-node.service`, as root. It runs
  the microVMs for one primary and serves no API and no panel. It belongs to
  exactly one primary and is enrolled during the install.

A node install needs a join token. Create one on the primary, in the panel or
with `POST /api/v1/nodes/join-tokens`, and pass it with the primary's URL and
TLS fingerprint:

```sh
curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh \
  | sudo QUORO_ROLE=node \
         QUORO_JOIN_URL=https://<primary>:8443 \
         QUORO_JOIN_TOKEN=<token> \
         QUORO_JOIN_FINGERPRINT=sha256:<hex> sh
```

The three values are checked before anything is written, so a missing one fails
before the host is touched. A join token is single use and expires, so create a
fresh one for every attempt. A host that is already enrolled needs no token:
re-running the installer with `QUORO_ROLE=node` finds its identity and skips the
enrollment.

A node install refuses a host that already runs a control plane (it finds
`/etc/systemd/system/quoro.service` or `/var/lib/quoro/quoro.db`): turning that
host into a node would orphan the database, its key files and its panel. It stops
without deleting anything; uninstall the control plane first.

## What it verifies, and what it trusts

1. **The script itself** arrives over TLS from `raw.githubusercontent.com`;
   `curl -fsSL` fails on an HTTP error and refuses plain HTTP.
2. **The tarball against `SHA256SUMS`** from the same release. This proves the
   download is the published bytes (integrity), nothing more. The check happens
   before anything is extracted, and a missing line for the tarball is a hard
   failure.
3. **The tarball against its GitHub artifact attestation**: a keyless Sigstore
   bundle (`quoro-<version>-x86_64-unknown-linux-musl.tar.gz.sigstore.jsonl`)
   verified with

   ```
   gh attestation verify <tarball> --bundle <bundle> \
     --repo OmarAlex24/quoro \
     --signer-workflow OmarAlex24/quoro/.github/workflows/release.yml \
     --source-ref refs/tags/v<version> \
     --deny-self-hosted-runners
   ```

   That pins the repository, the signing workflow, the tag and the runner kind.
   Verification uses a GitHub CLI downloaded from `github.com/cli/cli` whose
   sha256 is pinned in `install.sh` (`quoro-install:` prints when it fetches it).
   The pinned CLI runs with an empty config dir and with `GH_TOKEN` /
   `GITHUB_TOKEN` unset, so a token in the environment cannot change the result.
4. **The engine runtime against the pins in the tarball.** The tarball also
   carries `pins.txt` (`crates/engine/pins.txt`): one line per artifact with its
   pinned sha256. The installer reads the lines for this architecture, requires
   the `cloud-hypervisor` and `guest-kernel` pins, and stages each file only if
   its sha256 matches. Files come from `QUORO_INSTALL_FROM` first, then from an
   existing `/usr/local/lib/quoro`, then from the URL in the pin; a local
   candidate that does not match is ignored with a warning and the next source
   is tried. A download that does not match is fatal. Upstream publishes no
   signatures for these artifacts, so the integrity anchor is the chain TLS to
   GitHub → attested tarball → `pins.txt` → sha256, and the engine re-verifies
   the same pins on every start.
5. **The tunnel connector against its own pins in the tarball.** Nodes behind
   NAT serve app hostnames through Cloudflare Tunnel, which the node agent runs
   as a `cloudflared` child process (ADR 0024, ADR 0055). The tarball carries
   `ingress-pins.txt` (`crates/ingress/pins.txt`) in the same format, one
   `cloudflared` line per architecture. The installer reads the line for this
   architecture, requires it to be `cloudflared`, requires its URL to be under
   `https://github.com/cloudflare/cloudflared/releases/download/`, and stages
   the file with the same rules as (4): `QUORO_INSTALL_FROM`, then an existing
   `/usr/local/lib/quoro`, then the pinned URL; a local candidate that does not
   match is ignored with a warning, a download that does not match is fatal.
   The integrity chain is the same one: TLS to GitHub → attested tarball →
   `ingress-pins.txt` → sha256. A tarball from an older release has no
   `ingress-pins.txt`; the installer then warns and installs without a
   connector, because only the tunnel needs it.

The repository, signer workflow, tag ref and GitHub CLI version/sha256 are
constants in the script. No environment variable overrides them. The one
exception is `QUORO_INSTALL_DRY_RUN_REF`, which replaces only the attested
source ref, requires local artifacts and cannot be combined with the insecure
flag; it exists so a dry-run build can be verified before a release, and the
attestation check is otherwise unchanged. Changing any other constant is a
reviewed change (ADR 0027).

Any failure in (2), (3), (4) or (5) aborts with

    signature verification failed, nothing was installed

and no file is written outside the temporary directory. Host packages are
installed only after every check passed.

## Host layout created (ADR 0054, ADR 0089)

| Path | Role | Content |
| --- | --- | --- |
| `/usr/local/bin/quoro` | both | Static binary, mode 0755, replaced atomically via `quoro.new` + `mv`. |
| `/usr/local/lib/quoro/cloud-hypervisor-static` | both | Cloud Hypervisor, mode 0755, verified against `pins.txt`. |
| `/usr/local/lib/quoro/vmlinux-x86_64` | both | Guest kernel, mode 0644, verified against `pins.txt`. |
| `/usr/local/lib/quoro/cloudflared` | both | Cloudflare Tunnel connector, mode 0755, owner `root:root`, verified against `ingress-pins.txt`. The node agent runs it from this exact path for app hostnames (ADR 0024, ADR 0055). |
| `/usr/local/lib/quoro/quoro-init` | both | PID 1 inside every microVM, mode 0755. |
| `/etc/systemd/system/quoro-node.service` | both | Unit from `packaging/systemd/quoro-node.service`, mode 0644. Runs as root; stopping it leaves the VMs running. |
| `/var/lib/quoro-node` | both | Node agent directory, mode 0700, owner `root:root`: `identity.json`, `credential` and the private-key seed (0600), the local store, and the engine state in `engine/`. |
| `/etc/quoro/quoro.toml` | both | Config, mode 0640, owner `root:quoro-api` on a primary and `root:root` on a node: the API reads it, only root writes it. |
| `/etc/systemd/system/quoro.service` | primary | Unit from `packaging/systemd/quoro.service`, mode 0644. Runs as `quoro-api` with no capabilities. |
| `/var/lib/quoro` | primary | Control plane directory, mode 0700, owner `quoro-api:quoro-api`: SQLite database, the cluster key files (0600) and the local node's join file. |
| user `quoro-api` (group `quoro-api`) | primary | System account with no home, no login shell and no password, created only when missing. |

`/var/lib/quoro` and `/var/lib/quoro-node` are siblings, never nested, so each
service owns its own tree and the unprivileged control plane cannot read the
agent's state. A `quoro` user left by an older installer is no longer used; the
installer logs it and leaves it alone.

The config is a starting point, not a secret. On a primary:

```toml
# Quoro configuration (ADR 0054 host layout, ADR 0089 privilege split).
# Edit this file and restart quoro: systemctl restart quoro quoro-node

# One TLS listener for API, panel and MCP. Ingress keeps 80/443.
listen = "0.0.0.0:8443"

# SQLite database, cluster key files (0600) and the local join file.
data_dir = "/var/lib/quoro"

# Engine: "cloud-hypervisor" runs one microVM per app; "fake" runs nothing
# and exists for development only.
engine = "cloud-hypervisor"

# Base URL nodes dial, for example "https://quoro.example.com:8443".
# Unset, it defaults to the Host header of the request that created the
# join token, which is right for most installs.
# public_url = "https://<this host>:8443"

# Engine settings. The values below are the defaults, and the block is
# commented out; uncomment a line only to change it. Guest DNS defaults
# to this host's non-loopback resolvers; set dns to override them.
# [cloud_hypervisor]
# artifacts_dir = "/usr/local/lib/quoro"
# init_binary = "/usr/local/lib/quoro/quoro-init"
# state_dir = "/var/lib/quoro-node/engine"
# bridge = "quoro0"
# subnet = "10.42.0.0/24"
# dns = ["192.0.2.53"]

[node]
# The node agent runs as quoro-node.service, separate from the
# unprivileged control plane (ADR 0089).
local = false
```

On a node:

```toml
# Quoro configuration (ADR 0089 privilege split).
# This host is a node: it runs microVMs for its primary and runs no
# control plane. Edit this file and restart with:
#   systemctl restart quoro-node

# Engine: "cloud-hypervisor" runs one microVM per app; "fake" runs nothing
# and exists for development only.
engine = "cloud-hypervisor"

# Engine settings. The values below are the defaults, and the block is
# commented out; uncomment a line only to change it. Guest DNS defaults
# to this host's non-loopback resolvers; set dns to override them.
# [cloud_hypervisor]
# artifacts_dir = "/usr/local/lib/quoro"
# init_binary = "/usr/local/lib/quoro/quoro-init"
# state_dir = "/var/lib/quoro-node/engine"
# bridge = "quoro0"
# subnet = "10.42.0.0/24"
# dns = ["192.0.2.53"]

[node]
# The identity, local store and engine state of the agent on this host.
# data_dir = "/var/lib/quoro-node"
```

An existing config is never rewritten with a new one. When it has no `engine`
line, the installer logs that Quoro will run the fake engine and how to switch.
On a primary, a config with no `[node]` table gets `[node]` and `local = false`
appended, so the control plane stops trying to run the node agent itself; a
config whose `[node]` table has no `local = false` is left alone with a warning,
because `quoro serve` would then run the agent unprivileged and refuse.

After installing, the script prints the binary's `quoro version` output. On a
primary it also prints a one-time setup token from `quoro setup-token` (single
use, expires after one hour, needed by `POST /api/v1/setup`, which the panel
serves at `https://<host>:8443/` with a self-signed certificate) and this host's
TLS fingerprint, read from `journalctl -u quoro`. The token is printed to the
terminal only; it is never written to a file. Get a fresh one any time with:

```sh
sudo quoro setup-token
```

On a node it prints the node id from `/var/lib/quoro-node/identity.json`; the
node then shows as connected on the primary's Nodes page.

Back up the cluster key files under `/var/lib/quoro` separately from the
database (ADR 0050): a database copy does not contain them, and losing the
data-encryption key loses every stored secret.

## Privileges (ADR 0089)

Two processes, two privilege levels, and neither can borrow the other's:

- `quoro.service` (the control plane: API, panel and node sessions) runs as
  `quoro-api` with an empty capability bounding set and
  `AmbientCapabilities=` empty, so no capability can be gained;
  `NoNewPrivileges=yes`, `ProtectSystem=strict` and `/var/lib/quoro` as its only
  writable path. Port 8443 needs no capability. It signs desired state and never
  touches a hypervisor, a bridge or a firewall.
- `quoro-node.service` (the node agent) runs as root, because the engine creates
  the bridge, TAP devices and nftables rules and converts images with ownership
  preserved. It is the only privileged Quoro process.

The installer never hands `/var/lib/quoro` to a person: if a `quoro-api` account
already exists with a uid of 1000 or above, it stops before writing anything.

Hosts installed by the M1 installer have `/var/lib/quoro` owned by root. The
installer stops `quoro.service` before it changes that ownership, gives every
file to `quoro-api` except `engine/`, and leaves `engine/` owned by root: it is
the M1 engine state, which the control plane must not read. The node agent keeps
its engine state in `/var/lib/quoro-node/engine` from then on, and VMs that M1
created are not adopted.

## Idempotency

Re-running the installer is safe:

- `quoro.toml` is created only when absent; an existing config is kept as is
  and reported as `kept existing /etc/quoro/quoro.toml`. On a primary the only
  edit is the appended `[node]` table when it is missing.
- The binary, the units and the engine runtime files are compared with `cmp -s`
  before replacement; when unchanged they are reinstalled identically and the
  services are left running. The connector at
  `/usr/local/lib/quoro/cloudflared` is compared the same way.
- `/var/lib/quoro-node` is created or confirmed as `root:root` 0700, never
  recreated. `/var/lib/quoro` is the same for `quoro-api:quoro-api` 0700, and
  the ownership change is skipped when the directory already has that owner.
- A node that is already enrolled is not enrolled again, so no join token is
  needed on a re-run.
- Each unit is restarted only when the binary changed, its unit changed, an
  engine runtime file changed, the connector changed, or it is not active; the
  installer then waits up
  to 20 s for it to stay active for five consecutive seconds (so a refusal right
  after start is caught) and fails with that unit's
  `journalctl -u <unit> -n 50` hint otherwise. On a primary `quoro.service`
  starts before `quoro-node.service`, because the control plane writes the join
  file its own agent enrolls with.

## Virtualisation and the engine

The engine needs `/dev/kvm` and hardware virtualisation (vmx/svm), and it writes
`/proc/sys/net/ipv4/ip_forward`. It runs inside `quoro-node.service`, which is
root because it manages a bridge, TAP devices and nftables rules; the unit keeps
hardening that the engine tolerates (`NoNewPrivileges`, `ProtectHome`,
`PrivateTmp`, `ProtectSystem=full`). When the host cannot run the engine,
`quoro node` exits 78 and the unit stays failed (`RestartPreventExitStatus=78`)
instead of restarting in a loop; read `journalctl -u quoro-node`.
`KillMode=process` is deliberate: stopping or restarting the agent does not stop
the VMs, and the next agent adopts them from its engine state.

## Uninstall

A primary runs both units, a node only the agent. Stop and remove what the host
has:

```sh
sudo systemctl disable --now quoro-node
sudo systemctl disable --now quoro          # primary only
sudo rm -f /etc/systemd/system/quoro-node.service
sudo rm -f /etc/systemd/system/quoro.service  # primary only
sudo systemctl daemon-reload
sudo rm /usr/local/bin/quoro
sudo rm -rf /usr/local/lib/quoro
```

`systemctl stop quoro-node` leaves every VM running on purpose
(`KillMode=process`): stopping the agent is not stopping the workloads, and the
next agent adopts them. To stop the VMs as well, stop them from the primary
first, or stop the hypervisors directly:

```sh
sudo pkill -f /usr/local/lib/quoro/cloud-hypervisor
```

The engine leaves its network devices behind; remove them too:

```sh
sudo ip link delete quoro0
sudo nft delete table ip quoro_quoro0
```

The table is named `quoro_<bridge>`, so `quoro0` (the default bridge) gives
`quoro_quoro0`.

`/var/lib/quoro-node` (identity, credential, private key and engine state) and
`/etc/quoro` (config) are kept on purpose on both roles, and `/var/lib/quoro`
(database and cluster key files) is kept on a primary: remove them deliberately
if that is what you want, and back up the key files under `/var/lib/quoro` first.
Stop the VMs before deleting a node's directory, otherwise the hypervisors keep
running without their state:

```sh
sudo pkill -f /usr/local/lib/quoro/cloud-hypervisor
sudo rm -rf /var/lib/quoro-node
```

A node that is removed this way holds a credential the primary still knows
about; remove the node on the primary too. On a primary the installer's system
account goes with it:

```sh
sudo userdel quoro-api
```

Hosts installed by an older installer also have a `quoro` system user that this
installer no longer creates or uses; remove it, after the services are stopped,
with:

```sh
sudo userdel quoro
```

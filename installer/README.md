# Quoro installer

One command installs Quoro on a Linux host: the static binary, the Cloud
Hypervisor runtime it runs apps on, a systemd unit, the data directory and the
config file.

```sh
curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo sh
```

Requirements: Linux on x86_64 (`x86_64-unknown-linux-musl` is the only published
target for now), root, systemd running, and `curl`, `tar`, `gzip`, `sha256sum`,
`install`, `mktemp`, `cmp`, `systemctl` and `grep` on the host. The engine also
needs `mkfs.ext4` (e2fsprogs), `ip` (iproute2) and `nft` (nftables): when one is
missing the installer runs `apt-get update` and then installs the three packages
from apt without recommends; without `apt-get` it stops and names the missing
tool. `/dev/kvm` is required at run time: the installer warns when it is absent
(the service then refuses to start the engine), so the files can still be
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
| `QUORO_NO_START=1` | Install everything but do not start `quoro.service`, and do not issue a setup token. |

Example, pinning a version:

```sh
curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh \
  | sudo QUORO_VERSION=0.1.0 sh
```

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

   The repository, signer workflow, tag ref and GitHub CLI version/sha256 are
   constants in the script. No environment variable overrides them. The one
   exception is `QUORO_INSTALL_DRY_RUN_REF`, which replaces only the attested
   source ref, requires local artifacts and cannot be combined with the insecure
   flag; it exists so a dry-run build can be verified before a release, and the
   attestation check is otherwise unchanged. Changing any other constant is a
   reviewed change (ADR 0027).

Any failure in (2), (3) or (4) aborts with

    signature verification failed, nothing was installed

and no file is written outside the temporary directory. Host packages are
installed only after every check passed.

## Host layout created (ADR 0054)

| Path | Content |
| --- | --- |
| `/usr/local/bin/quoro` | Static binary, mode 0755, replaced atomically via `quoro.new` + `mv`. |
| `/usr/local/lib/quoro/cloud-hypervisor-static` | Cloud Hypervisor, mode 0755, verified against `pins.txt`. |
| `/usr/local/lib/quoro/vmlinux-x86_64` | Guest kernel, mode 0644, verified against `pins.txt`. |
| `/usr/local/lib/quoro/quoro-init` | PID 1 inside every microVM, mode 0755. |
| `/etc/systemd/system/quoro.service` | Unit from `packaging/systemd/quoro.service`, mode 0644. Runs as root. |
| `/etc/quoro/quoro.toml` | Config, mode 0640, owner `root:root`. |
| `/var/lib/quoro` | Data directory, mode 0700, owner `root:root`: SQLite database, the cluster key files (0600) and the engine state in `engine/`. |

No system user is created: the engine creates the bridge, TAP devices and
nftables rules and converts images with ownership preserved, so the service runs
as root. A `quoro` user left by an older installer is no longer used; the
installer logs it and leaves it alone. Hosts installed by an older installer
have their `/var/lib/quoro` moved to `root:root` 0700.

The config is a starting point, not a secret:

```toml
# Quoro configuration (ADR 0054 host layout).
# Edit this file and restart quoro: systemctl restart quoro

# One TLS listener for API, panel and MCP. Ingress keeps 80/443.
listen = "0.0.0.0:8443"

# SQLite database, cluster key files (0600) and engine state.
data_dir = "/var/lib/quoro"

# Engine: "cloud-hypervisor" runs one microVM per app; "fake" runs nothing
# and exists for development only.
engine = "cloud-hypervisor"

# Engine settings. The values below are the defaults, and the block is
# commented out; uncomment a line only to change it. Guest DNS defaults
# to this host's non-loopback resolvers; set dns to override them.
# [cloud_hypervisor]
# artifacts_dir = "/usr/local/lib/quoro"
# init_binary = "/usr/local/lib/quoro/quoro-init"
# state_dir = "/var/lib/quoro/engine"
# bridge = "quoro0"
# subnet = "10.42.0.0/24"
# dns = ["192.0.2.53"]
```

An existing config is never rewritten. When it has no `engine` line, the
installer logs that Quoro will run the fake engine and how to switch.

After installing, the script prints the binary's `quoro version` output and a
one-time setup token from `quoro setup-token` (single use, expires after one
hour, needed by `POST /api/v1/setup`, which the panel serves at
`https://<host>:8443/` with a self-signed certificate). The token is printed to
the terminal only; it is never written to a file. Get a fresh one any time with:

```sh
sudo quoro setup-token
```

Back up the cluster key files under `/var/lib/quoro` separately from the
database (ADR 0050): a database copy does not contain them, and losing the
data-encryption key loses every stored secret.

## Idempotency

Re-running the installer is safe:

- `quoro.toml` is created only when absent; an existing config is kept as is
  and reported as `kept existing /etc/quoro/quoro.toml`.
- The binary, the unit and the engine runtime files are compared with `cmp -s`
  before replacement; when unchanged they are reinstalled identically and the
  service is left running.
- `/var/lib/quoro` is created or confirmed, never recreated; an existing
  directory is moved to `root:root` 0700.
- The service is restarted only when the binary changed, the unit changed, an
  engine runtime file changed, or it is not active; the installer then waits up
  to 20 s for it to stay active for five consecutive seconds (so a host check
  refusal right after start is caught) and fails with a
  `journalctl -u quoro -n 50` hint otherwise.

## Virtualisation and the engine

The engine needs `/dev/kvm` and hardware virtualisation (vmx/svm), and it writes
`/proc/sys/net/ipv4/ip_forward`. The service runs as root because it manages a
bridge, TAP devices and nftables rules; the unit keeps hardening that the engine
tolerates (`NoNewPrivileges`, `ProtectHome`, `PrivateTmp`, `ProtectSystem=full`).
When the host cannot run the engine, `quoro serve` exits 78 and the unit stays
failed (`RestartPreventExitStatus=78`) instead of restarting in a loop; read
`journalctl -u quoro`.

## Uninstall

```sh
sudo systemctl disable --now quoro
sudo rm /etc/systemd/system/quoro.service
sudo systemctl daemon-reload
sudo rm /usr/local/bin/quoro
sudo rm -rf /usr/local/lib/quoro
```

`systemctl disable --now quoro` stops every VM this host runs. The engine leaves
its network devices behind; remove them too:

```sh
sudo ip link delete quoro0
sudo nft delete table ip quoro_quoro0
```

The table is named `quoro_<bridge>`, so `quoro0` (the default bridge) gives
`quoro_quoro0`.

`/var/lib/quoro` (database, key files and engine state) and `/etc/quoro`
(config) are kept on purpose: remove them deliberately if that is what you want,
and back up the key files under `/var/lib/quoro` first. Hosts installed by an
older installer also have a `quoro` system user that this installer no longer
creates or uses; remove it, after the service is stopped, with:

```sh
sudo userdel quoro
```

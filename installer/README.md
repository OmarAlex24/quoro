# Quoro installer

One command installs Quoro on a Linux host: a static binary, a systemd unit, a
system user, the data directory and the config file.

```sh
curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo sh
```

Requirements: Linux on x86_64 (`x86_64-unknown-linux-musl` is the only published
target for now), root, systemd running, and `curl`, `tar`, `gzip`, `sha256sum`,
`install`, `mktemp`, `cmp`, `systemctl` and `useradd` on the host. The installer
stops and lists anything missing.

The script is POSIX `sh`, and every statement that touches the host lives inside
a function with a single `main "$@"` at the end: a truncated download does
nothing.

## Environment variables

| Variable | Meaning |
| --- | --- |
| `QUORO_VERSION` | Install this version instead of the latest release. `v0.1.0` and `0.1.0` are both accepted. |
| `QUORO_INSTALL_FROM=<dir>` | Install from local artifacts in `<dir>` instead of downloading. For testing before a release exists. The version comes from `QUORO_VERSION`, or from the single `quoro-*-x86_64-unknown-linux-musl.tar.gz` in that directory. |
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

   The repository, signer workflow, tag ref and GitHub CLI version/sha256 are
   constants in the script. No environment variable overrides them; changing one
   is a reviewed change (ADR 0027).

Any failure in (2) or (3) aborts with `signature verification failed, nothing
was installed`, and no file is written outside the temporary directory.

## Host layout created (ADR 0054)

| Path | Content |
| --- | --- |
| `/usr/local/bin/quoro` | Static binary, mode 0755, replaced atomically via `quoro.new` + `mv`. |
| `/etc/systemd/system/quoro.service` | Unit from `packaging/systemd/quoro.service`, mode 0644. |
| `/etc/quoro/quoro.toml` | Config, mode 0640, owner `root:quoro`. |
| `/var/lib/quoro` | Data directory, mode 0750, owner `quoro:quoro`: SQLite database and the cluster key files (0600). |
| user/group `quoro` | System user, shell `/usr/sbin/nologin`, home `/var/lib/quoro` (not created by `useradd`). Added to `kvm` when that group exists. |

The config is a starting point, not a secret:

```toml
# Quoro configuration (ADR 0054 host layout).
# Edit this file and restart quoro: systemctl restart quoro

# One TLS listener for API, panel and MCP. Ingress keeps 80/443.
listen = "0.0.0.0:8443"

# SQLite database and the cluster key files (0600).
data_dir = "/var/lib/quoro"
```

After installing, the script prints the binary's `quoro version` output and a
one-time setup token from `quoro setup-token` (single use, expires after one
hour, needed by `POST /api/v1/setup`, which the panel serves at
`https://<host>:8443/` with a self-signed certificate). The token is printed to
the terminal only; it is never written to a file. Get a fresh one any time with:

```sh
sudo -u quoro quoro setup-token
```

Back up the cluster key files under `/var/lib/quoro` separately from the
database (ADR 0050): a database copy does not contain them, and losing the
data-encryption key loses every stored secret.

## Idempotency

Re-running the installer is safe:

- `quoro.toml` is created only when absent; an existing config is kept as is
  and reported as `kept existing /etc/quoro/quoro.toml`.
- The binary and the unit are compared with `cmp -s` before replacement; when
  unchanged they are reinstalled identically and the service is left running.
- The user, group, directories and `kvm` membership are only created or
  confirmed, never recreated.
- The service is restarted only when the binary changed, the unit changed, or it
  is not active; the installer then waits up to 10 s for it to become active and
  fails with a `journalctl -u quoro -n 50` hint otherwise.

## kvm group

The unit uses `SupplementaryGroups=kvm`. systemd refuses to start a unit whose
supplementary group does not exist, so a host without a `kvm` group (for example
a VM without nested virtualisation) must remove that line from
`/etc/systemd/system/quoro.service` and run `systemctl daemon-reload`. Quoro
needs `/dev/kvm` to run microVMs, so such a host is not useful in practice; the
installer prints a warning instead of failing so the files can still be
inspected.

## Uninstall

```sh
sudo systemctl disable --now quoro
sudo rm /etc/systemd/system/quoro.service
sudo systemctl daemon-reload
sudo rm /usr/local/bin/quoro
sudo userdel quoro
```

The uninstall command removes the program and the unit only. `/var/lib/quoro`
(database and key files) and `/etc/quoro` (config) are kept on purpose: remove
them deliberately if that is what you want, and back up the key files under
`/var/lib/quoro` first. Files left in those directories stay owned by the
numeric id of the removed `quoro` user. Run `userdel quoro` only after the
service is stopped: it refuses to remove a user with running processes.

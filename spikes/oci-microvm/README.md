# Spike: boot a microVM from an OCI image

Throwaway spike for the M0 roadmap item "boot a microVM from an OCI image"
(issue #10). It answers one question with evidence: can Quoro turn a public OCI
image into an ext4 root disk and boot it in a Cloud Hypervisor microVM with its
own minimal init? It feeds the open question Q-0003 (write the conversion and
the init ourselves, or reuse an existing component).

**Result: yes.** On a nested-virtualization lab VM, `alpine:3.22` and a 9-layer
non-root `nginx-unprivileged:1.29-alpine` image were pulled, verified,
flattened into ext4, booted under Cloud Hypervisor v54.0, and printed the
expected marker on the serial console; the app's stdout/stderr came back tagged
on the virtio-console and the guest powered itself off with the app's exit code.

This directory is **not** part of the repository's cargo workspace: it has its
own `Cargo.toml` with an empty `[workspace]`. A root workspace must list
`spikes/*` under `exclude`. Nothing here is production code; the engine
workstream re-implements what survives inside `crates/engine` and `crates/init`.

## Layout

| Path | What |
| --- | --- |
| `init/` | `quoro-init`: static (musl) PID 1 for the guest. One dependency: `libc`. |
| `oci2rootfs/` | `oci2rootfs`: pull an image, verify digests, apply layers with whiteouts, write `/quoro/config`, build ext4 with `mkfs.ext4 -d`. |
| `config-format.md` | The init's config file and console protocol (pipeline <-> init contract for the spike). |
| `build-rootfs.sh` | Builds both binaries and runs `oci2rootfs` under `sudo`. |
| `run.sh` | Downloads and verifies Cloud Hypervisor and the guest kernel, boots the rootfs, waits for the marker, prints timings. |

## How to run (Linux host with KVM)

Requirements: x86_64 Linux with `/dev/kvm` (bare metal or nested virt; host
kernel >= 6.8 for Cloud Hypervisor), the user in group `kvm`, passwordless
`sudo` for the conversion step, Rust with the `x86_64-unknown-linux-musl`
target, `musl-tools`, `e2fsprogs` (`mkfs.ext4` with `-d`, >= 1.43), `curl`.

```sh
cargo test --release                       # 21 unit tests (init config parser, whiteouts, users, ...)

./build-rootfs.sh docker.io/library/alpine:3.22 /tmp/alpine.ext4 \
  --hostname spike \
  --arg /bin/sh --arg -c --arg 'echo hello from alpine $(cat /etc/alpine-release) as $(id -u) on $(hostname); echo to-stderr >&2'

./run.sh /tmp/alpine.ext4                  # exit 0 = marker seen and app exited 0
```

Without `--arg`, the image's `Entrypoint` + `Cmd` run. `oci2rootfs --help`
lists the other flags (`--size-mb`, `--work-dir`, `--keep-work`).

What `run.sh` launches (Cloud Hypervisor v54.0):

```
cloud-hypervisor-static --api-socket path=<run>/api.sock \
  --kernel vmlinux-x86_64 \
  --cmdline "console=ttyS0 root=/dev/vda rw init=/quoro/init panic=-1" \
  --disk path=<rootfs.ext4>,image_type=raw \
  --cpus boot=1 --memory size=256M \
  --serial file=<run>/serial.log --console file=<run>/console.log
```

v54 rejects `--disk path=...` without `image_type=` ("Image type required for
disk").

## Results (2026-11-02)

Host: a Debian 13 VM (kernel 6.12, 2 vCPU, 4 GB) with nested KVM on an Intel
i7-10700T. **Nested virtualization: these numbers check correctness only and are
not benchmarks.**

Serial console of the alpine run (kernel lines omitted):

```
quoro-init: mounts ready
quoro-init: config parsed: 3 arg(s), 1 env entry, uid=0, gid=0, workdir=/
quoro-init: hostname set to spike
quoro-init: QUORO_SPIKE_BOOT_OK uptime_ms=1090
quoro-init: app started: pid=609, program=/bin/sh
quoro-init: QUORO_SPIKE_APP_EXIT code=0
```

virtio-console (app output, tagged per stream):

```
out hello from alpine 3.22.7 as 0 on spike
err to-stderr
```

| Image | Layers | Pull / apply / mkfs (ms) | ext4 | Launch -> marker (host) | Guest uptime at marker | Launch -> VMM exit |
| --- | --- | --- | --- | --- | --- | --- |
| `alpine:3.22` (manifest `sha256:dc92f017…bb4eec`) | 1 | 1516 / 129 / 61 | 75 MiB | 2330, 2467, 2565 ms | 1090-1160 ms | 2388-2639 ms |
| `nginxinc/nginx-unprivileged:1.29-alpine` (`sha256:80597010…2a648705`), runs as `101:101` from the image's `User` | 9 | 4653 / 676 / 332 | 132 MiB | 2417 ms | 1120 ms | 2507 ms |

- Launch -> marker is measured on the host from starting the VMM to the marker
  appearing in the serial log file (5 ms polling). Guest uptime is the guest
  kernel's own clock when the init prints the marker. About 1.2 s of the host
  figure happens before the guest clock starts or is spent in slow nested
  early timekeeping (a 0.6 s gap around early ACPI init in the kernel log).
- The 8.8 MB `bzImage` boots no faster than the 48 MB `vmlinux` here
  (2.64-2.69 s vs 2.41-2.55 s), so loading the kernel image is not the cost.
- Other conversions done during the spike (no boot): `nginx:1.29` (7 layers,
  267 MiB), `node:22-bookworm-slim` (5 layers, 346 MiB), `tomcat:11-jre21-temurin`
  (11 layers, 1 real whiteout applied, 2.1 GiB).
- Pull by digest works (`alpine@sha256:dc92…` resolves to the same manifest);
  an unknown digest fails at manifest resolution.
- The init's failure paths were booted too: unknown config key, missing
  binary (`FATAL child: … ENOENT`, exit 127), non-zero exit, app killed by a
  signal, `SIGTERM` forwarded to the app, `SIGKILL` 10 s later for an app that
  ignores it. Each one ends in the guest powering off with the matching marker.

## Choices

**Guest kernel: the prebuilt `vmlinux-x86_64` from `cloud-hypervisor/linux`
(release `ch-release-v7.2.8-20261009`, Linux 7.2.8).** It is built from the
Cloud Hypervisor project's kernel config for direct kernel boot (PVH ELF), with
virtio-blk, virtio-console and ext4 built in, so no initrd is needed. A distro
kernel (e.g. Debian's) needs an initrd because virtio-blk is a module, and
building our own kernel is real work that the spike does not need to answer
its question. For production, Quoro will want a pinned kernel it builds
reproducibly (D-0027); this release is the starting config.

**Verification of downloaded binaries.** Neither Cloud Hypervisor v54.0 nor the
guest kernel release publishes a signature, a SUMS file or a GitHub build
attestation. `run.sh` pins the sha256 that GitHub lists for each release asset
and checks it on every run. That proves "same bytes as the release", not who
built them.

**Init (`init/`, one dependency).** `libc`, for raw `mount`, `reboot`,
`signalfd`, `setuid` and `execve`. It mounts `/proc`, `/sys`, `/dev` (plus tmpfs
on `/tmp` and `/run` when the image has them), reads `/quoro/config`, sets the
hostname, prints the boot marker, runs the app as PID 2 in its own session under
the image's uid/gid with exactly the configured environment, tags each stdout or
stderr line onto the virtio-console, reaps orphans, forwards `SIGTERM`/`SIGINT`/`SIGHUP`
to the app's process group (then `SIGKILL` after 10 s), and powers off with the
app's exit status. Every failure prints `quoro-init: FATAL …` on the serial
console and powers off. Static-pie binary: 413 KiB.

**Pipeline (`oci2rootfs/`), dependencies.**

| Crate | Why |
| --- | --- |
| `oci-client` 0.18 (rustls, no default features) | OCI distribution protocol: anonymous token auth, index -> platform manifest, blob pull. The crate under evaluation. |
| `tokio` (`rt`, `macros`) | `oci-client` is async-only. |
| `sha2` | Verify every blob digest ourselves, independently of the client. |
| `tar` | Read layer tarballs; `unpack_in` refuses `..` escapes. |
| `flate2` | gzip layers. |
| `serde_json` | Parse the image config (Entrypoint, Cmd, Env, User, WorkingDir). |
| `anyhow` | Errors with context. |

No `clap`: argv is parsed by hand.

Layers are applied in two passes over each downloaded blob: first the
whiteouts (`.wh.<name>` removes that path, `.wh..wh..opq` empties the directory
of everything lower layers left), then every other entry. A non-directory entry
replaces whatever exists at its path, and so does a directory entry landing on a
symlink, the same rule Docker's layer applier uses. Deletions never follow
symlinks. The tool runs as root so `mkfs.ext4 -d` copies the image's real
ownership into the filesystem.

## Findings and limits

- **`oci-client` works but fits a CLI awkwardly.** Platform resolution and
  anonymous auth worked first time. But the only streaming blob API takes a
  `tokio::io::AsyncWrite` (no sync API, no `std::io::Write`), so a synchronous
  program needs a runtime and a hand-written writer. `Client::pull()` buffers
  every layer in memory, so per-layer `pull_blob` is the only usable path.
  There is no retry, resume, range request or progress callback, and layer
  media types are not validated. `pull_blob` already checks the digest; our
  independent check is defence in depth. Inside the async node agent most of
  this friction disappears.
- **Absolute symlinks inside the image are not resolved inside the rootfs.** A
  layer entry whose parent directory is an absolute symlink (Debian's
  `/var/run -> /run` is one) is refused with an error instead of being written
  through it. It fails closed, but a real implementation must resolve paths
  relative to the rootfs root (`openat2` with `RESOLVE_IN_ROOT`, or
  equivalent). Containerd and umoci do this with "secure join" helpers. This
  is the one piece where reusing a vetted library is worth weighing.
- **Layer formats:** gzip and uncompressed tar only. zstd layers fail with
  an error that names the media type. Foreign or non-distributable layers are
  not handled.
- **Conversion needs root.** This is acceptable for a node agent that already
  runs privileged. A rootless path would use `mke2fs -d` from a fakeroot-style
  ownership map, or write ext4 directly.
- **Not covered by the spike:** networking (no TAP, so no address configuration
  in the init yet), graceful shutdown on the ACPI power button (the init would
  have to read the input event device; only `SIGTERM` to PID 1 is handled),
  the writable data disk, config delivered on a separate disk or the kernel
  command line (here `/quoro/config` is baked into the rootfs), a page or block
  cache for converted images, and boot-time numbers on bare metal.

## What it means for Q-0003

The evidence supports writing both pieces ourselves:

- The init is about 900 lines of Rust, one dependency (`libc`), and a 413 KiB
  static binary. It already implements most of the guest contract in
  `docs/contracts/engine-trait.md` §6: mounts, env, user, working directory,
  tagged stdout/stderr on the virtio-console, reaping, signal forwarding,
  exit status, and FATAL-then-power-off. Network setup and the power button
  remain. A reused guest agent would be bigger, and the init is the
  component that has to be auditable.
- The conversion is about 1,400 lines including tests, on mature crates
  (`tar`, `flate2`, `sha2`), with `oci-client` for the registry protocol and
  `mkfs.ext4 -d` for the filesystem. Converting `alpine` takes 1.7 s, of
  which 1.5 s is the pull. The parts that need care are path resolution
  inside the rootfs and the registry client's ergonomics. Both are contained
  and testable.
- Caveats: a nested-virt lab, two images booted, one real-world whiteout
  exercised outside unit tests.

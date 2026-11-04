# Guest kernel

Quoro builds the Linux kernel its microVMs boot (ADR 0152). The kernel is
upstream stable plus Cloud Hypervisor's guest kernel changes and their
`ch_defconfig`. It is built reproducibly in a pinned container, attested in CI
(ADR 0081) and published as its own release. `crates/engine/pins.txt` then
pins it by sha256.

Why Quoro builds its own: Cloud Hypervisor publishes guest kernels only with its
VMM releases, about every two months. Their newest build (7.2.8) was older than
the stable release that fixed a critical vulnerability (7.2.10), and no newer
build existed. Building from upstream stable lets Quoro ship a fix as soon as
stable has one.

## Files

| File | What it is |
| --- | --- |
| `kernel.txt` | The recipe: the upstream version and Quoro revision, the tarball URL, its sha256, the signing key fingerprint, the Cloud Hypervisor commits the patch comes from, and the fixed build identity |
| `patches/cloud-hypervisor.patch` | Cloud Hypervisor's changes, vendored: the paravirtualised virtio watchdog driver, boot-time trace points and `arch/x86/configs/ch_defconfig`. GPL-2.0-only |
| `regenerate-patch.sh` | Rebuilds the patch from Cloud Hypervisor's repository (`--check` compares it with the vendored one) |
| `keys/stable-signing-key.asc` | Greg Kroah-Hartman's stable release signing key, from kernel.org's `pgpkeys.git`; pinned by sha256 and fingerprint in `kernel.txt` |
| `Containerfile` | The build image: the release image's base digest and apt snapshot (ADR 0090), with the kernel build packages at exact versions |
| `build-kernel-in-container.sh` | Builds the image and runs `build-kernel.sh` in it; `--print-tag` prints the tag of the recipe |
| `build-kernel.sh` | The build, inside the image only |
| `reproduce-kernel.sh` | Rebuilds a published kernel and compares hashes |

## What the build does

1. Downloads the tarball and its detached signature from `cdn.kernel.org`.
   Checks the tarball's sha256 against `kernel.txt`, then checks kernel.org's
   signature over the uncompressed tar with `gpgv`. The signature must be made
   by the pinned primary key fingerprint.
2. Unpacks it to a fixed path and applies the patch with `git apply`, which
   refuses fuzz.
3. Runs `make ch_defconfig` and requires the options the guests depend on:
   virtio block, net, console, entropy and vsock (ADR 0113), the PVH entry
   point, the 8250 serial console, ext4, overlayfs, the cgroup v2 controllers,
   namespaces and seccomp (BuildKit in the builder VM). It also requires that
   loadable modules, randomised struct layout, GCC plugins, embedded headers
   and BTF stay off. Then it prints `scripts/diffconfig` between
   `ch_defconfig` and the resolved `.config`.
4. Builds `vmlinux` with `KBUILD_BUILD_TIMESTAMP`, `KBUILD_BUILD_USER`,
   `KBUILD_BUILD_HOST` and `KBUILD_BUILD_VERSION` from `kernel.txt`, with
   `LC_ALL=C`, `TZ=UTC` and `umask 022`. kbuild's own variables from the host
   environment are cleared. The tree is not a git repository, so the release
   string is exactly the upstream version (`uname -r` prints `7.2.15`).
5. Writes `dist/kernel/vmlinux-x86_64`, `dist/kernel/config-x86_64` and their
   `SHA256SUMS`.

The asset name `vmlinux-x86_64` is the one the engine and the installer already
use. The engine boots it with PVH direct boot, as it booted Cloud Hypervisor's
build.

## The patch

`regenerate-patch.sh` takes the diff of Cloud Hypervisor's branch from
`Linux 7.2.8` to the commit behind their `ch-release-v7.2.8-20261009`, limited
to these paths:

| Kept | Why |
| --- | --- |
| `drivers/watchdog/{Kconfig,Makefile,virtio_wdt.c}` | The virtio watchdog driver (`CONFIG_VIRTIO_WDT=y` in their config) |
| `init/main.c`, `include/linux/amba/serial.h`, `drivers/tty/serial/amba-pl011.c`, `arch/arm64/kernel/head.S` | Boot-time trace points. Only the `init/main.c` port writes are built on x86_64; the arm64 parts are kept so the commits stay whole |
| `arch/x86/configs/ch_defconfig` | Their x86_64 configuration |

| Left out | Why |
| --- | --- |
| `drivers/virt/vmgenid.c` | "virt: vmgenid: remap memory as decrypted" is a stable backport and is already in 7.2.15 |
| `arch/arm64/configs/ch_defconfig`, `arch/riscv/configs/ch_defconfig` | Other architectures |
| `README.md`, `.github/workflows/release.yaml` | Their documentation and CI |

The script fails if the series touches a path it does not classify.

## Release and tags

A release is tagged `guest-kernel-<VERSION>-<REVISION>`, for example
`guest-kernel-7.2.15-q1`. The tag must equal
`build-kernel-in-container.sh --print-tag`. The workflow refuses any other tag.
A new upstream version resets the revision to `q1`. A new recipe for the same
version (patch, config or image change) takes the next revision.

`.github/workflows/guest-kernel.yml` runs only on such a tag or by hand (a dry
run that uploads workflow artifacts only, ADR 0082). On a tag it creates a
**draft prerelease** with:

- `vmlinux-x86_64` and `config-x86_64`
- `cloud-hypervisor.patch`
- the upstream `linux-<VERSION>.tar.xz` and `linux-<VERSION>.tar.sign`
- `SHA256SUMS` over those five files
- `guest-kernel-<VERSION>-<REVISION>.sigstore.jsonl`, the attestation bundle
  covering `vmlinux-x86_64`, `config-x86_64` and `cloud-hypervisor.patch`

It is a prerelease and never marked latest, because `install.sh` resolves the
latest release to find Quoro itself. When publishing, keep it that way:

```sh
gh release edit guest-kernel-7.2.15-q1 --draft=false --latest=false
```

Then bump `crates/engine/pins.txt` in its own pull request. The engine and the
installer verify the kernel by the sha256 in `pins.txt`. That file ships inside
the attested Quoro tarball. The kernel's own attestation is a check for the
maintainer before the bump:

```sh
gh attestation verify vmlinux-x86_64 \
  --bundle guest-kernel-7.2.15-q1.sigstore.jsonl \
  --repo OmarAlex24/quoro \
  --signer-workflow OmarAlex24/quoro/.github/workflows/guest-kernel.yml \
  --source-ref refs/tags/guest-kernel-7.2.15-q1 \
  --deny-self-hosted-runners
```

## Build or reproduce locally

Needs an x86_64 Linux host with podman (preferred) or docker. Expect about
3 GB in `target/kernel-build/` and 10 to 40 minutes, depending on the CPU count.

```sh
QUORO_KERNEL_JOBS=4 packaging/kernel/build-kernel-in-container.sh
cat dist/kernel/SHA256SUMS
```

Rebuild a published kernel and compare it with the release:

```sh
TMPDIR=$HOME/kernel-reproduce packaging/kernel/reproduce-kernel.sh guest-kernel-7.2.15-q1
```

It prints both hashes of each file and `REPRODUCIBLE` or `MISMATCH`.
`--ref <sha> --sums <SHA256SUMS>` rebuilds a dry run's commit instead. The job
count does not change the output.

## Bumping

1. Pick the newest stable release of the series from
   `https://www.kernel.org/releases.json`. Update `VERSION`, `TARBALL_URL`,
   `TARBALL_SHA256` (also listed in kernel.org's signed `sha256sums.asc`),
   `SIGNATURE_URL` and `KBUILD_BUILD_TIMESTAMP` (the release date). Reset
   `REVISION` to `q1`.
2. Build. If the patch no longer applies, or Cloud Hypervisor has published a
   newer series, move `CH_BASE` and `CH_HEAD` and run `regenerate-patch.sh`.
3. Read the `diffconfig` output in the build log and explain every line in the
   pull request.
4. Tag, publish and bump `pins.txt` as above, then boot an app, open its
   terminal and run a Git build on the lab.

## Licence

The kernel is GPL-2.0-only. It is a separate artifact that the engine passes to
Cloud Hypervisor as a file. It is not linked into Quoro, so Quoro's AGPL-3.0
licence does not extend to it and the reverse does not apply either. Each release attaches
its corresponding source: the upstream tarball and its signature, the patch,
the config and, at the tag, this directory. `patches/cloud-hypervisor.patch`
is GPL-2.0-only, like the kernel files it changes.

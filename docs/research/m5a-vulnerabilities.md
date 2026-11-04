# M5a spike: vulnerability tracking (SBOMs, OSV data, matching) and the guest kernel

- **Question:** ADR 0138 asks for a short spike before vulnerability tracking is built. It must cover how a node produces a package inventory (SBOM) of the root filesystem it already unpacks, in Rust or with a pinned `syft` binary, under the license policy (ADR 0080); how large OSV's data is, how often it changes, and what an offline cluster does; how the primary matches inventories against it; and the guest kernel problem in issue #211: is a newer Cloud Hypervisor guest kernel available, and what would building our own cost?
- **Researched:** 2026-11-04. Desk research, OSV's public bucket and API, and the GitHub API. **No prototype ran** inside Quoro. Sizes and query results below were measured on the access date.
- **Status:** design proposal for the M5a contract PR; the choices are raised as proposals for the maintainer.

## Short answer

1. **Guest kernel: no newer Cloud Hypervisor build exists; build our own.** The newest Cloud Hypervisor guest kernel release is still `ch-release-v7.2.8-20261009`. Upstream stable is 7.2.15 (2026-11-01), and CVE-2026-98432 is fixed from 7.2.10. Cloud Hypervisor's branch is upstream v7.2.8 plus 19 commits, mostly config and CI. The code changes are a paravirtualised virtio watchdog driver (three commits), boot-time trace points and a `vmgenid` fix. Their build is `make ch_defconfig` then `make`. Rebasing those commits onto 7.2.15 and building in the pinned release container (ADR 0090) is a small, repeatable job.
2. **Inventory: write it in Rust, in the engine's image path.** The engine already unpacks every layer into a directory before it builds the ext4 root disk. Reading `dpkg`, `apk` and `rpm` databases plus a few language manifests there costs one walk of a tree that is already on disk. No image leaves the node, and no extra binary ships. A pinned `syft` (v1.56.0, Apache-2.0, 31 MB tarball) would cover more ecosystems, but it adds a Go binary that parses untrusted image content as root on the host, and its dependency licenses are unverified.
3. **Match on the source package name, never the binary name.** OSV's Debian and Alpine records are keyed by *source* package. A query for Debian `libssl3t64` returns 0 vulnerabilities, while the same version under `openssl` returns 62. The inventory must record the source name: `dpkg`'s `Source:` field, `apk`'s `o:` origin line.
4. **OSV data: download per ecosystem, refresh incrementally, import offline.** `all.zip` is 2.87 GB, so never use it. The ecosystems Quoro needs are small, except Ubuntu (809 MB) and Wolfi (309 MB), which are fetched only when an image of that distribution appears. `modified_id.csv` per ecosystem (about 3.7 MB for Debian) gives a daily delta.
5. **Kernel matching works from the data, not the public API.** OSV's record for CVE-2026-98432 carries `ECOSYSTEM` ranges for package `Kernel` in ecosystem `Linux` (fixed in 6.12.113, 6.18.56 and 7.2.10, among others). The `v1/query` API returned nothing for `Kernel` 7.2.8 or 6.1.100 on the access date. A local matcher over the downloaded `Linux` ecosystem flags the guest kernel and host kernels directly.

## Findings

### 1. The guest kernel (issue #211)

| Item | Value | Label |
| --- | --- | --- |
| Pinned guest kernel | `ch-release-v7.2.8-20261009` (`crates/engine/pins.txt`) | Confirmed |
| Newest Cloud Hypervisor kernel releases | `ch-release-v7.2.8-20261009` (2026-10-09), `-20261002`, then 6.16.9 builds back to 2025-11 | Confirmed (GitHub API) |
| Cloud Hypervisor VMM releases | v54.0 (2026-10-14), v53.0 (2026-07-12), v52.0 (2026-05-14): roughly every two months; kernel builds have followed VMM releases, not upstream stable | Confirmed (dates); cadence is an inference |
| Upstream stable 7.2 | 7.2.15, released 2026-11-01; 7.3.1 stable; 6.18.60 and 6.12.115 longterm | Confirmed (kernel.org `releases.json`) |
| CVE-2026-98432 fix | OSV ranges: `[6.19.0, 7.2.10)` among others; CVSS 3.1 `AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H` | Confirmed (OSV) |
| Branch `ch-7.2.8` | Upstream `Linux 7.2.8` plus 19 commits. Code: virtio watchdog driver and two fixes, two boot-time trace and debug commits, `vmgenid: remap memory as decrypted`. Everything else: defconfigs, CI, README | Confirmed (GitHub API) |
| Their build | `make ch_defconfig`, then `make bzImage` on `ubuntu-22.04`; the release uploads `vmlinux` and `bzImage` (`.github/workflows/release.yaml` on `ch-7.2.8`) | Confirmed |
| Upstream signatures | None on the assets, as for the VMM (`pins.txt` comment) | Confirmed |

**Building our own** means a `kernel/` directory or a separate workflow that does the following:

- Check out upstream `v7.2.N` at a pinned commit and apply the 19 Cloud Hypervisor commits as patches. Their `ch_defconfig` is one of them.
- Build `vmlinux` in the digest-pinned Ubuntu 24.04 release container (ADR 0090), with `KBUILD_BUILD_TIMESTAMP`, `KBUILD_BUILD_USER` and `KBUILD_BUILD_HOST` fixed so two builds produce the same bytes.
- Attest the result like the release (ADR 0081) and bump `pins.txt` in its own PR.

Cost estimate (**Hypothesis**): the patches touch drivers and config, not the network stack, so they should apply to 7.2.15 without conflicts. A defconfig build of a minimal guest kernel takes minutes on a CI runner. The recurring cost is bumping to each stable release that fixes something relevant, which the vulnerability data itself can flag. Before building, check the new kernel still has what M4 depends on: virtio-vsock (ADR 0113), overlayfs and cgroup v2 for builds. The defconfig comes from the same branch, so this is expected but must be tested.

### 2. OSV data (measured 2026-11-04)

| Ecosystem zip | Size | Last modified (UTC) |
| --- | --- | --- |
| `all.zip` | 2,872,681,806 bytes | 2026-11-04 17:49 |
| Debian | 80,945,403 | 2026-11-04 17:34 |
| Alpine | 9,059,525 (10,647 records, 43.7 MB unpacked) | 2026-11-04 09:33 |
| Ubuntu | 808,902,661 | 2026-11-04 09:33 |
| Red Hat | 27,716,369 | 2026-10-28 16:48 |
| Wolfi | 309,183,359 | 2026-11-04 17:34 |
| Linux (kernel) | 59,025,439 | 2026-11-04 17:34 |
| npm | 219,941,498 | 2026-11-04 17:34 |
| PyPI | 37,630,473 | 2026-11-04 17:48 |
| Go | 12,714,819 | 2026-11-04 17:34 |
| Maven | 10,512,433 | 2026-11-04 17:18 |
| crates.io | 3,778,692 | 2026-11-04 06:48 |
| RubyGems | 5,213,495 | 2026-11-04 03:33 |
| Packagist | 11,087,676 | 2026-11-04 00:18 |

- **Cadence.** Most ecosystems changed within the hour of the check. `modified_id.csv` lists `(modified time, id)` newest first: 3.7 MB for Debian, 3.6 MB for Ubuntu, 158 MB for the whole bucket. A daily refresh reads the per-ecosystem CSV up to the last seen time and fetches only the changed records (`<ecosystem>/<id>.json`). **Confirmed** (bucket listing).
- **Record shape.** Distribution records carry `affected[].package {name, ecosystem: "Debian:13" | "Alpine:v3.22" …, purl}` and `ECOSYSTEM` ranges with `introduced` and `fixed` versions. Severity is a CVSS vector when present. **Confirmed.**
- **Source names.** Debian `libssl3t64` 3.5.1-1 returned 0 vulnerabilities and `openssl` 3.5.1-1 returned 62. Alpine `libcrypto3` returned 0 and `openssl` returned 48 (OSV `v1/query`). **Confirmed.**
- **Exploit signals.** CISA KEV is a 2.8 MB JSON file (`known_exploited_vulnerabilities.json`, modified 2026-11-03). EPSS publishes a daily CSV behind a redirect. Size was not measured. **Hypothesis** on EPSS size.
- **Privacy.** OSV's online API would need the package list sent to a third party. ADR 0138 says no image leaves the node, and the same spirit argues against sending inventories off the cluster. A local mirror avoids it.

### 3. SBOM generation options

| | Own Rust inventory in the engine | Pinned `syft` binary on nodes | `osv-scanner` binary |
| --- | --- | --- | --- |
| Version, license | Our code (AGPL-3.0-only) | v1.56.0 (2026-11-02), Apache-2.0; its Go dependency licenses not checked | v2.7.0, Apache-2.0, 57.7 MB static binary |
| Size shipped | None extra | ~31 MB tarball per arch | ~58 MB |
| Where it runs | Inside `prepare_image` / `prepare_layout`, on the unpacked tree, before `mkfs.ext4 -d` (`crates/engine/src/image/`) | A separate process over the unpacked directory (`syft dir:<path> -o cyclonedx-json`) | Same, plus matching |
| Coverage in M5a | dpkg (`/var/lib/dpkg/status` and `status.d/` for distroless), apk (`/lib/apk/db/installed`), rpm (`rpmdb.sqlite`), `os-release`; npm `package-lock.json`, Python `*.dist-info/METADATA`, Go build info in binaries, Rust `cargo auditable` sections | Dozens of ecosystems | Same family as syft |
| Risk | Parser bugs are ours, but memory-safe and in-process | Untrusted input parsed by a large Go program run as root by the node agent | Same |
| ADR 0080 | Rust deps checked by cargo-deny | A separate binary, outside cargo-deny; its licenses must be recorded like Cloud Hypervisor's | Same |

Version comparison crates for the matcher: `debversion` 0.5.5 (Apache-2.0) for Debian, `pep440_rs` 0.7.3 (Apache-2.0 OR BSD-2-Clause) for PyPI, `cvss` 3.0.0 (Apache-2.0 OR MIT) for scores. Alpine's and RPM's version orderings are small enough to write. The only RPM comparator crate found (`rpmvercmp`) is BSL-1.0, which is not on the ADR 0080 allowlist (crates.io, 2026-11-04).

## Design (Hypothesis until built)

- **Layer 1, what Quoro ships.** The release workflow emits a CycloneDX SBOM of the workspace (Rust crates) plus the pinned components (guest kernel, Cloud Hypervisor, BuildKit image, cloudflared). It is attested with the release (ADR 0081). The primary embeds its own list and matches it daily. A finding shows "this Quoro has known vulnerabilities; upgrade to X" once a release fixes them. CI keeps RustSec through cargo-deny.
- **Layer 2, app images.** The engine writes `inventory.json` next to `image.json` in the image cache: `{os: {id, version_id}, packages: [{ecosystem, name, source_name?, version, purl, path?}]}`, bounded to 20,000 packages and 4 MiB. The node sends it once per image digest in a new protocol message (or a `status_report` addition). The primary stores it per digest and maps digests to apps through the deployments it already knows.
- **The mirror.** The primary keeps a local, non-replicated SQLite index of the OSV ecosystems that appear in any inventory, plus `Linux` for kernels. It downloads each ecosystem zip once, refreshes daily from `modified_id.csv`, and records the data age. An offline cluster imports a directory of zips with `quoro vuln import <dir>`, and the panel shows how old the data is.
- **Matching.** Map `(ecosystem with release, source name or name)` to records, then evaluate `ECOSYSTEM` ranges with the ecosystem's version order. Store findings `(app_id, image_digest, vuln_id, package, installed, fixed?, severity, kev, epss)` with first-seen and resolved times.
- **Incident.** `app.vulnerable` fires on a new finding that is in KEV or has CVSS 9.0 or more. It opens an incident like `app.down`, so the webhook wake path applies (ADR 0032, ADR 0103).
- **Patch.** "Rebuild with refreshed base" is a build started with a `refresh_base` flag: BuildKit resolves every `FROM` again (`image-resolve-mode=pull`) instead of reusing the cached base. Image apps get the finding and the fixed version only.
- **Kernels.** The guest kernel is matched like any other component of layer 1. Host kernels are already reported (`nodes.kvm.kernel`), so they are matched against `Linux`/`Kernel` and shown. Quoro never upgrades a host (ADR 0138). As an example, lab-2's host kernel (6.12.113) is at the fixed version for CVE-2026-98432, and lab-1's (6.12.115) is above it.

## Open risks and unknowns

- **Distribution kernels are not upstream versions.** Debian's `6.12.113+deb13` carries backports, and OSV's `Linux` ranges are upstream versions. For host kernels the finding must say "by upstream version", and the `Debian` ecosystem's `linux` source package is the better source when the host is Debian.
- **Coverage gaps.** Static binaries without build info, vendored libraries, and images without a package database report nothing. The panel must say "not inventoried", not "no vulnerabilities".
- **False positives** from fixed-in-backport cases, and from records without a `fixed` event. An acknowledge or ignore action with an audit record will be wanted soon after M5a.
- **Ubuntu and Wolfi sizes.** The first Ubuntu image on a cluster costs an 809 MB download on the primary. Downloading only the release in use is not possible from the bucket layout (**Hypothesis**), so it is the whole ecosystem zip.
- **Own kernel build:** patch conflicts on later stable releases, keeping the defconfig in step with Cloud Hypervisor's, and arm64 if Quoro ever ships it.

## Sources

- Issue #211 and ADR 0138: `docs/adr/0138-vulnerability-tracking-and-patching-in-m5a-quoro-knows-the.md`
- Cloud Hypervisor guest kernel releases: https://github.com/cloud-hypervisor/linux/releases (GitHub API, accessed 2026-11-04)
- Cloud Hypervisor kernel branch and build workflow: https://github.com/cloud-hypervisor/linux/tree/ch-7.2.8 and `.github/workflows/release.yaml` (accessed 2026-11-04)
- Cloud Hypervisor VMM releases: https://github.com/cloud-hypervisor/cloud-hypervisor/releases (accessed 2026-11-04)
- Kernel releases: https://www.kernel.org/releases.json (accessed 2026-11-04)
- OSV data dumps and `modified_id.csv`: https://google.github.io/osv.dev/data/ and https://osv-vulnerabilities.storage.googleapis.com/ (accessed 2026-11-04)
- OSV API: https://google.github.io/osv.dev/api/ and the record https://api.osv.dev/v1/vulns/CVE-2026-98432 (accessed 2026-11-04)
- OSV schema: https://ossf.github.io/osv-schema/ (accessed 2026-11-04)
- CISA KEV feed: https://www.cisa.gov/known-exploited-vulnerabilities-catalog (accessed 2026-11-04)
- EPSS: https://www.first.org/epss/ (accessed 2026-11-04)
- syft releases and license: https://github.com/anchore/syft (GitHub API, accessed 2026-11-04)
- osv-scanner releases and license: https://github.com/google/osv-scanner (GitHub API, accessed 2026-11-04)
- Crates: https://crates.io/crates/debversion, https://crates.io/crates/pep440_rs, https://crates.io/crates/cvss, https://crates.io/crates/rpmvercmp, https://crates.io/crates/cyclonedx-bom (accessed 2026-11-04)
- Engine image path: `crates/engine/src/image/mod.rs`; pins: `crates/engine/pins.txt`

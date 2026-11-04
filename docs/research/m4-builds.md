# M4 builds spike: BuildKit in a builder microVM, Railpack and Nixpacks, and the Builder contract

- **Question:** can a BuildKit daemon run inside an ephemeral microVM under Cloud Hypervisor with Quoro's kernel and init, and produce an OCI layout the node feeds into the OCI-to-rootfs path (ADR 0060, ADR 0061)? Are Railpack and Nixpacks still viable zero-config builders? What shape should the Builder interface and the M4 contract take?
- **Researched:** 2026-11-03, on lab-3 (nested virtualization, 2 vCPU / 4 GB), with the pinned Cloud Hypervisor v54.0, the pinned guest kernel (`ch-release-v7.2.8-20261009`), the initramfs the node agent built from `crates/init`, and an unmodified M3 node agent running next to the spike.
- **Status:** answered. Prototype code stayed outside the repository and was deleted from the lab afterwards.

## Short answer

1. **BuildKit in a builder microVM: works, with four additions.** `buildkitd` v0.34.2 (rootful, `runc` executor, `overlayfs` snapshotter) ran as the app under our init and built every Dockerfile tried: a tiny Alpine image, a Node app, a multi-stage Go app cloned from Git by BuildKit itself, and a deliberately failing build. `buildctl --output type=oci` wrote an OCI layout tarball whose layers are all `tar+gzip` and whose config carries one `diff_id` per layer. A scratch importer that reuses the engine's own `layer::apply_layer`, `ext4::build` and `cache::write_entry` turned four of those layouts into root disks, and each booted under our init and answered **HTTP 200**. No kernel change was needed. The four additions: (a) a local-layout source in `ImageStore` (today it only pulls from a registry); (b) a fourth disk for the BuildKit state, the build context and the output, because the guest root is an overlay and the engine rejects extra disks until M5a; (c) mounting `cgroup2` on `/sys/fs/cgroup`, which our init does not do; (d) a way for logs and the result to leave the VM, which the existing tagged serial output already covers.
2. **Railpack: viable second Builder; Nixpacks: no.** Nixpacks' README states it is in maintenance mode and points to Railpack. Its last release, v1.41.0, came out on 2025-10-24, and it builds by shelling out to `docker build`. Railpack (MIT, v0.43.0 on 2026-10-19, releases every few days) ships a **BuildKit gateway frontend**: a JSON plan replaces the Dockerfile, the frontend turns it into LLB, and no Dockerfile is generated. It ran inside the same builder microVM with no change to the VM and produced an image that booted and answered 200. Both projects are MIT, which is on the ADR 0080 allowlist. Neither is linked into Quoro: they run inside the builder VM as separate binaries and images.
3. **Recommendation:** a node-agent `Builder` trait with one implementation per frontend (Dockerfile first, Railpack later). It takes a source, a build spec and limits, and returns a stream of build events and, on success, an OCI layout on the node. Builds run on the node that will run the app, in an ephemeral builder microVM created through the Engine trait. Logs leave the VM as tagged lines on the serial console, as app logs already do, and reach the panel over SSE through the existing `stream_open` path. The contract details and the risks are in [Implications for Quoro](#implications-for-quoro) and [Open risks](#open-risks-and-unknowns).

Every number below comes from nested virtualization on one shared physical host. The roadmap says the lab is not valid for benchmarks, so read the figures as orders of magnitude.

## Findings

### 1. How the spike was run

The spike drove `cloud-hypervisor` directly with the same boot payload and disk layout the engine uses (`crates/engine/src/ch/vmconfig.rs`). The production node agent kept running its own VM the whole time, and Quoro was never stopped.

| Item | Value |
| --- | --- |
| Kernel and cmdline | Pinned `vmlinux-x86_64`, `console=ttyS0 loglevel=4 panic=-1 rdinit=/init` |
| Initramfs | The node agent's `initramfs.cpio` (our `quoro-init`) |
| `quoro-root` | `moby/buildkit:v0.34.2@sha256:97719ccb2ac86fb44fd36f330a6d302f63dddd5326f3ada24f9c05ccac0d9098`, flattened into a 294 MiB read-only ext4 with `mkfs.ext4 -d` |
| `quoro-data` | Fresh 4 GiB sparse ext4 per VM (the overlay upper) |
| `quoro-config` | Written by hand from the grammar in `crates/init/README.md` |
| Fourth disk, serial `quoro-work` | 12 GiB sparse ext4 holding `run.sh`, the build context, `/var/lib/buildkit` and the output tarball |
| Network | A separate bridge, TAP and nftables masquerade table on a private /24, removed afterwards; guest DNS from the config disk |
| Builder VM size | 2 vCPU, 2048 MiB unless noted |

The config disk's argv was `/bin/sh -c '<find the disk whose serial is quoro-work, mount it on /mnt/work, exec /bin/sh /mnt/work/run.sh>'`. The environment was the image's `PATH` plus `BUILDKIT_SETUP_CGROUPV2_ROOT=1`, which the BuildKit image sets so that `buildkitd` moves itself out of the root cgroup. `run.sh` mounts `cgroup2`, starts `buildkitd --root /mnt/work/buildkit --oci-worker-net=host`, waits for `buildctl debug workers`, runs one `buildctl build`, prints the result and exits. Our init then reports the exit code in the status region and powers the VM off, and the `cloud-hypervisor` process exits.

### 2. What the guest needs

| Requirement | Observed in the guest | Label |
| --- | --- | --- |
| Kernel | 7.2.8 (pinned build) | Confirmed |
| overlayfs | `overlay` in `/proc/filesystems`; BuildKit picked the `overlayfs` snapshotter on the ext4 work disk | Confirmed |
| cgroups | cgroup v2 available; controllers `cpuset cpu io memory hugetlb pids` once mounted. **Our init does not mount it**: `run.sh` did | Confirmed |
| Namespaces | `cgroup ipc mnt net pid pid_for_children user uts`; `runc` created its containers with no error | Confirmed |
| User namespaces | `max_user_namespaces 7909` (not exercised: the rootful daemon was used) | Confirmed (present) |
| Kernel config | `/proc/config.gz` is absent, so features were probed by use, not read from a config | Confirmed |
| Executor network | `--oci-worker-net=host`, so `RUN` steps share the VM's single interface; no CNI, no veth, no bridge inside the guest | Confirmed |
| Base-image pulls and Git | Through the VM's NAT: Docker Hub, `ghcr.io`, `proxy.golang.org`, the npm registry and `github.com` all worked | Confirmed |
| Disk | The guest root stays tiny: `/` is an overlay whose upper is the data disk. BuildKit state needs its own ext4: 13 MB (tiny), 258 MB (Node), 688 MB (Go), 2.6 GB (Railpack Node) | Confirmed |
| Memory | Peak of the BuildKit cgroup: 650 MB for the Go build at 2048 MiB, 548 MB at 1024 MiB. The Node build succeeded with 1 vCPU and 384 MiB | Confirmed |

The init passes `/proc/self/status` with `Seccomp: 0` to the app. The VM is the isolation boundary here (ADR 0008). Whether `runc` applies BuildKit's default seccomp profile inside the VM was not checked.

### 3. Measurements

Cold means a fresh work disk: empty BuildKit cache, base images pulled. Warm means the same work disk on a new VM with an unchanged context. "Change" means one source file edited. Times are wall-clock for the whole VM, from `cloud-hypervisor` start to its exit. The guest phase columns are `/proc/uptime` markers, so boot means kernel plus init up to the app's first line.

| Build | VM wall (s) | Boot to app (s) | `buildkitd` ready (s) | Build and export (s) | OCI tarball |
| --- | --- | --- | --- | --- | --- |
| Tiny (`alpine:3.22` + `apk add busybox-extras`), cold | 6.69 | 0.29 | 0.38 | 5.65 | 3.8 MB |
| Tiny, warm | 2.64 | 0.31 | 0.27 | 1.74 | 3.8 MB |
| Node (`node:22-alpine`, `npm install express`), cold | 27.28 | 0.30 | 0.34 | 25.56 | 64 MB |
| Node, warm | 3.08 | 0.28 | 0.21 | 1.91 | 64 MB |
| Node, one source file changed | 4.22 | 0.29 | 0.24 | 2.77 | 64 MB |
| Node, cold, 1 vCPU / 384 MiB | 26.67 | 0.27 | 0.55 | 25.17 | 64 MB |
| Go (`traefik/whoami` v1.12.0 from Git, multi-stage to `scratch`), cold | 57.12 | 0.30 | 0.29 | 55.85 | 5.4 MB |
| Go, warm | 3.98 | 0.29 | 0.28 | 2.74 | 5.4 MB |
| Go, cold, 2 vCPU / 1024 MiB | 55.83 | 0.26 | 0.31 | 54.56 | 5.4 MB |
| Failing build (step 3 of 4 exits 1), cache warm | 2.69 | 0.31 | 0.20 | 1.80 | none |
| Railpack Node, cold, 1536 MiB, while a compile ran on the host | 126.39 | 0.28 | 0.40 | 9.6 (prepare) + 114.2 | 152 MB |
| Railpack Node, warm | 18.31 | 0.28 | 0.32 | 10.3 (prepare) + 4.8 | 152 MB |

The fixed cost of an ephemeral builder VM is about 0.3 s to boot and 0.2 to 0.4 s for `buildkitd` to come up. Cold builds are dominated by downloads and compilation. A warm cache on a persistent work disk brings typical rebuilds down to 2 to 4 s. In an earlier variant of the tiny image, a warm cached rebuild produced the same manifest digest as the cold build, because BuildKit reused the cached export.

Import into the engine's cache with the scratch importer (verify every blob digest, apply the layers with `openat2(RESOLVE_IN_ROOT)`, `mkfs.ext4 -d`, write `image.json`):

| Image | Import (s) | Root disk | Boot to first HTTP 200 |
| --- | --- | --- | --- |
| Tiny (busybox `httpd` on 8080) | 0.25 | 45 MB | 200 within 1.2 s |
| Node (`node server.js` on 3000) | 4.45 | 258 MB | 200 within 1.2 s |
| Go (`/whoami` on 80) | 0.32 | 52 MB | 200 within 1.2 s |
| Railpack Node (`npm run start` on 3000) | 11.12 | 599 MB | 200 within 1.2 s |

The app VMs used 1 vCPU and 512 MiB, our init, and the config the engine would derive from `image.json` (entrypoint plus cmd, env, workdir). An earlier tiny image failed with `quoro-init: FATAL entrypoint "httpd" not found`, because Alpine 3.22's busybox has no `httpd` applet. The init's failed-boot diagnostic surfaced the mistake as designed. The Dockerfile then added `busybox-extras`.

### 4. Logs and the failing step

- **Transport: works with the existing path.** `buildctl --progress=plain` writes to the app's stdout and stderr. Our init tags every line (`out ` or `err `) on the serial console, interleaved with `quoro-init:` diagnostics, exactly as for app logs (engine-trait.md section 6). Nothing else is needed to get the log out of the VM. Every step line carries BuildKit's vertex number (`#6 ...`). **Confirmed.**
- **Failing step: available in two forms.** The plain output ends with the failing vertex, its last log lines and the Dockerfile excerpt:

  ```
  #6 [3/4] RUN echo "about to fail" && ls /does-not-exist
  #6 0.187 ls: /does-not-exist: No such file or directory
  #6 ERROR: process "/bin/sh -c echo \"about to fail\" && ls /does-not-exist" did not complete successfully: exit code: 1
  Dockerfile:3
  --------------------
     2 |     RUN echo "step one ok"
     3 | >>> RUN echo "about to fail" && ls /does-not-exist
     4 |     RUN echo "never reached"
  --------------------
  ```

  With `--progress=rawjson` each status update is one JSON object, so the failing step is machine-readable: the vertex's `digest`, `name` (`[3/4] RUN ...`), `started`, `completed` and `error`, plus per-vertex `logs` entries. The Dockerfile excerpt is still printed as text. **Confirmed.**
- **Result:** `buildctl` exits 1, the app exits 1, and the init writes `exit=1` to the status region, so the node sees a failed build without parsing the log. `--metadata-file` gives the manifest digest (`containerimage.digest`) on success. **Confirmed.**
- **Output: the shared work disk works.** The OCI tarball, the metadata file and the BuildKit cache sit on the work disk, which the node reads after the VM exits. One wrinkle: the init powers off without unmounting that disk, so the host had to mount it with `noload` (journal not replayed). The builder's `run.sh`, or the init, must unmount it, or the node must run `e2fsck -p` before reading it. **Confirmed.**
- **vsock and virtio-fs** were not needed. vsock would allow a structured channel later, but adds a guest agent. virtio-fs needs a `virtiofsd` per share (see `cloud-hypervisor.md`). **Hypothesis**, not tested.

### 5. OCI layout compatibility with the engine pipeline

| Check in the engine pipeline | BuildKit `type=oci` output | Label |
| --- | --- | --- |
| Layer media type must be gzip (`layer.rs` rejects zstd by name) | `application/vnd.oci.image.layer.v1.tar+gzip` for every layer, Railpack's included (BuildKit's default compression is gzip) | Confirmed |
| `diff_ids.len() == layers.len()` | 2/2, 11/11 and others, all equal | Confirmed |
| Manifest selection | `index.json` with one `manifest.v1+json` entry, `platform: linux/amd64`; no nested index because attestations were not requested | Confirmed |
| Blob digests | Every blob matches its digest | Confirmed |
| Config fields | `Entrypoint`, `Cmd`, `Env`, `WorkingDir`, `ExposedPorts` mapped by `parse_image_config` as for registry images | Confirmed |
| Source for `ImageStore` | **Missing:** `registry::pull` is the only source, and `oci-client` speaks HTTPS to a registry. The scratch importer (about 90 lines) reads `index.json`, follows an index if one is present, verifies blobs, and reuses `apply_layer`, `ext4::build`, `cache::write_entry` and `cache::commit` unchanged | Confirmed |

### 6. Rootless and daemonless

- **Rootful inside the VM is the recommended mode.** Rootless BuildKit protects the host from the daemon. Here the microVM already does that (ADR 0008), and rootless would add user-namespace mapping, `rootlesskit` and the `--oci-worker-no-process-sandbox` trade-offs for no gain. Rootless was **not tested**.
- **Daemonless works.** The image ships `buildctl-daemonless.sh`. The spike's `run.sh` follows the same pattern (start `buildkitd`, wait, build, stop), so the builder VM needs no long-lived daemon. **Confirmed.**
- **Licenses of what runs inside the builder VM:** BuildKit Apache-2.0, runc Apache-2.0 (GitHub repository metadata). They are separate binaries in a separate image, not Rust dependencies, so cargo-deny does not see them. They should still be recorded the way the Cloud Hypervisor binary is. **Confirmed** (licenses).
- **Supply chain:** BuildKit's v0.34.2 release publishes `provenance.json`, `sbom.json` and a `sigstore.json` bundle per asset, which fits ADR 0027 better than an unpinned `moby/buildkit:latest` would. The builder image must be pinned by digest the way `pins.txt` pins Cloud Hypervisor and the kernel. **Confirmed** (assets exist). Whether the image itself carries attestations was not checked.

### 7. Railpack and Nixpacks (re-verification for ADR 0060)

| Item | Nixpacks | Railpack | Label |
| --- | --- | --- | --- |
| Status | README: "**Maintenance Mode:** This project is currently in maintenance mode and is not under active development. We recommend using Railpack as a replacement." | README: "It is the successor to Nixpacks" | Confirmed |
| Latest release | v1.41.0, 2025-10-24; last commit on `main` 2025-11-24 | v0.43.0, 2026-10-19 (v0.42.0 on 10-15, v0.41.0 on 10-13); last push 2026-11-03 | Confirmed |
| License | MIT (`LICENSE`, Railway Corp.) | MIT (`LICENSE`, Railway Corp.) | Confirmed |
| ADR 0080 allowlist | MIT is allowed | MIT is allowed | Confirmed |
| Output | `nixpacks build` runs `docker build` (`src/nixpacks/builder/docker/docker_image_builder.rs`, `Command::new("docker")`); `--out` saves the build directory instead of building | `railpack prepare` writes a JSON plan (`--plan-out`) and an info file (`--info-out`); the frontend image `ghcr.io/railwayapp/railpack-frontend` turns the plan into LLB through `buildctl --frontend=gateway.v0 --opt source=...` | Confirmed |
| Runs in our builder VM | Not tried: maintenance mode and the Docker dependency rule it out | **Yes**: `railpack prepare` (static musl binary on the work disk) plus the gateway frontend built a Node app with no Dockerfile; the image booted and answered 200 | Confirmed |
| Cost | n/a | Images are large (599 MB root disk for an Express app), the cold build pulled builder and runtime images (2.6 GB cache), and `prepare` took about 10 s even warm | Confirmed |
| Production guidance | n/a | Railway's docs recommend the frontend over the CLI for production; the version of the plan and the frontend must match; secrets are passed by name in the plan and by value at build time; `--build-arg cache-key=` isolates caches between tenants | Confirmed |

Railpack fits ADR 0060's "second Builder implementation" with no change to the builder VM: it is a different frontend and a `prepare` step in front of the same `buildctl` call. The ADR 0060 hypothesis that Nixpacks is in maintenance and Railpack is its successor is now confirmed.

## Versions checked

| Component | Version | Source |
| --- | --- | --- |
| BuildKit | v0.34.2 (2026-11-03), image digest above | GitHub releases API, Docker Hub via `crane digest` |
| Cloud Hypervisor | v54.0 (pinned) | `crates/engine/pins.txt` |
| Guest kernel | `ch-release-v7.2.8-20261009` (pinned) | `crates/engine/pins.txt` |
| crane (used only to flatten the builder image) | v0.22.1 | GitHub releases API |
| Railpack | v0.43.0, frontend `ghcr.io/railwayapp/railpack-frontend:v0.43.0@sha256:355ef61fae6f2551103cd8b10c710cb07167dd353d561f15c69b8d7ac1c676cc` | GitHub releases and packages API |
| Nixpacks | v1.41.0 | GitHub releases API |

## Implications for Quoro

- **ADR 0060: the hypothesis holds.** Dockerfile builds by BuildKit inside an ephemeral builder microVM work under Cloud Hypervisor with our kernel and init, and the output feeds the OCI-to-rootfs path once `ImageStore` gets a local-layout source. Railpack is confirmed as Nixpacks' successor and runs in the same VM.
- **Where builds run.** On the node that will run the app, in an ephemeral builder microVM created through the Engine trait, one VM per build. The layout is produced and imported on the same node, so no registry is needed and nothing is pushed anywhere. That matches ADR 0008 (no Docker on nodes) and ADR 0014 (logs stay on the node). The cost is that every node needs builder capacity (about 2 vCPU, 1 to 2 GiB and a cache disk while a build runs), and a build cache is per node.
- **Engine contract gaps (an additive Engine-trait change, standalone PR per ADR 0036):**
  1. `prepare_image` from a local OCI layout: an `ImageSource::Layout { path }` next to the registry reference, or a separate `import_layout` method, both cached by manifest digest.
  2. An extra writable disk on `VmSpec` for the build, identified by serial (for example `quoro-work`). `VolumeAttach` is rejected by the Cloud Hypervisor backend until M5a ("volumes ship in M5a"), so either a dedicated "scratch disk" field or an early, node-local subset of volumes.
  3. Either `cgroup2` mounted by the init (a small init change that is harmless for apps) or a documented builder entrypoint that mounts it. Mounting it in the init is simpler and also helps app images that expect it.
  4. Unmount, or `syncfs` plus remount read-only, extra disks before power-off, so the node can read the work disk without journal recovery.
- **Builder interface (node agent).** A trait the node agent owns, not the Engine. It uses the Engine to run the VM:

  ```rust
  pub trait Builder {
      /// Runs one build; events stream while it runs.
      fn build(&self, spec: &BuildSpec) -> impl Future<Output = Result<BuildHandle, BuildError>> + Send;
  }
  pub struct BuildSpec {
      pub build_id: BuildId,            // bld_<ULID>
      pub source: BuildSource,          // Git { url, ref_, commit } | Upload { archive }
      pub frontend: Frontend,           // Dockerfile { path, target } | Railpack (later)
      pub context_dir: String,          // subdirectory of the repo
      pub build_args: Vec<(String, String)>,
      pub secrets: Vec<SecretRef>,      // names; values decrypted on the node
      pub limits: BuildLimits,          // vcpus, memory_mb, timeout, cache_gb, output_gb
      pub cache_key: String,            // per app: isolates caches
  }
  pub enum BuildEvent { Phase(BuildPhase), Line { stream, vertex: Option<u32>, text }, Step(StepStatus) }
  pub struct BuildOutput { pub layout: PathBuf, pub manifest_digest: ImageDigest }
  pub struct BuildFailure { pub phase: BuildPhase, pub step: Option<FailedStep>, pub excerpt: Vec<String> }
  pub struct FailedStep { pub vertex: u32, pub name: String, pub error: String, pub dockerfile_line: Option<u32> }
  ```

  Implementations: `DockerfileBuilder` (M4) and `RailpackBuilder` (later). Both run the same builder VM and differ only in the frontend options and a `prepare` step. A `FakeBuilder` for tests mirrors `FakeEngine`.
- **Git source.** BuildKit clones `https://...git#ref` contexts itself, tested with a public repository, which avoids a host-side clone. A host-side clone onto the work disk is still the better default: the node resolves the ref to a commit SHA and records it, Quoro handles Git credentials (deploy keys, tokens) on the node instead of inside the build, and the same checkout feeds Railpack's `prepare`. Private-repository authentication is out of this spike.
- **Build records and states (control plane, replicated).** `builds` with `id`, `app_id`, `source` (URL, ref, resolved commit), `frontend`, `status` (`queued`, `preparing` (VM and checkout), `building`, `exporting`, `importing`, `succeeded`, `failed`, `cancelled`), `phase` on failure, `failed_step` (vertex, name, error, Dockerfile line), `image_digest`, timings, `created_by`, `trigger` (`api`, `push`, `redeploy`). A successful build starts a deployment with `trigger: build`, which `openapi.yaml` already lists, as it already lists `build` as a deployment phase.
- **Build logs over SSE (ADR 0057, ADR 0014).** The log stays on the node, next to the build's work directory, as tagged lines parsed into `BuildEvent`s. The panel reads it through a new `stream_open` kind `build_logs` (params `build_id`, `tail`) and an SSE endpoint `GET /api/v1/builds/{id}/logs/stream`, plus a paginated `GET .../logs` for finished builds. Only the status and the failing step (a few lines) are replicated, so the panel shows the failure even when the node is down. Use `--progress=rawjson` and keep the plain lines as the human log: rawjson gives the failing vertex structurally and plain gives the Dockerfile excerpt.
- **Cache and limits.** One persistent work disk per app per node (BuildKit cache plus output), sized by `cache_gb`, pruned with `buildctl prune --keep-storage` at the end of each build, and deleted with the app. Per build: vCPU and memory of the builder VM, a wall-clock timeout (on expiry, stop the VM and mark the build `failed` with phase `building`), an output-size cap, and one concurrent build per node by default. Builds count against the node's capacity like any VM.
- **Audit and grants.** Starting, cancelling and reading the logs of a build are API operations with scopes (`build:write` and `build:read`, or `app:deploy` and `app:logs`), recorded in the audit log like deployments (ADR 0031).

## Open risks and unknowns

- **Untrusted Dockerfiles on a production node.** The VM is the boundary, but the builder VM has outbound network to fetch bases and dependencies. It should get its own isolation group (ADR 0084) with egress only, no route to app VMs or the node API.
- **Secrets in builds.** Build args end up in image history. BuildKit `--secret` needs the values inside the VM, so the contract must say how they get there (config disk, work disk, or a later channel) and that they are never logged.
- **Disk pressure.** Railpack's cold cache was 2.6 GB for one small app. Per-app caches on every node can fill a small homelab disk, so the cap and prune policy must be in the contract, and the node must watch its free space.
- **Reproducibility.** Manifests carry `org.opencontainers.image.created`, so rebuilding after a cache prune changes the digest. `SOURCE_DATE_EPOCH` can be set if build reproducibility is wanted (ADR 0027 covers Quoro's own artifacts, not app images).
- **Base-image rate limits.** Every node pulls bases from Docker Hub anonymously, with no shared mirror. Registry credentials or a pull-through cache are later items.
- **Architecture.** Only `linux/amd64` was built. Multi-platform builds (QEMU binfmt inside the VM, `buildkit-qemu-*` ships in the image) were not tried.
- **Builder image updates.** The builder rootfs is about 300 MB and must be pinned, verified and refreshed like the kernel. The node prepares it with the normal `prepare_image` path, since it is an ordinary OCI image.
- **Nested-virt numbers only.** Real hosts should be faster. The relative costs (0.3 s boot, cold downloads dominating, 2 to 4 s warm rebuilds) are what the plan can rely on.
- **Not exercised:** rootless BuildKit, seccomp inside the VM, private Git and private registries, cancellation mid-build (expected to be "stop the VM"), concurrent builds on one node, and Nixpacks' `--out` Dockerfile path.

## Sources

- BuildKit v0.34.2 release and assets: https://github.com/moby/buildkit/releases/tag/v0.34.2 (accessed 2026-11-03)
- BuildKit license (Apache-2.0): https://github.com/moby/buildkit (repository metadata, accessed 2026-11-03)
- runc license (Apache-2.0): https://github.com/opencontainers/runc (repository metadata, accessed 2026-11-03)
- Nixpacks README with the maintenance-mode notice: https://github.com/railwayapp/nixpacks (accessed 2026-11-03)
- Nixpacks releases: https://github.com/railwayapp/nixpacks/releases (accessed 2026-11-03)
- Nixpacks Docker builder: https://github.com/railwayapp/nixpacks/blob/main/src/nixpacks/builder/docker/docker_image_builder.rs (accessed 2026-11-03)
- Nixpacks CLI (`build`, `--out`): https://nixpacks.com/docs/cli (accessed 2026-11-03)
- Railpack README and license: https://github.com/railwayapp/railpack (accessed 2026-11-03)
- Railpack releases: https://github.com/railwayapp/railpack/releases (accessed 2026-11-03)
- Railpack in production (frontend, plan, `buildctl` invocation): https://railpack.com/platforms/running-railpack-in-production (accessed 2026-11-03)
- Railpack frontend image versions: https://github.com/orgs/railwayapp/packages/container/package/railpack-frontend (accessed 2026-11-03)
- Engine code read for the pipeline: `crates/engine/src/image/` (`mod.rs`, `registry.rs`, `layer.rs`, `oci.rs`, `cache.rs`, `ext4.rs`), `crates/engine/src/ch/vmconfig.rs`, `crates/engine/src/ch/engine.rs`
- Init behaviour: `crates/init/README.md`, `crates/init/src/guest/mounts.rs`, `crates/init/src/guest/console.rs`

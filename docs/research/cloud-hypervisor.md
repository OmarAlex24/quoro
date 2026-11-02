# Cloud Hypervisor: license, API, boot time, snapshot/restore, virtio-fs, consoles

- **Question:** Q-0010 Cloud Hypervisor license, REST API surface, boot time, snapshot/restore, virtio-fs, serial/VNC console support
- **Researched:** 2026-11-02 by agent:ds-research-1
- **Status:** answered

## Short answer

Cloud Hypervisor (CH) is a Rust VMM for KVM and MSHV, dual-licensed **Apache-2.0 AND BSD-3-Clause**, governed as a Linux Foundation project, latest release **v54.0 (2026-10-14)**, roughly every 6-9 weeks. It exposes an HTTP/JSON API over a **UNIX socket** (`--api-socket`) with 32 documented endpoints in an OpenAPI 3.0.1 spec, plus an optional, off-by-default D-Bus API and the `ch-remote` CLI client. It boots guests by direct kernel boot or firmware (rust-hypervisor-firmware or CLOUDHV edk2); the official site claims boot to userspace in under 100 ms, with no published methodology. Snapshot is **pause + full guest-RAM dump** (`config.json`, `memory-ranges`, `state.json`) and does **not** contain disk contents; restore is file-based, same-version only, with eager/on-demand/copy-on-write memory modes, and live migration over UNIX or TCP/mTLS. virtio-fs works only through an external `virtiofsd` vhost-user daemon and requires `--memory shared=on`; **DAX is removed**. Consoles are text-only: serial and virtio-console to `tty`, `pty`, `file=`, `socket=`, `tcp=loopback`, `null` or `off`. **CH has no VNC, no GPU and no framebuffer device at all.**

## Findings

### 1. License

| Component | License (SPDX) | Source | Label |
|---|---|---|---|
| cloud-hypervisor | `Apache-2.0 AND BSD-3-Clause` | `cloud-hypervisor/Cargo.toml` `license` field: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/cloud-hypervisor/Cargo.toml (accessed 2026-11-02) | Confirmed |
| cloud-hypervisor repo files | Code: Apache-2.0; docs (`*.md`): CC-BY-4.0 | `.reuse/dep5`: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/.reuse/dep5 (accessed 2026-11-02) | Confirmed |
| Repo license texts | `LICENSES/Apache-2.0.txt`, `LICENSES/BSD-3-Clause.txt`, `LICENSES/CC-BY-4.0.txt` | https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/contents/LICENSES (accessed 2026-11-02) | Confirmed |
| virtiofsd | `Apache-2.0 AND BSD-3-Clause` | https://gitlab.com/virtio-fs/virtiofsd/-/raw/main/Cargo.toml (accessed 2026-11-02) | Confirmed |
| rust-hypervisor-firmware | Apache-2.0 (single `LICENSE` file) | https://raw.githubusercontent.com/cloud-hypervisor/rust-hypervisor-firmware/main/LICENSE (accessed 2026-11-02) | Confirmed |

- There is no single top-level `LICENSE` file; license texts live in a REUSE-style `LICENSES/` directory, and GitHub's license API returns `license: null` for the repo. The authoritative machine-readable statement is the crate manifest, `Apache-2.0 AND BSD-3-Clause`. **Confirmed.** Source: https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor (accessed 2026-11-02)
- Docs are separately licensed CC-BY-4.0 (documentation only, no effect on code use). **Confirmed.** Source: `.reuse/dep5` (accessed 2026-11-02)
- Both CH licenses are permissive, not copyleft. The FSF license list states Apache-2.0 "is a free software license, compatible with version 3 of the GNU GPL". **Confirmed** (statement about Apache-2.0/GPLv3). Source: https://www.gnu.org/licenses/license-list.html#apache2 (accessed 2026-11-02)
- **Hypothesis:** an AGPL-3.0 project that runs CH as a *separate process* (spawn `cloud-hypervisor`, talk to its socket) creates no derivative work and therefore no licensing conflict; and even linking CH crates into an AGPL-3.0 binary would be permissible because Apache-2.0 is GPLv3/AGPLv3-compatible and BSD-3-Clause is permissive. No CH or FSF source addresses this exact combination; AGPLv3 §13 network clause also only attaches to the AGPL-covered work itself. Treat as legal-review item, not as a settled fact.

### 2. Releases, cadence, governance

| Item | Value | Source | Label |
|---|---|---|---|
| Latest release | **v54.0**, published 2026-10-14 | https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/releases (accessed 2026-11-02) | Confirmed |
| Previous majors | v53.0 2026-07-12, v52.0 2026-05-14, v51.0 2026-02-20, v50.0 2025-12-19, v49.0 2025-11-09 | same releases API (accessed 2026-11-02) | Confirmed |
| Version in `main` | `55.0.0` (unreleased dev version) | https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/cloud-hypervisor/Cargo.toml (accessed 2026-11-02) | Confirmed |
| Stated cadence | "a new major release is issued approximately every 6 weeks"; point releases on demand; a major gets fixes for ~12 weeks then EOL | https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/releases.md (accessed 2026-11-02) | Confirmed |
| Observed cadence | 6-9 weeks between v49..v54 (see dates above), i.e. slower than the stated 6 weeks | computed from releases API (accessed 2026-11-02) | Confirmed (dates) / Hypothesis (that this is the new normal) |
| Governance | "governed openly as part of the Linux Foundation"; site footer "Cloud Hypervisor a Series of LF Projects, LLC" | https://www.cloudhypervisor.org/ (accessed 2026-11-02) | Confirmed |
| Backers named on site | Alibaba, AMD, Ampere, ARM, ByteDance, Cyberus Technology, Intel, Microsoft, SAP, Tencent Cloud | same page (accessed 2026-11-02) | Confirmed |
| Leadership | maintainers team, "@rbradford taking the lead"; named domain experts for KVM, MSHV, ARM, RISC-V, SEV-SNP, live migration, VFIO/vhost-user, block/QCOW2 | https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/MAINTAINERS.md (accessed 2026-11-02) | Confirmed |
| Stability promise | API and CLI options not removed/changed in a breaking way without ≥2 major releases notice | https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/README.md (accessed 2026-11-02) | Confirmed |

### 3. REST API

- Transport: HTTP/JSON over a UNIX domain socket (or an inherited fd) selected with `--api-socket path=...` / `--api-socket fd=...`. The API is available as soon as the binary starts, even with no VM configured. **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/api.md (accessed 2026-11-02)
- The HTTP server is Firecracker's `micro_http` crate; requests are translated to an internal MPSC command API consumed by the VMM control loop. **Confirmed.** Source: `docs/api.md` (accessed 2026-11-02)
- Spec: OpenAPI 3.0.1, `info.version: 0.3.0`, `servers: http://localhost/api/v1`. All paths are under `/api/v1`; the spec document version is **not** the CH release version. **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/vmm/src/api/openapi/cloud-hypervisor.yaml (accessed 2026-11-02)
- Methods are RPC-style: `GET` for reads, `PUT` for every mutation. **Confirmed.** Source: same spec (accessed 2026-11-02)

All 32 paths in the current spec (source: `vmm/src/api/openapi/cloud-hypervisor.yaml`, accessed 2026-11-02; purposes from `docs/api.md`, accessed 2026-11-02). **Confirmed.**

| Endpoint | Method | Purpose |
|---|---|---|
| `/vmm.ping` | GET | Check API availability; returns VMM version/state |
| `/vmm.shutdown` | PUT | Shut the VMM process down |
| `/vm.info` | GET | Dump VM configuration/state info |
| `/vm.counters` | GET | Dump VM I/O and device counters |
| `/vm.balloon-stats` | GET | Last virtio-balloon statistics sample from the guest |
| `/vm.create` | PUT | Create (not boot) a VM from a `VmConfig` |
| `/vm.delete` | PUT | Delete the VM |
| `/vm.boot` | PUT | Boot a created VM |
| `/vm.pause` | PUT | Pause vCPUs |
| `/vm.resume` | PUT | Resume a paused VM |
| `/vm.shutdown` | PUT | Graceful guest shutdown |
| `/vm.reboot` | PUT | Reboot the guest |
| `/vm.power-button` | PUT | Trigger ACPI power button |
| `/vm.resize` | PUT | Add/remove vCPUs or memory |
| `/vm.resize-disk` | PUT | Resize an attached disk |
| `/vm.resize-zone` | PUT | Add/remove memory in a memory zone |
| `/vm.add-device` | PUT | Hotplug a VFIO PCI device |
| `/vm.remove-device` | PUT | Remove a hotplugged device |
| `/vm.add-disk` | PUT | Hotplug a disk |
| `/vm.add-fs` | PUT | Hotplug a virtio-fs (vhost-user-fs) device |
| `/vm.add-generic-vhost-user` | PUT | Hotplug a generic vhost-user device |
| `/vm.add-pmem` | PUT | Hotplug a virtio-pmem device |
| `/vm.add-net` | PUT | Hotplug a network device |
| `/vm.add-vsock` | PUT | Hotplug a vsock device |
| `/vm.add-vdpa` | PUT | Hotplug a vDPA device |
| `/vm.add-user-device` | PUT | Hotplug a userspace PCI device (vfio-user) |
| `/vm.snapshot` | PUT | Snapshot a paused VM to a `file://` directory |
| `/vm.restore` | PUT | Restore a VM from a snapshot (optionally with new net FDs) |
| `/vm.coredump` | PUT | Guest coredump (x86_64, `guest_debug` build feature only) |
| `/vm.nmi` | PUT | Inject NMI |
| `/vm.send-migration` | PUT | Start sending this VM to a migration target |
| `/vm.receive-migration` | PUT | Prepare to receive a migration |
| `/vm.cancel-migration` | PUT | Cancel an in-flight migration (new in v54) |

- **D-Bus API:** mirrors the REST API (same JSON payloads, drop-in replacement) and adds a D-Bus `Event` signal fed by the event monitor. It is **not compiled by default**; requires the `dbus_api` cargo feature and `--dbus-service-name` / `--dbus-object-path` / `--dbus-system-bus`. Interface name `org.cloudhypervisor.DBusApi1`. **Confirmed.** Source: `docs/api.md` (accessed 2026-11-02)
- **`ch-remote`:** a separate CLI binary that drives the REST API over the socket (all doc examples use `ch-remote --api-socket=... <action>`). It is shipped as a release asset of v54.0. **Confirmed.** Sources: `docs/api.md`, `docs/snapshot_restore.md` (accessed 2026-11-02) and the `ch-remote` asset in https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/releases (accessed 2026-11-02)
- **Versioning/stability:** the API is covered by the stability guarantee (no breaking change without 2 major releases' notice), but it is *not* semantically versioned per release: the URL is fixed at `/api/v1` while the spec doc version stays `0.3.0`. New fields and endpoints are added per release under that same prefix. **Confirmed** (stability text) / **Hypothesis** (that `/api/v1` will never change; nothing states a v2 plan).
- A v54 fix raised the HTTP request body limit from 50 KiB to 4 MiB and corrected status codes (400/404/409). **Confirmed.** Source: https://www.cloudhypervisor.org/ (v54.0 release notes, accessed 2026-11-02)

### 4. Boot time

- Official marketing claim: "Boot to userspace in less than 100ms with direct kernel boot". **Confirmed that the claim is published**; it is a marketing claim with no stated hardware, image, kernel or measurement method. **Hypothesis** as a number we can design against. Source: https://www.cloudhypervisor.org/ (accessed 2026-11-02)
- No boot-time number appears in the README, `docs/releases.md` or the `docs/` pages fetched. **Confirmed** (absence) for those pages. Sources: README, `docs/releases.md`, `docs/performance_metrics.md` (accessed 2026-11-02)
- CH ships a performance harness that measures `boot_time_ms`, `boot_time_pmem_ms`, `boot_time_16_vcpus_ms` and `restore_latency_time_ms`; the table's "timeout 2 s" is a test timeout, **not** a boot-time target, and no results are published in the doc. **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/performance_metrics.md (accessed 2026-11-02)
- Two boot paths: (a) **direct kernel boot** — `--kernel` with a PVH-enabled ELF `vmlinux` or a bzImage on x86-64, `Image` on AArch64, plus `--cmdline` and optional `--initrd`; (b) **firmware boot** — `--firmware` pointing at rust-hypervisor-firmware (`hypervisor-fw`, a lightweight Rust PVH firmware, also bootable via `--kernel`) or the edk2 UEFI builds `CLOUDHV.fd` (x86-64) / `CLOUDHV_EFI.fd` (AArch64). **Confirmed.** Source: README (accessed 2026-11-02)
- Since v54, AArch64 can do firmware-aided direct kernel boot (kernel/cmdline/initramfs handed to firmware via fw_cfg). **Confirmed.** Source: cloudhypervisor.org v54.0 notes (accessed 2026-11-02)

### 5. Snapshot/restore and live migration

- Snapshot requires the VM to be **paused**; the snapshot is a directory of three files: `config.json` (VM config, human-editable), `memory-ranges` (full guest RAM image), `state.json` (per-device/VM state). **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/snapshot_restore.md (accessed 2026-11-02)
- The snapshot does **not** contain disk contents. The doc's restore example reuses the original disk images, and the offload section warns that to restore consistently "you should copy the disk images while the VM is still paused, and restore the daemon's snapshot against those copies". **Confirmed.** Source: `docs/snapshot_restore.md` (accessed 2026-11-02)
- Restore is `--restore source_url=file:///path` or `vm.restore`; the VM comes back **paused** unless `resume=true`. Net devices created with explicit FDs need fresh FDs passed as `net_fds=[id@[fd,fd],...]`. **Confirmed.** Source: `docs/snapshot_restore.md` (accessed 2026-11-02)
- Restore memory modes: default `copy` (eager full copy of `memory-ranges`), `ondemand` (userfaultfd, faults pages in on first access; **rejects** VFIO, vDPA, vfio-user and vhost-user devices; needs Linux ≥6.6), and `copyonwrite` (map the snapshot memory file COW, share page cache across clones; falls back to eager copy for shared memory/hugepages/hotplug/zones/passthrough). **Confirmed.** Source: `docs/snapshot_restore.md` (accessed 2026-11-02)
- Snapshotting a VM restored with `copyonwrite` uses a reflink and then writes only pages changed since restore — but only if snapshot and restore source are on the same reflink-capable filesystem (XFS/btrfs). **Confirmed.** Source: `docs/snapshot_restore.md` (accessed 2026-11-02)
- VFIO snapshot/restore is supported only for devices implementing the kernel VFIO migration v2 protocol (e.g. Mellanox NICs on `mlx5_vfio_pci`). **Confirmed.** Source: `docs/snapshot_restore.md` (accessed 2026-11-02)
- Snapshot/restore support for vhost-user devices (including virtio-fs migration) was added earlier and is present. **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/release-notes.md (line: "Snapshot/restore support for `vhost-user` devices has been filled out (#7908), including migration support for `virtio-fs` (#7937)"; accessed 2026-11-02)
- Cross-version support is **not** guaranteed: README states "Snapshot/restore is not supported across different versions" and "Live migration is not supported across different versions". **Confirmed** (README statement). Source: README (accessed 2026-11-02)
- However `docs/live_migration.md` states that **since v54** migrations from the previous two releases (n-2) are accepted; backward migration is not supported. This contradicts the README sentence; the live_migration doc is newer and more specific. **Confirmed** (both statements exist). Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/live_migration.md and cloudhypervisor.org v54 notes (accessed 2026-11-02)
- Live migration transports: **UNIX socket** (same host) and **TCP with mutual TLS** (`tls_dir`) across hosts; UNIX transport is not encrypted and cannot use TLS. Parameters include `downtime_ms` (default 300 ms), `timeout_s` (3600), `connections` (1-128), `memory_mode` (`precopy` default, `postcopy`, `memfds`), `preserve_source`, `zone_updates`, `vfio_fds`/`iommufd_fd`, `background_fetch_pages`. **Confirmed.** Source: `docs/live_migration.md` (accessed 2026-11-02)
- Postcopy migration and on-demand restore are rejected for VMs with VFIO, vDPA, vfio-user or vhost-user devices. vDPA dirty tracking is unavailable, so vDPA devices are suspended before transfer. **Confirmed.** Sources: `docs/live_migration.md`, cloudhypervisor.org v54 notes (accessed 2026-11-02)
- Offload snapshot/restore: CH can hand the snapshot stream to a user daemon over the migration protocol (`offload_daemon` reference binary), using `memory_mode=memfds` and a `unix:` URL, so snapshots can be encrypted/compressed/streamed to object storage. Requires `shared=on` or hugepages for all guest memory. **Confirmed.** Source: `docs/snapshot_restore.md` (accessed 2026-11-02)
- Restore/snapshot **on another host**: the file-based snapshot is portable in principle, but `config.json` holds absolute paths that must exist on the target, versions must match, and CPUID compatibility is checked. **Hypothesis** (assembled from the path requirements in `docs/snapshot_restore.md` and the migration/CPU-compatibility notes; no doc states "restore on another host is supported").
- Incremental/dirty-page tracking: there is **no user-facing API to obtain a dirty bitmap or an incremental snapshot**. CH tracks dirty pages internally for precopy migration (the `vm.migration-memory-iteration` event) and VFIO device writes during migration, but exposes neither a bitmap nor a "diff snapshot" endpoint. **Hypothesis** (strongly implied by the endpoint list and the event/migration docs; the docs never state it explicitly). Sources: `docs/live_migration.md`, `vmm/src/api/openapi/cloud-hypervisor.yaml` (accessed 2026-11-02)
- Disks are protected by host-local advisory OFD locks (`F_OFD_SETLK`), `lock_granularity=byte-range` (default) or `full`; locks do not coordinate across hosts. **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/disk_locking.md (accessed 2026-11-02)

### 6. virtio-fs

- CH supports virtio-fs only as a **vhost-user** device: an external `virtiofsd` process owns the backend and CH connects to its UNIX socket via `--fs tag=<tag>,socket=<path>,num_queues=...,queue_size=...` (or hotplug `vm.add-fs`). **Confirmed.** Sources: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/fs.md and `docs/device_model.md` (accessed 2026-11-02)
- `--memory shared=on` is required: "Correct functioning of `--fs` requires `--memory shared=on` to facilitate interprocess memory sharing." **Confirmed.** Source: `docs/fs.md` (accessed 2026-11-02)
- virtiofsd must be built from source in the CH docs (`cargo build --release`, `setcap cap_sys_admin+epi`). Upstream is a Rust daemon: https://gitlab.com/virtio-fs/virtiofsd, license `Apache-2.0 AND BSD-3-Clause`, latest tag **v1.14.0 (2026-07-06)**. **Confirmed.** Sources: `docs/fs.md` (accessed 2026-11-02); https://gitlab.com/virtio-fs/virtiofsd/-/raw/main/Cargo.toml and https://gitlab.com/api/v4/projects/virtio-fs%2Fvirtiofsd/repository/tags (accessed 2026-11-02)
- Recommended/typical options in CH docs: `--cache=never` (default, avoids host page cache, better density), `--cache=always` for performance, `--thread-pool-size=$N`. Guest kernel needs virtio-fs support (Linux ≥5.10). **Confirmed.** Source: `docs/fs.md` (accessed 2026-11-02)
- **DAX is not available**: "Given the DAX feature is not stable yet from a daemon standpoint, it is not available in Cloud Hypervisor." The `dax` option was deprecated and then removed (#3889). **Confirmed.** Sources: `docs/fs.md` and `release-notes.md` (accessed 2026-11-02)
- CH also has a `--generic-vhost-user` device, so any vhost-user backend CH does not model natively can still be attached (the fs tag then lives on the virtiofsd command line). **Confirmed.** Source: `docs/device_model.md` (accessed 2026-11-02)

### 7. Console and display

- **Serial** (`--serial`, legacy 16550A on x86-64 / PL011 on AArch64) and **virtio-console** (`--console`, enabled by default) share the same backends: `off`, `null`, `tty`, `pty`, `file=<path>`, `socket=<path>` (UNIX socket) and `tcp=<loopback_ip>:<port>` (IPv4/IPv6 literal on loopback only, listener only, one client at a time, output buffered until a client connects, boot does not wait for a client). **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/device_model.md (accessed 2026-11-02)
- `--console socket=<path>` for virtio-console is new in v54; configurations that pass a socket path without selecting socket mode (and vice versa) are now rejected for both `--serial` and `--console`. **Confirmed.** Source: cloudhypervisor.org v54.0 release notes (accessed 2026-11-02)
- A host process (e.g. a node agent) attaches by connecting to the UNIX socket and relaying bytes; this is the natural input for a browser terminal (xterm.js). The socket serves one client at a time and accepts a new client only after the current one disconnects. **Confirmed** (socket behaviour) / **Hypothesis** (that it is a plain byte stream suitable for direct relay; the docs say "UNIX socket" without stating the socket type).
- Boot loader/firmware interaction requires serial rather than virtio-console (`--serial tty --console off`), because virtio-console can cut early boot messages. **Confirmed.** Source: README (accessed 2026-11-02)
- TCP console live migration requires the listener address to be free on the destination; overriding the destination console address is not supported. **Confirmed.** Source: `docs/device_model.md` (accessed 2026-11-02)
- **No VNC, no SPICE, no GPU and no framebuffer.** The device model lists only serial, RTC/CMOS, I/O APIC, i8042, ACPI, PL061, virtio-{blk,console,iommu,net,pmem,rng,vsock}, vhost-user-{blk,fs,net,generic} and VFIO. There is no display device. **Confirmed** for the device model. Source: `docs/device_model.md` (accessed 2026-11-02)
- `vmm/src/config.rs` (8233 lines, the full CLI surface) contains no `vnc`, `spice`, `virtio-gpu` or display option; the only `gpu` matches are `VIRTIO_ID_GPU` (for generic vhost-user device type mapping) and NVIDIA `x_nv_gpudirect_clique` VFIO passthrough options. **Confirmed.** Source: https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/vmm/src/config.rs (accessed 2026-11-02)
- A graphical guest is only possible by passing through a physical GPU via VFIO (`--device`) into a guest that runs its own display stack; CH itself still provides no virtual display. **Hypothesis** (follows from the absence of a display device plus the VFIO documentation).

### 8. Other engine-relevant facts

| Fact | Value | Source | Label |
|---|---|---|---|
| Hypervisors | KVM and Microsoft Hypervisor (MSHV) | README (accessed 2026-11-02) | Confirmed |
| Architectures | x86-64 and AArch64 main; riscv64 experimental with limited functionality | README + `docs/riscv.md` (accessed 2026-11-02) | Confirmed |
| Guest OS | 64-bit Linux; Windows 10 / Windows Server 2019 | README (accessed 2026-11-02) | Confirmed |
| Minimum host kernel | **6.8** ("For required KVM functionality and adequate performance") | README (accessed 2026-11-02) | Confirmed |
| Hotplug | CPU, memory (resize + zones + virtio-mem), VFIO passthrough, virtio-{net,block,pmem,fs,vsock}, generic vhost-user, vDPA | README + `docs/hotplug.md` + API list (accessed 2026-11-02) | Confirmed |
| Networking | TAP created by CH (`--net`), macvtap via `fd=` on `/dev/tapN`, vhost-user-net (`vhost_user=true,socket=`), plus `--net fd=` for orchestrator-provided FDs | README, `docs/macvtap-bridge.md`, `docs/device_model.md` (accessed 2026-11-02) | Confirmed |
| Seccomp | Enabled by default, per-thread filters (vCPU, virtio, HTTP, main, VMM); disable with `--seccomp false` | `docs/seccomp.md` (accessed 2026-11-02) | Confirmed |
| Landlock | Optional `--landlock` sandbox, rules enumerated at `vm_create`; `--landlock-rules` for extra paths | `docs/landlock.md` (accessed 2026-11-02) | Confirmed |
| Rate limiting | Token-bucket I/O throttling on virtio-blk and virtio-net (`bw_size`/`bw_one_time_burst`/`bw_refill_time`, `ops_size`/`ops_one_time_burst`/`ops_refill_time`); fixed 100 ms cool-down makes real rates diverge from nominal | `docs/io_throttling.md` (accessed 2026-11-02) | Confirmed |
| Event monitor | `--event-monitor`, JSON events (e.g. `vm.migration-*`, `vmm.started`) | `docs/event-monitor.md`, v54 notes (accessed 2026-11-02) | Confirmed |
| Nested virt *to guests* | `--cpus nested=on` (x86_64 KVM/MSHV, aarch64 KVM); **default off since v54** | `docs/cpu.md` + v54 notes (accessed 2026-11-02) | Confirmed |
| CH *inside* a nested guest | No official doc found on running CH as an L2 VMM. It is an ordinary KVM/MSHV userspace process, so it should work wherever the L1 exposes VMX/SVM and `/dev/kvm`, with performance and feature caveats. | no primary source | Hypothesis / open risk |
| Virtio transport | virtio-pci only; multiple PCI segments; `pci_segment=` / `pci_device_id=` placement | `docs/device_model.md` (accessed 2026-11-02) | Confirmed |

### 9. Comparison with Firecracker (sourced)

Both are Rust rust-vmm VMMs, but Firecracker is Apache-2.0 only, has a narrower device model — its design doc lists VirtIO Net, Block and Vsock (plus serial, i8042, balloon, entropy), with **no virtio-fs and no PCI** — and its API is also HTTP/JSON over a UNIX socket but with a flat, Swagger 2.0 resource API (`/machine-config`, `/drives/{id}`, `/snapshot/create`, `/snapshot/load`, ...). Firecracker snapshots support full and **diff** snapshots, with diff snapshots marked developer preview and usable only when the base VM was booted/restored with `track_dirty_pages=true`; snapshots "must be resumed on a software and hardware configuration identical to what they were generated on". **Confirmed.** Sources: https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/README.md, https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/docs/design.md, https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/docs/snapshotting/snapshot-support.md, https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/src/firecracker/swagger/firecracker.yaml, https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/src/firecracker/Cargo.toml (accessed 2026-11-02)

## Versions checked

| Component | Version | Release date | Source |
|---|---|---|---|
| Cloud Hypervisor | **v54.0** | 2026-10-14 | https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/releases (accessed 2026-11-02) |
| Cloud Hypervisor (main branch) | 55.0.0 (unreleased) | n/a | https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/cloud-hypervisor/Cargo.toml (accessed 2026-11-02) |
| CH OpenAPI spec | 0.3.0 (document version) | current `main` | https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/vmm/src/api/openapi/cloud-hypervisor.yaml (accessed 2026-11-02) |
| virtiofsd | **v1.14.0** | 2026-07-06 | https://gitlab.com/api/v4/projects/virtio-fs%2Fvirtiofsd/repository/tags (accessed 2026-11-02) |
| rust-hypervisor-firmware | **0.5.0** | 2024-11-30 (≈2 years old) | https://api.github.com/repos/cloud-hypervisor/rust-hypervisor-firmware/releases (accessed 2026-11-02) |
| CH edk2 / CLOUDHV | `ch-97eeb7b09` | 2026-09-22 | https://api.github.com/repos/cloud-hypervisor/edk2/releases (accessed 2026-11-02) |
| Firecracker (comparison) | v1.17.0 | 2026-09-10 | https://api.github.com/repos/firecracker-microvm/firecracker/releases (accessed 2026-11-02) |

## Implications for Quoro

- **D-0007 (own engine on CH + KVM):** license risk is low. CH is `Apache-2.0 AND BSD-3-Clause` (permissive), so an AGPL-3.0 Quoro can spawn it as a separate process and even link its crates. Only the legal reading is unconfirmed; keep Apache-2.0 and BSD-3-Clause notice files in the distribution.
- **D-0007:** the API surface is sufficient for a real engine: create/boot/pause/resume/shutdown, CPU+memory+disk+net hotplug, counters, balloon, snapshot/restore, migration, event monitor. It is HTTP/JSON over a UNIX socket, so the Rust agent can use any HTTP client over `UnixStream` — no need for a CH-specific SDK. There is **no versioned v2 to design against**: pin one CH release per node-agent build.
- **D-0008 (one microVM per app, fast boot):** the <100 ms figure is a marketing claim without methodology; no primary benchmark was found. Boot time will be dominated by our image, kernel, init and disk, not by CH. The direct-kernel-boot path (PVH `vmlinux`/bzImage, no firmware) is the one to use for the warm path; firmware/edk2 adds a bootloader stage.
- **D-0028 (consoles/metrics: xterm.js, noVNC):** **CH has no VNC, no virtual GPU and no framebuffer device.** noVNC has nothing to attach to on the CH side. The console plan must be text-only: xterm.js over the serial or virtio-console UNIX socket (`--serial socket=` / `--console socket=`, one client at a time), relayed by the node agent, or vsock into an in-guest agent. If a graphical console is ever required, it needs a guest-side VNC/desktop server or GPU passthrough, both outside CH's device model. This is a factual gap in D-0028's stated tool list, not a preference.
- **Q-0004 (snapshot primitive):** evidence, not a decision. CH snapshot = pause + full `memory-ranges` dump + `config.json` + `state.json`, and it **excludes disk contents**, so any snapshot primitive must still solve disk consistency separately (that is exactly the open part of Q-0004). CH exposes **no dirty bitmap and no incremental/diff snapshot API**, so a chunk engine would re-read a full RAM image each cycle and rely on content-addressed dedup — workable but O(RAM) I/O per snapshot. Partial mitigations CH does offer: `copyonwrite` restore (page-cache sharing, and reflink-based delta when re-snapshotting a COW-restored VM on XFS/btrfs) and `ondemand` restore.
- **D-0023 (own chunk engine):** CH's offload-daemon mechanism (`send-migration` to a `unix:` peer with `memory_mode=memfds`, plus a documented wire protocol in `vm-migration/src/protocol.rs`) is a possible streaming feed for the chunk engine's snapshot source, instead of writing `memory-ranges` to local disk first. That is evidence for feasibility, not a recommendation.
- **D-0009 (KVM-capable host, nested virt accepted):** CH requires KVM (or MSHV) and host kernel **≥6.8**. No official document covers CH running inside an L2 guest; nested capability depends on the VPS exposing VMX/SVM. Also since v54, CH disables nested virt *to its own guests* by default (`--cpus nested=on` needed), which does not affect us unless an app needs KVM inside its microVM.
- **Snapshot portability:** file-based snapshots can in principle be moved between nodes, but `config.json` stores absolute paths, versions must match, and CPUID is checked. Cross-node snapshot/restore is a risk to validate on the lab, not an assumed capability.

## Open risks and unknowns

- **No primary benchmark for boot time.** The <100 ms claim is unverified for our kernels/images. Needs a lab measurement per image class before D-0008's "warm ≈ hot" reasoning is relied on.
- **AGPL-3.0 compatibility is a legal reading, not a sourced statement.** Apache-2.0 is GPLv3-compatible per the FSF; the separate-process argument is our inference. Legal review needed before shipping.
- **Snapshot/restore across versions is ambiguous in the docs**: README says not supported across versions; `docs/live_migration.md` promises n-2 migration since v54; `docs/releases.md` says not compatible across MAJOR. Only migration has a clear n-2 promise; snapshot restore has none. Pin versions per node.
- **Cross-host snapshot restore** (path layout, CPU model, disk lock/consistency) is undocumented; treat as needing validation.
- **Incremental snapshot / dirty bitmap:** no API found. If Q-0004 needs block-level deltas from the hypervisor, CH will not provide them; the diff must be computed above CH (or via the storage layer).
- **virtio-fs** requires an extra process (`virtiofsd`) per share, `shared=on` memory (which interacts badly with `copyonwrite` and with hugepages), and no DAX. Operationally this is a second daemon to supervise and secure (`cap_sys_admin`).
- **rust-hypervisor-firmware 0.5.0 is from 2024-11-30**, i.e. ~2 years old at access date; treat firmware path as less actively maintained than CH itself.
- **Nested-virt hosts** (D-0009) have no CH documentation; performance and feature availability (e.g. MSHV, SEV-SNP, IOMMU) inside a nested guest are unknown.
- The CH repo has **no top-level `LICENSE` file** and GitHub reports `license: null`; automated license scanners may flag it. Use the `Cargo.toml` SPDX expression and `LICENSES/` texts.

## Sources

1. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/README.md (accessed 2026-11-02)
2. https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor (accessed 2026-11-02)
3. https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/contents/ (accessed 2026-11-02)
4. https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/contents/LICENSES (accessed 2026-11-02)
5. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/.reuse/dep5 (accessed 2026-11-02)
6. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/cloud-hypervisor/Cargo.toml (accessed 2026-11-02)
7. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/api_client/Cargo.toml (accessed 2026-11-02)
8. https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/releases (accessed 2026-11-02)
9. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/releases.md (accessed 2026-11-02)
10. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/MAINTAINERS.md (accessed 2026-11-02)
11. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/api.md (accessed 2026-11-02)
12. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/vmm/src/api/openapi/cloud-hypervisor.yaml (accessed 2026-11-02)
13. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/snapshot_restore.md (accessed 2026-11-02)
14. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/live_migration.md (accessed 2026-11-02)
15. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/fs.md (accessed 2026-11-02)
16. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/device_model.md (accessed 2026-11-02)
17. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/threat-model.md (accessed 2026-11-02)
18. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/cpu.md (accessed 2026-11-02)
19. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/seccomp.md (accessed 2026-11-02)
20. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/landlock.md (accessed 2026-11-02)
21. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/io_throttling.md (accessed 2026-11-02)
22. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/macvtap-bridge.md (accessed 2026-11-02)
23. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/disk_locking.md (accessed 2026-11-02)
24. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/docs/performance_metrics.md (accessed 2026-11-02)
25. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/release-notes.md (accessed 2026-11-02)
26. https://raw.githubusercontent.com/cloud-hypervisor/cloud-hypervisor/main/vmm/src/config.rs (accessed 2026-11-02)
27. https://www.cloudhypervisor.org/ (accessed 2026-11-02)
28. https://gitlab.com/virtio-fs/virtiofsd/-/raw/main/Cargo.toml (accessed 2026-11-02)
29. https://gitlab.com/api/v4/projects/virtio-fs%2Fvirtiofsd/repository/tags (accessed 2026-11-02)
30. https://raw.githubusercontent.com/cloud-hypervisor/rust-hypervisor-firmware/main/LICENSE (accessed 2026-11-02)
31. https://raw.githubusercontent.com/cloud-hypervisor/rust-hypervisor-firmware/main/Cargo.toml (accessed 2026-11-02)
32. https://api.github.com/repos/cloud-hypervisor/rust-hypervisor-firmware/releases (accessed 2026-11-02)
33. https://api.github.com/repos/cloud-hypervisor/edk2/releases (accessed 2026-11-02)
34. https://www.gnu.org/licenses/license-list.html#apache2 (accessed 2026-11-02)
35. https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/README.md (accessed 2026-11-02)
36. https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/docs/design.md (accessed 2026-11-02)
37. https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/docs/snapshotting/snapshot-support.md (accessed 2026-11-02)
38. https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/src/firecracker/swagger/firecracker.yaml (accessed 2026-11-02)
39. https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/src/firecracker/Cargo.toml (accessed 2026-11-02)
40. https://api.github.com/repos/firecracker-microvm/firecracker/releases (accessed 2026-11-02)

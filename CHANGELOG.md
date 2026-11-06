# Changelog

All notable changes to Quoro are recorded in this file, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Quoro is an experimental preview: until the first general release, any version may break compatibility.

Each entry links the pull request that made the change and, where one exists, the architecture decision record (ADR) behind it. The milestone each release belongs to is shown in the [roadmap's status table](https://github.com/OmarAlex24/quoro/blob/main/docs/product/roadmap.md#status).

## [Unreleased]

### Added

- **Warm volume calls in the engine.** The engine trait gains the calls warm replication is built on: `volume_changed_ranges` compares the extent maps of two reflink snapshots of a volume and returns only the byte ranges that differ (a full copy when there is no base or the maps cannot be compared), `stage_volume`, `commit_staged_volume` and `discard_staged_volume` build a volume's next copy beside the last good one and swap it in with one rename, and `pause` and `resume` freeze a running VM. A volume can now keep several snapshots, each in its own file. Nothing calls these yet; replication between nodes comes next ([#389], [#390], [ADR 0172], [ADR 0173]).

### Fixed

- After a restore moved an app to another node, the old node recreated the app's VM, stopped and without its volume, and kept it with its Cloud Hypervisor process until the node agent restarted. The old node now deletes the VM as soon as the restore is accepted, and keeps only the detached volume's file ([#361]).
- After a primary restart, a node could take up to about 25 s to reconnect: when its first redial found the primary's listener still down, it tried every address the primary advertised, each with a 10 s connect timeout, before it dialed the primary's address again. A node now dials a primary that refuses the connection again for up to 2 s before it tries the other addresses, and gives each advertised address a 2 s connect timeout that grows with every failed attempt, so it reconnects within about a second of the restart. Stopping the node agent no longer waits for a dial in progress ([#368], [ADR 0139]).
- An edge reported a backend leaving or rejoining the rotation to the primary only with its next periodic status report, so the API and the panel could lag the edge by up to 30 s. The edge now sends a status report as soon as its backend list changes. A backend's `last_error` now names the backend and the root cause, in the same words whether the proxy or a health probe saw the failure, instead of `client error (Connect)` or `client error (SendRequest)` ([#369], [ADR 0143]).
- While a node was disconnected, the app's Instances list and the VM API kept showing its instances as `running`, the status the node last reported. The API now shows them as `unknown` until the node is connected again, and the panel says why: `unknown (node disconnected)`. Nothing is written for it: the primary keeps the last report, so a disconnect or a primary restart emits no event and changes no deployment ([#377]).
- A node reported a newly applied revision to the primary only with its next periodic status report, unless the revision also changed a VM or a route, so `applied_rev` in the API could lag by up to 30 s, on an edge above all. The node now sends a status report about 250 ms after it applies a revision ([#379]).
- A terminal command that finished before the client's WebSocket attached could not be read: the upgrade answered `401`, as if the ticket were bad, and the command's output and exit code were lost. A command with no output that exited within about a second was refused with `409`. The session now keeps the output and the exit until its ticket expires, so the client receives them, then `exit` and `closed`, and the audit record shows the session as connected ([#381], [ADR 0186]).
- A node agent that died in the middle of a self-fence came back with the lease revoked but did not stop its VMs again, and the VMs outlive the agent, so a VM could keep running without its lease. The agent now records a fence, together with the lease's state, only once its stop has finished, answers the revoke `ok` only once that record is written, and fences again at start any revoked or expired lease without a record. A fence also stops the VMs the latest desired state no longer names, so a VM dropped from it just before the revoke is not missed. Fences of different leases now run at the same time instead of one after another, the agent keeps taking messages while one runs, and a stateful VM no longer starts while a fence of its lease is under way ([#385], [ADR 0075], [ADR 0019]).
- After a restore was accepted, the old node could still receive a desired state that named the moved VM after its lease had been revoked, when that snapshot had been built just before the restore. A snapshot built before a lease revoke is now dropped and built again, so nothing the revoke took away reaches the node after it. Deleting a volume had the same race ([#388]).
- The panel offered `app:terminal`, `backup:manage` and `backup:restore` in the agent grant and webhook forms, and the API then refused the grant. Its human-only and destructive scope lists now match the permission model, and a test reads both lists from the contract ([#397]).
- The panel's webhook dialog did not offer the `build`, `backup`, `restore`, `volume` and `domain` events, so they could only be subscribed to through the API. The dialog now offers every event type of the contract, grouped by subject ([#398]).
- Re-running the installer to upgrade a set-up primary printed `warning: could not read a setup token` and the first-run steps again, and a node or an edge printed its first-run text. The installer now reads the version it replaces and says `upgraded from 0.1.1 to 0.2.0`. On a primary whose cluster is set up, it skips the setup token and the first-run steps and names the next step, upgrading the nodes and edges. A re-run without `QUORO_ROLE` on a node or an edge installed a second control plane next to the agent; the installer now refuses it and names the role to pass. The installer README documents the `edge` role, the `QUORO_JOIN_*` variables for an edge, and how to upgrade ([#401]).
- After a primary restart, the desired state could name a new digest for an app instance that was already running: the primary resolved the image tag again with an empty cache, and if the tag had moved, it asked the node for a digest no instance runs. The node refused the change, as a VM's digest is fixed when it is created, and logged the same warning on every pass. An instance now keeps the digest its deployment recorded when it was created; only a new deployment resolves the tag again. The node logs the refusal once per VM and digest ([#403]).
- A terminal opened on a VM that was still booting was refused with "the VM's init does not accept exec sessions; restart the VM", although the VM only needed a few more seconds: its init accepts terminals once it has mounted the volumes and set up the network. The message now says the VM may still be starting, and that only a VM which has been running for a while needs a restart. The engine's KVM test suite now waits for the guest before its first command, and its teardown removes both of the bridge's firewall tables instead of only one ([#418]).

## [v0.2.0] - 2026-11-05

Milestone M5a: stateless replicas behind an edge, local volumes, cold backups and vulnerability tracking. The milestone is complete: its demo ran on the lab and was approved on 2026-11-05. Still an experimental preview, not for production ([ADR 0137]).

### Storage

- **Local volumes.** An app can have a persistent volume on its node: `POST /apps/{app_id}/volumes` with a size and a mount path. The guest mounts it at boot, and the volume is attached again to the new VM on every redeploy. The app's node holds a `local` lease for it, and an agent needs a human's approval to delete a volume ([#251], [#259], [ADR 0145], [ADR 0019]).
- **Snapshots without pausing the app.** Volume snapshots use reflink when the node's data directory supports it. When it does not (ext4, for example), the node keeps its volumes in a loop-mounted XFS pool that can reflink, so a snapshot clones the file instead of pausing the VM ([#251], [#272], [ADR 0146], [ADR 0156]).
- **Chunk engine.** A new `quoro-backup` crate splits data into content-defined chunks, then compresses and encrypts every chunk on the client before upload. A recovery key protects the repository key ([#252], [ADR 0147]). It is paired with a new S3-compatible object-store client that writes without overwriting and probes a bucket's object lock ([#253]).
- **Cold backups and restore.** Apps with the `cold` policy back up their volume to an S3-compatible bucket on a schedule or on demand, and a backup can be restored onto another node. The recovery key is shown once and can be rotated ([#273], [ADR 0147]).
- **Control-plane backups.** The primary backs up its database with `VACUUM INTO` every 6 hours by default, through the same chunk engine. The new binary takes another backup before it runs any schema migration, and keeps the newest three of those outside the scheduled retention. `quoro restore` restores a backup offline, onto the same host or a new one ([#274], [ADR 0148], [ADR 0157], [ADR 0158]).
- **Off-site audit copy.** The primary copies the audit log to an S3-compatible bucket in segments that are never overwritten. A daily probe checks how strongly the bucket locks objects and records the result, and audit verification now detects a local log whose tail was cut, or that differs from the copy ([#275], [ADR 0149]).
- **Panel.** The app screen gains a storage section: its volumes, its backups with "back up now" and restore, and the backup schedule. The HA policy selector shows what `cold` costs next to the current policy. The audit screen shows the off-site copy, its lock strength and its last error ([#276], [ADR 0020]).
- Contracts for this work: the storage contract shipped in v0.1.1 ([#226]); later amendments covered the XFS volume pool ([#255]), the pre-upgrade backup ([#270]) and the audit export after a restore ([#298]).

### Replicas, mesh and edge

- **A guest subnet per node.** Every node gets its own /24 guest subnet from the primary. Running VMs keep their address and move to the new subnet on their next deploy. Upgrade the nodes together with the primary: an older node agent refuses the new subnet ([#256], [ADR 0144]).
- **Mesh.** The edge and the nodes are joined by a kernel WireGuard star with the edge as the hub, configured by the node agent over netlink. Each machine's private key never leaves it, and only the edge can reach a guest over the mesh ([#260], [ADR 0144]).
- **Replicas.** A stateless image app can run several replicas, at most one per node. A deploy rolls out one new instance per node and checks each instance's health before it retires the old ones; a failed rollout leaves the old instances serving. Changing the replica count needs no new deploy, and an app with only some of its instances up is reported `degraded` ([#261], [ADR 0142]).
- **Edge role.** `quoro join --role edge` enrolls a machine that runs no VMs and needs no KVM. The edge routes every replicated app round robin over its healthy replicas and runs its own health checks, so it takes a failed replica out of rotation and puts it back once it recovers. It keeps serving its last known routes while the primary is unreachable, and reports backend health to the primary ([#262], [#242], [ADR 0143]).
- **Panel.** The app screen shows the replica count, the node each instance runs on, and, behind an edge, each backend's rotation state and last error ([#263]).
- The contract for replicas, the edge and the mesh shipped in v0.1.1 ([#227]).

### Vulnerability tracking

- **Image inventory.** Every image a node prepares gets a package inventory, which the node sends to the primary. The inventory reads Debian, Alpine and RPM package databases, npm lockfiles, Python package metadata, Go build info and `cargo auditable` data, all inside the image root and without running anything from the image ([#300], [ADR 0150], [ADR 0151]).
- **Local OSV mirror.** The primary keeps its own copy of the OSV data for the ecosystems its images use, plus CISA's Known Exploited Vulnerabilities (KEV) catalog. It checks for updates hourly, and `quoro vuln import` loads the data offline ([#305], [ADR 0150]).
- **Findings and the `app.vulnerable` incident.** The primary matches each inventory against that data with each ecosystem's own version ordering and records the findings. A new finding that is in KEV, or scores 9.0 or more in CVSS, raises `app.vulnerable`. That opens an incident, which can wake an agent. The API serves the findings per app and as a cluster summary ([#307], [ADR 0150], [ADR 0160]).
- **SBOM and Quoro's own components.** The release build writes a CycloneDX SBOM that the release workflow attests, and the same component list is compiled into the binary. The primary checks Quoro's own crates, the guest kernel, Cloud Hypervisor, cloudflared, BuildKit and every node's host kernel against the same data. Host kernels stay the operator's: Quoro reports them and never upgrades them ([#309], [ADR 0138], [ADR 0151], [ADR 0161]).
- **Panel.** The app page has a "Vulnerabilities" section: what was checked, the age of the data, counts by severity, and the open findings with filters. The nodes page shows a cluster summary for the Quoro release, the guest kernel and each host kernel ([#310], [ADR 0138]).
- **Rebuild with refreshed base.** A Git app can be rebuilt with every base image pulled again, from the vulnerability section or next to "Build now" (`startBuild` with `refresh_base: true`). An automatic rebuild on a critical finding is deferred to M5b ([#294], [#310], [ADR 0155]).
- **Images cached before M5a.** A node now inventories, once and in the background, the images it cached before M5a, reading their cached root disk. No redeploy and no network access is needed ([#318], [#320], [ADR 0163]).
- **Linux kernel ranges.** Kernel findings are read per stable series, so a fully patched kernel on an older series is no longer flagged ([#314], [ADR 0162]). Ranges whose fix is below the version that introduced the bug are now ignored ([#323]).
- The contract for vulnerability tracking shipped in v0.1.1 ([#237]); the decisions taken while building it are ADRs 0160 to 0163 ([#319]).

### Fixed

- The edge could lose its WireGuard sessions with the nodes on every mesh refresh, which took replicas out of rotation for up to 25 seconds. Peers are now updated in place ([#285]).
- A VM created before its node's subnet changed could not start again after a host reboot. It now starts on the bridge it was created with ([#281]).
- A browser that opened a terminal just as its session ended got a reset connection. It now receives the real reason the session closed ([#278]).
- A node reported 0 GB of free disk, so the primary refused to create volumes on it ([#272]).
- After `quoro restore`, the restored primary showed the backup it came from as still running, which blocked scheduled backups for 24 hours. Interrupted control-plane backups are now resolved from their seals at start ([#301], [#302]).
- After `quoro restore` onto another host, the primary role now moves to that host's node ([#299], [#304]).
- A build's log stream could end without the build's last line ([#315]).
- A second process writing the database while the server ran (for example `quoro vuln import`) could make a write fail with "database is locked" ([#316]).
- A repository's first backup could start the garbage-collection sweep, which then refused new backups while it ran. The sweep now runs only after a pruning ([#317]).
- A node session that had already been replaced could, while closing, mark its live node as disconnected. Only the node's current session now records a disconnect ([#325]).
- Writes to the vulnerability cache could queue for many seconds while each one held a pooled connection, which stalled node sessions until they timed out. The cache now takes one writer at a time, and queued writers no longer hold connections ([#328]).
- When a replica died without closing its connections, a request the edge had already sent to it over a pooled connection waited for the full 60 second response timeout. The edge now cancels the requests still waiting on a replica that leaves the rotation, and drops its pooled connections. A `GET` or `HEAD` that has not answered yet is retried once on another replica, and any other request answers `502` at once ([#334]).
- A `GET` sent to a replica's pooled connection right after the replica died still got a `502` after about 20 seconds when the replica's host reported it unreachable. It is now retried once on another replica, and the replica leaves the rotation at once. A client that reset its own upload could also take a healthy replica out of rotation; it no longer does ([#338]).
- A machine with only the edge role always reported `applied_rev` 0 in `GET /api/v1/nodes`. It now reports the newest desired state it has applied ([#341]).
- A new `tunnel` domain, or a hostname that moved between a node and the edge when an app's replica count crossed 1, could wait up to 60 seconds for its tunnel route when the machine had no live session. The primary now publishes those changes at once. After such a move, the old tunnel could also delete the CNAME the new tunnel had just pointed at itself, which left the hostname without DNS. A tunnel now deletes a CNAME only while that CNAME still points at it ([#343]).
- Scaling an app with `tunnel` domains down to one replica could keep the instance on a node without a tunnel, and the hostnames stopped answering with no sign of why. A scale-down now keeps the instances on nodes with a tunnel first. When no remaining instance can serve the app's `tunnel` domains, each one gets a `certificate.failed` event with `tls_mode` `tunnel` ([#347]).
- When a `tunnel` hostname moved between a node's tunnel and the edge's, the tunnel that lost it deleted its CNAME and the other tunnel created a new one. Cloudflare took longer to route the new record, and the hostname answered `404` for up to about a minute. The tunnel that gains the hostname now repoints the existing record, and the one that loses it leaves the record alone. The losing node still stops serving the hostname at once, so a short window of `404`s remains ([#348]).

### Maintenance

- The M5a decisions were recorded as ADRs 0139 to 0158 ([#267]). `Cargo.lock` now records `quoro-backup` 0.1.1 ([#258]). Tests that failed intermittently on loaded CI runners were made deterministic ([#265], [#269], [#282], [#286], [#340]). A changelog and a milestone status table were added ([#329]). The decisions taken while finishing M5a, including the two deferred to M5b, are ADRs 0164 to 0168 ([#353]).

### Known issues

- Changing the replica count of an app with `tunnel` domains across 1 moves its hostnames between a node's tunnel and the edge's. For up to about two minutes afterwards, part of the requests answer `404` while Cloudflare moves traffic to the new tunnel. The DNS record is kept; a grace window, in which the machine that lost the hostname keeps serving it, is planned for M5b ([#344]).
- While a node is disconnected, the app's instances list keeps showing that node's instance as `running`. The edge backends, the metrics and the nodes page show the outage ([#342]).
- The primary's API can take up to about 30 seconds to show that the edge took a backend out of rotation. The edge itself acts within seconds, so traffic is not affected ([#339]).
- `applied_rev` in `GET /api/v1/nodes` can lag the revision a machine has applied by up to about 30 seconds when the revision changes no VM and no route ([#346]).
- Web terminal: a command that ends before the client attaches its WebSocket loses its output, and the upgrade answers `401`. The command still runs. An interactive shell is not affected ([#240]).

### Upgrading from v0.1.1

- Upgrade the node agents together with the primary. An older node agent refuses the per-node guest subnet, and the primary treats a node agent older than M5a as one that cannot hold a volume ([ADR 0144]).
- Every node except the oldest gets its own guest subnet. Running VMs keep their address and move to the new subnet on their next redeploy.
- After its node agent starts, a node inventories the images it cached before this release, once and in the background. No redeploy is needed ([ADR 0163]).
- The installer now installs the optional `xfsprogs` package when `mkfs.xfs` is missing. Without it, a node whose data directory cannot reflink (ext4, for example) pauses the app to snapshot a volume ([ADR 0156]).
- From this release on, a new binary backs up the control-plane database before it applies a schema migration, when a backup repository is configured ([ADR 0157]). v0.2.0 applies no migration to a v0.1.1 database, so this upgrade takes no such backup.

## [v0.1.1] - 2026-11-04

Still an experimental preview, not for production ([ADR 0137]). This release carries the groundwork for M5a: the guest kernel, a primary that can change address, durable webhook delivery, and the M5a contracts.

### Added

- **Quoro's own guest kernel.** App and builder microVMs boot Linux 7.2.15, built by Quoro from upstream stable plus Cloud Hypervisor's guest patches and `ch_defconfig`. It fixes CVE-2026-98432 in the guest kernel. The kernel ships as its own signed release, `guest-kernel-7.2.15-q1`, which `packaging/kernel/reproduce-kernel.sh` reproduces bit for bit, and the installer pins it by sha256 ([#229], [#244], [ADR 0152]).
- **Nodes follow a primary whose address changes.** The primary advertises its addresses when a node connects, and a node that cannot reach its primary tries them with the same pinned certificate. `quoro node set-primary <url>` points a node at a new address by hand ([#215], [#222], [#223], [ADR 0139]).
- **Webhooks survive primary restarts.** Delivery is at-least-once, with the event id as the deduplication key; a restart no longer drops events that were committed but not yet sent ([#214], [#221], [ADR 0140]).
- The M5a plan and contracts: vulnerability tracking planned ([#213], [ADR 0138]), spike notes ([#217]), and the contracts for storage ([#226]), for replicas, the edge and the mesh ([#227]), and for vulnerability tracking ([#237]).

### Upgrading from v0.1.0

Running VMs keep the kernel they booted with. Redeploy each app to move it to the 7.2.15 kernel.

## [v0.1.0] - 2026-11-04

Quoro's first public preview, carrying milestones M3 and M4. **Experimental, not for production**; expect breaking changes before the first general release ([ADR 0064], [ADR 0137]).

### Added

- **Agents as their own identities.** A service layer shared by the API and MCP, with agents, agent keys and `atk_` access tokens, and panel screens to manage agents, their keys and their grants ([#106], [#107], [#136], [ADR 0106]).
- **Grants and approvals.** Every call is authorized against the caller's scoped, time-limited grants. Destructive agent actions wait for a human's just-in-time approval, which the panel's approvals queue handles ([#108], [#113], [#114]).
- **Audit log.** List and read audit records and verify the hash chain, from the API and the panel ([#108], [#115]).
- **MCP server.** `/mcp` serves the same operations as the API, with tools for approvals, events, incidents and the audit log, and `quoro mcp stdio` connects local MCP clients ([#117], [#120], [#121], [#138]).
- **Events, incidents and webhooks.** Signed webhooks deliver events and can wake an agent for an incident, with `data.wake` naming the agent and its grant. The panel lists deliveries, sends test deliveries and marks them as tests ([#109], [#116], [#134], [#152], [#153], [ADR 0103], [ADR 0104], [ADR 0111]).
- **Reference agent runner** in `examples/agent-runner`, with a built-in deterministic policy ([#118], [#135], [ADR 0049], [ADR 0102]).
- **User names everywhere.** Operators and viewers can resolve user ids to names ([#134], [#136], [ADR 0107]).
- **Git apps.** An app can be created from a public Git repository with a Dockerfile. Each build runs on the target node in an ephemeral BuildKit builder microVM, and the built image is deployed. The panel follows a build live, and a failed build shows its failing step and log ([#171], [#172], [#173], [#174], [ADR 0117], [ADR 0118], [ADR 0122], [ADR 0123], [ADR 0124], [ADR 0125]).
- **Metrics.** CPU and memory for every node and app, sampled by the node agent, served by the API and shown live in the panel ([#164], [#167], [#168], [ADR 0129]).
- **Web terminal and VM console.** Humans can open a terminal into an app; every session is audited and its transcript is hashed. Every VM also has a read-only serial console. Both are opened with single-use tickets and relayed through the node agent ([#177], [#178], [#179], [#180], [#181], [ADR 0113], [ADR 0114], [ADR 0115], [ADR 0116], [ADR 0126], [ADR 0127], [ADR 0128]).
- **Users.** Invite users with roles and manage them from the API and the panel, and let a signed-in user change their own password ([#165], [#166], [#201], [#205], [ADR 0135]).
- **Documentation site.** The docs are built as an mdBook site in CI and published to GitHub Pages on every release ([#132], [#200], [ADR 0109], [ADR 0130]).
- The M3 and M4 contracts and spike notes ([#133], [#155], [#156], [#160], [#163], [#202]), ADRs 0102 to 0136 ([#131], [#159], [#194]), and the M2 and M3 Definitions of Done marked as met ([#111], [#146]).

### Changed

- VMs created before this version have no vsock device and no socket-mode serial port. Their terminal and console show "Redeploy to enable": a restart is not enough, only a redeploy adds them ([#204], [ADR 0136]).
- CI now type-checks, tests and builds the panel and tests the agent runner ([#125]).

### Fixed

- Webhooks never delivered from the release binary, because no TLS crypto provider was installed ([#126]).
- `quoro mcp stdio` now negotiates the MCP protocol version with the client ([#142]).
- The agent runner sends its User-Agent when it checks a URL, reports the status it saw, and waits for the app's health check before it reports a recovery ([#143], [#144]).
- The engine accepts the host-side port in Cloud Hypervisor's hybrid vsock reply ([#185]).
- The installer installs `git` on hosts that run the node agent ([#186]).
- A node's allocated capacity is summed from its VMs when it is read, so it no longer drifts ([#188]).
- The transcript hash is recorded when the primary ends a terminal session ([#191]).
- A build waiting for capacity fetches its source once and logs the wait once ([#207]).
- Tests of the node session, webhooks and VMM start-up are deterministic, and the release build image retries apt ([#203], [#208]).

### Upgrading from v0.0.2

Redeploy each app that needs the terminal or the console; see "Changed" above.

## [v0.0.2] - 2026-11-03

Milestone M2: deploy an OCI image with a domain and TLS, through nodes that keep running when the control plane is gone.

### Added

- **Dial-out node agent.** Nodes enroll with the primary, dial out to it, receive a signed desired state and reconcile it locally ([#71]).
- **Privilege split.** The control plane runs unprivileged; only the node agent runs as root, as its own service ([#73], [ADR 0089], [ADR 0096]).
- **Providers.** An API for provider credentials, with secrets encrypted under the cluster data-encryption key ([#70], [ADR 0050]).
- **App VMs.** The node agent runs app VMs with console log capture, health checks and log streams ([#84]).
- **Apps, deployments and domains API,** with live logs over server-sent events ([#85]).
- **Panel.** A deploy screen, the app list and detail, and a live log view ([#86]).
- **Ingress.** An ingress interface with the embedded reverse proxy and a Cloudflare Tunnel backend. The primary manages one tunnel per node and the ACME DNS-01 challenges; nodes serve app routes through the tunnel connector or ACME, and the installer installs `cloudflared` ([#82], [#83], [#88], [#89], [ADR 0059], [ADR 0094]).
- **Certificate events** name the domain they are about ([#93], [ADR 0099]).
- Contract additions: isolation groups, guest DNS, log fields and node credentials ([#69]); per-node tunnel tokens and DNS-01 through the primary ([#80]); `disk_gb` of at least 1 for apps and `domain_id` on routes ([#92], [ADR 0098], [ADR 0099]). ADRs 0094 to 0099 ([#81], [#91]), and the M1 Definition of Done marked as met ([#60]).

## [v0.0.1] - 2026-11-03

Milestones M0 and M1: frozen contracts, then a one-command install with the engine lifecycle and a basic panel. Both v0.0.x builds came before the first public preview, v0.1.0.

### Added

- **The bases.** Governance files and agent rules, sourced research, the product requirements, the roadmap, the glossary, the architecture, the threat model and the M0 contracts with a contract checker ([#1], [#2], [#3], [#4], [#22]).
- **Engine contract.** The `Engine` trait with an in-memory fake engine, and the node lease state machine with self-fencing ([#13], [#14], [#21], [ADR 0019], [ADR 0071]). A failed fence is reported with `fence_failed` and acknowledged by a human through an endpoint ([#32]).
- **Boot spike.** A microVM boots from an OCI image ([#15]).
- **Cloud Hypervisor engine.** An OCI-to-ext4 image pipeline and Quoro's own guest init, the Cloud Hypervisor backend, and `quoro serve` running it ([#39], [#41], [#49], [ADR 0061]).
- **Control plane** with setup, sessions and the VM lifecycle API; a VM's `disk_gb` is at least 1 ([#37], [#43], [ADR 0083]).
- **Web panel,** embedded in the binary ([#40]).
- **One-command installer** ([#35]).
- **Signed, reproducible releases.** Release binaries are signed with keyless GitHub artifact attestations and built in a pinned container, so any host reproduces them; the release includes the panel and the engine runtime ([#34], [#50], [#57], [ADR 0027], [ADR 0081], [ADR 0090]).
- **CI.** A cargo workspace with a pinned toolchain, and `cargo-deny` enforcing the dependency license and advisory policy ([#12], [#33], [ADR 0080], [ADR 0086]).
- ADRs 0081 to 0090 ([#53], [#56]); smaller debug info in dev and test builds ([#46]).

[Unreleased]: https://github.com/OmarAlex24/quoro/compare/v0.2.0...HEAD
[v0.2.0]: https://github.com/OmarAlex24/quoro/releases/tag/v0.2.0
[v0.1.1]: https://github.com/OmarAlex24/quoro/releases/tag/v0.1.1
[v0.1.0]: https://github.com/OmarAlex24/quoro/releases/tag/v0.1.0
[v0.0.2]: https://github.com/OmarAlex24/quoro/releases/tag/v0.0.2
[v0.0.1]: https://github.com/OmarAlex24/quoro/releases/tag/v0.0.1

[#1]: https://github.com/OmarAlex24/quoro/pull/1
[#2]: https://github.com/OmarAlex24/quoro/pull/2
[#3]: https://github.com/OmarAlex24/quoro/pull/3
[#4]: https://github.com/OmarAlex24/quoro/pull/4
[#12]: https://github.com/OmarAlex24/quoro/pull/12
[#13]: https://github.com/OmarAlex24/quoro/pull/13
[#14]: https://github.com/OmarAlex24/quoro/pull/14
[#15]: https://github.com/OmarAlex24/quoro/pull/15
[#21]: https://github.com/OmarAlex24/quoro/pull/21
[#22]: https://github.com/OmarAlex24/quoro/pull/22
[#32]: https://github.com/OmarAlex24/quoro/pull/32
[#33]: https://github.com/OmarAlex24/quoro/pull/33
[#34]: https://github.com/OmarAlex24/quoro/pull/34
[#35]: https://github.com/OmarAlex24/quoro/pull/35
[#37]: https://github.com/OmarAlex24/quoro/pull/37
[#39]: https://github.com/OmarAlex24/quoro/pull/39
[#40]: https://github.com/OmarAlex24/quoro/pull/40
[#41]: https://github.com/OmarAlex24/quoro/pull/41
[#43]: https://github.com/OmarAlex24/quoro/pull/43
[#46]: https://github.com/OmarAlex24/quoro/pull/46
[#49]: https://github.com/OmarAlex24/quoro/pull/49
[#50]: https://github.com/OmarAlex24/quoro/pull/50
[#53]: https://github.com/OmarAlex24/quoro/pull/53
[#56]: https://github.com/OmarAlex24/quoro/pull/56
[#57]: https://github.com/OmarAlex24/quoro/pull/57
[#60]: https://github.com/OmarAlex24/quoro/pull/60
[#69]: https://github.com/OmarAlex24/quoro/pull/69
[#70]: https://github.com/OmarAlex24/quoro/pull/70
[#71]: https://github.com/OmarAlex24/quoro/pull/71
[#73]: https://github.com/OmarAlex24/quoro/pull/73
[#80]: https://github.com/OmarAlex24/quoro/pull/80
[#81]: https://github.com/OmarAlex24/quoro/pull/81
[#82]: https://github.com/OmarAlex24/quoro/pull/82
[#83]: https://github.com/OmarAlex24/quoro/pull/83
[#84]: https://github.com/OmarAlex24/quoro/pull/84
[#85]: https://github.com/OmarAlex24/quoro/pull/85
[#86]: https://github.com/OmarAlex24/quoro/pull/86
[#88]: https://github.com/OmarAlex24/quoro/pull/88
[#89]: https://github.com/OmarAlex24/quoro/pull/89
[#91]: https://github.com/OmarAlex24/quoro/pull/91
[#92]: https://github.com/OmarAlex24/quoro/pull/92
[#93]: https://github.com/OmarAlex24/quoro/pull/93
[#106]: https://github.com/OmarAlex24/quoro/pull/106
[#107]: https://github.com/OmarAlex24/quoro/pull/107
[#108]: https://github.com/OmarAlex24/quoro/pull/108
[#109]: https://github.com/OmarAlex24/quoro/pull/109
[#111]: https://github.com/OmarAlex24/quoro/pull/111
[#113]: https://github.com/OmarAlex24/quoro/pull/113
[#114]: https://github.com/OmarAlex24/quoro/pull/114
[#115]: https://github.com/OmarAlex24/quoro/pull/115
[#116]: https://github.com/OmarAlex24/quoro/pull/116
[#117]: https://github.com/OmarAlex24/quoro/pull/117
[#118]: https://github.com/OmarAlex24/quoro/pull/118
[#120]: https://github.com/OmarAlex24/quoro/pull/120
[#121]: https://github.com/OmarAlex24/quoro/pull/121
[#125]: https://github.com/OmarAlex24/quoro/pull/125
[#126]: https://github.com/OmarAlex24/quoro/pull/126
[#131]: https://github.com/OmarAlex24/quoro/pull/131
[#132]: https://github.com/OmarAlex24/quoro/pull/132
[#133]: https://github.com/OmarAlex24/quoro/pull/133
[#134]: https://github.com/OmarAlex24/quoro/pull/134
[#135]: https://github.com/OmarAlex24/quoro/pull/135
[#136]: https://github.com/OmarAlex24/quoro/pull/136
[#138]: https://github.com/OmarAlex24/quoro/pull/138
[#142]: https://github.com/OmarAlex24/quoro/pull/142
[#143]: https://github.com/OmarAlex24/quoro/pull/143
[#144]: https://github.com/OmarAlex24/quoro/pull/144
[#146]: https://github.com/OmarAlex24/quoro/pull/146
[#152]: https://github.com/OmarAlex24/quoro/pull/152
[#153]: https://github.com/OmarAlex24/quoro/pull/153
[#155]: https://github.com/OmarAlex24/quoro/pull/155
[#156]: https://github.com/OmarAlex24/quoro/pull/156
[#159]: https://github.com/OmarAlex24/quoro/pull/159
[#160]: https://github.com/OmarAlex24/quoro/pull/160
[#163]: https://github.com/OmarAlex24/quoro/pull/163
[#164]: https://github.com/OmarAlex24/quoro/pull/164
[#165]: https://github.com/OmarAlex24/quoro/pull/165
[#166]: https://github.com/OmarAlex24/quoro/pull/166
[#167]: https://github.com/OmarAlex24/quoro/pull/167
[#168]: https://github.com/OmarAlex24/quoro/pull/168
[#171]: https://github.com/OmarAlex24/quoro/pull/171
[#172]: https://github.com/OmarAlex24/quoro/pull/172
[#173]: https://github.com/OmarAlex24/quoro/pull/173
[#174]: https://github.com/OmarAlex24/quoro/pull/174
[#177]: https://github.com/OmarAlex24/quoro/pull/177
[#178]: https://github.com/OmarAlex24/quoro/pull/178
[#179]: https://github.com/OmarAlex24/quoro/pull/179
[#180]: https://github.com/OmarAlex24/quoro/pull/180
[#181]: https://github.com/OmarAlex24/quoro/pull/181
[#185]: https://github.com/OmarAlex24/quoro/pull/185
[#186]: https://github.com/OmarAlex24/quoro/pull/186
[#188]: https://github.com/OmarAlex24/quoro/pull/188
[#191]: https://github.com/OmarAlex24/quoro/pull/191
[#194]: https://github.com/OmarAlex24/quoro/pull/194
[#200]: https://github.com/OmarAlex24/quoro/pull/200
[#201]: https://github.com/OmarAlex24/quoro/pull/201
[#202]: https://github.com/OmarAlex24/quoro/pull/202
[#203]: https://github.com/OmarAlex24/quoro/pull/203
[#204]: https://github.com/OmarAlex24/quoro/pull/204
[#205]: https://github.com/OmarAlex24/quoro/pull/205
[#207]: https://github.com/OmarAlex24/quoro/pull/207
[#208]: https://github.com/OmarAlex24/quoro/pull/208
[#213]: https://github.com/OmarAlex24/quoro/pull/213
[#214]: https://github.com/OmarAlex24/quoro/pull/214
[#215]: https://github.com/OmarAlex24/quoro/pull/215
[#217]: https://github.com/OmarAlex24/quoro/pull/217
[#221]: https://github.com/OmarAlex24/quoro/pull/221
[#222]: https://github.com/OmarAlex24/quoro/pull/222
[#223]: https://github.com/OmarAlex24/quoro/pull/223
[#226]: https://github.com/OmarAlex24/quoro/pull/226
[#227]: https://github.com/OmarAlex24/quoro/pull/227
[#229]: https://github.com/OmarAlex24/quoro/pull/229
[#237]: https://github.com/OmarAlex24/quoro/pull/237
[#240]: https://github.com/OmarAlex24/quoro/issues/240
[#242]: https://github.com/OmarAlex24/quoro/pull/242
[#244]: https://github.com/OmarAlex24/quoro/pull/244
[#251]: https://github.com/OmarAlex24/quoro/pull/251
[#252]: https://github.com/OmarAlex24/quoro/pull/252
[#253]: https://github.com/OmarAlex24/quoro/pull/253
[#255]: https://github.com/OmarAlex24/quoro/pull/255
[#256]: https://github.com/OmarAlex24/quoro/pull/256
[#258]: https://github.com/OmarAlex24/quoro/pull/258
[#259]: https://github.com/OmarAlex24/quoro/pull/259
[#260]: https://github.com/OmarAlex24/quoro/pull/260
[#261]: https://github.com/OmarAlex24/quoro/pull/261
[#262]: https://github.com/OmarAlex24/quoro/pull/262
[#263]: https://github.com/OmarAlex24/quoro/pull/263
[#265]: https://github.com/OmarAlex24/quoro/pull/265
[#267]: https://github.com/OmarAlex24/quoro/pull/267
[#269]: https://github.com/OmarAlex24/quoro/pull/269
[#270]: https://github.com/OmarAlex24/quoro/pull/270
[#272]: https://github.com/OmarAlex24/quoro/pull/272
[#273]: https://github.com/OmarAlex24/quoro/pull/273
[#274]: https://github.com/OmarAlex24/quoro/pull/274
[#275]: https://github.com/OmarAlex24/quoro/pull/275
[#276]: https://github.com/OmarAlex24/quoro/pull/276
[#278]: https://github.com/OmarAlex24/quoro/pull/278
[#281]: https://github.com/OmarAlex24/quoro/pull/281
[#282]: https://github.com/OmarAlex24/quoro/pull/282
[#285]: https://github.com/OmarAlex24/quoro/pull/285
[#286]: https://github.com/OmarAlex24/quoro/pull/286
[#294]: https://github.com/OmarAlex24/quoro/pull/294
[#298]: https://github.com/OmarAlex24/quoro/pull/298
[#299]: https://github.com/OmarAlex24/quoro/pull/299
[#300]: https://github.com/OmarAlex24/quoro/pull/300
[#301]: https://github.com/OmarAlex24/quoro/pull/301
[#302]: https://github.com/OmarAlex24/quoro/pull/302
[#304]: https://github.com/OmarAlex24/quoro/pull/304
[#305]: https://github.com/OmarAlex24/quoro/pull/305
[#307]: https://github.com/OmarAlex24/quoro/pull/307
[#309]: https://github.com/OmarAlex24/quoro/pull/309
[#310]: https://github.com/OmarAlex24/quoro/pull/310
[#314]: https://github.com/OmarAlex24/quoro/pull/314
[#315]: https://github.com/OmarAlex24/quoro/pull/315
[#316]: https://github.com/OmarAlex24/quoro/pull/316
[#317]: https://github.com/OmarAlex24/quoro/pull/317
[#318]: https://github.com/OmarAlex24/quoro/pull/318
[#319]: https://github.com/OmarAlex24/quoro/pull/319
[#320]: https://github.com/OmarAlex24/quoro/pull/320
[#323]: https://github.com/OmarAlex24/quoro/pull/323
[#325]: https://github.com/OmarAlex24/quoro/pull/325
[#328]: https://github.com/OmarAlex24/quoro/pull/328
[#329]: https://github.com/OmarAlex24/quoro/pull/329
[#334]: https://github.com/OmarAlex24/quoro/pull/334
[#338]: https://github.com/OmarAlex24/quoro/pull/338
[#339]: https://github.com/OmarAlex24/quoro/issues/339
[#340]: https://github.com/OmarAlex24/quoro/pull/340
[#341]: https://github.com/OmarAlex24/quoro/pull/341
[#342]: https://github.com/OmarAlex24/quoro/issues/342
[#343]: https://github.com/OmarAlex24/quoro/pull/343
[#344]: https://github.com/OmarAlex24/quoro/issues/344
[#346]: https://github.com/OmarAlex24/quoro/issues/346
[#347]: https://github.com/OmarAlex24/quoro/pull/347
[#348]: https://github.com/OmarAlex24/quoro/pull/348
[#353]: https://github.com/OmarAlex24/quoro/pull/353
[#361]: https://github.com/OmarAlex24/quoro/pull/361
[#368]: https://github.com/OmarAlex24/quoro/pull/368
[#369]: https://github.com/OmarAlex24/quoro/pull/369
[#377]: https://github.com/OmarAlex24/quoro/pull/377
[#379]: https://github.com/OmarAlex24/quoro/pull/379
[#381]: https://github.com/OmarAlex24/quoro/pull/381
[#385]: https://github.com/OmarAlex24/quoro/pull/385
[#388]: https://github.com/OmarAlex24/quoro/pull/388
[#389]: https://github.com/OmarAlex24/quoro/issues/389
[#390]: https://github.com/OmarAlex24/quoro/issues/390
[#397]: https://github.com/OmarAlex24/quoro/pull/397
[#398]: https://github.com/OmarAlex24/quoro/pull/398
[#401]: https://github.com/OmarAlex24/quoro/pull/401
[#403]: https://github.com/OmarAlex24/quoro/pull/403
[#418]: https://github.com/OmarAlex24/quoro/issues/418
[ADR 0019]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0019-fencing-is-mandatory-and-lease-based-node-agent-self-fences.md
[ADR 0020]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0020-per-app-ha-policies-none-cold-warm-hot-with-cost-shown-in.md
[ADR 0027]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0027-signed-binaries-and-reproducible-builds-are-mandatory.md
[ADR 0049]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0049-agent-runner-d-0032-wake-reference-runner-in-examples-agent.md
[ADR 0050]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0050-cluster-keys-separate-ed25519-signing-key-and-xchacha20.md
[ADR 0059]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0059-q-0001-embedded-rust-reverse-proxy-behind-the-ingress.md
[ADR 0061]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0061-q-0003-write-our-own-minimal-init-and-oci-to-ext4-pipeline.md
[ADR 0064]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0064-first-public-release-is-an-m3-preview-v0-1-experimental-not.md
[ADR 0071]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0071-engine-trait-async-methods-return-send-futures-fn-impl.md
[ADR 0075]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0075-concurrent-fences-and-a-mid-fence-crash-test-are-mandatory.md
[ADR 0080]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0080-dependency-license-policy-for-the-agpl-3-0-only-core-an.md
[ADR 0081]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0081-release-signing-keyless-github-artifact-attestations.md
[ADR 0083]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0083-vm-disk-gb-minimum-is-1-in-openapi-yaml-createvmrequest-and.md
[ADR 0086]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0086-cargo-deny-checks-the-targets-quoro-ships-x86-64-and.md
[ADR 0089]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0089-privilege-split-in-m2-the-api-and-panel-run-unprivileged.md
[ADR 0090]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0090-release-builds-and-reproduce-release-sh-run-inside-the-same.md
[ADR 0094]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0094-non-primary-nodes-get-only-a-per-node-tunnel-run-token-from.md
[ADR 0096]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0096-m2-process-model-quoro-service-is-the-unprivileged-control.md
[ADR 0098]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0098-appresources-disk-gb-minimum-is-1-in-openapi-yaml-matching.md
[ADR 0099]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0099-add-domain-id-to-desired-state-spec-routes-so-node.md
[ADR 0102]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0102-m3-demo-agent-runner-uses-the-built-in-deterministic-policy.md
[ADR 0103]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0103-a-webhook-delivery-from-a-webhook-with-wake-carries-data.md
[ADR 0104]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0104-ratify-the-provisional-webhook-and-event-contract-choices.md
[ADR 0106]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0106-add-get-api-v1-agents-agent-id-keys-to-openapi-yaml-so-the.md
[ADR 0107]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0107-let-operators-and-viewers-resolve-user-ids-to-names-a-read.md
[ADR 0109]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0109-docs-site-mdbook-built-from-the-repo-s-markdown-published.md
[ADR 0111]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0111-user-read-stays-human-only-webhookdelivery-exposes-the-test.md
[ADR 0113]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0113-m4-web-terminal-path-exec-through-quoro-init-over-virtio.md
[ADR 0114]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0114-m4-terminal-and-console-access-app-terminal-human-only.md
[ADR 0115]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0115-m4-session-audit-an-open-record-plus-a-session-end-record.md
[ADR 0116]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0116-m4-vm-console-read-only-serial-in-socket-mode-with-the-node.md
[ADR 0117]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0117-builder-interface-a-node-agent-builder-trait-dockerfile.md
[ADR 0118]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0118-builds-run-on-the-target-node-in-an-ephemeral-builder.md
[ADR 0122]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0122-git-apps-createapp-takes-exactly-one-of-image-source-source.md
[ADR 0123]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0123-m4-build-scope-public-https-repos-only-trigger-api-only-one.md
[ADR 0124]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0124-build-lifecycle-runs-through-desired-state-primary-emits.md
[ADR 0125]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0125-builder-vm-networking-egress-only-with-no-isolation-group.md
[ADR 0126]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0126-terminal-exec-channel-vsock-port-1024-framed-u8-type-u32-be.md
[ADR 0127]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0127-console-engine-console-superseded-by-serial-output-node.md
[ADR 0128]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0128-transcripts-terminal-only-16-mib-cap-closes-session.md
[ADR 0129]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0129-metrics-host-side-vmm-rss-and-cpu-5-s-sampling-10-minute-in.md
[ADR 0130]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0130-public-site-layout-on-quoro-dev-the-mdbook-docs-site-serves.md
[ADR 0135]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0135-q-0099-invited-users-change-their-own-password-through-a.md
[ADR 0136]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0136-q-0101-pre-m4-vms-get-the-terminal-and-console-only-after-a.md
[ADR 0137]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0137-v0-1-0-ships-without-the-trademark-search-from-main-after.md
[ADR 0138]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0138-vulnerability-tracking-and-patching-in-m5a-quoro-knows-the.md
[ADR 0139]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0139-nodes-follow-a-primary-whose-address-changes-advertised.md
[ADR 0140]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0140-webhook-delivery-is-at-least-once-across-primary-restarts.md
[ADR 0142]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0142-replicas-app-replicas-1-eligible-nodes-default-1-stateless.md
[ADR 0143]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0143-edge-role-an-enrolled-machine-with-role-edge-runs-the.md
[ADR 0144]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0144-mesh-in-m5a-kernel-wireguard-star-with-the-edge-as-hub-node.md
[ADR 0145]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0145-local-volumes-api-post-apps-app-id-volumes-size-gb-mount.md
[ADR 0146]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0146-volume-snapshots-for-m5a-reflink-ficlone-where-the-node-s.md
[ADR 0147]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0147-backup-repository-format-and-keys-fastcdc-2020-chunks-256k.md
[ADR 0148]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0148-control-plane-backup-vacuum-into-on-the-primary-every-6-h.md
[ADR 0149]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0149-off-site-audit-copy-replace-object-lock-in-compliance-mode.md
[ADR 0150]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0150-vulnerability-tracking-contract-node-sends-one-image.md
[ADR 0151]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0151-sbom-generation-own-rust-inventory-inside-the-engine-s.md
[ADR 0152]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0152-guest-kernel-build-quoro-s-own-from-upstream-stable-7-2-15.md
[ADR 0155]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0155-defer-the-opt-in-automatic-rebuild-on-critical-finding.md
[ADR 0156]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0156-volume-snapshots-reflink-where-the-node-s-data-dir-supports.md
[ADR 0157]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0157-pre-upgrade-control-plane-backup-the-new-binary-takes-the.md
[ADR 0158]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0158-control-plane-backups-with-trigger-upgrade-are-kept-outside.md
[ADR 0160]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0160-canonical-json-quoro-proto-jcs-accepts-floats-per-rfc-8785.md
[ADR 0161]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0161-m5a-quoro-component-findings-match-crates-crates-io-guest.md
[ADR 0162]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0162-evaluate-linux-kernel-osv-ranges-per-stable-series-when-a.md
[ADR 0163]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0163-inventory-images-prepared-before-m5a-from-their-cached-root.md
[ADR 0172]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0172-warm-snapshot-primitive-keep-the-reflink-snapshots-of-d.md
[ADR 0173]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0173-warm-replication-path-the-source-node-ships-changed-ranges.md
[ADR 0186]: https://github.com/OmarAlex24/quoro/blob/main/docs/adr/0186-add-287-old-node-recreates-a-moved-app-s-vm-and-240.md

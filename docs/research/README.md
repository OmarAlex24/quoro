# Research

Sourced answers to the VERIFY questions that the bases documents depend on (D-0040: every "to verify" claim is researched before it enters a document).

Each file gives the question id, a short answer, findings with a source URL and access date for every claim, the versions checked, implications for Quoro, open risks, and a source list. Every claim carries a label:

- **Confirmed**: stated by a primary source (official docs, pricing page, repository, registry) that was fetched on the access date.
- **Hypothesis**: inferred, taken from a secondary or old source, or not stated directly. Do not design on a Hypothesis without checking it first.

Research date: 2026-11-02 for the files answering Q-0010 to Q-0015; later spikes give their own date in the file. Versions and prices change, so re-check them before relying on them in a later milestone.

| Question | File | Short answer |
| --- | --- | --- |
| Q-0010 Cloud Hypervisor: license, API, boot, snapshot, virtio-fs, consoles | [cloud-hypervisor.md](cloud-hypervisor.md) | v54.0, Apache-2.0 AND BSD-3-Clause, HTTP/JSON API over a UNIX socket. Snapshots hold RAM and device state but not disks, and there is no incremental API. virtio-fs needs an external virtiofsd. Consoles are text-only: there is no VNC or framebuffer. |
| Q-0011 R2 conditional writes as a lease arbiter | [r2-conditional-writes.md](r2-conditional-writes.md) | R2 supports atomic `If-Match` / `If-None-Match` PUT with a single winner. Limits: 1 write/s per key, no conditional delete, no TTL, so fencing tokens are required. Support is not portable across S3 providers (B2 appears to lack it). |
| Q-0012 Durable Objects as a lease arbiter | [durable-objects.md](durable-objects.md) | A single-threaded actor with strongly consistent storage and alarms for a server-side TTL. It fits the Workers Free plan at 5 s renewal. It needs a Cloudflare account and a deployed Worker, and fencing tokens are still required. |
| Q-0013 Cloudflare Load Balancing and cloudflared replicas | [cloudflare-lb-and-tunnel.md](cloudflare-lb-and-tunnel.md) | Load Balancing is a paid add-on starting at $5/month. An HTTPS monitor using the app hostname checks the app, but there are no TCP monitors for tunnels, and the LB sees all replicas of one tunnel as a single endpoint. Replicas (up to 25) are identical copies routed to the nearest one, with no app health check. |
| Q-0014 rmcp, Litestream to R2, utoipa to a TS client | [rust-api-and-mcp-tooling.md](rust-api-and-mcp-tooling.md) | rmcp 3.8 is official but changes fast. Litestream 0.5.17 supports R2 but dropped client-side encryption. utoipa 6 (OpenAPI 3.1) works with orval and openapi-ts, with one known orval break. |
| Q-0015 VPS providers with nested virtualization | [vps-nested-virt.md](vps-nested-virt.md) | Nested KVM is the exception. Hetzner Cloud, Hostinger, Contabo VPS and netcup VPS say no. AWS (7i/8i families, opt-in), GCP (Intel), Azure (Dv3/Dv4), OCI Intel VMs, OVHcloud Public Cloud, Contabo VDS, Vultr VX1 and DigitalOcean say yes. Bare metal is the reliable path. Always probe `/dev/kvm` with a real boot. |
| M4 spike (#151): web terminal and VM console | [m4-terminal-and-console.md](m4-terminal-and-console.md) | Web terminal: exec through Quoro's own init over virtio-vsock, relayed by the node agent; human-only `app:terminal` in M4, audited as an open and a close record with a node-local transcript referenced by hash. VM console: read-only, serial in socket mode with the node agent as its only client and a node-side ring buffer, because Cloud Hypervisor serves one client and a new connection takes over. |

Accepted decisions stay authoritative. Where a finding bears on a decision, the file's "Implications for Quoro" section says so, and the conflict is raised in the project's decision ledger rather than changed here.

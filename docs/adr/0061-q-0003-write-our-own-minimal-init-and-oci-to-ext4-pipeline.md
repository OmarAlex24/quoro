# ADR 0061: Q-0003: write our own minimal init and OCI-to-ext4 pipeline, with in-root path resolution as a required hardening item

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0061
- **Tags:** engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

M0 spike (PR #15) booted two OCI images end to end on lab-1 with a 1-dependency Rust init and a ~1.4k-line pipeline; the only hard part found is resolving absolute symlinks inside the rootfs

## Decision

Q-0003: write our own minimal init and OCI-to-ext4 pipeline, with in-root path resolution as a required hardening item.

Evidence: spikes/oci-microvm (PR #15). The init (libc only, 413 KiB static) covers most of engine-trait.md section 6 (mounts, env, user, workdir, tagged stdout/stderr on hvc0, reaping, SIGTERM forwarding, exit status, FATAL then poweroff); network config and the ACPI power button remain. The pipeline (oci-client + tar + flate2 + sha2 + mkfs.ext4 -d) converts alpine in 1.7 s (1.5 s pull), handles whiteouts/opaque dirs and User resolution. Gaps to close in crates/engine: resolve layer paths inside the rootfs (openat2 RESOLVE_IN_ROOT) instead of refusing absolute-symlink parents; zstd layers; config delivery via config disk or cmdline instead of baking /quoro/config into the rootfs; oci-client friction is acceptable in the async node agent. Kernel: start from the prebuilt cloud-hypervisor/linux vmlinux config, but build and pin our own reproducibly (D-0027). Reuse alternatives (Kata agent, umoci/skopeo) not needed on this evidence.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0061 in the project decision ledger. Do not edit by hand; changes go through a new decision.

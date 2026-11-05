# ADR 0152: Guest kernel: build Quoro's own from upstream stable (7.2.15 now) plus Cloud Hypervisor's 19 patches and ch_defconfig, reproducibly in the pinned release container, attested, pin bump in its own PR (#211)

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0152
- **Tags:** m5a, security, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

No newer CH guest kernel exists (F-0242) and CVE-2026-98432 (CVSS 9.8) is fixed from 7.2.10; CH kernel builds follow VMM releases every ~2 months.

## Decision

Guest kernel: build Quoro's own from upstream stable (7.2.15 now) plus Cloud Hypervisor's 19 patches and ch_defconfig, reproducibly in the pinned release container, attested, pin bump in its own PR (#211).

A kernel workflow builds vmlinux with fixed KBUILD_BUILD_TIMESTAMP/USER/HOST in the ADR 0090 container, attests it (ADR 0081), and publishes it as a release asset that pins.txt points at; lab test: vsock exec, overlayfs and cgroup2 for builds, an app boot. Bumps follow stable releases that fix guest-relevant CVEs, flagged by the vulnerability data. Alternative: wait for CH (unknown date). Recommendation: build our own now, first task of M5a.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0152 in the project decision ledger. Do not edit by hand; changes go through a new decision.

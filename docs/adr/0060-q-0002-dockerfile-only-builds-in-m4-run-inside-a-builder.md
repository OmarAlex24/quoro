# ADR 0060: Q-0002: Dockerfile-only builds in M4, run inside a builder microVM; re-verify Railpack/Nixpacks before M4

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0060
- **Tags:** builds
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0033 starts M4 with Dockerfile builds; D-0008 forbids Docker on nodes, so builds cannot use a host Docker daemon. Railpack and Nixpacks were not researched in the bases pipeline.

## Decision

Q-0002: Dockerfile-only builds in M4, run inside a builder microVM; re-verify Railpack/Nixpacks before M4.

Options: Dockerfile only; Railpack; Nixpacks. Accepted option: Dockerfile only for M4, executed by a BuildKit daemon inside an ephemeral builder microVM (hypothesis: not yet tried in Cloud Hypervisor), output pushed to a node-local OCI layout and fed to the normal OCI->rootfs path. Railpack/Nixpacks: hypothesis that Nixpacks is in maintenance with Railpack as its successor; verify before M4 and add a zero-config builder as a second Builder implementation if it holds.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0060 in the project decision ledger. Do not edit by hand; changes go through a new decision.

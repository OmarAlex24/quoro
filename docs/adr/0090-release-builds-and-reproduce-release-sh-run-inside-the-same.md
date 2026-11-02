# ADR 0090: Release builds and reproduce-release.sh run inside the same digest-pinned Ubuntu 24.04 container image with snapshot-pinned packages, so any host with podman or docker reproduces the published artifact byte for byte

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0090
- **Tags:** release, security, installer
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer chose a pinned container on 2026-11-02. A rebuild on another distribution showed that the host C compiler (GCC 13.3 on CI vs 14.2 on Debian) changes the quoro binary, so reproduction across hosts fails the M1 DoD step 4.

## Decision

Release builds and reproduce-release.sh run inside the same digest-pinned Ubuntu 24.04 container image with snapshot-pinned packages, so any host with podman or docker reproduces the published artifact byte for byte.

Base image pinned by digest; apt from snapshot.ubuntu.com at a fixed timestamp (musl-tools, gcc and every build package at exact versions); the Rust toolchain and Node stay pinned as today. CI runs the build in that container; reproduce-release.sh runs the identical container with podman (or docker). Proven when a rebuild on a separate test host matches the CI artifact.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0090 in the project decision ledger. Do not edit by hand; changes go through a new decision.

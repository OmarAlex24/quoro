# ADR 0118: Builds run on the target node in an ephemeral builder microVM (one per build) with a persistent per-app work disk for the BuildKit cache

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0118
- **Tags:** builds, m4, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Spike: boot about 0.3 s, buildkitd ready in 0.2-0.4 s, warm rebuilds 2-4 s with the cache on a persistent work disk vs 27-57 s cold; layout produced and imported on the same node needs no registry (nothing pushed).

## Decision

Builds run on the target node in an ephemeral builder microVM (one per build) with a persistent per-app work disk for the BuildKit cache.

Builder VM: pinned moby/buildkit image (by digest, prepared by the normal prepare_image path), rootful buildkitd daemonless pattern, --oci-worker-net=host, about 2 vCPU / 1-2 GiB (Node built at 384 MiB, Go peaked at 650 MB). Work disk per app per node, capped by cache_gb and pruned with buildctl prune --keep-storage after each build, deleted with the app. One concurrent build per node by default; builds count against node capacity; timeout stops the VM. Builder VM gets its own isolation group (ADR 0084) with egress only.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0118 in the project decision ledger. Do not edit by hand; changes go through a new decision.

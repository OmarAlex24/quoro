# ADR 0274: M7, deploy experience: after M6, Quoro adds docker-compose import, private registries and Git repositories, a container engine for hosts without KVM, and Railpack builds

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0274
- **Tags:** roadmap, m7, builds, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

On 2026-11-07 the maintainer noted that many apps are deployed with Docker, so the next phases should support it. Quoro already runs OCI images from public registries (anonymous pulls only) and builds Dockerfiles from public HTTPS Git repositories (D-0060, D-0123). What people mean by deploying with Docker is mostly the four missing pieces below. The maintainer chose all four, in a new milestone after M6.

## Decision

M7, deploy experience: after M6, Quoro adds docker-compose import, private registries and Git repositories, a container engine for hosts without KVM, and Railpack builds.

Scope of M7 (deploy experience):
1. Compose import. A compose file becomes a project with one app per service: image or build, environment, volumes, ports and the internal network. Keys with no Quoro equivalent are reported to the operator, never dropped silently.
2. Private registries and private Git repositories. Credential references for image pulls and for Git sources. The AppSource description in openapi.yaml already announces this contract change.
3. A container engine for hosts without KVM. This is the later Engine implementation that D-0009 foresees, behind the Engine trait. It is not the Docker daemon: D-0008 still holds for microVM nodes. Its weaker isolation is written into the threat model before it ships.
4. Railpack builds. A second Builder behind D-0117's trait, for repositories without a Dockerfile.
M7 starts after M6's demo is approved (D-0036). Its designs, its contracts and its Definition of Done are decided when M7 is planned.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0274 in the project decision ledger. Do not edit by hand; changes go through a new decision.

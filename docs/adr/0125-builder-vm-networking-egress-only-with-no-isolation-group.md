# ADR 0125: Builder VM networking: egress_only with no isolation group

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0125
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Untrusted Dockerfiles must not reach other VMs or host services

## Decision

Builder VM networking: egress_only with no isolation group.

No other VM, no host address except NodeNetwork::dns on port 53, no 169.254.0.0/16; egress_only with an isolation group is Conflict. Engine creates a missing work disk as ext4 and never removes it; the node agent deletes it with the app.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0125 in the project decision ledger. Do not edit by hand; changes go through a new decision.

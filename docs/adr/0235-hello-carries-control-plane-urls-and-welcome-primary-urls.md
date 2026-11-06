# ADR 0235: hello carries control_plane_urls, and welcome.primary_urls lists the promotable machines' URLs after the primary's own, up to 16

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0235
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0139](0139-nodes-follow-a-primary-whose-address-changes-advertised.md)

## Context

ADR 0181 has welcome.primary_urls list the promotable nodes, but the primary cannot compute another host's addresses, and ADR 0139 caps the list at 8 entries, which several promotable machines exceed.

## Decision

hello carries control_plane_urls, and welcome.primary_urls lists the promotable machines' URLs after the primary's own, up to 16.

hello gains control_plane_urls (machines with the node role, at most 4): where this machine's control-plane listener would answer if promoted, built from its own addresses and the configured listener port (8443 by default). welcome.primary_urls lists the primary's own URLs first, then those of every other connected machine whose custody is sealed: the first URL of each, oldest enrolled first, then the second of each. The node keeps the first 16. Each dial (TCP, TLS against the pin, WebSocket upgrade) gives up after the connect timeout the node agent already applies to stored entries (#368), rather than a fixed 5 s: 2 s in the first redial attempt, doubling with each failed attempt up to 10 s. A 503 on the upgrade is a failed dial. Rejected: keeping the cap at 8, which can leave out every other machine.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0139](0139-nodes-follow-a-primary-whose-address-changes-advertised.md): the cap of welcome.primary_urls (16 entries instead of 8). The rest of [ADR 0139](0139-nodes-follow-a-primary-whose-address-changes-advertised.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0235 in the project decision ledger. Do not edit by hand; changes go through a new decision.

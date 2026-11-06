# ADR 0181: A promoted primary presents the same TLS certificate: the primary's TLS key travels sealed like the cluster keys, so every node's pin keeps working; a lower term is refused

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0181
- **Tags:** m5b, security, installer, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Nodes pin the SHA-256 of the primary's TLS leaf (ADR 0139). When the test lab's primary moved in M5a, the pin was kept by copying the certificate and key by hand. Nothing in the contracts covers the promoted node's TLS identity.

## Decision

A promoted primary presents the same TLS certificate: the primary's TLS key travels sealed like the cluster keys, so every node's pin keeps working; a lower term is refused.

The maintainer accepted option a on 2026-11-06: seal the primary TLS key and certificate per node (ADR 0180); the promoted node serves them, nodes reach it through welcome.primary_urls (which then lists the promotable nodes' addresses) with no re-pin, and the panel's certificate does not change for browsers. The blast radius does not grow beyond D-0078, which already gives every node the signing key. Option b: the primary announces each promotable node's own certificate fingerprint in signed state and nodes widen their pin set; no key copying, more protocol. Option c: the operator re-pins every node with quoro node set-primary; manual, per node, during an incident. Option a comes with the stale_term check, so an old primary that comes back with the same certificate cannot direct nodes.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0181 in the project decision ledger. Do not edit by hand; changes go through a new decision.

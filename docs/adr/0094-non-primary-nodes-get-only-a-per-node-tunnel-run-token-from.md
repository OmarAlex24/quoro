# ADR 0094: Non-primary nodes get only a per-node tunnel run token from the primary; the account API token never leaves the primary

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0094
- **Tags:** ingress, providers, contract
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Smallest blast radius: a node holds a credential that can only run its own tunnel (Q-0090 option a).

## Decision

Non-primary nodes get only a per-node tunnel run token from the primary; the account API token never leaves the primary.

The primary creates or looks up each node's Cloudflare tunnel through the providers API credential, writes DNS CNAMEs and the tunnel ingress config itself, and sends the node only {tunnel_id, token} in a new desired_state field (e.g. spec.ingress.tunnel), stored with the node-local key like env values. ACME DNS-01 TXT records for public nodes are written by the primary on the node's behalf. Ships as a standalone contract PR before epic #64 wires domains.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0094 in the project decision ledger. Do not edit by hand; changes go through a new decision.

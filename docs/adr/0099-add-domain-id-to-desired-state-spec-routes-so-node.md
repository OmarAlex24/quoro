# ADR 0099: Add domain_id to desired_state spec.routes[] so node certificate.* events can name their domain

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0099
- **Tags:** contracts, apps, ingress, m2
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

events.schema.json requires data.domain_id and a domain subject for certificate.issued/failed/expiring, but spec.routes[] carries only hostname, app_id, vm_ids, port and tls_mode; the node cannot emit those events without inventing an id. Today the M2 node agent logs the transition and reports cert_status in status_report.routes (which the primary maps to domains.cert_status), but buffers no certificate.* event.

## Decision

Add domain_id to desired_state spec.routes[] so node certificate.* events can name their domain.

An additive optional field spec.routes[].domain_id in node-agent-protocol.md 5.11, filled by the primary's desired-state builder. Ships as a standalone contract PR.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0099 in the project decision ledger. Do not edit by hand; changes go through a new decision.

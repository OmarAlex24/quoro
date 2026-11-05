# ADR 0167: Per-domain serving status and last error, so a tunnel domain that no tunnel serves shows in GET /apps/{app_id}/domains (a contract change for M5b)

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0167
- **Tags:** m5a, contracts, ingress
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Issue #345: a scale-down left tunnel domains on a node without a tunnel and the domains list showed nothing. PR #347 keeps instances on tunnel-mapped nodes and, as an in-contract stopgap, emits certificate.failed with tls_mode tunnel. Domain has no status or last_error, cert_status is fixed at not_applicable in tunnel mode, and the README describes certificate.failed as a certificate failure, so the domain resource itself still looks healthy.

## Decision

Per-domain serving status and last error, so a tunnel domain that no tunnel serves shows in GET /apps/{app_id}/domains (a contract change for M5b).

The maintainer accepted this on 2026-11-05 as a standalone contract PR in M5b; until then M5a keeps the certificate.failed stopgap of PR #347. Proposed shape: openapi Domain gains serving_status (serving | unserved | pending) and last_error (string|null); db-schema domains gains the same columns (migration). The primary sets unserved + last_error 'no instance of the app runs on a node with a tunnel' when a one-replica app's tunnel domain has no running instance on a tunnel-mapped node (and, for replicated apps, when the edge has no tunnel mapping), and clears it when a tunnel serves it again. Optionally a domain.unserved / domain.served event pair instead of reusing certificate.failed, and README wording of certificate.failed widened to 'certificate or tunnel route'. Alternative considered and rejected: refuse the scale-down with validation_failed naming the domains; domain creation on an unmapped node is not refused today, so refusing only at scale-down is inconsistent and blocks a legitimate scale-down.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0167 in the project decision ledger. Do not edit by hand; changes go through a new decision.

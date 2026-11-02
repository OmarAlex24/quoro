# ADR 0051: Provider credentials: a human-only providers resource (API + replicated table with secret_enc) added to the contract before the M0 freeze

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0051
- **Tags:** providers, ingress, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0024/D-0025 need Cloudflare API tokens, tunnel tokens, DNS zones and S3 credentials, but no API, table or CLI says where they are entered, stored, scoped or rotated; the M2 Tunnel path needs one.

## Decision

Provider credentials: a human-only providers resource (API + replicated table with secret_enc) added to the contract before the M0 freeze.

Options: (a) a providers resource before the freeze: POST/GET/DELETE /api/v1/providers (kind: dns | tunnel | lb | object_store; config; secret write-only), new human-only scope provider:manage, table providers(id, kind, name, config_json, secret_enc, created_at, rotated_at) replicated through the change log and encrypted with the cluster data-encryption key; secrets never returned by the API and redacted from audit params; the audit-copy credential is a separate put-only provider entry (threat model T-PRV-02). (b) M2 supports one path whose credential is supplied at install time (installer flag or a 0600 file), and the providers resource is a post-freeze standalone contract PR before M5a. Accepted option: (a), because the M2 DoD already allows the Tunnel path and a post-freeze PR for a security-relevant resource is the riskier order.

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0051 in the project decision ledger. Do not edit by hand; changes go through a new decision.

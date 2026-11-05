# ADR 0160: Canonical JSON (quoro_proto::jcs) accepts floats per RFC 8785 for magnitudes 0 or in [1e-6, 1e21); vulnerability findings carry cvss_score as a JSON number

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0160
- **Tags:** m5a, vulns, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

events.schema.json data_app_vulnerable requires findings[].cvss_score as number|null, and audit-log-format.md cites RFC 8785, which defines float serialization. Only our jcs.rs refused floats, so an app.vulnerable event (stored through the change log) could not carry the score without breaking the schema.

## Decision

Canonical JSON (quoro_proto::jcs) accepts floats per RFC 8785 for magnitudes 0 or in [1e-6, 1e21); vulnerability findings carry cvss_score as a JSON number.

Change: jcs::canonicalize writes a float whose magnitude is 0 or in [1e-6, 1e21) as Rust's shortest round-trip Display (equal to ECMAScript Number.prototype.toString in that range; -0 becomes 0, 10.0 becomes 10) and still refuses non-finite values, exponent-range values and unsafe integers. No existing hash changes: every float was refused before. No mixed-version risk in M5a: nodes do not follow the change log (D-0154) and the exporter and verifier ship in the same binary. Findings round scores to one decimal before hashing. Alternative rejected: score as decimal text, which violates the frozen event schema.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0160 in the project decision ledger. Do not edit by hand; changes go through a new decision.

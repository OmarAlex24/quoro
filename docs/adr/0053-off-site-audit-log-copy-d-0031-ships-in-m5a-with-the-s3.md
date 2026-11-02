# ADR 0053: Off-site audit-log copy (D-0031) ships in M5a with the S3-compatible path, gated by an object-lock probe per provider

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0053
- **Tags:** audit, roadmap, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

audit-log-format.md, permission-model.md, db-schema.sql and architecture say M5a, while the roadmap says D-0033 assigns it to no milestone; D-0031's off-site copy has no DoD anywhere, and object-lock support per provider is unresearched.

## Decision

Off-site audit-log copy (D-0031) ships in M5a with the S3-compatible path, gated by an object-lock probe per provider.

Options: (a) M5a, with the first S3-compatible client (cold backups): DoD 'export one audit segment with object lock to a bucket; an overwrite or delete is refused; tail truncation of the local log is detected against the exported copy'. (b) M3, with the audit log itself: needs an S3 client two milestones early. (c) leave unscheduled. Prerequisite for (a): a VERIFY research task on object lock in compliance mode for R2, S3, B2 and MinIO before M5a. Accepted option: (a).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0053 in the project decision ledger. Do not edit by hand; changes go through a new decision.

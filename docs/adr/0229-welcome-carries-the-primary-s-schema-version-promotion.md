# ADR 0229: welcome carries the primary's schema_version; promotion refuses a newer log, and a column missing from an older payload takes its default

- **Status:** Accepted; refined by [ADR 0297](0297-change-log-payloads-an-update-carries-the-fields-it-writes.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0229
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0297](0297-change-log-payloads-an-update-carries-the-fields-it-writes.md)

## Context

The followed log spans schema versions and its payloads are full rows as of their write. A candidate with an older binary could rebuild a database it does not understand, and nothing says what an older payload means for a newer column.

## Decision

welcome carries the primary's schema_version; promotion refuses a newer log, and a column missing from an older payload takes its default.

welcome gains an optional schema_version (the primary's meta.schema_version); the node keeps the newest it saw, and quoro cluster promote refuses when it is above the newest version the binary knows. When rebuilding, a payload written before a column existed lacks it and the row takes the column's default, so this and every later migration gives a new column a default or allows NULL. Rejected: storing the schema version in every change-log entry, which changes the entry format and its hash.

## Consequences

- [ADR 0297](0297-change-log-payloads-an-update-carries-the-fields-it-writes.md) refines which payloads take a column's default: only a create that lacks the column; an update carries the fields it writes and is merged over the row, and a delete's payload is ignored. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0229 in the project decision ledger. Do not edit by hand; changes go through a new decision.

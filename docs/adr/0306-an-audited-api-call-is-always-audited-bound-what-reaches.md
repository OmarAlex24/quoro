# ADR 0306: An audited API call is always audited: bound what reaches the audit change-log entry

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0306
- **Tags:** contracts, audit, m5b
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0031](0031-audit-log-append-only-hash-chained-optionally-copied-to-s3.md), [ADR 0302](0302-change-log-append-refuses-an-entry-a-follower-could-not.md)

## Context

Since #723 (ADR 0302), changelog::append refuses an entry a node could not receive alone and verify. Audit records travel as change-log entries with their redacted params, so a call whose params carry a body near 1 MiB, or a float the JSON parser reads back as another number, now fails with a 500 and leaves no audit record, a denial included. Before #723 the record was written and stalled change-log follow for every node, so the failure is now confined to one call, but an unaudited denial still breaks the audit promise. The API sets no body limit of its own (axum's 2 MB default applies) and the contracts state none.

## Decision

An audited API call is always audited: bound what reaches the audit change-log entry.

Options: (A) a contract body limit for the API well under 1 MiB, for example 256 KiB, answered 413 and audited without params; (B) a cap on audited params: above a size, or when the params entry would be refused, the record keeps a digest and the size instead of the params, so the audit write cannot fail for its params; (C) both. The maintainer chose option C (2026-11-09): an audited API call must always be audited. A alone still lets a float through; B alone keeps accepting large bodies the rest of the write path may refuse anyway. Either way, a contract change first, then a small code change.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0031](0031-audit-log-append-only-hash-chained-optionally-copied-to-s3.md): what an audit record keeps of its params when they would make its change-log entry one a node could not receive alone and verify: their digest and size instead; and the API's request body limit, whose 413 is audited. The rest of [ADR 0031](0031-audit-log-append-only-hash-chained-optionally-copied-to-s3.md) stands.
- Refines [ADR 0302](0302-change-log-append-refuses-an-entry-a-follower-could-not.md): what the primary writes when an audit record's params would make its entry one it may not write: the record keeps their digest and size instead, so the audit write does not fail. The rest of [ADR 0302](0302-change-log-append-refuses-an-entry-a-follower-could-not.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0306 in the project decision ledger. Do not edit by hand; changes go through a new decision.

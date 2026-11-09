# ADR 0283: listAudit can return records newest first: order (asc default, desc) and before_seq; list_audit takes the same

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0283
- **Tags:** contracts, panel, audit
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The redesigned panel's Activity feed (#563) can only read the audit log oldest first, so it opens at the first record ever written. The maintainer asked for the contract issue on 2026-11-07 (#567).

## Decision

listAudit can return records newest first: order (asc default, desc) and before_seq; list_audit takes the same.

listAudit gains optional order (asc default, desc; with desc items come newest first and next_cursor continues toward older records) and before_seq (only records with seq below it; with after_seq it bounds a window). MCP list_audit takes both. No parameter keeps today's ascending order. Filters apply in both orders. verifyAudit and the off-site copy do not change. Panel: Activity opens newest first with Load older; agent call counts stop reading from the oldest record. Standalone contract PR after the ADR. Issue #567.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0283 in the project decision ledger. Do not edit by hand; changes go through a new decision.

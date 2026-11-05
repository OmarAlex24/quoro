# ADR 0140: Webhook delivery is at-least-once across primary restarts: the dispatcher keeps a durable cursor (the last dispatched change-log seq) and resumes from it at start-up, so no committed event is skipped; receivers dedupe on the event id

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0140
- **Tags:** webhooks, contracts, m5a
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

#209: the dispatcher seeds its cursor from max_seq when its task first runs, so events committed just before a restart or during start-up are never delivered (at-most-once by accident). A woken agent that misses an incident event never acts.

## Decision

Webhook delivery is at-least-once across primary restarts: the dispatcher keeps a durable cursor (the last dispatched change-log seq) and resumes from it at start-up, so no committed event is skipped; receivers dedupe on the event id.

Store the cursor in the local database, advanced in the same transaction that records the delivery attempts for a seq. At start-up, resume from it; on first start ever, start from the current max_seq (no backlog of history). A delivery may repeat after a crash between send and commit, so the contract states at-least-once and that the event id is the dedupe key (it already is unique). Bounded catch-up: events older than the delivery retention are skipped with a dispatcher.gap log line. Contract change: one paragraph in the webhook section, standalone PR. Proposed for M5a, or for the M4 close-out if it is small.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0140 in the project decision ledger. Do not edit by hand; changes go through a new decision.

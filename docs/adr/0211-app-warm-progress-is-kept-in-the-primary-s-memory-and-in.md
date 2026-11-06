# ADR 0211: App.warm progress is kept in the primary's memory, and in_sync means a lag of at most two intervals

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0211
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The milestone plan asks for App.warm but not what is stored or how the state is derived.

## Decision

App.warm progress is kept in the primary's memory, and in_sync means a lag of at most two intervals.

The nodes report status_report.warm entries (role, snapshot_seq, taken/completed times, allocated_bytes, extent_count, frozen, rejected_token, error). The primary keeps the newest per node in memory, like edge backends, and never writes them: storing them would append a change-log entry per warm app every interval. WarmCopy.state: syncing until a first complete copy, in_sync while lag_s <= 2 x interval_s, lagging above, failed on an error, a disconnected node or no eligible node. The progress fields are null after a primary restart until the nodes report. extent_count is reported because the spike showed cycle cost grows with it.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0211 in the project decision ledger. Do not edit by hand; changes go through a new decision.

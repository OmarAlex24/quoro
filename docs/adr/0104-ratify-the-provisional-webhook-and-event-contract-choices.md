# ADR 0104: Ratify the provisional webhook and event contract choices from #116

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0104
- **Tags:** contracts, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-03.

## Decision

Ratify the provisional webhook and event contract choices from #116.

(1) deleteWebhook deletes its delivery history (db-schema ON DELETE CASCADE wins; fix the openapi prose). (2) testWebhook replays the newest event as a test delivery and answers 422 validation_failed on an empty event log; add 422 to its responses; a test delivery must never collide with real deliveries (#127). (3) UpdateWebhookRequest cannot clear wake; to stop a webhook from waking an agent, delete and recreate it (its signing secret rotates); document it. (4) list cursors for events and incidents are opaque strings; events page by seq ascending, incidents by id newest first; document both. Standalone contract PR.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0104 in the project decision ledger. Do not edit by hand; changes go through a new decision.

# ADR 0286: An app's change-log payload carries warm_node_id, warm_node_pinned and warm_interval_s beside the App fields, and App.warm stays null in it

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0286
- **Tags:** m5b, warm, contracts, changelog
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The warm policy is the first writer of apps.warm_node_id, warm_node_pinned and warm_interval_s (migration 0011). An app's change-log payload is the App object (db-schema.sql: 'a row, an API object and an event payload use the same field names'). App has no top-level field for these columns. They appear only inside App.warm (WarmCopy), which is derived on read from in-memory reports, like ha_cost and lease, so update_app writes it as null in the payload. A replica that rebuilds apps from the change log would lose the warm node and the pin on promotion. Nothing applies app payloads on a replica yet.

## Decision

An app's change-log payload carries warm_node_id, warm_node_pinned and warm_interval_s beside the App fields, and App.warm stays null in it.

Options. A: the payload's App.warm carries the stored settings (node_id, node_pinned, interval_s), with the progress fields null and state as derived without reports, and a replica reads the settings from there. No schema change, but a payload state that is not the API's. B: the contract says an app's change-log payload adds the three columns under their own names, beside the App fields (like other payloads that carry stored-only columns, if any). C: leave it to the replica-apply work, which reads apps rows by other means. The warm policy's first code does what main does for every derived field: App.warm is null in the payload. The maintainer, 2026-11-09, chose option B: an app's change-log payload carries warm_node_id, warm_node_pinned and warm_interval_s under their own names, beside the App fields. App.warm stays null in the payload, and the progress fields, which live in memory, stay out of the hash chain. A promoted primary that rebuilds apps from the change log keeps the warm node and the pin that a failover needs.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0286 in the project decision ledger. Do not edit by hand; changes go through a new decision.

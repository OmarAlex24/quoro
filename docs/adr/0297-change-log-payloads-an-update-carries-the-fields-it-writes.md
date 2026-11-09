# ADR 0297: Change-log payloads: an update carries the fields it writes and the rebuild merges them over the row; a delete's payload is ignored

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0297
- **Tags:** m5b, contracts, changelog, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0229](0229-welcome-carries-the-primary-s-schema-version-promotion.md), [ADR 0286](0286-an-app-s-change-log-payload-carries-warm-node-id-warm-node.md)

## Context

The contract says an update's payload is the full new row and a delete's is {}, and the promotion rebuild writes the payload's row, a missing column taking its default (ADR 0229). Main writes partial update payloads, such as an app's {id, status, updated_at} from deploy, status and restore, and a grant's {id, status} or {id, uses, status}. Its delete payloads are {} for a VM, the whole object for an app or a domain, {id} for a user and {node_id} for a tunnel. A rebuild that follows the contract literally would reset or refuse rows on every partial entry. The log is hash-chained, so entries already written stay partial whatever the writers do from now on. Found while checking ADR 0286 (warm columns in whole-App payloads only).

## Decision

Change-log payloads: an update carries the fields it writes and the rebuild merges them over the row; a delete's payload is ignored.

Gap between node-agent-protocol.md (the log_entries payload; promote step 4), the db-schema.sql header and main. Nothing applies payloads yet: the node stores entries verbatim (#668), and the first apply code is the promotion rebuild.

Option A, which the maintainer chose: the contract states merge semantics. A create carries the whole row. An update carries the entity's key and every field it writes, and the rebuild merges those over the current row: a field absent from the payload keeps its value, and a column's default applies only when a create lacks it (ADR 0229's rule, narrowed). A delete removes the row found by entity_id; its payload is ignored, whether it is {} or the deleted object. A payload field with no column (a derived field such as App.warm, or a column a later migration dropped) is ignored. ADR 0286 then reads as it is implemented: whole-App update payloads carry warm_node_id, warm_node_pinned and warm_interval_s; a partial app payload omits them and the rebuild keeps the current values. Cost: contract text only; no writer changes; the promotion rebuild implements merge.

Option B: writers change so every update carries the whole row and every delete {}. Cost: dozens of writers across the control plane, and the rebuild must still merge for the partial entries already in existing logs, so B is A plus work.

Option C: leave the contract as it is and let the rebuild's pull request decide. Cost: the contract stays wrong, and the first literal implementation resets rows.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0229](0229-welcome-carries-the-primary-s-schema-version-promotion.md): which payloads take a column's default: only a create that lacks the column; an update carries the fields it writes and is merged over the row, and a delete's payload is ignored. The rest of [ADR 0229](0229-welcome-carries-the-primary-s-schema-version-promotion.md) stands.
- Refines [ADR 0286](0286-an-app-s-change-log-payload-carries-warm-node-id-warm-node.md): what a partial app payload does: it omits the warm columns, and the rebuild keeps their values. The rest of [ADR 0286](0286-an-app-s-change-log-payload-carries-warm-node-id-warm-node.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0297 in the project decision ledger. Do not edit by hand; changes go through a new decision.

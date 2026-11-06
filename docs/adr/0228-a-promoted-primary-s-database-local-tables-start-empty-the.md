# ADR 0228: A promoted primary's database: local tables start empty, the webhook cursor starts at the newest event, and the off-site export resumes as after a restore

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0228
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0179 replays the log into a fresh database but does not say how the state outside the log is set: meta, the webhook dispatcher's cursor and the audit export's progress.

## Decision

A promoted primary's database: local tables start empty, the webhook cursor starts at the newest event, and the off-site export resumes as after a restore.

Every followed entry is applied in seq order to a schema of the promoting binary; the change-log and audit chains are verified and the promotion stops on a break. Local tables start empty: sessions, access tokens, webhook delivery attempts, interactive sessions and the vulnerability mirror. meta: cluster_id from the agent's identity, term t + 1, schema_version of the binary. webhook_cursor is the seq of the newest event in the rebuilt log, so an event the old primary never delivered is lost rather than delivered twice: a second incident.opened to a webhook with wake would issue a second grant. audit_exports is replicated, so the export resumes from its row with the rules of audit-log-format 8.5. Rejected: starting the cursor at the old primary's last known position, which redelivers and can double a wake grant.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0228 in the project decision ledger. Do not edit by hand; changes go through a new decision.

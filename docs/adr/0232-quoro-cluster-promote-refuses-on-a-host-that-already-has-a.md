# ADR 0232: quoro cluster promote refuses on a host that already has a control plane, prepares the host as the installer does for a primary, and has a --dry-run

- **Status:** Accepted; refined by [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0232
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)

## Context

A machine installed with the node role has no control-plane user, configuration or unit, and ADR 0182 only says promote starts quoro.service. Comparing candidates before acting needs a mode that changes nothing.

## Decision

quoro cluster promote refuses on a host that already has a control plane, prepares the host as the installer does for a primary, and has a --dry-run.

Promote refuses before any change when this host has a control-plane database or unit, when the agent has no identity or its log is empty or quarantined, when the stored schema version is too new, or when a seal of any key in use cannot be opened. After the vote and the database rebuild, it writes the keys, creates what the installer creates for a primary (the control plane's system user, data and configuration directories, unit), starts the control plane and points the host's agent at it as quoro node set-primary does. --dry-run stops after the log position and the vote pre-check, changing nothing. It moves cp_status and writes cluster.promoted (system, actor cluster, channel cli, params from_node_id, to_node_id, term, log_seq, ack_fence) in one change-log write, not through the restore path's primary-node file.

## Consequences

- [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) refines the candidate builds from the log it froze, takes its term from the vote answers, and gains --accept-loss. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0232 in the project decision ledger. Do not edit by hand; changes go through a new decision.

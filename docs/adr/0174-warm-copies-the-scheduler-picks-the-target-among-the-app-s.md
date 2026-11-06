# ADR 0174: Warm copies: the scheduler picks the target among the app's placement nodes (or the operator pins it); the target prepares the image and keeps a standby VM that never starts without the lease; warm apps' domains are served through the edge when one exists

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0174
- **Tags:** m5b, control-plane, ingress, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0020: warm = data replicated, image pulled, instance stopped. A node cut off from the other machines can keep its internet, so its Cloudflare tunnel stays up and keeps receiving a one-replica domain; moving the CNAME then has the 70-115 s tail measured in #344. Through the edge, failover and switchover are a backend switch.

## Decision

Warm copies: the scheduler picks the target among the app's placement nodes (or the operator pins it); the target prepares the image and keeps a standby VM that never starts without the lease; warm apps' domains are served through the edge when one exists.

Contract: ha_policy warm accepted for apps with a volume and at least two eligible nodes (validation_failed otherwise); App gains warm {node_id, interval_s, state (syncing|in_sync|lagging|failed), last_snapshot_seq, last_shipped_at, lag_s}; HaPolicyCost for warm: storage_bytes = the target's volume allocated bytes, reserved = disk only (memory and vCPU only after a failover), restore_estimate_s = boot time. spec.vms on the target carries the VM with desired_status stopped and standby: true; the node prepares the image and the replica volume and starts nothing without a held lease with the matching token (6.4 unchanged). Routes: a warm app's domains go to the edge (spec.edge.routes with the active node's backend) when the cluster has an edge; without one the tunnel route moves with the D-0168 grace window. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0174 in the project decision ledger. Do not edit by hand; changes go through a new decision.

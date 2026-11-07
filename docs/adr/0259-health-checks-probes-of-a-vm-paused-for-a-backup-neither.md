# ADR 0259: Health checks: probes of a VM paused for a backup neither count nor restart it, and a health restart stops a VM only when it can start again

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0259
- **Tags:** m5b, node-agent, health, backups, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Decided on 2026-11-06 for #511 (#78). On a node without reflink a backup pauses the VM for the whole copy (62-98 s for 5 GiB, ADR 0156), the engine reports it running, and the default health check would kill it mid-backup. Section 6.3 (stop and start on failed health) and section 8.4 (an uncertain lease keeps its VMs running) are reconciled by stopping for a health restart only when the VM may start again.

## Decision

Health checks: probes of a VM paused for a backup neither count nor restart it, and a health restart stops a VM only when it can start again.

(1) The backup manager marks each VM it pauses; while marked, failed probes do not count toward failures and the reconciler does not restart the VM for health; when the VM is resumed the failure count starts from zero. (2) A health restart stops the VM only when it may start again (lease held and, under an expiring lease, the start margin of ADR 0242); if the lease leaves held between the stop and the start, the VM stays stopped as section 6.3 says. Contract text: one paragraph in node-agent-protocol section 6.3; #511 implements both parts, (1) in crates/node-agent/src/backup/.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0259 in the project decision ledger. Do not edit by hand; changes go through a new decision.

# ADR 0074: A failed forced stop escalates: SIGKILL the VMM process; if the VM may still run, the node never emits node.fenced, emits node.fence_failed, marks itself failed and keeps the lease unstartable

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0074
- **Tags:** reliability, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. node.fenced must mean the VM is not running; anything else could let a second copy start elsewhere (D-0019).

## Decision

A failed forced stop escalates: SIGKILL the VMM process; if the VM may still run, the node never emits node.fenced, emits node.fence_failed, marks itself failed and keeps the lease unstartable.

The primary treats fence_failed like node loss: no re-grant elsewhere until an operator acknowledges the fence. Contract update in the pre-freeze contract PR.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0074 in the project decision ledger. Do not edit by hand; changes go through a new decision.

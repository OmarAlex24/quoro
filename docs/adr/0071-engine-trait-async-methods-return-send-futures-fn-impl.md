# ADR 0071: Engine trait async methods return Send futures (fn ... -> impl Future<Output = ...> + Send), via a standalone contract PR before the M0 freeze

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0071
- **Tags:** engine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. The control plane and node agent must spawn engine calls on tokio from generic code.

## Decision

Engine trait async methods return Send futures (fn ... -> impl Future<Output = ...> + Send), via a standalone contract PR before the M0 freeze.

Also adopt as additive contract items the derives and impls M0 added (Clone/Debug/PartialEq/Eq on VmSpec and friends, MacAddr([u8;6]), ConsoleIo blanket impl, Display/Error for EngineError).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0071 in the project decision ledger. Do not edit by hand; changes go through a new decision.

# ADR 0293: getSetup: a public GET /api/v1/setup answers {required: bool}, whether first-run setup is still open

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0293
- **Tags:** contracts, panel, setup
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The panel offers the setup link and the owner form after the owner exists, until a submit answers conflict (#640): nothing public says whether setup is done, getHealth reveals no state by design and GET /api/v1/setup answers 405. Found in the panel redesign's demo on a real cluster; the maintainer asked for the proposal on 2026-11-09 (#673).

## Decision

getSetup: a public GET /api/v1/setup answers {required: bool}, whether first-run setup is still open.

Add GET /api/v1/setup (operationId getSetup, security: [], scope public): SetupState {required: boolean}, true while the users table is empty, false afterwards. It reveals that one bit and nothing else; a setup still needs the setup token printed on the host, so knowing a cluster has no owner yet does not let anyone claim it. POST /api/v1/setup, its token and its conflict answer do not change. getHealth stays free of state. No MCP tool (first-run setup is human-only) and no CLI command. The threat model notes the new public bit. Panel: the sign-in page hides its setup link when required is false, and /setup shows that the cluster is already set up without a submit. Standalone contract PR after the ADR, with the control-plane handler in the same PR because openapi_diff compares the implemented operations with the frozen file; then the panel PR. Issue #673.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0293 in the project decision ledger. Do not edit by hand; changes go through a new decision.

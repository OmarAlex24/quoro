# ADR 0114: M4 terminal and console access: app:terminal human-only (owner, operator); vm:console grantable read-only (owner, operator); agents get a non-interactive app:exec later via approval

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0114
- **Tags:** m4, terminal, console, security, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A PTY shell reads every app secret and acts outside single audited API calls; console output is kernel and init diagnostics, closer to logs, and helps agents diagnose failed boots (D-0032).

## Decision

M4 terminal and console access: app:terminal human-only (owner, operator); vm:console grantable read-only (owner, operator); agents get a non-interactive app:exec later via approval.

New scopes in permission-model.md: app:terminal (human-only flag in M4, owner+operator bundles, not viewer) and vm:console (grantable, owner+operator; viewer unchanged in M4). Open flow: POST /api/v1/apps/{app_id}/terminal-sessions or POST /api/v1/vms/{vm_id}/console-sessions -> 201 {id trm_, single-use 30 s ticket, websocket_path}; GET /api/v1/interactive-sessions/{id}/ws?ticket= upgrade with Origin check; DELETE to end. Long-lived sessions are re-checked: close on identity disabled, role change, cookie/token expiry, grant revoke, 15 min idle, 8 h max. A later app:exec (one argv, captured output, approval grant) is a separate decision. Evidence: docs/research/m4-terminal-and-console.md (PR #155, issue #151).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0114 in the project decision ledger. Do not edit by hand; changes go through a new decision.

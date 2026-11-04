# ADR 0126: Terminal exec channel: vsock port 1024, framed [u8 type][u32 BE len][payload], quoro-init config v2 with exec_port

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0126
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Fixes the engine-init wire format before the terminal epic

## Decision

Terminal exec channel: vsock port 1024, framed [u8 type][u32 BE len][payload], quoro-init config v2 with exec_port.

Host types 1 open(JSON argv,user,rows,cols) 2 input 3 resize 4 close; guest 129 output 130 exit 131 error; 64 KiB frame cap; v1 configs still accepted without exec listener. Default command /bin/sh, TERM=xterm-256color. createTerminalSession takes no justification header (human-only); createConsoleSession and endInteractiveSession do; endInteractiveSession allowed for the opener and owners.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0126 in the project decision ledger. Do not edit by hand; changes go through a new decision.

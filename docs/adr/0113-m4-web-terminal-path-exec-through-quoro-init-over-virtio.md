# ADR 0113: M4 web terminal path: exec through quoro-init over virtio-vsock, relayed by the root node agent

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0113
- **Tags:** m4, terminal, engine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Runs inside the app's guest (T-LOG-06), multi-session, resize, exit codes, vsock back-pressure; no new privilege in the unprivileged API (D-0089/D-0096). Measured on the test lab: uname -a 21 ms RTT, 56 MB/s, VMM RSS flat under a 10 s reader stall.

## Decision

M4 web terminal path: exec through quoro-init over virtio-vsock, relayed by the root node agent.

Every VM gets `--vsock cid=3,socket=<vm dir>/vsock.sock` (hybrid vsock, no vhost). quoro-init listens on one fixed port after switch_root, spawns a PTY session per connection with the app's env/workdir/user via a self re-exec helper (pre_exec is unsafe; workspace forbids unsafe), using the vsock crate (0.5.5, Apache-2.0) since rustix 1.1.6 has no safe vsock address. quoro-init-config v2. Node never creates `<socket>_<port>` listeners, so the guest cannot dial the host. Rejected: shell on node, mounting disks, SSH, serial getty, network agent. Images without a shell: only programs the image ships; a toolbox disk (BusyBox, GPL-2.0) is after M4. Evidence: docs/research/m4-terminal-and-console.md (PR #155, issue #151).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0113 in the project decision ledger. Do not edit by hand; changes go through a new decision.

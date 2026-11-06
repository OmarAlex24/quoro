# ADR 0204: quoro cluster maintenance talks to the running primary over a local control socket and first takes majority support for W

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0204
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0171 names the command and W but not how a root CLI reaches the running primary, the bounds, the success condition, how the window ends, or what the installer does when it fails.

## Decision

quoro cluster maintenance talks to the running primary over a local control socket and first takes majority support for W.

quoro cluster maintenance [--duration `<s>`] [--end] runs as root on the primary's host and reaches the running primary over /run/quoro/control.sock (root and the service user only; nothing on the network). W default 180 s, at least D, at most 600 s. The primary first asks its voters for support with max_lease_ms = W; with a majority it raises leases.duration_ms to W and answers every renewal with W while the window is open. Success once every expiring lease has had a renewal answered with W (prints until when they are covered); failure when no majority supports W or a holder did not renew within D/3 plus a margin. The window closes at the primary's next start, with --end, or W after it opened. Audit: system record cluster.maintenance (actor cluster, channel cli, params {duration_s}; --end writes {end: true}). The installer runs it before restarting quoro.service and continues the upgrade when it fails, warning that warm apps fence if the restart takes longer than about D (fencing is the safe outcome).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0204 in the project decision ledger. Do not edit by hand; changes go through a new decision.

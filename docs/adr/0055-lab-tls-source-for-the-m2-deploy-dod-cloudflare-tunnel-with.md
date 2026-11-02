# ADR 0055: Lab TLS source for the M2 deploy DoD: Cloudflare Tunnel with a test domain; ACME exercised separately via DNS-01 against a test zone

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0055
- **Tags:** ingress, lab, roadmap
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The lab VMs have no public IP, so ACME HTTP-01 cannot run there; the roadmap left the choice open.

## Decision

Lab TLS source for the M2 deploy DoD: Cloudflare Tunnel with a test domain; ACME exercised separately via DNS-01 against a test zone.

Options: (a) Cloudflare Tunnel with a test domain: matches D-0024's homelab-behind-NAT case; needs a tunnel token (stored as a provider credential, D-0051). (b) ACME DNS-01 against a test zone: exercises the ACME path from the lab; needs a DNS provider token and DNS-01 support in M2. (c) a lab-only internal CA: exercises neither real path. Accepted: (a) for the M2 DoD; (b) as a separate ingress task before M2 closes; never (c). Needs a test domain the maintainer controls.

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0055 in the project decision ledger. Do not edit by hand; changes go through a new decision.

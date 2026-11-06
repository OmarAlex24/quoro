# ADR 0139: Nodes follow a primary whose address changes: advertised address list, pinned rediscovery on the node's subnet, and quoro node set-primary

- **Status:** Accepted; refined by [ADR 0235](0235-hello-carries-control-plane-urls-and-welcome-primary-urls.md)
- **Date:** 2026-11-04
- **Ledger id:** D-0139
- **Tags:** networking, node-agent, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0235](0235-hello-carries-control-plane-urls-and-welcome-primary-urls.md)

## Context

#157: a node dials only the primary_url stored at enrollment. On the test lab, the primary gets a new DHCP address after a reboot and the router cannot reserve it, so its nodes lose their session until someone edits each identity file by hand. The TLS pin (PinnedCertVerifier checks only the SHA-256 of the leaf, never the host name) already makes any address safe to try: a host that does not hold the primary's key cannot complete the handshake.

## Decision

Nodes follow a primary whose address changes: advertised address list, pinned rediscovery on the node's subnet, and quoro node set-primary.

Design (three layers, each usable alone; no implementation without a decision).
(1) Manual, always available: 'quoro node set-primary `<url>`' on the node. It performs a TLS handshake to `<url>` with the pinned fingerprint, refuses if the pin does not match, then rewrites primary_url in the identity file atomically (same write path as signing_key_rotate) and restarts the session loop. The pin and the node credential never change. Small, no protocol change.
(2) Advertised addresses: welcome gains an optional additive field primary_urls: [string] (node-agent-protocol 5.10; a protocol contract PR first). The primary fills it with public_url when configured, plus https://`<addr>`:`<port>` for each non-loopback, non-link-local address of its host, plus https://`<hostname>`.local:`<port>` when the host name resolves locally. The node persists the list next to primary_url (identity file, bounded to 8 entries, https only) and, after a dial to primary_url fails, tries the others in order with the same pin; the first that completes hello becomes primary_url. Helps multi-homed primaries, a primary whose public_url is a DNS name, and planned moves (the node learns the new address while still connected). It does NOT help an unplanned DHCP change on reboot, because the primary does not know its next address before it changes.
(3) Pinned rediscovery, opt-in per node (node config primary_discovery = 'subnet', default off): after 5 minutes without a session and after the advertised list failed, the node probes TCP `<port>` on the addresses of its own directly attached IPv4 subnets (only /24 or smaller, at most 4 probes per second, one pass every 10 minutes), and for each open port completes a TLS handshake with the pinned verifier. The only host that can pass is the one holding the primary's private key, so a scan cannot be steered to an impostor; it can at worst cost probes. A match is persisted as primary_url and logged at info with old and new address; the primary records node.connected as usual. Rejected alternatives: trusting the host name or a CA (weaker than the pin), the primary pushing its address before the change (it does not know it), a cloud rendezvous (adds an external dependency to a self-hosted core), mDNS only (needs avahi on every host and fails across subnets).
Recommendation: (1) and (2) for v0.2 as one protocol contract PR plus one node-agent PR; (3) behind the opt-in flag in the same release if the maintainer wants homelab DHCP covered without router access; document at install time that the primary should have a fixed address or a DNS name (installer README).

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- [ADR 0235](0235-hello-carries-control-plane-urls-and-welcome-primary-urls.md) refines the cap of welcome.primary_urls (16 entries instead of 8). The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0139 in the project decision ledger. Do not edit by hand; changes go through a new decision.

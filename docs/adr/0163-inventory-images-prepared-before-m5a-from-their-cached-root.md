# ADR 0163: Inventory images prepared before M5a from their cached root disk, once, at node start

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0163
- **Tags:** m5a, vulns, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

engine-trait.md 3.21 states that a pre-M5a image is NotFound until prepared again; 3.4/3.17 make a cached digest return without network or layout; the cache keeps only rootfs.ext4 + image.json, no unpacked tree. A same-digest redeploy also hits the cache, so it does NOT re-inventory either: those apps stay pending until a new digest.

## Decision

Inventory images prepared before M5a from their cached root disk, once, at node start.

The maintainer chose option A on 2026-11-05. Options considered. (A, chosen) Engine inventories a cached image that has no inventory.json from its rootfs.ext4, read-only, once, in the background at node start; the node then sends image_inventory as for a freshly prepared image. Covers pulled and built images, no network. Contract: engine-trait 3.21 (and 6.11 'same pass' wording), node-agent-protocol 6.11 'Sending'. Cost: an ext4 reader in user space (license check, ADR 0080) or a read-only nodev/nosuid/noexec loop mount as root; the parsers already take a tree path. (B) Re-pull pulled images by digest into scratch to inventory without touching the cached rootfs: changes 3.4, needs network, cannot cover built images (layout deleted after prepare_layout). (C) Honest status only: node marks such images as not inventoriable and the API reports e.g. not_inventoried 'prepared_before_m5a' instead of pending; openapi + protocol 5.3 change; fixes the message, not the gap. (D) No change: evict and re-prepare by deploying a new digest; operational only, not possible for running apps' current digest. Also: the primary skips images whose status_report entry has no inventory_sha256 ('still building'), so there is no resend loop today.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0163 in the project decision ledger. Do not edit by hand; changes go through a new decision.

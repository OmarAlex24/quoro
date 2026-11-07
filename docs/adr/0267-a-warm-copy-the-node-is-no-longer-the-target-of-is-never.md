# ADR 0267: A warm copy the node is no longer the target of is never reported, so the primary cannot always delete it

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0267
- **Tags:** m5b, warm, contracts, volumes
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Section 5.3 reports only the warm entries the spec names; the primary keeps warm progress in memory only; after a primary restart, a disconnected node or one failed delete, an old warm copy (a full copy of the app's data) stays on the node with no way to remove it

## Decision

A warm copy the node is no longer the target of is never reported, so the primary cannot always delete it.

Gap. A node that is no longer the warm node of an app may still hold that app's warm copy: the app left `warm`, the warm node was picked again, or the app was deleted. The copy holds the app's data and fills the disk. Nothing in the contract makes the primary remove it reliably:
- Section 5.3 says `status_report.warm` has "one entry per volume of `spec.volumes` that carries `warm`". The node agent follows this: it reports only the `present` target entries of its spec. The moment a snapshot stops naming the copy, or names it `deleted`, the copy is no longer reported.
- Nothing about a warm copy is stored on the primary (migration 0011). So the primary learns that a node holds a copy only from that node's newest report, which it keeps in memory.
- The primary can send `deleted` only while its memory says the node reported the copy. Three cases leave the copy behind for good:
  (1) The primary restarts between the change and the next snapshot.
  (2) The node is disconnected when the change happens, and the primary restarts before the node comes back.
  (3) The node's delete fails once. The node already stopped reporting the copy when the `deleted` entry arrived, so the primary drops the entry and never sends it again.
- No API lists or removes such a copy. It stays on the node's disk until someone with root removes it.

What the primary does today (#532), within the contract: while the primary's memory holds a node's report of a copy that the node is no longer the target of, that node's snapshot carries a `deleted` entry for the copy. That covers the change while the primary runs and the node reports. It does not cover cases (1) to (3).

Options:
A (recommended). Section 5.3: the target also reports, with `role: target`, every warm copy it still holds that `spec.volumes` does not name as a `present` target entry, until the copy is removed. Section 5.11: the primary sends a `deleted` entry for each copy reported that way. The entry stays until the copy is no longer reported. No new field and no schema change. The node keeps reporting a copy whose delete failed, so the primary keeps asking. After a primary restart, the first report brings the copy back into view.
B. The primary stores the former warm nodes of each volume, in a new table or column of migration 0011, and sends `deleted` until the node confirms. This needs a confirmation, such as a `deleted` warm entry, which the contract lacks, and it adds change-log writes. It is heavier than A for the same result.
C. Leave it. The copies leak, and the docs say how to remove one by hand. Not recommended: the copy is a full second copy of the app's data.

The node-side part of A is in the warm receiver's `publish`: it reports the rows of its store that are not removed and that the spec does not name as targets. The primary side is the `deleted` entry it already sends.

Option A was chosen.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0267 in the project decision ledger. Do not edit by hand; changes go through a new decision.

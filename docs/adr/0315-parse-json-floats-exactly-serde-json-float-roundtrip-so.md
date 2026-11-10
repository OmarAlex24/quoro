# ADR 0315: Parse JSON floats exactly (serde_json float_roundtrip) so audit and change-log hashes re-hash from their stored text

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0315
- **Tags:** contracts, audit, changelog, m5b
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0302](0302-change-log-append-refuses-an-entry-a-follower-could-not.md), [ADR 0306](0306-an-audited-api-call-is-always-audited-bound-what-reaches.md)

## Context

The audit hash covers the params as the primary parsed them (V0) and stores their RFC 8785 text jcs(V0). serde_json without float_roundtrip parses floats best-effort, so for many floats that text reads back as another number: verifyAudit then reports hash_mismatch at that seq for good. This is on main today, not caused by #731: one authenticated call carrying e.g. 1548.2162860866162 in its body leaves a record that never verifies. The change log since #723 hashes its entries over the payload as read back by that same best-effort parser, so it verifies only under this exact parser and breaks for a reader that parses floats exactly. A probe (container, 1.5M+ values over the 17-digit, shortest-form, uniform and ±(1.8e19,1e21) bands) found, without float_roundtrip, up to 434k of 1M values whose jcs text reads back as another number and up to 252k whose frame a best-effort node would refuse; with float_roundtrip every such count is 0, Cargo.lock is unchanged and cargo-deny 0.20.2 passes. A node that cannot verify a batch answers bad_message (non-fatal) and stays at its last ack while the primary resends every 3 s, so follow stalls rather than diverging.

## Decision

Parse JSON floats exactly (serde_json float_roundtrip) so audit and change-log hashes re-hash from their stored text.

Options: (A) enable serde_json float_roundtrip in quoro-proto, where jcs lives; feature unification applies it to every crate of the binary. Then V1 = parse(jcs(V0)) = V0 for every float, so audit records re-hash from their stored and exported text with any exact parser (section 5 as written), and the change log's read-back form equals the payload's. The maintainer chose option (A) (2026-11-10). Costs: (1) audit-log-format.md section 4.1's third worked case (18983552425072.563) and #731's float test no longer describe a digest case, and #723's test changelog::tests::a_float_a_node_parses_back_as_another_number_is_refused trips its premise guard: a contract PR re-words the worked case and the code PR replaces those tests; (2) during a rolling upgrade a new primary parses exactly while older nodes parse best-effort, so ADR 0302's check, run with the primary's parser, cannot see that an older node would refuse a float entry; such a node stalls at that entry (bad_message, resent every 3 s) until it is upgraded. A contract sentence should say nodes must run the same parser as the primary, and the release notes say to upgrade nodes promptly; (3) records written before the change with an unstable float stay unverifiable (append-only); verifyAudit names their seq. (B) keep best-effort parsing and add a new section 4.1 condition: keep a digest whenever jcs(V0) does not read back to V0; the same rule would be needed for change-log payloads; the chain then verifies only under the best-effort parser, contrary to section 5. (C) hash the record over V1 = parse(jcs(V0)) (built and withdrawn in #731's review): breaks section 5 for an exact parser and is irreversible in the chain. #731 ships without any of these, the float limit stated as pre-existing.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0302](0302-change-log-append-refuses-an-entry-a-follower-could-not.md): the parser the primary checks entries with: floats are parsed exactly, and nodes run the same parser. The rest of [ADR 0302](0302-change-log-append-refuses-an-entry-a-follower-could-not.md) stands.
- Refines [ADR 0306](0306-an-audited-api-call-is-always-audited-bound-what-reaches.md): which floats keep parameters as a digest: once floats are parsed exactly, no float reads back on a node as another number. The rest of [ADR 0306](0306-an-audited-api-call-is-always-audited-bound-what-reaches.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0315 in the project decision ledger. Do not edit by hand; changes go through a new decision.

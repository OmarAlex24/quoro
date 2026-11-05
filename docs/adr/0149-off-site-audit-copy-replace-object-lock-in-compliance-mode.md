# ADR 0149: Off-site audit copy: replace 'object lock in compliance mode' with a per-provider lock strength the probe reports (compliance on S3/B2, bucket_rule on R2, none); every put uses If-None-Match: *; verification reads by version where versioned

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0149
- **Tags:** m5a, audit, contracts, providers
- **Supersedes:** [ADR 0053](0053-off-site-audit-log-copy-d-0031-ships-in-m5a-with-the-s3.md)
- **Superseded by:** none
- **Related:** none

## Context

Research task D-0053 required before M5a (F-0238..F-0240): R2 has no S3 object lock and no compliance mode, only removable bucket rules that refuse with 409; S3/B2 compliance mode still lets a plain PUT add a version and a plain DELETE add a delete marker. The DoD line only holds literally per provider.

## Decision

Off-site audit copy: replace 'object lock in compliance mode' with a per-provider lock strength the probe reports (compliance on S3/B2, bucket_rule on R2, none); every put uses If-None-Match: *; verification reads by version where versioned.

Amends D-0053 and audit-log-format.md 8.2: audit_exports.object_lock becomes lock_strength (none | bucket_rule | compliance); the probe writes, tries overwrite and delete with the exporter credential, reads back; on R2 Quoro can add the bucket rule through the Cloudflare account credential of a provider entry or the operator does; the panel states R2's rule is removable by an account admin. DoD wording: 'an overwrite or delete of the exported segment is refused (R2: 409) or leaves the locked version intact (S3/B2), and the panel shows the strength'. Lab demo on R2 (the maintainer's Cloudflare account), so the demo shows bucket_rule. Recommendation: accept.

## Consequences

- Replaces ADR 0053.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0149 in the project decision ledger. Do not edit by hand; changes go through a new decision.

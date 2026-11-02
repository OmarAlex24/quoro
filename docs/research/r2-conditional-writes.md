# R2 conditional writes as a lease arbiter

- **Question:** Q-0011 does Cloudflare R2 support reliable conditional writes (If-Match / If-None-Match) usable as a lease arbiter?
- **Researched:** 2026-11-02 by agent:ds-research-2
- **Status:** answered

## Short answer

Yes, on the R2 side the primitive is real: `PutObject` supports `If-None-Match: *` (create-once) and `If-Match: <etag>` (compare-and-swap), evaluated atomically at commit time, with `412 PreconditionFailed` for the losers and at most one winner. R2 is strongly consistent for reads, writes, deletes and list, and competing conditional writes to one key are serialized. Two caveats matter for a lease: R2 rate-limits concurrent writes to the same key to **1 per second** (beyond that: HTTP 429), and a failed conditional write is still billed as a Class A operation.

The bigger caveat is D-0025: the same primitive is *not* uniformly available on other S3-compatible stores. AWS S3 has had `If-None-Match` since 2024-08 and `If-Match` on PutObject/CompleteMultipartUpload since 2024-11; MinIO AIStor documents both; Backblaze B2 does **not** document conditional PUT support and third-party compatibility matrices say it lacks it. So "R2/S3 conditional writes" is a Cloudflare-and-AWS-class capability, not a portable S3 one.

Object storage also gives no server-side TTL: expiry is computed by clients from their own clocks, so the object must carry an owner id, an expiry timestamp and a **monotonically increasing fencing token**, and consumers must reject stale tokens (Kleppmann). Without that, a paused node can still act after its lease expired.

## Findings

### R2 S3-compatible conditional writes

- `PutObject` supports the standard S3 conditional headers `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`; `If-None-Match: *` is documented as the "create-once" form. **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/extensions/ (accessed 2026-11-02)
- Conditions "are evaluated atomically, as part of the same write that commits the object"; when several clients concurrently `PutObject` with `If-None-Match: *` on an absent key, "at most one request succeeds and every other request fails with a `412 PreconditionFailed` error code. Competing conditional writes can never both observe success." **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/extensions/ (accessed 2026-11-02)
- The S3 compatibility table lists `PutObject` conditional operations as ✅ If-Match, If-Modified-Since, If-None-Match, If-Unmodified-Since, and repeats the atomicity statement in a "Conditional operations" note under "Behaviors & Limitations". **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/api/ (accessed 2026-11-02)
- Failure code is documented as `412 PreconditionFailed`. No `409 Conflict` appears anywhere in R2's conditional-write documentation, unlike AWS S3 (see below). **Confirmed** for "412 is documented"; the absence of 409 is an absence in the docs, not a guarantee it can never occur (see open risks). Source: https://developers.cloudflare.com/r2/api/s3/extensions/ (accessed 2026-11-02)
- `CopyObject` supports conditionals on the **destination** via R2-specific headers `cf-copy-destination-if-match` / `cf-copy-destination-if-none-match` (plus `-if-modified-since` / `-if-unmodified-since`), returning `412 PreconditionFailed`. This feature is labelled **beta**. **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/extensions/ (accessed 2026-11-02)
- Those destination conditions are **not atomic with respect to** the `x-amz-copy-source-if-*` source conditions: the source is selected at one moment and the destination committed at another. **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/extensions/ (accessed 2026-11-02)
- `CompleteMultipartUpload` and `DeleteObject`/`DeleteObjects` list **no** conditional-operation features in R2's compatibility table (only unimplemented MFA / object-lock / request-payer / bucket-owner rows). So conditional complete-MPU and conditional delete are **not** available on R2. **Confirmed** (as documented feature lists). Source: https://developers.cloudflare.com/r2/api/s3/api/ (accessed 2026-11-02)
- `CopyObject` also supports source-side `x-amz-copy-source-if-match` / `-if-none-match`; `UploadPartCopy` does **not** (all four copy-source conditionals marked ❌). **Confirmed.** Source: https://developers.cloudflare.com/r2/api/s3/api/ (accessed 2026-11-02)

### Workers binding `onlyIf`

- `R2PutOptions.onlyIf` accepts an `R2Conditional` or a `Headers` object: "Specifies that the object should only be stored given satisfaction of certain conditions". **Confirmed.** Source: https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ (accessed 2026-11-02)
- On a failed precondition, `put()` "returns `null`, and the object will not be stored". On success it returns an `R2Object` whose `etag` can be used as the next `If-Match` value. **Confirmed.** Source: https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ (accessed 2026-11-02)
- `R2Conditional` fields: `etagMatches`, `etagDoesNotMatch`, `uploadedBefore`, `uploadedAfter`. Alternatively a `Headers` object with conditional headers is accepted ("All conditional headers aside from `If-Range` are supported"). **Confirmed.** Source: https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ (accessed 2026-11-02)
- Binding writes are strongly consistent: "Once the Promise resolves, all subsequent read operations will see this key value pair globally." Deletes and lists are likewise documented as strongly consistent. **Confirmed.** Source: https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ (accessed 2026-11-02)

### Consistency and concurrency

- R2 is strongly consistent for read-after-write, metadata update, delete ("reads ... immediately return a 'does not exist' error") and list ("will list all objects at that point in time"). IAM/permission changes are the exception (eventually consistent, up to a minute). **Confirmed.** Source: https://developers.cloudflare.com/r2/reference/consistency/ (accessed 2026-11-02)
- Without a conditional header, "in the event two clients are writing (`PUT` or `DELETE`) to the same key, the last writer to complete 'wins'". **Confirmed.** Source: https://developers.cloudflare.com/r2/reference/consistency/ (accessed 2026-11-02)
- With a conditional header the semantics change to single-winner: conditions are "always evaluated against the committed state of the bucket at the moment the write commits, never against an older snapshot", and "competing conditional writes to the same key are serialized". **Confirmed.** Source: https://developers.cloudflare.com/r2/reference/consistency/ (accessed 2026-11-02)
- **Negative-read trap:** a `HeadObject`/`GetObject` returning 404 "does not exist at the moment the read was processed" but "is **not** final with respect to an earlier `PutObject` whose outcome is still unknown" — an in-flight write can still commit afterwards. The documented remedy is to retry with `If-None-Match: *`, which fails with 412 if the ambiguous write did commit. **Confirmed.** Source: https://developers.cloudflare.com/r2/reference/consistency/ (accessed 2026-11-02)
- **Per-key write rate limit:** "Maximum concurrent writes to the same object name (key): 1 per second", and "Concurrent writes to the same object name (key) at a higher rate return HTTP 429 (rate limited) responses." The 50/s bucket-management limit explicitly does not apply to object reads/writes. **Confirmed.** Source: https://developers.cloudflare.com/r2/platform/limits/ (accessed 2026-11-02)
- Bucket management operations are limited to 50/s per bucket; the Cloudflare REST API to 1,200 requests per five minutes (use S3 or Workers API for object operations). **Confirmed.** Source: https://developers.cloudflare.com/r2/platform/limits/ (accessed 2026-11-02)

### Other S3-compatible providers

| Provider | `If-None-Match: *` on PutObject | `If-Match: <etag>` on PutObject | Orientation |
| --- | --- | --- | --- |
| Cloudflare R2 | Yes (S3 API and Workers `onlyIf`) | Yes | **Confirmed** — https://developers.cloudflare.com/r2/api/s3/extensions/ |
| AWS S3 | Yes, since 2024-08-20 | Yes, since 2024-11-25 | **Confirmed** — https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/ , https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/ |
| MinIO AIStor | Yes | Yes | **Confirmed** for AIStor — https://docs.min.io/aistor/developers/s3-api-compatibility/ |
| MinIO (community server) | Disputed | Yes per AIStor docs, not verified for community build | **Hypothesis** — a 2024 discussion reports `IfNoneMatch=*` not preventing a second write on community MinIO — https://github.com/minio/minio/discussions/20318 |
| Backblaze B2 | Not documented / not supported | Not documented / not supported | **Hypothesis** — B2's own S3 `Put Object` header reference lists no conditional headers (https://www.backblaze.com/apidocs/s3-put-object, page dated 2023-06-01), and a third-party provider compatibility page states B2 "don't support the If-Match and If-None-Match HTTP headers" (https://docs.nebius.com/object-storage/transfer/requirements) |

- AWS documents conditional writes for `PutObject`, `CompleteMultipartUpload` and `CopyObject` (both header types), and **conditional deletes** for `DeleteObject`/`DeleteObjects`. **Confirmed.** Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html (accessed 2026-11-02)
- AWS failure semantics are richer than R2's: `If-None-Match` mismatch → `412 Precondition Failed`; `If-Match` mismatch → `412`; but a concurrent delete can also produce `409 Conflict` (If-None-Match) or `404 Not Found` (If-Match); "the first write operation to finish succeeds. Amazon S3 then fails subsequent writes with a `412`". **Confirmed.** Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html (accessed 2026-11-02)
- AWS conditional writes require Signature Version 4. **Confirmed.** Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html (accessed 2026-11-02)
- AWS conditional writes cost nothing extra, and failed conditional requests are still charged at normal request rates. **Confirmed.** Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html (accessed 2026-11-02)

### Lease semantics on object storage

- Object storage has no server-side lease/TTL: nothing expires a key or revokes an owner. Expiry is a timestamp the client writes and other clients interpret, so safety depends on the *clients'* clocks and on their willingness to stop when the timestamp passes. **Hypothesis** (inference from the documented API surface: R2 documents no TTL, expiry or lock primitive anywhere in its S3 extensions, consistency or Workers API references). Source: https://developers.cloudflare.com/r2/api/s3/extensions/ (accessed 2026-11-02)
- A lease alone is not safe for correctness: a client can be paused (GC, page fault, `SIGSTOP`, scheduler) or its packet delayed past expiry, and then act while another client holds the lease. The fix is a **fencing token** — a number the lock service increments on every acquisition — which the *protected resource* checks and rejects if it has already seen a higher value. **Confirmed** (as published argument). Source: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html (accessed 2026-11-02)
- Kleppmann's argument is explicitly about locks whose failure corrupts state (the Quoro case per D-0019), and he notes that a lock acquisition is essentially compare-and-set and that safety must not rest on timing assumptions. **Confirmed** (as published argument). Source: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html (accessed 2026-11-02)
- Mapping that onto R2: the lease object must hold `{owner, expiry, token}`; acquisition is one `PutObject` with `If-Match` on the current etag (or `If-None-Match: *` to create), the token is incremented inside that same write, and every stateful action on a node carries the token so the node-agent can refuse to start apps under a stale token (D-0019 self-fencing). **Hypothesis** (design inference; the pieces are each Confirmed, the composition is not documented as a lease recipe by Cloudflare). Source: https://developers.cloudflare.com/r2/reference/consistency/ (accessed 2026-11-02)

### Cost

Published prices (Standard storage, Workers plan independent):

| Item | Price | Free tier |
| --- | --- | --- |
| Storage | $0.015 / GB-month | 10 GB-month/month |
| Class A (PutObject, CopyObject, ListObjects, CreateMultipartUpload, …) | $4.50 / million requests | 1 million requests/month |
| Class B (HeadObject, GetObject, …) | $0.36 / million requests | 10 million requests/month |
| DeleteObject / DeleteBucket / AbortMultipartUpload | free | — |
| Egress | free | — |

- **Confirmed.** Source: https://developers.cloudflare.com/r2/pricing/ (accessed 2026-11-02)
- A `PutObject` that returns 412 because of a conditional header "counts as a Class A operation"; auth failures (401/403 auth) are not charged. **Confirmed.** Source: https://developers.cloudflare.com/r2/pricing/ (accessed 2026-11-02)
- Billable usage is rounded **up** to the next billing unit (1,000,001 operations are billed as 2 million). **Confirmed.** Source: https://developers.cloudflare.com/r2/pricing/ (accessed 2026-11-02)

Arithmetic for a lease renewed every 5 s (30-day month):

- One renewing node: 86,400 / 5 = 17,280 writes/day → × 30 = **518,400 Class A/month**. Under the 1M free tier → **$0.00**. Reads (`HeadObject`, Class B) at the same cadence: 518,400 → within 10M free → $0.00. Storage: one object well under 1 GB → within 10 GB free.
- Two nodes both attempting renewals (contending conditional PUTs, losers paying 412): 17,280 × 2 = 34,560/day → **1,036,800 Class A/month**. That is 36,800 over the free million; with round-up that is 1,000,000 billable → 1 × $4.50 = **$4.50/month**. **Hypothesis** (arithmetic on Confirmed prices + Confirmed 412-billing rule).
- Add retries and 429 back-off and the two-node figure grows; a 1 s retry loop would blow past the free tier and approach the 1-write/s-per-key limit.
- No official R2 latency figures were found. **Confirmed** as "not found" (no latency number appears on the pricing, limits, consistency or S3 API pages fetched). Open risk.

### Availability incidents

- 2025-02-06: R2 was unavailable for **59 minutes** (08:14–09:13 UTC) with a **100% error rate on all operations against R2 buckets and objects**, caused by human error in abuse remediation disabling the R2 Gateway service. No data loss; the gateway is the front door for both the S3 and REST APIs, and in-flight requests were terminated. **Confirmed.** Source: https://blog.cloudflare.com/cloudflare-incident-on-february-6-2025 (accessed 2026-11-02)
- In the same incident, Durable Objects (used by R2's own metadata layer) saw only a 0.09% error increase in North America during recovery — i.e. the R2 API frontend failed while the DO-based metadata layer did not. **Confirmed.** Source: https://blog.cloudflare.com/cloudflare-incident-on-february-6-2025 (accessed 2026-11-02)
- Any single external arbiter (R2 or a DO) is a new dependency: a 59-minute arbiter outage means no leader can be *renewed* for 59 minutes. With D-0019 self-fencing, that converts into "stateful apps stop" unless the node is allowed to keep running on an unexpired lease. **Hypothesis** (consequence of the Confirmed incident plus D-0019's self-fencing rule).

## Versions checked

| Component | Version | Release date / last updated | Source |
| --- | --- | --- | --- |
| R2 S3 API compatibility page | n/a | last updated 2026-10-14 | https://developers.cloudflare.com/r2/api/s3/api/ |
| R2 S3 extensions (conditional ops) | n/a | last updated 2026-10-14 | https://developers.cloudflare.com/r2/api/s3/extensions/ |
| R2 consistency model | n/a | last updated 2026-10-14 | https://developers.cloudflare.com/r2/reference/consistency/ |
| R2 limits | n/a | last updated 2026-10-30 | https://developers.cloudflare.com/r2/platform/limits/ |
| R2 pricing | n/a | last updated 2026-10-30 | https://developers.cloudflare.com/r2/pricing/ |
| R2 Workers API reference | n/a | last updated 2026-07-31 | https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ |
| AWS S3 conditional writes (If-None-Match) | n/a | announced 2024-08-20 | https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/ |
| AWS S3 conditional writes (If-Match) | n/a | announced 2024-11-25 | https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/ |
| MinIO AIStor S3 compatibility | AIStor docs, changed in RELEASE.2026-08-07T18-34-35Z | 2026 | https://docs.min.io/aistor/developers/s3-api-compatibility/ |
| Backblaze B2 S3 `Put Object` reference | n/a | page dated 2023-06-01 (>12 months old) | https://www.backblaze.com/apidocs/s3-put-object |

## Implications for Quoro

- **D-0017 (external lease arbiter via S3/R2 conditional writes): the premise holds for R2 specifically.** A single `PutObject` with `If-Match`/`If-None-Match` is a genuine CAS with one winner and 412 for the rest, on strongly consistent storage. This is enough to arbitrate a lease between two nodes.
- **Portability (D-0025) is the weak point.** The arbiter is described as "S3/R2 conditional writes", but the conditional-PUT capability is not part of the portable S3 compatibility surface Quoro promises. AWS S3 and MinIO AIStor support it; B2 appears not to; community MinIO is disputed. If the arbiter is offered as "bring your own S3 bucket", Quoro must *probe* the capability at setup (one `If-None-Match: *` against a scratch key, then a second with the same header expecting 412) and refuse to enable the arbiter if the probe fails. Without that probe, a provider that silently ignores `If-None-Match` (the failure mode reported for some stores) would give Quoro a lease arbiter that can hand the same lease to both nodes — a split-brain path, which is exactly what D-0016/D-0017 exist to prevent.
- **R2's 1-write-per-second-per-key limit constrains the lease design.** Renewals every 5 s by the leader are 0.2 writes/s — fine. A design where both nodes poll the key aggressively, or where a node retries in a tight loop, will hit 429s; a 429 must be treated as "lease not renewed" (fail closed) and not silently retried, otherwise the node's self-fencing timer and the arbiter disagree.
- **No TTL means the "expiry" is Quoro's own number.** The arbiter cannot revoke a lease. So D-0019's fencing must be end-to-end: the lease object carries a monotonic token, and the token is what the node-agent checks before starting a stateful app, and what any downstream resource (volume, VM, LB target) can reject. A lease timestamp alone does not satisfy D-0019.
- **Release must not be a delete.** `DeleteObject` has no conditional support on R2, so "release the lease" has to be a conditional put that marks the object free (e.g. `owner: null`, token incremented), or the delete/create race becomes a second split-brain path.
- **Cost is negligible, but the free-tier cliff is sharp.** Leader-only renewal (518k writes/month) is $0; two contending nodes (1.04M writes/month) is $4.50/month because of the round-up rule. Neither is a design constraint; the retry policy is.
- **A 59-minute R2 outage stops all renewals.** Because D-0019 has nodes self-fence, an arbiter outage becomes an availability event for stateful apps on Quoro, not just a control-plane event. This is evidence that the arbiter needs a documented "arbiter unreachable" behaviour distinct from "lease lost" (the current decisions do not distinguish them).

## Open risks and unknowns

- Whether R2 can ever return `409 Conflict` (as AWS does for delete-vs-conditional-write races) is not documented; a client that only handles 412 may mis-handle a 409. Not found.
- No official R2 latency figures (p50/p99 for PutObject/HeadObject from a given region) were found. The lease timeout must therefore be chosen without a published latency budget — this affects how long a node can safely believe it holds a lease.
- Whether community MinIO (as opposed to paid AIStor) implements `If-Match`/`If-None-Match` correctly is unresolved: the AIStor docs say yes, a 2024 discussion says `IfNoneMatch=*` did not prevent a second write. Needs a hands-on probe, not more reading.
- Backblaze B2's conditional-PUT support is asserted only by absence in its own (2023-dated) S3 reference plus a third-party matrix. B2 may have added it since; the doc is older than 12 months. Needs a probe or a current B2 changelog.
- The precise semantics of `If-Match` when the target key does not exist on R2 (404 vs 412) are not stated in the pages fetched; AWS documents 404 for that case. Not found for R2.
- Whether R2's 1-write/s-per-key limit is enforced per bucket or per account, and how it interacts with conditional writes specifically (as opposed to unconditional writers), is not stated.

## Sources

1. https://developers.cloudflare.com/r2/api/s3/api/ — accessed 2026-11-02
2. https://developers.cloudflare.com/r2/api/s3/extensions/ — accessed 2026-11-02
3. https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ — accessed 2026-11-02
4. https://developers.cloudflare.com/r2/reference/consistency/ — accessed 2026-11-02
5. https://developers.cloudflare.com/r2/platform/limits/ — accessed 2026-11-02
6. https://developers.cloudflare.com/r2/pricing/ — accessed 2026-11-02
7. https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html — accessed 2026-11-02
8. https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html — accessed 2026-11-02
9. https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/ — accessed 2026-11-02
10. https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/ — accessed 2026-11-02
11. https://docs.min.io/aistor/developers/s3-api-compatibility/ — accessed 2026-11-02
12. https://github.com/minio/minio/discussions/20318 — accessed 2026-11-02
13. https://www.backblaze.com/apidocs/s3-put-object — accessed 2026-11-02
14. https://www.backblaze.com/docs/cloud-storage-s3-compatible-api — accessed 2026-11-02
15. https://docs.nebius.com/object-storage/transfer/requirements — accessed 2026-11-02
16. https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html — accessed 2026-11-02
17. https://blog.cloudflare.com/cloudflare-incident-on-february-6-2025 — accessed 2026-11-02

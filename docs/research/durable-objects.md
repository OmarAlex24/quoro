# Cloudflare Durable Objects as an external lease arbiter

- **Question:** Q-0012 Cloudflare Durable Objects as an external lease arbiter (consistency guarantees, cost)
- **Researched:** 2026-11-02 by agent:ds-research-2
- **Status:** answered

## Short answer

A Durable Object is a good fit for the *shape* of a lease arbiter: one globally-unique, single-threaded instance per id, with private transactional storage, input gates (no concurrent event delivery while a storage op is in flight) and output gates (a reply is not emitted until the write is durable, and is replaced by an error if the write fails). That is a stronger primitive than an S3 conditional PUT, because the arbitration logic can be a single synchronous critical section instead of a client-side CAS loop.

It costs almost nothing: 2 nodes renewing every 5 s is ~1.04M requests/month, which fits the Workers **Free** plan limits (100k requests/day, 13,000 GB-s/day), and on the Paid plan is ~$0.15 of overage on top of the $5/month minimum.

The costs are structural, not financial. It requires a Cloudflare account and a deployed Worker (a DO has no public hostname — it is reached through a Worker binding), the arbiter logic must be written in JavaScript/TypeScript on Cloudflare's runtime, and it adds a third-party dependency in the exact path that decides who is primary. Also note the platform's own escape hatch: objects can be shut down and restarted at any time, and in-flight requests that touch storage are terminated with an error — so the lease protocol has to be correct across an abrupt restart, not just across a network partition.

## Findings

### Consistency guarantees

- Each Durable Object has a **globally-unique** id/name and "can be used to coordinate between multiple clients who need to work together"; its storage "lives together with the object, [is] strongly consistent yet fast to access". **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- Storage is "durable, transactional, and strongly consistent", "persisted across requests, and accessible only within that object". **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- **Single-threaded:** "Durable Objects are single-threaded and cooperatively multi-tasked, just like code running in a web browser." Objects are "single-threaded and cooperatively multi-tasked". **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- The actor framing is documented explicitly: "Each Durable Object instance can be seen as an Actor instance, receiving messages ... executing some logic in its own single-threaded context". **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- **Uniqueness is enforced at creation, not just assumed:** for a name-derived id, "the system must check that the Durable Object has not been created anywhere else ... this round-the-world check can take up to a few hundred milliseconds", and the stated purpose is "to guarantee that only one instance of the Durable Object is created". **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/api/namespace/ (accessed 2026-11-02)
- **Input gates:** "While a storage operation is executing, no events shall be delivered to the object except for storage completion events. Any other events will be deferred until such a time as the object is no longer executing JavaScript code *and* is no longer waiting for any storage operations." **Confirmed.** Source: https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/ (accessed 2026-11-02; post published 2021-08-03, page `dateModified` 2026-07-15)
- **Output gates:** "When a storage write operation is in progress, any new outgoing network messages will be held back until the write has completed. ... If the write ultimately fails, the outgoing network messages will be discarded and replaced with errors, while the Durable Object will be shut down and restarted from scratch." This is the property that makes "reply OK = write is durable" true. **Confirmed.** Source: https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/ (accessed 2026-11-02)
- Writes are automatically coalesced and ordered: multiple non-awaited `put()`/`delete()` calls are stored atomically together, and operations are performed "in exactly the order in which they were initiated". **Confirmed.** Source: https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/ (accessed 2026-11-02)
- **During shutdown/migration:** "When a Durable Object is shut down, the object instance is automatically restarted and new requests are routed to the new instance." In-flight **HTTP & RPC** requests are "allowed to finish if they do not access a Durable Object's storage. If a request attempts to access a Durable Object's storage, it will be stopped immediately and return an error **to maintain Durable Objects global uniqueness property**." WebSockets are terminated. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ (accessed 2026-11-02)
- Shutdown causes are routine, not exceptional: code updates, lack of requests, "Cloudflare updates to the Workers runtime system", and "Workers runtime decisions on where to host objects". No shutdown hooks are provided; Cloudflare says to write state incrementally instead. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ (accessed 2026-11-02)
- **Can two instances ever run concurrently?** The docs state the invariant ("Each Durable Object runs in exactly one location, in one single thread, at a time" — blog) and that storage-touching requests are killed at shutdown specifically to preserve uniqueness. **No page fetched documents a window in which two instances serve the same object.** **Confirmed** for the documented statements; the absence of a documented counterexample is an absence in the docs, not a proof. Source: https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/ , https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ (accessed 2026-11-02)
- Practical consequence for a lease: an arbiter call can fail with a generic error mid-write and the object can come back with the *pre-write* state. A client must treat "no definitive response" as "lease not held", exactly as with the R2 negative-read caveat. **Hypothesis** (composition of the Confirmed shutdown behaviour with the Confirmed output-gate rule). Source: https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ (accessed 2026-11-02)

### Availability

- "Durable Objects migrate among healthy servers, and therefore applications never have to worry about managing them." **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- "Durable Objects are not yet available in every Cloudflare data center." **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- **Location hints are best effort:** `get(id, {locationHint})` "Only the first call to `get()` for a particular Object will respect the hint", and "Hints are a best effort and not a guarantee. Unlike with jurisdictions, Durable Objects will not necessarily be instantiated in the hinted location". **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/reference/data-location/ (accessed 2026-11-02)
- **Objects do not move:** "Durable Objects do not currently change locations after they are created"; dynamic relocation is "planned for the future". South America, Africa and Middle East hints spawn the object in a nearby supported location instead. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/reference/data-location/ (accessed 2026-11-02)
- **Jurisdictions** (`eu`, `us`, `fedramp`) constrain where the DO runs and stores data, and are a hard constraint (unlike hints): "You will run into an error if the jurisdiction on your `DurableObjectNamespace` and the jurisdiction on `DurableObjectId` are different." **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/reference/data-location/ (accessed 2026-11-02)
- **Latency, sourced numbers only:** every request to a DO must be forwarded to the data center where it lives, which "will often add response latency". The only published figure found is for *first* access to a name-derived id: the uniqueness check "can take up to a few hundred milliseconds". No steady-state cross-region latency figure was found. **Confirmed** (statements) / **not found** (steady-state numbers). Source: https://developers.cloudflare.com/durable-objects/reference/data-location/ , https://developers.cloudflare.com/durable-objects/api/namespace/ (accessed 2026-11-02)
- **Evidence from a real Cloudflare incident:** during the 2025-02-06 R2 outage, Durable Objects "were impacted as the stampeding horde of clients re-connecting to R2 drove an increase in load", with "a 0.09% actual increase in error rates in calls to Durable Objects running in North America" between 09:13 and 09:36 UTC. The failing component was the R2 gateway, not DO. **Confirmed.** Source: https://blog.cloudflare.com/cloudflare-incident-on-february-6-2025 (accessed 2026-11-02)
- What happens if the DO's *own* location is down (colo failure) is **not** documented on the pages fetched; the lifecycle page only says the runtime decides to "move the object to a different host". **Hypothesis / not found.** Source: https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ (accessed 2026-11-02)

### Programming model for a lease

- **Storage** for the fencing counter: KV API (`get`/`put`) or SQL API, both available on SQLite-backed objects (KV only on the legacy backend). A `put()` of `{owner, expiry, token}` inside one input-gated critical section is the acquire operation. **Confirmed** (API surface); the lease recipe is **Hypothesis**. Source: https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ (accessed 2026-11-02)
- **Alarms for TTL:** one alarm per object at a time (`setAlarm` overrides), `alarm()` is called at the scheduled time, execution is **at-least-once**, retried on throw with exponential backoff "starting at a 2 second delay ... with up to 6 retries allowed". "Only one instance of `alarm()` will ever run at a given time per Durable Object instance." Alarm handlers have a **15 minute** wall-time limit. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/api/alarms/ , https://developers.cloudflare.com/durable-objects/platform/limits/ (accessed 2026-11-02)
- Alarm caveat for correctness: at-least-once means an alarm handler can run twice, so "lease expired" transitions must be idempotent (compare the token before revoking). **Hypothesis.** Source: https://developers.cloudflare.com/durable-objects/api/alarms/ (accessed 2026-11-02)
- **Reachability:** a DO is addressed through a `DurableObjectNamespace` binding available "from the fetch handler on a Cloudflare Worker", via `getByName` / `get(id)` returning a stub. There is no documented public hostname for a DO; reaching it from a plain HTTPS client requires a Worker route in front that forwards the request to the stub. **Confirmed** for the binding-only access path; the "no public endpoint" conclusion is **Hypothesis** (stated as the access model, never as an explicit prohibition). Source: https://developers.cloudflare.com/durable-objects/api/namespace/ (accessed 2026-11-02)
- Lifecycle cost/behaviour relevant to a 5 s lease: an object is "active, in-memory" while handling, then idle (hibernateable or not), then hibernated after "10 seconds of inactivity", then evicted entirely after "70-140 seconds of inactivity". In-memory state is lost on hibernation; storage survives. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ (accessed 2026-11-02)
- A lease renewed every 5 s from 2 nodes means the object is hit every ~2.5 s, so it will effectively never hibernate. That is fine for correctness (state is in storage) but it means the code cannot rely on in-memory state across quiet periods. **Hypothesis** (arithmetic on the Confirmed lifecycle timings).

### Cost

Published DO prices:

| | Workers Free | Workers Paid |
| --- | --- | --- |
| Requests | 100,000 / day | 1M / month included, + $0.15 / million |
| Duration | 13,000 GB-s / day | 400,000 GB-s / month included, + $12.50 / million GB-s |
| Rows read (SQLite) | 5M / day | first 25B / month included, + $0.001 / million |
| Rows written (SQLite) | 100,000 / day | first 50M / month included, + $1.00 / million |
| SQL stored data | 5 GB total | 5 GB-month, + $0.20 / GB-month |
| Storage backend | SQLite only | SQLite (KV only for pre-existing KV namespaces) |

- **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/platform/pricing/ (accessed 2026-11-02)
- Billing nuances: duration is billed in wall-clock time "as long as the Object is active and not eligible for hibernation", shared across concurrent requests; an idle object that qualifies for hibernation is **not** billed even before the runtime hibernates it. `accept()` on a standard-API WebSocket bills duration for the whole connection, hence the Hibernation API recommendation. Duration is charged for the full 128 MB allocation regardless of actual use. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/platform/pricing/ (accessed 2026-11-02)
- Each `setAlarm()` is billed as **one row written**; KV-style `get`/`put`/`delete` are billed as rows read/written. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/platform/pricing/ (accessed 2026-11-02)
- Paid-plan examples include a "minimum $5/mo usage" line, i.e. the Workers Paid plan floor. **Confirmed.** Source: https://developers.cloudflare.com/durable-objects/platform/pricing/ (accessed 2026-11-02)
- The pricing page still carries the note that SQLite storage billing starts 2026-01-07; given today's date that billing is in effect. **Confirmed** (note text); interpretation is **Hypothesis**. Source: https://developers.cloudflare.com/durable-objects/platform/pricing/ (accessed 2026-11-02)

Arithmetic, 2 nodes renewing every 5 s (30-day month):

- Requests: 86,400 / 5 = 17,280 per node per day → 34,560/day → **1,036,800/month**.
  - **Free plan:** 34,560/day vs 100,000/day limit → fits. $0.
  - **Paid plan:** 36,800 over the included 1M, rounded up to 1,000,000 billable → 1 × $0.15 = **$0.15**, plus the **$5/month** plan minimum → **$5.15/month**.
- Duration (assume ~10 ms wall time per renewal): 1,036,800 × 0.010 s = 10,368 s → × 128 MB/1 GB = **1,327 GB-s/month**. Paid included 400,000 → $0. Free daily limit is 13,000 GB-s (~390,000/month) → $0.
- Rows written (one `put` per renewal, plus one `setAlarm` when the expiry moves): ~1.04M–2.07M/month. Free limit 100,000/day (~3M/month) → fits. Paid included 50M → $0.
- Storage: one small record, far under 5 GB → $0.
- **Hypothesis** overall (arithmetic over Confirmed prices and the Confirmed request/duration model). The one guessed input is the 10 ms per-request wall time. Even a 100 ms handler lands at 103,680 s → 13,270 GB-s/month, i.e. ~442 GB-s/day against the Free plan's 13,000 GB-s/day — still a large margin. The free plan is viable either way.

### Operational burden for a self-hosted Quoro user

- Needs a Cloudflare account, an API token with Workers Scripts write (or `wrangler login`), a Worker script exporting the DO class, and a DO namespace declared in the Wrangler configuration; the namespace must be migrated/created on first deploy. **Confirmed** for the config/binding model. Source: https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ (binding model), https://developers.cloudflare.com/durable-objects/api/namespace/ (accessed 2026-11-02)
- Deployment from code is possible without wrangler: `PUT /accounts/{account_id}/workers/scripts/{script_name}` ("Upload Worker Module") accepts multipart form data with a JSON `metadata` part carrying `bindings`, including a `durable_object_namespace` binding with `class_name` / `namespace_id` / `script_name`. Token permissions: `Workers Scripts Write`, `Workers Admin`, `Workers Editor`. **Confirmed** (described only; not used). Source: https://developers.cloudflare.com/api/resources/workers/subresources/scripts/methods/update/ (accessed 2026-11-02)
- This means Quoro could ship "create the arbiter" as a one-click/one-command flow that uploads a small Worker — but it still requires the user to hold a Cloudflare account and an API token, and it puts the arbiter's source code inside the user's Cloudflare account, where Quoro cannot upgrade it silently without another deploy. **Hypothesis** (operational inference).

### Comparison with R2 and with a self-hosted witness

| | R2 conditional writes | Durable Object | Self-hosted witness daemon |
| --- | --- | --- | --- |
| Primitive | CAS on one object (`If-Match` / `If-None-Match`), 412 on loss | Single-threaded actor with transactional storage; arbitration is plain code | Whatever the daemon implements (vote / tie-break) |
| Server-side TTL | None — expiry is a client-written timestamp | Possible (alarm fires in the object, on Cloudflare's clock) | Possible (daemon's own clock) |
| Access from Rust nodes | Any S3 client (`aws-sdk-s3`, `rust-s3`) | Needs a Worker route in front; HTTPS + JSON/RPC | Plain HTTP to the user's own host |
| Portability | Depends on the provider's S3 conditional support (AWS yes, B2 apparently no) | Cloudflare-only, JS/TS worker code | Fully self-hosted, any machine |
| Failure mode | Provider API outage (R2: 59 min at 100% errors on 2025-02-06) | Provider outage; object shutdown/restart kills in-flight storage requests | The user's own third box; no third-party dependency |
| Cost at 5 s renewal, 2 nodes | $0 leader-only; $4.50 if both contend | $0 free plan; $5.15/month paid | Free (user's hardware) |
| Fits D-0017 "witness in a different failure domain" | Yes (Cloudflare's domain) | Yes (Cloudflare's domain) | Yes, *if* the user actually places it elsewhere |

- Every cell above traces to the Findings sections of this file and `docs/research/r2-conditional-writes.md`; the comparison itself is **Hypothesis** (synthesis).
- D-0017 lists the self-hosted witness as "free and first-class" and the external arbiter as optional. Nothing in this research contradicts that ordering: the DO is the cheapest *managed* option but it is the only option that requires a Cloudflare account and JavaScript code in the critical path, whereas the R2 option requires an S3 bucket the user may already have.

## Versions checked

| Component | Version | Release date / last updated | Source |
| --- | --- | --- | --- |
| Durable Objects — What are Durable Objects | n/a | last updated 2026-07-15 | https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ |
| Durable Objects — Lifecycle | n/a | last updated 2026-10-30 | https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ |
| Durable Objects — Limits | n/a | last updated 2026-10-29 | https://developers.cloudflare.com/durable-objects/platform/limits/ |
| Durable Objects — Pricing | n/a | last updated 2026-10-18 | https://developers.cloudflare.com/durable-objects/platform/pricing/ |
| Durable Objects — Data location | n/a | last updated 2026-06-26 | https://developers.cloudflare.com/durable-objects/reference/data-location/ |
| Durable Objects — Alarms API | n/a | last updated 2026-04-21 | https://developers.cloudflare.com/durable-objects/api/alarms/ |
| Durable Objects — Namespace API | n/a | last updated 2026-04-21 | https://developers.cloudflare.com/durable-objects/api/namespace/ |
| "Easy, Fast, Correct" gates post | n/a | published 2021-08-03 (>12 months old), page modified 2026-07-15 | https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/ |
| SQLite storage billing start | n/a | target 2026-01-07 | https://developers.cloudflare.com/durable-objects/platform/pricing/ |

## Implications for Quoro

- **D-0017's "Cloudflare Durable Object (strong consistency)" premise is accurate.** The DO gives a real single-writer critical section, and input/output gates mean a successful reply implies the write is durable — the property a lease arbiter needs and the one an S3 bucket cannot give without client-side care.
- **A DO is the only option in D-0017 that can own the lease's *expiry*.** R2 has no TTL, so with R2 the "lease expired" event is something the two nodes infer from their own clocks. With a DO, an alarm fires inside the object and the arbiter itself can revoke the lease and bump the fencing token. That is a materially different safety story for D-0019, though the fencing token is still mandatory on the node side.
- **But the DO does not remove the need for fencing.** Objects are shut down for routine reasons, in-flight storage-touching requests are killed with an error, and the object can restart with pre-write state. A client that retries an acquire after an error can therefore hold a token the arbiter no longer considers current. D-0019's "node self-fences" requirement stays exactly as written; the DO only makes it easier to issue monotonic tokens.
- **Self-hosting tension with D-0012.** D-0012 says "the stateful API stays self-hosted". A DO arbiter does not move the control plane, but it does put a piece of *decision logic* (who is primary) on Cloudflare, in JavaScript, in a repo the user cannot `git pull`. If Quoro ships this, the arbiter Worker should be treated as a thin, versioned, published artifact (source in the Quoro repo, deployed by the user) rather than as an opaque managed service — otherwise the "self-hosted" claim weakens in exactly the failure path users care about.
- **Free-plan viability makes the DO the cheapest managed arbiter.** 2 nodes at 5 s is 34,560 requests/day against a 100,000/day limit; there is ~3× headroom, which is enough for retries but not for a 1 s renewal loop (which would be 172,800/day and blow the limit). This is a hard constraint on any future "faster failover" tuning.
- **Operational burden is a Cloudflare account + an API token + a deploy.** Not zero, and it is a step a self-hoster may refuse. The R2 option reuses an S3 credential the user may already have; the self-hosted witness needs a third box. These are three genuinely different user populations, which supports D-0017 keeping all three.
- **The Feb 2025 incident is the number to design against:** R2 was 100% down for 59 minutes while Durable Objects degraded by 0.09%. If the arbiter is a DO, a Cloudflare-wide event may still be survivable; if it is R2 conditional writes, the same event is a total arbiter outage. Both still need a documented "arbiter unreachable" policy separate from "lease lost" (D-0019 currently only defines the latter).

## Open risks and unknowns

- No documented behaviour for "the DO's data center is unavailable" — only "migrate among healthy servers" and a runtime decision to host elsewhere. The worst-case unavailability window for a single object is not published. Not found.
- No published steady-state latency for a client far from the DO's location; only "up to a few hundred milliseconds" for the first name lookup. This matters for choosing the lease TTL relative to renewal interval.
- Whether Cloudflare ever runs two instances of one object concurrently (e.g. during a runtime upgrade or a colo evacuation) is stated as never, but no formal or documented edge-case analysis was found. The safety of a lease protocol resting on that invariant should be treated as a platform claim, not a proof.
- Durable Objects are not available in every Cloudflare data center; users in unsupported regions get an object in a nearby location, so latency and failure-domain assumptions vary by user geography.
- The 2021 gates blog is the only primary description of input/output gates; it is older than 12 months (page modified 2026-07-15). No current doc page restates the invariants in the same detail, so a behaviour change would be hard to detect from docs alone.
- Free-plan headroom (100k requests/day) is shared with any other use of the same Cloudflare account; if a user already runs Workers there, the arbiter's budget is not guaranteed.
- SQLite storage billing started 2026-01-07; the exact storage cost for a lease object is negligible but the billing model changed recently and the page still describes it as a future change.

## Sources

1. https://developers.cloudflare.com/durable-objects/what-are-durable-objects/ — accessed 2026-11-02
2. https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/ — accessed 2026-11-02
3. https://developers.cloudflare.com/durable-objects/api/namespace/ — accessed 2026-11-02
4. https://developers.cloudflare.com/durable-objects/api/alarms/ — accessed 2026-11-02
5. https://developers.cloudflare.com/durable-objects/reference/data-location/ — accessed 2026-11-02
6. https://developers.cloudflare.com/durable-objects/platform/limits/ — accessed 2026-11-02
7. https://developers.cloudflare.com/durable-objects/platform/pricing/ — accessed 2026-11-02
8. https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/ — accessed 2026-11-02
9. https://developers.cloudflare.com/api/resources/workers/subresources/scripts/methods/update/ — accessed 2026-11-02
10. https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ — accessed 2026-11-02
11. https://blog.cloudflare.com/cloudflare-incident-on-february-6-2025 — accessed 2026-11-02

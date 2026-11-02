# Cloudflare Load Balancing pricing/health checks and cloudflared replica behaviour

- **Question:** Q-0013 VERIFY: Cloudflare Load Balancing pricing/health checks and cloudflared multi-replica failover behavior
- **Researched:** 2026-11-02 by agent:ds-research-3
- **Status:** answered (with two documented contradictions flagged)

## Short answer

Load Balancing is a paid add-on available on the Free, Pro and Business plans: $5/month covers up to 2 origin servers, +$5/month per origin up to 20, plus usage at $0.50 per 500k DNS requests after the first 500k (per month). Geo routing is +$10/month. Health checks start at 60s intervals from 1 location; 30s and 15s intervals cost +$10 and +$15/month, and 3 or 5 check locations cost +$10 and +$15/month. Monitors speak HTTP/HTTPS, TCP, ICMP, UDP-ICMP and SMTP, with configurable timeout/retries, so failover is on the order of tens of seconds to a couple of minutes, not seconds.

A Load Balancer **can** use a tunnel as an origin, but only as `<UUID>.cfargotunnel.com` with the app hostname in the Host header — not as `app.example.com` — and a single pool cannot reference the same tunnel UUID twice. An HTTPS monitor with `Host` set to the app hostname goes through `cloudflared` to the app, so it does test the app. TCP monitors are not supported for tunnel endpoints, though. The documented workaround (an `HTTP_STATUS` route on `cloudflared`) only proves that `cloudflared` is reachable, not that the app is up. A Load Balancer also cannot tell replicas of one tunnel UUID apart: they count as one endpoint.

`cloudflared` replicas (max 25 per tunnel, 4 connections each) give host-level failover only: the nearest replica is chosen, there is no steering guarantee, and each replica forwards to its locally configured service. If a replica's local app is down, that replica still receives traffic and returns the app's error (typically 502). Replicas are documented as identical copies of one tunnel ("all replicas serve the same routes and use the same configuration parameters"), not as N hosts each serving a different app; for that, the documented pattern is one tunnel per site plus a Load Balancer.

## Findings

### 1. Load Balancing pricing and tier availability

All figures below are quoted verbatim from Cloudflare's own plans comparison table on the Analytics product page (the "Load Balancing", "Global Load Balancing (Geo-based Routing)" and "Load Balancing Health Monitors" rows). **Confirmed.** Source: https://www.cloudflare.com/en-gb/application-services/products/analytics/ (accessed 2026-11-02)

Note on sourcing: the previously cited `/en-in/` URL returns a bot challenge to a plain fetch and is not the Load Balancing page, so it has been replaced. The `developers.cloudflare.com/load-balancing/` docs no longer publish prices at all — the product index page now sends readers to the dashboard ("Explore pricing options for Load Balancing in the dashboard", https://developers.cloudflare.com/load-balancing/index.md, accessed 2026-11-02). The marketing page above is therefore the only public, fetchable, itemised source.

| Item | Price / limit | Vendor wording (verbatim) | Label |
| --- | --- | --- | --- |
| Base fee | $5/month for up to 2 origin servers | "Up to 2 origin servers: $5 per month" | Confirmed |
| Extra origins | $5/month per origin, up to 20 | "Additional origins, up to 20: $5 per month per origin" | Confirmed |
| Usage (DNS queries) | First 500k DNS requests/month free; $0.50 per 500k after | "First 500k DNS requests are free" / "$0.50 per every 500k DNS requests after" | Confirmed |
| Geo routing (Global LB) | +$10/month | "Enable for $10 per month" | Confirmed |
| Health check interval | 60s included; 30s +$10/month; 15s +$15/month | "Every 60 seconds: included" / "Every 30 seconds: additional $10 per month" / "Every 15 seconds: additional $15 per month" | Confirmed |
| Health check locations | 1 included; up to 3 +$10/month; up to 5 +$15/month | "1 location included" / "Up to 3 locations: additional $10 per month" / "Up to 5 locations: additional $15 per month" | Confirmed |
| Availability by plan | Free, Pro, Business (self-serve add-on); Enterprise custom | The table's four columns are Free / Pro / Business / Contract, and Load Balancing is marked "Paid Add-on" under Free as well as Pro and Business | Confirmed |
| Enterprise | Custom pricing; checks every 5s from all data centres | "Custom Pricing" / "Includes health checks every 5 seconds." / "Includes health checks from every Cloudflare data center." | Confirmed |

- The plans pages themselves only repeat "Starting at $5 per month" for Load Balancing and do not itemise anything. **Confirmed.** Sources: https://www.cloudflare.com/plans/application-services/ and https://www.cloudflare.com/plans/network-cdn.md (both accessed 2026-11-02).
- The same base-fee and usage text also appears in English on the Web Optimization page, which is an independent fetchable copy. **Confirmed.** Source: https://www.cloudflare.com/application-services/products/website-optimization/ (accessed 2026-11-02) — "Up to 2 origin servers: $5 per month", "Additional origins, up to 20: $5 per month per origin", "First 500k DNS requests are free", "$0.50 per every 500k DNS requests after", "Enable for $10 per month".
- "First 500K included" is corroborated by the billing docs, which also confirm the metered dimension is DNS queries to load-balanced hostnames, not bytes. **Confirmed.** Source: https://developers.cloudflare.com/billing/understand/how-charges-accrue/index.md (accessed 2026-11-02; last updated 2026-04-24): "If you use Load Balancing, DNS queries to load-balanced hostnames are metered (first 500K included)." Also https://developers.cloudflare.com/billing/understand/usage-based-billing/index.md (accessed 2026-11-02): Load Balancing billable metric "DNS queries", free tier "First 500K queries".
- Account quotas (non-Enterprise): 20 load balancers, 20 pools, 20 endpoints, monitors limited to 1.5x pools, monitor interval 15s–3600s. **Confirmed.** Source: https://developers.cloudflare.com/load-balancing/reference/limitations/ (accessed 2026-11-02; page last updated 2026-04-16)
- **Contradiction (unresolved):** the limitations page says the minimum interval is 15s for non-Enterprise and 10s for Enterprise, while the pricing table says Enterprise includes 5s checks. Both statements are now confirmed quotes from Cloudflare pages, so the disagreement is real and not a fetch artefact. Treat 15s as the safe non-Enterprise floor and do not design an Enterprise tier against either number. **Hypothesis** (which is current).

### 2. Health checks (monitors)

- Supported monitor types: HTTP/HTTPS, TCP, ICMP Ping, UDP-ICMP, SMTP. HTTP/HTTPS and TCP work for public and private endpoints; ICMP and UDP-ICMP also work for Tunnel; SMTP is public only. **Confirmed.** Source: https://developers.cloudflare.com/load-balancing/monitors/ (accessed 2026-11-02; last updated 2026-07-23)
- Each selected region probes from three data centres; a region is healthy if the majority of its data centres pass, and the endpoint is healthy if the majority of regions pass. **Confirmed.** Same URL.
- Configurable per monitor: interval, timeout, retries, expected codes/body, method, path, headers, `consecutive_up`/`consecutive_down`. **Confirmed.** Sources: https://developers.cloudflare.com/load-balancing/monitors/ and https://developers.cloudflare.com/load-balancing/understand-basics/health-details/ (accessed 2026-11-02)
- A pool becomes Critical when healthy endpoints fall below its Health Threshold, and traffic then follows the steering policy; the fallback pool is used as last resort and its health is ignored. **Confirmed.** Source: https://developers.cloudflare.com/load-balancing/understand-basics/health-details/ (accessed 2026-11-02; last updated 2026-04-16)
- With no monitor attached, pool health is not considered during steering. **Confirmed.** Same URL.
- Failover latency is roughly `interval x (retries + 1)`, plus region majority voting, plus DNS/proxy caching (Cloudflare caches resolved IPs for up to 5s). At the cheapest tier (60s, 1 region, 3 data centres) that is ~2–3 minutes worst case. **Hypothesis** (arithmetic from documented interval/retries; Cloudflare does not publish a failover-time figure). Sources: monitors page and https://developers.cloudflare.com/load-balancing/troubleshooting/load-balancing-faq/ (accessed 2026-11-02)
- Steering policies: Standard (failover/random), Geo, Dynamic (latency), Proximity, Least Outstanding Requests; origin-level: random, hash, least outstanding. **Confirmed.** Source: https://developers.cloudflare.com/load-balancing/understand-basics/traffic-steering/steering-policies/ (accessed 2026-11-02; last updated 2026-09-10)
- Session affinity: cookie (`__cflb`, default 23h TTL), cookie + client-IP fallback, or HTTP header; public load balancers only, not in DNS-only (grey cloud) mode. **Confirmed.** Source: https://developers.cloudflare.com/load-balancing/understand-basics/session-affinity/ (accessed 2026-11-02; last updated 2026-04-17)
- Zero-Downtime Failover (none/temporary/sticky) covers failures between health checks. **Confirmed.** Same URL.

### 3. Cloudflare Tunnel endpoints as Load Balancer origins

- A published application route generates `<UUID>.cfargotunnel.com`. That hostname plus the app hostname in the endpoint's Host header is how you add a tunnel to a pool. **Confirmed.** Source: https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/public-load-balancers (accessed 2026-11-02; last updated 2026-06-23)
- "Load Balancer does not support directly adding `app.example.com` as an endpoint if the service is behind Cloudflare Tunnel." **Confirmed.** Same URL.
- "A single origin pool cannot reference the same tunnel UUID twice." **Confirmed.** Same URL.
- Recommended monitor for tunnel origins: HTTPS monitor with `Host` = published app hostname. **Confirmed.** Same URL.
- "TCP monitors are not supported for tunnel endpoints. Instead, create a health check endpoint on the `cloudflared` host and use an HTTPS monitor", e.g. a route with service type `HTTP_STATUS` 200. **Confirmed.** Same URL.
- "This monitor verifies that `cloudflared` is reachable. It does not check whether the upstream service is accepting requests." **Confirmed.** Same URL. Scope (reviewer correction, agent:so-research): this sentence in the docs refers only to the `HTTP_STATUS` health-check route offered as a workaround for the missing TCP monitors. The recommended HTTPS monitor with `Host` = app hostname (above) is proxied through `cloudflared` to the app and returns the app's status, so it is an app-level check. **Hypothesis** for the app-level reading: the docs recommend that monitor but do not spell out what it covers. With replicas of one UUID, though, the monitor reaches whichever replica is nearest, so it can pass while another replica's local app is down.
- "The load balancer does not distinguish between replicas of the same tunnel. If you run the same tunnel UUID on two separate hosts, the load balancer treats both hosts as a single endpoint." Session affinity to a specific host therefore requires a different tunnel UUID per host. **Confirmed.** Same URL.
- Private hostname routes are not compatible with Load Balancing; use replicas for HA there. **Confirmed.** Source: https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-availability/ (accessed 2026-11-02; last updated 2026-04-17)
- Documented topology for real per-site failover: one dedicated tunnel per data centre, one pool per tunnel, one endpoint per pool, plus ≥2 replicas per tunnel inside each site; active-active requires multiple tunnel endpoints per pool (different UUIDs). **Confirmed.** Source: public-load-balancers page above.
- Caveat on distribution even with LB: `cloudflared` prefers connections in the same Cloudflare data centre, which can skew traffic across endpoints; Cloudflare suggests separate tunnels for finer steering. **Confirmed.** Same URL.

### 4. cloudflared replicas

- Every `cloudflared` instance opens four outbound connections to four different Cloudflare servers across at least two data centres. **Confirmed.** Source: tunnel-availability page (accessed 2026-11-02)
- Replicas: "You can run the same tunnel across various `cloudflared` processes for up to 100 connections (25 replicas) per tunnel." **Confirmed.** Source: https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-availability/deploy-replicas/ (accessed 2026-11-02; last updated 2026-04-17)
- Account limit agrees: "Active `cloudflared` replicas per tunnel: 25". **Confirmed.** Source: https://developers.cloudflare.com/cloudflare-one/account-limits/ (accessed 2026-11-02; last updated 2026-10-16)
- "If your tunnel runs as a service, only one `cloudflared` instance is allowed per host." **Confirmed.** Same deploy-replicas URL.
- Traffic distribution: "Replicas do not support traffic steering (such as round-robin or hash-based routing). When a request arrives at Cloudflare, it is forwarded to the geographically closest replica. If that connection fails, Cloudflare retries with other replicas, but there is no guarantee about which one is chosen." **Confirmed.** Source: tunnel-availability page.
- On host death: remaining replicas keep serving; Cloudflare retries other replicas when a connection fails. **Confirmed** that this is the design; the *time* to detect and reroute is not published. **Hypothesis** that it is sub-second to a few seconds (connection-level failure, not health-check driven).
- Does a replica check the health of its local origin service? No documented health checking of the upstream service is performed by `cloudflared`. **Hypothesis** — no Cloudflare page states "cloudflared health-checks the local service"; the external LB monitor is the only health check documented, and the `HTTP_STATUS` variant of it explicitly "does not check whether the upstream service is accepting requests". Consequence: a replica whose local app is down keeps receiving traffic and returns the local connection error (502/1033-style). **Hypothesis.**
- Are replicas meant to be identical? The docs describe replicas as the same tunnel run on multiple hosts: "All replicas serve the same routes and use the same configuration parameters." The HA purpose is "if a single host running `cloudflared` goes down, the remaining replicas continue to serve traffic", and the diagram shows both replicas pointing at the same `Application`. **Confirmed** (wording). Therefore "replicas on different hosts each serving a *different* local app" is **not** the documented pattern; the documented equivalent is one tunnel per site/pool. Using one UUID across hosts with divergent local services would put requests for any route on any host, i.e. random 502s. **Hypothesis** (behavioural consequence, not an explicit prohibition).

### 5. cloudflared version and licence

- Latest release: `2026.10.7`, published 2026-10-30. **Confirmed.** Source: https://github.com/cloudflare/cloudflared/releases (accessed 2026-11-02)
- Licence: Apache License 2.0. **Confirmed.** Source: https://raw.githubusercontent.com/cloudflare/cloudflared/master/LICENSE (accessed 2026-11-02)

### 6. Tunnel limits relevant to a free/hobby deployment

| Limit | Value | Label |
| --- | --- | --- |
| `cloudflared` tunnels per account | 1,000 | Confirmed |
| Active replicas per tunnel | 25 | Confirmed |
| Routes (CIDR + hostname) per account | 50,000 (shared with Cloudflare Mesh) | Confirmed |
| Published applications per tunnel | limited by 1 MB configuration size (~5,000–10,000 per changelog) | Confirmed |

Sources: https://developers.cloudflare.com/cloudflare-one/account-limits/ and https://developers.cloudflare.com/cloudflare-one/changelog/tunnel/ (accessed 2026-11-02).
- Tunnel itself is not priced per tunnel at these volumes. Beyond the 1,000 tunnels included in every account, "Tunnels for Platforms" sells extra capacity at $4.50 per additional tunnel per month. **Confirmed** (two Cloudflare pages, exact wording below).
  - Changelog entry dated 2026-10-16: "Every account includes 1,000 tunnels. With Tunnels for Platforms, you can now run more than 1,000 tunnels in a single account... Additional tunnels are billed at $4.50 per tunnel per month. Enterprise customers can contact their account team for committed or usage-based pricing." Source: https://developers.cloudflare.com/cloudflare-one/changelog/tunnel/ (accessed 2026-11-02).
  - Product page: "Tunnels for Platforms costs $4.50 per additional tunnel per month. The first 1,000 tunnels in every account remain included at no cost." Source: https://developers.cloudflare.com/tunnel/features/tunnels-for-platforms/ (accessed 2026-11-02; last updated 2026-10-16). Same page: every tunnel counts toward the limit, including inactive ones with no connected replicas.

## Versions checked

| Component | Version | Release date | Source |
| --- | --- | --- | --- |
| cloudflared | 2026.10.7 | 2026-10-30 | https://github.com/cloudflare/cloudflared/releases |
| cloudflared licence | Apache-2.0 | n/a | https://raw.githubusercontent.com/cloudflare/cloudflared/master/LICENSE |
| LB docs — Monitors | page last updated 2026-07-23 | — | https://developers.cloudflare.com/load-balancing/monitors/ |
| LB docs — Limitations | page last updated 2026-04-16 | — | https://developers.cloudflare.com/load-balancing/reference/limitations/ |
| Tunnel docs — availability/failover | page last updated 2026-04-17 | — | https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-availability/ |
| Tunnel docs — public load balancers | page last updated 2026-06-23 | — | https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/public-load-balancers |
| Zero Trust account limits | page last updated 2026-10-16 | — | https://developers.cloudflare.com/cloudflare-one/account-limits/ |

## Implications for Quoro

- **D-0024 needs a correction, not a reversal.** It says "Multiple cloudflared replicas on the same tunnel give basic failover; Cloudflare paid LB adds real health checks." The first half is right (host-level failover). The second half is only half right: over a tunnel, an LB monitor checks that `cloudflared` answers, not that the app answers (unless the app exposes a distinctive health route on a published hostname). So Cloudflare LB does not give Quoro app-level health for tunnelled origins out of the box; Quoro's own health probe (the witness/edge of D-0018), or an app-level `/health` route published through the tunnel and monitored by content/code, is still required.
- **Replicas are not a poor-man's multi-node LB across different apps.** One tunnel UUID = one logical origin; replicas are identical copies. If Quoro puts one `cloudflared` per node and each node serves that node's apps, the documented pattern is one tunnel per node + an LB pool with one endpoint per tunnel UUID (active-active), not one shared UUID. Sharing a UUID across nodes would route requests to a node that may not host the requested app.
- **Failover is slow relative to a supervisor.** Cheapest tier: 60s interval, 1 region, 3 data centres, plus ~5s DNS caching → tens of seconds to minutes. Quoro should not present Cloudflare failover as a substitute for in-cluster failover; it is a last-resort layer.
- **D-0018 (edge as witness) still holds, but Cloudflare cannot be the witness.** The LB's health view over a tunnel is connector-level, so a Cloudflare-only edge cannot testify about app health. A small VPS edge that probes apps directly remains the only way to get the "sees both nodes, decides where traffic goes" property described in D-0018.
- **D-0025 (provider abstraction) is cheap to honour.** Everything used here (pools, monitors, tunnel UUID endpoints, DNS) is API-addressable, so a `LbProvider` interface with a Cloudflare implementation does not require modelling proprietary concepts beyond pool/endpoint/monitor.
- Cost of a load balancer for 2 origins with 15 s checks from 3 regions: $5 base + $15 for 15 s checks + $10 for 3 regions ≈ $30/month before DNS-query usage; a single-region 30 s setup is about $15/month.

## Open risks and unknowns

- **Enterprise minimum interval is contradictory** (5s in the pricing table vs 10s in the limitations table). Do not design against either without a live check.
- **No official failover-time figure.** The interval x retries estimate ignores region majority voting and Cloudflare's internal propagation; real numbers must be measured.
- **502 behaviour from a dead local upstream is inferred, not documented.** No Cloudflare page states what status `cloudflared` returns when its configured service refuses connections; it must be measured (it also differs per protocol: HTTP vs TCP/SSH).
- **No documented statement that a replica never checks its local service.** Absence of documentation is not proof; the claim is labelled Hypothesis on purpose.
- **Pricing is only published in marketing/comparison tables**, not in the docs or an API. The `/plans/*.md` pages dropped the itemisation, and the Load Balancing docs index now says only "Explore pricing options for Load Balancing in the dashboard". Prices can change without a docs changelog, so re-check before quoting a cost to a user.
- **Same-UUID replicas and LB**: docs say LB treats them as one endpoint, but whether per-request retry causes a *different* replica to serve a retried request is not documented.
- Not verified: whether Cloudflare Zero Trust "free" tier imposes any additional tunnel limits beyond the account limits page (no separate free-tier table was found).

## Sources

1. https://developers.cloudflare.com/load-balancing/ (accessed 2026-11-02)
2. https://www.cloudflare.com/en-gb/application-services/products/analytics/ (accessed 2026-11-02) — plans comparison table with the itemised Load Balancing, Global LB and health-monitor pricing rows
3. https://www.cloudflare.com/application-services/products/website-optimization/ (accessed 2026-11-02) — independent English copy of the base fee / usage / geo-routing text
4. https://www.cloudflare.com/plans/application-services/ (accessed 2026-11-02)
5. https://www.cloudflare.com/plans/network-cdn.md (accessed 2026-11-02)
6. https://developers.cloudflare.com/billing/understand/how-charges-accrue/index.md (accessed 2026-11-02)
7. https://developers.cloudflare.com/billing/understand/usage-based-billing/index.md (accessed 2026-11-02)
8. https://developers.cloudflare.com/load-balancing/monitors/ (accessed 2026-11-02)
9. https://developers.cloudflare.com/load-balancing/understand-basics/health-details/ (accessed 2026-11-02)
10. https://developers.cloudflare.com/load-balancing/reference/limitations/ (accessed 2026-11-02)
11. https://developers.cloudflare.com/load-balancing/understand-basics/traffic-steering/steering-policies/ (accessed 2026-11-02)
12. https://developers.cloudflare.com/load-balancing/understand-basics/session-affinity/ (accessed 2026-11-02)
13. https://developers.cloudflare.com/load-balancing/troubleshooting/load-balancing-faq/ (accessed 2026-11-02)
14. https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/public-load-balancers (accessed 2026-11-02)
15. https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-availability/ (accessed 2026-11-02)
16. https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-availability/deploy-replicas/ (accessed 2026-11-02)
17. https://developers.cloudflare.com/cloudflare-one/account-limits/ (accessed 2026-11-02)
18. https://developers.cloudflare.com/cloudflare-one/changelog/tunnel/ (accessed 2026-11-02) — 2026-10-16 entry with the $4.50/tunnel wording
19. https://developers.cloudflare.com/tunnel/features/tunnels-for-platforms/ (accessed 2026-11-02) — product page repeating $4.50 per additional tunnel per month
20. https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/ (accessed 2026-11-02)
21. https://github.com/cloudflare/cloudflared/releases (accessed 2026-11-02)
22. https://raw.githubusercontent.com/cloudflare/cloudflared/master/LICENSE (accessed 2026-11-02)

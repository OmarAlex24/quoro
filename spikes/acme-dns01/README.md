# Spike: ACME DNS-01 with instant-acme

Throwaway spike for the M2 roadmap item "ACME client crate maturity with
DNS-01" (issue #65). It answers one question with evidence: is there a Rust
ACME client good enough for the embedded reverse proxy (ADR 0059), able to
obtain and renew a certificate through DNS-01, so Quoro does not need the Caddy
fallback?

**Result: yes, with `instant-acme` 0.8.5.** Against Pebble and against the
Let's Encrypt **staging** directory with Cloudflare DNS, the program obtained a
certificate through DNS-01, then renewed it with an ARI-suggested window
(RFC 9773) and a replacement order that names the old certificate in
`replaces`.

This directory is **not** part of the repository's cargo workspace: it has its
own `Cargo.toml` with an empty `[workspace]`. Nothing here is production code;
what survives lives in `crates/ingress/src/acme.rs`.

## Candidates

| Crate | Last release | License | DNS-01 | Notes |
| --- | --- | --- | --- | --- |
| `instant-acme` | 0.8.5, 2026-02 | Apache-2.0 | yes | Maintained by a rustls maintainer; `ring` or `aws-lc-rs`; ARI (`renewal_info`, `NewOrder::replaces`), profiles, EAB; rustls 0.23 and hyper 1 |
| `rustls-acme` | 0.15.4, 2026-08 | Apache-2.0 OR MIT | no | TLS-ALPN-01 only: unusable behind NAT and for DNS-01 |
| `tokio-rustls-acme` | 0.9.1, 2026-05 | Apache-2.0 OR MIT | no | Fork of the above, same limitation |
| `acme-lib` | 0.9.1, 2024-01 | MIT | yes | OpenSSL, synchronous, unmaintained |
| `acme2` | 0.5.1, 2022-05 | MIT | yes | Unmaintained |

Features used: `default-features = false, features = ["ring", "hyper-rustls",
"time", "x509-parser"]`. `cargo deny check` with the repository's `deny.toml`
passes for the shipped targets (ADR 0080, ADR 0086). The crate gives the order,
authorization and challenge primitives, not a renewal manager: the renewal
policy, the DNS record lifecycle, storage and backoff are Quoro's code.

## What the runs showed

1. **Pebble 2.10.1 with pebble-challtestsrv (offline).** `obtain` issued a
   certificate in about 0.5 s; `renew` received an ARI window, Pebble logged the
   new order as a replacement of the old one, and the serial changed.
2. **Let's Encrypt staging with Cloudflare DNS.** The TXT record
   `_acme-challenge.<name>` was visible on public DNS-over-HTTPS about 10 s after
   it was written. A fresh authorization took about 15 s from order to
   certificate; a renewal with `replaces` took about 5 s. The chain is the
   staging hierarchy (4 certificates).
3. **One difference Pebble does not catch.** rcgen's default
   `CertificateParams` subject is `CN=rcgen self signed cert`. Pebble ignores
   it; Let's Encrypt refuses the order at finalize with `rejectedIdentifier`.
   A CSR must carry the name only in the subject alternative name
   (`params.distinguished_name = DistinguishedName::new()`).

## How to run

```text
acme-dns01-spike obtain|renew --state <dir> --domain <name> --directory <url>
    [--root <pem>]                      trust this CA for the ACME directory (Pebble)
    --dns challtestsrv=<url>            Pebble's challenge test server management API
    --dns cloudflare --zone-id <id>     Cloudflare DNS API; the token is read from the
                                        file named by QUORO_SPIKE_CF_TOKEN_FILE
```

Always use the staging directory
(`https://acme-staging-v02.api.letsencrypt.org/directory`) for experiments. The
token needs DNS edit permission on the zone. The program removes the TXT record
it wrote whether the order succeeds or fails.

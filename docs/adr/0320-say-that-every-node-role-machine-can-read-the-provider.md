# ADR 0320: Say that every node-role machine can read the provider credentials and tunnel tokens in the change log, add a rotation procedure, and harden custody after M5b

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0320
- **Tags:** m5b, security, custody, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0094](0094-non-primary-nodes-get-only-a-per-node-tunnel-run-token-from.md)

## Context

Since custody shipped, every machine with the node role holds the change log's encrypted provider credentials, every tunnel run token and a seal of the data-encryption key its root can open. The threat model accepts this (T-NODE-16, T-PRV-06), but ADR 0094 and three bullets of node-agent-protocol.md section 11 still say a node never receives a provider credential, cannot reach another node's tunnel and never gets the audit-copy credential.

## Decision

Say that every node-role machine can read the provider credentials and tunnel tokens in the change log, add a rotation procedure, and harden custody after M5b.

### What is true on main today

Every machine with the `node` role follows the change log, and only those machines do (`node-agent-protocol.md` section 7.1). Since custody shipped (#743), each of them also holds a seal of the data-encryption key that root on that machine can open at any time. So a follower's own disk holds, in a form its root can read:
- `providers.secret_enc`: every provider's account credential. That includes the Cloudflare API token, the backup bucket credential and the audit-copy credential, which is its own `object_store` provider entry;
- `node_tunnels.token_enc`: every machine's tunnel run token, not only its own;
- `webhooks.secret_enc` and every `app_env.value_enc`.

The threat model already accepts this: T-NODE-16 says every promotable machine holds the keys a primary holds (ADR 0078), and T-PRV-06's residual risk says a compromised node-role machine "reads every secret in the log". Four texts still say otherwise:
- ADR 0094, which is binding: "the account API token never leaves the primary";
- section 11 "Provider credentials stay on the primary": "A node never receives a provider's account credential", and a run token "cannot reach another node's tunnel";
- section 11 "Volumes and backups": "never the audit-copy credential";
- section 11 "A compromised node": "Its blast radius is its own workloads".

Section 7.1 "Secrets in the log" adds that a follower "cannot read" the ciphertext "until a promotion opens its seal". That is a policy, not a barrier.

### Options

**A. A text change, plus a rotation procedure (smallest).**
- A new ADR supersedes ADR 0094's wording: the account credential never leaves the machines that can become primary, that is, the `node`-role machines.
- Section 11 and section 7.1 state the exposure plainly.
- The procedure is: when a `node`-role machine is removed or suspected compromised, rotate:
  - every provider credential (`rotateProviderSecret`), the audit-copy credential included;
  - every tunnel (`deleteNodeTunnel` then `setNodeTunnel`, which issues new run tokens);
  - every webhook secret (delete and recreate);
  - the app secrets.
- **Gaps:**
  - The DEK has no rotation operation, and signing-key rotation (#76) is below the cut line.
  - A removed machine keeps the old DEK, so it can still read any old ciphertext it holds. Rotating the credentials makes that old ciphertext worthless.
- **Code:** none required. A panel or API hint after a removal, listing what to rotate, is optional.
- **Residual risk:** one compromised `node`-role machine reads every secret in the cluster.

**B. Sealing per node, instead of under the DEK.**
- On its own this changes nothing. Every `node`-role machine is promotable, so each would get a seal, and the set of readers stays the same.
- It only pays off together with C.
- For tunnel tokens there is a simpler form: stop replicating `node_tunnels.token_enc`. The primary already reads a tunnel's run token from the provider (`GET /accounts/{id}/cfd_tunnel/{id}/token`), so a promoted primary that holds the provider credential can fetch each token again.
  - It needs a contract line, as `repo_key_enc` has, because ADR 0297 says a create carries the whole row. The column is already nullable.
  - Alone, that does not help, because the provider credential would still be readable.
  - With C, D or E, followers stop holding other machines' run tokens.

**C. Seal the cluster keys only to designated promotion candidates.**
- The operator marks some `node`-role machines as candidates (a flag, set like a role).
- Only candidates get seals. The others hold ciphertext and no key, so section 11's claims hold again for them.
- Candidates stay primary-equivalent (T-NODE-16), and A's text applies to them.
- **Contract change:** section 7.2 (who is sealed), 7.3 (refuse a non-candidate) and `Node.custody` (null for a non-candidate).
- **Code:** moderate (the binding filter, the reconcile, the flag operation).
- **Cost:** fewer machines can be promoted. In a three-machine cluster all three are likely candidates, so it gains little there.

**D. Threshold custody (strongest).**
- Each cluster key is split with Shamir's scheme among the voters. Each share is sealed to its voter.
- In its `granted` vote answer (section 8.9), a voter re-seals its share to the candidate's custody key. The candidate opens the keys only with a majority of shares.
- A single compromised machine, node or witness, then reads nothing. Reading the secrets needs a majority of voters to grant a vote, and a voter grants only once it no longer supports a live primary.
- **Contract change, large:**
  - sections 7.2, 8.9 and 11 change;
  - witnesses would hold a share, which ends "a witness holds no seals";
  - the keys are re-split at every voter-set change and every key rotation.
- **Timing:** too large for M5b. Proposed as hardening after it.

**E. Keep the account credentials out of the log (as for `repo_key_enc`).**
- `providers.secret_enc` is not replicated, and neither is `token_enc` (see B). After a promotion, the operator enters the provider credentials again.
- Until then:
  - DNS, ACME, tunnel management, backups and the off-site audit copy pause;
  - nodes keep running their tunnels with the run tokens they already hold. That needs a contract line, so that a desired state from a primary without the token keeps the tunnel.
- This restores ADR 0094 literally. It costs unattended recovery of ingress management, and it leaves a gap in the audit copy until a human acts.

### Recommendation

- **A now,** because the exposure is already accepted in the threat model, and the texts should say what the code does before the release that carries the seals.
- **C or D as hardening after M5b,** together with B's "stop replicating `token_enc`". D is the one that brings back section 11's single-machine blast radius without reducing the number of promotable machines.

The maintainer chose on 2026-11-10: option A now (the contract and a superseding ADR say what the code does, plus the rotation procedure; no code in M5b), and hardening after M5b. For the hardening the maintainer was shown threshold custody (option D, with tunnel run tokens no longer replicated) as the target; it gets its own proposal and design after M5b. Options C and E are not adopted.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0094](0094-non-primary-nodes-get-only-a-per-node-tunnel-run-token-from.md): where a provider's account credential lives: on every machine with the `node` role, each of which can read it from the change log, not only on the primary. The rest of [ADR 0094](0094-non-primary-nodes-get-only-a-per-node-tunnel-run-token-from.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0320 in the project decision ledger. Do not edit by hand; changes go through a new decision.

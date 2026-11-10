# ADR 0314: A returning source deletes its file only when it matches its base, not the snapshot it was shipping

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0314
- **Tags:** m5b, warm, quarantine, contract-gap
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0215](0215-a-returning-source-keeps-divergent-writes-as-a-reflink.md)

## Context

ADR 0215 and section 9 step 1 delete the returning source's live file when it shows no change against either kept snapshot, its base or the one it was shipping. When the target never completed the shipped snapshot, a file equal to that snapshot still holds writes the target lacks, and deleting it drops them, against ADR 0022.

## Decision

A returning source deletes its file only when it matches its base, not the snapshot it was shipping.

Case. S ships to warm node T. S's base is seq N, which T completed and acknowledged. S takes N+1 and starts shipping it; T never completes it (the cut comes first). The app writes nothing after N+1 was taken. A failover starts the app on T from N, T's last complete copy. S returns. Its live file equals N+1 (no change) but differs from N by the writes between N and N+1. Section 9 step 1 ('If the file has no changed range against one of them, it holds no divergent write, and it deletes the file') deletes it. Step 3 then resyncs S from candidate N, the copy T ships from, and deletes the other candidate, N+1. The N to N+1 writes are then on no node: they were divergent writes, and ADR 0022 and ADR 0176 say those are never dropped. The rule is right only when T completed N+1, which S cannot tell (a lost acknowledgement looks the same). A file equal to the base N is always safe to delete: no block changed since N, so every later snapshot, and T's copy whichever it is, is the same data. Options. (A) Delete only when the file shows no change against the base, the snapshot T acknowledged; otherwise quarantine. Never drops a write; may quarantine a file T in fact holds (when T completed N+1 but the acknowledgement was lost), which a human then deletes. (B) Defer the compare until T's first stream arrives and names its base_seq; delete only when the file shows no change against S's kept snapshot of that seq; with no base_seq (a full stream) or no such snapshot, quarantine. Exact, but the hand-over waits for T's stream, and section 9 says the compare comes before the node accepts a stream (it still would: the decision is made at the stream's header, before staging). (C) Keep ADR 0215 as written and accept the loss. The maintainer chose option (A) (2026-11-10): delete only when the file shows no change against the base the target acknowledged; otherwise quarantine.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0215](0215-a-returning-source-keeps-divergent-writes-as-a-reflink.md): when a returning source deletes its file: only when it shows no change against the base the target acknowledged, and otherwise it quarantines it. The rest of [ADR 0215](0215-a-returning-source-keeps-divergent-writes-as-a-reflink.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0314 in the project decision ledger. Do not edit by hand; changes go through a new decision.

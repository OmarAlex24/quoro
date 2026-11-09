# ADR 0302: Change log: append refuses an entry a follower could not receive (one that alone exceeds a log_entries frame, or whose stored payload has no RFC 8785 form)

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0302
- **Tags:** m5b, contracts, changelog, follow
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md)

## Context

The contract has no rule for a change-log entry that cannot be sent to a follower. Two cases exist. (1) An entry whose serialized form alone exceeds the 1 MiB message limit of section 3 cannot travel in any log_entries message. (2) An entry whose payload holds a whole-number float above 2^53: append hashes the float form, the stored text reads back as an integer above 2^53, and RFC 8785 (quoro_proto::jcs) refuses it, so the primary cannot rebuild the entry the node must verify. No payload holds either today: payloads are rows of at most a few KiB, and the only floats are vulnerability scores rounded to one decimal. The primary's follow (#661) treats both cases conservatively: it logs an error with the seq and stops following on that session, since sending it would only draw bad_message every few seconds. Every follower then stops at that seq on every session, so no machine can be promoted with a complete log, and the only signal is a log line and a growing log_lag. The independent review of #661 raised it; it is a decision, not an implementation detail.

## Decision

Change log: append refuses an entry a follower could not receive (one that alone exceeds a log_entries frame, or whose stored payload has no RFC 8785 form).

Option A, which the maintainer chose: changelog::append refuses such an entry, so it never enters the log. append hashes the entry as it will be read back (payload parsed from its stored text), fails with an error when that form has no RFC 8785 form, and fails when the entry's serialized form exceeds the frame budget a single log_entries message allows (1 MiB minus the message envelope). The write that carries it fails like any other write error. The contract gains one sentence in section 7.1 (or 5.14): the primary never writes a change-log entry that cannot be sent alone in one log_entries message and verified by a node. Cost: one contract change and a small code change in changelog.rs; the follow code keeps its stop as a guard that should never fire. Risk: a write that would have succeeded now fails, which no current writer comes near.

Option B: keep the behaviour #661 ships (stop following on that session, log an error) and state it in section 7.1, adding a visible signal (an event such as node.follow_blocked, or a field on Node). Cost: a contract change for the signal plus code; the cluster still cannot promote with a complete log once such an entry exists.

Option C: leave the contract silent and the code as shipped. Cost: the gap stays undocumented.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md): which entries the primary may write: only one that can be sent alone in one log_entries message and verified by a node. The rest of [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0302 in the project decision ledger. Do not edit by hand; changes go through a new decision.

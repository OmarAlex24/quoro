# ADR 0284: Ignore RUSTSEC-2026-0459 (bundled SQLite before 3.54 via libsqlite3-sys 0.30) in cargo-deny until sqlx allows libsqlite3-sys 0.39 or later

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0284
- **Tags:** governance, dependencies, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer, 2026-11-09. The advisory needs a database file and its -wal written by a less trusted party. Quoro's SQLite opens only files Quoro writes in its own private data directory; a control-plane restore starts from an authenticated backup and removes stale -wal and -shm files before it opens the database; the image inventory reads rpmdb.sqlite with its own bounded reader, not the SQLite library. No sqlx release allows libsqlite3-sys 0.39 or later (sqlx 0.9.0 caps it below 0.38), so the fix cannot be taken now, and the advisory fails cargo-deny on every pull request.

## Decision

Ignore RUSTSEC-2026-0459 (bundled SQLite before 3.54 via libsqlite3-sys 0.30) in cargo-deny until sqlx allows libsqlite3-sys 0.39 or later.

deny.toml lists RUSTSEC-2026-0459 under [advisories] ignore, with this decision, the reason and the date, as its comment requires. A tracking issue removes the ignore and updates sqlx once a sqlx release allows libsqlite3-sys 0.39 or later. Any new code that opens an SQLite file another party could have written, together with its -wal, must not ship while the ignore stands. RUSTSEC-2026-0462 (rmcp before 3.9.0) is not ignored: rmcp moves to 3.9 in the same change.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0284 in the project decision ledger. Do not edit by hand; changes go through a new decision.

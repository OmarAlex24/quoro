-- Migration 0013: the unsettled mark of an expiring lease after a restore or a
-- forced recovery (ADR 0261, which completes ADR 0255).
--
-- leases.unsettled is 1 while the row may lack the write that ends an
-- expiring lease's renewals, or the writes after it, and 0 otherwise. The
-- first change-log write of any restore, plain or with --accept-loss
-- (backup-format.md section 8.2), and of any `quoro cluster promote
-- --accept-loss` (node-agent-protocol.md section 7.3) sets it on every row
-- whose mode is 'expiring'. The column is part of the row, so the mark is in
-- the change log for every later primary. An unsettled row moves only as a
-- local lease does: never on its gate alone. Only the holder's report clears
-- it (node-agent-protocol.md section 8.4, "Unsettled rows"): a report of the
-- lease as local writes the row local, and a report of it as expiring
-- settles the lease, whose row write clears the mark. Existing rows start
-- with 0.
--
-- A promotion that rebuilds the database from a log written before this
-- migration finds no unsettled field in the lease payloads, and the rows take
-- the default 0 (section 7.3).

ALTER TABLE leases ADD COLUMN unsettled INTEGER NOT NULL DEFAULT 0 CHECK (unsettled IN (0, 1));

UPDATE meta SET value = '14' WHERE key = 'schema_version';

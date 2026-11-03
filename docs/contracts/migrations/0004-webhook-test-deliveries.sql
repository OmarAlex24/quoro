-- Migration 0004: test deliveries are their own rows (ADR 0104).
--
-- POST /api/v1/webhooks/{webhook_id}/test replays the newest event as a test
-- delivery. With UNIQUE (webhook_id, event_id, attempt) a test row and a real
-- row for the same webhook and event fought for the same attempt numbers, so
-- a test after a real delivery failed, and a test before the dispatcher
-- reached the event blocked the real delivery. `test` marks a test row
-- (attempt 1, never retried); uniqueness of (webhook_id, event_id, attempt)
-- now applies to real deliveries only, so a test delivery never collides with
-- a real one and any number of test deliveries may replay the same event.
--
-- SQLite cannot drop a table constraint, so the table is rebuilt. Nothing
-- references webhook_deliveries, and the table is local (not replicated), so
-- no change-log entry is written.

CREATE TABLE webhook_deliveries_new (
    id              TEXT PRIMARY KEY,
    webhook_id      TEXT NOT NULL REFERENCES webhooks (id) ON DELETE CASCADE,
    event_id        TEXT NOT NULL REFERENCES events (id),
    attempt         INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 10),
    status          TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    response_code   INTEGER,
    error           TEXT,
    next_attempt_at TEXT,
    created_at      TEXT NOT NULL,
    test            INTEGER NOT NULL DEFAULT 0 CHECK (test IN (0, 1)),
    CHECK (test = 0 OR (attempt = 1 AND next_attempt_at IS NULL))
) STRICT;

INSERT INTO webhook_deliveries_new
    (id, webhook_id, event_id, attempt, status, response_code, error, next_attempt_at, created_at, test)
SELECT id, webhook_id, event_id, attempt, status, response_code, error, next_attempt_at, created_at, 0
FROM webhook_deliveries;

DROP TABLE webhook_deliveries;

ALTER TABLE webhook_deliveries_new RENAME TO webhook_deliveries;

CREATE UNIQUE INDEX idx_webhook_deliveries_attempt
    ON webhook_deliveries (webhook_id, event_id, attempt) WHERE test = 0;
CREATE INDEX idx_webhook_deliveries_status ON webhook_deliveries (status, next_attempt_at);
CREATE INDEX idx_webhook_deliveries_event ON webhook_deliveries (event_id);

UPDATE meta SET value = '5' WHERE key = 'schema_version';

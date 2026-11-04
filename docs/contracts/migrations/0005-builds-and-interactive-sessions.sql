-- Migration 0005: Git sources and builds (ADR 0117, ADR 0118, ADR 0119) and
-- interactive sessions (ADR 0114, ADR 0115).
--
-- apps.source holds an app's Git source as JSON (openapi.yaml `AppSource`):
-- {kind, repo_url, ref, dockerfile_path, context_dir, target, build_args,
-- secrets}. NULL means the app deploys images given by reference. Build args
-- are stored as sent: they are not secrets (they end up in the image history).
-- Build secrets are names of the app's environment variables (app_env); their
-- values stay encrypted there and reach the node only inside the signed
-- desired state. For an app with a source, apps.image holds the reference of
-- its last successful build, `build.quoro.invalid/<app_id>:<build_id>`
-- (README.md "Built images"), or '' before the first one.
--
-- builds is a replicated state table, a projection of the change log like
-- deployments. It holds the status and, for a failure, the failing step with a
-- few excerpt lines, so the panel shows a failure while the node is down; the
-- full build log stays on the node (ADR 0014, ADR 0119). source is the snapshot
-- of the app's source the build started from (same JSON shape as apps.source),
-- so a later change to the app does not rewrite what an old build built.
--
-- interactive_sessions is LOCAL: not replicated. It records every terminal and
-- console session the primary opened, so that each one gets exactly one
-- session.end audit record even when the primary restarts while it is open:
-- at start-up the primary closes every row with ended_at NULL with
-- close_reason primary_restarted (audit-log-format.md section 3.3). The
-- single-use ticket is never stored; it lives in the primary's memory for its
-- 30 s. opener_* and the accountability columns copy what the open's audit
-- record named, so the end record repeats them exactly.

ALTER TABLE apps ADD COLUMN source TEXT CHECK (source IS NULL OR json_valid(source));

CREATE TABLE builds (
    id              TEXT PRIMARY KEY,
    app_id          TEXT NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    node_id         TEXT NOT NULL REFERENCES nodes (id),
    source          TEXT NOT NULL CHECK (json_valid(source)),          -- AppSource snapshot
    status          TEXT NOT NULL CHECK (status IN ('queued', 'preparing', 'building', 'exporting', 'importing', 'succeeded', 'failed', 'cancelled')),
    "commit"        TEXT CHECK ("commit" IS NULL OR (length("commit") = 40 AND "commit" NOT GLOB '*[^0-9a-f]*')),
    image           TEXT,                                              -- build.quoro.invalid/<app_id>:<build_id>, on success
    image_digest    TEXT,                                              -- manifest digest, on success
    failed_phase    TEXT CHECK (failed_phase IN ('preparing', 'building', 'exporting', 'importing') OR failed_phase IS NULL),
    error           TEXT,
    failed_step     TEXT CHECK (failed_step IS NULL OR json_valid(failed_step)),  -- {vertex, name, error, dockerfile_line, excerpt[]}
    deployment_id   TEXT REFERENCES deployments (id),                  -- the deployment a success started
    "trigger"       TEXT NOT NULL CHECK ("trigger" IN ('api')),
    created_by_type TEXT NOT NULL CHECK (created_by_type IN ('user', 'agent', 'system')),
    created_by_id   TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    CHECK ((status = 'failed') = (failed_phase IS NOT NULL)),
    CHECK (status = 'succeeded' OR (image IS NULL AND image_digest IS NULL))
) STRICT;

CREATE INDEX idx_builds_app ON builds (app_id, created_at);
CREATE INDEX idx_builds_node_status ON builds (node_id, status);

CREATE TABLE interactive_sessions (
    id                TEXT PRIMARY KEY,                                -- trm_
    kind              TEXT NOT NULL CHECK (kind IN ('terminal', 'console')),
    vm_id             TEXT NOT NULL,                                   -- no foreign key: the row outlives the VM
    app_id            TEXT,
    project_id        TEXT NOT NULL,
    node_id           TEXT NOT NULL,
    scope             TEXT NOT NULL CHECK (scope IN ('app:terminal', 'vm:console')),
    actor_type        TEXT NOT NULL CHECK (actor_type IN ('user', 'agent')),
    actor_id          TEXT NOT NULL,
    agent_model       TEXT,
    on_behalf_of      TEXT,
    channel           TEXT NOT NULL CHECK (channel IN ('api', 'panel', 'mcp', 'cli')),
    grant_id          TEXT,
    target_type       TEXT NOT NULL CHECK (target_type IN ('app', 'vm')),
    target_id         TEXT NOT NULL,
    open_audit_seq    INTEGER NOT NULL,                                -- seq of the open's audit record
    opened_at         TEXT NOT NULL,
    connected_at      TEXT,
    ended_at          TEXT,
    end_audit_seq     INTEGER,                                         -- seq of the session.end record
    CHECK ((ended_at IS NULL) = (end_audit_seq IS NULL))
) STRICT;

CREATE INDEX idx_interactive_sessions_open ON interactive_sessions (ended_at) WHERE ended_at IS NULL;

UPDATE meta SET value = '6' WHERE key = 'schema_version';

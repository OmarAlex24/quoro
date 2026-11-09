-- Quoro control-plane schema (SQLite).
--
-- Purpose: the persistent state of one Quoro control plane. It holds the numbered
-- change log, the audit log, the event log and the state tables that are derived
-- from them. Column names and types follow the shared entity model used by
-- openapi.yaml and events.schema.json, so a row, an API object and an event
-- payload use the same field names. A column with no field of its own in the
-- API object travels in the object's change-log payload under the column's
-- name, beside the object's fields, so a database rebuilt from the log keeps
-- it (ADR 0286). An app's warm settings are the case: apps.warm_node_id,
-- apps.warm_node_pinned and apps.warm_interval_s (migration 0011) appear in
-- the API only inside `App.warm`. A field the primary derives on read is null
-- in the payload or absent from it: `App.warm` is null. What the primary
-- keeps only in memory, such as the progress of a warm copy, stays out of the
-- payload and the hash chain.
--
-- D-0015: SQLite first; state is stored as a numbered change log, and the state
-- tables (nodes, projects, vms, apps, deployments, domains, volumes, leases,
-- users, agents, api_tokens, agent_keys, grants, approvals, incidents, webhooks,
-- providers)
-- are a projection of that log. They can be dropped and rebuilt by replaying
-- change_log from seq 1 (D-0013). Only the projection is queried by the API; the
-- change log is the source of truth.
--
-- Tables marked LOCAL are not replicated through the change log. They hold
-- machine-local secrets, session state or per-node delivery state, so a replica
-- node must not receive them.
--
-- change_seq on audit_log and seq on events are both replica-visible: each audit
-- record is also written as a change-log entry with entity_type 'audit', and each
-- event is written as a change-log entry with entity_type 'event', so a replica
-- node holds both logs (D-0013).
--
-- This file is the M0 contract. It is frozen at the end of M0 (D-0033, D-0036).
-- After the freeze a schema change is a standalone pull request that adds a new
-- numbered migration; it never edits this file in place.
--
-- Only hashes of secrets are stored, never the secrets themselves:
--   users.password_hash   Argon2id hash string (never returned by the API)
--   api_tokens.token_hash / sessions.token_hash / agent_keys.key_hash /
--   access_tokens.token_hash / node_join_tokens.token_hash
--                         SHA-256 hex of the secret, shown once on creation
--
-- Encrypted columns hold the cluster data-encryption key id, a 24-byte nonce and
-- the ciphertext with its tag, concatenated in that order:
--   app_env.value_enc     encrypted variable value
--   webhooks.secret_enc   encrypted webhook signing secret
--   providers.secret_enc  encrypted external-provider credentials
--
-- Keys (D-0050). Desired state and lease messages are signed with the cluster
-- signing key (Ed25519); nodes pin its public key at enrollment. Data at rest
-- uses a separate cluster data-encryption key (DEK), 256 bits, with
-- XChaCha20-Poly1305; AES-256-GCM is not used. Every *_enc BLOB stores the DEK
-- key id, the 24-byte nonce and the ciphertext with its tag, so rotating the DEK
-- re-encrypts rows lazily under a new key id. Each node also has a node-local
-- key that encrypts the node's local copy of desired-state secrets at rest. All
-- keys are generated at the primary's first start, before /setup, never derived
-- from a password, and stored as files mode 0600 owned by the quoro user in the
-- data directory /var/lib/quoro, outside the SQLite file; the operator must back
-- them up separately from the database, and losing the DEK loses every secret.
-- The signing key rotates with a key-change message signed by the old key, which
-- nodes accept. Custody of the keys when a replica is promoted is decided
-- (ADR 0078, read by ADR 0180): the keys are sealed to a custody key of each
-- node with the `node` role, never to its node-local key. The seals and the
-- promotion itself ship in M5b (D-0067).
--
-- **Hypothesis**: the SQLite version bundled with the control-plane image
-- supports STRICT tables and the JSON functions used here (json_valid,
-- json_extract). To be confirmed when the SQLite version is pinned.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- meta: cluster identity and format version. Not part of the projection.
-- ---------------------------------------------------------------------------

-- Keys: schema_version ('1', written below), cluster_id (cls_), cluster_name,
-- term (current term, 1 until the first promotion, ADR 0169), created_at and
-- webhook_cursor (the seq of the last event the local webhook dispatcher has
-- dispatched; written by the dispatcher, local like webhook_deliveries, ADR 0140).
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

INSERT INTO meta (key, value) VALUES ('schema_version', '1');

-- ---------------------------------------------------------------------------
-- change_log: the numbered, append-only, hash-chained log of state changes.
-- Replicas ask for everything after N and verify the chain (D-0013).
-- ---------------------------------------------------------------------------

-- seq is assigned by the primary as max+1 inside the same transaction that
-- writes the row; the triggers below enforce that rule.
-- hash is SHA-256 over the RFC 8785 canonical JSON of the entry without hash.
CREATE TABLE change_log (
    seq         INTEGER PRIMARY KEY,
    term        INTEGER NOT NULL CHECK (term >= 1),
    time        TEXT NOT NULL,
    actor_type  TEXT NOT NULL CHECK (actor_type IN ('user', 'agent', 'node', 'system')),
    actor_id    TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    op          TEXT NOT NULL CHECK (op IN ('create', 'update', 'delete')),
    payload     TEXT NOT NULL CHECK (json_valid(payload)),
    prev_hash   TEXT NOT NULL CHECK (length(prev_hash) = 64 AND prev_hash NOT GLOB '*[^0-9a-f]*'),
    hash        TEXT NOT NULL CHECK (length(hash) = 64 AND hash NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE INDEX idx_change_log_entity ON change_log (entity_type, entity_id);
CREATE INDEX idx_change_log_time ON change_log (time);

CREATE TRIGGER change_log_append_only_update
BEFORE UPDATE ON change_log
BEGIN
    SELECT RAISE(ABORT, 'change_log is append-only');
END;

CREATE TRIGGER change_log_append_only_delete
BEFORE DELETE ON change_log
BEGIN
    SELECT RAISE(ABORT, 'change_log is append-only');
END;

-- seq must be exactly one more than the current maximum (gap-free from 1).
CREATE TRIGGER change_log_seq_contiguous
BEFORE INSERT ON change_log
WHEN NEW.seq <> (SELECT IFNULL(MAX(seq), 0) + 1 FROM change_log)
BEGIN
    SELECT RAISE(ABORT, 'change_log seq must be the previous seq + 1');
END;

-- prev_hash must equal the previous entry's hash; seq 1 starts from 64 zeros.
CREATE TRIGGER change_log_prev_hash_chained
BEFORE INSERT ON change_log
WHEN NEW.prev_hash <> COALESCE(
        (SELECT hash FROM change_log WHERE seq = NEW.seq - 1),
        '0000000000000000000000000000000000000000000000000000000000000000')
BEGIN
    SELECT RAISE(ABORT, 'change_log prev_hash must equal the previous entry hash');
END;

-- ---------------------------------------------------------------------------
-- state tables (projection of change_log)
-- ---------------------------------------------------------------------------

CREATE TABLE nodes (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    roles          TEXT NOT NULL CHECK (json_valid(roles)),          -- array of 'node' | 'witness' | 'edge'
    cp_status      TEXT CHECK (cp_status IN ('primary', 'replica') OR cp_status IS NULL),
    status         TEXT NOT NULL CHECK (status IN ('pending', 'connected', 'disconnected', 'fenced', 'removed')),
    public_key     TEXT NOT NULL,                                    -- base64 Ed25519
    address        TEXT,
    behind_nat     INTEGER NOT NULL CHECK (behind_nat IN (0, 1)),
    labels         TEXT NOT NULL CHECK (json_valid(labels)),
    capacity       TEXT NOT NULL CHECK (json_valid(capacity)),       -- {cpus, memory_mb, disk_gb}
    allocated      TEXT NOT NULL CHECK (json_valid(allocated)),      -- same shape as capacity
    kvm            TEXT NOT NULL CHECK (json_valid(kvm)),            -- {available, kernel, nested?}
    engine_version TEXT,
    agent_version  TEXT,
    desired_rev    INTEGER NOT NULL DEFAULT 0,
    applied_rev    INTEGER NOT NULL DEFAULT 0,
    log_seq        INTEGER NOT NULL DEFAULT 0,                       -- last change-log seq verified by the node
    last_seen_at   TEXT,
    created_at     TEXT NOT NULL
) STRICT;

CREATE INDEX idx_nodes_status ON nodes (status);

-- node_join_tokens: one-time enrollment token. The secret is shown once; only
-- its SHA-256 hash is stored.
CREATE TABLE node_join_tokens (
    id         TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE CHECK (length(token_hash) = 64 AND token_hash NOT GLOB '*[^0-9a-f]*'),
    created_by TEXT NOT NULL REFERENCES users (id),
    expires_at TEXT NOT NULL,
    used_at    TEXT,
    node_id    TEXT REFERENCES nodes (id)
) STRICT;

CREATE TABLE projects (
    id         TEXT PRIMARY KEY,
    slug       TEXT NOT NULL UNIQUE CHECK (length(slug) BETWEEN 1 AND 40 AND slug NOT GLOB '*[^a-z0-9-]*'),
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE apps (
    id                    TEXT PRIMARY KEY,
    project_id            TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    name                  TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 40 AND name NOT GLOB '*[^a-z0-9-]*'),
    stateful              INTEGER NOT NULL DEFAULT 0 CHECK (stateful IN (0, 1)),
    ha_policy             TEXT NOT NULL DEFAULT 'none' CHECK (ha_policy IN ('none', 'cold', 'warm', 'hot')),
    image                 TEXT NOT NULL,
    port                  INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    health_check          TEXT NOT NULL CHECK (json_valid(health_check)),  -- {path, interval_s, timeout_s, failures}
    resources             TEXT NOT NULL CHECK (json_valid(resources)),     -- {vcpus, memory_mb, disk_gb}
    placement             TEXT NOT NULL CHECK (json_valid(placement)),     -- {node_ids: []}
    status                TEXT NOT NULL CHECK (status IN ('deploying', 'running', 'degraded', 'down', 'stopped')),
    current_deployment_id TEXT REFERENCES deployments (id),
    restart_generation    INTEGER NOT NULL DEFAULT 0,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    UNIQUE (project_id, name)
) STRICT;

CREATE INDEX idx_apps_project ON apps (project_id);
CREATE INDEX idx_apps_status ON apps (status);

CREATE TABLE vms (
    id                 TEXT PRIMARY KEY,
    project_id         TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    app_id             TEXT REFERENCES apps (id) ON DELETE SET NULL,
    node_id            TEXT NOT NULL REFERENCES nodes (id),
    name               TEXT NOT NULL,
    vcpus              INTEGER NOT NULL CHECK (vcpus >= 1),
    memory_mb          INTEGER NOT NULL CHECK (memory_mb >= 1),
    disk_gb            INTEGER NOT NULL CHECK (disk_gb >= 1),
    image              TEXT NOT NULL,                                -- OCI reference with digest
    desired_status     TEXT NOT NULL CHECK (desired_status IN ('running', 'stopped')),
    observed_status    TEXT NOT NULL CHECK (observed_status IN ('creating', 'stopped', 'starting', 'running', 'stopping', 'crashed', 'deleted', 'unknown')),
    ip                 TEXT,
    restart_generation INTEGER NOT NULL DEFAULT 0,
    restart_count      INTEGER NOT NULL DEFAULT 0,
    last_exit_code     INTEGER,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
) STRICT;

CREATE INDEX idx_vms_node ON vms (node_id);
CREATE INDEX idx_vms_app ON vms (app_id);
CREATE INDEX idx_vms_project ON vms (project_id);

-- app_env: one row per variable. Value is write-only through the API (names are
-- returned, values never are) and is stored encrypted with the cluster
-- data-encryption key (AEAD).
CREATE TABLE app_env (
    app_id    TEXT NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    name      TEXT NOT NULL,
    value_enc BLOB NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (app_id, name)
) STRICT;

CREATE TABLE deployments (
    id              TEXT PRIMARY KEY,
    app_id          TEXT NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    image           TEXT NOT NULL,                                   -- reference as requested
    image_digest    TEXT,                                            -- resolved digest
    status          TEXT NOT NULL CHECK (status IN ('pending', 'preparing', 'starting', 'succeeded', 'failed', 'superseded')),
    phase           TEXT CHECK (phase IN ('pull', 'rootfs', 'boot', 'health', 'build') OR phase IS NULL),
    error           TEXT,
    "trigger"       TEXT NOT NULL CHECK ("trigger" IN ('api', 'redeploy', 'rollback', 'build')),
    created_by_type TEXT NOT NULL CHECK (created_by_type IN ('user', 'agent', 'system')),
    created_by_id   TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    finished_at     TEXT
) STRICT;

CREATE INDEX idx_deployments_app ON deployments (app_id, created_at);

CREATE TABLE domains (
    id              TEXT PRIMARY KEY,
    app_id          TEXT NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    hostname        TEXT NOT NULL UNIQUE CHECK (length(hostname) BETWEEN 1 AND 253),
    tls_mode        TEXT NOT NULL CHECK (tls_mode IN ('acme', 'tunnel')),
    cert_status     TEXT NOT NULL CHECK (cert_status IN ('pending', 'issued', 'renewing', 'failed', 'not_applicable')),
    cert_expires_at TEXT,
    created_at      TEXT NOT NULL
) STRICT;

CREATE INDEX idx_domains_app ON domains (app_id);

-- volumes: shape only, empty until local volumes ship in M5a (D-0052).
CREATE TABLE volumes (
    id         TEXT PRIMARY KEY,
    app_id     TEXT NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
    node_id    TEXT NOT NULL REFERENCES nodes (id),
    size_gb    INTEGER NOT NULL CHECK (size_gb >= 1),
    mount_path TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

-- leases: replicated. The primary is the lease authority in M0-M5a (D-0019);
-- from M5b it grants and renews expiring leases only while a majority of
-- voters supports it (ADR 0169). Renewals write nothing (migration 0010),
-- except the answer to a converted lease's first renewal (below).
-- Two modes (D-0047): `local` for HA policy `none`/`cold` and `expiring` for
-- `warm`/`hot`. When the policy becomes `warm`, mode stays `local` until the
-- primary answers the holder's first renewal after the conversion grant; that
-- answer writes `expiring` before it is sent (ADR 0218, ADR 0244). Before it
-- sends the conversion grant, the primary raises duration_ms to at least that
-- grant's duration (ADR 0254), and it sends a longer grant only once that raise
-- is replicated (ADR 0268). A lease changes mode only with the same token
-- (ADR 0258). The node-side states `held`, `uncertain`, `expired` and
-- `revoked` are fixed in node-agent-protocol.md.
CREATE TABLE leases (
    id             TEXT PRIMARY KEY,
    resource_type  TEXT NOT NULL CHECK (resource_type IN ('app', 'primary')),
    resource_id    TEXT NOT NULL,
    holder_node_id TEXT NOT NULL REFERENCES nodes (id),
    mode           TEXT NOT NULL CHECK (mode IN ('local', 'expiring')),
    fencing_token  INTEGER NOT NULL CHECK (fencing_token >= 1),
    duration_ms    INTEGER CHECK (duration_ms >= 1),
    granted_at     TEXT NOT NULL,
    renewed_at     TEXT,
    expires_at     TEXT,
    revoked_at     TEXT,
    UNIQUE (resource_type, resource_id)
) STRICT;

CREATE INDEX idx_leases_holder ON leases (holder_node_id);

CREATE TABLE users (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('owner', 'operator', 'viewer')),
    password_hash TEXT NOT NULL,
    disabled      INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1)),
    created_at    TEXT NOT NULL
) STRICT;

CREATE TABLE api_tokens (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    token_hash   TEXT NOT NULL UNIQUE CHECK (length(token_hash) = 64 AND token_hash NOT GLOB '*[^0-9a-f]*'),
    expires_at   TEXT,
    last_used_at TEXT,
    created_at   TEXT NOT NULL,
    revoked_at   TEXT
) STRICT;

-- LOCAL: not replicated. Browser session state is machine-local and churns on
-- every login; a replica node has no use for another node's sessions.
CREATE TABLE sessions (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE CHECK (length(token_hash) = 64 AND token_hash NOT GLOB '*[^0-9a-f]*'),
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE INDEX idx_sessions_user ON sessions (user_id);

CREATE TABLE agents (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    model         TEXT NOT NULL,
    owner_user_id TEXT NOT NULL REFERENCES users (id),
    description   TEXT,
    disabled      INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1)),
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
) STRICT;

CREATE INDEX idx_agents_owner ON agents (owner_user_id);

CREATE TABLE agent_keys (
    id           TEXT PRIMARY KEY,
    agent_id     TEXT NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    key_hash     TEXT NOT NULL UNIQUE CHECK (length(key_hash) = 64 AND key_hash NOT GLOB '*[^0-9a-f]*'),
    created_at   TEXT NOT NULL,
    revoked_at   TEXT,
    last_used_at TEXT
) STRICT;

CREATE INDEX idx_agent_keys_agent ON agent_keys (agent_id);

-- LOCAL: not replicated. Access tokens are short-lived (5 minutes) secrets; they
-- are minted and redeemed on the node that serves the agent's request, and
-- replicating them would copy live credentials onto every node.
CREATE TABLE access_tokens (
    id         TEXT PRIMARY KEY,
    agent_id   TEXT NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    key_id     TEXT NOT NULL REFERENCES agent_keys (id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE CHECK (length(token_hash) = 64 AND token_hash NOT GLOB '*[^0-9a-f]*'),
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE INDEX idx_access_tokens_agent ON access_tokens (agent_id);

-- grants: scope + selector + mandatory TTL (D-0030). Revoking a grant locks the
-- agent out within seconds, so status and revoked_at are read on every request.
CREATE TABLE grants (
    id              TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK (kind IN ('standard', 'incident', 'approval')),
    scopes          TEXT NOT NULL CHECK (json_valid(scopes)),        -- array of scope strings
    selector        TEXT NOT NULL CHECK (json_valid(selector)),      -- selector object
    expires_at      TEXT NOT NULL,
    max_uses        INTEGER CHECK (max_uses >= 1),
    uses            INTEGER NOT NULL DEFAULT 0 CHECK (uses >= 0),
    status          TEXT NOT NULL CHECK (status IN ('active', 'expired', 'revoked', 'exhausted')),
    incident_id     TEXT REFERENCES incidents (id),
    approval_id     TEXT REFERENCES approvals (id),
    on_behalf_of    TEXT NOT NULL REFERENCES users (id),
    created_by_type TEXT NOT NULL CHECK (created_by_type IN ('user', 'system')),
    created_by_id   TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    revoked_at      TEXT,
    revoked_by      TEXT REFERENCES users (id)
) STRICT;

CREATE INDEX idx_grants_agent ON grants (agent_id, status);
CREATE INDEX idx_grants_status ON grants (status);
CREATE INDEX idx_grants_expires ON grants (expires_at);

CREATE TABLE approvals (
    id            TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    scope         TEXT NOT NULL,
    target        TEXT NOT NULL CHECK (json_valid(target)),         -- {type, id}
    operation     TEXT NOT NULL,
    params        TEXT NOT NULL CHECK (json_valid(params)),
    params_hash   TEXT NOT NULL CHECK (length(params_hash) = 64 AND params_hash NOT GLOB '*[^0-9a-f]*'),
    justification TEXT NOT NULL,
    incident_id   TEXT REFERENCES incidents (id),
    status        TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'denied', 'expired')),
    expires_at    TEXT NOT NULL,
    decided_by    TEXT REFERENCES users (id),
    decided_at    TEXT,
    decision_note TEXT,
    grant_id      TEXT REFERENCES grants (id),
    created_at    TEXT NOT NULL
) STRICT;

CREATE INDEX idx_approvals_status ON approvals (status, expires_at);
CREATE INDEX idx_approvals_agent ON approvals (agent_id);

CREATE TABLE incidents (
    id              TEXT PRIMARY KEY,
    status          TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    title           TEXT NOT NULL,
    trigger_event_id TEXT NOT NULL REFERENCES events (id),
    subject         TEXT NOT NULL CHECK (json_valid(subject)),       -- {type, id}
    project_id      TEXT REFERENCES projects (id),
    opened_at       TEXT NOT NULL,
    resolved_at     TEXT,
    resolved_by_type TEXT CHECK (resolved_by_type IN ('user', 'agent', 'system') OR resolved_by_type IS NULL),
    resolved_by_id  TEXT,
    resolution_note TEXT
) STRICT;

CREATE INDEX idx_incidents_status ON incidents (status, opened_at);

-- ---------------------------------------------------------------------------
-- events: the event log behind GET /api/v1/events and the SSE stream.
-- Append-only. Retention pruning is a later, explicit migration, never an
-- UPDATE or DELETE from application code.
-- ---------------------------------------------------------------------------

CREATE TABLE events (
    id          TEXT PRIMARY KEY,
    seq         INTEGER NOT NULL UNIQUE CHECK (seq >= 1),
    type        TEXT NOT NULL,
    time        TEXT NOT NULL,
    source      TEXT NOT NULL CHECK (json_valid(source)),            -- {kind, node_id?}
    subject     TEXT NOT NULL CHECK (json_valid(subject)),           -- {type, id}
    project_id  TEXT REFERENCES projects (id),
    incident_id TEXT REFERENCES incidents (id),
    request_id  TEXT,
    actor       TEXT CHECK (actor IS NULL OR json_valid(actor)),     -- {type, id}
    data        TEXT NOT NULL CHECK (json_valid(data))
) STRICT;

CREATE INDEX idx_events_type ON events (type, seq);
CREATE INDEX idx_events_subject ON events (json_extract(subject, '$.id'), seq);
CREATE INDEX idx_events_incident ON events (incident_id);
CREATE INDEX idx_events_project ON events (project_id);

CREATE TRIGGER events_append_only_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events is append-only');
END;

CREATE TRIGGER events_append_only_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events is append-only');
END;

-- ---------------------------------------------------------------------------
-- webhooks and deliveries
-- ---------------------------------------------------------------------------

CREATE TABLE webhooks (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL CHECK (url LIKE 'https://%' OR url LIKE 'http://localhost%'),
    event_types TEXT NOT NULL CHECK (json_valid(event_types)),       -- array; empty = all
    selector    TEXT NOT NULL CHECK (json_valid(selector)),
    secret_enc  BLOB NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    wake        TEXT CHECK (wake IS NULL OR json_valid(wake)),
    created_by  TEXT NOT NULL REFERENCES users (id),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
) STRICT;

-- LOCAL: not replicated. A delivery attempt is per-node state: it records what
-- this control plane sent and when it will retry, so replicating it would make
-- two nodes deliver the same event.
CREATE TABLE webhook_deliveries (
    id              TEXT PRIMARY KEY,
    webhook_id      TEXT NOT NULL REFERENCES webhooks (id) ON DELETE CASCADE,
    event_id        TEXT NOT NULL REFERENCES events (id),
    attempt         INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 10),
    status          TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    response_code   INTEGER,
    error           TEXT,
    next_attempt_at TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE (webhook_id, event_id, attempt)
) STRICT;

CREATE INDEX idx_webhook_deliveries_status ON webhook_deliveries (status, next_attempt_at);
CREATE INDEX idx_webhook_deliveries_event ON webhook_deliveries (event_id);

-- providers: credentials of external providers (D-0025, D-0051). Replicated
-- through the change log like the other state tables.
-- `kind` is the provider family: `dns`, `tunnel`, `lb` or `object_store`;
-- `config_json` holds the non-secret settings. `secret_enc` is encrypted with
-- the cluster DEK (key id || nonce || ciphertext), is never returned by the API
-- and is redacted from audit params under the `secret` key. The credential for
-- the off-site audit-log copy is a separate `object_store` row whose key is
-- put-only (audit-log-format.md section 8).
CREATE TABLE providers (
    id          TEXT PRIMARY KEY,   -- prv_
    kind        TEXT NOT NULL CHECK (kind IN ('dns', 'tunnel', 'lb', 'object_store')),
    name        TEXT NOT NULL UNIQUE,
    config_json TEXT NOT NULL CHECK (json_valid(config_json)),
    secret_enc  BLOB NOT NULL,
    created_by  TEXT NOT NULL REFERENCES users (id),
    created_at  TEXT NOT NULL,
    rotated_at  TEXT
) STRICT;

CREATE INDEX idx_providers_kind ON providers (kind);

-- ---------------------------------------------------------------------------
-- audit_log: append-only, hash-chained record of every action (D-0031).
-- It has its own chain, separate from change_log. Each audit record is also
-- written as a change-log entry with entity_type 'audit', so replica nodes hold
-- it through the same replication path (D-0013).
-- ---------------------------------------------------------------------------

CREATE TABLE audit_log (
    seq             INTEGER PRIMARY KEY,
    time            TEXT NOT NULL,
    actor_type      TEXT NOT NULL CHECK (actor_type IN ('user', 'agent', 'node', 'system')),
    actor_id        TEXT NOT NULL,
    agent_model     TEXT,
    on_behalf_of    TEXT REFERENCES users (id),
    channel         TEXT NOT NULL CHECK (channel IN ('api', 'panel', 'mcp', 'cli', 'system')),
    operation       TEXT NOT NULL,
    scope           TEXT,
    target_type     TEXT,
    target_id       TEXT,
    project_id      TEXT REFERENCES projects (id),
    params          TEXT NOT NULL CHECK (json_valid(params)),
    justification   TEXT,
    trigger_event_id TEXT,
    incident_id     TEXT,
    grant_id        TEXT,
    approval_id     TEXT,
    request_id      TEXT NOT NULL,
    result          TEXT NOT NULL CHECK (result IN ('ok', 'refused', 'error')),
    result_detail   TEXT,
    change_seq      INTEGER,
    prev_hash       TEXT NOT NULL CHECK (length(prev_hash) = 64 AND prev_hash NOT GLOB '*[^0-9a-f]*'),
    hash            TEXT NOT NULL CHECK (length(hash) = 64 AND hash NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE INDEX idx_audit_actor ON audit_log (actor_id, seq);
CREATE INDEX idx_audit_target ON audit_log (target_id, seq);
CREATE INDEX idx_audit_incident ON audit_log (incident_id, seq);
CREATE INDEX idx_audit_time ON audit_log (time);

CREATE TRIGGER audit_log_append_only_update
BEFORE UPDATE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only');
END;

CREATE TRIGGER audit_log_append_only_delete
BEFORE DELETE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only');
END;

CREATE TRIGGER audit_log_seq_contiguous
BEFORE INSERT ON audit_log
WHEN NEW.seq <> (SELECT IFNULL(MAX(seq), 0) + 1 FROM audit_log)
BEGIN
    SELECT RAISE(ABORT, 'audit_log seq must be the previous seq + 1');
END;

CREATE TRIGGER audit_log_prev_hash_chained
BEFORE INSERT ON audit_log
WHEN NEW.prev_hash <> COALESCE(
        (SELECT hash FROM audit_log WHERE seq = NEW.seq - 1),
        '0000000000000000000000000000000000000000000000000000000000000000')
BEGIN
    SELECT RAISE(ABORT, 'audit_log prev_hash must equal the previous record hash');
END;

-- audit_exports: shape only (off-site copy, D-0031). The off-site copy ships in
-- M5a with the S3-compatible path (D-0053).
-- Object-lock support per provider is unverified (Hypothesis) until the research
-- task before M5a and the per-provider probe.
-- The object_lock column is set to 1 only after a successful object-lock probe
-- on the bucket (audit-log-format.md section 8.2); a failed or skipped probe
-- leaves it 0.
CREATE TABLE audit_exports (
    id                TEXT PRIMARY KEY,
    target            TEXT NOT NULL,
    last_exported_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_exported_seq >= 0),
    object_lock       INTEGER NOT NULL DEFAULT 0 CHECK (object_lock IN (0, 1)),
    updated_at        TEXT NOT NULL
) STRICT;

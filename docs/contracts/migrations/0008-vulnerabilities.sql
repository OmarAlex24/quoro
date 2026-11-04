-- Migration 0008: vulnerability tracking (ADR 0138, ADR 0150, ADR 0151) and
-- builds that refresh their base images (ADR 0150, ADR 0155).
--
-- osv_mirror is LOCAL: not replicated. One row per OSV ecosystem the primary
-- holds, with the age of its data. The primary fetches an ecosystem the first
-- time an image inventory needs it (plus `Linux`, for kernels), refreshes it
-- daily from that ecosystem's modified_id.csv, and records an offline import
-- (`quoro vuln import`) the same way. data_modified_at is the newest record
-- modification time in the data the primary holds, which is what the API shows
-- as the data's age; refreshed_at is when the primary last brought the
-- ecosystem up to date, and checked_at when it last tried. The records
-- themselves, and the image inventories nodes send (node-agent-protocol.md
-- section 5.26), are not in this database: they live in a separate cache file
-- in the primary's data directory, which is described in
-- node-agent-protocol.md section 6.11, never replicated, and left out of the
-- control-plane backups (ADR 0148) because it is large and can be fetched
-- again. When that file is missing, the primary empties osv_mirror and fetches
-- again; a new primary starts with an empty osv_mirror.
--
-- vulnerability_findings is replicated, a projection of the change log. One
-- row per vulnerability found in one package of one subject:
--   * `app`: a package of the image an app currently runs (app_id and
--     image_digest set);
--   * `quoro`: a component of the running Quoro release, from its embedded
--     SBOM (component set: a crate, the guest kernel, Cloud Hypervisor,
--     cloudflared, the BuildKit image);
--   * `host_kernel`: a node's host kernel, matched by upstream version
--     (node_id set).
-- vuln_id is the OSV id (for example `CVE-...`, `DSA-...`, `GHSA-...`), aliases
-- its other ids as a JSON array. severity is derived from the CVSS score
-- (critical 9.0 or more, high 7.0, medium 4.0, low above 0) or `unknown` when
-- the record carries none. kev is 1 when the id or an alias is in CISA's Known
-- Exploited Vulnerabilities catalog; epss is the EPSS probability when the
-- primary holds it. A row is written when a finding appears, changes or is
-- resolved, never on a refresh that changes nothing, so the change log does
-- not grow with every daily refresh. resolved_at and resolved_reason are set
-- when the finding no longer applies: the app runs another image without it
-- (`image_changed`), the data no longer matches (`data_changed`, for example a
-- record withdrawn or a range corrected), the app was deleted (`app_deleted`),
-- the node was removed (`node_removed`) or Quoro was upgraded past it
-- (`upgraded`). A finding that comes back is a new row.
--
-- builds.refresh_base records whether the build resolved every base image
-- again instead of using the builder's cache (openapi.yaml
-- `StartBuildRequest.refresh_base`).

CREATE TABLE osv_mirror (
    ecosystem        TEXT PRIMARY KEY,                                 -- OSV ecosystem name, e.g. Debian, Alpine, Linux
    source           TEXT NOT NULL CHECK (source IN ('download', 'import')),
    record_count     INTEGER NOT NULL CHECK (record_count >= 0),
    data_modified_at TEXT,                                             -- newest record modification held
    refreshed_at     TEXT,
    checked_at       TEXT NOT NULL,
    last_error       TEXT
) STRICT;

CREATE TABLE vulnerability_findings (
    id                TEXT PRIMARY KEY,                                -- vln_
    subject_kind      TEXT NOT NULL CHECK (subject_kind IN ('app', 'quoro', 'host_kernel')),
    app_id            TEXT,                                            -- no foreign key: kept after the app is deleted, resolved
    project_id        TEXT,
    image_digest      TEXT,
    node_id           TEXT,                                            -- no foreign key: kept after the node is removed, resolved
    component         TEXT,
    vuln_id           TEXT NOT NULL CHECK (length(vuln_id) BETWEEN 1 AND 200),
    aliases           TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(aliases)),
    summary           TEXT,
    ecosystem         TEXT NOT NULL,
    package           TEXT NOT NULL,
    source_package    TEXT,
    installed_version TEXT NOT NULL,
    fixed_version     TEXT,
    severity          TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low', 'unknown')),
    cvss_score        REAL CHECK (cvss_score IS NULL OR (cvss_score >= 0 AND cvss_score <= 10)),
    cvss_vector       TEXT,
    kev               INTEGER NOT NULL DEFAULT 0 CHECK (kev IN (0, 1)),
    epss              REAL CHECK (epss IS NULL OR (epss >= 0 AND epss <= 1)),
    first_seen_at     TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    resolved_at       TEXT,
    resolved_reason   TEXT CHECK (resolved_reason IN ('image_changed', 'data_changed', 'app_deleted', 'node_removed', 'upgraded') OR resolved_reason IS NULL),
    CHECK ((resolved_at IS NULL) = (resolved_reason IS NULL)),
    CHECK (subject_kind <> 'app' OR (app_id IS NOT NULL AND image_digest IS NOT NULL)),
    CHECK (subject_kind <> 'quoro' OR component IS NOT NULL),
    CHECK (subject_kind <> 'host_kernel' OR node_id IS NOT NULL)
) STRICT;

CREATE INDEX idx_vuln_findings_app ON vulnerability_findings (app_id, resolved_at);
CREATE INDEX idx_vuln_findings_subject ON vulnerability_findings (subject_kind, resolved_at, severity);
CREATE UNIQUE INDEX idx_vuln_findings_open ON vulnerability_findings (
    subject_kind, coalesce(app_id, ''), coalesce(node_id, ''), coalesce(component, ''), vuln_id, package
) WHERE resolved_at IS NULL;

ALTER TABLE builds ADD COLUMN refresh_base INTEGER NOT NULL DEFAULT 0 CHECK (refresh_base IN (0, 1));

UPDATE meta SET value = '9' WHERE key = 'schema_version';

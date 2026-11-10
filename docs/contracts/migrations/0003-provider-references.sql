-- Migration 0003: which provider each domain, node tunnel and off-site audit
-- copy uses (D-0094, D-0095).
--
-- DELETE /api/v1/providers/{provider_id} answers `conflict` (409) while the
-- provider is referenced. These columns make the reference checkable: the
-- handler refuses the delete while any row below names the provider, and the
-- foreign keys (no ON DELETE action) refuse it at the storage layer too.
--
-- domains.provider_id: the provider whose credential manages the hostname's
-- DNS records. For tls_mode `acme` it is a `dns` provider: the primary writes
-- the ACME DNS-01 TXT records with it on the serving node's behalf
-- (node-agent-protocol.md section 5.22). For tls_mode `tunnel` it is a
-- `tunnel` provider: the primary writes the tunnel ingress rule and the
-- proxied CNAME that points the hostname at the serving node's tunnel. NULL
-- only for rows created before this migration; the API sets it on every new
-- domain (openapi.yaml, CreateDomainRequest).
--
-- audit_exports.provider_id: the `object_store` provider of the off-site
-- audit-log copy (audit-log-format.md section 8). NULL until the copy ships
-- in M5a (D-0053).
--
-- node_tunnels: the node-to-tunnel-provider mapping (D-0094). One row per node
-- that serves `tunnel` routes. The primary creates or looks up the node's
-- tunnel with the provider's account credential, stores its id here, and
-- sends the node only the tunnel id and the tunnel's run token in
-- desired_state (spec.ingress.tunnel, node-agent-protocol.md section 5.11).
-- Only the primary uses the account credential and no desired state carries
-- it, but every machine with the `node` role can read it from the change log
-- (node-agent-protocol.md sections 7.1 and 11, ADR 0320). tunnel_id and
-- token_enc are NULL until the tunnel exists at the provider. token_enc is the run token
-- encrypted with the cluster DEK (key id || nonce || ciphertext, like the
-- other *_enc columns listed at the top of db-schema.sql); it is never
-- returned by the API. origin_port is the loopback port on the node where the
-- node's proxy accepts the tunnel's traffic; the primary writes
-- http://127.0.0.1:<origin_port> as the service of every ingress rule of this
-- tunnel. Replicated state: it changes through the change log like the other
-- state tables. Removing a node removes its mapping.

ALTER TABLE domains ADD COLUMN provider_id TEXT REFERENCES providers (id);

CREATE INDEX idx_domains_provider ON domains (provider_id);

ALTER TABLE audit_exports ADD COLUMN provider_id TEXT REFERENCES providers (id);

CREATE TABLE node_tunnels (
    node_id     TEXT PRIMARY KEY REFERENCES nodes (id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers (id),
    tunnel_id   TEXT,
    token_enc   BLOB,
    origin_port INTEGER NOT NULL CHECK (origin_port BETWEEN 1 AND 65535),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    CHECK ((tunnel_id IS NULL) = (token_enc IS NULL))
) STRICT;

CREATE INDEX idx_node_tunnels_provider ON node_tunnels (provider_id);

UPDATE meta SET value = '4' WHERE key = 'schema_version';

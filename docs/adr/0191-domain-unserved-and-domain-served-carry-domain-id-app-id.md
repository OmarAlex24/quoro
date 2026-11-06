# ADR 0191: domain.unserved and domain.served carry domain_id, app_id, hostname, tls_mode plus error or unserved_since, open no incident, and the certificate.failed tunnel stopgap stays allowed until the primary emits them

- **Status:** Superseded by [ADR 0206](0206-domain-served-carries-unserved-since-as-an-optional-field.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0191
- **Tags:** m5b, contracts, events
- **Supersedes:** none
- **Superseded by:** [ADR 0206](0206-domain-served-carries-unserved-since-as-an-optional-field.md)
- **Related:** none

## Context

D-0167 lists the event pair as optional and widens certificate.failed; the first M5b contract change must fix payloads and incident behaviour, and the v0.2.0 primary still emits certificate.failed with tls_mode tunnel (PR #347).

## Decision

domain.unserved and domain.served carry domain_id, app_id, hostname, tls_mode plus error or unserved_since, open no incident, and the certificate.failed tunnel stopgap stays allowed until the primary emits them.

Subject {type: domain, id}. domain.unserved: required domain_id, app_id, hostname, tls_mode, error (= Domain.last_error). domain.served: required domain_id, app_id, hostname, tls_mode, unserved_since. Emitted only on changes between serving and unserved. Neither opens an incident (like app.degraded); permission-model 8.2 says so. certificate.failed wording (README, events.schema): a primary that does not yet emit domain.unserved also emits certificate.failed with tls_mode tunnel for an unserved tunnel domain; #349 switches the stopgap over. The panel's webhook dialog groups event types by an explicit subject list that has no 'domain' (nor build/backup/restore), so the new types are not selectable there yet.

_Some wording was changed for the public repository._

## Consequences

- Superseded by [ADR 0206](0206-domain-served-carries-unserved-since-as-an-optional-field.md); this decision is no longer binding and is kept for history.

---
Generated from decision D-0191 in the project decision ledger. Do not edit by hand; changes go through a new decision.

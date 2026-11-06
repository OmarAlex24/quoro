# ADR 0190: Domain.serving_status has three states for every TLS mode; pending is the default and the state of pre-migration rows

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0190
- **Tags:** m5b, contracts, ingress
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0167 defines unserved only for tunnel domains; the first M5b contract change needs a meaning for acme domains, for new domains and for rows that exist before migration 0009.

## Decision

Domain.serving_status has three states for every TLS mode; pending is the default and the state of pre-migration rows.

serving: the hostname's route is in the desired state of a machine that can serve it (for tunnel, a machine whose tunnel carries the hostname). unserved: the app has an instance meant to run but no machine can serve the hostname; last_error says why (in M5b only the two tunnel cases of D-0167). pending: the domain was just added or the app has no instance meant to run. Migration 0009 adds serving_status NOT NULL DEFAULT 'pending' and last_error; existing rows start pending and are set at the primary's next recompute (on any change of an app's placement, replicas or routes). last_error is non-null only while unserved. Openapi: serving_status required, last_error nullable and not required, like cert_expires_at.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0190 in the project decision ledger. Do not edit by hand; changes go through a new decision.

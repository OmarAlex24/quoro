# Quoro documentation

Quoro is a self-hosted tool that runs one microVM per application on Cloud
Hypervisor and KVM, and deploys, exposes and observes those applications from
the same interface. It is built to be operated by AI agents through scoped,
time-limited grants, with every action recorded in an append-only,
hash-chained audit log.

Quoro is experimental. Read the [roadmap](product/roadmap.md) for what each
milestone delivers before you rely on it.

## Where to start

- **Install**: [Installing Quoro](site/install.md) covers the one-command
  installer, its roles (primary and node), what it verifies and the host layout
  it creates.
- **Concepts**: the [product requirements](product/prd.md) explain what Quoro
  is for and who it is for; the [glossary](glossary.md) defines the terms used
  everywhere else.
- **Architecture**: [how the pieces fit](architecture/architecture.md): control
  plane, node agents, engine, ingress, state replication and the agent loop.
- **Contracts**: the [interfaces](contracts/README.md) the components are built
  against: the HTTP API, events, the database schema, the node-agent protocol,
  the permission model and the audit log format.
- **Security**: the [threat model](architecture/threat-model.md) and
  [how to report a vulnerability](site/security-policy.md).
- **Decisions**: every accepted design decision is an
  [architecture decision record](adr/README.md).

This documentation is built from the Markdown in the repository's `docs/`
directory with [mdBook](https://rust-lang.github.io/mdBook/).

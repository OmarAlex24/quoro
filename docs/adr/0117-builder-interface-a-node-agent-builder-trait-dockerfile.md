# ADR 0117: Builder interface: a node-agent Builder trait (Dockerfile first, Railpack later) that returns build events and a node-local OCI layout

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0117
- **Tags:** builds, m4, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

M4 builds spike (PR #156, docs/research/m4-builds.md): BuildKit in a builder microVM works; Railpack is a BuildKit gateway frontend that runs in the same VM, so both builders differ only in frontend options and a prepare step.

## Decision

Builder interface: a node-agent Builder trait (Dockerfile first, Railpack later) that returns build events and a node-local OCI layout.

Builder is owned by the node agent, not the Engine; it uses the Engine to run the builder VM. build(BuildSpec) -> stream of BuildEvent {Phase, Line{stream, vertex, text}, Step} and Result<BuildOutput{layout path, manifest_digest}, BuildFailure{phase, step: FailedStep{vertex, name, error, dockerfile_line}, excerpt}>. BuildSpec: build_id (bld_), source (Git{url, ref, resolved commit} | Upload), frontend (Dockerfile{path, target} | Railpack), context_dir, build_args, secrets (names), limits {vcpus, memory_mb, timeout, cache_gb, output_gb}, cache_key per app. States: queued, preparing, building, exporting, importing, succeeded, failed, cancelled. A success starts a deployment with trigger=build (already in openapi). FakeBuilder for tests. Additive Engine-trait items in a standalone PR: prepare_image from a local OCI layout (cached by manifest digest), a scratch/work disk on VmSpec before M5a volumes, cgroup2 mounted by the init, extra disks unmounted before power-off. Git: host-side clone onto the work disk by default (node resolves ref to SHA and handles credentials); BuildKit's own git context works as a fallback.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0117 in the project decision ledger. Do not edit by hand; changes go through a new decision.

# AGENTS.md

## Purpose

Quoro is a self-hosted tool that unifies a hypervisor with application deployment: one engine built on Cloud Hypervisor + KVM runs one microVM per app, and the same interface deploys, exposes and observes those apps. It is designed to be operated by AI agents through scoped, time-limited grants and an append-only audit log. These rules apply to every agent (Claude, Codex and others) and every human contributor. `CLAUDE.md` only points here; this file is the single source of rules.

## Where things live

- `docs/` — product, architecture, contracts, roadmap and workstream briefs.
- `docs/adr/` — one ADR per accepted decision. Generated from the decision ledger; do not edit by hand.
- `AGENTS.md` — these rules. `CLAUDE.md` — a pointer to this file.
- `CONTRIBUTING.md` — how to contribute, including the CLA. `CLA.md` — the contributor license agreement.
- `SECURITY.md` — how to report vulnerabilities. `CODE_OF_CONDUCT.md` — Contributor Covenant 2.1.
- `LICENSE` — AGPL-3.0.
- `ee/` (future) — enterprise code under a separate license.

Read the ADRs that touch your task before you change anything. Maintainer coordination happens in a private ops repository; its accepted decisions are mirrored as ADRs in `docs/adr/`.

## Working rules

1. **Ambiguous or destructive instructions.** If an instruction is ambiguous, or destructive (deleting code, killing processes, choosing between tools), state how you understood it and confirm before acting.
2. **Search before creating.** Before creating a file, script or component, search whether it already exists.
3. **Done means it ran.** Before calling anything done, run it and watch it work. "It compiles" is not done.
4. **Database writes.** Before any query, migration or write to a database, print which environment it targets. Never write to a target without confirming it.
5. **References are literal.** If you are given a reference (an image, a production implementation, a page), reproduce it 1:1. If you cannot see it, ask for it.
6. **Visual work.** Aim for a distinctive, intentional design and avoid the generic look: no cream backgrounds, no italic words in headlines, no `01/02/03` or monospace labels, no pill buttons. Check the rendered result before delivering it.
7. **Models and tools.** Use only the models, tools and external CLIs the maintainer names. Never switch providers or delegate to another model unasked. Small fixes (1–3 lines) can be done directly; larger approved plans are implemented as planned tasks, not improvised.
8. **After a silence.** If you come back after several turns without an answer from the maintainer, open with a summary of what happened and what you need from the maintainer.

## Anti-drift rules

- **ADRs are law.** An accepted ADR is binding. Changing one requires a new ADR approved by the maintainer. If a decision looks wrong, do not work around it: raise it (issue or proposal) so a new ADR can supersede it.
- **Contracts freeze after milestone M0.** Contract changes are standalone PRs, never mixed into feature work.
- **Paths are owned.** Each workstream owns its paths; do not write outside the paths assigned to you.
- **Issues and milestones are the task source of truth.** Every PR closes an issue. Do not invent work outside the milestone.
- **Fixed progress format.** Report progress as **Done / Next / Blocked / Decisions needed**.
- **Milestones end with a demo.** Each milestone closes with a demo on the test lab and the maintainer's approval before the next one starts.
- **Done = it runs on the lab,** not "it compiles".

## Git and PRs

- Commits: `<type>: <description>`, for example `feat: add microVM start endpoint`. Types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`.
- PRs that belong to one plan or workstream are stacked with `gh stack`. Parallel workstreams are separate PRs.
- Only the maintainer merges. An authorization to merge applies only to the PR the maintainer names, never as a standing rule.
- If the previous PR was already merged, start a new branch and a new PR from the updated base.
- PR test plans contain only checked boxes: list only what you actually ran.
- Every PR from an external contributor requires the signed CLA (see `CONTRIBUTING.md`).

## Security basics

- Never commit secrets, tokens or private keys.
- Report vulnerabilities privately, following `SECURITY.md`. Do not open a public issue for a vulnerability.
- Releases are signed and builds reproducible (see ADR 0027).

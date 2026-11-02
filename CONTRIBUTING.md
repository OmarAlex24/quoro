# Contributing to Quoro

Quoro is a self-hosted tool that unifies a hypervisor with application deployment: one engine runs one microVM per app, and the same interface deploys, exposes and observes those apps. It is built to be operated by AI agents through scoped, time-limited grants and an append-only audit log.

Thanks for taking the time to contribute. This document explains how work happens here.

## Project status

Quoro is at the design stage. There are no releases yet, and the architecture, contracts and roadmap are still being written.

At this stage:

- **Issues and discussion are open and welcome.** Design feedback, use cases and bug reports about the documented behaviour are the most useful contributions right now.
- **Code contributions are not open yet**, beyond documentation fixes and small corrections. The codebase is not in a state where outside patches can be reviewed fairly.
- This document will be updated when code contributions open.

## Before you start

Read these first. They are not optional reading:

1. [`AGENTS.md`](AGENTS.md) — the project rules. They apply to humans and to AI agents alike, and they are the single source of rules.
2. [`docs/adr/`](docs/adr/) — the architecture decision records. Read the ones that touch your area before you change anything.

Accepted ADRs are binding. If a decision looks wrong to you, do not work around it: open an issue or a proposal so that a new ADR can supersede it. Changing an accepted ADR requires a new ADR approved by the maintainer.

## Workflow

1. **Open or claim an issue first.** Issues and milestones are the task source of truth. Do not start work that is not tied to an issue; a pull request without one will be asked to open it first.
2. **Branch from an up-to-date `main`.** If your previous pull request was already merged, start a new branch and a new pull request from the updated base.
3. **One pull request closes one issue.** Reference the issue in the pull request body so it closes automatically on merge.
4. **Commit messages** follow `<type>: <description>`, for example `feat: add microVM start endpoint`. Types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`.
5. **Stack related pull requests** with `gh stack` when they belong to one plan or workstream. Parallel workstreams are separate pull requests, not one large one.
6. **Write the test plan honestly.** The test plan in a pull request lists only checked boxes, for things you actually ran. Do not list tests you did not run, and do not check a box for "it compiles".
7. **Only the maintainer merges.** Review may come from anyone; the merge decision is the maintainer's. An authorization to merge applies only to the pull request the maintainer names, never as a standing rule.

Keep changes consistent with the structure, naming and style of the surrounding code, and keep unrelated changes out of a pull request.

## Contributor License Agreement

Every pull request from an external contributor requires a signed Contributor License Agreement, starting with your first one. See [`CLA.md`](CLA.md) for the full text and how to sign it (a single comment on your first pull request).

Quoro uses this CLA instead of the Developer Certificate of Origin. There is no `Signed-off-by` requirement.

## Licensing

Quoro is open-core:

- The core is licensed under **AGPL-3.0**. See [`LICENSE`](LICENSE).
- The `ee/` directory, once it exists, holds enterprise code under a separate license. Contributions there are by invitation only [to be confirmed by the maintainer] and are governed by that license.
- Everything outside `ee/` is AGPL-3.0.

Contributions outside `ee/` are distributed as part of the AGPL-3.0 core. The rights you grant the maintainer are the ones in the CLA.

## Code of conduct

Participation is governed by the [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). Report unacceptable behaviour as described there.

## Security

Never report a security vulnerability in a public issue, pull request or discussion. Follow [`SECURITY.md`](SECURITY.md) and report it privately.

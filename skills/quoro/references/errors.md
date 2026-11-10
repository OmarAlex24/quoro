# Errors and what to do next

The CLI prints one line on stderr, `quoro: <what failed> (<code>[, <reason>])`, then `request id: req_...` when the primary answered. Quote the request id when you report a failure to a human. Over MCP the same problem comes back as the tool's error.

## Exit codes

Branch on these; they never change meaning.

| Exit | Meaning | Next step |
| --- | --- | --- |
| 0 | Success. | Read the state back to confirm the change. |
| 1 | Any other failure: `unauthenticated`, `payload_too_large`, `validation_failed`, `rate_limited`, `not_primary`, `internal`, a local file, or a build that `quoro up` followed and that failed or was cancelled. | Read the message. For a build, read `quoro logs --build`. For `unauthenticated`, the key file or the context is wrong: tell the human; never ask for a key. |
| 2 | The CLI refused the command line, the context or a request body before sending anything. | Fix the command: check [commands](commands.md). Pass `--reason` on a change. |
| 3 | `approval_required`: a human must approve this call first; or the approval you waited for was denied or expired. | Run the same command with `--request-approval --wait`. After a denial or an expiry, stop and report. |
| 4 | `forbidden`, whatever the reason, `human_only` included. Retrying the same call cannot succeed. | Read the reason below. |
| 5 | `not_found`. | List what exists (`quoro project list`, `quoro app list`, `quoro vm list`) and use its name or id. |
| 6 | `conflict`: the resource is not in a state that allows the call. | Read the state (`quoro status`), wait for what is in progress, or take the step the message names. |
| 7 | The primary could not be reached (connection, certificate pin, timeout), or it could not reach the node (`node_unreachable`). | Retry once after a short wait; for a node, check `quoro status --cluster`. A pin mismatch is not transient: tell the human. |

## Problem codes

| Code | HTTP | Exit | Next step |
| --- | --- | --- | --- |
| `unauthenticated` | 401 | 1 | The credential is missing or wrong. Tell the human which context failed. |
| `forbidden` | 403 | 4 | See the reason. |
| `approval_required` | 403 | 3 | The answer names the `scope` and `target`. File the approval: `--request-approval --wait`, or MCP `request_approval`. |
| `not_found` | 404 | 5 | The id or name does not exist, or your grant's selector hides it. |
| `conflict` | 409 | 6 | The state does not allow the call: a deploy or build in progress, an image given to a Git app (use `quoro up`), a name in use. |
| `payload_too_large` | 413 | 1 | The request body is larger than 256 KiB, so the primary did not read it; the call is still recorded in the audit log. Send less at once: split the change into smaller calls. Repeating the same call cannot succeed. |
| `validation_failed` | 422 | 1 | The input is invalid; the message names the field. Fix it and repeat. |
| `rate_limited` | 429 | 1 | Wait and repeat once. |
| `not_primary` | 503 | 1 | The call reached a node that is not the primary. Retry; if it persists, tell the human. |
| `node_unreachable` | 504 | 7 | The primary cannot reach the node holding the data (logs, metrics, a console). Retry later. |
| `internal` | 500 | 1 | Report it to the human with the request id. |

## Refusal reasons (`forbidden`)

When several apply, the first in this order is given.

| Reason | Meaning | Next step |
| --- | --- | --- |
| `human_only` | The call needs a scope no agent can hold. | Ask a human to do it in the panel. |
| `agent_disabled` | Your agent is disabled. | Stop and tell the human. |
| `scope_missing` | No active grant has the scope. | Ask the human for a grant with that scope, naming the call and why. |
| `selector_mismatch` | A grant has the scope but does not cover this resource. | Check the resource with `quoro whoami`; ask for a grant that covers it. |
| `grant_expired` | The grants with the scope have expired. | Ask the human to renew the grant. |
| `grant_revoked` | The grants with the scope were revoked. | Stop; a human took the access back on purpose. |
| `grant_exhausted` | The grants with the scope have no uses left. | Ask the human for a new grant. |

Never work around a refusal by another route (such as `quoro api` on the same operation): the primary applies the same check to every channel.

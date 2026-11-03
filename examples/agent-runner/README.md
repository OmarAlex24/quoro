# Quoro agent runner (reference)

This is a small, self-contained service that receives Quoro webhook deliveries
and wakes an agent to handle an incident. It is an example, not part of the
`quoro` binary, and it uses only the Python 3.11+ standard library — there is no
package to install and nothing to build.

When an app goes down, Quoro opens an incident and emits `incident.opened`. A
webhook configured on that event delivers it here. The runner verifies the
delivery, exchanges the configured agent key for a short-lived access token, and
starts an agent session against Quoro's MCP server. Quoro already issued that
agent an incident grant: a handful of scopes on the affected app for a limited
time. The agent acts inside that grant and nothing else.

The reference flow is a deterministic one: read the incident, read the app,
restart it, wait until its health check passes, optionally check a URL,
and optionally resolve the incident. A second policy plugs in an LLM backend
without changing any of the rules.

## The security model

- **Quoro never sends credentials in a webhook.** The delivery carries an event
  and a signature, nothing secret. The operator puts the agent's long-lived key
  (`akey_...`) on the runner, and the runner keeps it in a file readable only by
  its own account (mode 0600) or in an environment variable.
- **Every delivery is authenticated.** The body is signed with the webhook
  secret: `Quoro-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of
  "<t>.<raw body>">`. The runner rejects a missing or invalid signature with
  `401` and does nothing else. A timestamp more than five minutes away from the
  runner's clock is rejected too.
- **The agent key authorizes nothing by itself.** The runner exchanges it for a
  five-minute access token (`atk_...`), and Quoro authorizes each MCP call
  against the agent's live grants. Revoking the grant locks the agent out on its
  next call.
- **The grant bounds the action, not the runner.** The runner does not decide
  what is allowed. A refused tool call is reported back as a problem
  (`code`/`reason`) and the run stops; it never retries the call, and it never
  tries another tool to get the same effect.
- **Mutations say why and for which incident.** Every mutating tool call
  (`restart_app`, `redeploy_app`, `delete_app`, `resolve_incident`,
  `request_approval`) carries a `justification` and the `incident_id`. The MCP
  client refuses to send one without both.
- **TLS is pinned when asked.** With `tls_fingerprint`, the runner compares the
  SHA-256 of Quoro's certificate right after the handshake and closes the
  connection before sending a single request byte if it does not match. With
  `ca_file`, normal CA and hostname verification is used.
- **Logs never contain secrets.** Log lines name the delivery, the incident, the
  tool and the result. Agent keys, tokens and the webhook secret are never
  written, including in error messages and config errors.

The runner does not make the agent trusted. It makes the wake reliable and the
grant the only thing the agent can use.

## Setup

1. **Create the agent.** In Quoro, create an agent with the identity you want to
   appear in the audit log. Create an agent key for it and keep the one-time
   value (`akey_...`) — Quoro shows it once.
2. **Put the key on the runner host**, for example
   `/etc/agent-runner/agt_....key`, owned by the runner's account with mode
   `0600`.
3. **Create the webhook.** Point it at the runner's URL (for example
   `https://runner.example/webhook`), choose `event_types:
   ["incident.opened"]`, set a secret, and give it a wake configuration:

   ```json
   {
     "agent_id": "agt_...",
     "grant_scopes": [
       "app:read",
       "app:logs",
       "app:restart",
       "incident:read",
       "incident:resolve"
     ],
     "grant_ttl_s": 3600
   }
   ```

   The scopes are validated when the webhook is saved: destructive and
   human-only scopes are refused, so a webhook cannot widen what an agent may do.
   Keep the webhook secret Quoro shows once and write it to
   `/etc/agent-runner/webhook-secret` (mode 0600).

   When the webhook has `wake`, the delivery also carries `data.wake` with the
   `agent_id` it wakes and the `grant_id` of the incident grant Quoro just
   issued for it. The runner acts as that agent, and names the grant in the
   justifications it sends. A webhook without `wake` — and every test delivery —
   carries no `data.wake`, so the runner falls back to `default_agent_id`.

   Optionally, give the agent a standing grant with `app:read` and
   `incident:read` on the apps it looks after. Quoro resolves an incident on
   its own when the app comes back up (`app.up`) and revokes the incident
   grant with it, so without another read grant the runner's next read is
   refused with `grant_revoked` and it cannot see the recovery in Quoro; it
   then relies on `check_url`.
4. **Write the runner config.** Copy `runner.example.toml` to `runner.toml` and
   fill in the listen address, Quoro's URL, the TLS option you use, the secret
   path and one `[agents."agt_..."]` entry per agent the runner may wake.
5. **Run it.**

   ```sh
   cd examples/agent-runner
   python3 -m agent_runner --config runner.toml
   ```

   Startup validates the config and reads every secret once, so a wrong path or
   a bad key fails immediately with a message naming the field. The receiver
   listens on `POST /webhook`; anything else gets `404` or `405`.

Quoro retries a delivery it could not confirm. The runner answers `202` as soon
as it has authenticated and accepted the delivery, and then runs the agent in a
background thread, so the dispatcher never waits on the agent. A retry of the
same `Quoro-Delivery-Id` is answered `200` without acting again, and two
deliveries for one incident never run concurrently.

## Configuration reference

`runner.toml` is read with `tomllib`.

| Key | Default | Meaning |
| --- | --- | --- |
| `listen` | required | `host:port` the webhook receiver binds to. The route is always `/webhook`. |
| `quoro_url` | required | Base URL of Quoro (`http://` or `https://`). |
| `tls_fingerprint` | system trust | `sha256:<64 hex>` pin of Quoro's certificate. |
| `ca_file` | system trust | PEM file used to verify Quoro's certificate. Do not set together with `tls_fingerprint`. |
| `webhook_secret_file` | required* | File holding the webhook secret. |
| `webhook_secret_env` | required* | Environment variable holding the webhook secret. Set exactly one of the two. |
| `default_agent_id` | none | Agent used when a delivery carries no usable `data.wake.agent_id` (absent, not an `agt_` id, or not an object). Must have an `[agents]` entry. |
| `policy` | `deterministic` | `deterministic` or `llm`. |
| `check_url` | none | After a restart, `GET` this URL and require 200 before calling the app recovered. Uses system trust and sends `User-Agent: quoro-agent-runner/<version>`; the outcome names the status the URL answered. |
| `resolve_incident` | `false` | Resolve the incident after a successful recovery, when the scope and tool are available. |
| `poll_attempts` | `12` | How many times to read the app while waiting for its health check to pass. Together with `poll_interval_s` this bounds the wait. |
| `poll_interval_s` | `5.0` | Seconds between recovery reads. |
| `max_body_bytes` | `1048576` | Largest accepted webhook body; larger deliveries get `413`. |
| `http_timeout_s` | `30.0` | Timeout for calls to Quoro. |
| `health_timeout_s` | `10.0` | Timeout for the `check_url` request. |
| `log_level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`. Logs go to stderr. |
| `[agents."agt_..."]` | required | One entry per agent. Each has exactly one of `key_file` or `key_env`. |
| `[llm]` | only for `llm` | `backend = "module:Class"`, plus any keyword arguments for that class. |

\* Exactly one of `webhook_secret_file` and `webhook_secret_env` is required.

### TLS against Quoro's self-signed listener

The certificate fingerprint is the one the `quoro join` command prints. Use the
pin when Quoro's listener is self-signed; use `ca_file` when you have the cluster
CA and prefer normal verification; use neither when Quoro's certificate is
already trusted by the system store. The two options are mutually exclusive.

## The policy interface

A policy decides what the agent does. It receives the wake and a started MCP
session, and returns an outcome:

```python
class Policy(Protocol):
    def handle(self, wake: Wake, mcp: McpSession) -> Outcome: ...
```

`Wake` carries the `incident_id`, the `agent_id`, the event envelope, and the
`grant_id` of the incident grant when the delivery named one (`None` otherwise).
An `agent_id` from `data.wake` is used only when it is a well-formed `agt_` id;
anything else falls back to `default_agent_id`, and a delivery that names an
agent without a configured key is ignored as `unknown_agent` rather than
retried as the default. A `grant_id` is kept only when it is a well-formed
`grt_` id, and the policies name it wherever they name the incident.
`Outcome` records the actions taken, whether the app recovered, whether the
incident was resolved, and a one-line summary. `McpSession` exposes
`tool_names()`, `list_tools()` and `call(name, arguments, justification=None)`.

`DeterministicRestartPolicy` is the default. It reads the incident (through
`get_incident` when the tool is listed, otherwise from the event payload), checks
that the subject is an app, reads it, restarts it with a justification naming the
incident, and waits, at most `poll_attempts` reads `poll_interval_s` apart, until
Quoro reports the app healthy. The signal is the app's `status`: it is `running`
only while the instance runs and its health check passes. An instance's
`observed_status` follows the VM process, so a VM that runs while the app does
not answer is not taken as a recovery. When a read is refused with
`grant_revoked` (Quoro resolves the incident on `app.up` and revokes the
incident grant), the policy reads the incident: resolved by the system means the
app came back up. If it cannot tell, recovery is not confirmed unless
`check_url` answers 200. With a health signal it then checks `check_url` when
set, and resolves the incident when asked to and it is still open. Any other
refusal stops the run.

`LlmPolicy` is provider-agnostic. It loads `[llm].backend` (an importable
`module:Class`), instantiates it with the rest of the `[llm]` table as keyword
arguments, and asks it for one action at a time:

```python
class LlmBackend(Protocol):
    def next_action(
        self, messages: list[dict], tools: list[dict]
    ) -> dict: ...
```

The return value is either `{"tool": name, "arguments": {...}}` or
`{"done": "summary"}`. The policy builds the system message (incident context,
the rules, the available tools), feeds every tool result back as a message,
caps the run at eight steps, and injects the `incident_id` into a mutating call
if the backend forgot it. It stops at the first refusal. The runner ships no
vendor backend; `tests/fake_backend.py` shows the shape.

## Running the tests

The tests are offline and need no services, keys or network access:

```sh
cd examples/agent-runner
python3 -m unittest discover -s tests -v
```

They cover signature verification, the HTTP receiver against a fake Quoro
(token minting, tool calls, deduplication, agent selection from `data.wake` and
the default, malformed wakes, unknown agents, refusals, waiting for the app's
health rather than its VM), the `check_url` request, TLS fingerprint
comparison, the MCP client's JSON and event-stream parsing, the mutating-call
rules, config validation and the LLM policy with a fake backend.

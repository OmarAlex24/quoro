# `quoro-mcp`

Quoro's MCP server. It lets an AI agent operate Quoro through the tools listed
in [`docs/contracts/README.md`](../../docs/contracts/README.md), section "MCP
tools", and nothing else. The binding rules are
[`docs/contracts/permission-model.md`](../../docs/contracts/permission-model.md)
section 10.

The server is a thin adapter (ADR 0031): each tool checks the shape of its
arguments and calls the same control-plane service function as the matching
HTTP endpoint. Scope and grant checks, the justification rule, approvals and
the audit record all happen in that function. The crate depends on no
database crate.

There are two ways to reach it, and both end at the same endpoint.

## Streamable HTTP at `/mcp`

The primary serves MCP at `https://<primary>:8443/mcp` on its one TLS listener
(ADR 0054). Clients authenticate with `Authorization: Bearer atk_...`, an agent
access token minted from an agent key at `POST /api/v1/auth/agent-token`. A
session cookie, a personal API token or the agent key itself is refused. The
reference runner in [`examples/agent-runner/`](../../examples/agent-runner/)
uses this endpoint.

## `quoro mcp stdio` for local MCP clients

MCP clients that start a local process and speak JSON-RPC on its stdin and
stdout can run `quoro mcp stdio`. It relays every message unchanged to the
primary's `/mcp` and writes every answer back on stdout, one JSON object per
line. It knows no tool and adds no policy or audit record of its own: the tool
list comes from the primary, and every call is authorized and audited there,
on the `mcp` channel, as the agent's.

```
quoro mcp stdio --url https://quoro.example.com:8443 \
  --fingerprint sha256:<64 hex> \
  --key-file /home/<user>/.config/quoro/agent.key \
  [--justification "why this session needs access"]
```

- `--url` is the primary's HTTPS address.
- `--fingerprint` is the SHA-256 of the primary's certificate, the same value a
  `quoro join` command carries; the primary also logs it at start
  (`tls fingerprint: sha256:...`). The connection is pinned to it exactly as
  `quoro join` pins it: no root store is consulted, and nothing is sent to a
  server whose certificate does not match.
- `--key-file` is a file that holds the agent key (`akey_...`) and nothing
  else, owned by the account that runs the client, with mode `0600` or
  stricter. A missing file, a file other users can read, or a file that does
  not hold an agent key ends the process with a message on stderr. The key is
  never accepted on the command line, where every local user could read it.
- `--justification` is recorded in the audit log with every access token the
  session mints (the contract requires one because the caller is an agent). The
  default names the stdio relay.

At start the relay mints a five-minute access token, so a wrong key, a wrong
fingerprint or an unreachable primary fails before the client sends anything.
It mints the next token a minute before the current one expires, and once more
if the primary answers 401. A refused call stays an ordinary MCP result from
the primary (`isError` with Quoro's problem body); a message the primary could
not take at all comes back as a JSON-RPC error with the request's `id`. The
process exits 0 when stdin closes, after answering what was already sent.

What the agent can do is decided by its grants, not by the relay: create the
agent and its key in the panel or with `POST /api/v1/agents` and
`POST /api/v1/agents/{agent_id}/keys`, then issue the grants it needs.

A client configuration usually looks like this (the shape varies by client):

```json
{
  "mcpServers": {
    "quoro": {
      "command": "quoro",
      "args": [
        "mcp", "stdio",
        "--url", "https://quoro.example.com:8443",
        "--fingerprint", "sha256:<64 hex>",
        "--key-file", "/home/<user>/.config/quoro/agent.key"
      ]
    }
  }
}
```

Mutating tools (`restart_app`, `redeploy_app`, `delete_app`,
`resolve_incident`, `request_approval`) still need a `justification` argument,
and an `incident_id` when the agent acts on an incident; the primary refuses a
mutating call without one.

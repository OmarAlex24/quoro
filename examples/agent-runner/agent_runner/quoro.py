# SPDX-License-Identifier: AGPL-3.0-only
"""Minimal Quoro client: access-token minting and an MCP session.

The agent key (``akey_``) authorizes nothing by itself; this client exchanges it
for a five-minute access token (``atk_``) and uses that token against Quoro's
MCP endpoint. Every call is authorized by Quoro against the agent's incident
grant at request time, so a revoked grant locks the agent out immediately.

Mutating tools must carry a justification and the incident they belong to; the
client enforces both before any bytes leave the process.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import logging
import ssl
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import urlsplit

LOGGER = logging.getLogger("agent_runner.quoro")

PROTOCOL_VERSION = "2025-11-25"
CLIENT_NAME = "quoro-agent-runner"
CLIENT_VERSION = "0.1.0"

#: Tools that change state and therefore need ``justification`` and ``incident_id``.
MUTATING_TOOLS = frozenset(
    {
        "restart_app",
        "redeploy_app",
        "delete_app",
        "resolve_incident",
        "request_approval",
    }
)

TOKEN_REFRESH_MARGIN_S = 30.0


class QuoroError(Exception):
    """A failed exchange with Quoro that is not a tool refusal."""


class McpError(QuoroError):
    """A JSON-RPC error returned by Quoro's MCP server."""

    def __init__(self, code: int | None, message: str) -> None:
        super().__init__(f"MCP error {code}: {message}")
        self.code = code
        self.message = message


class TlsPinMismatch(QuoroError):
    """The TLS certificate does not match the pinned fingerprint."""


@dataclass(frozen=True)
class ToolResult:
    """Outcome of one ``tools/call``.

    A refusal is a normal result (``ok=False``) with Quoro's problem body in
    ``problem``; only transport and protocol failures raise.
    """

    ok: bool
    value: Mapping[str, Any] | None = None
    problem: Mapping[str, Any] | None = None

    @property
    def problem_code(self) -> str | None:
        if self.problem is None:
            return None
        code = self.problem.get("code")
        return code if isinstance(code, str) else None

    @property
    def problem_reason(self) -> str | None:
        if self.problem is None:
            return None
        reason = self.problem.get("reason")
        return reason if isinstance(reason, str) else None


@dataclass(frozen=True)
class Response:
    """One HTTP response, with headers keyed lower-case."""

    status: int
    headers: Mapping[str, str]
    body: bytes


def canonical_fingerprint(value: str) -> str:
    """Normalize ``sha256:<hex>`` (or bare hex) to lower-case hex."""
    normalized = value.strip().lower()
    prefix = "sha256:"
    return normalized[len(prefix) :] if normalized.startswith(prefix) else normalized


def verify_fingerprint(cert_der: bytes | None, expected: str) -> bool:
    """Return whether the SHA-256 of a DER certificate equals the pinned value."""
    if not cert_der:
        return False
    actual = hashlib.sha256(cert_der).hexdigest()
    return hmac.compare_digest(actual, canonical_fingerprint(expected))


class HttpTransport:
    """A one-request-per-connection HTTP(S) transport with optional pinning.

    When a fingerprint is pinned, CA verification and hostname checks are
    disabled by design and replaced by the pin. The TLS handshake happens
    explicitly, the certificate is compared, and only then is the request
    written, so a mismatching listener never sees request bytes.
    """

    def __init__(
        self,
        base_url: str,
        *,
        tls_fingerprint: str | None = None,
        ca_file: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        parts = urlsplit(base_url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise QuoroError(f"unsupported Quoro URL {base_url!r}")
        self._scheme = parts.scheme
        self._host = parts.hostname
        self._port = parts.port or (443 if parts.scheme == "https" else 80)
        self._prefix = parts.path.rstrip("/")
        self._fingerprint = tls_fingerprint
        self._ca_file = ca_file
        self._timeout = timeout

    def _ssl_context(self) -> ssl.SSLContext:
        if self._fingerprint:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        return ssl.create_default_context(cafile=self._ca_file)

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        """Send one request and return the full response body."""
        send_headers = dict(headers or {})
        if body is not None:
            send_headers.setdefault("Content-Length", str(len(body)))

        if self._scheme == "https":
            connection: http.client.HTTPConnection = http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=self._timeout,
                context=self._ssl_context(),
            )
            connection.connect()
            self._check_peer(connection)
        else:
            connection = http.client.HTTPConnection(
                self._host, self._port, timeout=self._timeout
            )

        try:
            connection.request(method, self._prefix + path, body=body, headers=send_headers)
            raw = connection.getresponse()
            payload = raw.read()
            response_headers = {key.lower(): value for key, value in raw.getheaders()}
            return Response(status=raw.status, headers=response_headers, body=payload)
        finally:
            connection.close()

    def _check_peer(self, connection: http.client.HTTPConnection) -> None:
        if not self._fingerprint:
            return
        socket = connection.sock
        cert_der = socket.getpeercert(binary_form=True) if socket is not None else None
        if verify_fingerprint(cert_der, self._fingerprint):
            return
        connection.close()
        raise TlsPinMismatch(
            "the Quoro TLS certificate does not match tls_fingerprint; "
            "refusing to send any request"
        )


def iter_sse_json(text: str) -> Iterator[Any]:
    """Yield the JSON payload of every server-sent event in ``text``."""
    data_lines: list[str] = []
    for line in text.splitlines() + [""]:
        if not line.strip():
            if data_lines:
                try:
                    yield json.loads("\n".join(data_lines))
                except json.JSONDecodeError:
                    LOGGER.warning("ignoring an MCP event whose data is not JSON")
                data_lines = []
            continue
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())


def decode_rpc_response(response: Response, request_id: int) -> Mapping[str, Any]:
    """Pick the JSON-RPC response for ``request_id`` from an HTTP response."""
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        messages: Iterator[Any] = iter_sse_json(response.body.decode("utf-8", "replace"))
    else:
        try:
            decoded = json.loads(response.body)
        except json.JSONDecodeError:
            raise QuoroError(
                f"Quoro's MCP server answered {response.status} with a body that is not JSON"
            ) from None
        messages = iter(decoded if isinstance(decoded, list) else [decoded])

    for message in messages:
        if isinstance(message, dict) and message.get("id") == request_id:
            return message
    raise QuoroError(f"no MCP response for request id {request_id}")


class QuoroClient:
    """Access-token custody plus the JSON-RPC methods the runner needs."""

    def __init__(
        self,
        transport: HttpTransport,
        agent_key: str,
        agent_id: str,
        *,
        justification: str = "",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._transport = transport
        self._agent_key = agent_key
        self.agent_id = agent_id
        self._justification = justification
        self._clock = clock
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._next_id = 0

    def _new_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def mint_token(self, justification: str | None = None) -> str:
        """Exchange the agent key for a five-minute access token."""
        reason = justification or self._justification
        if not reason.strip():
            raise ValueError("minting a token needs a non-empty justification")
        response = self._transport.request(
            "POST",
            "/api/v1/auth/agent-token",
            body=b"",
            headers={
                "Authorization": f"Bearer {self._agent_key}",
                "Quoro-Justification": reason,
                "Accept": "application/json",
                "Content-Length": "0",
            },
        )
        if response.status != 200:
            raise QuoroError(
                f"agent-token request failed with status {response.status}"
            )
        try:
            payload = json.loads(response.body)
            token = payload["access_token"]
            expires_at = payload["expires_at"]
        except (json.JSONDecodeError, KeyError, TypeError):
            raise QuoroError("agent-token response is missing access_token or expires_at") from None

        self._token = str(token)
        self._token_expires_at = _parse_timestamp(expires_at, self._clock())
        return self._token

    def token(self, justification: str | None = None) -> str:
        """Return a token, minting a fresh one when none exists or it is near expiry."""
        if self._token is None or self._clock() >= self._token_expires_at - TOKEN_REFRESH_MARGIN_S:
            self.mint_token(justification)
        assert self._token is not None
        return self._token

    def initialize(self) -> Mapping[str, Any]:
        """Run the MCP handshake and its ``notifications/initialized`` follow-up."""
        result = self._rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": CLIENT_NAME, "version": CLIENT_VERSION},
            },
        )
        self._notify("notifications/initialized")
        return result

    def list_tools(self) -> list[Mapping[str, Any]]:
        """Return the tools Quoro exposes to this agent."""
        result = self._rpc("tools/list", {})
        tools = result.get("tools")
        return [tool for tool in tools or [] if isinstance(tool, dict)]

    def call_tool(
        self,
        name: str,
        arguments: Mapping[str, Any],
        justification: str | None = None,
    ) -> ToolResult:
        """Call one tool and translate a refusal into ``ToolResult(ok=False)``."""
        if name in MUTATING_TOOLS:
            if not (justification or "").strip():
                raise ValueError(f"{name} needs a non-empty justification")
            if not str(arguments.get("incident_id") or "").strip():
                raise ValueError(f"{name} needs the incident_id it acts on")

        result = self._rpc(
            "tools/call",
            {"name": name, "arguments": dict(arguments)},
            justification=justification,
        )
        if result.get("isError"):
            problem = result.get("structuredContent")
            return ToolResult(ok=False, problem=problem if isinstance(problem, dict) else {})
        value = result.get("structuredContent")
        return ToolResult(ok=True, value=value if isinstance(value, dict) else {})

    def _headers(self, token: str, justification: str | None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        }
        if justification:
            headers["Quoro-Justification"] = justification
        return headers

    def _notify(self, method: str) -> None:
        """Send a JSON-RPC notification; Quoro answers 202 (or 200) with no body."""
        body = json.dumps({"jsonrpc": "2.0", "method": method, "params": {}}).encode("utf-8")
        for attempt in (0, 1):
            response = self._transport.request(
                "POST", "/mcp", body=body, headers=self._headers(self.token(), None)
            )
            if response.status == 401 and attempt == 0:
                self._token = None
                continue
            if response.status not in (200, 202):
                raise QuoroError(f"MCP notification {method} failed with status {response.status}")
            return
        raise QuoroError(f"MCP notification {method} was refused with status 401")

    def _rpc(
        self,
        method: str,
        params: Mapping[str, Any],
        justification: str | None = None,
    ) -> Mapping[str, Any]:
        """Send one JSON-RPC request, re-minting the token once after a 401."""
        request_id = self._new_id()
        body = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)}
        ).encode("utf-8")

        for attempt in (0, 1):
            headers = self._headers(self.token(justification), justification)
            response = self._transport.request("POST", "/mcp", body=body, headers=headers)
            if response.status == 401 and attempt == 0:
                LOGGER.info("MCP access token was refused; minting a new one")
                self._token = None
                continue
            if response.status != 200:
                raise QuoroError(f"MCP {method} failed with status {response.status}")
            message = decode_rpc_response(response, request_id)
            error = message.get("error")
            if isinstance(error, dict):
                raise McpError(error.get("code"), str(error.get("message", "unknown error")))
            result = message.get("result")
            if not isinstance(result, dict):
                raise QuoroError(f"MCP {method} returned no result object")
            return result
        raise QuoroError(f"MCP {method} was refused with status 401 after minting a new token")


class McpSession:
    """A started MCP session bound to one agent's client."""

    def __init__(self, client: QuoroClient) -> None:
        self._client = client
        self._tools: list[Mapping[str, Any]] | None = None

    def start(self) -> None:
        """Perform the handshake; call once before any other method."""
        self._client.initialize()

    def list_tools(self) -> list[Mapping[str, Any]]:
        """Return the tool definitions, fetched once per session."""
        if self._tools is None:
            self._tools = self._client.list_tools()
        return self._tools

    def tool_names(self) -> set[str]:
        """Return the names of the tools this agent may call."""
        names = set()
        for tool in self.list_tools():
            name = tool.get("name")
            if isinstance(name, str):
                names.add(name)
        return names

    def call(
        self,
        name: str,
        arguments: Mapping[str, Any],
        justification: str | None = None,
    ) -> ToolResult:
        """Call a tool; refusals come back as ``ToolResult(ok=False)``."""
        result = self._client.call_tool(name, arguments, justification)
        if result.ok:
            LOGGER.info("tool %s: ok", name)
        else:
            LOGGER.warning(
                "tool %s: refused (%s%s)",
                name,
                result.problem_code,
                f"/{result.problem_reason}" if result.problem_reason else "",
            )
        return result


def _parse_timestamp(value: Any, fallback: float) -> float:
    """Turn an RFC 3339 timestamp into epoch seconds, falling back to ``fallback``."""
    if not isinstance(value, str):
        return fallback
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        LOGGER.warning("could not parse token expiry; assuming the standard five minutes")
        return fallback + 300.0

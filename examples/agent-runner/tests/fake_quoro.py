# SPDX-License-Identifier: AGPL-3.0-only
"""A small in-process stand-in for Quoro's token endpoint and MCP server.

Tests start it on ``127.0.0.1:0`` and inspect what the runner did: which agent
key it used, which justification it sent, and every tool call with its arguments
and bearer token. No network access and no dependencies outside the standard
library.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Mapping

AGENT_KEY = "akey_01JTESTAGENTKEY0000000000"
ACCESS_TOKEN = "atk_01JTESTACCESSTOKEN0000000"


def _tool(name: str, description: str, properties: Mapping[str, Any], required: list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": dict(properties),
            "required": required,
        },
    }


TOOLS = [
    _tool("get_incident", "Read one incident.", {"incident_id": {"type": "string"}}, ["incident_id"]),
    _tool("get_app", "Read one app and its instances.", {"app_id": {"type": "string"}}, ["app_id"]),
    _tool(
        "restart_app",
        "Restart an app's instances.",
        {"app_id": {"type": "string"}, "incident_id": {"type": "string"}, "justification": {"type": "string"}},
        ["app_id", "justification"],
    ),
    _tool(
        "resolve_incident",
        "Resolve an incident.",
        {"incident_id": {"type": "string"}, "justification": {"type": "string"}},
        ["incident_id", "justification"],
    ),
]


class FakeQuoro:
    """An HTTP server that answers the two endpoints the runner uses."""

    def __init__(self, *, app_id: str, incident_id: str) -> None:
        self.app_id = app_id
        self.incident_id = incident_id
        self.instance_status = "stopped"
        self.refuse_restart: Mapping[str, Any] | None = None
        self.mints: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []
        self.paths: list[str] = []
        self._lock = threading.Lock()

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_class())
        self._server.daemon_threads = True
        self._server.fake = self
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> "FakeQuoro":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def record_path(self, path: str) -> None:
        with self._lock:
            self.paths.append(path)

    def record_mint(self, key: str, justification: str) -> None:
        with self._lock:
            self.mints.append({"key": key, "justification": justification})

    def record_call(self, tool: str, arguments: Mapping[str, Any], token: str) -> None:
        with self._lock:
            self.calls.append(
                {"tool": tool, "arguments": dict(arguments), "token": token}
            )

    def calls_named(self, tool: str) -> list[dict[str, Any]]:
        with self._lock:
            return [call for call in self.calls if call["tool"] == tool]

    def wait_until(self, predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()


def _handler_class() -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        @property
        def fake(self) -> FakeQuoro:
            return self.server.fake  # type: ignore[attr-defined]

        def do_POST(self) -> None:  # noqa: N802
            fake = self.fake
            fake.record_path(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
            if self.path == "/api/v1/auth/agent-token":
                self._mint(body)
            elif self.path == "/mcp":
                self._mcp(body)
            else:
                self._json(404, {"error": "not_found"})

        def _mint(self, body: bytes) -> None:
            del body
            fake = self.fake
            authorization = self.headers.get("Authorization", "")
            if authorization != f"Bearer {AGENT_KEY}":
                self._json(401, {"code": "unauthenticated"})
                return
            justification = self.headers.get("Quoro-Justification", "")
            if not justification:
                self._json(422, {"code": "validation_failed"})
                return
            fake.record_mint(authorization.removeprefix("Bearer "), justification)
            expires_at = (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat().replace("+00:00", "Z")
            self._json(200, {"access_token": ACCESS_TOKEN, "expires_at": expires_at})

        def _mcp(self, body: bytes) -> None:
            fake = self.fake
            if self.headers.get("Authorization", "") != f"Bearer {ACCESS_TOKEN}":
                self._json(401, {"code": "unauthenticated"})
                return
            try:
                request = json.loads(body)
            except json.JSONDecodeError:
                self._json(400, {"code": "validation_failed"})
                return

            method = request.get("method")
            if method == "notifications/initialized":
                self._empty(202)
                return

            request_id = request.get("id")
            if method == "initialize":
                result: dict[str, Any] = {
                    "protocolVersion": request.get("params", {}).get("protocolVersion", ""),
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fake-quoro", "version": "0.0.0"},
                }
            elif method == "tools/list":
                result = {"tools": TOOLS}
            elif method == "tools/call":
                result = self._call_tool(request.get("params") or {})
            else:
                self._json(
                    200,
                    {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": "unknown method"}},
                )
                return
            self._json(200, {"jsonrpc": "2.0", "id": request_id, "result": result})

        def _call_tool(self, params: Mapping[str, Any]) -> dict[str, Any]:
            fake = self.fake
            name = str(params.get("name"))
            arguments = params.get("arguments") or {}
            fake.record_call(name, arguments, self.headers.get("Authorization", "").removeprefix("Bearer "))

            if name == "get_incident":
                return {
                    "content": [],
                    "structuredContent": {
                        "id": fake.incident_id,
                        "status": "open",
                        "title": "health check failed",
                        "subject": {"type": "app", "id": fake.app_id},
                    },
                }
            if name == "get_app":
                return {
                    "content": [],
                    "structuredContent": {
                        "id": fake.app_id,
                        "status": "down" if fake.instance_status != "running" else "running",
                        "instances": [{"id": "vm_test", "status": fake.instance_status}],
                    },
                }
            if name == "restart_app":
                if fake.refuse_restart is not None:
                    return {
                        "content": [],
                        "isError": True,
                        "structuredContent": dict(fake.refuse_restart),
                    }
                fake.instance_status = "running"
                return {"content": [], "structuredContent": {"id": fake.app_id, "restart_generation": 2}}
            if name == "resolve_incident":
                return {
                    "content": [],
                    "structuredContent": {"id": fake.incident_id, "status": "resolved"},
                }
            return {
                "content": [],
                "isError": True,
                "structuredContent": {"code": "not_found"},
            }

        def _json(self, status: int, payload: Mapping[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _empty(self, status: int) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    return Handler

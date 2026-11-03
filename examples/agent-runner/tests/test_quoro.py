# SPDX-License-Identifier: AGPL-3.0-only
"""Unit tests for the Quoro client: TLS pinning, SSE framing and tool rules."""

from __future__ import annotations

import json
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from agent_runner.quoro import (
    HttpTransport,
    QuoroClient,
    Response,
    decode_rpc_response,
    iter_sse_json,
    verify_fingerprint,
)


class UnusedTransport:
    """A transport that fails the test if the client sends anything."""

    def request(self, *args: Any, **kwargs: Any) -> Response:  # pragma: no cover
        raise AssertionError("the client should not have sent a request")


class StubTransport:
    """Answer with a canned body and record every request."""

    def __init__(self, responses: list[Response]) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        self.requests.append(
            {"method": method, "path": path, "body": body, "headers": dict(headers or {})}
        )
        return self.responses.pop(0)


def _json_response(payload: Mapping[str, Any], status: int = 200) -> Response:
    return Response(status=status, headers={"content-type": "application/json"}, body=json.dumps(payload).encode())


def _expires_at(minutes: int = 5) -> str:
    moment = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    return moment.isoformat().replace("+00:00", "Z")


def _client(responses: list[Response]) -> QuoroClient:
    """A client whose token mint is answered first, then ``responses`` in order."""
    mint = _json_response({"access_token": "atk_live", "expires_at": _expires_at()})
    return QuoroClient(
        StubTransport([mint, *responses]),
        "akey_test",
        "agt_test",
        justification="testing",
    )


class FingerprintTests(unittest.TestCase):
    def test_matches_the_sha256_of_the_certificate(self) -> None:
        certificate = b"a-der-encoded-certificate"
        import hashlib

        digest = hashlib.sha256(certificate).hexdigest()
        self.assertTrue(verify_fingerprint(certificate, f"sha256:{digest}"))
        self.assertTrue(verify_fingerprint(certificate, digest.upper()))

    def test_rejects_a_different_certificate(self) -> None:
        import hashlib

        other = hashlib.sha256(b"the-real-certificate").hexdigest()
        self.assertFalse(verify_fingerprint(b"another-certificate", f"sha256:{other}"))

    def test_rejects_a_missing_certificate(self) -> None:
        self.assertFalse(verify_fingerprint(None, f"sha256:{'0' * 64}"))

    def test_plain_http_needs_no_ssl_context(self) -> None:
        transport = HttpTransport("http://127.0.0.1:9")
        self.assertIsNotNone(transport)


class SseTests(unittest.TestCase):
    def test_parses_every_event(self) -> None:
        text = 'event: message\ndata: {"id":1}\n\ndata: {"id":2}\n\n'
        self.assertEqual(list(iter_sse_json(text)), [{"id": 1}, {"id": 2}])

    def test_joins_multiline_data(self) -> None:
        text = 'data: {"id":1,\ndata: "ok":true}\n\n'
        self.assertEqual(list(iter_sse_json(text)), [{"id": 1, "ok": True}])

    def test_skips_events_that_are_not_json(self) -> None:
        text = "data: not-json\n\ndata: {\"id\": 1}\n\n"
        self.assertEqual(list(iter_sse_json(text)), [{"id": 1}])

    def test_picks_the_response_with_the_matching_id(self) -> None:
        body = (
            'data: {"jsonrpc":"2.0","method":"notifications/message"}\n\n'
            'data: {"jsonrpc":"2.0","id":7,"result":{"ok":true}}\n\n'
        ).encode()
        response = Response(status=200, headers={"content-type": "text/event-stream"}, body=body)
        self.assertEqual(decode_rpc_response(response, 7)["result"], {"ok": True})

    def test_parses_a_plain_json_response(self) -> None:
        response = _json_response({"jsonrpc": "2.0", "id": 3, "result": {"tools": []}})
        self.assertEqual(decode_rpc_response(response, 3)["result"], {"tools": []})

    def test_a_missing_response_raises(self) -> None:
        response = _json_response({"jsonrpc": "2.0", "id": 1, "result": {}})
        with self.assertRaises(Exception):
            decode_rpc_response(response, 2)


class MutatingCallTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = QuoroClient(UnusedTransport(), "akey_test", "agt_test", justification="testing")

    def test_every_mutating_tool_requires_a_justification(self) -> None:
        for name in (
            "restart_app",
            "redeploy_app",
            "delete_app",
            "resolve_incident",
            "request_approval",
        ):
            with self.subTest(tool=name):
                with self.assertRaises(ValueError):
                    self.client.call_tool(name, {"incident_id": "inc_test"})

    def test_a_mutating_tool_requires_the_incident_id(self) -> None:
        with self.assertRaises(ValueError):
            self.client.call_tool("restart_app", {"app_id": "app_test"}, justification="restart it")

    def test_a_read_only_tool_needs_no_justification(self) -> None:
        client = _client(
            [_json_response({"jsonrpc": "2.0", "id": 1, "result": {"structuredContent": {"id": "app_test"}}})]
        )
        result = client.call_tool("get_app", {"app_id": "app_test"})
        self.assertTrue(result.ok)
        self.assertEqual(result.value, {"id": "app_test"})


class ToolResultTests(unittest.TestCase):
    def test_a_refusal_is_a_result_not_an_exception(self) -> None:
        client = _client(
            [
                _json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "isError": True,
                            "structuredContent": {"code": "forbidden", "reason": "scope_missing"},
                        },
                    }
                )
            ]
        )
        result = client.call_tool(
            "restart_app",
            {"app_id": "app_test", "incident_id": "inc_test"},
            justification="restart it",
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.problem_code, "forbidden")
        self.assertEqual(result.problem_reason, "scope_missing")

    def test_a_jsonrpc_error_raises(self) -> None:
        client = _client(
            [_json_response({"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "nope"}})]
        )
        with self.assertRaises(Exception):
            client.call_tool("get_app", {"app_id": "app_test"})


class TokenTests(unittest.TestCase):
    def test_mints_with_the_agent_key_and_justification(self) -> None:
        transport = StubTransport(
            [_json_response({"access_token": "atk_live", "expires_at": _expires_at()})]
        )
        client = QuoroClient(
            transport, "akey_live", "agt_test", justification="incident inc_test needs a token"
        )
        self.assertEqual(client.token(), "atk_live")
        request = transport.requests[0]
        self.assertEqual(request["path"], "/api/v1/auth/agent-token")
        self.assertEqual(request["headers"]["Authorization"], "Bearer akey_live")
        self.assertEqual(request["headers"]["Quoro-Justification"], "incident inc_test needs a token")

    def test_reuses_a_token_until_it_is_near_expiry(self) -> None:
        clock = [time.time()]
        transport = StubTransport(
            [
                _json_response({"access_token": "atk_one", "expires_at": _expires_at()}),
                _json_response({"access_token": "atk_two", "expires_at": _expires_at()}),
            ]
        )
        client = QuoroClient(
            transport,
            "akey_live",
            "agt_test",
            justification="testing",
            clock=lambda: clock[0],
        )
        self.assertEqual(client.token(), "atk_one")
        clock[0] += 60
        self.assertEqual(client.token(), "atk_one")
        clock[0] += 240
        self.assertEqual(client.token(), "atk_two")
        self.assertEqual(len(transport.requests), 2)


if __name__ == "__main__":
    unittest.main()

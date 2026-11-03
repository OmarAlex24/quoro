# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for the pluggable decision step, including the LLM path with a fake backend."""

from __future__ import annotations

import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping

from agent_runner import __version__
from agent_runner.policy import (
    USER_AGENT,
    DeterministicRestartPolicy,
    LlmPolicy,
    Outcome,
    UrlCheck,
    Wake,
    app_recovered,
    check_url,
)
from agent_runner.quoro import ToolResult
from fake_backend import FakeBackend

APP_ID = "app_01JTESTAPP00000000000000"
INCIDENT_ID = "inc_01JTESTINCIDENT0000000000"
AGENT_ID = "agt_01JTESTAGENT00000000000000"
GRANT_ID = "grt_01JTESTGRANT00000000000000"

TOOLS = ["get_incident", "get_app", "restart_app", "resolve_incident"]


class FakeSession:
    """An MCP session that records calls and can refuse selected tools."""

    def __init__(self, tools: list[str] = TOOLS, refuse: set[str] | None = None) -> None:
        self._tools = [{"name": name, "description": name, "inputSchema": {}} for name in tools]
        self._refuse = refuse or set()
        self.calls: list[dict[str, Any]] = []

    def list_tools(self) -> list[Mapping[str, Any]]:
        return self._tools

    def tool_names(self) -> set[str]:
        return {str(tool["name"]) for tool in self._tools}

    def call(
        self,
        name: str,
        arguments: Mapping[str, Any],
        justification: str | None = None,
    ) -> ToolResult:
        self.calls.append(
            {"tool": name, "arguments": dict(arguments), "justification": justification}
        )
        if name in self._refuse:
            return ToolResult(
                ok=False, problem={"code": "forbidden", "reason": "scope_missing"}
            )
        if name == "get_app":
            return ToolResult(ok=True, value={"instances": [{"status": "running"}]})
        return ToolResult(ok=True, value={})


def _wake(grant_id: str | None = None) -> Wake:
    wake = {"agent_id": AGENT_ID}
    if grant_id is not None:
        wake["grant_id"] = grant_id
    return Wake(
        incident_id=INCIDENT_ID,
        agent_id=AGENT_ID,
        grant_id=grant_id,
        event={
            "type": "incident.opened",
            "data": {
                "incident_id": INCIDENT_ID,
                "title": "health check failed",
                "subject": {"type": "app", "id": APP_ID},
                "wake": wake,
            },
        },
    )


class AppRecoveredTests(unittest.TestCase):
    def test_all_instances_running(self) -> None:
        self.assertTrue(app_recovered({"instances": [{"status": "running"}]}))

    def test_one_instance_down(self) -> None:
        self.assertFalse(
            app_recovered({"instances": [{"status": "running"}, {"status": "crashed"}]})
        )

    def test_tolerates_missing_fields(self) -> None:
        self.assertFalse(app_recovered({}))
        self.assertFalse(app_recovered({"instances": []}))
        self.assertFalse(app_recovered({"instances": [{}]}))
        self.assertTrue(app_recovered({"instances": [{"observed_status": "running"}]}))


class EdgeHandler(BaseHTTPRequestHandler):
    """An edge that refuses the default urllib agent, as some CDNs do."""

    seen: list[str] = []

    def do_GET(self) -> None:  # noqa: N802
        agent = self.headers.get("User-Agent", "")
        EdgeHandler.seen.append(agent)
        if self.path == "/down":
            status = 503
        elif agent.startswith("Python-urllib"):
            status = 403
        else:
            status = 200
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        del format, args


class CheckUrlTests(unittest.TestCase):
    def setUp(self) -> None:
        EdgeHandler.seen = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), EdgeHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def test_sends_the_runner_user_agent(self) -> None:
        seen = check_url(f"{self.base}/", 5.0)

        self.assertEqual(seen, UrlCheck(ok=True, detail="HTTP 200"))
        self.assertEqual(EdgeHandler.seen, [f"quoro-agent-runner/{__version__}"])
        self.assertEqual(USER_AGENT, f"quoro-agent-runner/{__version__}")

    def test_reports_the_status_it_saw(self) -> None:
        seen = check_url(f"{self.base}/down", 5.0)

        self.assertEqual(seen, UrlCheck(ok=False, detail="HTTP 503"))

    def test_reports_a_url_that_does_not_answer(self) -> None:
        port = self.server.server_address[1]
        self.server.shutdown()
        self.server.server_close()

        seen = check_url(f"http://127.0.0.1:{port}/", 2.0)

        self.assertFalse(seen.ok)
        self.assertTrue(seen.detail.startswith("no answer"), seen.detail)


class LlmPolicyTests(unittest.TestCase):
    def test_calls_tools_in_order_and_injects_the_incident_id(self) -> None:
        backend = FakeBackend(
            [
                {"tool": "get_app", "arguments": {"app_id": APP_ID}},
                {
                    "tool": "restart_app",
                    "arguments": {"app_id": APP_ID, "justification": "restart after an incident"},
                },
                {"done": "restarted and verified"},
            ]
        )
        session = FakeSession()

        outcome: Outcome = LlmPolicy(backend).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertEqual([call["tool"] for call in session.calls], ["get_app", "restart_app"])
        restart = session.calls[1]["arguments"]
        self.assertEqual(restart["incident_id"], INCIDENT_ID)
        self.assertEqual(restart["justification"], "restart after an incident")
        self.assertEqual(outcome.summary, "restarted and verified")
        self.assertTrue(all(action.ok for action in outcome.actions))

        self.assertEqual(len(backend.seen), 3)
        final_messages = backend.seen[-1][0]
        self.assertEqual(final_messages[0]["role"], "system")
        self.assertIn("incident_id", final_messages[0]["content"])
        self.assertTrue(any(message["role"] == "tool" for message in final_messages))

    def test_stops_at_a_refusal(self) -> None:
        backend = FakeBackend(
            [
                {"tool": "get_app", "arguments": {"app_id": APP_ID}},
                {
                    "tool": "restart_app",
                    "arguments": {"app_id": APP_ID, "justification": "restart after an incident"},
                },
                {"tool": "get_app", "arguments": {"app_id": APP_ID}},
            ]
        )
        session = FakeSession(refuse={"restart_app"})

        outcome = LlmPolicy(backend).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertEqual([call["tool"] for call in session.calls], ["get_app", "restart_app"])
        self.assertIn("forbidden", outcome.summary)
        self.assertEqual(len(backend.seen), 2, "the backend was asked again after a refusal")

    def test_refuses_to_call_a_mutating_tool_without_justification(self) -> None:
        backend = FakeBackend(
            [{"tool": "restart_app", "arguments": {"app_id": APP_ID}}, {"done": "never"}]
        )
        session = FakeSession()

        outcome = LlmPolicy(backend).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertEqual(session.calls, [])
        self.assertIn("justification", outcome.summary)

    def test_caps_the_number_of_steps(self) -> None:
        backend = FakeBackend(
            [{"tool": "get_app", "arguments": {"app_id": APP_ID}}] * 50
        )
        session = FakeSession()

        outcome = LlmPolicy(backend, max_steps=3).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertEqual(len(session.calls), 3)
        self.assertIn("3 steps", outcome.summary)


class DeterministicPolicyTests(unittest.TestCase):
    def test_a_non_app_subject_does_nothing(self) -> None:
        session = FakeSession()
        wake = Wake(
            incident_id=INCIDENT_ID,
            agent_id=AGENT_ID,
            event={"data": {"subject": {"type": "node", "id": "nod_test"}}},
        )

        outcome = DeterministicRestartPolicy().handle(wake, session)  # type: ignore[arg-type]

        self.assertEqual([call["tool"] for call in session.calls], ["get_incident"])
        self.assertIsNone(outcome.recovered)
        self.assertIn("not an app", outcome.summary)

    def test_the_grant_is_named_wherever_the_incident_is(self) -> None:
        session = FakeSession()
        policy = DeterministicRestartPolicy(
            resolve_incident=True, poll_interval_s=0.0, poll_attempts=1
        )

        policy.handle(_wake(GRANT_ID), session)  # type: ignore[arg-type]

        restart = next(call for call in session.calls if call["tool"] == "restart_app")
        self.assertIn(GRANT_ID, restart["arguments"]["justification"])
        resolve = next(
            call for call in session.calls if call["tool"] == "resolve_incident"
        )
        self.assertIn(GRANT_ID, resolve["arguments"]["justification"])

    def test_a_refused_check_url_names_the_status_in_the_outcome(self) -> None:
        session = FakeSession()
        policy = DeterministicRestartPolicy(
            check_url="https://app.example/",
            resolve_incident=True,
            poll_interval_s=0.0,
            poll_attempts=1,
            health_check=lambda url: UrlCheck(ok=False, detail="HTTP 403"),
        )

        outcome = policy.handle(_wake(), session)  # type: ignore[arg-type]

        self.assertFalse(outcome.recovered)
        self.assertIn("answered HTTP 403", outcome.summary)
        self.assertNotIn(
            "resolve_incident", [call["tool"] for call in session.calls]
        )

    def test_no_grant_id_leaves_the_text_unchanged(self) -> None:
        session = FakeSession()

        DeterministicRestartPolicy(
            poll_interval_s=0.0, poll_attempts=1
        ).handle(_wake(), session)  # type: ignore[arg-type]

        restart = next(call for call in session.calls if call["tool"] == "restart_app")
        self.assertNotIn("grant grt_", restart["arguments"]["justification"])


if __name__ == "__main__":
    unittest.main()

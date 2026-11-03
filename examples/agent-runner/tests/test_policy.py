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
    app_healthy,
    check_url,
    resolved_by_app_up,
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

    def __init__(
        self,
        tools: list[str] = TOOLS,
        refuse: set[str] | None = None,
        answers: Mapping[str, list[ToolResult]] | None = None,
    ) -> None:
        self._tools = [{"name": name, "description": name, "inputSchema": {}} for name in tools]
        self._refuse = refuse or set()
        # Scripted answers per tool, consumed in order; the last one repeats.
        self._answers = {name: list(results) for name, results in (answers or {}).items()}
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
        script = self._answers.get(name)
        if script:
            return script.pop(0) if len(script) > 1 else script[0]
        if name == "get_app":
            return HEALTHY
        return ToolResult(ok=True, value={})

    def count(self, name: str) -> int:
        return sum(1 for call in self.calls if call["tool"] == name)


def _app(status: str) -> ToolResult:
    """A ``get_app`` answer: the VM runs whatever the app's health says."""
    return ToolResult(
        ok=True,
        value={"status": status, "instances": [{"observed_status": "running"}]},
    )


HEALTHY = _app("running")
FAILING = _app("down")
REVOKED = ToolResult(ok=False, problem={"code": "forbidden", "reason": "grant_revoked"})


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


class AppHealthyTests(unittest.TestCase):
    def test_the_app_status_carries_the_health_check(self) -> None:
        self.assertTrue(app_healthy({"status": "running"}))
        for status in ("down", "degraded", "deploying", "stopped"):
            self.assertFalse(app_healthy({"status": status}), status)

    def test_a_running_vm_with_failing_health_is_not_healthy(self) -> None:
        self.assertFalse(app_healthy(FAILING.value))

    def test_tolerates_missing_fields(self) -> None:
        self.assertFalse(app_healthy(None))
        self.assertFalse(app_healthy({}))
        self.assertFalse(app_healthy({"instances": [{"observed_status": "running"}]}))

    def test_only_the_system_resolving_means_app_up(self) -> None:
        self.assertTrue(resolved_by_app_up({"status": "resolved", "resolved_by_type": "system"}))
        self.assertFalse(resolved_by_app_up({"status": "resolved", "resolved_by_type": "user"}))
        self.assertFalse(resolved_by_app_up({"status": "open", "resolved_by_type": None}))
        self.assertFalse(resolved_by_app_up(None))


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


class RecoveryWaitTests(unittest.TestCase):
    """The deterministic policy waits for the app's health, not the VM."""

    def policy(self, **options: Any) -> DeterministicRestartPolicy:
        self.slept: list[float] = []
        options.setdefault("poll_attempts", 4)
        options.setdefault("poll_interval_s", 5.0)
        return DeterministicRestartPolicy(sleep=self.slept.append, **options)

    def test_a_running_vm_with_failing_health_never_recovers(self) -> None:
        session = FakeSession(answers={"get_app": [FAILING]})

        outcome = self.policy(resolve_incident=True).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertFalse(outcome.recovered)
        self.assertIn("did not report healthy", outcome.summary)
        self.assertIn("4 reads over 20s", outcome.summary)
        self.assertIn("last status down", outcome.summary)
        # One read before the restart, then the whole bounded budget.
        self.assertEqual(session.count("get_app"), 1 + 4)
        self.assertEqual(self.slept, [5.0, 5.0, 5.0])
        self.assertEqual(session.count("resolve_incident"), 0)

    def test_recovers_once_the_health_check_passes(self) -> None:
        session = FakeSession(answers={"get_app": [FAILING, FAILING, FAILING, HEALTHY]})

        outcome = self.policy(resolve_incident=True).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertTrue(outcome.recovered)
        self.assertIn("health check passing", outcome.summary)
        # The pre-restart read, two failing polls, then the healthy one.
        self.assertEqual(session.count("get_app"), 4)
        self.assertEqual(session.count("resolve_incident"), 1)
        self.assertTrue(outcome.resolved)

    def test_a_revoked_grant_after_app_up_reads_the_incident(self) -> None:
        resolved = ToolResult(
            ok=True,
            value={"id": INCIDENT_ID, "status": "resolved", "resolved_by_type": "system"},
        )
        session = FakeSession(
            answers={
                "get_app": [FAILING, FAILING, REVOKED],
                "get_incident": [ToolResult(ok=True, value={}), resolved],
            }
        )

        outcome = self.policy(resolve_incident=True).handle(_wake(), session)  # type: ignore[arg-type]

        self.assertTrue(outcome.recovered)
        self.assertIn("app.up", outcome.summary)
        self.assertEqual(session.count("resolve_incident"), 0, "already resolved")
        self.assertFalse(outcome.resolved)

    def test_a_revoked_grant_without_a_readable_incident_is_not_a_recovery(self) -> None:
        session = FakeSession(
            answers={
                "get_app": [FAILING, REVOKED],
                "get_incident": [ToolResult(ok=True, value={}), REVOKED],
            }
        )

        outcome = self.policy().handle(_wake(), session)  # type: ignore[arg-type]

        self.assertFalse(outcome.recovered)
        self.assertIn("not confirmed", outcome.summary)

    def test_a_revoked_grant_leaves_the_verdict_to_check_url(self) -> None:
        session = FakeSession(
            answers={
                "get_app": [FAILING, REVOKED],
                "get_incident": [ToolResult(ok=True, value={}), REVOKED],
            }
        )
        policy = self.policy(
            check_url="https://app.example/",
            health_check=lambda url: UrlCheck(ok=True, detail="HTTP 200"),
        )

        outcome = policy.handle(_wake(), session)  # type: ignore[arg-type]

        self.assertTrue(outcome.recovered)
        self.assertIn("answered HTTP 200", outcome.summary)

    def test_check_url_is_not_asked_while_quoro_reports_the_app_down(self) -> None:
        asked: list[str] = []
        session = FakeSession(answers={"get_app": [FAILING]})
        policy = self.policy(
            check_url="https://app.example/",
            health_check=lambda url: asked.append(url) or UrlCheck(ok=True, detail="HTTP 200"),
        )

        outcome = policy.handle(_wake(), session)  # type: ignore[arg-type]

        self.assertFalse(outcome.recovered)
        self.assertEqual(asked, [])

    def test_another_refusal_stops_the_wait(self) -> None:
        scope_missing = ToolResult(
            ok=False, problem={"code": "forbidden", "reason": "scope_missing"}
        )
        session = FakeSession(answers={"get_app": [FAILING, scope_missing]})

        outcome = self.policy().handle(_wake(), session)  # type: ignore[arg-type]

        self.assertFalse(outcome.recovered)
        self.assertIn("refused (forbidden)", outcome.summary)
        self.assertEqual(session.count("get_app"), 2)


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

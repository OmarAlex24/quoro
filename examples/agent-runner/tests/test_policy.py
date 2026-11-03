# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for the pluggable decision step, including the LLM path with a fake backend."""

from __future__ import annotations

import unittest
from typing import Any, Mapping

from agent_runner.policy import (
    DeterministicRestartPolicy,
    LlmPolicy,
    Outcome,
    Wake,
    app_recovered,
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

    def test_no_grant_id_leaves_the_text_unchanged(self) -> None:
        session = FakeSession()

        DeterministicRestartPolicy(
            poll_interval_s=0.0, poll_attempts=1
        ).handle(_wake(), session)  # type: ignore[arg-type]

        restart = next(call for call in session.calls if call["tool"] == "restart_app")
        self.assertNotIn("grant grt_", restart["arguments"]["justification"])


if __name__ == "__main__":
    unittest.main()

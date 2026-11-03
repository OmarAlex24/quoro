# SPDX-License-Identifier: AGPL-3.0-only
"""End-to-end tests of the webhook receiver against a fake Quoro.

Every test runs fully offline: a fake Quoro and the runner both listen on
``127.0.0.1:0``. Polling is shortened through config so the suite stays fast.
"""

from __future__ import annotations

import http.client
import json
import os
import threading
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from agent_runner.config import AgentConfig, RunnerConfig
from agent_runner.policy import Outcome, Wake
from agent_runner.server import create_server
from agent_runner.signature import sign
from fake_quoro import ACCESS_TOKEN, AGENT_KEY, FakeQuoro

APP_ID = "app_01JTESTAPP00000000000000"
INCIDENT_ID = "inc_01JTESTINCIDENT0000000000"
AGENT_ID = "agt_01JTESTAGENT00000000000000"
DEFAULT_AGENT_ID = "agt_01JTESTSTANDBY000000000000"
UNKNOWN_AGENT_ID = "agt_01JXNNVWNAGENT000000000000"
GRANT_ID = "grt_01JTESTGRANT00000000000000"
WEBHOOK_SECRET = "test-webhook-secret"
WEBHOOK_SECRET_ENV = "QUORO_TEST_WEBHOOK_SECRET"
AGENT_KEY_ENV = "QUORO_TEST_AGENT_KEY"


def _post(
    port: int, body: bytes, headers: dict[str, str]
) -> tuple[int, dict[str, Any]]:
    """POST to the runner and return ``(status, decoded JSON body)``."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request("POST", "/webhook", body=body, headers=headers)
        response = connection.getresponse()
        payload = response.read()
        return response.status, json.loads(payload) if payload else {}
    finally:
        connection.close()


UNWAKED = object()


class RecordingPolicy:
    """A policy that records the wakes it receives and does nothing else."""

    def __init__(self) -> None:
        self.wakes: list[Wake] = []
        self._lock = threading.Lock()

    def handle(self, wake: Wake, mcp: Any) -> Outcome:
        del mcp
        with self._lock:
            self.wakes.append(wake)
        return Outcome(incident_id=wake.incident_id, summary="recorded")


class RunnerTestCase(unittest.TestCase):
    """Start one fake Quoro and one runner for each test."""

    resolve_incident = True

    def make_config(self) -> RunnerConfig:
        return RunnerConfig(
            listen_host="127.0.0.1",
            listen_port=0,
            quoro_url=self.fake.base_url,
            webhook_secret_env=WEBHOOK_SECRET_ENV,
            agents={
                AGENT_ID: AgentConfig(agent_id=AGENT_ID, key_env=AGENT_KEY_ENV),
            },
            resolve_incident=self.resolve_incident,
            poll_attempts=5,
            poll_interval_s=0.01,
            http_timeout_s=5.0,
        )

    def make_policy(self) -> Any:
        """The policy to run; ``None`` builds the configured one."""
        return None

    def setUp(self) -> None:
        os.environ[WEBHOOK_SECRET_ENV] = WEBHOOK_SECRET
        os.environ[AGENT_KEY_ENV] = AGENT_KEY

        self.fake = FakeQuoro(app_id=APP_ID, incident_id=INCIDENT_ID).start()
        self.addCleanup(self.fake.stop)

        self.config = self.make_config()
        self.server = create_server(self.config, policy=self.make_policy())
        self.port = int(self.server.server_address[1])
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server.app.wait_idle(timeout=5.0)
        self.thread.join(timeout=5)

    def event(
        self,
        *,
        event_type: str = "incident.opened",
        agent_id: str | None = AGENT_ID,
        grant_id: str | None = None,
        wake: Any = UNWAKED,
        incident_id: str = INCIDENT_ID,
    ) -> dict[str, Any]:
        """Build an ``incident.opened`` delivery.

        ``wake`` overrides the whole ``data.wake`` value: pass ``None`` for a
        delivery without wake (the webhook had none, or it is a test delivery)
        or any other value for a malformed one.
        """
        if wake is UNWAKED:
            wake = {"agent_id": agent_id} if agent_id is not None else {}
            if grant_id is not None:
                wake["grant_id"] = grant_id

        data: dict[str, Any] = {
            "incident_id": incident_id,
            "title": "health check failed",
            "trigger_event_id": "evt_01JTESTEVENT000000000000",
            "subject": {"type": "app", "id": APP_ID},
        }
        if wake is not None:
            data["wake"] = wake
        return {
            "schema_version": 1,
            "id": "evt_01JTESTEVENT000000000000",
            "seq": 1,
            "type": event_type,
            "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": "primary",
            "subject": {"type": "app", "id": APP_ID},
            "incident_id": incident_id,
            "data": data,
        }

    def deliver(
        self,
        event: dict[str, Any],
        *,
        delivery_id: str = "dlv_01JTESTDELIVERY0000000000",
        secret: str = WEBHOOK_SECRET,
        signature: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        body = json.dumps(event).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Quoro-Delivery-Id": delivery_id,
            "Quoro-Event-Id": str(event.get("id", "")),
            "Quoro-Event-Type": str(event.get("type", "")),
            "Quoro-Signature": signature if signature is not None else sign(body, secret),
        }
        return _post(self.port, body, headers)


class IncidentFlowTests(RunnerTestCase):
    def test_restarts_the_app_and_resolves_the_incident(self) -> None:
        status, payload = self.deliver(self.event())

        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "accepted")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))

        self.assertTrue(self.fake.mints, "the runner never minted an access token")
        for mint in self.fake.mints:
            self.assertEqual(mint["key"], AGENT_KEY)
            self.assertTrue(mint["justification"].strip())

        restarts = self.fake.calls_named("restart_app")
        self.assertEqual(len(restarts), 1)
        arguments = restarts[0]["arguments"]
        self.assertEqual(arguments["app_id"], APP_ID)
        self.assertEqual(arguments["incident_id"], INCIDENT_ID)
        self.assertTrue(str(arguments["justification"]).strip())
        self.assertIn(INCIDENT_ID, arguments["justification"])

        self.assertTrue(self.fake.calls, "the runner made no MCP calls")
        for call in self.fake.calls:
            self.assertEqual(call["token"], ACCESS_TOKEN)

        names = [call["tool"] for call in self.fake.calls]
        self.assertEqual(names[0], "get_incident")
        restart = names.index("restart_app")
        # The VM runs right after the restart, but the runner waits until
        # Quoro reports the app healthy: two reads in the fake.
        self.assertEqual(
            names[restart + 1 :], ["get_app", "get_app", "get_incident", "resolve_incident"]
        )
        self.assertEqual(self.fake.health, "passing")

        resolves = self.fake.calls_named("resolve_incident")
        self.assertEqual(len(resolves), 1)
        self.assertEqual(resolves[0]["arguments"]["incident_id"], INCIDENT_ID)

    def test_bad_signature_is_rejected_before_anything_else(self) -> None:
        status, payload = self.deliver(
            self.event(), secret="the-wrong-secret"
        )

        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "invalid_signature")
        self.assertEqual(self.fake.paths, [])
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.fake.mints, [])

    def test_missing_signature_is_rejected(self) -> None:
        body = json.dumps(self.event()).encode("utf-8")
        status, _ = _post(
            self.port,
            body,
            {
                "Content-Type": "application/json",
                "Quoro-Delivery-Id": "dlv_01JTESTDELIVERY0000000001",
            },
        )
        self.assertEqual(status, 401)
        self.assertEqual(self.fake.paths, [])

    def test_duplicate_delivery_acts_once(self) -> None:
        event = self.event()
        first_status, _ = self.deliver(event)
        self.assertEqual(first_status, 202)
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        calls_after_first = len(self.fake.calls)
        mints_after_first = len(self.fake.mints)
        self.assertEqual(len(self.fake.calls_named("restart_app")), 1)

        second_status, payload = self.deliver(event)

        self.assertEqual(second_status, 200)
        self.assertEqual(payload["status"], "duplicate")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(len(self.fake.calls), calls_after_first)
        self.assertEqual(len(self.fake.mints), mints_after_first)

    def test_other_event_types_are_ignored(self) -> None:
        status, payload = self.deliver(self.event(event_type="app.down"))

        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "ignored")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(self.fake.mints, [])
        self.assertEqual(self.fake.calls, [])

    def test_an_unknown_agent_is_ignored(self) -> None:
        status, payload = self.deliver(self.event(agent_id=UNKNOWN_AGENT_ID))

        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "ignored")
        self.assertEqual(payload["error"], "unknown_agent")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(self.fake.mints, [])
        self.assertEqual(self.fake.calls, [])

    def test_a_delivery_without_an_agent_is_ignored_without_a_default(self) -> None:
        status, payload = self.deliver(self.event(agent_id=None))

        self.assertEqual(status, 202)
        self.assertEqual(payload["error"], "no_agent")
        self.assertEqual(self.fake.calls, [])

    def test_a_delivery_without_wake_is_ignored_without_a_default(self) -> None:
        status, payload = self.deliver(self.event(wake=None))

        self.assertEqual(status, 202)
        self.assertEqual(payload["error"], "no_agent")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(self.fake.calls, [])


class DefaultAgentTests(RunnerTestCase):
    """A delivery that names no agent falls back to ``default_agent_id``."""

    def make_config(self) -> RunnerConfig:
        return replace(super().make_config(), default_agent_id=AGENT_ID)

    def test_falls_back_to_the_default_agent(self) -> None:
        status, payload = self.deliver(self.event(agent_id=None))

        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "accepted")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(len(self.fake.calls_named("restart_app")), 1)

    def test_a_malformed_agent_id_falls_back_to_the_default(self) -> None:
        status, payload = self.deliver(self.event(agent_id="agt_not-a-ulid"))

        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "accepted")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(len(self.fake.calls_named("restart_app")), 1)


class WakeSelectionTests(RunnerTestCase):
    """``data.wake`` chooses the agent; only a valid one is honoured."""

    def make_config(self) -> RunnerConfig:
        return replace(
            super().make_config(),
            default_agent_id=DEFAULT_AGENT_ID,
            agents={
                AGENT_ID: AgentConfig(agent_id=AGENT_ID, key_env=AGENT_KEY_ENV),
                DEFAULT_AGENT_ID: AgentConfig(
                    agent_id=DEFAULT_AGENT_ID, key_env=AGENT_KEY_ENV
                ),
            },
        )

    def make_policy(self) -> Any:
        self.policy = RecordingPolicy()
        return self.policy

    def _wake_after(self, event: dict[str, Any]) -> Wake:
        status, payload = self.deliver(event)
        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "accepted")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertTrue(self.policy.wakes, "the policy never received a wake")
        return self.policy.wakes[-1]

    def test_wake_names_the_agent_and_its_grant(self) -> None:
        wake = self._wake_after(self.event(agent_id=AGENT_ID, grant_id=GRANT_ID))

        self.assertEqual(wake.agent_id, AGENT_ID)
        self.assertEqual(wake.grant_id, GRANT_ID)

    def test_a_malformed_grant_id_is_dropped(self) -> None:
        wake = self._wake_after(self.event(agent_id=AGENT_ID, grant_id="grt_nope"))

        self.assertEqual(wake.agent_id, AGENT_ID)
        self.assertIsNone(wake.grant_id)

    def test_no_wake_uses_the_default_agent(self) -> None:
        wake = self._wake_after(self.event(wake=None))

        self.assertEqual(wake.agent_id, DEFAULT_AGENT_ID)
        self.assertIsNone(wake.grant_id)

    def test_a_wake_that_is_not_a_dict_uses_the_default_agent(self) -> None:
        wake = self._wake_after(self.event(wake="agt_01JTESTAGENT00000000000000"))

        self.assertEqual(wake.agent_id, DEFAULT_AGENT_ID)

    def test_a_wake_without_a_valid_pattern_uses_the_default_agent(self) -> None:
        wake = self._wake_after(
            self.event(wake={"agent_id": "agt_not-a-ulid", "grant_id": GRANT_ID})
        )

        self.assertEqual(wake.agent_id, DEFAULT_AGENT_ID)
        self.assertIsNone(
            wake.grant_id, "a grant id without the agent it was issued to was kept"
        )

    def test_a_wake_naming_an_agent_without_a_key_is_unknown(self) -> None:
        status, payload = self.deliver(self.event(agent_id=UNKNOWN_AGENT_ID))

        self.assertEqual(status, 202)
        self.assertEqual(payload["error"], "unknown_agent")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(self.policy.wakes, [])
        self.assertEqual(self.fake.mints, [])


class HealthWaitTests(RunnerTestCase):
    """What the runner reports depends on the app's health, not on its VM."""

    def run_incident(self) -> str:
        """Deliver one incident and return the outcome the runner logged."""
        with self.assertLogs("agent_runner.server", level="INFO") as logs:
            status, _ = self.deliver(self.event())
            self.assertEqual(status, 202)
            self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        outcomes = [line for line in logs.output if "outcome for incident" in line]
        self.assertEqual(len(outcomes), 1, logs.output)
        return outcomes[0]

    def test_a_running_vm_with_failing_health_is_not_recovered(self) -> None:
        self.fake.health_passes_after_reads = None

        outcome = self.run_incident()

        self.assertIn("did not report healthy", outcome)
        self.assertIn("last status down", outcome)
        self.assertEqual(self.fake.instance_status, "running")
        # The pre-restart read and the whole poll budget, then nothing more.
        self.assertEqual(len(self.fake.calls_named("get_app")), 1 + self.config.poll_attempts)
        self.assertEqual(self.fake.calls_named("resolve_incident"), [])

    def test_app_up_resolving_the_incident_is_a_recovery(self) -> None:
        self.fake.app_up_resolves = True

        outcome = self.run_incident()

        self.assertIn("recovered after restart", outcome)
        self.assertEqual(self.fake.resolved_by_type, "system")
        self.assertEqual(
            self.fake.calls_named("resolve_incident"), [], "the incident was already resolved"
        )

    def test_a_revoked_grant_without_other_reads_is_not_confirmed(self) -> None:
        self.fake.app_up_resolves = True
        self.fake.standing_reads = False

        outcome = self.run_incident()

        self.assertIn("did not report healthy", outcome)
        self.assertIn("not confirmed", outcome)
        names = [call["tool"] for call in self.fake.calls]
        self.assertEqual(names[-2:], ["get_app", "get_incident"])
        self.assertEqual(self.fake.calls_named("resolve_incident"), [])


class RefusalTests(RunnerTestCase):
    def test_a_refused_restart_stops_the_run(self) -> None:
        self.fake.refuse_restart = {
            "code": "forbidden",
            "reason": "scope_missing",
            "scope": "app:restart",
            "target": {"type": "app", "id": APP_ID},
        }

        status, _ = self.deliver(self.event())
        self.assertEqual(status, 202)
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))

        self.assertEqual(len(self.fake.calls_named("restart_app")), 1)
        self.assertEqual(self.fake.calls_named("resolve_incident"), [])
        self.assertEqual(
            [call["tool"] for call in self.fake.calls][-1],
            "restart_app",
            "the runner kept calling tools after a refusal",
        )


if __name__ == "__main__":
    unittest.main()

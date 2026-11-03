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
from agent_runner.server import create_server
from agent_runner.signature import sign
from fake_quoro import ACCESS_TOKEN, AGENT_KEY, FakeQuoro

APP_ID = "app_01JTESTAPP00000000000000"
INCIDENT_ID = "inc_01JTESTINCIDENT0000000000"
AGENT_ID = "agt_01JTESTAGENT00000000000000"
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

    def setUp(self) -> None:
        os.environ[WEBHOOK_SECRET_ENV] = WEBHOOK_SECRET
        os.environ[AGENT_KEY_ENV] = AGENT_KEY

        self.fake = FakeQuoro(app_id=APP_ID, incident_id=INCIDENT_ID).start()
        self.addCleanup(self.fake.stop)

        self.config = self.make_config()
        self.server = create_server(self.config)
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
        incident_id: str = INCIDENT_ID,
    ) -> dict[str, Any]:
        wake = {"agent_id": agent_id} if agent_id is not None else {}
        return {
            "schema_version": 1,
            "id": "evt_01JTESTEVENT000000000000",
            "seq": 1,
            "type": event_type,
            "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": "primary",
            "subject": {"type": "app", "id": APP_ID},
            "incident_id": incident_id,
            "data": {
                "incident_id": incident_id,
                "title": "health check failed",
                "trigger_event_id": "evt_01JTESTEVENT000000000000",
                "subject": {"type": "app", "id": APP_ID},
                "wake": wake,
            },
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
        self.assertGreaterEqual(names.count("get_app"), 2, "no recovery poll happened")
        last_get_app = max(index for index, name in enumerate(names) if name == "get_app")
        self.assertGreater(last_get_app, names.index("restart_app"))
        self.assertEqual(self.fake.instance_status, "running")

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
        status, payload = self.deliver(
            self.event(agent_id="agt_01JUNKNOWNAGENT0000000000")
        )

        self.assertEqual(status, 202)
        self.assertEqual(payload["status"], "ignored")
        self.assertTrue(self.server.app.wait_idle(timeout=5.0))
        self.assertEqual(self.fake.mints, [])
        self.assertEqual(self.fake.calls, [])

    def test_a_delivery_without_an_agent_is_ignored_without_a_default(self) -> None:
        status, payload = self.deliver(self.event(agent_id=None))

        self.assertEqual(status, 202)
        self.assertEqual(payload["error"], "no_agent")
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

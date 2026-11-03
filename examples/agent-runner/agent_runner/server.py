# SPDX-License-Identifier: AGPL-3.0-only
"""Webhook receiver that wakes an agent for ``incident.opened``.

The HTTP handler only reads and authenticates the delivery; the agent runs in a
background thread so Quoro's dispatcher never waits on it. Deliveries are
deduplicated by ``Quoro-Delivery-Id`` because Quoro retries them, and each
incident has a lock so two different deliveries for one incident cannot act at
the same time.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import OrderedDict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from .config import ConfigError, RunnerConfig
from .policy import Outcome, Policy, Wake, build_policy
from .quoro import HttpTransport, McpSession, QuoroClient
from .signature import verify

LOGGER = logging.getLogger("agent_runner.server")

WEBHOOK_PATH = "/webhook"
SEEN_DELIVERY_LIMIT = 10_000
ADDRESS_IN_USE_HINT = "another process may already be listening on this address"

AGENT_ID_PATTERN = re.compile(r"^agt_[0-9A-HJKMNP-TV-Z]{26}$")
GRANT_ID_PATTERN = re.compile(r"^grt_[0-9A-HJKMNP-TV-Z]{26}$")


class SeenDeliveries:
    """Remember the last ``limit`` delivery ids, oldest evicted first."""

    def __init__(self, limit: int = SEEN_DELIVERY_LIMIT) -> None:
        self._limit = limit
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()

    def mark_seen(self, delivery_id: str) -> bool:
        """Return ``True`` when ``delivery_id`` was already seen, else record it."""
        with self._lock:
            if delivery_id in self._seen:
                self._seen.move_to_end(delivery_id)
                return True
            self._seen[delivery_id] = None
            if len(self._seen) > self._limit:
                self._seen.popitem(last=False)
            return False


class App:
    """Delivery handling, independent of the HTTP plumbing."""

    def __init__(
        self,
        config: RunnerConfig,
        *,
        policy: Policy | None = None,
        clock: Callable[[], float] = time.time,
        transport_factory: Callable[[], HttpTransport] | None = None,
    ) -> None:
        self.config = config
        self._policy = policy or build_policy(config)
        self._clock = clock
        self._secret = config.read_webhook_secret()
        self._seen = SeenDeliveries()
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._threads: set[threading.Thread] = set()
        self._threads_guard = threading.Lock()
        self._transport_factory = transport_factory or self._default_transport

    def _default_transport(self) -> HttpTransport:
        return HttpTransport(
            self.config.quoro_url,
            tls_fingerprint=self.config.tls_fingerprint,
            ca_file=str(self.config.ca_file) if self.config.ca_file else None,
            timeout=self.config.http_timeout_s,
        )

    def process(self, headers: Mapping[str, str], body: bytes) -> tuple[int, dict[str, Any]]:
        """Authenticate, deduplicate and enqueue one delivery; return the HTTP answer."""
        signature = _header(headers, "quoro-signature")
        if not verify(signature, body, self._secret, now=self._clock()):
            LOGGER.warning("rejected a delivery with a missing or invalid signature")
            return HTTPStatus.UNAUTHORIZED, {"error": "invalid_signature"}

        delivery_id = _header(headers, "quoro-delivery-id")
        if delivery_id:
            if self._seen.mark_seen(delivery_id):
                LOGGER.info("ignored duplicate delivery %s", delivery_id)
                return HTTPStatus.OK, {"status": "duplicate"}
        else:
            LOGGER.warning("delivery has no Quoro-Delivery-Id; acting without dedupe")

        try:
            event = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            LOGGER.warning("rejected a delivery whose body is not JSON")
            return HTTPStatus.BAD_REQUEST, {"error": "invalid_json"}
        if not isinstance(event, dict):
            LOGGER.warning("rejected a delivery whose body is not a JSON object")
            return HTTPStatus.BAD_REQUEST, {"error": "invalid_envelope"}

        event_type = event.get("type")
        if event_type != "incident.opened":
            LOGGER.info("ignored event type %s", event_type)
            return HTTPStatus.ACCEPTED, {"status": "ignored"}

        data = event.get("data")
        data = data if isinstance(data, dict) else {}
        incident_id = event.get("incident_id") or data.get("incident_id")
        if not isinstance(incident_id, str) or not incident_id:
            LOGGER.warning("incident.opened delivery carries no incident id")
            return HTTPStatus.ACCEPTED, {"status": "ignored", "error": "no_incident_id"}

        wake_payload = data.get("wake")
        wake_agent_id, grant_id = _wake_ids(wake_payload)
        if wake_agent_id is not None:
            agent_id = wake_agent_id
            LOGGER.info(
                "incident %s wakes agent %s from data.wake (grant %s)",
                incident_id,
                agent_id,
                grant_id or "not named",
            )
        else:
            if wake_payload is not None:
                LOGGER.warning(
                    "incident %s carries a data.wake without a usable agent_id; "
                    "falling back to default_agent_id",
                    incident_id,
                )
            agent_id = self.config.default_agent_id
            if agent_id:
                LOGGER.info(
                    "incident %s uses default_agent_id %s", incident_id, agent_id
                )
        if not isinstance(agent_id, str) or not agent_id:
            LOGGER.warning(
                "incident %s names no agent and no default_agent_id is configured",
                incident_id,
            )
            return HTTPStatus.ACCEPTED, {"status": "ignored", "error": "no_agent"}
        if agent_id not in self.config.agents:
            LOGGER.warning(
                "incident %s wakes agent %s, for which no key is configured",
                incident_id,
                agent_id,
            )
            return HTTPStatus.ACCEPTED, {"status": "ignored", "error": "unknown_agent"}

        wake = Wake(
            incident_id=incident_id,
            agent_id=agent_id,
            grant_id=grant_id,
            event=event,
        )
        self._spawn(wake)
        LOGGER.info(
            "accepted delivery for incident %s (agent %s, grant %s)",
            incident_id,
            agent_id,
            grant_id or "none",
        )
        return HTTPStatus.ACCEPTED, {"status": "accepted", "incident_id": incident_id}

    def _spawn(self, wake: Wake) -> None:
        thread = threading.Thread(
            target=self._run, args=(wake,), name=f"incident-{wake.incident_id}", daemon=True
        )
        with self._threads_guard:
            self._threads.add(thread)
        thread.start()

    def _run(self, wake: Wake) -> None:
        try:
            with self._lock_for(wake.incident_id):
                outcome = self._act(wake)
            LOGGER.info("outcome for incident %s: %s", wake.incident_id, outcome.summary)
        except ConfigError as error:
            LOGGER.error("cannot act on incident %s: %s", wake.incident_id, error)
        except Exception:  # noqa: BLE001 - a background thread must not die silently
            LOGGER.exception("acting on incident %s failed", wake.incident_id)
        finally:
            with self._threads_guard:
                self._threads.discard(threading.current_thread())

    def _act(self, wake: Wake) -> Outcome:
        agent = self.config.agents[wake.agent_id]
        subject = wake.event.get("subject") or {}
        target = f"{subject.get('type', 'resource')} {subject.get('id', '')}".strip()
        grant = f" under incident grant {wake.grant_id}" if wake.grant_id else ""
        client = QuoroClient(
            self._transport_factory(),
            agent.read_key(),
            wake.agent_id,
            justification=(
                f"Incident {wake.incident_id}: agent woken by incident.opened to "
                f"handle {target}{grant}."
            ),
        )
        session = McpSession(client)
        session.start()
        return self._policy.handle(wake, session)

    def _lock_for(self, incident_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(incident_id, threading.Lock())

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """Wait until no delivery is being handled; used by tests and shutdown."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._threads_guard:
                running = [thread for thread in self._threads if thread.is_alive()]
            if not running:
                return True
            running[0].join(0.05)
        return False


def _wake_ids(wake: Any) -> tuple[str | None, str | None]:
    """Validate ``data.wake`` and return its ``(agent_id, grant_id)``.

    Both fields are optional at this layer: a delivery without ``data.wake``
    (no wake on the webhook, or a test delivery) and a malformed one are left
    for the caller to handle. A value that is present but does not match the
    contract's ULID shape is treated as absent.
    """
    if not isinstance(wake, dict):
        return None, None

    agent_id = wake.get("agent_id")
    if not isinstance(agent_id, str) or not AGENT_ID_PATTERN.match(agent_id):
        # A grant without the agent it was issued to is not useful either.
        return None, None

    grant_id = wake.get("grant_id")
    if not isinstance(grant_id, str) or not GRANT_ID_PATTERN.match(grant_id):
        grant_id = None
    return agent_id, grant_id


def _header(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup over an ``email.message``-style mapping."""
    try:
        value = headers.get(name)
    except AttributeError:  # pragma: no cover - defensive for exotic mappings
        value = None
    return value if isinstance(value, str) else None


class WebhookHandler(BaseHTTPRequestHandler):
    """Serve exactly one route: ``POST /webhook``."""

    server: "WebhookServer"
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802 - http.server names these
        if urlsplit(self.path).path != WEBHOOK_PATH:
            self._respond(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        length_text = self.headers.get("Content-Length")
        try:
            length = int(length_text) if length_text is not None else -1
        except ValueError:
            length = -1
        if length < 0:
            self.close_connection = True
            self._respond(HTTPStatus.BAD_REQUEST, {"error": "length_required"})
            return
        if length > self.server.app.config.max_body_bytes:
            self.close_connection = True
            self._respond(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "body_too_large"})
            return

        body = self.rfile.read(length)
        status, payload = self.server.app.process(self.headers, body)
        self._respond(status, payload)

    def do_GET(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_PUT(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_DELETE(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def _method_not_allowed(self) -> None:
        if urlsplit(self.path).path == WEBHOOK_PATH:
            self._respond(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method_not_allowed"})
        else:
            self._respond(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _respond(self, status: int, payload: Mapping[str, Any]) -> None:
        body = json.dumps(dict(payload)).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Route http.server's access log into our logger instead of stderr."""
        LOGGER.debug("%s %s", self.address_string(), format % args)


class WebhookServer(ThreadingHTTPServer):
    """A threaded HTTP server that owns one :class:`App`."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], app: App) -> None:
        self.app = app
        super().__init__(address, WebhookHandler)


def create_server(config: RunnerConfig, policy: Policy | None = None) -> WebhookServer:
    """Build the receiver, bound but not yet serving."""
    app = App(config, policy=policy)
    return WebhookServer((config.listen_host, config.listen_port), app)

# SPDX-License-Identifier: AGPL-3.0-only
"""The decision step: what the woken agent does about an incident.

Two policies ship. :class:`DeterministicRestartPolicy` is the predictable one
used by the tests and the lab demo: read, restart, wait for the app's health,
optionally check a URL and resolve. :class:`LlmPolicy` hands the decision to a
pluggable backend while keeping the same rules (act inside the grant, justify
every mutation, never work around a refusal).
"""

from __future__ import annotations

import importlib
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

from . import __version__
from .config import RunnerConfig
from .quoro import MUTATING_TOOLS, McpSession

LOGGER = logging.getLogger("agent_runner.policy")

USER_AGENT = f"quoro-agent-runner/{__version__}"
"""The ``User-Agent`` of the ``check_url`` request. Some edges refuse the
default ``Python-urllib`` agent, so the runner names itself."""

DEFAULT_MAX_STEPS = 8

SYSTEM_PROMPT = """You are an incident agent operating a Quoro installation through its MCP server.

You were woken by an incident grant: a short-lived, narrowly scoped permission set
for one affected resource. Rules you must follow:

1. Act only within that grant. A refused call is final: do not retry it and do not
   use another tool to achieve the same effect.
2. Read before you change anything. Inspect the incident and the affected resource.
3. Every mutating tool call must include a non-empty justification and the incident_id.
4. Prefer the smallest action that can restore the service. Destructive actions stop
   at an approval and wait for a human.
5. When the service is healthy again, resolve the incident if you hold the scope;
   otherwise report what you found and stop.

Answer with exactly one action per turn."""


@dataclass(frozen=True)
class Wake:
    """The delivery that woke the agent.

    ``grant_id`` names the incident grant Quoro issued alongside the delivery
    (``data.wake.grant_id``). It is ``None`` when the delivery carried no wake,
    which is what a webhook without wake and a test delivery send.
    """

    incident_id: str
    agent_id: str
    grant_id: str | None = None
    event: Mapping[str, Any] = field(default_factory=dict)

    @property
    def data(self) -> Mapping[str, Any]:
        data = self.event.get("data")
        return data if isinstance(data, dict) else {}

    @property
    def grant_note(self) -> str:
        """Text naming the grant, to append where the incident is named."""
        return f" under incident grant {self.grant_id}" if self.grant_id else ""


@dataclass(frozen=True)
class Action:
    """One tool call the policy made, and whether Quoro accepted it."""

    tool: str
    arguments: Mapping[str, Any]
    ok: bool
    problem: Mapping[str, Any] | None = None


@dataclass
class Outcome:
    """What the policy did, and whether the app came back."""

    incident_id: str
    actions: list[Action] = field(default_factory=list)
    recovered: bool | None = None
    resolved: bool = False
    summary: str = ""


@dataclass(frozen=True)
class UrlCheck:
    """What a ``check_url`` request saw: whether it answered 200, and how."""

    ok: bool
    detail: str


def check_url(url: str, timeout_s: float) -> UrlCheck:
    """``GET`` ``url`` with system trust and the runner's ``User-Agent``.

    Only 200 is healthy. ``detail`` names the status the URL answered
    (``HTTP 403``) or why no answer arrived, so a refusal by an edge in front
    of the app is told apart from the app being down.
    """
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            status = response.status
    except urllib.error.HTTPError as error:
        with error:
            status = error.code
    except (urllib.error.URLError, OSError, ValueError) as error:
        reason = getattr(error, "reason", None) or error
        return UrlCheck(ok=False, detail=f"no answer ({reason})")
    return UrlCheck(ok=status == 200, detail=f"HTTP {status}")


class Policy(Protocol):
    """The pluggable decision step."""

    def handle(self, wake: Wake, mcp: McpSession) -> Outcome:
        """Act on ``wake`` through ``mcp`` and report the outcome."""
        ...


class LlmBackend(Protocol):
    """A provider-agnostic decision backend.

    ``messages`` is a list of chat-shaped dictionaries (roles ``system``,
    ``user``, ``assistant``, ``tool``) and ``tools`` is the MCP tool list. The
    backend returns either ``{"tool": name, "arguments": {...}}`` or
    ``{"done": "summary"}``.
    """

    def next_action(
        self, messages: list[dict[str, Any]], tools: list[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        ...


def app_healthy(detail: Mapping[str, Any] | None) -> bool:
    """Whether Quoro reports the app healthy.

    The app's aggregated ``status`` is ``running`` only while its instance runs
    *and* its health check passes; a failing health check makes it ``down``.
    An instance's ``observed_status`` follows the VM process alone, so a VM
    that runs while the app does not answer still says ``running`` there and
    is never read as a recovery.
    """
    return bool(detail) and detail.get("status") == "running"


def resolved_by_app_up(incident: Mapping[str, Any] | None) -> bool:
    """Whether the system resolved the incident, which ``app.up`` does."""
    return (
        bool(incident)
        and incident.get("status") == "resolved"
        and incident.get("resolved_by_type") == "system"
    )


@dataclass(frozen=True)
class Recovery:
    """What the wait after a restart found.

    ``healthy`` is ``True`` when Quoro reported the app healthy, ``False`` when
    it did not within the budget or a read was refused, and ``None`` when the
    incident grant was revoked before any health signal could be read (only
    ``check_url`` can then tell). ``incident_closed`` is set when the incident
    is already resolved, so the policy does not try to resolve it again.
    """

    healthy: bool | None
    note: str
    incident_closed: bool = False


def _subject_of(wake: Wake, detail: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """The incident subject, from the incident detail when available."""
    candidates = []
    if detail:
        candidates.append(detail.get("subject"))
    candidates.append(wake.data.get("subject"))
    candidates.append(wake.event.get("subject"))
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return None


def _title_of(wake: Wake, detail: Mapping[str, Any] | None) -> str:
    """A one-line incident title, with a safe fallback."""
    if detail:
        title = detail.get("title")
        if isinstance(title, str) and title.strip():
            return title.strip()
    for key in ("title", "last_error"):
        title = wake.data.get(key)
        if isinstance(title, str) and title.strip():
            return title.strip()
    return "service reported down"


class DeterministicRestartPolicy:
    """Read the incident, restart the app, wait for health, maybe resolve."""

    def __init__(
        self,
        *,
        check_url: str | None = None,
        resolve_incident: bool = False,
        poll_attempts: int = 12,
        poll_interval_s: float = 5.0,
        health_timeout_s: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        health_check: Callable[[str], UrlCheck] | None = None,
    ) -> None:
        self._check_url = check_url
        self._resolve_incident = resolve_incident
        self._poll_attempts = max(1, poll_attempts)
        self._poll_interval_s = max(0.0, poll_interval_s)
        self._health_timeout_s = health_timeout_s
        self._sleep = sleep
        self._health_check = health_check or self._http_check

    def _http_check(self, url: str) -> UrlCheck:
        return check_url(url, self._health_timeout_s)

    def handle(self, wake: Wake, mcp: McpSession) -> Outcome:
        outcome = Outcome(incident_id=wake.incident_id)
        tools = mcp.tool_names()

        detail: Mapping[str, Any] | None = None
        if "get_incident" in tools:
            result = mcp.call("get_incident", {"incident_id": wake.incident_id})
            if result.ok:
                detail = result.value
            else:
                LOGGER.warning(
                    "get_incident refused (%s); falling back to the event payload",
                    result.problem_code,
                )

        subject = _subject_of(wake, detail)
        if not subject or subject.get("type") != "app":
            outcome.summary = "incident subject is not an app; nothing to do"
            LOGGER.info("%s", outcome.summary)
            return outcome

        app_id = subject.get("id")
        if not isinstance(app_id, str) or not app_id.startswith("app_"):
            outcome.summary = "incident subject has no app id; nothing to do"
            LOGGER.info("%s", outcome.summary)
            return outcome

        title = _title_of(wake, detail)
        app = mcp.call("get_app", {"app_id": app_id})
        outcome.actions.append(Action("get_app", {"app_id": app_id}, app.ok, app.problem))
        if not app.ok:
            outcome.summary = f"get_app refused with {app.problem_code}; stopping"
            LOGGER.warning("%s", outcome.summary)
            return outcome

        justification = (
            f"Incident {wake.incident_id}{wake.grant_note}: app {app_id} reported down "
            f"({title}); restarting its instances as allowed by the incident grant."
        )
        restart = mcp.call(
            "restart_app",
            {
                "app_id": app_id,
                "incident_id": wake.incident_id,
                "justification": justification,
            },
            justification=justification,
        )
        outcome.actions.append(
            Action(
                "restart_app",
                {"app_id": app_id, "incident_id": wake.incident_id},
                restart.ok,
                restart.problem,
            )
        )
        if not restart.ok:
            outcome.summary = (
                f"restart_app refused ({restart.problem_code}"
                f"{'/' + restart.problem_reason if restart.problem_reason else ''}); stopping"
            )
            LOGGER.warning("%s", outcome.summary)
            return outcome

        recovery = self._wait_for_recovery(app_id, wake, mcp, tools)
        notes = [recovery.note]
        recovered = recovery.healthy
        if recovered is not False and self._check_url:
            seen = self._health_check(self._check_url)
            notes.append(f"check_url {self._check_url} answered {seen.detail}")
            if seen.ok:
                LOGGER.info("check_url %s answered %s", self._check_url, seen.detail)
            else:
                LOGGER.warning("check_url %s answered %s", self._check_url, seen.detail)
            recovered = seen.ok
        outcome.recovered = bool(recovered)

        if outcome.recovered:
            outcome.summary = f"app {app_id} recovered after restart: {'; '.join(notes)}"
        else:
            outcome.summary = (
                f"app {app_id} did not report healthy after restart: {'; '.join(notes)}"
            )

        if (
            outcome.recovered
            and self._resolve_incident
            and "resolve_incident" in tools
            and not recovery.incident_closed
            and not self._already_resolved(wake, mcp, tools)
        ):
            note = (
                f"Incident {wake.incident_id}{wake.grant_note}: app {app_id} recovered "
                "after restart issued by the agent runner."
            )
            resolved = mcp.call(
                "resolve_incident",
                {"incident_id": wake.incident_id, "justification": note},
                justification=note,
            )
            outcome.actions.append(
                Action(
                    "resolve_incident",
                    {"incident_id": wake.incident_id},
                    resolved.ok,
                    resolved.problem,
                )
            )
            outcome.resolved = resolved.ok
            if not resolved.ok:
                LOGGER.warning("resolve_incident refused with %s", resolved.problem_code)

        LOGGER.info("%s", outcome.summary)
        return outcome

    def _already_resolved(self, wake: Wake, mcp: McpSession, tools: set[str]) -> bool:
        """Whether the incident is resolved already, which app.up does on its own."""
        if "get_incident" not in tools:
            return False
        incident = mcp.call("get_incident", {"incident_id": wake.incident_id})
        value = incident.value if incident.ok and isinstance(incident.value, dict) else {}
        if value.get("status") == "resolved":
            LOGGER.info(
                "incident %s is already resolved by %s; not resolving it again",
                wake.incident_id,
                value.get("resolved_by_type"),
            )
            return True
        return False

    def _wait_for_recovery(
        self, app_id: str, wake: Wake, mcp: McpSession, tools: set[str]
    ) -> Recovery:
        """Poll ``get_app`` until Quoro reports the app healthy.

        The wait is bounded: at most ``poll_attempts`` reads,
        ``poll_interval_s`` apart. Quoro resolves the incident when the app
        comes back (``app.up``) and revokes the incident grant with it, so a
        read refused with ``grant_revoked`` is not a failure: the incident is
        read to learn whether ``app.up`` closed it, which it can only be when
        the agent holds another grant with ``incident:read``.
        """
        total_s = self._poll_attempts * self._poll_interval_s
        budget = f"{self._poll_attempts} reads over {total_s:g}s"
        last_status = None
        for attempt in range(self._poll_attempts):
            if attempt:
                self._sleep(self._poll_interval_s)
            result = mcp.call("get_app", {"app_id": app_id})
            if result.ok:
                if app_healthy(result.value):
                    LOGGER.info("app %s is running with its health check passing", app_id)
                    return Recovery(
                        True, "Quoro reports the app running with its health check passing"
                    )
                value = result.value if isinstance(result.value, dict) else {}
                last_status = value.get("status")
                LOGGER.info("app %s status %s; waiting for its health check", app_id, last_status)
                continue
            if result.problem_reason == "grant_revoked":
                return self._after_revocation(wake, mcp, tools)
            LOGGER.warning(
                "get_app refused while waiting (%s); stopping the wait", result.problem_code
            )
            return Recovery(False, f"get_app was refused ({result.problem_code}) while waiting")
        return Recovery(
            False,
            f"Quoro did not report the app healthy within {budget} (last status {last_status})",
        )

    def _after_revocation(self, wake: Wake, mcp: McpSession, tools: set[str]) -> Recovery:
        """The incident grant was revoked while waiting: learn why, if allowed."""
        LOGGER.info("the incident grant was revoked while waiting; reading the incident")
        if "get_incident" in tools:
            incident = mcp.call("get_incident", {"incident_id": wake.incident_id})
            if incident.ok:
                if resolved_by_app_up(incident.value):
                    return Recovery(
                        True,
                        "Quoro resolved the incident when the app came back up (app.up)",
                        incident_closed=True,
                    )
                value = incident.value if isinstance(incident.value, dict) else {}
                if value.get("status") == "resolved":
                    return Recovery(
                        None,
                        f"the incident was resolved by {value.get('resolved_by_type')} "
                        "before the app's health was read",
                        incident_closed=True,
                    )
        return Recovery(
            None,
            "the incident grant was revoked before the app's health could be read; recovery "
            "is not confirmed by Quoro (give the agent a standing app:read and incident:read "
            "grant, or set check_url)",
        )


class LlmPolicy:
    """Let a pluggable backend decide, under the same grant rules."""

    def __init__(self, backend: LlmBackend, *, max_steps: int = DEFAULT_MAX_STEPS) -> None:
        self._backend = backend
        self._max_steps = max(1, max_steps)

    def handle(self, wake: Wake, mcp: McpSession) -> Outcome:
        outcome = Outcome(incident_id=wake.incident_id)
        tools = mcp.list_tools()
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _incident_prompt(wake, tools)},
        ]

        for _ in range(self._max_steps):
            decision = self._backend.next_action(messages, tools)
            if not isinstance(decision, dict):
                outcome.summary = "the backend returned a decision that is not an object"
                LOGGER.warning("%s", outcome.summary)
                return outcome
            if "done" in decision:
                outcome.summary = str(decision.get("done") or "done")
                LOGGER.info("%s", outcome.summary)
                return outcome

            name = decision.get("tool")
            arguments = decision.get("arguments")
            if not isinstance(name, str) or not isinstance(arguments, dict):
                outcome.summary = 'the backend returned neither "tool" nor "done"'
                LOGGER.warning("%s", outcome.summary)
                return outcome

            if name in MUTATING_TOOLS:
                arguments.setdefault("incident_id", wake.incident_id)
                if not str(arguments.get("justification") or "").strip():
                    outcome.summary = (
                        f"{name} was proposed without a justification; refusing to call it"
                    )
                    LOGGER.warning("%s", outcome.summary)
                    return outcome

            result = mcp.call(
                name, arguments, justification=arguments.get("justification")
            )
            outcome.actions.append(Action(name, arguments, result.ok, result.problem))
            if not result.ok:
                outcome.summary = (
                    f"{name} refused ({result.problem_code}"
                    f"{'/' + result.problem_reason if result.problem_reason else ''}); stopping"
                )
                LOGGER.warning("%s", outcome.summary)
                return outcome

            messages.append(
                {
                    "role": "assistant",
                    "content": f"Calling {name}",
                    "tool_call": {"name": name, "arguments": arguments},
                }
            )
            messages.append(
                {
                    "role": "tool",
                    "name": name,
                    "content": f"Result of {name}",
                    "result": result.value,
                }
            )

        outcome.summary = f"stopped after {self._max_steps} steps without an answer"
        LOGGER.warning("%s", outcome.summary)
        return outcome


def _incident_prompt(wake: Wake, tools: list[Mapping[str, Any]]) -> str:
    """Describe the incident and the available tools to a backend."""
    names = sorted(
        tool.get("name") for tool in tools if isinstance(tool.get("name"), str)
    )
    lines = [
        f"Incident {wake.incident_id}{wake.grant_note} opened for agent {wake.agent_id}.",
        f"Event payload: {dict(wake.data)}",
        f"Tools available: {', '.join(names) if names else 'none'}",
        "Decide the next action, or say done with a short summary.",
    ]
    return "\n".join(lines)


def load_backend(spec: str, options: Mapping[str, Any]) -> LlmBackend:
    """Import ``"module:Class"`` and instantiate it with the ``[llm]`` options."""
    module_name, _, class_name = spec.partition(":")
    if not module_name or not class_name:
        raise ValueError(f'llm backend must be "module:Class", got {spec!r}')
    module = importlib.import_module(module_name)
    factory = getattr(module, class_name, None)
    if factory is None:
        raise ValueError(f"{module_name} has no attribute {class_name}")
    kwargs = {key: value for key, value in options.items() if key != "backend"}
    return factory(**kwargs)


def build_policy(config: RunnerConfig) -> Policy:
    """Build the policy named by the config."""
    if config.policy == "llm":
        backend = load_backend(config.llm["backend"], config.llm)
        return LlmPolicy(backend)
    return DeterministicRestartPolicy(
        check_url=config.check_url,
        resolve_incident=config.resolve_incident,
        poll_attempts=config.poll_attempts,
        poll_interval_s=config.poll_interval_s,
        health_timeout_s=config.health_timeout_s,
    )

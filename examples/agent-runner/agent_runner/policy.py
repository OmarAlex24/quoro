# SPDX-License-Identifier: AGPL-3.0-only
"""The decision step: what the woken agent does about an incident.

Two policies ship. :class:`DeterministicRestartPolicy` is the predictable one
used by the tests and the lab demo: read, restart, wait for the app, optionally
check a URL and resolve. :class:`LlmPolicy` hands the decision to a pluggable
backend while keeping the same rules (act inside the grant, justify every
mutation, never work around a refusal).
"""

from __future__ import annotations

import importlib
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

from .config import RunnerConfig
from .quoro import MUTATING_TOOLS, McpSession

LOGGER = logging.getLogger("agent_runner.policy")

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
    """The delivery that woke the agent."""

    incident_id: str
    agent_id: str
    event: Mapping[str, Any] = field(default_factory=dict)

    @property
    def data(self) -> Mapping[str, Any]:
        data = self.event.get("data")
        return data if isinstance(data, dict) else {}


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


def _running(instance: Any) -> bool:
    """Whether one instance reports itself healthy, tolerating absent fields."""
    if not isinstance(instance, dict):
        return False
    status = instance.get("status", instance.get("observed_status"))
    return status == "running"


def app_recovered(detail: Mapping[str, Any] | None) -> bool:
    """Whether every instance of an app reports ``running``."""
    if not detail:
        return False
    instances = detail.get("instances")
    if not isinstance(instances, list) or not instances:
        return False
    return all(_running(instance) for instance in instances)


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
        health_check: Callable[[str], bool] | None = None,
    ) -> None:
        self._check_url = check_url
        self._resolve_incident = resolve_incident
        self._poll_attempts = max(1, poll_attempts)
        self._poll_interval_s = max(0.0, poll_interval_s)
        self._health_timeout_s = health_timeout_s
        self._sleep = sleep
        self._health_check = health_check or self._http_ok

    def _http_ok(self, url: str) -> bool:
        """GET ``url`` with system trust and require status 200."""
        try:
            with urllib.request.urlopen(url, timeout=self._health_timeout_s) as response:
                return response.status == 200
        except (urllib.error.URLError, OSError, ValueError) as error:
            LOGGER.warning("health check of %s failed: %s", url, error)
            return False

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
            f"Incident {wake.incident_id}: app {app_id} reported down ({title}); "
            "restarting its instances as allowed by the incident grant."
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

        outcome.recovered = self._wait_for_recovery(app_id, mcp, outcome)
        if outcome.recovered and self._check_url:
            if not self._health_check(self._check_url):
                outcome.recovered = False

        if outcome.recovered:
            outcome.summary = f"app {app_id} recovered after restart"
        else:
            outcome.summary = f"app {app_id} did not report healthy after restart"

        if outcome.recovered and self._resolve_incident and "resolve_incident" in tools:
            note = (
                f"Incident {wake.incident_id}: app {app_id} recovered after restart "
                "issued by the agent runner."
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

    def _wait_for_recovery(
        self, app_id: str, mcp: McpSession, outcome: Outcome
    ) -> bool:
        """Poll ``get_app`` until every instance runs, or the attempt budget ends."""
        for attempt in range(self._poll_attempts):
            if attempt:
                self._sleep(self._poll_interval_s)
            result = mcp.call("get_app", {"app_id": app_id})
            if not result.ok:
                LOGGER.warning(
                    "get_app refused while polling (%s); stopping the poll",
                    result.problem_code,
                )
                return False
            if app_recovered(result.value):
                LOGGER.info("app %s reports every instance running", app_id)
                return True
        return False


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
        f"Incident {wake.incident_id} opened for agent {wake.agent_id}.",
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

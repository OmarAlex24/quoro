# SPDX-License-Identifier: AGPL-3.0-only
"""A scripted :class:`agent_runner.policy.LlmBackend` for tests.

It plays a fixed list of decisions and records the messages and tool lists it
was shown, so a test can assert both the calls the policy made and what the
policy told the backend.
"""

from __future__ import annotations

from typing import Any, Mapping


class FakeBackend:
    """Return decisions in order; ``done`` once the script runs out."""

    def __init__(self, decisions: list[Mapping[str, Any]] | None = None) -> None:
        self.decisions = list(decisions or [])
        self.seen: list[tuple[list[dict[str, Any]], list[Mapping[str, Any]]]] = []

    def next_action(
        self, messages: list[dict[str, Any]], tools: list[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        self.seen.append((list(messages), list(tools)))
        if self.decisions:
            return self.decisions.pop(0)
        return {"done": "script finished"}

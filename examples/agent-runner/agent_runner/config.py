# SPDX-License-Identifier: AGPL-3.0-only
"""Loading and validation of ``runner.toml``.

The config names where secrets live (files or environment variables) but never
contains them, so a config file can be read without leaking a key. Secret
values are read on demand and never stored in a dataclass, so no ``repr``,
log line or exception can print one.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

DEFAULT_MAX_BODY_BYTES = 1024 * 1024
DEFAULT_POLL_ATTEMPTS = 12
DEFAULT_POLL_INTERVAL_S = 5.0
DEFAULT_HTTP_TIMEOUT_S = 30.0
DEFAULT_HEALTH_TIMEOUT_S = 10.0
DEFAULT_LOG_LEVEL = "INFO"

_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_POLICIES = ("deterministic", "llm")


class ConfigError(Exception):
    """A config file that cannot be used, with a message that names the field."""


@dataclass(frozen=True)
class AgentConfig:
    """An agent the runner may wake, and where its long-lived key lives."""

    agent_id: str
    key_file: Path | None = field(default=None)
    key_env: str | None = field(default=None)

    def read_key(self) -> str:
        """Read the agent key (``akey_...``) from its file or environment variable."""
        if self.key_env:
            value = os.environ.get(self.key_env)
            if not value:
                raise ConfigError(
                    f"agent {self.agent_id}: environment variable {self.key_env} is unset or empty"
                )
        elif self.key_file:
            try:
                value = self.key_file.read_text(encoding="utf-8")
            except OSError as error:
                raise ConfigError(
                    f"agent {self.agent_id}: cannot read key file {self.key_file}: {error.strerror}"
                ) from None
        else:  # pragma: no cover - load_config rejects this
            raise ConfigError(f"agent {self.agent_id}: no key_file or key_env configured")

        key = value.strip()
        if not key.startswith("akey_"):
            raise ConfigError(f"agent {self.agent_id}: the key does not start with akey_")
        return key


@dataclass(frozen=True)
class RunnerConfig:
    """Everything one runner process needs, with secrets still unread."""

    listen_host: str
    listen_port: int
    quoro_url: str
    webhook_secret_file: Path | None = None
    webhook_secret_env: str | None = None
    default_agent_id: str | None = None
    tls_fingerprint: str | None = None
    ca_file: Path | None = None
    policy: str = "deterministic"
    check_url: str | None = None
    resolve_incident: bool = False
    agents: dict[str, AgentConfig] = field(default_factory=dict)
    llm: dict[str, Any] = field(default_factory=dict)
    poll_attempts: int = DEFAULT_POLL_ATTEMPTS
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    http_timeout_s: float = DEFAULT_HTTP_TIMEOUT_S
    health_timeout_s: float = DEFAULT_HEALTH_TIMEOUT_S
    log_level: str = DEFAULT_LOG_LEVEL

    def read_webhook_secret(self) -> bytes:
        """Read the HMAC secret of the webhook from its file or environment variable."""
        if self.webhook_secret_env:
            value = os.environ.get(self.webhook_secret_env)
            if not value:
                raise ConfigError(
                    f"environment variable {self.webhook_secret_env} is unset or empty"
                )
        elif self.webhook_secret_file:
            try:
                value = self.webhook_secret_file.read_text(encoding="utf-8").strip()
            except OSError as error:
                raise ConfigError(
                    f"cannot read webhook secret file {self.webhook_secret_file}: {error.strerror}"
                ) from None
        else:  # pragma: no cover - load_config rejects this
            raise ConfigError("no webhook_secret_file or webhook_secret_env configured")
        if not value:
            raise ConfigError("the webhook secret is empty")
        return value.encode("utf-8")


def _require_table(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key, {})
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a table")
    return value


def _parse_listen(value: Any) -> tuple[str, int]:
    if not isinstance(value, str) or ":" not in value:
        raise ConfigError('listen must be "host:port", for example "127.0.0.1:8787"')
    host, _, port_text = value.rpartition(":")
    host = host.strip("[]")
    if not host:
        raise ConfigError("listen must name a host before the colon")
    try:
        port = int(port_text)
    except ValueError:
        raise ConfigError(f"listen port {port_text!r} is not a number") from None
    if not 1 <= port <= 65535:
        raise ConfigError("listen port must be between 1 and 65535")
    return host, port


def _parse_url(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{field_name} must be a URL")
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError(f'{field_name} must be an http:// or https:// URL, got {value!r}')
    return value


def _parse_fingerprint(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError("tls_fingerprint must be a string")
    normalized = value.strip().lower()
    if not _FINGERPRINT_RE.match(normalized):
        raise ConfigError(
            'tls_fingerprint must be "sha256:" followed by 64 hex characters'
        )
    return normalized


def _parse_agents(raw: dict[str, Any]) -> dict[str, AgentConfig]:
    table = _require_table(raw, "agents")
    agents: dict[str, AgentConfig] = {}
    for agent_id, entry in table.items():
        if not agent_id.startswith("agt_"):
            raise ConfigError(f"[agents.{agent_id}] is not an agt_ id")
        if not isinstance(entry, dict):
            raise ConfigError(f"[agents.{agent_id}] must be a table")
        key_file = entry.get("key_file")
        key_env = entry.get("key_env")
        if bool(key_file) == bool(key_env):
            raise ConfigError(
                f"[agents.{agent_id}] needs exactly one of key_file or key_env"
            )
        path = Path(key_file).expanduser() if key_file else None
        if path is not None and not path.is_file():
            raise ConfigError(f"[agents.{agent_id}].key_file {path} is not a file")
        env = key_env if isinstance(key_env, str) and key_env else None
        if key_env is not None and env is None:
            raise ConfigError(f"[agents.{agent_id}].key_env must be a non-empty string")
        agents[agent_id] = AgentConfig(agent_id=agent_id, key_file=path, key_env=env)
    return agents


def _parse_bool(raw: dict[str, Any], key: str, default: bool) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be true or false")
    return value


def _parse_number(raw: dict[str, Any], key: str, default: float, *, minimum: float) -> float:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{key} must be a number")
    if value < minimum:
        raise ConfigError(f"{key} must be at least {minimum}")
    return float(value)


def _parse_int(raw: dict[str, Any], key: str, default: int, *, minimum: int) -> int:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{key} must be an integer")
    if value < minimum:
        raise ConfigError(f"{key} must be at least {minimum}")
    return int(value)


def load_config(path: str | Path) -> RunnerConfig:
    """Read ``path`` and return a validated config, or raise :class:`ConfigError`."""
    config_path = Path(path).expanduser()
    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError:
        raise ConfigError(f"config file {config_path} does not exist") from None
    except OSError as error:
        raise ConfigError(f"cannot read {config_path}: {error.strerror}") from None
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"{config_path} is not valid TOML: {error}") from None

    host, port = _parse_listen(raw.get("listen"))
    quoro_url = _parse_url(raw.get("quoro_url"), "quoro_url")
    fingerprint = _parse_fingerprint(raw.get("tls_fingerprint"))
    ca_file = raw.get("ca_file")
    if ca_file is not None:
        ca_file = Path(ca_file).expanduser()
        if not ca_file.is_file():
            raise ConfigError(f"ca_file {ca_file} is not a file")
    if fingerprint and ca_file:
        raise ConfigError("set either tls_fingerprint or ca_file, not both")

    secret_file = raw.get("webhook_secret_file")
    secret_env = raw.get("webhook_secret_env")
    if bool(secret_file) == bool(secret_env):
        raise ConfigError(
            "set exactly one of webhook_secret_file or webhook_secret_env"
        )
    if secret_file is not None:
        secret_file = Path(secret_file).expanduser()
        if not secret_file.is_file():
            raise ConfigError(f"webhook_secret_file {secret_file} is not a file")
    if secret_env is not None and not (
        isinstance(secret_env, str) and secret_env.strip()
    ):
        raise ConfigError("webhook_secret_env must be a non-empty string")

    policy = raw.get("policy", "deterministic")
    if policy not in _POLICIES:
        raise ConfigError(f"policy must be one of {', '.join(_POLICIES)}, got {policy!r}")

    llm = _require_table(raw, "llm")
    if policy == "llm":
        backend = llm.get("backend")
        if not isinstance(backend, str) or ":" not in backend:
            raise ConfigError(
                '[llm] must set backend = "module:Class" when policy = "llm"'
            )

    default_agent_id = raw.get("default_agent_id")
    if default_agent_id is not None and not isinstance(default_agent_id, str):
        raise ConfigError("default_agent_id must be an agt_ id")

    agents = _parse_agents(raw)
    if default_agent_id is not None and default_agent_id not in agents:
        raise ConfigError(
            f"default_agent_id {default_agent_id} has no [agents] entry, so its key is unknown"
        )

    check_url = raw.get("check_url")
    if check_url is not None:
        check_url = _parse_url(check_url, "check_url")

    return RunnerConfig(
        listen_host=host,
        listen_port=port,
        quoro_url=quoro_url,
        webhook_secret_file=secret_file,
        webhook_secret_env=secret_env,
        default_agent_id=default_agent_id,
        tls_fingerprint=fingerprint,
        ca_file=ca_file,
        policy=policy,
        check_url=check_url,
        resolve_incident=_parse_bool(raw, "resolve_incident", False),
        agents=agents,
        llm=dict(llm),
        poll_attempts=_parse_int(raw, "poll_attempts", DEFAULT_POLL_ATTEMPTS, minimum=1),
        poll_interval_s=_parse_number(
            raw, "poll_interval_s", DEFAULT_POLL_INTERVAL_S, minimum=0.0
        ),
        max_body_bytes=_parse_int(
            raw, "max_body_bytes", DEFAULT_MAX_BODY_BYTES, minimum=1
        ),
        http_timeout_s=_parse_number(
            raw, "http_timeout_s", DEFAULT_HTTP_TIMEOUT_S, minimum=1.0
        ),
        health_timeout_s=_parse_number(
            raw, "health_timeout_s", DEFAULT_HEALTH_TIMEOUT_S, minimum=1.0
        ),
        log_level=str(raw.get("log_level", DEFAULT_LOG_LEVEL)),
    )

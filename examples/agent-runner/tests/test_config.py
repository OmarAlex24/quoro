# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for config loading: validation messages and defaults."""

from __future__ import annotations

import tempfile
import tomllib
import unittest
from pathlib import Path

from agent_runner.config import ConfigError, load_config

EXAMPLES = Path(__file__).resolve().parent.parent

BASE = """
listen = "127.0.0.1:8787"
quoro_url = "https://primary.example:8443"
webhook_secret_env = "QUORO_TEST_WEBHOOK_SECRET"
"""

AGENTS = """
[agents."agt_01JTESTAGENT00000000000000"]
key_env = "QUORO_TEST_AGENT_KEY"
"""


def config_text(extra: str = "", agents: str = AGENTS) -> str:
    """A minimal valid config with ``extra`` inserted before the agents table."""
    return BASE + extra + agents


class ConfigTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # Honours TMPDIR, so a host can keep test files off a small /tmp.
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.directory = Path(self._temp.name)

    def write(self, text: str, name: str = "runner.toml") -> Path:
        path = self.directory / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_loads_a_minimal_config(self) -> None:
        config = load_config(self.write(config_text()))

        self.assertEqual(config.listen_host, "127.0.0.1")
        self.assertEqual(config.listen_port, 8787)
        self.assertEqual(config.policy, "deterministic")
        self.assertEqual(config.poll_attempts, 12)
        self.assertEqual(config.poll_interval_s, 5.0)
        self.assertIsNone(config.tls_fingerprint)
        self.assertIn("agt_01JTESTAGENT00000000000000", config.agents)

    def test_a_missing_file_is_reported(self) -> None:
        with self.assertRaisesRegex(ConfigError, "does not exist"):
            load_config(self.directory / "absent.toml")

    def test_rejects_a_bad_listen_value(self) -> None:
        text = config_text().replace('"127.0.0.1:8787"', '"127.0.0.1"')
        with self.assertRaisesRegex(ConfigError, "listen"):
            load_config(self.write(text))

    def test_requires_exactly_one_secret_source(self) -> None:
        text = config_text('\nwebhook_secret_file = "secret"\n')
        with self.assertRaisesRegex(ConfigError, "exactly one"):
            load_config(self.write(text))

    def test_rejects_an_unknown_policy(self) -> None:
        with self.assertRaisesRegex(ConfigError, "policy must be"):
            load_config(self.write(config_text('\npolicy = "vibes"\n')))

    def test_rejects_a_malformed_fingerprint(self) -> None:
        with self.assertRaisesRegex(ConfigError, "tls_fingerprint"):
            load_config(self.write(config_text('\ntls_fingerprint = "sha256:abc"\n')))

    def test_rejects_a_default_agent_without_a_key(self) -> None:
        with self.assertRaisesRegex(ConfigError, "default_agent_id"):
            load_config(
                self.write(config_text('\ndefault_agent_id = "agt_01JOTHER000000000000000"\n'))
            )

    def test_rejects_a_missing_key_file(self) -> None:
        agents = (
            f'\n[agents."agt_01JTESTAGENT00000000000000"]\n'
            f'key_file = "{self.directory}/absent.key"\n'
        )
        with self.assertRaisesRegex(ConfigError, "key_file"):
            load_config(self.write(config_text(agents=agents)))

    def test_llm_policy_requires_a_backend(self) -> None:
        with self.assertRaisesRegex(ConfigError, "backend"):
            load_config(self.write(config_text('\npolicy = "llm"\n')))

    def test_rejects_polling_with_zero_attempts(self) -> None:
        with self.assertRaisesRegex(ConfigError, "poll_attempts"):
            load_config(self.write(config_text("\npoll_attempts = 0\n")))


class ExampleConfigTests(unittest.TestCase):
    def test_the_example_is_valid_toml_and_documents_the_keys(self) -> None:
        with (EXAMPLES / "runner.example.toml").open("rb") as handle:
            raw = tomllib.load(handle)

        self.assertIn("listen", raw)
        self.assertIn("quoro_url", raw)
        self.assertIn("webhook_secret_file", raw)
        self.assertIn("agents", raw)
        self.assertEqual(raw["policy"], "deterministic")


if __name__ == "__main__":
    unittest.main()

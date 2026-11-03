# SPDX-License-Identifier: AGPL-3.0-only
"""Command-line entry point: ``python3 -m agent_runner --config runner.toml``."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import ConfigError, RunnerConfig, load_config
from .policy import build_policy
from .server import ADDRESS_IN_USE_HINT, create_server

LOGGER = logging.getLogger("agent_runner")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="agent_runner",
        description="Receive Quoro incident webhooks and wake an agent over MCP.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("runner.toml"),
        help="path to runner.toml (default: ./runner.toml)",
    )
    return parser.parse_args(argv)


def _validate_secrets(config: RunnerConfig) -> None:
    """Read every secret once at startup so a bad path fails before any delivery."""
    config.read_webhook_secret()
    for agent in config.agents.values():
        agent.read_key()


def main(argv: list[str] | None = None) -> int:
    """Load the config, start the receiver and serve until interrupted."""
    args = _parse_args(argv)
    try:
        config = load_config(args.config)
    except ConfigError as error:
        print(f"config error: {error}", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        _validate_secrets(config)
        policy = build_policy(config)
        server = create_server(config, policy=policy)
    except (ConfigError, ValueError, ImportError) as error:
        LOGGER.error("cannot start: %s", error)
        return 2
    except OSError as error:
        LOGGER.error("cannot listen on %s:%s: %s (%s)", config.listen_host, config.listen_port, error, ADDRESS_IN_USE_HINT)
        return 2

    LOGGER.info(
        "listening on http://%s:%s%s, Quoro at %s",
        config.listen_host,
        config.listen_port,
        "/webhook",
        config.quoro_url,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("interrupted; shutting down")
    finally:
        server.server_close()
        server.app.wait_idle(timeout=5.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

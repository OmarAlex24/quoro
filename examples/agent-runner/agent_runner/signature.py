# SPDX-License-Identifier: AGPL-3.0-only
"""Verification of Quoro webhook signatures.

A delivery carries ``Quoro-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256>``,
where the HMAC is computed over ``"<t>.<raw body>"`` with the webhook secret.
Receivers reject a signature outside a five-minute window (contracts/README.md,
"Request headers").

The header may carry several ``v1=`` values; a delivery passes when any of them
matches. This keeps verification working while a secret is being rotated.
"""

from __future__ import annotations

import hashlib
import hmac
import time

HEADER_NAME = "Quoro-Signature"
MAX_SKEW_S = 300


def _as_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else value


def _digest(timestamp: int, body: bytes, secret: bytes) -> str:
    """Return the hex HMAC-SHA256 of ``"<timestamp>.<body>"``."""
    message = str(timestamp).encode("ascii") + b"." + body
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def parse_header(header: str | None) -> tuple[int | None, list[str]]:
    """Split a signature header into its timestamp and every ``v1`` value.

    Malformed parts are ignored. A header without a usable ``t`` or without any
    ``v1`` returns ``(None, [])`` so callers can reject it as malformed.
    """
    if not header:
        return None, []

    timestamp: int | None = None
    signatures: list[str] = []
    for part in header.split(","):
        key, separator, value = part.strip().partition("=")
        if not separator:
            continue
        key = key.strip().lower()
        value = value.strip()
        if key == "t" and timestamp is None:
            try:
                timestamp = int(value)
            except ValueError:
                return None, []
        elif key == "v1" and value:
            signatures.append(value)
    return timestamp, signatures


def verify(
    header: str | None,
    body: bytes,
    secret: bytes | str,
    now: float | None = None,
    max_skew_s: int = MAX_SKEW_S,
) -> bool:
    """Return whether ``header`` signs ``body`` with ``secret`` and is fresh.

    ``now`` defaults to the current clock; tests pass it explicitly. A header
    that cannot be parsed, is too old or too far in the future, or does not
    match any ``v1`` value is rejected.
    """
    timestamp, signatures = parse_header(header)
    if timestamp is None or not signatures:
        return False

    current = time.time() if now is None else now
    if abs(current - timestamp) > max_skew_s:
        return False

    expected = _digest(timestamp, body, _as_bytes(secret))
    # Compare bytes: `compare_digest` raises on non-ASCII `str`, and a header
    # is attacker-controlled.
    return any(
        hmac.compare_digest(expected.encode("ascii"), candidate.lower().encode("utf-8"))
        for candidate in signatures
    )


def sign(body: bytes, secret: bytes | str, t: int | None = None) -> str:
    """Produce a signature header for ``body``; used by tests and local tooling."""
    timestamp = int(time.time()) if t is None else int(t)
    return f"t={timestamp},v1={_digest(timestamp, body, _as_bytes(secret))}"

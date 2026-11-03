# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for webhook signature parsing and verification."""

from __future__ import annotations

import time
import unittest

from agent_runner.signature import MAX_SKEW_S, parse_header, sign, verify

SECRET = b"a-webhook-secret"
BODY = b'{"type":"incident.opened"}'


class VerifyTests(unittest.TestCase):
    def test_accepts_a_valid_signature(self) -> None:
        now = 1_700_000_000
        header = sign(BODY, SECRET, t=now)
        self.assertTrue(verify(header, BODY, SECRET, now=now))

    def test_accepts_a_secret_given_as_text(self) -> None:
        now = 1_700_000_000
        self.assertTrue(verify(sign(BODY, SECRET, t=now), BODY, SECRET.decode(), now=now))

    def test_rejects_a_tampered_body(self) -> None:
        now = 1_700_000_000
        header = sign(BODY, SECRET, t=now)
        self.assertFalse(verify(header, BODY + b" ", SECRET, now=now))

    def test_rejects_a_wrong_secret(self) -> None:
        now = 1_700_000_000
        header = sign(BODY, SECRET, t=now)
        self.assertFalse(verify(header, BODY, b"another-secret", now=now))

    def test_rejects_a_stale_signature(self) -> None:
        now = 1_700_000_000
        header = sign(BODY, SECRET, t=now - MAX_SKEW_S - 1)
        self.assertFalse(verify(header, BODY, SECRET, now=now))

    def test_accepts_a_signature_at_the_edge_of_the_window(self) -> None:
        now = 1_700_000_000
        header = sign(BODY, SECRET, t=now - MAX_SKEW_S)
        self.assertTrue(verify(header, BODY, SECRET, now=now))

    def test_rejects_a_signature_too_far_in_the_future(self) -> None:
        now = 1_700_000_000
        header = sign(BODY, SECRET, t=now + MAX_SKEW_S + 1)
        self.assertFalse(verify(header, BODY, SECRET, now=now))

    def test_rejects_a_missing_header(self) -> None:
        self.assertFalse(verify(None, BODY, SECRET))

    def test_rejects_a_malformed_header(self) -> None:
        for header in ("", "garbage", "t=abc,v1=deadbeef", "t=1700000000", "v1=deadbeef"):
            with self.subTest(header=header):
                self.assertFalse(verify(header, BODY, SECRET, now=1_700_000_000))

    def test_accepts_when_any_v1_matches(self) -> None:
        now = 1_700_000_000
        valid = sign(BODY, SECRET, t=now).split("v1=", 1)[1]
        header = f"t={now},v1={'0' * 64},v1={valid.upper()}"
        self.assertTrue(verify(header, BODY, SECRET, now=now))

    def test_rejects_when_no_v1_matches(self) -> None:
        now = 1_700_000_000
        header = f"t={now},v1={'0' * 64},v1={'1' * 64}"
        self.assertFalse(verify(header, BODY, SECRET, now=now))


class ParseHeaderTests(unittest.TestCase):
    def test_parses_timestamp_and_every_signature(self) -> None:
        timestamp, signatures = parse_header("t=1700000000, v1=aa ,v1=bb")
        self.assertEqual(timestamp, 1_700_000_000)
        self.assertEqual(signatures, ["aa", "bb"])

    def test_ignores_unknown_keys_and_empty_parts(self) -> None:
        timestamp, signatures = parse_header("v0=old,t=1700000000,,v1=aa")
        self.assertEqual(timestamp, 1_700_000_000)
        self.assertEqual(signatures, ["aa"])

    def test_keeps_the_first_timestamp(self) -> None:
        timestamp, _ = parse_header("t=1700000000,t=1800000000,v1=aa")
        self.assertEqual(timestamp, 1_700_000_000)


class NonAsciiTests(unittest.TestCase):
    def test_a_non_ascii_signature_is_rejected_without_raising(self) -> None:
        self.assertFalse(verify("t=1700000000,v1=\u00e9\u00e9", BODY, SECRET, now=1_700_000_000))


class SignTests(unittest.TestCase):
    def test_sign_defaults_to_the_current_time(self) -> None:
        header = sign(BODY, SECRET)
        self.assertTrue(verify(header, BODY, SECRET, now=time.time()))


if __name__ == "__main__":
    unittest.main()

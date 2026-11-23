"""Canonical HMAC V2 consumer cursor tests."""

from __future__ import annotations

import base64
import hmac
import struct
import unittest

from xuanling_tradecore.consumer import CursorKeyRingV2, CursorSigningKeyV2, CursorTokenError
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1


class ConsumerCursorV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.ring = CursorKeyRingV2(
            current=CursorSigningKeyV2("current", b"c" * 32),
            retired=(CursorSigningKeyV2("retired", b"r" * 32, verify_until_wall_ns=200),),
        )

    def test_issues_and_verifies_canonical_binary_position(self) -> None:
        after_position = ConsumerEventPositionV2(RedisStreamIdV1(10, 3), 1)
        high_watermark_position = ConsumerEventPositionV2(RedisStreamIdV1(11, 0), 2)
        token = self.ring.issue(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            journal_id="journal-1",
            stream_generation=2,
            after_position=after_position,
            authorization_epoch=4,
            issued_at_wall_ns=100,
            expires_at_wall_ns=150,
            high_watermark_position=high_watermark_position,
        )
        claims = self.ring.verify(token, evaluated_wall_ns=120)
        self.assertEqual(2, claims.stream_generation)
        self.assertEqual(after_position, claims.after_position)
        self.assertEqual(high_watermark_position, claims.high_watermark_position)

    def test_rejects_tamper_expiry_and_retired_key_after_deadline(self) -> None:
        token = self.ring.issue(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            journal_id="journal-1",
            stream_generation=1,
            after_position=None,
            authorization_epoch=1,
            issued_at_wall_ns=10,
            expires_at_wall_ns=20,
        )
        with self.assertRaisesRegex(CursorTokenError, "expired"):
            self.ring.verify(token, evaluated_wall_ns=20)
        with self.assertRaisesRegex(CursorTokenError, "signature"):
            self.ring.verify(token[:-1] + ("A" if token[-1] != "A" else "B"), evaluated_wall_ns=11)
        long_lived = self.ring.issue(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            journal_id="journal-1",
            stream_generation=1,
            after_position=None,
            authorization_epoch=1,
            issued_at_wall_ns=10,
            expires_at_wall_ns=100,
        )
        retired_ring = CursorKeyRingV2(
            current=CursorSigningKeyV2("next", b"n" * 32),
            retired=(CursorSigningKeyV2("current", b"c" * 32, verify_until_wall_ns=50),),
        )
        with self.assertRaisesRegex(CursorTokenError, "signing key has expired"):
            retired_ring.verify(long_lived, evaluated_wall_ns=60)

    def test_verifies_legacy_token_without_fixed_watermark_claim(self) -> None:
        values = ("consumer-1", "consumer-account-1", "journal-1", "current")
        payload = bytearray(b"\x01")
        for value in values:
            encoded = value.encode("utf-8")
            payload.extend((len(encoded),))
            payload.extend(encoded)
        position = ConsumerEventPositionV2(RedisStreamIdV1(10, 3), 1)
        payload.extend(struct.pack(">Q", 2))
        payload.extend(b"\x01")
        payload.extend(position.to_bytes())
        payload.extend(struct.pack(">QQQ", 4, 100, 150))
        raw = bytes(payload)
        token = "ct2_" + base64.urlsafe_b64encode(
            raw + hmac.digest(b"c" * 32, raw, "sha256")
        ).rstrip(b"=").decode("ascii")
        claims = self.ring.verify(token, evaluated_wall_ns=120)
        self.assertEqual(position, claims.after_position)
        self.assertIsNone(claims.high_watermark_position)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import hmac
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.snapshot_cursor_v2 import (
    SnapshotCursorClaimsV2,
    SnapshotCursorCodecV2,
    SnapshotCursorErrorV2,
    SnapshotCursorSigningKeyV2,
)


def _claims() -> SnapshotCursorClaimsV2:
    return SnapshotCursorClaimsV2(
        "consumer-1",
        "account-1",
        1,
        "snapshot-1",
        "journal-1",
        1,
        "position-1",
        0,
        100,
        1_000,
        2_000,
        "key-1",
    )


def test_verified_cursor_cache_rechecks_expiry_without_repeating_hmac() -> None:
    key = SnapshotCursorSigningKeyV2("key-1", b"k" * 32)
    token = SnapshotCursorCodecV2(key).issue(_claims())
    verifier = SnapshotCursorCodecV2(key)
    original_digest = hmac.digest

    with patch(
        "xuanling_tradecore.consumer.snapshot_cursor_v2.hmac.digest",
        wraps=original_digest,
    ) as digest:
        assert verifier.verify(token, evaluated_wall_ns=1_100) == _claims()
        assert verifier.verify(token, evaluated_wall_ns=1_200) == _claims()
    assert digest.call_count == 1

    with pytest.raises(SnapshotCursorErrorV2) as expired:
        verifier.verify(token, evaluated_wall_ns=2_000)
    assert expired.value.code == "SNAPSHOT_EXPIRED"


def test_verified_cursor_cache_is_bounded() -> None:
    key = SnapshotCursorSigningKeyV2("key-1", b"k" * 32)
    codec = SnapshotCursorCodecV2(key, maximum_cached_tokens=1)
    first = codec.issue(_claims())
    second_claims = SnapshotCursorClaimsV2(
        **{**_claims().__dict__, "snapshot_id": "snapshot-2"}
    )
    second = codec.issue(second_claims)

    assert codec.verify(second, evaluated_wall_ns=1_100) == second_claims
    assert codec.verify(first, evaluated_wall_ns=1_100) == _claims()


def test_cached_token_does_not_outlive_its_retired_signing_key() -> None:
    old = SnapshotCursorSigningKeyV2("key-1", b"k" * 32)
    token = SnapshotCursorCodecV2(old).issue(_claims())
    verifier = SnapshotCursorCodecV2(
        SnapshotCursorSigningKeyV2("key-2", b"n" * 32),
        retired_keys=(SnapshotCursorSigningKeyV2("key-1", b"k" * 32, 1500),),
    )
    assert verifier.verify(token, evaluated_wall_ns=1200) == _claims()
    with pytest.raises(SnapshotCursorErrorV2) as error:
        verifier.verify(token, evaluated_wall_ns=1600)
    assert error.value.code == "CURSOR_INVALID"

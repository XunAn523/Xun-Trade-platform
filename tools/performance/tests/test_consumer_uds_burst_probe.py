from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from consumer_uds_burst_probe import _exchange
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame


@pytest.mark.parametrize(
    ("payload", "expected_count", "expected_status"),
    [
        ({}, None, "COMPLETED"),
        ({"count": 100}, 100, "COMPLETED"),
        ({"count": 99}, 100, "INVALID_SNAPSHOT_PAGE"),
    ],
)
def test_page_validation_does_not_change_other_operations(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, int],
    expected_count: int | None,
    expected_status: str,
) -> None:
    class Writer:
        def write(self, data: bytes) -> None:
            pass

        async def drain(self) -> None:
            pass

        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    async def run() -> dict[str, object]:
        reader = asyncio.StreamReader()
        reader.feed_data(
            encode_frame(MessageKind.RESPONSE, 1, {"status": "COMPLETED", "payload": payload})
        )

        async def connect(path: str) -> tuple[asyncio.StreamReader, Writer]:
            return reader, Writer()

        monkeypatch.setattr(asyncio, "open_unix_connection", connect, raising=False)
        return await _exchange(
            path=Path("unused.sock"),
            frame=b"request",
            sample={},
            expected_page_count=expected_count,
        )

    assert asyncio.run(run())["status"] == expected_status

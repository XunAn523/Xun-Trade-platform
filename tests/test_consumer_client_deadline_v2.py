from pathlib import Path
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientErrorV2,
    ConsumerUnixClientV2,
    MessageKind,
    encode_frame,
)


class Peer:
    def __init__(self, *, quantum, chunk=1):
        self.now, self.quantum, self.chunk = 0.0, quantum, chunk
        self.timeouts, self.sends = [], []
        self.closed = False
        self.response = encode_frame(MessageKind.RESPONSE, 1, {
            "message_type": "response", "schema_version": 2, "request_id": "test",
            "status": "COMPLETED", "payload": {},
        })

    def settimeout(self, value):
        self.timeouts.append(value)

    def connect(self, _):
        self.now += self.quantum

    def sendall(self, frame):
        self.now += self.quantum
        self.sends.append(frame)

    def recv(self, _):
        self.now += self.quantum
        result, self.response = self.response[:self.chunk], self.response[self.chunk:]
        return result

    def close(self):
        self.closed = True


def invoke(peer, budget):
    with (
        patch("xuanling_tradecore.consumer.client_v2.socket.AF_UNIX", 1, create=True),
        patch("xuanling_tradecore.consumer.client_v2.socket.socket", return_value=peer),
        patch("xuanling_tradecore.consumer.client_v2.monotonic", side_effect=lambda: peer.now),
    ):
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(Path.cwd() / "unused", budget))
        return client.call({"request_id": "test"})


def test_trickled_reply_does_not_renew_the_budget_or_retry_submit():
    peer = Peer(quantum=0.02)
    with pytest.raises(ConsumerUnixClientErrorV2) as error:
        invoke(peer, 0.05)
    assert isinstance(error.value.__cause__, TimeoutError)
    assert peer.now == pytest.approx(0.06)
    assert peer.timeouts == pytest.approx([0.05, 0.03, 0.01])
    assert len(peer.sends) == 1 and peer.closed


def test_complete_but_late_decoded_reply_is_not_reported_within_deadline():
    peer = Peer(quantum=0.02, chunk=65_536)
    with pytest.raises(ConsumerUnixClientErrorV2):
        invoke(peer, 0.05)
    assert len(peer.sends) == 1 and peer.closed


def test_fragmentation_within_one_budget_remains_supported():
    peer = Peer(quantum=0.001, chunk=3)
    assert invoke(peer, 1)["status"] == "COMPLETED"
    assert all(a >= b > 0 for a, b in zip(peer.timeouts, peer.timeouts[1:]))
    assert peer.closed


@pytest.mark.parametrize("value", (True, 0, -1, float("inf"), float("nan"), "1"))
def test_invalid_or_unbounded_client_deadline_is_rejected(value):
    with pytest.raises(ConsumerUnixClientErrorV2):
        ConsumerUnixClientConfigV2(Path.cwd() / "unused", value)

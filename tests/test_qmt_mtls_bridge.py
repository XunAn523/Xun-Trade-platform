"""Linux command-client failure boundaries without sockets or QMT side effects."""

from pathlib import Path

import pytest
from xuanling_tradecore.adapters.base import BrokerQuery
from xuanling_tradecore.adapters.qmt.bridge_wire import (
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
)
from xuanling_tradecore.adapters.qmt.mtls_bridge import (
    QmtMtlsBridgeConfig,
    QmtMtlsBridgeError,
    QmtMtlsBridgePort,
)
from xuanling_tradecore.adapters.qmt.mtls_channel import QmtMtlsClientConfig


def health_frame(ready=True, detail_code="READY"):
    return QmtBridgeWireFrame(
        QmtBridgeWireKind.HEALTH,
        canonical_wire_payload(
            {"schema": "QmtEdgeHealthV1", "ready": ready, "detail_code": detail_code}
        ),
    )


def acceptance_frame(**changes):
    payload = {
        "schema": "QmtEdgeAcceptanceV1",
        "request_id": "query-1",
        "command_sequence": 1,
        "accepted_monotonic_ns": 10,
        "state": "PUBLISH_ATTEMPTED",
    }
    payload.update(changes)
    return QmtBridgeWireFrame(QmtBridgeWireKind.ACCEPTED, canonical_wire_payload(payload))


class Connection:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.sent = []
        self.closed = False

    def send(self, frame):
        assert not self.closed
        self.sent.append(frame)

    def receive(self):
        response = next(self.responses)
        if isinstance(response, BaseException):
            raise response
        return response

    def close(self):
        self.closed = True


def bridge_for(connection):
    config = QmtMtlsBridgeConfig(
        tls=QmtMtlsClientConfig(
            host="127.0.0.1",
            port=12345,
            ca_certificate_path=Path("unused-ca.pem"),
            certificate_path=Path("unused-client.pem"),
            private_key_path=Path("unused-key.pem"),
            expected_server_certificate_sha256="a" * 64,
            server_hostname="unused-edge",
        ),
        scope_digest="b" * 64,
        adapter_id="test-adapter",
        adapter_instance_id="test-instance",
        adapter_client_scope="test-scope",
    )
    return QmtMtlsBridgePort(config, connection_factory=lambda _: connection)


def query(bridge):
    return bridge.query(BrokerQuery("query-1", "test-account", "QMT", "test-instance", "orders", 1))


@pytest.mark.parametrize(
    "response",
    [
        OSError("connection lost"),
        acceptance_frame(),
        health_frame(False, "DISARMED"),
        health_frame(detail_code=""),
        health_frame(detail_code="BAD\nHEALTH"),
        health_frame(detail_code="x" * 129),
        QmtBridgeWireFrame(QmtBridgeWireKind.HEALTH, b"{}"),
    ],
)
def test_failed_hello_always_closes_connection(response):
    connection = Connection([response])
    bridge = bridge_for(connection)
    with pytest.raises(QmtMtlsBridgeError):
        bridge.open(lambda _: None)
    assert connection.closed
    with pytest.raises(QmtMtlsBridgeError, match="not open"):
        query(bridge)
    assert len(connection.sent) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": "another-query"},
        {"command_sequence": True},
        {"command_sequence": 0},
        {"command_sequence": -1},
        {"accepted_monotonic_ns": True},
        {"accepted_monotonic_ns": -1},
        {"state": "UNKNOWN_EFFECT"},
        {"state": []},
        {"schema": "unsupported"},
        {"unexpected": "field"},
    ],
)
def test_invalid_acceptance_closes_stream_and_never_retries_command(changes):
    connection = Connection([health_frame(), acceptance_frame(**changes)])
    bridge = bridge_for(connection)
    bridge.open(lambda _: None)
    with pytest.raises(QmtMtlsBridgeError):
        query(bridge)
    assert connection.closed
    with pytest.raises(QmtMtlsBridgeError, match="not open"):
        query(bridge)
    assert len(connection.sent) == 2


def test_valid_query_acceptance_returns_only_a_correlated_handle():
    connection = Connection([health_frame(), acceptance_frame()])
    bridge = bridge_for(connection)
    bridge.open(lambda _: None)
    handle = query(bridge)
    assert handle.request_id == "query-1"
    assert handle.adapter_instance_id == "test-instance"
    assert not connection.closed
    bridge.close()


@pytest.mark.parametrize(
    "response",
    [
        health_frame(False, "FAULT"),
        health_frame(detail_code=""),
        health_frame(detail_code="BAD\nHEALTH"),
    ],
)
def test_bad_health_invalidates_existing_stream(response):
    connection = Connection([health_frame(), response])
    bridge = bridge_for(connection)
    bridge.open(lambda _: None)
    if response == health_frame(False, "FAULT"):
        assert not bridge.health().ready
    else:
        with pytest.raises(QmtMtlsBridgeError):
            bridge.health()
    assert connection.closed
    with pytest.raises(QmtMtlsBridgeError, match="not open"):
        query(bridge)


def test_peer_rejection_payload_is_not_copied_to_exception():
    response = QmtBridgeWireFrame(
        QmtBridgeWireKind.REJECTED,
        canonical_wire_payload({"schema": "QmtEdgeRejectV1", "code": "SENSITIVE_PEER_PAYLOAD"}),
    )
    connection = Connection([health_frame(), response])
    bridge = bridge_for(connection)
    bridge.open(lambda _: None)
    with pytest.raises(QmtMtlsBridgeError) as error:
        query(bridge)
    assert "SENSITIVE_PEER_PAYLOAD" not in str(error.value)
    assert connection.closed

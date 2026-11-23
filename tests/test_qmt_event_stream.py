"""QMT connection integration through real framing, SQLite and BrokerAdapter."""

from dataclasses import replace
from pathlib import Path

import pytest
from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerAdapter,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.qmt import (
    QmtAdapterProfile,
    load_builtin_qmt_fake_manifest,
)
from xuanling_tradecore.adapters.qmt.bridge_protocol import (
    QmtBridgeEventDelivery,
    QmtBridgeGapDelivery,
    QmtBridgeNativeEvent,
)
from xuanling_tradecore.adapters.qmt.bridge_wire import (
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)
from xuanling_tradecore.adapters.qmt.edge_event_endpoint import QmtEdgeEventEndpoint
from xuanling_tradecore.adapters.qmt.edge_store import QmtEdgeStore, QmtEdgeStoreError
from xuanling_tradecore.adapters.qmt.event_wire import (
    QmtEventStreamConfig,
    QmtEventWireCodec,
    QmtEventWireError,
)
from xuanling_tradecore.adapters.qmt.mtls_bridge import (
    QmtMtlsBridgeConfig,
    QmtMtlsBridgeError,
    QmtMtlsBridgePort,
)
from xuanling_tradecore.adapters.qmt.mtls_channel import QmtMtlsClientConfig
from xuanling_tradecore.domain import (
    BrokerEventKind,
    BrokerSourceKind,
    CallbackGapEvidence,
    TradingDayEpoch,
)

ACCOUNT = "qmt-fake-account-1"
SCOPE = "a" * 64
EPOCH = "b" * 64


def event(event_id="event-1"):
    return QmtBridgeNativeEvent(
        external_event_id=event_id,
        event_kind=BrokerEventKind.ORDER,
        source_kind=BrokerSourceKind.CALLBACK,
        account_id=ACCOUNT,
        trading_day=TradingDayEpoch("2026-09-30"),
        received_monotonic_ns=10,
        execution_id="execution-1",
    )


class Observer:
    def __init__(self):
        self.events = []
        self.gaps = []
        self.fail = False

    def on_broker_event(self, fact):
        if self.fail:
            raise OSError("journal unavailable")
        self.events.append(fact)

    def on_callback_gap(self, fact):
        self.gaps.append(fact)


class Connection:
    def __init__(self, endpoint=None):
        self.endpoint = endpoint
        self.response = None
        self.closed = False
        self.sent = []
        self.drop_ack = False
        self.corrupt_event = None

    def send(self, frame):
        assert not self.closed
        frame = QmtBridgeWireFrame.decode(frame.encode())
        self.sent.append(frame)
        if self.endpoint is None:
            self.response = QmtBridgeWireFrame(
                QmtBridgeWireKind.HEALTH,
                canonical_wire_payload(
                    {"schema": "QmtEdgeHealthV1", "ready": True, "detail_code": "READY"}
                ),
            )
        elif frame.kind is QmtBridgeWireKind.HELLO:
            self.response = self.endpoint.open(frame)
        else:
            self.response = self.endpoint.acknowledge(frame)

    def receive(self):
        assert not self.closed
        if self.response is not None:
            frame, self.response = self.response, None
            if frame.kind is QmtBridgeWireKind.ACK and self.drop_ack:
                raise OSError("ACK response lost after Edge commit")
        else:
            frame = self.endpoint.next_frame()
            if self.corrupt_event is not None and frame.kind is QmtBridgeWireKind.EVENT:
                fields = dict(decode_canonical_wire_payload(frame.payload))
                self.corrupt_event(fields)
                frame = QmtBridgeWireFrame(frame.kind, canonical_wire_payload(fields))
        return QmtBridgeWireFrame.decode(frame.encode())

    def close(self):
        self.closed = True


class Harness:
    def __init__(self, path, *, resume=0, epoch=EPOCH):
        self.store = QmtEdgeStore(path, synchronous="FULL")
        self.codec = QmtEventWireCodec(
            scope_digest=SCOPE, config=QmtEventStreamConfig(ACCOUNT, EPOCH)
        )
        self.connections = []
        self.healthy = True
        manifest = load_builtin_qmt_fake_manifest()
        profile = QmtAdapterProfile(manifest)
        mapping = profile.activate(
            runtime=RuntimeVenueProfile(
                fingerprint=manifest.fingerprint,
                capabilities=manifest.capabilities,
                account_capabilities=AccountTradingCapabilities(ACCOUNT, True, False, False),
                scope_ownership=manifest.scope_ownership,
                route_token_binding=manifest.route_token_binding,
                trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
                route_token_probe_succeeded=True,
            ),
            activated_at_unix_s=0,
        )
        self.config = QmtMtlsBridgeConfig(
            tls=QmtMtlsClientConfig(
                "unused", 12345, Path("ca"), Path("cert"), Path("key"), "c" * 64, "unused"
            ),
            scope_digest=SCOPE,
            adapter_id=manifest.fingerprint.adapter_id,
            adapter_instance_id=manifest.scope_ownership.adapter_instance_id,
            adapter_client_scope=manifest.scope_ownership.adapter_client_scope,
            events=QmtEventStreamConfig(ACCOUNT, epoch, resume),
        )
        self.bridge = QmtMtlsBridgePort(self.config, connection_factory=self.connect)
        self.adapter = profile.create_bridge_adapter(
            mapping, bridge=self.bridge, client_order_ref_factory=lambda _: "unused"
        )
        self.observer = Observer()

    def connect(self, _):
        endpoint = (
            None
            if len(self.connections) % 2 == 0
            else QmtEdgeEventEndpoint(
                store=self.store, codec=self.codec, healthy=lambda: self.healthy
            )
        )
        connection = Connection(endpoint)
        self.connections.append(connection)
        return connection

    def append(self, fact=None):
        fact = event() if fact is None else fact
        return self.store.append_outbox(
            event_id=fact.external_event_id,
            payload=self.codec.encode_fact(QmtBridgeEventDelivery(0, fact)),
        )

    def close(self):
        self.adapter.stop()
        self.store.close()


@pytest.fixture
def harness(tmp_path):
    value = Harness(tmp_path / "outbox.sqlite")
    try:
        yield value
    finally:
        value.close()


def test_qmt_two_streams_deliver_through_common_observer_then_ack(harness):
    harness.append()
    assert isinstance(harness.adapter, BrokerAdapter)
    harness.adapter.start(harness.observer)
    assert len(harness.connections) == 2
    assert harness.bridge.receive_one_event() == 1
    assert harness.observer.events[0].venue_id == "QMT"
    assert harness.observer.events[0].received_monotonic_ns != 10
    assert harness.store.pending_outbox() == ()
    assert harness.adapter.snapshot().acknowledged_delivery_id == 1
    assert harness.bridge.receive_one_event() == 0  # health is not query completion
    harness.adapter.stop()
    harness.append(event("event-2"))
    harness.adapter.start(harness.observer)
    assert harness.bridge.receive_one_event() == 1
    assert len(harness.observer.events) == 2


def test_qmt_lost_ack_reply_replays_retained_fact_without_duplicate_projection(harness):
    harness.append()
    harness.adapter.start(harness.observer)
    harness.connections[1].drop_ack = True
    with pytest.raises(QmtMtlsBridgeError, match="event delivery failed") as error:
        harness.bridge.receive_one_event()
    assert isinstance(error.value.__cause__, OSError)
    assert harness.store.pending_outbox() == ()  # server committed the ACK
    assert harness.adapter.snapshot().acknowledged_delivery_id is None
    assert all(connection.closed for connection in harness.connections)
    assert harness.adapter.snapshot().mutation_blocked
    assert len(harness.observer.gaps) == 1
    harness.adapter.stop()
    harness.adapter.start(harness.observer)
    assert harness.bridge.receive_one_event() == 1
    assert len(harness.observer.events) == 1
    assert harness.adapter.snapshot().acknowledged_delivery_id == 1
    assert harness.adapter.snapshot().mutation_blocked


def test_qmt_journal_failure_never_sends_ack_and_keeps_outbox(harness):
    harness.append()
    harness.adapter.start(harness.observer)
    harness.observer.fail = True
    with pytest.raises(QmtMtlsBridgeError, match="event delivery failed") as error:
        harness.bridge.receive_one_event()
    assert isinstance(error.value.__cause__, OSError)
    assert len(harness.store.pending_outbox()) == 1
    assert not any(frame.kind is QmtBridgeWireKind.ACK for frame in harness.connections[1].sent)
    assert all(connection.closed for connection in harness.connections)


@pytest.mark.parametrize(
    "field,value",
    [
        ("scope_digest", "c" * 64),
        ("outbox_epoch", "d" * 64),
        ("delivery_id", 2),
        ("delivery_id", True),
    ],
)
def test_qmt_wrong_fence_or_sequence_closes_both_streams(harness, field, value):
    harness.append()
    harness.adapter.start(harness.observer)
    harness.connections[1].corrupt_event = lambda payload: payload.update({field: value})
    with pytest.raises(QmtEventWireError):
        harness.bridge.receive_one_event()
    assert harness.observer.events == []
    assert len(harness.observer.gaps) == 1
    assert harness.adapter.snapshot().mutation_blocked
    assert all(connection.closed for connection in harness.connections)


def test_qmt_unhealthy_event_stream_closes_admission_before_pending_facts(harness):
    harness.append()
    harness.adapter.start(harness.observer)
    harness.healthy = False
    with pytest.raises(QmtEventWireError, match="unhealthy"):
        harness.bridge.receive_one_event()
    assert harness.observer.events == []
    assert harness.adapter.snapshot().mutation_blocked


def test_qmt_gap_uses_common_observer_and_is_acknowledged(harness):
    gap = CallbackGapEvidence("gap-1", ACCOUNT, "native", 4, 4, 1, 20)
    harness.store.append_outbox(
        event_id="gap-1", payload=harness.codec.encode_fact(QmtBridgeGapDelivery(0, gap))
    )
    harness.adapter.start(harness.observer)
    assert harness.bridge.receive_one_event() == 1
    assert harness.observer.gaps == [gap]
    assert harness.store.pending_outbox() == ()
    assert harness.adapter.snapshot().mutation_blocked


@pytest.mark.parametrize("resume,epoch", [(1, EPOCH), (0, "d" * 64)])
def test_qmt_invalid_resume_hello_cleans_up_both_connections(tmp_path, resume, epoch):
    harness = Harness(tmp_path / "outbox.sqlite", resume=resume, epoch=epoch)
    try:
        with pytest.raises(QmtEventWireError):
            harness.adapter.start(harness.observer)
        assert all(connection.closed for connection in harness.connections)
        assert not harness.adapter.snapshot().started
    finally:
        harness.close()


def test_qmt_foreign_account_and_unknown_fields_are_rejected(harness):
    with pytest.raises(QmtEventWireError, match="another account"):
        harness.codec.encode_fact(QmtBridgeEventDelivery(1, replace(event(), account_id="foreign")))
    payload = dict(
        decode_canonical_wire_payload(harness.codec.encode_fact(QmtBridgeEventDelivery(1, event())))
    )
    payload["fact"]["unexpected"] = "vendor-object"
    with pytest.raises(QmtEventWireError):
        harness.codec.decode_fact(canonical_wire_payload(payload), delivery_id=1)


def test_qmt_outbox_identity_is_persistent(tmp_path):
    path = tmp_path / "outbox.sqlite"
    with_store = QmtEdgeStore(path)
    with_store.bind_event_stream(scope_digest=SCOPE, outbox_epoch=EPOCH, account_id=ACCOUNT)
    with_store.close()
    reopened = QmtEdgeStore(path)
    try:
        with pytest.raises(QmtEdgeStoreError, match="another scope"):
            reopened.bind_event_stream(
                scope_digest=SCOPE, outbox_epoch="d" * 64, account_id=ACCOUNT
            )
    finally:
        reopened.close()


def test_qmt_edge_rejects_ack_for_an_unsent_or_different_fact(harness):
    harness.append()
    endpoint = QmtEdgeEventEndpoint(store=harness.store, codec=harness.codec, healthy=lambda: True)
    hello = harness.codec.control(
        QmtBridgeWireKind.HELLO, "QmtEventHelloV1", role="EVENT", resume_delivery_id=0
    )
    endpoint.open(hello)
    ack = harness.codec.control(
        QmtBridgeWireKind.ACK, "QmtEventAckV1", delivery_id=1, payload_hash="0" * 64
    )
    with pytest.raises(QmtEventWireError, match="no in-flight"):
        endpoint.acknowledge(ack)
    endpoint.next_frame()
    with pytest.raises(QmtEventWireError, match="does not match"):
        endpoint.acknowledge(ack)
    assert len(harness.store.pending_outbox()) == 1


def test_qmt_raw_ring_payload_is_never_presented_as_a_normalized_event(harness):
    harness.store.append_outbox(event_id="raw-native", payload=b"raw-ring-frame")
    harness.adapter.start(harness.observer)
    with pytest.raises(QmtEventWireError):
        harness.bridge.receive_one_event()
    assert harness.observer.events == []
    assert len(harness.store.pending_outbox()) == 1


def test_qmt_fresh_client_resumes_from_supplied_durable_cursor(tmp_path):
    first = Harness(tmp_path / "outbox.sqlite")
    first.append()
    first.adapter.start(first.observer)
    assert first.bridge.receive_one_event() == 1
    first.close()
    resumed = Harness(tmp_path / "outbox.sqlite", resume=1)
    try:
        resumed.append(event("event-2"))
        resumed.adapter.start(resumed.observer)
        assert resumed.bridge.receive_one_event() == 1
        assert [fact.event_id for fact in resumed.observer.events] == ["qmt:event-2"]
        assert resumed.adapter.snapshot().acknowledged_delivery_id == 2
    finally:
        resumed.close()


def test_qmt_command_only_client_cannot_send_mutations(harness):
    bridge = QmtMtlsBridgePort(
        replace(harness.config, events=None), connection_factory=harness.connect
    )
    bridge.open(lambda _: None)
    try:
        with pytest.raises(QmtMtlsBridgeError, match="connected event stream"):
            bridge.submit(None)
        with pytest.raises(QmtMtlsBridgeError, match="connected event stream"):
            bridge.cancel(None, None)
    finally:
        bridge.close()


def test_qmt_two_connections_over_real_loopback_tls13(harness, tmp_path):
    """Real certificates/framing/ACK; this server has no native trading port."""
    import socket
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, timedelta, timezone
    from hashlib import sha256

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    from xuanling_tradecore.adapters.qmt.bridge_wire import receive_wire_frame, send_wire_frame
    from xuanling_tradecore.adapters.qmt.mtls_channel import (
        QmtMtlsServerConfig,
        create_qmt_mtls_server_context,
        verify_qmt_mtls_client,
    )

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "QMT test CA")])
    now = datetime.now(timezone.utc)

    def certificate(name, key, *, ca=False, usage=None):
        builder = (
            x509.CertificateBuilder()
            .subject_name(
                ca_name if ca else x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
            )
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        )
        if usage is not None:
            builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            builder = builder.add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
            )
        result = builder.sign(ca_key, hashes.SHA256())
        cert_path, key_path = tmp_path / f"{name}.pem", tmp_path / f"{name}.key"
        cert_path.write_bytes(result.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        return (
            cert_path,
            key_path,
            sha256(result.public_bytes(serialization.Encoding.DER)).hexdigest(),
        )

    ca_path, _, _ = certificate("ca", ca_key, ca=True)
    server_path, server_key, server_pin = certificate(
        "server",
        rsa.generate_private_key(public_exponent=65537, key_size=2048),
        usage=ExtendedKeyUsageOID.SERVER_AUTH,
    )
    client_path, client_key, client_pin = certificate(
        "client",
        rsa.generate_private_key(public_exponent=65537, key_size=2048),
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )
    harness.append()
    done = threading.Event()
    with socket.socket() as listener, ThreadPoolExecutor(max_workers=2) as pool:
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        listener.settimeout(5)
        port = listener.getsockname()[1]
        server_config = QmtMtlsServerConfig(
            "127.0.0.1", port, ca_path, server_path, server_key, client_pin
        )
        context = create_qmt_mtls_server_context(server_config)

        def serve(command):
            raw, _ = listener.accept()
            raw.settimeout(5)
            with context.wrap_socket(raw, server_side=True) as connection:
                verify_qmt_mtls_client(connection, server_config)
                assert connection.version() == "TLSv1.3"
                hello = receive_wire_frame(connection)
                if command:
                    assert decode_canonical_wire_payload(hello.payload)["role"] == "COMMAND"
                    send_wire_frame(
                        connection,
                        QmtBridgeWireFrame(
                            QmtBridgeWireKind.HEALTH,
                            canonical_wire_payload(
                                {"schema": "QmtEdgeHealthV1", "ready": True, "detail_code": "READY"}
                            ),
                        ),
                    )
                    assert done.wait(5)
                else:
                    store = QmtEdgeStore(tmp_path / "outbox.sqlite", synchronous="FULL")
                    try:
                        endpoint = QmtEdgeEventEndpoint(
                            store=store, codec=harness.codec, healthy=lambda: True
                        )
                        send_wire_frame(connection, endpoint.open(hello))
                        send_wire_frame(connection, endpoint.next_frame())
                        send_wire_frame(
                            connection, endpoint.acknowledge(receive_wire_frame(connection))
                        )
                    finally:
                        store.close()

        command_future = pool.submit(serve, True)
        # Dispatch the second handler only after the COMMAND HELLO completed.
        from xuanling_tradecore.adapters.qmt.mtls_channel import QmtMtlsConnection

        calls = 0
        event_futures = []

        def connect(config):
            nonlocal calls
            calls += 1
            if calls == 2:
                event_futures.append(pool.submit(serve, False))
            return QmtMtlsConnection.connect(config)

        tls = QmtMtlsClientConfig(
            "127.0.0.1", port, ca_path, client_path, client_key, server_pin, "localhost", 3.0
        )
        bridge = QmtMtlsBridgePort(replace(harness.config, tls=tls), connection_factory=connect)
        received = []

        def sink(delivery):
            received.append(delivery)
            bridge.acknowledge(delivery.delivery_id)

        try:
            bridge.open(sink)
            assert bridge.receive_one_event() == 1
            assert received[0].event.external_event_id == "event-1"
            assert harness.store.pending_outbox() == ()
        finally:
            done.set()
            bridge.close()
        command_future.result(timeout=5)
        for future in event_futures:
            future.result(timeout=5)

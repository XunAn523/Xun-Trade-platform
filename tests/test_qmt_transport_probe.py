"""Real TLS/service/SQLite boundaries, with no native runtime or trading account."""

import json
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import replace

import pytest
from tools.qmt_bridge_pki import certificate_summary, initialize_ca, request_leaf, sign_leaf
from xuanling_tradecore.adapters.qmt.bridge_wire import (
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)
from xuanling_tradecore.adapters.qmt.edge_service import QmtEdgeService
from xuanling_tradecore.adapters.qmt.edge_store import QmtEdgeStore
from xuanling_tradecore.adapters.qmt.event_wire import QmtEventWireCodec, QmtEventWireError
from xuanling_tradecore.adapters.qmt.mtls_channel import (
    QmtMtlsChannelError,
    QmtMtlsClientConfig,
    QmtMtlsConnection,
    QmtMtlsServerConfig,
)
from xuanling_tradecore.adapters.qmt.probe_deployment import load_config
from xuanling_tradecore.adapters.qmt.transport_probe import (
    QmtProbeJournal,
    QmtTransportProbeCodec,
    QmtTransportProbeConfig,
    QmtTransportProbeOwner,
    run_transport_probe,
)


def wait_until(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.01)


@pytest.fixture
def harness(tmp_path):
    ca, edge, client = (tmp_path / name for name in ("ca", "edge", "client"))
    initialize_ca(ca)
    for directory, role in ((edge, "windows-edge"), (client, "linux-client")):
        request_leaf(directory, role)
        sign_leaf(ca, directory / f"{role}.csr", directory / f"{role}.pem", role, "127.0.0.1")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tls = QmtMtlsClientConfig(
        "127.0.0.1",
        port,
        ca / "bridge-ca.pem",
        client / "linux-client.pem",
        client / "linux-client-key.pem",
        certificate_summary(edge / "windows-edge.pem")["certificate_sha256"],
        "127.0.0.1",
        2.0,
        "127.0.0.1",
    )
    server_tls = QmtMtlsServerConfig(
        "127.0.0.1",
        port,
        ca / "bridge-ca.pem",
        edge / "windows-edge.pem",
        edge / "windows-edge-key.pem",
        certificate_summary(client / "linux-client.pem")["certificate_sha256"],
    )
    config = QmtTransportProbeConfig("a" * 64, "b" * 64, 4)
    actor_threads = []

    def factory():
        actor_threads.append(threading.get_ident())
        return QmtTransportProbeOwner(tmp_path / "outbox.sqlite", config)

    service = QmtEdgeService(owner_factory=factory, tls=server_tls, allowed_peer_host="127.0.0.1")
    service.start()
    value = dict(
        tls=tls,
        server_tls=server_tls,
        config=config,
        service=service,
        factory=factory,
        journal_path=tmp_path / "journal.sqlite",
        path=tmp_path,
        actor_threads=actor_threads,
    )
    try:
        yield value
    finally:
        value["service"].stop()


def run(harness, **kwargs):
    return run_transport_probe(
        **{k: harness[k] for k in ("tls", "config", "journal_path")}, **kwargs
    )


def pending(harness):
    with sqlite3.connect(harness["path"] / "outbox.sqlite") as connection:
        return connection.execute("SELECT count(*) FROM outbox WHERE acknowledged=0").fetchone()[0]


def test_real_dual_tls_probe_keeps_trading_disarmed_and_resumes(harness):
    result = run(harness)
    assert result == dict(
        status="TRANSPORT_PROBE_COMPLETE",
        probe_only=True,
        mutation_allowed=False,
        capability_accepted=False,
        confirmed_cursor=4,
        received=4,
        replayed=0,
        event_count=4,
    )
    assert pending(harness) == 0
    wait_until(lambda: harness["service"].snapshot().active_connections == 0)
    assert run(harness)["received"] == 0
    assert harness["actor_threads"] != [threading.get_ident()]
    with sqlite3.connect(harness["path"] / "outbox.sqlite") as connection:
        assert connection.execute("SELECT count(*) FROM requests").fetchone()[0] == 0


@pytest.mark.parametrize("phase,unacked", [("after_journal", 3), ("after_ack", 2)])
def test_restart_replays_persisted_receipt_before_advancing_cursor(harness, phase, unacked):
    result = run(harness, interrupt_after=2, interrupt_phase=phase)
    assert result["confirmed_cursor"] == 1
    assert pending(harness) == unacked
    harness["service"].stop()
    harness["service"] = QmtEdgeService(owner_factory=harness["factory"], tls=harness["server_tls"])
    harness["service"].start()
    result = run(harness)
    assert result["confirmed_cursor"] == 4 and result["replayed"] == 1
    with sqlite3.connect(harness["journal_path"]) as connection:
        assert connection.execute("SELECT count(*) FROM receipts").fetchone()[0] == 4


@pytest.mark.parametrize("kind", ["pin", "hostname", "scope", "epoch", "client_pin", "ca"])
def test_wrong_tls_or_stream_identity_does_not_ack(harness, kind):
    tls, config = harness["tls"], harness["config"]
    if kind == "pin":
        tls = replace(tls, expected_server_certificate_sha256="c" * 64)
    elif kind == "hostname":
        tls = replace(tls, server_hostname="wrong.invalid")
    elif kind == "scope":
        config = replace(config, scope_digest="c" * 64)
    elif kind == "epoch":
        config = replace(config, outbox_epoch="c" * 64)
    elif kind == "client_pin":
        harness["service"].stop()
        harness["service"] = QmtEdgeService(
            owner_factory=harness["factory"],
            tls=replace(harness["server_tls"], expected_client_certificate_sha256="c" * 64),
        )
        harness["service"].start()
    else:
        initialize_ca(harness["path"] / "untrusted")
        tls = replace(tls, ca_certificate_path=harness["path"] / "untrusted/bridge-ca.pem")
    with pytest.raises((QmtMtlsChannelError, QmtEventWireError)):
        run_transport_probe(tls=tls, config=config, journal_path=harness["journal_path"])
    assert pending(harness) == 4


def test_journal_failure_never_acknowledges(harness, monkeypatch):
    def fail(*_):
        raise OSError("fsync failed")

    monkeypatch.setattr(QmtProbeJournal, "record", fail)
    with pytest.raises(OSError):
        run(harness)
    assert pending(harness) == 4


def test_stop_interrupts_incomplete_tls_handshake(harness):
    raw = socket.create_connection((harness["tls"].host, harness["tls"].port))
    try:
        wait_until(lambda: harness["service"].snapshot().active_connections == 1)
        start = time.monotonic()
        harness["service"].stop()
        assert time.monotonic() - start < 3
        assert harness["service"].snapshot().active_connections == 0
    finally:
        raw.close()


def test_command_channel_rejects_all_commands_without_native_runtime(harness):
    connection = QmtMtlsConnection.connect(harness["tls"])
    try:
        connection.send(
            QmtBridgeWireFrame(
                QmtBridgeWireKind.HELLO,
                canonical_wire_payload(
                    {
                        "schema": "QmtEdgeHelloV1",
                        "role": "COMMAND",
                        "scope_digest": harness["config"].scope_digest,
                    }
                ),
            )
        )
        assert connection.receive() == QmtTransportProbeOwner.command_health()
        connection.send(QmtBridgeWireFrame(QmtBridgeWireKind.COMMAND, b"{}"))
        assert (
            decode_canonical_wire_payload(connection.receive().payload)["code"]
            == "TRANSPORT_PROBE_DISARMED"
        )
    finally:
        connection.close()


def test_probe_schema_is_not_a_production_broker_event():
    config = QmtTransportProbeConfig("a" * 64, "b" * 64)
    codec = QmtTransportProbeCodec(config)
    frame = codec.encode_delivery(delivery_id=1, fact_payload=codec.fact(1))
    production = QmtEventWireCodec(scope_digest=config.scope_digest, config=config.events)
    with pytest.raises(QmtEventWireError):
        production.decode_delivery(frame)


def test_journal_rejects_identity_and_replay_drift(tmp_path):
    config = QmtTransportProbeConfig("a" * 64, "b" * 64)
    journal = QmtProbeJournal(tmp_path / "journal", config, "c" * 64)
    try:
        assert journal.record(1, b"original")
        assert not journal.record(1, b"original")
        with pytest.raises(QmtEventWireError, match="bytes changed"):
            journal.record(1, b"changed")
        with pytest.raises(QmtEventWireError, match="durable receipt"):
            journal.confirm(2)
    finally:
        journal.close()
    with pytest.raises(QmtEventWireError, match="identity changed"):
        QmtProbeJournal(tmp_path / "journal", replace(config, outbox_epoch="d" * 64), "c" * 64)


def test_changed_probe_count_refuses_old_database(tmp_path):
    config = QmtTransportProbeConfig("a" * 64, "b" * 64, 2)
    QmtTransportProbeOwner(tmp_path / "outbox", config).close()
    with pytest.raises(RuntimeError):
        QmtTransportProbeOwner(tmp_path / "outbox", replace(config, event_count=3))


def test_probe_refuses_native_store_and_competing_writer(tmp_path):
    config = QmtTransportProbeConfig("a" * 64, "b" * 64)
    QmtEdgeStore(tmp_path / "native").close()
    with pytest.raises(RuntimeError, match="non-probe"):
        QmtTransportProbeOwner(tmp_path / "native", config)
    owner = QmtTransportProbeOwner(tmp_path / "probe", config)
    try:
        with pytest.raises(OSError):
            QmtTransportProbeOwner(tmp_path / "probe", config)
    finally:
        owner.close()
    QmtTransportProbeOwner(tmp_path / "probe", config).close()


def test_probe_journal_has_exclusive_writer(tmp_path):
    config = QmtTransportProbeConfig("a" * 64, "b" * 64)
    journal = QmtProbeJournal(tmp_path / "journal", config, "c" * 64)
    try:
        with pytest.raises(OSError):
            QmtProbeJournal(tmp_path / "journal", config, "c" * 64)
    finally:
        journal.close()


def test_duplicate_event_session_and_wrong_ack_are_rejected(harness):
    config = harness["config"]
    codec = QmtTransportProbeCodec(config)
    first = QmtMtlsConnection.connect(harness["tls"])
    second = QmtMtlsConnection.connect(harness["tls"])
    hello = codec.control(
        QmtBridgeWireKind.HELLO, "QmtEventHelloV1", role="EVENT", resume_delivery_id=0
    )
    try:
        first.send(hello)
        assert first.receive().kind == QmtBridgeWireKind.ACCEPTED
        first.receive()
        second.send(hello)
        with pytest.raises(QmtMtlsChannelError):
            second.receive()
        first.send(
            codec.control(
                QmtBridgeWireKind.ACK, "QmtEventAckV1", delivery_id=1, payload_hash="0" * 64
            )
        )
        with pytest.raises(QmtMtlsChannelError):
            first.receive()
        assert pending(harness) == 4
    finally:
        first.close()
        second.close()


def test_tls_peer_capacity_is_bounded(harness):
    raw = [socket.create_connection((harness["tls"].host, harness["tls"].port)) for _ in range(4)]
    try:
        wait_until(lambda: harness["service"].snapshot().active_connections == 4)
        with socket.create_connection((harness["tls"].host, harness["tls"].port)) as fifth:
            fifth.settimeout(2)
            assert fifth.recv(1) == b""
        assert harness["service"].snapshot().active_connections == 4
    finally:
        for connection in raw:
            connection.close()


def test_deployed_client_entry_reports_interruption_then_resume(harness):
    tls, config = harness["tls"], harness["config"]
    fields = {
        "schema": "TradeCoreQmtTransportProbeConfigV1",
        "role": "LINUX_CLIENT",
        "mode": "DISARMED",
        "local_host": "127.0.0.1",
        "peer_host": "127.0.0.1",
        "port": tls.port,
        "scope_digest": config.scope_digest,
        "outbox_epoch": config.outbox_epoch,
        "event_count": config.event_count,
        "database_path": str(harness["journal_path"]),
        "ca_certificate_path": str(tls.ca_certificate_path),
        "certificate_path": str(tls.certificate_path),
        "private_key_path": str(tls.private_key_path),
        "expected_peer_certificate_sha256": tls.expected_server_certificate_sha256,
    }
    path = harness["path"] / "config.json"
    path.write_text(json.dumps(fields), encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "xuanling_tradecore.adapters.qmt.probe_deployment",
        "--config",
        str(path),
    ]
    interrupted = subprocess.run(
        command + ["--interrupt-after", "2"], capture_output=True, text=True, timeout=10
    )
    assert interrupted.returncode == 3, interrupted.stderr
    assert json.loads(interrupted.stdout)["confirmed_cursor"] == 1
    wait_until(lambda: harness["service"].snapshot().active_connections == 0)
    resumed = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout)["replayed"] == 1
    fields["mode"] = "ARMED"
    path.write_text(json.dumps(fields), encoding="utf-8")
    with pytest.raises(ValueError, match="DISARMED"):
        load_config(path)


def test_test_pki_never_overwrites_keys_and_checks_csr_role(tmp_path):
    initialize_ca(tmp_path / "ca")
    original = (tmp_path / "ca/bridge-ca-key.pem").read_bytes()
    with pytest.raises(FileExistsError):
        initialize_ca(tmp_path / "ca")
    assert (tmp_path / "ca/bridge-ca-key.pem").read_bytes() == original
    request_leaf(tmp_path / "edge", "windows-edge")
    with pytest.raises(ValueError, match="CSR role"):
        sign_leaf(
            tmp_path / "ca",
            tmp_path / "edge/windows-edge.csr",
            tmp_path / "wrong.pem",
            "linux-client",
            "127.0.0.1",
        )

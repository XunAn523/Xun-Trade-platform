"""DISARMED transport evidence, isolated from native rings and OMS journals.

The probe schema is intentionally rejected by the production event decoder.
It cannot represent orders, queries, capability activation or trading READY.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from .bridge_wire import (
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)
from .edge_event_endpoint import QmtEdgeEventEndpoint
from .edge_service import QmtEdgeServiceError
from .edge_store import QmtEdgeStore
from .event_wire import QmtEventStreamConfig, QmtEventWireCodec, QmtEventWireError
from .mtls_channel import QmtMtlsClientConfig, QmtMtlsConnection
from .probe_storage import QmtProbeStorageLease


@dataclass(frozen=True)
class QmtTransportProbeConfig:
    scope_digest: str
    outbox_epoch: str
    event_count: int = 8

    def __post_init__(self) -> None:
        QmtEventWireCodec(scope_digest=self.scope_digest, config=self.events)
        if type(self.event_count) is not int or not 1 <= self.event_count <= 1000:
            raise ValueError("QMT transport probe count must be 1..1000")

    @property
    def events(self) -> QmtEventStreamConfig:
        return QmtEventStreamConfig("__transport_probe_only__", self.outbox_epoch)


class QmtTransportProbeCodec(QmtEventWireCodec):
    """Reuse scope/epoch and ACK controls, never production broker event facts."""

    def __init__(self, config: QmtTransportProbeConfig) -> None:
        super().__init__(scope_digest=config.scope_digest, config=config.events)
        self.probe_config = config

    def fact(self, sequence: int) -> bytes:
        return canonical_wire_payload(
            {
                "schema": "QmtTransportProbeFactV1",
                "probe_only": True,
                "sequence": sequence,
                "event_count": self.probe_config.event_count,
            }
        )

    def encode_delivery(self, *, delivery_id: int, fact_payload: bytes) -> QmtBridgeWireFrame:
        if not 1 <= delivery_id <= self.probe_config.event_count or fact_payload != self.fact(
            delivery_id
        ):
            raise QmtEventWireError("QMT transport probe outbox has another fact")
        return self.control(
            QmtBridgeWireKind.EVENT,
            "QmtTransportProbeDeliveryV1",
            delivery_id=delivery_id,
            probe_only=True,
            content=dict(decode_canonical_wire_payload(fact_payload)),
        )

    def validate_probe(self, frame: QmtBridgeWireFrame, expected: int) -> None:
        if frame != self.encode_delivery(delivery_id=expected, fact_payload=self.fact(expected)):
            raise QmtEventWireError("QMT transport probe delivery or sequence differs")


class QmtTransportProbeOwner:
    """One actor owns both roles and a dedicated FULL synchronous test outbox."""

    def __init__(self, database_path: Path, config: QmtTransportProbeConfig) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.codec = QmtTransportProbeCodec(config)
        self._lease = QmtProbeStorageLease(database_path, application_id=0x514D5450)
        try:
            self.store = QmtEdgeStore(database_path, synchronous="FULL")
        except BaseException:
            self._lease.close()
            raise
        self.sessions: dict[int, QmtEdgeEventEndpoint | None] = {}
        try:
            self.store.bind_event_stream(
                scope_digest=config.scope_digest,
                outbox_epoch=config.outbox_epoch,
                account_id=config.events.account_id,
            )
            if self.store.outbox_high_watermark() > config.event_count:
                raise QmtEdgeServiceError("QMT probe count changed; use a new test epoch/database")
            for sequence in range(1, config.event_count + 1):
                self.store.append_outbox(
                    event_id=f"transport-probe:{sequence}",
                    payload=self.codec.fact(sequence),
                )
        except BaseException:
            self.store.close()
            self._lease.close()
            raise

    def open_session(self, session: int, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        payload = decode_canonical_wire_payload(frame.payload)
        role = payload.get("role")
        if role == "COMMAND":
            if frame.kind is not QmtBridgeWireKind.HELLO or payload != {
                "schema": "QmtEdgeHelloV1",
                "role": "COMMAND",
                "scope_digest": self.codec.scope_digest,
            }:
                raise QmtEdgeServiceError("QMT probe command HELLO differs")
            if None in self.sessions.values():
                raise QmtEdgeServiceError("QMT probe command session already owned")
            self.sessions[session] = None
            return self.command_health()
        if role != "EVENT" or any(value is not None for value in self.sessions.values()):
            raise QmtEdgeServiceError("QMT probe event role invalid or already owned")
        endpoint = QmtEdgeEventEndpoint(store=self.store, codec=self.codec, healthy=lambda: True)
        response = endpoint.open(frame)
        self.sessions[session] = endpoint
        return response

    def handle(self, session: int, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        endpoint = self.sessions[session]
        if endpoint is not None:
            return endpoint.acknowledge(frame)
        if frame.kind is QmtBridgeWireKind.HEALTH and frame.payload == b"{}":
            return self.command_health()
        # No native runtime/controller is created. Reject even a valid QUERY.
        return QmtBridgeWireFrame(
            QmtBridgeWireKind.REJECTED,
            canonical_wire_payload(
                {
                    "schema": "QmtEdgeRejectV1",
                    "code": "TRANSPORT_PROBE_DISARMED",
                }
            ),
        )

    def next_event(self, session: int) -> QmtBridgeWireFrame:
        endpoint = self.sessions[session]
        if endpoint is None:
            raise QmtEdgeServiceError("QMT command session cannot read events")
        return endpoint.next_frame()

    def close_session(self, session: int) -> None:
        self.sessions.pop(session, None)

    def close(self) -> None:
        try:
            self.store.close()
        finally:
            self._lease.close()

    @staticmethod
    def command_health() -> QmtBridgeWireFrame:
        return QmtBridgeWireFrame(
            QmtBridgeWireKind.HEALTH,
            canonical_wire_payload(
                {
                    "schema": "QmtEdgeHealthV1",
                    "ready": False,
                    "detail_code": "TRANSPORT_PROBE_DISARMED",
                }
            ),
        )


class QmtProbeJournal:
    """Probe-only durable receipt log and confirmed-ACK cursor in one transaction DB.

    Record+fsync precedes ACK. Cursor advances only after an exact ACK reply.
    A crash between either boundary replays and compares the original bytes.
    This is not the OMS mmap Journal observer or a production recovery proof.
    """

    def __init__(self, path: Path, config: QmtTransportProbeConfig, peer_pin: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lease = QmtProbeStorageLease(path, application_id=0x514D544A)
        try:
            self.connection = sqlite3.connect(path, isolation_level=None)
        except BaseException:
            self._lease.close()
            raise
        try:
            self.connection.execute("PRAGMA journal_mode=DELETE")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.executescript("""
                CREATE TABLE IF NOT EXISTS identity (id INTEGER PRIMARY KEY CHECK(id=1), value BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS receipts (id INTEGER PRIMARY KEY, payload BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS cursor (id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL);
                INSERT OR IGNORE INTO cursor VALUES (1, 0);
            """)
            identity = canonical_wire_payload(
                {
                    "schema": "QmtTransportProbeJournalV1",
                    "scope": config.scope_digest,
                    "epoch": config.outbox_epoch,
                    "count": config.event_count,
                    "peer_pin": peer_pin,
                }
            )
            self.connection.execute("INSERT OR IGNORE INTO identity VALUES (1, ?)", (identity,))
            if (
                self.connection.execute("SELECT value FROM identity WHERE id=1").fetchone()[0]
                != identity
            ):
                raise QmtEventWireError("QMT probe journal identity changed")
        except BaseException:
            self.connection.close()
            self._lease.close()
            raise

    @property
    def cursor(self) -> int:
        return int(self.connection.execute("SELECT value FROM cursor WHERE id=1").fetchone()[0])

    def record(self, delivery_id: int, payload: bytes) -> bool:
        if delivery_id != self.cursor + 1:
            raise QmtEventWireError("QMT probe journal sequence is not contiguous")
        existing = self.connection.execute(
            "SELECT payload FROM receipts WHERE id=?", (delivery_id,)
        ).fetchone()
        if existing is not None:
            if existing[0] != payload:
                raise QmtEventWireError("QMT probe replay bytes changed")
            return False
        self.connection.execute("INSERT INTO receipts VALUES (?, ?)", (delivery_id, payload))
        return True

    def confirm(self, delivery_id: int) -> None:
        if (
            delivery_id != self.cursor + 1
            or self.connection.execute(
                "SELECT id FROM receipts WHERE id=?", (delivery_id,)
            ).fetchone()
            is None
        ):
            raise QmtEventWireError("QMT probe ACK lacks a durable receipt")
        self.connection.execute("UPDATE cursor SET value=? WHERE id=1", (delivery_id,))

    def close(self) -> None:
        try:
            self.connection.close()
        finally:
            self._lease.close()


def run_transport_probe(
    *,
    tls: QmtMtlsClientConfig,
    config: QmtTransportProbeConfig,
    journal_path: Path,
    interrupt_after: int | None = None,
    interrupt_phase: str = "after_journal",
) -> dict[str, object]:
    """Open both channels; no native command is constructed or transmitted.

    Fault options stop before ACK or after its reply but before cursor commit.
    Re-running with the same DB/epoch must replay that receipt exactly once.
    """
    if interrupt_after is not None and not 1 <= interrupt_after <= config.event_count:
        raise ValueError("QMT probe interrupt sequence is out of range")
    if interrupt_phase not in {"after_journal", "after_ack"}:
        raise ValueError("QMT probe interrupt phase is invalid")
    codec = QmtTransportProbeCodec(config)
    journal = QmtProbeJournal(journal_path, config, tls.expected_server_certificate_sha256)
    command: QmtMtlsConnection | None = None
    events: QmtMtlsConnection | None = None
    replayed = received = 0
    try:
        command = QmtMtlsConnection.connect(tls)
        command.send(
            QmtBridgeWireFrame(
                QmtBridgeWireKind.HELLO,
                canonical_wire_payload(
                    {
                        "schema": "QmtEdgeHelloV1",
                        "role": "COMMAND",
                        "scope_digest": config.scope_digest,
                    }
                ),
            )
        )
        if command.receive() != QmtTransportProbeOwner.command_health():
            raise QmtEventWireError("QMT probe requires explicit DISARMED command health")
        events = QmtMtlsConnection.connect(tls)
        events.send(
            codec.control(
                QmtBridgeWireKind.HELLO,
                "QmtEventHelloV1",
                role="EVENT",
                resume_delivery_id=journal.cursor,
            )
        )
        expected = codec.control(
            QmtBridgeWireKind.ACCEPTED, "QmtEventHelloAcceptedV1", resume_delivery_id=journal.cursor
        )
        if events.receive() != expected:
            raise QmtEventWireError("QMT probe resume cursor was not accepted exactly")
        status = "TRANSPORT_PROBE_COMPLETE"
        while journal.cursor < config.event_count:
            frame = events.receive()
            sequence = journal.cursor + 1
            codec.validate_probe(frame, sequence)
            fresh = journal.record(sequence, frame.payload)
            received += 1
            replayed += int(not fresh)
            if sequence == interrupt_after and interrupt_phase == "after_journal":
                status = "INTERRUPTED_AFTER_JOURNAL"
                break
            ack = codec.control(
                QmtBridgeWireKind.ACK,
                "QmtEventAckV1",
                delivery_id=sequence,
                payload_hash=sha256(frame.payload).hexdigest(),
            )
            events.send(ack)
            if events.receive() != ack:
                raise QmtEventWireError("QMT probe ACK reply differs")
            if sequence == interrupt_after and interrupt_phase == "after_ack":
                status = "INTERRUPTED_AFTER_ACK_BEFORE_CURSOR"
                break
            journal.confirm(sequence)
            # Keep COMMAND alive while testing the independent event stream.
            command.send(QmtBridgeWireFrame(QmtBridgeWireKind.HEALTH, b"{}"))
            if command.receive() != QmtTransportProbeOwner.command_health():
                raise QmtEventWireError("QMT probe command health changed")
        # A completed resume still checks the event channel, not just its HELLO.
        if status == "TRANSPORT_PROBE_COMPLETE":
            expected = codec.control(QmtBridgeWireKind.HEALTH, "QmtEventHealthV1", ready=True)
            if events.receive() != expected:
                raise QmtEventWireError("QMT probe expected retained outbox end")
        return {
            "status": status,
            "probe_only": True,
            "mutation_allowed": False,
            "capability_accepted": False,
            "confirmed_cursor": journal.cursor,
            "received": received,
            "replayed": replayed,
            "event_count": config.event_count,
        }
    finally:
        if events is not None:
            events.close()
        if command is not None:
            command.close()
        journal.close()

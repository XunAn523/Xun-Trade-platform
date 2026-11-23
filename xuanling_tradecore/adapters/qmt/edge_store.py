"""SQLite state machine used by the Windows QMT Edge deployment.

The store is intentionally vendor-free.  It persists request identity before
the command ring is touched and persists callback/outbox records before Linux
delivery.  Native QMT calls are never made from this module.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns

from ...domain._validation import require_nonnegative, require_sha256, require_text
from .bridge_protocol import QmtBridgeError, QmtSubmitCommand


class QmtEdgeStoreError(QmtBridgeError):
    """A request, outbox, or SQLite invariant was violated."""


class QmtEdgeIdempotencyConflict(QmtEdgeStoreError):
    """One request id was reused with different canonical content."""


@dataclass(frozen=True)
class QmtEdgeRequest:
    request_id: str
    command_hash: str
    command_sequence: int
    state: str
    accepted_monotonic_ns: int


@dataclass(frozen=True)
class QmtEdgeOutboxRecord:
    delivery_id: int
    event_id: str
    payload_hash: str
    payload: bytes


class QmtEdgeStore:
    """One scope-local SQLite store with deterministic request transitions."""

    def __init__(self, database_path: Path, *, synchronous: str = "NORMAL") -> None:
        if synchronous not in {"NORMAL", "FULL"}:
            raise QmtEdgeStoreError("QMT Edge SQLite synchronous mode must be NORMAL or FULL")
        self._connection = sqlite3.connect(str(database_path), isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute(f"PRAGMA synchronous={synchronous}")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._initialize()

    def close(self) -> None:
        self._connection.close()

    def accept_submit(self, command: QmtSubmitCommand) -> QmtEdgeRequest:
        """Persist a deterministic command before native publication.

        The same request and canonical content returns its existing row.  The
        same request id with different content is always a conflict.
        """

        return self.accept_request(
            request_id=command.instruction.execution_id,
            canonical_content=self._command_hash(command).encode("ascii"),
        )

    def accept_request(self, *, request_id: str, canonical_content: bytes) -> QmtEdgeRequest:
        """Persist a generic canonical Edge command before native publication.

        This is used by the mTLS Edge process.  It preserves the same request
        identity/idempotency behavior as :meth:`accept_submit` without making
        the Windows service deserialize Linux domain objects.
        """

        require_text(request_id, "QMT Edge request_id")
        if not canonical_content:
            raise QmtEdgeStoreError("QMT Edge canonical command cannot be empty")
        if len(canonical_content) > 1 << 20:
            raise QmtEdgeStoreError("QMT Edge canonical command exceeds its bounded limit")
        command_hash = sha256(canonical_content).hexdigest()
        self._begin()
        try:
            row = self._connection.execute(
                "SELECT request_id, command_hash, command_sequence, state, accepted_monotonic_ns "
                "FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                if str(row["command_hash"]) != command_hash:
                    raise QmtEdgeIdempotencyConflict("QMT Edge request id has another command hash")
                self._connection.execute("COMMIT")
                return self._request_from_row(row)
            sequence = self._next_command_sequence()
            accepted = monotonic_ns()
            self._connection.execute(
                "INSERT INTO requests "
                "(request_id, command_hash, command_sequence, state, accepted_monotonic_ns) "
                "VALUES (?, ?, ?, 'EDGE_ACCEPTED', ?)",
                (request_id, command_hash, sequence, accepted),
            )
            self._connection.execute("COMMIT")
            return QmtEdgeRequest(request_id, command_hash, sequence, "EDGE_ACCEPTED", accepted)
        except BaseException:
            self._rollback()
            raise

    def record_publish_attempt(self, request_id: str, command_sequence: int) -> QmtEdgeRequest:
        """Record that the command may have reached the native ring.

        A recovery after this transition cannot blindly republish: it must use
        the original request key and authoritative QMT evidence.
        """

        row = self._require_request(request_id)
        if row.command_sequence != command_sequence:
            raise QmtEdgeStoreError("QMT Edge publish sequence belongs to another request")
        if row.state == "PUBLISH_PREPARED":
            self._connection.execute(
                "UPDATE requests SET state = 'PUBLISH_ATTEMPTED' WHERE request_id = ?",
                (request_id,),
            )
            return QmtEdgeRequest(
                row.request_id,
                row.command_hash,
                row.command_sequence,
                "PUBLISH_ATTEMPTED",
                row.accepted_monotonic_ns,
            )
        return row

    def record_publish_prepared(self, request_id: str, command_sequence: int) -> QmtEdgeRequest:
        """Persist a conservative pre-publication crash boundary.

        The row is marked before writing a ring slot.  A crash after this
        transition is intentionally treated as an unknown effect even if no
        slot was ultimately visible; this trades a possible false positive for
        never publishing the same native command twice after restart.
        """

        row = self._require_request(request_id)
        if row.command_sequence != command_sequence:
            raise QmtEdgeStoreError("QMT Edge publish sequence belongs to another request")
        if row.state == "EDGE_ACCEPTED":
            self._connection.execute(
                "UPDATE requests SET state = 'PUBLISH_PREPARED' WHERE request_id = ?", (request_id,)
            )
            return QmtEdgeRequest(
                row.request_id,
                row.command_hash,
                row.command_sequence,
                "PUBLISH_PREPARED",
                row.accepted_monotonic_ns,
            )
        return row

    def record_native_call_started(self, request_id: str, command_sequence: int) -> QmtEdgeRequest:
        return self._advance_native_state(request_id, command_sequence, "CALL_STARTED")

    def record_native_call_returned(self, request_id: str, command_sequence: int) -> QmtEdgeRequest:
        return self._advance_native_state(request_id, command_sequence, "CALL_RETURNED")

    def mark_unknown_effects(self) -> tuple[QmtEdgeRequest, ...]:
        """Fail closed after an unclean Edge/native restart."""

        self._connection.execute(
            "UPDATE requests SET state = 'UNKNOWN_EFFECT' "
            "WHERE state IN ('PUBLISH_PREPARED', 'PUBLISH_ATTEMPTED', 'CALL_STARTED')"
        )
        rows = self._connection.execute(
            "SELECT request_id, command_hash, command_sequence, state, accepted_monotonic_ns "
            "FROM requests WHERE state = 'UNKNOWN_EFFECT' ORDER BY command_sequence"
        ).fetchall()
        return tuple(self._request_from_row(row) for row in rows)

    def append_outbox(self, *, event_id: str, payload: bytes) -> QmtEdgeOutboxRecord:
        """Durably append one event before Edge sends it to Linux."""

        require_text(event_id, "QMT Edge outbox event_id", max_length=256)
        if not payload:
            raise QmtEdgeStoreError("QMT Edge outbox payload cannot be empty")
        payload_hash = sha256(payload).hexdigest()
        self._begin()
        try:
            row = self._connection.execute(
                "SELECT delivery_id, event_id, payload_hash, payload FROM outbox WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is not None:
                if str(row["payload_hash"]) != payload_hash:
                    raise QmtEdgeIdempotencyConflict("QMT Edge event id has another payload hash")
                self._connection.execute("COMMIT")
                return self._outbox_from_row(row)
            cursor = self._connection.execute(
                "INSERT INTO outbox (event_id, payload_hash, payload, acknowledged) VALUES (?, ?, ?, 0)",
                (event_id, payload_hash, payload),
            )
            if cursor.lastrowid is None:
                raise QmtEdgeStoreError("QMT Edge outbox insert returned no delivery id")
            delivery_id = int(cursor.lastrowid)
            self._connection.execute("COMMIT")
            return QmtEdgeOutboxRecord(delivery_id, event_id, payload_hash, payload)
        except BaseException:
            self._rollback()
            raise

    def pending_outbox(self) -> tuple[QmtEdgeOutboxRecord, ...]:
        rows = self._connection.execute(
            "SELECT delivery_id, event_id, payload_hash, payload FROM outbox "
            "WHERE acknowledged = 0 ORDER BY delivery_id"
        ).fetchall()
        return tuple(self._outbox_from_row(row) for row in rows)

    def acknowledge_outbox(self, delivery_id: int) -> None:
        require_nonnegative(delivery_id, "QMT Edge acknowledgement delivery_id")
        cursor = self._connection.execute(
            "UPDATE outbox SET acknowledged = 1 WHERE delivery_id = ?", (delivery_id,)
        )
        if cursor.rowcount != 1:
            raise QmtEdgeStoreError("QMT Edge acknowledgement references no delivery")

    def outbox_high_watermark(self) -> int:
        row = self._connection.execute(
            "SELECT COALESCE(MAX(delivery_id), 0) FROM outbox"
        ).fetchone()
        assert row is not None
        return int(row[0])

    def bind_event_stream(self, *, scope_digest: str, outbox_epoch: str, account_id: str) -> None:
        """Permanently bind a normalized event database before opening a session."""
        require_sha256(scope_digest, "QMT event scope")
        require_sha256(outbox_epoch, "QMT event epoch")
        require_text(account_id, "QMT event account")
        expected = (scope_digest, outbox_epoch, account_id)
        self._begin()
        try:
            row = self._connection.execute(
                "SELECT scope_digest, outbox_epoch, account_id FROM event_stream_identity WHERE singleton = 1"
            ).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO event_stream_identity VALUES (1, ?, ?, ?)",
                    expected,
                )
            elif tuple(row) != expected:
                raise QmtEdgeStoreError(
                    "QMT event database belongs to another scope, epoch or account"
                )
            self._connection.execute("COMMIT")
        except BaseException:
            self._rollback()
            raise

    def outbox_after(self, delivery_id: int) -> QmtEdgeOutboxRecord | None:
        """Replay one retained fact, including a fact whose ACK reply was lost.

        Do not filter on acknowledged: the Linux durable cursor controls resume.
        No pruning is permitted until a coordinated retention contract exists.
        """
        require_nonnegative(delivery_id, "QMT outbox cursor")
        row = self._connection.execute(
            "SELECT delivery_id, event_id, payload_hash, payload FROM outbox "
            "WHERE delivery_id > ? ORDER BY delivery_id LIMIT 1",
            (delivery_id,),
        ).fetchone()
        return None if row is None else self._outbox_from_row(row)

    def _advance_native_state(
        self, request_id: str, command_sequence: int, state: str
    ) -> QmtEdgeRequest:
        row = self._require_request(request_id)
        if row.command_sequence != command_sequence:
            raise QmtEdgeStoreError("QMT Edge native state sequence belongs to another request")
        ordering = {
            "EDGE_ACCEPTED": 0,
            "PUBLISH_PREPARED": 1,
            "PUBLISH_ATTEMPTED": 2,
            "CALL_STARTED": 3,
            "CALL_RETURNED": 4,
            "UNKNOWN_EFFECT": 5,
        }
        if row.state not in ordering or ordering[row.state] > ordering[state]:
            return row
        self._connection.execute(
            "UPDATE requests SET state = ? WHERE request_id = ?", (state, request_id)
        )
        return QmtEdgeRequest(
            row.request_id, row.command_hash, row.command_sequence, state, row.accepted_monotonic_ns
        )

    def _require_request(self, request_id: str) -> QmtEdgeRequest:
        require_text(request_id, "QMT Edge request_id")
        row = self._connection.execute(
            "SELECT request_id, command_hash, command_sequence, state, accepted_monotonic_ns "
            "FROM requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise QmtEdgeStoreError("QMT Edge request is unknown")
        return self._request_from_row(row)

    def _next_command_sequence(self) -> int:
        row = self._connection.execute(
            "SELECT value FROM metadata WHERE key = 'next_command_sequence'"
        ).fetchone()
        assert row is not None
        sequence = int(row["value"])
        self._connection.execute(
            "UPDATE metadata SET value = ? WHERE key = 'next_command_sequence'", (sequence + 1,)
        )
        return sequence

    @staticmethod
    def _command_hash(command: QmtSubmitCommand) -> str:
        instruction = command.instruction
        mapped = command.mapped_order
        payload = {
            "execution_id": instruction.execution_id,
            "oms_order_id": instruction.oms_order_id,
            "account_id": instruction.account_id,
            "trading_day": instruction.trading_day.value,
            "symbol": instruction.symbol,
            "market": instruction.market,
            "side": instruction.side,
            "price_type": instruction.price_type,
            "business_type": instruction.business_type,
            "quantity": instruction.quantity,
            "limit_price_ticks": instruction.limit_price_ticks,
            "mapped": {
                "manifest_hash": mapped.manifest_hash,
                "market": mapped.market,
                "side": mapped.side,
                "price_type": mapped.price_type,
                "business_type": mapped.business_type,
            },
            "client_order_ref": command.client_order_ref,
            "adapter_instance_id": command.adapter_instance_id,
            "adapter_client_scope": command.adapter_client_scope,
        }
        return sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _request_from_row(row: sqlite3.Row) -> QmtEdgeRequest:
        return QmtEdgeRequest(
            str(row["request_id"]),
            str(row["command_hash"]),
            int(row["command_sequence"]),
            str(row["state"]),
            int(row["accepted_monotonic_ns"]),
        )

    @staticmethod
    def _outbox_from_row(row: sqlite3.Row) -> QmtEdgeOutboxRecord:
        payload = row["payload"]
        if not isinstance(payload, bytes):
            raise QmtEdgeStoreError("QMT Edge outbox payload is not bytes")
        return QmtEdgeOutboxRecord(
            int(row["delivery_id"]), str(row["event_id"]), str(row["payload_hash"]), payload
        )

    def _begin(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

    def _rollback(self) -> None:
        if self._connection.in_transaction:
            self._connection.execute("ROLLBACK")

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO metadata (key, value) VALUES ('next_command_sequence', 1);
            CREATE TABLE IF NOT EXISTS requests (
                request_id TEXT PRIMARY KEY,
                command_hash TEXT NOT NULL,
                command_sequence INTEGER NOT NULL UNIQUE,
                state TEXT NOT NULL,
                accepted_monotonic_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                payload_hash TEXT NOT NULL,
                payload BLOB NOT NULL,
                acknowledged INTEGER NOT NULL CHECK (acknowledged IN (0, 1))
            );
            CREATE TABLE IF NOT EXISTS event_stream_identity (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                scope_digest TEXT NOT NULL,
                outbox_epoch TEXT NOT NULL,
                account_id TEXT NOT NULL
            );
            """
        )

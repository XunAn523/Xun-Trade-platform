"""Encrypted replay payload custody and fail-closed recovery planning.

No recovered command is sent automatically. A `CALL_STARTED` command has an
ambiguous native outcome after a crash and must be reconciled by a controlled
operator or a future authoritative EMT-status lookup implementation.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from cryptography.fernet import Fernet, InvalidToken

from .journal import DurableJournal, JournalRecord


class RecoveryError(RuntimeError):
    """Encrypted replay material is missing, invalid, or unsafe to use."""


class RecoveryDisposition(str, Enum):
    NOT_SENT = "not_sent"
    RECONCILE = "reconcile"


@dataclass(frozen=True)
class RecoverableCommand:
    command_id: str
    correlation_id: str
    payload_hash: str
    disposition: RecoveryDisposition


class EncryptedReplayStore:
    """Fernet-authenticated encrypted custody for JSON command payloads.

    The Fernet key is injected by deployment and is never written to the
    journal, replay directory, trace or generated reports.
    """

    def __init__(self, root: str | Path, *, fernet_key: bytes) -> None:
        self.root = Path(root)
        try:
            self._cipher = Fernet(fernet_key)
        except (TypeError, ValueError) as exc:
            raise RecoveryError("a valid deployment-provided Fernet key is required") from exc
        self.root.mkdir(parents=True, exist_ok=True)

    def preserve(self, *, command_id: str, payload: Mapping[str, Any]) -> str:
        """Encrypt one command payload and return its canonical SHA-256 digest."""

        if not command_id or not isinstance(payload, Mapping):
            raise RecoveryError("command_id and object payload are required")
        try:
            canonical = json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise RecoveryError("replay payload must be JSON-serializable") from exc
        digest = sha256(canonical).hexdigest()
        envelope = json.dumps(
            {"command_id": command_id, "payload_hash": digest, "payload": payload},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self._write_durable(self._path_for(command_id), self._cipher.encrypt(envelope))
        return digest

    def load(self, *, command_id: str, expected_payload_hash: str) -> dict[str, Any]:
        """Decrypt and authenticate a payload only for manual reconciliation."""

        try:
            plaintext = self._cipher.decrypt(self._path_for(command_id).read_bytes())
            envelope = json.loads(plaintext.decode("utf-8"))
        except (OSError, InvalidToken, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RecoveryError(
                "encrypted replay payload is unavailable or failed authentication"
            ) from exc
        if not isinstance(envelope, dict) or envelope.get("command_id") != command_id:
            raise RecoveryError("encrypted replay payload command identity mismatch")
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            raise RecoveryError("encrypted replay payload is not an object")
        canonical = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        digest = sha256(canonical).hexdigest()
        if digest != expected_payload_hash or envelope.get("payload_hash") != digest:
            raise RecoveryError("encrypted replay payload hash mismatch")
        return payload

    def _path_for(self, command_id: str) -> Path:
        return self.root / f"{sha256(command_id.encode('utf-8')).hexdigest()}.fernet"

    def _write_durable(self, destination: Path, contents: bytes) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".pending-", suffix=".fernet", dir=self.root
        )
        try:
            with os.fdopen(descriptor, "wb") as temporary:
                temporary.write(contents)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, destination)
            if os.name != "nt":
                directory_fd = os.open(str(self.root), os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except OSError as exc:
            raise RecoveryError("cannot durably preserve encrypted replay payload") from exc
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


class CommandCustody:
    """Couple encrypted payload preservation to a durable command identity.

    Call :meth:`prepare` before `CALL_STARTED`. The payload is written and
    authenticated before the identity-only journal evidence is returned. The
    recovery API exposes payloads only for an explicit reconciliation workflow;
    it intentionally has no replay/send method.
    """

    def __init__(self, *, journal: DurableJournal, payloads: EncryptedReplayStore) -> None:
        self._journal = journal
        self._payloads = payloads

    def prepare(
        self,
        *,
        command_id: str,
        correlation_id: str,
        payload: Mapping[str, Any],
    ) -> JournalRecord:
        payload_hash = self._payloads.preserve(command_id=command_id, payload=payload)
        return self._journal.append(
            kind="COMMAND_PREPARED",
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )

    def record_state(
        self,
        *,
        kind: str,
        command_id: str,
        correlation_id: str,
        payload_hash: str,
    ) -> JournalRecord:
        if kind not in {"CALL_STARTED", "NATIVE_ACCEPTED", "LOCAL_SEND_REJECTED", "SUBMIT_UNKNOWN"}:
            raise RecoveryError(f"invalid command custody state: {kind}")
        return self._journal.append(
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )

    def pending(self) -> tuple[RecoverableCommand, ...]:
        records = getattr(self._journal, "records", None)
        if not callable(records):
            raise RecoveryError("command recovery requires a journal with readable records")
        return recovery_plan(tuple(records()))

    def load_for_reconciliation(self, command: RecoverableCommand) -> dict[str, Any]:
        if command.disposition is not RecoveryDisposition.RECONCILE:
            raise RecoveryError("only ambiguous commands may load a reconciliation payload")
        return self._payloads.load(
            command_id=command.command_id,
            expected_payload_hash=command.payload_hash,
        )


def recovery_plan(records: tuple[JournalRecord, ...]) -> tuple[RecoverableCommand, ...]:
    """Return only commands that need an explicit post-restart decision.

    `COMMAND_PREPARED` without `CALL_STARTED` was never sent and may be
    rescheduled by higher-level idempotency logic. `CALL_STARTED` or
    `SUBMIT_UNKNOWN` commands are ambiguous and must never be retried blindly.
    """

    command_state_kinds = {
        "COMMAND_PREPARED",
        "CALL_STARTED",
        "NATIVE_ACCEPTED",
        "LOCAL_SEND_REJECTED",
        "SUBMIT_UNKNOWN",
    }
    states: dict[str, JournalRecord] = {}
    for record in records:
        if record.command_id is not None and record.kind in command_state_kinds:
            states[record.command_id] = record
    pending: list[RecoverableCommand] = []
    for command_id, record in states.items():
        if record.kind == "COMMAND_PREPARED":
            pending.append(
                RecoverableCommand(
                    command_id,
                    record.correlation_id,
                    record.payload_hash,
                    RecoveryDisposition.NOT_SENT,
                )
            )
        elif record.kind in {"CALL_STARTED", "SUBMIT_UNKNOWN"}:
            pending.append(
                RecoverableCommand(
                    command_id,
                    record.correlation_id,
                    record.payload_hash,
                    RecoveryDisposition.RECONCILE,
                )
            )
    return tuple(pending)

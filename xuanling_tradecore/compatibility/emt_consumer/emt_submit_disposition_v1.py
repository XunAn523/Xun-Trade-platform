"""File-backed, scope-bound Consumer EMT submit-disposition facts.

The caller owns the exclusive Journal and its lifecycle. Committed mmap
frames are a same-host process-crash boundary, not fsync or recovery proof.
"""

from __future__ import annotations

import re
from hashlib import sha256
from threading import RLock

from ...consumer.execution_runtime_v1 import (
    ConsumerExecutionDispatchRecordV1,
    ConsumerExecutionDispatchStateV1,
)
from ...oms import MmapOmsFactJournalV1, OmsFactKind, OmsJournalCommit, OmsJournalFact


class EmtSubmitDispositionErrorV1(RuntimeError):
    """A worker disposition cannot be committed or safely recovered."""


class MmapEmtSubmitDispositionJournalV1:
    """Persist one payload-free worker result per exact execution and binding."""

    def __init__(
        self,
        *,
        journal: MmapOmsFactJournalV1,
        account_id: str,
        binding_id: str,
        binding_generation: int,
    ) -> None:
        if (
            not account_id
            or not binding_id
            or type(binding_generation) is not int
            or binding_generation < 1
        ):
            raise EmtSubmitDispositionErrorV1("EMT submit disposition scope is invalid")
        self._journal = journal
        self._account_id = account_id
        self._binding_id = binding_id
        self._generation = binding_generation
        self._records: dict[str, tuple[ConsumerExecutionDispatchRecordV1, OmsJournalCommit]] = {}
        self._lock = RLock()
        self._failed = False
        scan = journal.scan()
        if not scan.clean:
            raise EmtSubmitDispositionErrorV1("EMT submit disposition Journal is untrusted")
        for row in scan.records:
            if row.batch_size != 1 or row.ordinal != 0:
                raise EmtSubmitDispositionErrorV1("EMT submit disposition batch is invalid")
            record = self._decode(row.fact)
            if record.execution_id in self._records:
                raise EmtSubmitDispositionErrorV1("EMT submit disposition repeats an execution")
            self._records[record.execution_id] = (
                record,
                OmsJournalCommit(journal.journal_id, row.lsn, row.lsn, 1),
            )

    def record(self, record: ConsumerExecutionDispatchRecordV1) -> None:
        with self._lock:
            self._require_open()
            self._validate(record)
            existing = self._records.get(record.execution_id)
            if existing is not None:
                if self._read_existing(existing) != record:
                    self._failed = True
                    raise EmtSubmitDispositionErrorV1("EMT submit disposition conflicts")
                return
            fact = OmsJournalFact(
                fact_id=f"emt-disposition:{sha256(record.execution_id.encode()).hexdigest()}",
                kind=OmsFactKind.EMT_CONSUMER_SUBMIT_DISPOSITION,
                account_id=self._account_id,
                oms_order_id=None,
                execution_id=record.execution_id,
                attributes={
                    "schema": "1",
                    "command_id": record.command_id,
                    "binding_id": record.binding_id,
                    "binding_generation": str(record.binding_generation),
                    "state": record.state.value,
                    "result_code": record.result_code,
                },
            )
            try:
                commit = self._journal.append_batch((fact,))
            except Exception:
                self._failed = True
                raise
            self._records[record.execution_id] = (record, commit)

    def get(self, execution_id: str) -> ConsumerExecutionDispatchRecordV1 | None:
        with self._lock:
            self._require_open()
            stored = self._records.get(execution_id)
            return None if stored is None else self._read_existing(stored)

    def _read_existing(
        self, stored: tuple[ConsumerExecutionDispatchRecordV1, OmsJournalCommit]
    ) -> ConsumerExecutionDispatchRecordV1:
        try:
            rows = self._journal.read_committed_batch(stored[1])
            decoded = self._decode(rows[0].fact)
            if decoded != stored[0]:
                raise EmtSubmitDispositionErrorV1("EMT submit disposition changed")
            return decoded
        except Exception:
            self._failed = True
            raise

    def _decode(self, fact: OmsJournalFact) -> ConsumerExecutionDispatchRecordV1:
        attrs = dict(fact.attributes)
        if (
            fact.kind is not OmsFactKind.EMT_CONSUMER_SUBMIT_DISPOSITION
            or fact.account_id != self._account_id
            or fact.oms_order_id is not None
            or fact.execution_id is None
            or fact.recovery_payload is not None
            or set(attrs)
            != {"schema", "command_id", "binding_id", "binding_generation", "state", "result_code"}
            or attrs["schema"] != "1"
            or attrs["binding_generation"] != str(self._generation)
        ):
            raise EmtSubmitDispositionErrorV1("EMT submit disposition fact is invalid")
        try:
            record = ConsumerExecutionDispatchRecordV1(
                command_id=attrs["command_id"],
                execution_id=fact.execution_id,
                binding_id=attrs["binding_id"],
                binding_generation=self._generation,
                state=ConsumerExecutionDispatchStateV1(attrs["state"]),
                result_code=attrs["result_code"],
            )
        except ValueError as exc:
            raise EmtSubmitDispositionErrorV1("EMT submit disposition state is invalid") from exc
        self._validate(record)
        if fact.fact_id != f"emt-disposition:{sha256(record.execution_id.encode()).hexdigest()}":
            raise EmtSubmitDispositionErrorV1("EMT submit disposition identity is invalid")
        return record

    def _validate(self, record: ConsumerExecutionDispatchRecordV1) -> None:
        if (
            record.binding_id != self._binding_id
            or record.binding_generation != self._generation
            or record.command_id != record.execution_id
            or record.state
            in {
                ConsumerExecutionDispatchStateV1.QUEUED,
                ConsumerExecutionDispatchStateV1.CANCEL_ACCEPTED,
            }
            or re.fullmatch(r"[A-Z0-9_]{1,128}", record.result_code) is None
        ):
            raise EmtSubmitDispositionErrorV1("EMT submit disposition does not match its scope")

    def _require_open(self) -> None:
        if self._failed or self._journal.recovery_required:
            raise EmtSubmitDispositionErrorV1("EMT submit disposition Journal is closed")

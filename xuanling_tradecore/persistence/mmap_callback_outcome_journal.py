"""Process-crash callback Journal for the file-backed Python profile.

The Journal keeps only fixed identity and hashes in a preallocated mmap file.
It deliberately does not promise a physical flush, host-failure durability, or
automatic recovery; callers must reconcile a restarted process with the
authoritative broker snapshot before reopening admission.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import blake2s, sha256
from time import monotonic_ns
from typing import Callable, Mapping

from ..oms import BrokerOrderObservationV1
from .mmap_journal_contracts import (
    AccountJournalBinding,
    CancelRouteCommand,
    JournalIntegrityError,
    NativeCallbackOutcomeRequest,
    PreNativeRouteCommand,
)
from .mmap_pre_native_journal import (
    MmapCallbackObservedAppend,
    MmapJournalPreparation,
    MmapPreNativeJournal,
)


@dataclass(frozen=True)
class MmapCallbackOutcomeCommit:
    """Payload-free local proof returned after a committed mmap callback frame."""

    command_id: str
    transition_hash: str
    shard_id: str
    shard_sequence: int


class MmapCallbackOutcomeJournal:
    """Append callback and cancel facts without crossing mmap writer ownership.

    Callback observations (including cancel-error callbacks) are emitted by a
    callback Journal worker.  Cancel-start and cancel-accepted facts are
    emitted by the native cancel caller instead.  They must therefore use a
    distinct mapping of mmap files: sharing a file would either violate the
    single-writer crash-fence invariant or require a hot-path lock.
    """

    def __init__(
        self,
        *,
        journals: Mapping[str, MmapPreNativeJournal],
        cancel_command_journals: Mapping[str, MmapPreNativeJournal] | None = None,
        callback_stage_observer: Callable[[str, str, int], None] | None = None,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        if not journals or any(not key for key in journals):
            raise ValueError("mmap callback Journal lane mapping is invalid")
        if cancel_command_journals is not None and (
            not cancel_command_journals or any(not key for key in cancel_command_journals)
        ):
            raise ValueError("mmap cancel-command Journal lane mapping is invalid")
        self._callback_journals = dict(journals)
        self._callback_lane_ids = tuple(sorted(self._callback_journals))
        # The default preserves the single-writer unit composition.  Real
        # concurrent callback/native compositions must provide the separate
        # map explicitly, as the performance driver does.
        self._cancel_command_journals = (
            dict(cancel_command_journals)
            if cancel_command_journals is not None
            else self._callback_journals
        )
        self._cancel_command_lane_ids = tuple(sorted(self._cancel_command_journals))
        self._callback_stage_observer = callback_stage_observer
        self._clock = clock

    @staticmethod
    def _lane_id_for_client_order_ref(client_order_ref: str, *, lane_ids: tuple[str, ...]) -> str:
        if not client_order_ref.isascii() or not client_order_ref:
            raise JournalIntegrityError("mmap callback Journal client reference is invalid")
        digest = blake2s(client_order_ref.encode("ascii"), digest_size=8).digest()
        return lane_ids[int.from_bytes(digest, "big") % len(lane_ids)]

    def lane_id_for_client_order_ref(self, client_order_ref: str) -> str:
        """Return the callback-worker lane for one order reference."""

        return self._lane_id_for_client_order_ref(
            client_order_ref, lane_ids=self._callback_lane_ids
        )

    def cancel_command_lane_id_for_client_order_ref(self, client_order_ref: str) -> str:
        """Return the native-cancel-writer lane for one order reference."""

        return self._lane_id_for_client_order_ref(
            client_order_ref, lane_ids=self._cancel_command_lane_ids
        )

    def record_callback_observed(
        self,
        binding: AccountJournalBinding,
        command: PreNativeRouteCommand,
        *,
        transition_id: str,
        observation: BrokerOrderObservationV1,
        attributes: Mapping[str, str] | None = None,
    ) -> MmapCallbackOutcomeCommit:
        self._validate_order(binding, command, transition_id)
        observation.validate_total_quantity(command.recovery_seed.order.execution.quantity)
        transition_hash = _callback_hash(transition_id, observation, attributes)
        lane_id = self.lane_id_for_client_order_ref(command.client_order_ref)
        self._callback_stage(
            command.client_order_ref,
            "C5B3_AFTER_CALLBACK_MMAP_HASH",
        )
        self._callback_stage(
            command.client_order_ref,
            "C5M0_BEFORE_CALLBACK_MMAP_COMMIT",
        )
        record = self._callback_journals[lane_id].append_callback_observed(
            monotonic_ns=self._clock(),
            account_id=command.account_id,
            execution_id=command.execution_id,
            transition_id=transition_id,
            payload_hash=transition_hash,
            client_order_ref=command.client_order_ref,
            native_order_id=observation.venue_order_id or "",
        )
        self._callback_stage(
            command.client_order_ref,
            "C5M1_AFTER_CALLBACK_MMAP_COMMIT",
        )
        return MmapCallbackOutcomeCommit(
            command.command_id, transition_hash, lane_id, record.sequence
        )

    def record_callback_observed_batch(
        self, requests: tuple[NativeCallbackOutcomeRequest, ...]
    ) -> tuple[MmapCallbackOutcomeCommit, ...]:
        if not 1 <= len(requests) <= 32:
            raise JournalIntegrityError("mmap callback Journal batch length is outside 1..32")
        if len({request.command.client_order_ref for request in requests}) != len(requests):
            raise JournalIntegrityError("mmap callback Journal batch repeats an ordered route")
        prepared_by_lane: dict[
            str,
            list[tuple[int, NativeCallbackOutcomeRequest, str]],
        ] = {}
        for index, request in enumerate(requests):
            self._validate_order(request.binding, request.command, request.transition_id)
            request.observation.validate_total_quantity(
                request.command.recovery_seed.order.execution.quantity
            )
            transition_hash = _callback_hash(
                request.transition_id, request.observation, request.attributes
            )
            lane_id = self.lane_id_for_client_order_ref(request.command.client_order_ref)
            self._callback_stage(
                request.command.client_order_ref,
                "C5B3_AFTER_CALLBACK_MMAP_HASH",
            )
            prepared_by_lane.setdefault(lane_id, []).append((index, request, transition_hash))
        commits: list[MmapCallbackOutcomeCommit | None] = [None] * len(requests)
        for lane_id, entries in prepared_by_lane.items():
            records = self._callback_journals[lane_id].append_callback_observed_batch(
                tuple(
                    MmapCallbackObservedAppend(
                        monotonic_ns=self._clock(),
                        account_id=request.command.account_id,
                        execution_id=request.command.execution_id,
                        transition_id=request.transition_id,
                        payload_hash=transition_hash,
                        client_order_ref=request.command.client_order_ref,
                        native_order_id=request.observation.venue_order_id or "",
                    )
                    for _index, request, transition_hash in entries
                ),
                on_before_commit=lambda record: self._callback_stage(
                    record.client_order_ref,
                    "C5M0_BEFORE_CALLBACK_MMAP_COMMIT",
                ),
                on_committed=lambda record: self._callback_stage(
                    record.client_order_ref,
                    "C5M1_AFTER_CALLBACK_MMAP_COMMIT",
                ),
            )
            if len(records) != len(entries):
                raise JournalIntegrityError(
                    "mmap callback batch did not commit every prepared frame"
                )
            for (index, request, transition_hash), record in zip(entries, records, strict=True):
                commits[index] = MmapCallbackOutcomeCommit(
                    request.command.command_id,
                    transition_hash,
                    lane_id,
                    record.sequence,
                )
        if any(commit is None for commit in commits):
            raise JournalIntegrityError("mmap callback batch commit mapping is incomplete")
        return tuple(commit for commit in commits if commit is not None)

    def record_rejected(
        self,
        binding: AccountJournalBinding,
        command: CancelRouteCommand,
        *,
        transition_id: str,
        error_category: str,
    ) -> MmapCallbackOutcomeCommit:
        if (
            binding.account_id != command.account_id
            or not transition_id.isascii()
            or not error_category
        ):
            raise JournalIntegrityError("mmap cancel outcome does not match its route")
        lane_id = self.lane_id_for_client_order_ref(command.target_client_order_ref)
        transition_hash = sha256(f"{transition_id}\0{error_category}".encode("ascii")).hexdigest()
        record = self._callback_journals[lane_id].append_cancel_rejected(
            monotonic_ns=self._clock(),
            account_id=command.account_id,
            execution_id=command.execution_id,
            transition_id=transition_id,
            payload_hash=transition_hash,
            client_order_ref=command.target_client_order_ref,
            native_order_id="",
        )
        return MmapCallbackOutcomeCommit(
            command.cancel_request_id, transition_hash, lane_id, record.sequence
        )

    def append_cancel_started(
        self, binding: AccountJournalBinding, command: CancelRouteCommand, *, native_order_id: str
    ) -> MmapCallbackOutcomeCommit:
        if binding.account_id != command.account_id:
            raise JournalIntegrityError("mmap cancel start does not match its route")
        lane_id = self.cancel_command_lane_id_for_client_order_ref(command.target_client_order_ref)
        record = self._cancel_command_journals[lane_id].append_cancel_started(
            monotonic_ns=self._clock(),
            account_id=command.account_id,
            execution_id=command.execution_id,
            cancel_request_id=command.cancel_request_id,
            payload_hash=command.payload_hash,
            client_order_ref=command.target_client_order_ref,
            native_order_id=native_order_id,
        )
        return MmapCallbackOutcomeCommit(
            command.cancel_request_id, command.payload_hash, lane_id, record.sequence
        )

    def append_cancel_native_accepted(
        self, binding: AccountJournalBinding, command: CancelRouteCommand, *, native_order_id: str
    ) -> MmapCallbackOutcomeCommit:
        if binding.account_id != command.account_id:
            raise JournalIntegrityError("mmap cancel receipt does not match its route")
        lane_id = self.cancel_command_lane_id_for_client_order_ref(command.target_client_order_ref)
        receipt_hash = sha256(
            (command.payload_hash + "\0" + native_order_id).encode("ascii")
        ).hexdigest()
        record = self._cancel_command_journals[lane_id].append_cancel_native_accepted(
            monotonic_ns=self._clock(),
            account_id=command.account_id,
            execution_id=command.execution_id,
            cancel_request_id=command.cancel_request_id,
            payload_hash=receipt_hash,
            client_order_ref=command.target_client_order_ref,
            native_order_id=native_order_id,
        )
        return MmapCallbackOutcomeCommit(
            command.cancel_request_id, receipt_hash, lane_id, record.sequence
        )

    def close(self) -> None:
        for journal in {
            id(journal): journal
            for journal in (
                *self._callback_journals.values(),
                *self._cancel_command_journals.values(),
            )
        }.values():
            journal.close()

    def prepare_for_measurement(
        self,
        *,
        account_id: str,
        execution_ids: Iterable[str] = (),
        command_ids: Iterable[str] = (),
        client_order_refs: Iterable[str] = (),
    ) -> tuple[MmapJournalPreparation, ...]:
        """Pre-touch callback and cancel-command lanes before C0.

        Both writer sets are included even though callback and native-cancel
        callers own different files.  The returned records are payload-free
        setup evidence for the performance report.
        """

        execution_ids = tuple(execution_ids)
        command_ids = tuple(command_ids)
        client_order_refs = tuple(client_order_refs)
        return tuple(
            journal.prepare_for_measurement(
                account_id=account_id,
                execution_ids=execution_ids,
                command_ids=command_ids,
                client_order_refs=client_order_refs,
            )
            for journal in {
                id(journal): journal
                for journal in (
                    *self._callback_journals.values(),
                    *self._cancel_command_journals.values(),
                )
            }.values()
        )

    def _callback_stage(self, client_order_ref: str, stage: str) -> None:
        observer = self._callback_stage_observer
        if observer is None:
            return
        try:
            observer(client_order_ref, stage, self._clock())
        except BaseException:
            # Observation must never change the durable callback proof.
            return

    @staticmethod
    def _validate_order(
        binding: AccountJournalBinding, command: PreNativeRouteCommand, transition_id: str
    ) -> None:
        if (
            binding.account_id != command.account_id
            or not transition_id
            or len(transition_id) > 128
            or not transition_id.isascii()
        ):
            raise JournalIntegrityError("mmap callback outcome does not match its route")


def _callback_hash(
    transition_id: str, observation: BrokerOrderObservationV1, attributes: Mapping[str, str] | None
) -> str:
    raw = "\0".join(
        (
            transition_id,
            observation.broker_event_id,
            observation.source_key,
            observation.order_state.value,
            str(observation.cumulative_filled_quantity),
            str(observation.leaves_quantity),
            observation.venue_order_id or "",
            *observation.trade_keys,
            *(f"{key}={value}" for key, value in sorted((attributes or {}).items())),
        )
    )
    if not raw.isascii():
        raise JournalIntegrityError("mmap callback outcome fields are not ASCII")
    return sha256(raw.encode("ascii")).hexdigest()

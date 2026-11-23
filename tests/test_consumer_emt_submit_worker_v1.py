from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from emt_gateway.fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeObserver,
)
from emt_gateway.fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from emt_gateway.tradecore_mmap_journal import EmtMmapNativeCallJournal
from emt_gateway.tradecore_mutation import EmtPlatformMutationPort
from xuanling_tradecore.adapters import SubmitAccepted
from xuanling_tradecore.adapters.emt import MmapRouteLedgerV2
from xuanling_tradecore.adapters.emt.mapping import load_builtin_emt_legacy_sim_manifest
from xuanling_tradecore.adapters.emt.mutation import EmtSubmitRequest
from xuanling_tradecore.adapters.mapping import MappedVenueOrder
from xuanling_tradecore.consumer.base import ConsumerGatewayError
from xuanling_tradecore.consumer.emt_submit_composition_v1 import (
    ManagedEmtConsumerSubmitV1,
    build_consumer_emt_submit_operations_v1,
)
from xuanling_tradecore.consumer.emt_submit_disposition_v1 import MmapEmtSubmitDispositionJournalV1
from xuanling_tradecore.consumer.emt_submit_worker_v1 import (
    EmtConsumerSubmitWorkerErrorV1,
    EmtConsumerSubmitWorkerV1,
)
from xuanling_tradecore.consumer.execution_runtime_v1 import ConsumerExecutionDispatchStateV1
from xuanling_tradecore.domain import VenueOrderIdentity
from xuanling_tradecore.ems import ExecutionDispatchOutcome
from xuanling_tradecore.oms import MmapOmsFactJournalV1
from xuanling_tradecore.persistence import MmapCallbackOutcomeJournal, MmapPreNativeJournal
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapJournalRecordKind

from test_consumer_emt_submit_route_preparer_v1 import _composition

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux namespace Journal")


class _Lane:
    def __init__(self, resolver):
        self.route_resolver = resolver
        self.entered = False

    @contextmanager
    def mmap_native_lane(self, name):
        assert name == "lane-1" and not self.entered
        self.entered = True
        try:
            yield
        finally:
            self.entered = False


class _Runtime:
    def __init__(self, binding, resolver, lane):
        self.binding = binding
        self.resolver = resolver
        self.lane = lane
        self.calls = []
        self.closed = []
        self.instruction = None

    def admit_submit(self, instruction, **_kwargs):
        self.instruction = instruction
        return SimpleNamespace(decision=SimpleNamespace(accepted=True), authorized_result=object())

    def dispatch_once(self, **_kwargs):
        assert self.lane.entered and self.instruction is not None
        instruction = self.instruction
        ref = self.resolver.client_order_ref_for(instruction)
        self.calls.append(instruction.execution_id)
        identity = VenueOrderIdentity("EMT", "adapter-tradecore-a", "101", ref, "venue-1")
        return SimpleNamespace(
            scheduled=SimpleNamespace(command=instruction),
            outcome=ExecutionDispatchOutcome.SUBMIT_ACCEPTED,
            submit_accepted=SubmitAccepted(
                execution_id=instruction.execution_id,
                adapter_id="emt-adapter",
                adapter_instance_id="adapter-tradecore-a",
                accepted_monotonic_ns=22,
                venue_order_identity=identity,
            ),
        )

    def close_admission(self, *, reason):
        self.closed.append(reason)


def _worker(preparer, resolver, *, sink, max_pending=2):
    lane = _Lane(resolver)
    runtime = _Runtime(preparer.execution_binding, resolver, lane)
    worker = EmtConsumerSubmitWorkerV1(
        preparer=preparer,
        runtime=runtime,
        native_journal=lane,
        lane_id="lane-1",
        market_for_execution=lambda _: SimpleNamespace(),
        kill_switches_for_account=lambda _: SimpleNamespace(),
        serialized_size_for_execution=lambda _: 64,
        record_disposition=sink,
        now_monotonic_ns=lambda: 22,
        max_pending=max_pending,
        max_seen=4,
    )
    return worker, runtime


def test_post_commit_wakeup_is_not_inline_and_single_owner_dispatches(tmp_path):
    journal, refs, preparer, resolver, (order, commit), errors = _composition(tmp_path)
    try:
        dispositions = []
        worker, runtime = _worker(preparer, resolver, sink=dispositions.append)
        worker.on_execution_committed(order, commit)
        worker.on_execution_committed(order, commit)
        assert runtime.calls == []
        assert refs.read_recovery(preparer._allocator.scope).leases == ()
        result = worker.run_one()
        assert (
            result is not None and result.state is ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED
        )
        assert runtime.calls == [order.execution.execution_id]
        assert dispositions == [result]
        assert worker.run_one() is None
        assert (
            refs.read_committed_lease(preparer._allocator.scope, order.execution.execution_id)
            is not None
        )
        assert runtime.closed == [] and errors == []
    finally:
        refs.close()
        journal.close()


def test_worker_commits_disposition_to_file_backed_sink(tmp_path):
    journal, refs, preparer, resolver, (order, commit), _errors = _composition(tmp_path)
    dispositions = MmapOmsFactJournalV1(
        path=tmp_path / "dispositions.mmap", journal_id="dispositions", capacity=8
    )
    try:
        sink = MmapEmtSubmitDispositionJournalV1(
            journal=dispositions,
            account_id="sim-account",
            binding_id=preparer.consumer_binding.binding_id,
            binding_generation=1,
        )
        worker, runtime = _worker(preparer, resolver, sink=sink.record)
        worker.on_execution_committed(order, commit)
        result = worker.run_one()
        assert result is not None and sink.get(order.execution.execution_id) == result
        assert runtime.calls == [order.execution.execution_id]
        assert len(dispositions.scan().records) == 1
    finally:
        dispositions.close()
        refs.close()
        journal.close()


def test_consumer_oms_wakeup_reaches_managed_ems_owner_with_fake_native(tmp_path):
    (
        journal,
        refs,
        preparer,
        resolver,
        _first,
        _errors,
        original_ops,
        fixture,
        context,
    ) = _composition(tmp_path, include_operations=True)
    dispositions = MmapOmsFactJournalV1(
        path=tmp_path / "dispositions.mmap", journal_id="dispositions", capacity=8
    )
    managed = None
    try:
        sink = MmapEmtSubmitDispositionJournalV1(
            journal=dispositions,
            account_id="sim-account",
            binding_id=preparer.consumer_binding.binding_id,
            binding_generation=1,
        )
        worker, runtime = _worker(preparer, resolver, sink=sink.record)
        managed = ManagedEmtConsumerSubmitV1(worker)
        operations = build_consumer_emt_submit_operations_v1(
            order_book=original_ops._order_book,
            managed_worker=managed,
            admission_context_for=lambda *_: context,
            account_ready=lambda _: True,
            now_monotonic_ns=lambda: 20,
        )
        managed.start()
        signal = replace(
            fixture._signal(), trading_day=_first[0].order.trading_day, strategy_order_id="second"
        )
        receipt = operations.submit_signal(
            session=fixture.session,
            account_binding=fixture.account,
            strategy_binding_id="strategy-bind",
            signal=signal,
        )
        deadline = time.monotonic() + 3
        while sink.get(receipt.execution_id) is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert runtime.calls == [receipt.execution_id]
        assert (
            sink.get(receipt.execution_id).state is ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED
        )
        assert not managed.failed
        with pytest.raises(ConsumerGatewayError, match="identity"):
            operations.cancel_order(
                session=fixture.session,
                account_binding=fixture.account,
                consumer_request_id="cancel",
                oms_order_id=receipt.oms_order_id,
                reason="test",
            )
    finally:
        if managed is not None:
            managed.close()
        dispositions.close()
        refs.close()
        journal.close()


def test_disarmed_or_unstarted_managed_owner_rejects_before_oms_commit(tmp_path):
    (
        journal,
        refs,
        preparer,
        resolver,
        first,
        _errors,
        original_ops,
        fixture,
        context,
    ) = _composition(tmp_path, authorized=False, include_operations=True)
    worker, runtime = _worker(preparer, resolver, sink=lambda _: None)
    managed = ManagedEmtConsumerSubmitV1(worker)
    try:
        operations = build_consumer_emt_submit_operations_v1(
            order_book=original_ops._order_book,
            managed_worker=managed,
            admission_context_for=lambda *_: context,
            account_ready=lambda _: True,
            now_monotonic_ns=lambda: 20,
        )
        before = journal.scan().records
        signal = replace(
            fixture._signal(), trading_day=first[0].order.trading_day, strategy_order_id="blocked"
        )
        for start in (False, True):
            if start:
                managed.start()
            with pytest.raises(ConsumerGatewayError, match="admission is closed"):
                operations.submit_signal(
                    session=fixture.session,
                    account_binding=fixture.account,
                    strategy_binding_id="strategy-bind",
                    signal=signal,
                )
            assert journal.scan().records == before
            assert runtime.calls == []
        assert refs.read_recovery(preparer._allocator.scope).leases == ()
    finally:
        managed.close()
        refs.close()
        journal.close()


def test_unready_account_rejects_even_when_worker_gate_is_open(tmp_path):
    (
        journal,
        refs,
        preparer,
        resolver,
        first,
        _errors,
        original_ops,
        fixture,
        context,
    ) = _composition(tmp_path, include_operations=True)
    worker, runtime = _worker(preparer, resolver, sink=lambda _: None)
    managed = ManagedEmtConsumerSubmitV1(worker)
    try:
        operations = build_consumer_emt_submit_operations_v1(
            order_book=original_ops._order_book,
            managed_worker=managed,
            admission_context_for=lambda *_: context,
            account_ready=lambda _: False,
            now_monotonic_ns=lambda: 20,
        )
        before = journal.scan().records
        managed.start()
        assert managed.admission_permitted()
        signal = replace(
            fixture._signal(), trading_day=first[0].order.trading_day, strategy_order_id="blocked"
        )
        with pytest.raises(ConsumerGatewayError, match="admission is closed"):
            operations.submit_signal(
                session=fixture.session,
                account_binding=fixture.account,
                strategy_binding_id="strategy-bind",
                signal=signal,
            )
        assert journal.scan().records == before
        assert runtime.calls == []
    finally:
        managed.close()
        refs.close()
        journal.close()


def test_disposition_failure_after_fake_native_call_fences_and_never_retries(tmp_path):
    journal, refs, preparer, resolver, (order, commit), _errors = _composition(tmp_path)
    try:

        def failing_sink(_record):
            raise OSError("injected disposition commit failure")

        worker, runtime = _worker(preparer, resolver, sink=failing_sink)
        worker.on_execution_committed(order, commit)
        with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
            worker.run_one()
        assert runtime.calls == [order.execution.execution_id]
        assert runtime.closed == ["consumer-emt-submit-dispatch-failed"]
        with pytest.raises(EmtConsumerSubmitWorkerErrorV1, match="latched"):
            worker.run_one()
        worker.on_execution_wakeup_failed()  # idempotent, already fenced
        assert runtime.calls == [order.execution.execution_id]
    finally:
        refs.close()
        journal.close()


@pytest.mark.parametrize("bad_identity", ["client-ref", "missing-native-id"])
def test_unproven_native_acceptance_records_unknown_and_fences(tmp_path, bad_identity):
    journal, refs, preparer, resolver, (order, commit), _errors = _composition(tmp_path)
    try:
        dispositions = []
        worker, runtime = _worker(preparer, resolver, sink=dispositions.append)
        dispatch = runtime.dispatch_once

        def wrong_identity(**kwargs):
            result = dispatch(**kwargs)
            identity = result.submit_accepted.venue_order_identity
            if bad_identity == "client-ref":
                identity = replace(identity, client_order_ref="999")
            else:
                identity = replace(identity, venue_order_id=None)
            result.submit_accepted = replace(result.submit_accepted, venue_order_identity=identity)
            return result

        runtime.dispatch_once = wrong_identity
        worker.on_execution_committed(order, commit)
        with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
            worker.run_one()
        assert runtime.calls == [order.execution.execution_id]
        assert len(dispositions) == 1
        assert (
            dispositions[0].state
            is ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION
        )
        assert dispositions[0].result_code == "NATIVE_ACCEPTANCE_IDENTITY_MISMATCH"
        assert runtime.closed == ["consumer-emt-submit-dispatch-failed"]
        with pytest.raises(EmtConsumerSubmitWorkerErrorV1, match="latched"):
            worker.run_one()
    finally:
        refs.close()
        journal.close()


def test_full_bounded_wakeup_queue_fences_before_any_native_call(tmp_path):
    journal, refs, preparer, resolver, (order, commit), _errors = _composition(tmp_path)
    try:
        worker, runtime = _worker(preparer, resolver, sink=lambda _: None, max_pending=1)
        worker.on_execution_committed(order, commit)
        other = replace(order, execution=replace(order.execution, execution_id="other-execution"))
        with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
            worker.on_execution_committed(other, commit)
        assert runtime.calls == []
        assert runtime.closed == ["consumer-emt-submit-wakeup-failed"]
        assert refs.read_recovery(preparer._allocator.scope).leases == ()
    finally:
        refs.close()
        journal.close()


@pytest.mark.parametrize("failure", ["none", "call-started", "expired"])
def test_real_pre_native_journals_commit_before_fake_emt_call(tmp_path, monkeypatch, failure):
    journal, refs, preparer, resolver, (order, commit), _errors = _composition(tmp_path)
    native = MmapPreNativeJournal(path=tmp_path / "native.mmap", capacity=8)
    callbacks = MmapCallbackOutcomeJournal(
        journals={"callback": MmapPreNativeJournal(path=tmp_path / "callbacks.mmap", capacity=8)},
        cancel_command_journals={
            "cancel": MmapPreNativeJournal(path=tmp_path / "cancel.mmap", capacity=8)
        },
    )
    routes = MmapRouteLedgerV2(
        path=tmp_path / "routes.mmap", capacity=8, directory=preparer._directory
    )
    outcome = FastSpiV2RouteOutcomeDirectory(
        config=FastSpiV2RouteOutcomeDirectoryConfig("sim-account", "adapter-tradecore-a", "101", 8)
    )
    cancel = FastSpiV2CancelOutcomeObserver(
        config=FastSpiV2CancelOutcomeDirectoryConfig(
            "sim-account", "adapter-tradecore-a", "101", 8
        ),
        journal=callbacks,
    )
    real_lane = EmtMmapNativeCallJournal(
        route_resolver=resolver,
        order_outcome_directory=outcome,
        cancel_outcome_observer=cancel,
        mmap_pre_native_journals={"lane-1": native},
        mmap_callback_outcomes=callbacks,
        mmap_route_ledger=routes,
        clock=lambda: 1020 if failure == "expired" else 22,
    )

    class FakeApi:
        def insertOrder(self, request, session_id):
            assert session_id == 71
            ref = request.order_client_id
            instruction = request.instruction
            scan = native.scan_recovery_prefix()
            assert scan.clean
            assert [item.kind for item in scan.records] == [
                MmapJournalRecordKind.ORDER_PRE_NATIVE,
                MmapJournalRecordKind.ORDER_CALL_STARTED,
            ]
            route_fact = routes.load_route_ledger().route_for_client_id(ref)
            assert route_fact is not None and route_fact.execution_id == instruction.execution_id
            runtime.calls.append(instruction.execution_id)
            return 5001

    class RuntimeWithJournal(_Runtime):
        def dispatch_once(self, **_kwargs):
            instruction = self.instruction
            ref = int(self.resolver.client_order_ref_for(instruction))
            manifest = load_builtin_emt_legacy_sim_manifest()
            mapped = MappedVenueOrder(
                instruction.execution_id,
                instruction.oms_order_id,
                instruction.account_id,
                manifest.manifest_id,
                manifest.manifest_version,
                manifest.manifest_hash,
                1,
                1,
                1,
                0,
                manifest.identity,
            )
            request = EmtSubmitRequest(instruction, mapped, ref)
            port = EmtPlatformMutationPort(
                FakeApi(),
                native_session_id=71,
                order_request_factory=lambda value: value,
                native_call_journal=real_lane,
                clock=lambda: 23,
            )
            port.submit(request)
            identity = VenueOrderIdentity("EMT", "adapter-tradecore-a", "101", str(ref), "5001")
            return SimpleNamespace(
                scheduled=SimpleNamespace(command=instruction),
                outcome=ExecutionDispatchOutcome.SUBMIT_ACCEPTED,
                submit_accepted=SubmitAccepted(
                    instruction.execution_id,
                    "emt-adapter",
                    "adapter-tradecore-a",
                    23,
                    identity,
                ),
            )

    try:
        runtime = RuntimeWithJournal(preparer.execution_binding, resolver, None)
        recorded = []
        worker = EmtConsumerSubmitWorkerV1(
            preparer=preparer,
            runtime=runtime,
            native_journal=real_lane,
            lane_id="lane-1",
            market_for_execution=lambda _: SimpleNamespace(),
            kill_switches_for_account=lambda _: SimpleNamespace(),
            serialized_size_for_execution=lambda _: 64,
            record_disposition=recorded.append,
            now_monotonic_ns=lambda: 22,
            max_pending=2,
            max_seen=4,
        )
        worker.on_execution_committed(order, commit)
        assert runtime.calls == [] and native.scan_recovery_prefix().records == ()
        if failure == "call-started":

            def fail_append(**_kwargs):
                raise OSError("injected call-started journal failure")

            monkeypatch.setattr(native, "append_call_started", fail_append)
            with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
                worker.run_one()
            assert runtime.calls == []
            assert [item.kind for item in native.scan_recovery_prefix().records] == [
                MmapJournalRecordKind.ORDER_PRE_NATIVE,
            ]
            assert runtime.closed
            return
        if failure == "expired":
            with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
                worker.run_one()
            assert runtime.calls == [] and native.scan_recovery_prefix().records == ()
            assert runtime.closed
            return
        result = worker.run_one()
        assert (
            result is not None and result.state is ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED
        )
        assert runtime.calls == [order.execution.execution_id]
        assert [item.kind for item in native.scan_recovery_prefix().records] == [
            MmapJournalRecordKind.ORDER_PRE_NATIVE,
            MmapJournalRecordKind.ORDER_CALL_STARTED,
            MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
        ]
    finally:
        real_lane.close()
        refs.close()
        journal.close()

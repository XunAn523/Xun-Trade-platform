from __future__ import annotations

import unittest

from emt_gateway.gateway import CallbackPipeline
from emt_gateway.journal import InMemoryJournal
from emt_gateway.query_registry import QueryCompletion, QueryRegistry, QueryRegistryError


class TraceSpy:
    def __init__(self) -> None:
        self.traces: dict[str, dict[str, object]] = {}

    def begin(self, correlation_id: str, **identity: object) -> None:
        self.traces[correlation_id] = {"stages": ["C0"], "outcome": None}

    def mark(self, correlation_id: str, stage: str, timestamp_ns: int | None = None) -> None:
        self.traces[correlation_id]["stages"].append(stage)  # type: ignore[index]  # heterogeneous test trace

    def complete(self, correlation_id: str, outcome: str = "ok", reason: str | None = None) -> None:
        self.traces[correlation_id]["outcome"] = outcome
        self.traces[correlation_id]["reason"] = reason

    def record_metric(self, metric: str, value: int, timestamp_ns: int | None = None) -> None:
        return None


class QueryRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.trace = TraceSpy()
        self.registry = QueryRegistry(trace=self.trace, cooldown_ns=1_000)
        self.pipeline = CallbackPipeline(
            journal=InMemoryJournal(),
            trace=self.trace,
            lookup_source=lambda record: None,
            capacity=4,
            lifecycle_observer=self.registry,
        )

    def test_terminal_query_joins_active_and_callback_c0_to_c7_paths(self) -> None:
        handle = self.registry.open(
            api_name="QueryAsset",
            callback_name="OnQueryAsset",
            correlation_id="query-correlation-1",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.LAST,
        )
        self.registry.mark_native_started(handle.request_id)
        self.registry.mark_native_returned(handle.request_id, 0)
        self.pipeline.receive(
            "OnQueryAsset",
            profile="ordinary",
            workload="steady",
            request_id=handle.request_id,
            is_last=True,
        )
        delivery = self.pipeline.normalize_one()
        self.assertIsNotNone(delivery)
        self.pipeline.ack(delivery.delivery_id)  # type: ignore[union-attr]  # assertion fixture guarantees delivery

        self.assertEqual(
            ["C0", "C1", "C3", "C4", "C5", "C6", "C7"],
            self.trace.traces[handle.correlation_id]["stages"],
        )
        self.assertEqual("ok", self.trace.traces[handle.correlation_id]["outcome"])
        self.assertEqual(0, self.registry.active_count)

    def test_multicast_query_waits_for_last_callback_before_c6_and_ack(self) -> None:
        handle = self.registry.open(
            api_name="QueryOrders",
            callback_name="OnQueryOrder",
            correlation_id="query-correlation-2",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.LAST,
        )
        self.registry.mark_native_started(handle.request_id)
        self.registry.mark_native_returned(handle.request_id, 0)
        self.pipeline.receive(
            "OnQueryOrder",
            profile="ordinary",
            workload="steady",
            request_id=handle.request_id,
            is_last=False,
        )
        first = self.pipeline.normalize_one()
        self.pipeline.ack(first.delivery_id)  # type: ignore[union-attr]  # assertion fixture guarantees delivery
        self.assertEqual(
            ["C0", "C1", "C3", "C4", "C5"], self.trace.traces[handle.correlation_id]["stages"]
        )
        self.assertEqual(1, self.registry.active_count)

        self.pipeline.receive(
            "OnQueryOrder",
            profile="ordinary",
            workload="steady",
            request_id=handle.request_id,
            is_last=True,
        )
        terminal = self.pipeline.normalize_one()
        self.pipeline.ack(terminal.delivery_id)  # type: ignore[union-attr]  # assertion fixture guarantees delivery
        self.assertEqual("ok", self.trace.traces[handle.correlation_id]["outcome"])
        self.assertEqual(0, self.registry.active_count)

    def test_completed_query_exposes_only_payload_free_semantic_facts(self) -> None:
        handle = self.registry.open(
            api_name="QueryOrdersByPage",
            callback_name="OnQueryOrderByPage",
            correlation_id="query-semantic-observation",
            profile="ordinary",
            workload="pagination",
            completion=QueryCompletion.LAST,
        )
        self.registry.mark_native_started(handle.request_id)
        self.registry.mark_native_returned(handle.request_id, 0)
        self.pipeline.receive(
            "OnQueryOrderByPage",
            profile="ordinary",
            workload="pagination",
            request_id=handle.request_id,
            is_last=True,
            query_item_present=True,
            query_reference=7,
        )
        delivery = self.pipeline.normalize_one()
        self.pipeline.ack(delivery.delivery_id)  # type: ignore[union-attr]  # assertion fixture guarantees delivery

        observation = self.registry.semantic_observation(handle.correlation_id)
        self.assertIsNotNone(observation)
        self.assertEqual(1, observation.callback_count)  # type: ignore[union-attr]  # observation asserted present above
        self.assertEqual(1, observation.item_count)  # type: ignore[union-attr]  # observation asserted present above
        self.assertTrue(observation.terminal_seen)  # type: ignore[union-attr]  # observation asserted present above
        self.assertEqual("true", observation.terminal_flag_states)  # type: ignore[union-attr]  # observation asserted present above
        self.assertEqual(7, observation.query_reference)  # type: ignore[union-attr]  # observation asserted present above

    def test_request_id_cools_before_reuse_and_nonzero_return_completes_rejected(self) -> None:
        registry = QueryRegistry(
            trace=self.trace,
            cooldown_ns=10**15,
            minimum_request_id=1,
            maximum_request_id=1,
        )
        first = registry.open(
            api_name="QueryFund",
            callback_name="OnQueryFund",
            correlation_id="query-correlation-3",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.SINGLE,
        )
        registry.mark_native_started(first.request_id)
        registry.mark_native_returned(first.request_id, 9)
        self.assertEqual("rejected", self.trace.traces[first.correlation_id]["outcome"])
        self.assertEqual("native_return_9", self.trace.traces[first.correlation_id]["reason"])
        with self.assertRaises(QueryRegistryError):
            registry.open(
                api_name="QueryFund",
                callback_name="OnQueryFund",
                correlation_id="query-correlation-4",
                profile="ordinary",
                workload="steady",
                completion=QueryCompletion.SINGLE,
            )
        registry.expire(now_ns=10**18)
        second = registry.open(
            api_name="QueryFund",
            callback_name="OnQueryFund",
            correlation_id="query-correlation-4",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.SINGLE,
        )
        self.assertEqual(1, second.request_id)

    def test_timeout_retires_only_expired_requests_unless_session_is_forced_closed(self) -> None:
        handle = self.registry.open(
            api_name="QueryPosition",
            callback_name="OnQueryPosition",
            correlation_id="query-correlation-timeout",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.LAST,
            timeout_ns=1,
        )
        self.assertEqual((), self.registry.expire(now_ns=0))
        self.assertEqual((handle.request_id,), self.registry.expire(now_ns=10**18))
        self.assertEqual("timeout", self.trace.traces[handle.correlation_id]["outcome"])

    def test_legacy_callback_without_request_id_still_requires_a_terminal_marker(self) -> None:
        handle = self.registry.open(
            api_name="QueryYesterdayAsset",
            callback_name="OnQueryYesterdayAsset",
            correlation_id="query-yesterday-asset",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.LAST,
        )
        self.registry.mark_native_started(handle.request_id)
        self.registry.mark_native_returned(handle.request_id, 0)
        self.pipeline.receive(
            "OnQueryYesterdayAsset",
            profile="ordinary",
            workload="steady",
            is_last=None,
        )
        delivery = self.pipeline.normalize_one()
        self.pipeline.ack(delivery.delivery_id)  # type: ignore[union-attr]  # assertion fixture guarantees delivery
        self.assertIsNone(self.trace.traces[handle.correlation_id]["outcome"])
        self.registry.expire(now_ns=10**18)
        self.assertEqual("timeout", self.trace.traces[handle.correlation_id]["outcome"])

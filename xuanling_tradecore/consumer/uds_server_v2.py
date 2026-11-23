"""Production-shaped, no-Adapter bootstrap for the server-local Consumer V2 UDS.

This bootstrap is intentionally narrow.  It creates a real Linux UDS listener,
authenticates its peer with ``SO_PEERCRED``, commits accepted OMS facts to a
file-backed mmap journal, and returns an intent receipt.  It does *not* import
or start EMS, EMT, QMT, credentials, or any broker SDK.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import signal
import stat
import uuid
from bisect import bisect_right
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Lock
from time import monotonic_ns, time_ns
from typing import Mapping, cast

from ..domain import (
    AccountLifecycle,
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerEventCursorV2,
    ConsumerEventKind,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    ConsumerPrincipal,
    ConsumerStrategyBinding,
    DecisionContext,
    ReconciliationStatus,
    RedisStreamIdV1,
    RiskSnapshot,
)
from ..oms import MmapOmsFactJournalV1, OmsOrderEvent, OrderBook, ReservationRequirement
from .base import ConsumerAccountOrderSnapshotV2, ConsumerAccountView, PositionedConsumerEventV2
from .bindings_v2 import ConsumerBindingSnapshotV2
from .cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from .dispatch_v2 import ConsumerWireDispatcherV2
from .gateway_v2 import ConsumerGatewayV2
from .oms_operations_v2 import ConsumerOmsAdmissionContextV2, OrderBookConsumerOperationsV2
from .read_worker_v2 import ConsumerSnapshotReadWorkerV2
from .replay_v2 import (
    ConsumerCursorReplayServiceV2,
    ConsumerReplayPartitionBoundsV2,
    ConsumerReplaySliceV2,
)
from .request_trace_v2 import (
    BoundedRequestTraceWriterV2,
    mark_request_stage,
    mark_request_value,
)
from .runtime_profile import (
    ConsumerLongRunningPolicy,
    ConsumerRuntimePolicyError,
    ConsumerRuntimeProfile,
    require_long_running_capabilities,
)
from .service_v2 import ConsumerUnixServiceV2
from .snapshot_cursor_v2 import SnapshotCursorCodecV2, SnapshotCursorSigningKeyV2
from .snapshot_index_v2 import ConsumerSnapshotIndexV2, ConsumerSnapshotVersionV2
from .snapshot_v2 import ConsumerSnapshotCapacityErrorV2, ConsumerSnapshotProjectionV2
from .stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from .unix_v2 import ConsumerUnixPeerV2, ConsumerUnixSocketError, ConsumerUnixSocketListenerV2
from .wire_v2 import load_contract_identity


class ConsumerUdsServerConfigErrorV2(ValueError):
    """The local, non-secret UDS bootstrap profile is invalid or mutable."""


@dataclass(frozen=True)
class ConsumerUdsServerConfigV2:
    """Immutable server-local binding for one no-Adapter benchmark principal."""

    socket_path: Path
    journal_root: Path
    journal_capacity: int
    peer_uid: int
    peer_gid: int
    consumer_id: str
    login_name: str
    legal_name: str
    consumer_account_id: str
    account_id: str
    account_display_name: str
    venue_id: str
    adapter_instance_id: str
    strategy_id: str
    listen_backlog: int = 8_192
    pending_capacity: int = 2_048
    preframe_capacity: int = 8_192
    max_requests_per_connection: int = 1_024
    additional_peer_credentials: tuple[tuple[int, int], ...] = ()
    socket_mode: int = 0o600
    runtime_profile: ConsumerRuntimeProfile = ConsumerRuntimeProfile.BENCHMARK
    long_running_policy: ConsumerLongRunningPolicy | None = None


def load_consumer_uds_server_config_v2(path: Path) -> ConsumerUdsServerConfigV2:
    """Load a root-owned, non-secret profile without accepting unknown fields."""

    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config must be a regular file")
    if metadata.st_uid != 0 or metadata.st_mode & 0o022:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config ownership is unsafe")
    try:
        parsed: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config is unreadable") from exc
    if not isinstance(parsed, dict):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config must be an object")
    raw = cast(dict[str, object], parsed)
    required = {
        "schema_version",
        "socket_path",
        "journal_root",
        "journal_capacity",
        "peer_uid",
        "peer_gid",
        "consumer_id",
        "login_name",
        "legal_name",
        "consumer_account_id",
        "account_id",
        "account_display_name",
        "venue_id",
        "adapter_instance_id",
        "strategy_id",
    }
    allowed = required | {
        "listen_backlog",
        "pending_capacity",
        "preframe_capacity",
        "max_requests_per_connection",
        "additional_peer_credentials",
        "socket_mode",
        "runtime_profile",
        "long_running_policy",
    }
    if not required <= set(raw) or not set(raw) <= allowed or raw.get("schema_version") != 1:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config schema is unsupported")
    texts = {
        name: raw[name]
        for name in required
        if name not in {"schema_version", "journal_capacity", "peer_uid", "peer_gid"}
    }
    if any(not isinstance(value, str) or not value for value in texts.values()):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config text field is invalid")
    if (
        any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (raw["peer_uid"], raw["peer_gid"])
        )
        or not isinstance(raw["journal_capacity"], int)
        or isinstance(raw["journal_capacity"], bool)
    ):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server config numeric field is invalid")
    request_limit = raw.get("max_requests_per_connection", 1_024)
    if (
        not isinstance(request_limit, int)
        or isinstance(request_limit, bool)
        or not 1 <= request_limit <= 1_024
    ):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS max requests per connection is invalid")
    ingress_limits = {
        "listen_backlog": raw.get("listen_backlog", 8_192),
        "pending_capacity": raw.get("pending_capacity", 2_048),
        "preframe_capacity": raw.get("preframe_capacity", 8_192),
    }
    if any(
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= 65_536
        for value in ingress_limits.values()
    ) or (
        cast(int, ingress_limits["pending_capacity"])
        + cast(int, ingress_limits["preframe_capacity"]) > 65_536
    ):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS ingress capacity is invalid")
    additional_peers_raw = raw.get("additional_peer_credentials", [])
    if not isinstance(additional_peers_raw, list):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS additional peers are invalid")
    additional_items = cast(list[object], additional_peers_raw)
    if len(additional_items) > 16:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS additional peers are invalid")
    additional_peers: list[tuple[int, int]] = []
    for item in additional_items:
        if not isinstance(item, dict):
            raise ConsumerUdsServerConfigErrorV2("Consumer UDS additional peer is invalid")
        peer = cast(dict[str, object], item)
        if set(peer) != {"uid", "gid"}:
            raise ConsumerUdsServerConfigErrorV2("Consumer UDS additional peer is invalid")
        uid, gid = peer["uid"], peer["gid"]
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (uid, gid)
        ):
            raise ConsumerUdsServerConfigErrorV2("Consumer UDS additional peer is invalid")
        additional_peers.append((cast(int, uid), cast(int, gid)))
    primary_peer = (cast(int, raw["peer_uid"]), cast(int, raw["peer_gid"]))
    if len(set(additional_peers)) != len(additional_peers) or primary_peer in additional_peers:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS additional peers must be unique")
    socket_mode = raw.get("socket_mode", 0o600)
    if socket_mode not in {0o600, 0o660}:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS socket mode is invalid")
    socket_path = Path(cast(str, raw["socket_path"]))
    journal_root = Path(cast(str, raw["journal_root"]))
    if not socket_path.is_absolute() or not journal_root.is_absolute():
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS server paths must be absolute")
    try:
        profile = ConsumerRuntimeProfile(raw.get("runtime_profile", "BENCHMARK"))
        policy_raw = raw.get("long_running_policy")
        if profile is ConsumerRuntimeProfile.BENCHMARK and policy_raw is not None:
            raise ConsumerRuntimePolicyError("benchmark cannot configure persistent budgets")
        if profile is ConsumerRuntimeProfile.LONG_RUNNING:
            if not isinstance(policy_raw, dict):
                raise ConsumerRuntimePolicyError("LONG_RUNNING requires explicit budget policy")
            policy = ConsumerLongRunningPolicy.from_mapping(cast(dict[str, object], policy_raw))
        else:
            policy = None
    except (ValueError, TypeError) as exc:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS runtime profile is invalid") from exc
    return ConsumerUdsServerConfigV2(
        socket_path=socket_path,
        journal_root=journal_root,
        journal_capacity=raw["journal_capacity"],
        peer_uid=cast(int, raw["peer_uid"]),
        peer_gid=cast(int, raw["peer_gid"]),
        consumer_id=cast(str, raw["consumer_id"]),
        login_name=cast(str, raw["login_name"]),
        legal_name=cast(str, raw["legal_name"]),
        consumer_account_id=cast(str, raw["consumer_account_id"]),
        account_id=cast(str, raw["account_id"]),
        account_display_name=cast(str, raw["account_display_name"]),
        venue_id=cast(str, raw["venue_id"]),
        adapter_instance_id=cast(str, raw["adapter_instance_id"]),
        strategy_id=cast(str, raw["strategy_id"]),
        listen_backlog=cast(int, ingress_limits["listen_backlog"]),
        pending_capacity=cast(int, ingress_limits["pending_capacity"]),
        preframe_capacity=cast(int, ingress_limits["preframe_capacity"]),
        max_requests_per_connection=request_limit,
        additional_peer_credentials=tuple(additional_peers),
        socket_mode=cast(int, socket_mode),
        runtime_profile=profile,
        long_running_policy=policy,
    )


class _OrderBookProjectionV2:
    """Current-process event reader for the same committed mmap-backed OrderBook.

    The mmap file is the fact fence.  This projection does not claim restart
    recovery: the service creates a fresh journal namespace on launch and must
    be explicitly restarted/reset between independent benchmark rounds.
    """

    LEGACY_SNAPSHOT_MAX_ORDERS = 1_024

    def __init__(
        self,
        *,
        order_book: OrderBook,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
    ) -> None:
        self._order_book, self._binding, self._stream = order_book, binding, stream
        self._snapshot_lock = Lock()
        self._replay_lock = Lock()
        self._replay_items_view: list[tuple[ConsumerEventPositionV2, ConsumerOrderEvent]] = []
        self._replay_positions: list[ConsumerEventPositionV2] = []
        self._positioned_events: list[PositionedConsumerEventV2] = []
        self._latest_orders: dict[str, ConsumerOrderEvent] = {}
        self._snapshot_view: tuple[
            str | None, ConsumerEventPositionV2, tuple[ConsumerOrderEvent, ...]
        ] = (
            None,
            ConsumerEventPositionV2(RedisStreamIdV1(0, 0), 0),
            (),
        )

    def publish(self, event: ConsumerOrderEvent) -> None:
        """Incrementally publish one already-durable event to replay readers.

        The submit/cancel writer invokes this only after its Journal commit.
        Maintaining the position index here prevents the first
        ``CreateReplayPartitions`` request from projecting the entire history
        on the single request reactor.
        """

        if (
            event.account_id != self._binding.account_id
            or event.consumer_trace.consumer_id != self._binding.consumer_id
            or event.consumer_trace.consumer_account_id
            != self._binding.consumer_account_id
            or event.journal_id != self._stream.journal_id
        ):
            raise ConsumerUnixSocketError(
                "Consumer UDS incremental projection scope is invalid"
            )
        position = ConsumerEventPositionV2(RedisStreamIdV1(event.journal_lsn, 0), 0)
        with self._replay_lock:
            event_count = self._order_book.event_count(self._binding.account_id)
            published = len(self._replay_items_view)
            if event_count == published:
                # A concurrent/lazy recovery may already have projected this
                # committed event.  Accept only the exact idempotent tail.
                if (
                    self._replay_positions
                    and self._replay_positions[-1] == position
                    and self._replay_items_view[-1][1] == event
                ):
                    return
                raise ConsumerUnixSocketError(
                    "Consumer UDS incremental projection did not advance"
                )
            if event_count != published + 1 or (
                self._replay_positions and position <= self._replay_positions[-1]
            ):
                raise ConsumerUnixSocketError(
                    "Consumer UDS incremental projection has a sequence gap"
                )
            self._replay_items_view.append((position, event))
            self._replay_positions.append(position)
            self._positioned_events.append(PositionedConsumerEventV2(position, event))
            self._latest_orders[event.oms_order_id] = event

    def _replay_items(
        self, through: ConsumerEventPositionV2 | None = None
    ) -> tuple[
        list[tuple[ConsumerEventPositionV2, ConsumerOrderEvent]],
        list[ConsumerEventPositionV2],
    ]:
        # Fixed-watermark replay need not refresh for later writes. Both the
        # immutable items and their binary-search index are published together.
        if through is not None and self._replay_positions and through <= self._replay_positions[-1]:
            return self._replay_items_view, self._replay_positions
        event_count = self._order_book.event_count(self._binding.account_id)
        if event_count != len(self._replay_items_view):
            with self._replay_lock:
                event_count = self._order_book.event_count(self._binding.account_id)
                start = len(self._replay_items_view)
                if event_count < start:
                    raise ConsumerUnixSocketError("Consumer UDS projection history regressed")
                for source in self._order_book.events_from(self._binding.account_id, start):
                    position = self._position_for_oms(source)
                    event = self._project(source)
                    self._replay_items_view.append((position, event))
                    self._replay_positions.append(position)
                    self._positioned_events.append(PositionedConsumerEventV2(position, event))
                    self._latest_orders[event.oms_order_id] = event
        return self._replay_items_view, self._replay_positions

    def _items(self) -> list[tuple[ConsumerEventPositionV2, ConsumerOrderEvent]]:
        return self._replay_items()[0]

    def replay_partition_bounds(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2, partition_count: int
    ) -> tuple[ConsumerReplayPartitionBoundsV2, ...]:
        if binding != self._binding or stream != self._stream:
            raise ConsumerUnixSocketError("Consumer UDS partition binding is unavailable")
        _items, positions = self._replay_items()
        count = min(partition_count, len(positions))
        bounds: list[ConsumerReplayPartitionBoundsV2] = []
        for index in range(count):
            start = len(positions) * index // count
            end = len(positions) * (index + 1) // count
            bounds.append(
                ConsumerReplayPartitionBoundsV2(
                    positions[start - 1] if start else None, positions[end - 1], end - start
                )
            )
        return tuple(bounds)

    def read_events(
        self,
        *,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        after_position: ConsumerEventPositionV2 | None,
        high_watermark_position: ConsumerEventPositionV2 | None,
        limit: int,
    ) -> ConsumerReplaySliceV2:
        if binding != self._binding or stream != self._stream:
            raise ConsumerUnixSocketError("Consumer UDS projection binding is unavailable")
        all_items, positions = self._replay_items(high_watermark_position)
        current_high = all_items[-1][0] if all_items else None
        high = high_watermark_position if high_watermark_position is not None else current_high
        start = bisect_right(positions, after_position) if after_position is not None else 0
        end = bisect_right(positions, high) if high is not None else 0
        # The immutable view owns the wrappers; don't allocate them again per page.
        # Capture after refresh; a later appended view preserves the same prefix.
        selected = tuple(self._positioned_events[start : min(start + limit, end)])

        return ConsumerReplaySliceV2(
            selected,
            high,
            start + len(selected) < end,
        )

    def read_snapshot(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2
    ) -> ConsumerAccountOrderSnapshotV2:
        if binding != self._binding or stream != self._stream:
            raise ConsumerUnixSocketError("Consumer UDS snapshot binding is unavailable")
        mark_request_stage("snapshot_guard_started_ns")
        order_count = self._order_book.order_count(binding.account_id)
        mark_request_value("snapshot_order_count", order_count)
        mark_request_stage("snapshot_guard_finished_ns")
        if order_count > self.LEGACY_SNAPSHOT_MAX_ORDERS:
            raise ConsumerSnapshotCapacityErrorV2(
                "SNAPSHOT_REQUIRES_PAGINATION",
                "legacy account snapshot exceeds its bounded single-frame capacity",
            )
        mark_request_stage("snapshot_projection_started_ns")
        tail = self._order_book.last_event(binding.account_id)
        tail_id = f"order:{tail.event_id}" if tail is not None else None
        if tail_id != self._snapshot_view[0]:
            with self._snapshot_lock:
                # A second reader may have refreshed the cache while this one
                # waited. The event tuple is a committed, immutable view; a
                # later writer is picked up by the next snapshot request.
                tail = self._order_book.last_event(binding.account_id)
                tail_id = f"order:{tail.event_id}" if tail is not None else None
                if tail_id != self._snapshot_view[0]:
                    items = self._items()
                    position = (
                        items[-1][0] if items else ConsumerEventPositionV2(RedisStreamIdV1(0, 0), 0)
                    )
                    self._snapshot_view = (
                        items[-1][1].consumer_event_id if items else None,
                        position,
                        tuple(self._latest_orders.values()),
                    )
        _cached_tail, position, orders = self._snapshot_view
        now = time_ns()
        cursor = ConsumerEventCursorV2(
            consumer_id=binding.consumer_id,
            consumer_account_id=binding.consumer_account_id,
            journal_id=stream.journal_id,
            stream_generation=stream.stream_generation,
            after_position=position,
            authorization_epoch=1,
            issued_at_wall_ns=now,
            expires_at_wall_ns=now + 3_600_000_000_000,
            cursor_key_id="uds-ephemeral-v1",
        )
        result = ConsumerAccountOrderSnapshotV2(
            consumer_account_id=binding.consumer_account_id,
            snapshot_position=position,
            next_cursor=cursor,
            orders=orders,
        )
        mark_request_stage("snapshot_projection_finished_ns")
        return result

    def capture_snapshot_version(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2
    ) -> ConsumerSnapshotVersionV2:
        """Freeze the latest projected state for the paged snapshot contract."""

        if binding != self._binding or stream != self._stream:
            raise ConsumerUnixSocketError("Consumer UDS snapshot binding is unavailable")
        items = self._items()
        position = (
            items[-1][0] if items else ConsumerEventPositionV2(RedisStreamIdV1(0, 0), 0)
        )
        return ConsumerSnapshotVersionV2(position, tuple(self._latest_orders.values()))

    @staticmethod
    def _position_for_oms(event: OmsOrderEvent) -> ConsumerEventPositionV2:
        return ConsumerEventPositionV2(RedisStreamIdV1(event.journal_lsn, 0), 0)

    def _project(self, event: OmsOrderEvent) -> ConsumerOrderEvent:
        order = self._order_book.order(event.oms_order_id).order
        return ConsumerOrderEvent(
            consumer_event_id=f"order:{event.event_id}",
            event_kind=ConsumerEventKind.ORDER,
            schema_version=2,
            account_id=event.account_id,
            trading_day=order.trading_day,
            journal_id=event.journal_id,
            journal_lsn=event.journal_lsn,
            consumer_trace=order.consumer_trace,
            oms_order_id=event.oms_order_id,
            execution_id=event.execution_id,
            order_state=event.order_state,
            order_revision=event.order_revision,
        )


def build_consumer_uds_service_v2(
    *,
    config: ConsumerUdsServerConfigV2,
    contract_root: Path,
) -> tuple[ConsumerUnixServiceV2, MmapOmsFactJournalV1]:
    """Assemble the only server-local UDS profile allowed for this benchmark."""

    if config.runtime_profile is ConsumerRuntimeProfile.LONG_RUNNING:
        # Capability registration is code-owned, never a deployment override.
        require_long_running_capabilities(frozenset())
    require_safe_consumer_runtime_root(config.journal_root)
    journal_id = f"consumer-uds-{uuid.uuid4().hex}"
    journal = MmapOmsFactJournalV1(
        path=config.journal_root / f"{journal_id}.mmap",
        journal_id=journal_id,
        capacity=config.journal_capacity,
    )
    account = ConsumerAccountBinding(
        "consumer-uds-account-v1",
        1,
        config.consumer_id,
        config.consumer_account_id,
        config.account_id,
        config.account_display_name,
        config.venue_id,
        config.adapter_instance_id,
    )
    strategy = ConsumerStrategyBinding(
        "consumer-uds-strategy-v1",
        1,
        config.consumer_id,
        config.consumer_account_id,
        account.binding_id,
        config.strategy_id,
    )
    attribution = AttributionBinding(
        "consumer-uds-attribution-v1",
        account.binding_id,
        strategy.binding_id,
        config.consumer_id,
        config.consumer_account_id,
        config.strategy_id,
        1,
    )
    snapshot = ConsumerBindingSnapshotV2(1, (account,), (strategy,))
    stream = CursorStreamRefV2(journal_id, 1)
    # The no-Adapter benchmark process has one immutable stream generation.
    # Validate its graph once, then reuse it for every read and signed cursor.
    registry = CursorStreamRegistryV2(stream, (stream,))
    order_book = OrderBook(journal=journal)
    projection = _OrderBookProjectionV2(order_book=order_book, binding=account, stream=stream)
    # This process is a dedicated, short-lived no-Adapter benchmark service.
    # Disable automatic cyclic GC before seed objects are published so no
    # request inherits an unbounded full-generation collection.  Normal
    # reference counting remains active and the worker restores the original
    # process-wide setting during managed shutdown.
    snapshot_worker = ConsumerSnapshotReadWorkerV2(
        disable_parent_automatic_gc_on_start=True
    )

    def publish_order_event(event: ConsumerOrderEvent) -> None:
        # Each projection is advisory and independently recoverable from the
        # committed Journal.  Attempt both even if one projection rejects an
        # update so one stale read model cannot suppress the other.
        failures: list[Exception] = []
        for publish in (projection.publish, snapshot_worker.publish):
            try:
                publish(event)
            except Exception as exc:
                failures.append(exc)
        if failures:
            raise failures[0]

    def admission_context_for(
        binding: ConsumerAccountBinding, strategy_binding_id: str, request: object
    ) -> ConsumerOmsAdmissionContextV2:
        from .base import ConsumerOrderSignal

        if (
            binding != account
            or strategy_binding_id != strategy.binding_id
            or not isinstance(request, ConsumerOrderSignal)
        ):
            raise ConsumerUnixSocketError("Consumer UDS admission binding is invalid")
        now = monotonic_ns()
        risk_id = f"consumer-uds-risk-{request.trading_day.value}"
        risk = RiskSnapshot(
            risk_id,
            account.account_id,
            request.trading_day,
            "0" * 64,
            now,
            now + 60_000_000_000,
            10**18,
            10**18,
        )
        decision = DecisionContext(
            f"consumer-uds-decision-{uuid.uuid4().hex}",
            account.account_id,
            request.trading_day,
            risk_id,
            "0" * 64,
            request.protection_price_ticks,
            "consumer-uds-fee-v1",
            "consumer-uds-margin-v1",
            "consumer-uds-reservation-v1",
            now,
        )
        unit_price = request.limit_price_ticks or request.protection_price_ticks or 1
        return ConsumerOmsAdmissionContextV2(
            strategy,
            attribution,
            risk,
            decision,
            ReservationRequirement(
                request.symbol, cash_minor=request.quantity * max(1, unit_price)
            ),
        )

    operations = OrderBookConsumerOperationsV2(
        order_book=order_book,
        admission_context_for=admission_context_for,
        snapshot_for=ConsumerSnapshotProjectionV2(
            registry_for=lambda binding: consumer_registry_for_binding(binding, account, registry),
            reader=projection,
        ),
        on_order_event_committed=publish_order_event,
    )
    key_ring = CursorKeyRingV2(
        current=CursorSigningKeyV2("uds-ephemeral-v1", secrets.token_bytes(32))
    )
    replay = ConsumerCursorReplayServiceV2(
        key_ring_for=lambda binding: _key_ring_for(binding, account, key_ring),
        registry_for=lambda binding: consumer_registry_for_binding(binding, account, registry),
        event_reader=projection,
        partition_reader=projection,
        cursor_ttl_ns=3_600_000_000_000,
    )
    paged_snapshots = ConsumerSnapshotIndexV2(
        registry_for=lambda binding: consumer_registry_for_binding(binding, account, registry),
        reader=snapshot_worker,
        cursor_codec=SnapshotCursorCodecV2(
            SnapshotCursorSigningKeyV2("uds-snapshot-ephemeral-v1", secrets.token_bytes(32))
        ),
    )
    contract = load_contract_identity(contract_root)
    gateway = ConsumerGatewayV2(
        binding_snapshot=lambda: snapshot,
        account_view_for=lambda binding: consumer_no_adapter_account_view(binding, account),
        wire_contract=contract,
        event_replay=replay,
        operations=operations,
        paged_snapshots=paged_snapshots,
    )
    schema = load_consumer_request_schema(contract_root)
    configured_principal = ConsumerPrincipal(
        config.consumer_id, config.login_name, config.legal_name
    )

    authorized_peers = frozenset(
        ((config.peer_uid, config.peer_gid), *config.additional_peer_credentials)
    )

    def principal_for_peer(peer: ConsumerUnixPeerV2) -> ConsumerPrincipal:
        if (peer.uid, peer.gid) not in authorized_peers:
            raise ConsumerUnixSocketError("Consumer UDS peer is not authorized")
        return configured_principal

    listener = ConsumerUnixSocketListenerV2(
        socket_path=config.socket_path,
        gateway=gateway,
        principal_for_peer=principal_for_peer,
        request_schema=schema,
        request_handler=ConsumerWireDispatcherV2(gateway),
        max_requests_per_connection=config.max_requests_per_connection,
        listen_backlog=config.listen_backlog,
        remove_verified_stale_socket=True,
        parallel_read_operations=frozenset(
            {"GetAccountOrderSnapshot", "CreateOrderSnapshot", "ReadOrderSnapshotPage"}
        ),
        socket_mode=config.socket_mode,
    )
    return (
        ConsumerUnixServiceV2(
            listener,
            frame_ready_ingress=True,
            request_reactor=True,
            pending_capacity=config.pending_capacity,
            preframe_capacity=config.preframe_capacity,
            managed_components=(snapshot_worker,),
        ),
        journal,
    )


def run_consumer_uds_service_v2(*, config_path: Path, contract_root: Path) -> int:
    """Run the no-Adapter listener until systemd sends SIGTERM/SIGINT."""

    logging.basicConfig(level=logging.INFO)
    config = load_consumer_uds_server_config_v2(config_path)
    service, journal = build_consumer_uds_service_v2(config=config, contract_root=contract_root)
    trace = None
    previous: dict[int, object] = {}

    def stop(_signal_number: int, _frame: object) -> None:
        service.request_stop()

    try:
        if os.environ.get("TRADECORE_UDS_TRACE") == "1":
            trace_root = config.journal_root / "traces"
            trace_root.mkdir(mode=0o700, exist_ok=True)
            require_safe_consumer_runtime_root(trace_root)
            trace = BoundedRequestTraceWriterV2(
                trace_root / f"{journal.journal_id}.jsonl", journal_id=journal.journal_id
            )
            trace.set_status_provider(lambda: {"ingress": asdict(service.ingress_stats())})
            service.set_request_trace(trace)
        for number in (signal.SIGTERM, signal.SIGINT):
            previous[number] = signal.signal(number, stop)
        service.start()
        service.serve_forever()
        return 0
    finally:
        service.stop()
        if trace is not None:
            trace.close()
        journal.close()
        for number, handler in previous.items():
            signal.signal(number, cast(signal.Handlers, handler))


def consumer_registry_for_binding(
    binding: ConsumerAccountBinding,
    expected: ConsumerAccountBinding,
    registry: CursorStreamRegistryV2,
) -> CursorStreamRegistryV2:
    if binding != expected:
        raise ConsumerUnixSocketError("Consumer UDS stream binding is unavailable")
    return registry


def _key_ring_for(
    binding: ConsumerAccountBinding,
    expected: ConsumerAccountBinding,
    key_ring: CursorKeyRingV2,
) -> CursorKeyRingV2:
    if binding != expected:
        raise ConsumerUnixSocketError("Consumer UDS cursor binding is unavailable")
    # The no-Adapter benchmark process starts with a fresh namespace and a
    # fresh ephemeral cursor key.  No key is placed in the user-supplied config.
    return key_ring


def consumer_no_adapter_account_view(
    binding: ConsumerAccountBinding, expected: ConsumerAccountBinding
) -> ConsumerAccountView:
    if binding != expected:
        raise ConsumerUnixSocketError("Consumer UDS account binding is unavailable")
    return ConsumerAccountView(
        binding.consumer_account_id,
        binding.account_display_name,
        binding.venue_id,
        "consumer-uds-no-adapter-v1",
        AccountLifecycle.DISARMED,
        ReconciliationStatus.NOT_REQUIRED,
    )


def load_consumer_request_schema(contract_root: Path) -> Mapping[str, object]:
    try:
        raw = json.loads((contract_root / "schemas" / "request.schema.json").read_text("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS request schema is unavailable") from exc
    if not isinstance(raw, dict):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS request schema is invalid")
    return cast(Mapping[str, object], raw)


def require_safe_consumer_runtime_root(path: Path) -> None:
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_mode & 0o077
    ):
        raise ConsumerUdsServerConfigErrorV2("Consumer UDS journal runtime root is unsafe")

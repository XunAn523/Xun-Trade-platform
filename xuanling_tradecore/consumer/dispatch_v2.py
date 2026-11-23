"""Concrete Consumer V2 wire dispatcher for the authorized data plane."""

from __future__ import annotations

from typing import Mapping, cast

from ..domain import (
    ConsumerAccountRiskEvent,
    ConsumerAccountStatusEvent,
    ConsumerAuditTrail,
    ConsumerOrderEvent,
    TradingDayEpoch,
)
from ..domain._validation import DomainValidationError
from .base import (
    ConsumerAccountView,
    ConsumerAuthorizationError,
    ConsumerNotFoundError,
    ConsumerOrderSignal,
    ConsumerSession,
    IntentReceipt,
)
from .cold_admission_v2 import ConsumerAdmissionCapacityV2
from .gateway_v2 import ConsumerGatewayConfigurationError, ConsumerGatewayV2
from .payload_v2 import (
    CanonicalPayloadV2,
    CanonicalResponsePayloadV2,
    canonical_payload_bytes,
    order_event_payload,
)
from .projection_reads_v2 import ConsumerProjectionReadErrorV2
from .replay_v2 import ConsumerCursorReplayError
from .request_trace_v2 import mark_request_failure, mark_request_stage, mark_request_value
from .snapshot_index_v2 import ConsumerPagedSnapshotErrorV2
from .snapshot_v2 import ConsumerSnapshotResetErrorV2


class ConsumerWireDispatchError(RuntimeError):
    def __init__(self, code: str, *, retryable: bool = False) -> None:
        super().__init__(code)
        self.code, self.retryable = code, retryable


class ConsumerWireDispatcherV2:
    """Map validated wire payloads onto one session-bound Gateway V2."""

    def __init__(self, gateway: ConsumerGatewayV2) -> None:
        self._gateway = gateway

    def __call__(
        self, session: ConsumerSession, request: Mapping[str, object]
    ) -> Mapping[str, object]:
        operation = request.get("operation")
        request_id = request.get("request_id")
        payload = request.get("payload")
        if (
            not isinstance(operation, str)
            or not isinstance(request_id, str)
            or not isinstance(payload, Mapping)
        ):
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
        typed_payload = cast(Mapping[str, object], payload)
        try:
            if operation in {"CreateOrderSnapshot", "ReadOrderSnapshotPage"}:
                return self._snapshot_page_operation(session, operation, request_id, typed_payload)
            if operation == "ListAccounts":
                return self._response(
                    request_id,
                    {
                        "accounts": [
                            self._account(item) for item in self._gateway.list_accounts(session)
                        ]
                    },
                )
            if operation == "CreateReplayPartitions":
                return self._replay_partitions_operation(session, request_id, typed_payload)
            if operation == "PollEvents":
                cursor, limit = typed_payload.get("cursor"), typed_payload.get("limit")
                if (
                    not isinstance(cursor, str)
                    or not isinstance(limit, int)
                    or isinstance(limit, bool)
                ):
                    raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
                page = self._gateway.poll_event_cursor(session, token=cursor, limit=limit)
                events = [
                    self._event(item.position.to_external_id(), item.event) for item in page.items
                ]
                mark_request_stage("event_payload_built_ns")
                return self._response(
                    request_id,
                    {
                        "events": events,
                        "high_watermark_position": None
                        if page.high_watermark_position is None
                        else page.high_watermark_position.to_external_id(),
                        "has_more": page.has_more,
                        "next_cursor": page.next_cursor_token,
                    },
                )
            if operation == "SubmitSignal":
                signal = self._signal(request_id, typed_payload)
                receipt = self._gateway.submit_signal(session, signal)
                return self._response(request_id, self._receipt(receipt), status="ACCEPTED")
            if operation == "CancelOrder":
                oms_order_id, reason = self._required_texts(typed_payload, "oms_order_id", "reason")
                receipt = self._gateway.cancel_order(
                    session,
                    consumer_request_id=request_id,
                    oms_order_id=oms_order_id,
                    reason=reason,
                )
                return self._response(request_id, self._receipt(receipt), status="ACCEPTED")
            if operation == "GetOrder":
                (oms_order_id,) = self._required_texts(typed_payload, "oms_order_id")
                return self._response(
                    request_id,
                    {
                        "order": self._event(
                            "", self._gateway.get_order(session, oms_order_id=oms_order_id)
                        )
                    },
                )
            if operation == "GetOrderAuditTrail":
                (oms_order_id,) = self._required_texts(typed_payload, "oms_order_id")
                trail = self._gateway.get_order_audit_trail(session, oms_order_id=oms_order_id)
                return self._response(
                    request_id,
                    {"audit_trail": [self._audit(item) for item in trail]},
                )
            if operation == "GetAccountOrderSnapshot":
                consumer_account_id, reset_context = self._snapshot_arguments(typed_payload)
                snapshot = self._gateway.get_account_order_snapshot(
                    session,
                    consumer_account_id=consumer_account_id,
                    reset_context=reset_context,
                )
                mark_request_stage("snapshot_payload_build_started_ns")
                orders = [self._event("", item) for item in snapshot.orders]
                mark_request_value("snapshot_response_order_count", len(orders))
                mark_request_stage("snapshot_payload_build_finished_ns")
                return self._response(
                    request_id,
                    {
                        "consumer_account_id": snapshot.consumer_account_id,
                        "snapshot_position": snapshot.snapshot_position.to_external_id(),
                        "orders": orders,
                        "next_cursor": self._gateway.issue_snapshot_cursor(session, snapshot),
                    },
                )
            if operation == "SubscribeOrderEvents":
                cursor = typed_payload.get("cursor")
                if not isinstance(cursor, str):
                    raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
                self._gateway.subscribe_order_events(session, cursor_token=cursor)
                return self._response(request_id, {"cursor": cursor}, status="ACCEPTED")
        except ConsumerAuthorizationError as exc:
            raise ConsumerWireDispatchError("UNAUTHORIZED") from exc
        except ConsumerAdmissionCapacityV2 as exc:
            raise ConsumerWireDispatchError("RATE_LIMITED", retryable=True) from exc
        except ConsumerProjectionReadErrorV2 as exc:
            mark_request_failure(
                "audit_evidence" if exc.code == "AUDIT_EVIDENCE_UNAVAILABLE" else "projection_read",
                exc.code if exc.code == "AUDIT_EVIDENCE_UNAVAILABLE" else "READ_MODEL_UNAVAILABLE",
            )
            raise ConsumerWireDispatchError(exc.code, retryable=True) from exc
        except ConsumerCursorReplayError as exc:
            mark_request_failure(
                "cursor_replay",
                exc.code
                if exc.code
                in {
                    "CURSOR_INVALID",
                    "CURSOR_EXPIRED",
                    "CURSOR_RECOVERY_UNAVAILABLE",
                    "JOURNAL_GAP",
                }
                else "CURSOR_REPLAY_FAILED",
            )
            raise ConsumerWireDispatchError(exc.code) from exc
        except ConsumerSnapshotResetErrorV2 as exc:
            mark_request_failure("snapshot", exc.code)
            raise ConsumerWireDispatchError(exc.code) from exc
        except ConsumerPagedSnapshotErrorV2 as exc:
            mark_request_failure("snapshot_page", exc.code)
            raise ConsumerWireDispatchError(exc.code, retryable=exc.retryable) from exc
        except ConsumerNotFoundError as exc:
            raise ConsumerWireDispatchError("NOT_FOUND") from exc
        except ConsumerGatewayConfigurationError as exc:
            raise ConsumerWireDispatchError("INTERNAL_ERROR") from exc
        except DomainValidationError as exc:
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED") from exc
        raise ConsumerWireDispatchError("UPGRADE_REQUIRED")

    def _replay_partitions_operation(
        self, session: ConsumerSession, request_id: str, payload: Mapping[str, object]
    ) -> Mapping[str, object]:
        (account_id,) = self._required_texts(payload, "consumer_account_id")
        count = payload.get("partition_count")
        if type(count) is not int or not 1 <= count <= 64:
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
        after_cursor = payload.get("after_cursor")
        if after_cursor is not None and (
            not isinstance(after_cursor, str) or not 1 <= len(after_cursor) <= 4096
        ):
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
        plan = self._gateway.create_replay_partitions(
            session,
            consumer_account_id=account_id,
            partition_count=count,
            after_cursor=after_cursor,
        )
        partitions = [
            {
                "partition_id": index,
                "after_position": None
                if item.bounds.after_position is None
                else item.bounds.after_position.to_external_id(),
                "high_watermark_position": item.bounds.high_watermark_position.to_external_id(),
                "event_count": item.bounds.event_count,
                "cursor": item.cursor_token,
            }
            for index, item in enumerate(plan.partitions)
        ]
        return self._response(
            request_id,
            {
                "consumer_account_id": account_id,
                "journal_id": plan.stream.journal_id,
                "stream_generation": plan.stream.stream_generation,
                "event_count": sum(item.bounds.event_count for item in plan.partitions),
                "high_watermark_position": partitions[-1]["high_watermark_position"]
                if partitions
                else None,
                "partitions": partitions,
            },
        )

    def _snapshot_page_operation(
        self,
        session: ConsumerSession,
        operation: str,
        request_id: str,
        payload: Mapping[str, object],
    ) -> Mapping[str, object]:
        if operation == "CreateOrderSnapshot":
            (consumer_account_id,) = self._required_texts(payload, "consumer_account_id")
            page_size = payload.get("page_size")
            if type(page_size) is not int:
                raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
            mark_request_stage("snapshot_version_acquire_started_ns")
            plan = self._gateway.create_order_snapshot(
                session, consumer_account_id=consumer_account_id, page_size=page_size
            )
            mark_request_stage("snapshot_version_acquired_ns")
            return self._response(
                request_id,
                {
                    "snapshot_id": plan.snapshot_id,
                    "consumer_account_id": plan.consumer_account_id,
                    "journal_id": plan.journal_id,
                    "stream_generation": plan.stream_generation,
                    "snapshot_position": plan.snapshot_position.to_external_id(),
                    "order_count": plan.order_count,
                    "page_size": plan.page_size,
                    "page_cursor": plan.page_cursor,
                    "created_at_wall_ns": plan.created_at_wall_ns,
                    "expires_at_wall_ns": plan.expires_at_wall_ns,
                    "projection_lag": plan.projection_lag,
                    "stale": plan.stale,
                },
            )
        (page_cursor,) = self._required_texts(payload, "page_cursor")
        mark_request_stage("snapshot_page_read_started_ns")
        page, replay_cursor = self._gateway.read_order_snapshot_page(
            session, page_cursor=page_cursor
        )
        mark_request_stage("snapshot_page_read_finished_ns")
        orders = list(page.order_payloads)
        mark_request_value("snapshot_page_order_count", page.page_count)
        mark_request_stage("snapshot_page_payload_built_ns")
        response_payload = {
            "snapshot_id": page.snapshot_id,
            "consumer_account_id": page.consumer_account_id,
            "journal_id": page.journal_id,
            "stream_generation": page.stream_generation,
            "snapshot_position": page.snapshot_position.to_external_id(),
            "orders": orders,
            "count": page.page_count,
            "has_more": page.has_more,
            "next_page_cursor": page.next_page_cursor,
            "replay_cursor": replay_cursor,
        }
        response = (
            CanonicalResponsePayloadV2(
                request_id=request_id,
                payload=response_payload,
                payload_bytes=page.response_payload_bytes,
            )
            if replay_cursor is None
            else CanonicalPayloadV2(self._response(request_id, response_payload))
        )
        if len(canonical_payload_bytes(response)) > page.payload_budget_bytes:
            raise ConsumerPagedSnapshotErrorV2(
                "FRAME_TOO_LARGE", "snapshot response exceeds its encoded byte budget"
            )
        return response

    @staticmethod
    def _response(
        request_id: str, payload: Mapping[str, object], *, status: str = "COMPLETED"
    ) -> Mapping[str, object]:
        return {
            "message_type": "response",
            "schema_version": 2,
            "request_id": request_id,
            "status": status,
            "payload": dict(payload),
        }

    @staticmethod
    def _account(view: ConsumerAccountView) -> Mapping[str, object]:
        return {
            "consumer_account_id": view.consumer_account_id,
            "account_display_name": view.account_display_name,
            "venue_id": view.venue_id,
            "capability_profile_id": view.capability_profile_id,
            "lifecycle": view.lifecycle.value,
            "reconciliation_status": view.reconciliation_status.value,
        }

    @staticmethod
    def _event(position: str, event: object) -> Mapping[str, object]:
        if isinstance(event, ConsumerOrderEvent):
            return order_event_payload(event, position=position)
        elif isinstance(event, ConsumerAccountStatusEvent):
            payload: Mapping[str, object] = {
                "lifecycle": event.lifecycle.value,
                "reconciliation_status": event.reconciliation_status.value,
                "gates": [gate.kind.value for gate in event.gates],
            }
        elif isinstance(event, ConsumerAccountRiskEvent):
            payload = {
                "risk_snapshot_id": event.risk_snapshot_id,
                "error_category": event.error_category.value,
                "summary": dict(event.summary),
            }
        else:
            raise ConsumerWireDispatchError("EVENT_SCHEMA_INCOMPATIBLE")
        result: dict[str, object] = {
            "event_id": event.consumer_event_id,
            "event_kind": event.event_kind.value,
            "payload": payload,
        }
        if position:
            result["position"] = position
        return result

    @staticmethod
    def _signal(request_id: str, payload: Mapping[str, object]) -> ConsumerOrderSignal:
        required = ConsumerWireDispatcherV2._required_texts(
            payload,
            "consumer_account_id",
            "strategy_id",
            "strategy_order_id",
            "trading_day",
            "symbol",
            "market",
            "side",
            "price_type",
            "business_type",
        )
        quantity, protection = payload.get("quantity"), payload.get("protection_price_ticks")
        limit, parameters = (
            payload.get("limit_price_ticks"),
            payload.get("execution_parameters", {}),
        )
        raw_parameters = cast(Mapping[object, object], parameters)
        if (
            not isinstance(quantity, int)
            or isinstance(quantity, bool)
            or not isinstance(protection, int)
            or isinstance(protection, bool)
            or (limit is not None and (not isinstance(limit, int) or isinstance(limit, bool)))
            or not isinstance(parameters, Mapping)
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in raw_parameters.items()
            )
        ):
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
        return ConsumerOrderSignal(
            consumer_request_id=request_id,
            consumer_account_id=required[0],
            strategy_id=required[1],
            strategy_order_id=required[2],
            trading_day=TradingDayEpoch(required[3]),
            symbol=required[4],
            market=required[5],
            side=required[6],
            price_type=required[7],
            business_type=required[8],
            quantity=quantity,
            protection_price_ticks=protection,
            limit_price_ticks=limit,
            execution_parameters=cast(Mapping[str, str], parameters),
        )

    @staticmethod
    def _required_texts(payload: Mapping[str, object], *names: str) -> tuple[str, ...]:
        values = tuple(payload.get(name) for name in names)
        if any(not isinstance(value, str) or not value for value in values):
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
        return cast(tuple[str, ...], values)

    @staticmethod
    def _snapshot_arguments(payload: Mapping[str, object]) -> tuple[str, str | None]:
        (consumer_account_id,) = ConsumerWireDispatcherV2._required_texts(
            payload, "consumer_account_id"
        )
        reset_context = payload.get("reset_context")
        if reset_context is not None and (not isinstance(reset_context, str) or not reset_context):
            raise ConsumerWireDispatchError("SCHEMA_VALIDATION_FAILED")
        return consumer_account_id, reset_context

    @staticmethod
    def _receipt(receipt: IntentReceipt) -> Mapping[str, object]:
        # Keep receipt serialization narrow: no broker credential or profile extension leaks here.
        return {
            "consumer_request_id": receipt.consumer_request_id,
            "accepted_event_id": receipt.accepted_event_id,
            "oms_order_id": receipt.oms_order_id,
            "execution_id": receipt.execution_id,
            "journal_id": receipt.journal_id,
            "journal_lsn": receipt.journal_lsn,
        }

    @staticmethod
    def _audit(item: ConsumerAuditTrail) -> Mapping[str, object]:
        return {
            "audit_id": item.audit_id,
            "oms_order_id": item.oms_order_id,
            "execution_id": item.execution_id,
            "journal_id": item.journal_id,
            "journal_lsn": item.journal_lsn,
            "action": item.action,
        }

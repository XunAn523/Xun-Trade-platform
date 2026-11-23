"""Snapshot failures and full-response budgets through the authorized wire path."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
from xuanling_tradecore.consumer.bindings_v2 import ConsumerBindingSnapshotV2
from xuanling_tradecore.consumer.dispatch_v2 import (
    ConsumerWireDispatchError,
    ConsumerWireDispatcherV2,
)
from xuanling_tradecore.consumer.gateway_v2 import ConsumerGatewayV2
from xuanling_tradecore.consumer.replay_v2 import ConsumerCursorReplayServiceV2
from xuanling_tradecore.consumer.snapshot_index_v2 import ConsumerPagedSnapshotErrorV2
from xuanling_tradecore.consumer.snapshot_v2 import (
    ConsumerSnapshotCapacityErrorV2,
    ConsumerSnapshotProjectionV2,
    ConsumerSnapshotReaderV2,
)
from xuanling_tradecore.consumer.wire_v2 import HEADER_SIZE_BYTES, MessageKind, encode_frame
from xuanling_tradecore.domain import ConsumerPrincipal

import test_consumer_gateway_v2 as gateway_fixtures
import test_consumer_snapshot_index_v2 as snapshot_fixtures


@pytest.fixture
def context():
    case = snapshot_fixtures.ConsumerSnapshotIndexV2Tests()
    case.setUp()
    # The valid cursor targets the second account: a nonmatching first account
    # must not hide the later account's success or actionable error.
    other = replace(
        case.binding, binding_id="other", consumer_account_id="ca-0", account_id="other"
    )
    bindings = ConsumerBindingSnapshotV2(
        authorization_epoch=7, accounts=(other, case.binding), strategies=()
    )

    def build(index, operations=None):
        gateway = ConsumerGatewayV2(
            binding_snapshot=lambda: bindings,
            account_view_for=gateway_fixtures.ConsumerGatewayV2Tests._ready_view,
            wire_contract=gateway_fixtures._contract(),
            paged_snapshots=index,
            operations=operations,
            event_replay=cast(
                ConsumerCursorReplayServiceV2,
                SimpleNamespace(issue_cursor=Mock(return_value="replay-" + "r" * 500)),
            ),
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), gateway_fixtures._versions()
        )
        return gateway, session, ConsumerWireDispatcherV2(gateway)

    return case, build


def request(cursor, request_id="request-1"):
    return {
        "operation": "ReadOrderSnapshotPage",
        "request_id": request_id,
        "payload": {"page_cursor": cursor},
    }


def test_legacy_capacity_error_survives_projection_gateway_and_dispatch(context):
    case, build = context
    reader = SimpleNamespace(
        read_snapshot=Mock(
            side_effect=ConsumerSnapshotCapacityErrorV2(
                "SNAPSHOT_REQUIRES_PAGINATION", "capacity exceeded"
            )
        )
    )
    projection = ConsumerSnapshotProjectionV2(
        registry_for=lambda _: case.registry,
        reader=cast(ConsumerSnapshotReaderV2, reader),
    )
    operations = SimpleNamespace(
        get_account_order_snapshot=lambda **kwargs: projection(
            kwargs["session"], kwargs["account_binding"], kwargs["reset_context"]
        )
    )
    _, session, dispatcher = build(None, operations)
    req = {
        "operation": "GetAccountOrderSnapshot",
        "request_id": "r",
        "payload": {"consumer_account_id": "ca-1"},
    }
    with pytest.raises(ConsumerWireDispatchError) as failure:
        dispatcher(session, req)
    assert failure.value.code == "SNAPSHOT_REQUIRES_PAGINATION"
    # Unexpected failures retain the existing sanitized recovery error.
    reader.read_snapshot.side_effect = RuntimeError("private reader failure")
    with pytest.raises(ConsumerWireDispatchError) as failure:
        dispatcher(session, req)
    assert failure.value.code == "CURSOR_RECOVERY_UNAVAILABLE"


def test_page_capacity_error_survives_gateway_and_dispatch(context):
    case, build = context
    index = case._index(page_payload_budget_bytes=120)
    gateway, session, dispatcher = build(index)
    plan = gateway.create_order_snapshot(session, consumer_account_id="ca-1", page_size=1)
    # The old five-field estimate was 119 bytes and incorrectly passed this budget.
    with pytest.raises(ConsumerWireDispatchError) as failure:
        dispatcher(session, request(plan.page_cursor))
    assert failure.value.code == "FRAME_TOO_LARGE"
    assert failure.value.retryable is False


@pytest.mark.parametrize("final", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_full_response_including_cursors_and_unicode_enforces_exact_budget(context, final, empty):
    case, build = context
    if empty:
        case.reader.orders = ()
    else:
        case.reader.orders = tuple(
            replace(item, consumer_event_id='事件-"\\' + item.consumer_event_id)
            for item in case.reader.orders
        )
    index = case._index()
    gateway, session, dispatcher = build(index)
    plan = gateway.create_order_snapshot(
        session, consumer_account_id="ca-1", page_size=3 if final else 1
    )
    req = request(plan.page_cursor, request_id='请求-"\\')
    response = dispatcher(session, req)
    expected = len(encode_frame(MessageKind.RESPONSE, 1, response)) - HEADER_SIZE_BYTES
    if final or empty:
        assert response["payload"]["replay_cursor"] is not None
    else:
        assert response["payload"]["next_page_cursor"] is not None
    # Keep the same published version and signed cursors for exact boundaries.
    index._page_budget = expected
    assert dispatcher(session, req) == response
    index._page_budget = expected - 1
    with pytest.raises(ConsumerWireDispatchError) as failure:
        dispatcher(session, req)
    assert failure.value.code == "FRAME_TOO_LARGE"


def test_authorized_projection_unavailable_retains_retryability(context):
    case, build = context
    index = case._index()
    gateway, session, dispatcher = build(index)
    plan = gateway.create_order_snapshot(session, consumer_account_id="ca-1", page_size=1)
    index._registry = Mock(
        side_effect=ConsumerPagedSnapshotErrorV2(
            "SNAPSHOT_NOT_READY", "projection restarting", retryable=True
        )
    )
    with pytest.raises(ConsumerWireDispatchError) as failure:
        dispatcher(session, request(plan.page_cursor))
    assert failure.value.code == "SNAPSHOT_NOT_READY"
    assert failure.value.retryable is True


def test_malformed_cursor_still_fails_without_exposing_snapshot_capacity(context):
    case, build = context
    _, session, dispatcher = build(case._index(page_payload_budget_bytes=1))
    with pytest.raises(ConsumerWireDispatchError) as failure:
        dispatcher(session, request("invalid-token"))
    assert failure.value.code == "CURSOR_INVALID"

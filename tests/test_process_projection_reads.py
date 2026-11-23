import os
import socket
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from multiprocessing.connection import Connection
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.projection_reads_v2 import (
    ConsumerProjectionReadErrorV2,
    projection_read_scope,
)
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStale,
)
from xuanling_tradecore.read_model.consumer_order_reader_v2 import ConsumerOrderProjectionReaderV2
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalProjectionHealth
from xuanling_tradecore.read_model.process_projection_reads import ProcessProjectionReads, _receive

from test_order_projection_store import SCOPE, admit, create, drain
from test_tradecore_oms import _bindings

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="explicit Linux read owners")


def test_partial_ipc_frame_has_absolute_deadline_and_oversize_is_rejected_before_body():
    for data, message in ((b"\x00", "deadline"), (struct.pack(">I", 4097), "capacity")):
        left, right = socket.socketpair()
        channel = Connection(left.detach())
        try:
            os.set_blocking(channel.fileno(), False)
            right.sendall(data)
            started = time.monotonic()
            with pytest.raises(OrderProjectionError, match=message):
                _receive(channel, 4096, deadline=started + 0.03)
            assert time.monotonic() - started < 1
        finally:
            channel.close()
            right.close()


@pytest.mark.parametrize("reuse", [False, True])
def test_child_readers_preserve_full_store_results_and_parent_guards(tmp_path, reuse):
    journal, book, store, tail = create(tmp_path)
    owners = ProcessProjectionReads(
        root=tmp_path / "projection",
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        maximum_database_bytes=8 * 1024**3,
        cpus=(min(os.sched_getaffinity(0)),),
        reuse_connections=reuse,
    )
    try:
        order = admit(book)
        drain(store, tail)
        cut = journal.read_bound().committed_lsn
        expected = store.events_after(SCOPE, 0)
        expected_plan = store.order_audit_plan(
            SCOPE, order.order.order.oms_order_id, minimum_watermark=cut
        )
        owners.start()
        with patch(
            "xuanling_tradecore.read_model.process_projection_reads.OrderProjectionStore",
            side_effect=AssertionError("parent must not open SQL"),
        ):
            assert owners.events_after(SCOPE, 0) == expected
            assert owners.partition_events(SCOPE, partition_count=4) == store.partition_events(
                SCOPE, partition_count=4
            )
            assert owners.lookup(SCOPE, order.order.order.oms_order_id) == expected.events[0]
            assert (
                owners.order_audit_plan(
                    SCOPE, order.order.order.oms_order_id, minimum_watermark=cut
                )
                == expected_plan
            )
        with pytest.raises(OrderProjectionStale):
            owners.events_after(SCOPE, 0, minimum_watermark=cut + 1)
        assert not owners.stats()["failed"]
        assert owners.lookup(("foreign", *SCOPE[1:]), order.order.order.oms_order_id) is None
        with ThreadPoolExecutor(max_workers=4) as workers:
            assert all(
                result == expected
                for result in workers.map(lambda _: owners.events_after(SCOPE, 0), range(16))
            )
        health = [IncrementalProjectionHealth("RUNNING", cut, cut, 1, 100)]
        stream = CursorStreamRefV2(journal.journal_id, 1)
        reader = ConsumerOrderProjectionReaderV2(
            root=tmp_path / "projection",
            journal=journal,
            stream=stream,
            current_stream=lambda: stream,
            health_for=lambda: health[0],
            clock_ns=lambda: 100,
            process_reads=owners,
        )
        with projection_read_scope("PollEvents"):
            page = reader.read_events(
                binding=_bindings()[0],
                stream=stream,
                after_position=None,
                high_watermark_position=None,
                limit=100,
            )
            assert len(page.items) == 1
        before = owners.stats()["calls"]
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.get_order(
                account_binding=_bindings()[0], oms_order_id=order.order.order.oms_order_id
            )
        assert owners.stats()["calls"] == before
        health[0] = replace(health[0], state="FAILED")
        with projection_read_scope("PollEvents"), pytest.raises(ConsumerProjectionReadErrorV2):
            reader.read_events(
                binding=_bindings()[0],
                stream=stream,
                after_position=None,
                high_watermark_position=None,
                limit=100,
            )
        assert owners.stats()["calls"] == before
        assert journal.read_bound().committed_lsn == cut
    finally:
        owners.close()
        tail.close()
        store.close()
        journal.close()
    assert owners.stats()["alive"] == 0


@pytest.mark.parametrize("reuse", [False, True])
def test_child_death_poisoned_without_retry_restart_or_source_change(tmp_path, reuse):
    journal, _, store, tail = create(tmp_path)
    owners = ProcessProjectionReads(
        root=tmp_path / "projection",
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        maximum_database_bytes=8 * 1024**3,
        cpus=(min(os.sched_getaffinity(0)),),
        reuse_connections=reuse,
    )
    try:
        owners.start()
        process = owners._owners[0].process
        original_pid = process.pid
        process.terminate()
        process.join(3)
        for _ in range(2):
            with pytest.raises(OrderProjectionError):
                owners.events_after(SCOPE, 0)
        assert owners.stats()["failed"]
        assert process.pid == original_pid and owners.stats()["calls"] == 0
        assert journal.read_bound().committed_lsn == 0
    finally:
        owners.close()
        tail.close()
        store.close()
        journal.close()

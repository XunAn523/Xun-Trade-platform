import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Thread
from unittest.mock import patch

import pytest
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStale,
    OrderProjectionStore,
)
from xuanling_tradecore.read_model.process_projection_reads import ProjectionReadSession

from test_order_projection_store import SCOPE, admit, create, drain


def session(tmp_path, journal):
    return ProjectionReadSession(
        root=tmp_path / "projection",
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        budget=8 * 1024**3,
    )


def test_ten_thousand_commands_reuse_one_connection_but_each_sees_fresh_commits(tmp_path):
    journal, book, store, tail = create(tmp_path)
    owner = session(tmp_path, journal)
    try:
        first = admit(book, "first")
        drain(store, tail)
        expected = store.events_after(SCOPE, 0)
        owner.call("events_after", (SCOPE, 0), {})
        descriptors = Path("/proc/self/fd")
        fd_count = len(tuple(descriptors.iterdir())) if descriptors.exists() else None
        with patch(
            "xuanling_tradecore.read_model.process_projection_reads.OrderProjectionStore",
            wraps=OrderProjectionStore,
        ) as constructor:
            for _ in range(10_000):
                assert owner.call("events_after", (SCOPE, 0), {}) == expected
            assert constructor.call_count == 0 and owner.connection_opens == 1
        if fd_count is not None:
            assert len(tuple(descriptors.iterdir())) == fd_count
        with pytest.raises(OrderProjectionStale):
            owner.call("events_after", (SCOPE, 0), {"minimum_watermark": 4})
        assert not owner._failed and not owner._store._db.in_transaction
        admit(book, "second")
        drain(store, tail)
        assert len(owner.call("events_after", (SCOPE, 0), {"minimum_watermark": 4}).events) == 2
        assert owner.connection_opens == 1
        assert (
            owner.call("lookup", (("foreign", *SCOPE[1:]), first.order.order.oms_order_id), {})
            is None
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(OrderProjectionError):
                pool.submit(owner.call, "events_after", (SCOPE, 0), {}).result()
        assert len(owner.call("events_after", (SCOPE, 0), {}).events) == 2
    finally:
        owner.close()
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("corruption", ["schema", "schema_cookie", "hash", "scope"])
def test_schema_or_row_corruption_poison_session_without_reopen(tmp_path, corruption):
    journal, book, store, tail = create(tmp_path)
    owner = session(tmp_path, journal)
    try:
        admit(book)
        drain(store, tail)
        owner.call("events_after", (SCOPE, 0), {})
        if corruption == "schema":
            store._db.execute("PRAGMA user_version=99")
        elif corruption == "schema_cookie":
            store._db.execute("CREATE TABLE unexpected(value)")
        elif corruption == "scope":
            store._db.execute(
                "UPDATE meta SET body=replace(CAST(body AS TEXT),?,?)",
                (journal.profile_hash, "f" * 64),
            )
        else:
            store._db.execute("UPDATE events SET digest=zeroblob(32)")
        store._db.commit()
        with pytest.raises(OrderProjectionError):
            owner.call("events_after", (SCOPE, 0), {})
        assert owner._closed and owner._failed and owner.connection_opens == 1
        store._db.execute("PRAGMA user_version=3")
        store._db.commit()
        with pytest.raises(OrderProjectionError):
            owner.call("events_after", (SCOPE, 0), {})
        assert owner.connection_opens == 1
    finally:
        owner.close()
        tail.close()
        store.close()
        journal.close()


@pytest.mark.skipif(sys.platform != "linux", reason="open inode replacement fixture")
def test_inode_replacement_is_not_an_implicit_generation_switch(tmp_path):
    journal, book, store, tail = create(tmp_path)
    owner = session(tmp_path, journal)
    replacement = None
    try:
        admit(book)
        drain(store, tail)
        owner.call("events_after", (SCOPE, 0), {})
        replacement = OrderProjectionStore(
            tmp_path / "next",
            journal_id=journal.journal_id,
            profile_hash=journal.profile_hash,
            initialize=True,
        )
        replacement.close()
        store.close()
        os.replace(tmp_path / "projection/orders.sqlite", tmp_path / "old.sqlite")
        os.replace(tmp_path / "next/orders.sqlite", tmp_path / "projection/orders.sqlite")
        with pytest.raises(OrderProjectionError):
            owner.call("events_after", (SCOPE, 0), {})
        assert owner.connection_opens == 1
        fresh = session(tmp_path, journal)
        try:
            with pytest.raises(OrderProjectionStale):
                fresh.call("events_after", (SCOPE, 0), {"minimum_watermark": 2})
        finally:
            fresh.close()
    finally:
        owner.close()
        tail.close()
        store.close()
        journal.close()
        if replacement:
            replacement.close()


@pytest.mark.skipif(sys.platform != "linux", reason="private POSIX filesystem fixtures")
@pytest.mark.parametrize("mutation", ["symlink", "permissions"])
def test_file_safety_is_rechecked_on_every_command(tmp_path, mutation):
    journal, book, store, tail = create(tmp_path)
    owner = session(tmp_path, journal)
    path = tmp_path / "projection/orders.sqlite"
    try:
        admit(book)
        drain(store, tail)
        owner.call("events_after", (SCOPE, 0), {})
        if mutation == "permissions":
            path.chmod(0o644)
        else:
            moved = tmp_path / "old.sqlite"
            path.rename(moved)
            path.symlink_to(moved)
        with pytest.raises((ValueError, OrderProjectionError)):
            owner.call("events_after", (SCOPE, 0), {})
        assert owner._failed and owner._closed
        with pytest.raises(OrderProjectionError):
            owner.call("events_after", (SCOPE, 0), {})
    finally:
        if not path.is_symlink():
            path.chmod(0o600)
        owner.close()
        tail.close()
        store.close()
        journal.close()


def test_session_does_not_pin_reader_snapshot_or_reject_concurrent_valid_commits(tmp_path):
    journal, book, store, tail = create(tmp_path)
    owner = session(tmp_path, journal)
    done, opened = Event(), Event()
    errors = []
    worker = None
    try:
        for index in range(100):
            admit(book, f"concurrent-{index}")
        batches = []
        while (batch := tail.read_next()) is not None:
            batches.append(batch)
        store.close()
        owner.call("events_after", (SCOPE, 0), {})

        def write():
            writer = None
            try:
                writer = OrderProjectionStore(
                    tmp_path / "projection",
                    journal_id=journal.journal_id,
                    profile_hash=journal.profile_hash,
                )
                opened.set()
                for batch in batches:
                    writer.apply(batch)
            except BaseException as error:
                errors.append(error)
            finally:
                if writer is not None:
                    writer.close()
                done.set()

        worker = Thread(target=write)
        worker.start()
        assert opened.wait(5)
        previous = 0
        for _ in range(1000):
            page = owner.call("events_after", (SCOPE, 0), {})
            assert page.watermark >= previous
            previous = page.watermark
            assert not owner._store._db.in_transaction
        assert done.wait(15)
        worker.join(1)
        assert not errors
        page = owner.call("events_after", (SCOPE, 0), {})
        assert len(page.events) == 100 and owner.connection_opens == 1
    finally:
        if worker is not None:
            worker.join(20)
        owner.close()
        tail.close()
        store.close()
        journal.close()

from __future__ import annotations

import json
from pathlib import Path
from threading import Event, Thread
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.control_root_v2 import ConsumerControlRootError
from xuanling_tradecore.consumer.runtime_identity import PersistentConsumerRuntimeIdentity
from xuanling_tradecore.consumer.snapshot_cursor_v2 import (
    SnapshotCursorClaimsV2,
    SnapshotCursorErrorV2,
)


def identity(root: Path, create: bool = False) -> PersistentConsumerRuntimeIdentity:
    return PersistentConsumerRuntimeIdentity(
        root=root, scope_sha256="a" * 64, create=create, cursor_lifetime_ns=1000
    )


def event_token(runtime: PersistentConsumerRuntimeIdentity) -> str:
    return runtime.event_codec().issue(
        consumer_id="c",
        consumer_account_id="a",
        journal_id=runtime.journal_id,
        stream_generation=1,
        after_position=None,
        authorization_epoch=1,
        issued_at_wall_ns=1000,
        expires_at_wall_ns=2000,
    )


def page_token(runtime: PersistentConsumerRuntimeIdentity) -> str:
    return runtime.snapshot_codec().issue(
        SnapshotCursorClaimsV2(
            "c",
            "a",
            1,
            "snapshot",
            runtime.journal_id,
            1,
            "position",
            0,
            100,
            1000,
            2000,
            runtime.snapshot_key.key_id,
        )
    )


def test_identity_and_both_cursor_signatures_survive_restart(tmp_path: Path) -> None:
    root = tmp_path / "identity"
    runtime = identity(root, True)
    journal = runtime.journal_id
    event, page = event_token(runtime), page_token(runtime)
    runtime.close()
    reopened = identity(root)
    try:
        assert reopened.journal_id == journal
        assert reopened.registry.current.journal_id == journal
        assert reopened.event_codec().verify(event, evaluated_wall_ns=1200).journal_id == journal
        assert reopened.snapshot_codec().verify(page, evaluated_wall_ns=1200).journal_id == journal
    finally:
        reopened.close()


def test_rotation_persists_grace_and_cache_cannot_bypass_key_expiry(tmp_path: Path) -> None:
    root = tmp_path / "identity"
    runtime = identity(root, True)
    event, page = event_token(runtime), page_token(runtime)
    runtime.rotate(now_wall_ns=1100, grace_ns=1000)
    runtime.close()
    reopened = identity(root)
    try:
        codec = reopened.snapshot_codec()
        assert codec.verify(page, evaluated_wall_ns=1200)
        assert reopened.event_codec().verify(event, evaluated_wall_ns=1200)
        with pytest.raises(SnapshotCursorErrorV2):
            codec.verify(page, evaluated_wall_ns=2100)
    finally:
        reopened.close()


def test_rotation_io_does_not_block_readers_or_publish_a_mixed_key_generation(tmp_path):
    runtime = identity(tmp_path / "identity", True)
    old_event, old_page = runtime.event_codec(), runtime.snapshot_codec()
    token = event_token(runtime)
    entered, release = Event(), Event()
    persist = runtime._persist
    failures = []

    def slow_persist():
        entered.set()
        assert release.wait(5)
        persist()

    def rotate():
        try:
            runtime.rotate(now_wall_ns=1100, grace_ns=2000)
        except BaseException as exc:
            failures.append(exc)

    thread = Thread(target=rotate)
    try:
        with patch.object(runtime, "_persist", side_effect=slow_persist):
            thread.start()
            assert entered.wait(2)
            assert runtime.event_codec() is old_event
            assert runtime.snapshot_codec() is old_page
            assert runtime.event_codec().verify(token, evaluated_wall_ns=1200)
            with pytest.raises(ConsumerControlRootError, match="in progress"):
                runtime.rotate(now_wall_ns=1100, grace_ns=2000)
            release.set()
            thread.join(timeout=5)
        assert not thread.is_alive() and not failures
        assert runtime.event_codec() is not old_event
        assert runtime.snapshot_codec() is not old_page
        assert runtime.event_codec().verify(token, evaluated_wall_ns=1200)
    finally:
        release.set()
        thread.join(timeout=5)
        runtime.close()


@pytest.mark.parametrize("damage", ["scope", "truncate", "key"])
def test_existing_damaged_identity_is_never_regenerated(tmp_path: Path, damage: str) -> None:
    root = tmp_path / "identity"
    identity(root, True).close()
    path = root / "runtime-identity.json"
    data = json.loads(path.read_text())
    if damage == "scope":
        data["scope_sha256"] = "b" * 64
    elif damage == "key":
        data["event_keys"][0]["secret_b64"] = "bad!"
    path.write_text("{" if damage == "truncate" else json.dumps(data))
    with pytest.raises(ConsumerControlRootError):
        identity(root, True)


def test_missing_identity_and_live_second_owner_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "identity"
    with pytest.raises(ConsumerControlRootError):
        identity(root)
    runtime = identity(root, True)
    try:
        with pytest.raises(OSError):
            identity(root)
        with pytest.raises(ConsumerControlRootError):
            runtime.rotate(now_wall_ns=1100, grace_ns=999)
    finally:
        runtime.close()


def test_unknown_metadata_commit_result_blocks_further_use(tmp_path: Path) -> None:
    runtime = identity(tmp_path / "identity", True)
    try:
        with patch(
            "xuanling_tradecore.consumer.runtime_identity.atomic_runtime_json",
            side_effect=OSError("injected"),
        ):
            with pytest.raises(OSError):
                runtime.rotate(now_wall_ns=1100, grace_ns=1000)
        with pytest.raises(ConsumerControlRootError, match="requires recovery"):
            runtime.event_codec()
    finally:
        runtime.close()

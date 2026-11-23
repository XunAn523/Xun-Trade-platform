"""Cross-copy ABI and disarmed deployment regression tests, without QMT."""

from __future__ import annotations

import ast
import ctypes
import json
import runpy
import sqlite3
import struct
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from xuanling_tradecore.adapters.qmt.native_engine_contract import (
    RING_ABI_VERSION,
    QmtNativeContractError,
    QmtNativeFrameKind,
    QmtRingHeaderV1,
)
from xuanling_tradecore.adapters.qmt.windows_ring import (
    QmtNativeAtomicLibrary,
    QmtNativeRing,
)

DEPLOY = Path(__file__).resolve().parents[1] / "deploy" / "windows-qmt-edge"


def load_engine():
    def frequency(pointer):
        pointer._obj.value = 1_000_000_000
        return 1

    kernel = SimpleNamespace(QueryPerformanceFrequency=frequency)
    with patch.object(ctypes, "WinDLL", return_value=kernel, create=True):
        return runpy.run_path(str(DEPLOY / "qmt_native_engine.py"))


class TestAtomic:
    __test__ = False

    def is_aligned(self, pointer):
        return ctypes.addressof(pointer) % 8 == 0

    def get(self, pointer):
        assert ctypes.addressof(pointer) % 8 == 0
        return pointer.value

    def set(self, pointer, value):
        assert ctypes.addressof(pointer) % 8 == 0
        pointer.value = value


@pytest.mark.parametrize("layout", ["<8sHH32s16sIIQQQQ", "<8sHH32s16sII4xQQQQ"])
@pytest.mark.parametrize("counters", [(0, 0, 0, 0), (7, 3, 100, 200)])
def test_both_abi1_layouts_are_rejected_without_modifying_evidence(tmp_path, layout, counters):
    raw = bytearray(128 + 8 * 512)
    struct.pack_into(layout, raw, 0, b"TCQMT001", 1, 0, b"s" * 32, b"g" * 16, 512, 8, *counters)
    path = tmp_path / "legacy.ring"
    path.write_bytes(raw)
    with pytest.raises(QmtNativeContractError, match="ABI"):
        QmtNativeRing.open_file(
            path,
            expected_scope_digest=b"s" * 32,
            expected_generation=b"g" * 16,
            test_only_unsafe_fallback=True,
        )
    engine = load_engine()
    with pytest.raises(RuntimeError, match="ABI"):
        engine["_Ring"](str(path), b"s" * 32, b"g" * 16, TestAtomic())
    assert path.read_bytes() == raw
    # Windows also verifies that rejection released the mmap/file handles.
    path.unlink()


def test_embedded_producer_and_edge_consumer_share_aligned_abi_through_wrap(tmp_path):
    engine = load_engine()
    engine["_Ring"].write.__globals__["_now"] = lambda: 123
    assert engine["ABI"] == RING_ABI_VERSION == 2
    path = tmp_path / "event.ring"
    ring = QmtNativeRing.create_file(
        path, QmtRingHeaderV1(b"s" * 32, b"g" * 16, 512, 2), test_only_unsafe_fallback=True
    )
    embedded = engine["_Ring"](str(path), b"s" * 32, b"g" * 16, TestAtomic())
    try:
        for sequence in range(1, 10):
            embedded.write(31, b"r" * 32, b"i" * 32, (sequence, 0, 0, 0), b"PROBE_INIT")
            frame = ring.consume()
            assert frame.sequence == sequence
            assert frame.kind == QmtNativeFrameKind.HEALTH
            assert frame.payload == b"PROBE_INIT"
        assert ring.snapshot().producer_sequence == ring.snapshot().consumer_sequence == 9
    finally:
        embedded.close()
        ring.close()


def test_edge_atomic_rejects_misalignment_before_native_call():
    library = object.__new__(QmtNativeAtomicLibrary)
    called = []
    library._aligned = lambda _: called.append("aligned") or 1
    library._load = lambda _: called.append("load") or 0
    library._store = lambda *_: called.append("store") or 0
    buffer = (ctypes.c_char * 24)()
    offset = next(i for i in range(8) if (ctypes.addressof(buffer) + i) % 8)
    pointer = ctypes.c_longlong.from_buffer(buffer, offset)
    with pytest.raises(QmtNativeContractError, match="aligned"):
        library.load(pointer)
    with pytest.raises(QmtNativeContractError, match="aligned"):
        library.store(pointer, 1)
    assert called == []


def test_create_ring_never_overwrites_existing_evidence(tmp_path):
    path = tmp_path / "event.ring"
    path.write_bytes(b"evidence")
    with pytest.raises(FileExistsError):
        QmtNativeRing.create_file(
            path, QmtRingHeaderV1(b"s" * 32, b"g" * 16, 512, 2), test_only_unsafe_fallback=True
        )
    assert path.read_bytes() == b"evidence"


def legacy_config(tmp_path):
    config = json.loads((DEPLOY / "edge_config.example.json").read_text())
    config.update(
        scope_digest_hex="73" * 32,
        generation_hex="67" * 16,
        command_ring_path=str(tmp_path / "command.ring"),
        event_ring_path=str(tmp_path / "event.ring"),
        database_path=str(tmp_path / "edge.sqlite"),
        atomic_dll_path=str(tmp_path / "atomic.dll"),
        qmt_timer_period="100nMilliSecond",
        ring_capacity=2,
    )
    raw = bytearray(128 + 2 * 512)
    struct.pack_into(
        "<8sHH32s16sIIQQQQ", raw, 0, b"TCQMT001", 1, 0, b"s" * 32, b"g" * 16, 512, 2, 0, 0, 0, 0
    )
    for name in ("command_ring_path", "event_ring_path"):
        Path(config[name]).write_bytes(raw)
    connection = sqlite3.connect(config["database_path"])
    connection.execute("CREATE TABLE requests (request_id TEXT)")
    connection.commit()
    connection.close()
    source = tmp_path / "edge.json"
    source.write_text(json.dumps(config))
    return source, config


def test_upgrade_prepares_new_generation_without_touching_runtime_or_database(tmp_path):
    prepare = runpy.run_path(str(DEPLOY / "prepare_probe_ring_upgrade.py"))["prepare"]
    source, config = legacy_config(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.iterdir()}
    output = tmp_path / "edge-abi2.json"
    assert prepare(source, output)["status"] == "PROBE_UPGRADE_CONFIG_PREPARED"
    prepared = json.loads(output.read_text())
    assert prepared["generation_hex"] != config["generation_hex"]
    assert prepared["database_path"] == config["database_path"]
    assert prepared["scope_digest_hex"] == config["scope_digest_hex"]
    for name in ("command_ring_path", "event_ring_path"):
        assert prepared[name] != config[name]
        assert not Path(prepared[name]).exists()
    for path, content in before.items():
        assert path.read_bytes() == content
    with pytest.raises(FileExistsError):
        prepare(source, output)


def test_embedded_files_remain_python36_compatible_and_engine_uses_lf():
    for path in [DEPLOY / "qmt_native_engine.py", *(DEPLOY / "bigqmt_strategy").glob("*.py")]:
        ast.parse(path.read_text(encoding="utf-8"), feature_version=(3, 6))
    assert b"\r" not in (DEPLOY / "qmt_native_engine.py").read_bytes()


def test_upgrade_refuses_a_database_with_command_history(tmp_path):
    prepare = runpy.run_path(str(DEPLOY / "prepare_probe_ring_upgrade.py"))["prepare"]
    source, config = legacy_config(tmp_path)
    connection = sqlite3.connect(config["database_path"])
    connection.execute("INSERT INTO requests VALUES ('previous-command')")
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="command history"):
        prepare(source, tmp_path / "edge-abi2.json")
    assert not (tmp_path / "edge-abi2.json").exists()

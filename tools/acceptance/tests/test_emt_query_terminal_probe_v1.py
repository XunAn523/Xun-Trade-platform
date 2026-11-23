from __future__ import annotations

import json

from tools.acceptance.emt_query_terminal_probe_v1 import (
    _callback_shape,
    _make_probe_api,
    _PendingQuery,
    _persist_report,
    _record_progress,
    _terminal_success,
    _wait_for_terminal,
)
from xuanling_tradecore.compatibility.emt_trading_platform import (
    OFFICIAL_TRADER_SPI,
    python_callback_name,
)


def test_callback_shape_keeps_only_terminal_correlation_scalars() -> None:
    shape = _callback_shape(
        ({"account": "must-not-be-copied"}, {"error_id": 0, "error_msg": "hidden"}, 71, True, 99),
        request_id=71,
        session_id=99,
    )
    assert shape == {
        "arity": 5,
        "data_mapping_present": True,
        "error_id": 0,
        "request_id_matches": True,
        "is_last": True,
        "session_id_matches": True,
    }


def test_nonterminal_or_mismatched_callback_cannot_pass() -> None:
    pending = _PendingQuery(request_id=71, session_id=99)
    pending.callback_count = 1
    pending.matching_callback_count = 1
    assert not _terminal_success(pending, 0)
    pending.terminal_callback_count = 1
    pending.final_error_id = 17
    assert not _terminal_success(pending, 0)
    pending.final_error_id = 0
    assert _terminal_success(pending, 0)


def test_progress_checkpoint_is_payload_free(tmp_path) -> None:
    _record_progress(tmp_path, "before_native_query")
    assert (tmp_path / "terminal-probe-progress.json").read_text(encoding="utf-8") == (
        '{"phase":"before_native_query"}\n'
    )


def test_terminal_polling_returns_without_a_callback() -> None:
    pending = _PendingQuery(request_id=71, session_id=99)
    assert not _wait_for_terminal(pending, 0.0)
    pending.done.set()
    assert _wait_for_terminal(pending, 0.0)


def test_terminal_probe_supplies_payload_free_handler_for_every_official_spi() -> None:
    class _NativeBase:
        pass

    probe_type = _make_probe_api(_NativeBase)
    for official_name in OFFICIAL_TRADER_SPI:
        assert callable(getattr(probe_type, python_callback_name(official_name), None))


def test_terminal_probe_report_is_payload_free_and_persisted_before_wrapper_exit(tmp_path) -> None:
    output = tmp_path / "result.json"
    _persist_report(output, {"result": {"api": "QueryIPOInfoList", "terminal_verified": False}})
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "result": {"api": "QueryIPOInfoList", "terminal_verified": False}
    }

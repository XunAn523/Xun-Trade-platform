from types import MappingProxyType
from unittest.mock import patch

import pytest
from xuanling_tradecore.domain import _validation as validation


class OverriddenKey(str):
    def isprintable(self):
        raise RuntimeError("subclass validation must not be skipped")


def outcome(mapping, field_name):
    try:
        result = validation.freeze_text_mapping(mapping, field_name)
        return ("ok", tuple(result.items()))
    except Exception as error:
        return (type(error), str(error))


def test_only_known_fact_keys_are_prevalidated():
    assert validation._KNOWN_OMS_FACT_KEYS
    for key in validation._KNOWN_OMS_FACT_KEYS:
        assert type(key) is str
        assert validation.require_text(key, "key", max_length=64) == key


def test_fastpath_still_validates_every_value_and_unknown_key(monkeypatch):
    monkeypatch.setattr(validation, "_DIAGNOSTIC_KNOWN_FACT_KEYS", True)
    original = validation.require_text
    with patch.object(validation, "require_text", wraps=original) as checked:
        result = validation.freeze_text_mapping(
            {"symbol": "SH", "unknown": "other"}, "OMS fact attributes"
        )
    assert dict(result) == {"symbol": "SH", "unknown": "other"}
    assert [call.args[0] for call in checked.call_args_list] == ["SH", "unknown", "other"]


@pytest.mark.parametrize(
    "mapping",
    [
        {"intent_hash": "abc", "symbol": "\u4e2d\u6587"},
        {"intent_hash": ""},
        {"intent_hash": "\n"},
        {"unknown": "ok"},
        {"": "ok"},
        {"x" * 65: "ok"},
        {1: "ok"},
        MappingProxyType({"symbol": "SH"}),
        {"symbol": "y" * 257},
        {OverriddenKey("symbol"): "SH"},
        {str(i): "x" for i in range(33)},
    ],
)
@pytest.mark.parametrize("field_name", ["OMS fact attributes", "another mapping"])
def test_fastpath_matches_original_values_errors_and_copy(monkeypatch, mapping, field_name):
    monkeypatch.setattr(validation, "_DIAGNOSTIC_KNOWN_FACT_KEYS", False)
    before = outcome(mapping, field_name)
    monkeypatch.setattr(validation, "_DIAGNOSTIC_KNOWN_FACT_KEYS", True)
    assert outcome(mapping, field_name) == before

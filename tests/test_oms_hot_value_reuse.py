import uuid
from dataclasses import replace
from unittest.mock import patch

import pytest
from xuanling_tradecore.domain._validation import DomainValidationError, freeze_text_mapping
from xuanling_tradecore.oms.order_book import _uuid7, intent_hash_for

from test_tradecore_oms import _intent


@pytest.mark.parametrize("entropy", (0, 1, 2**74 - 1, 0x1545A3F041255ABCDEF))
def test_uuid7_keeps_full_entropy_timestamp_version_variant_with_one_draw(entropy):
    milliseconds = 1_793_000_000_123
    with (
        patch("xuanling_tradecore.oms.order_book.secrets.randbits", return_value=entropy) as draw,
        patch("xuanling_tradecore.oms.order_book.time.time_ns", return_value=milliseconds * 10**6),
    ):
        value = uuid.UUID(_uuid7())
    draw.assert_called_once_with(74)
    assert value.version == 7
    assert value.variant == uuid.RFC_4122
    assert value.int >> 80 == milliseconds
    assert (((value.int >> 64) & 0xFFF) << 62) | (value.int & ((1 << 62) - 1)) == entropy


@pytest.mark.parametrize("text", ('"\\/\t\x00', "ASCII", "\x7f", "中文", "é𝄞", "\ud800"))
@pytest.mark.parametrize("quantity", (1, 2**80))
def test_intent_hash_fast_encoding_is_byte_equivalent_to_original_reference(text, quantity):
    intent = replace(_intent(), quantity=quantity, execution_parameters={"tag": text, "a": "first"})
    current = intent_hash_for(intent)
    with patch("xuanling_tradecore.oms.order_book.msgspec.json.encode", side_effect=TypeError):
        assert intent_hash_for(intent) == current


def test_shared_empty_mapping_is_immutable_but_nonempty_mapping_is_still_validated_copied():
    first = freeze_text_mapping(None, "none")
    assert first is freeze_text_mapping({}, "empty")
    assert first is freeze_text_mapping(first, "frozen")
    with pytest.raises(TypeError):
        first["key"] = "value"
    original = {"key": "value"}
    frozen = freeze_text_mapping(original, "present")
    original["key"] = "changed"
    assert frozen["key"] == "value"
    with pytest.raises(DomainValidationError):
        freeze_text_mapping({"key": "\n"}, "bad")
    with pytest.raises(DomainValidationError):
        freeze_text_mapping({}, "bad-capacity", max_items=-1)

import json
from dataclasses import replace
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.oms import MmapOmsFactJournalError, MmapOmsFactJournalV1

from test_mmap_oms_fact_journal_v1 import _fact


@pytest.mark.parametrize("text", ("ASCII", '"\\/\t\x00', "中文", "é𝄞", "\ud800"))
@pytest.mark.parametrize("integer", (1, 2**80))
def test_full_frame_bytes_crc_and_recovered_payload_match_reference(tmp_path, text, integer):
    payload = json.dumps(
        {"text": text, "integer": integer}, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    fact = replace(_fact("seed"), recovery_payload=payload)
    parameters = dict(lsn=1, batch_id=1, ordinal=0, batch_size=1, fact=fact)
    fast = MmapOmsFactJournalV1.encode_uncommitted_frame(**parameters)
    # Force the legacy full-object parser, without changing the original Raw
    # bytes embedded into the frame or its CRC. No alternate recovery decoder.
    with patch(
        "xuanling_tradecore.oms.mmap_fact_journal_v1._RECOVERY_OBJECT_DECODER",
    ) as parser:
        parser.decode.side_effect = msgspec.DecodeError("reference parser")
        assert MmapOmsFactJournalV1.encode_uncommitted_frame(**parameters) == fast
    journal = MmapOmsFactJournalV1(path=tmp_path / "facts.mmap", journal_id="j", capacity=4)
    try:
        journal.append_batch((fact,))
        assert journal.scan().records[0].fact.recovery_payload == payload
    finally:
        journal.close()


@pytest.mark.parametrize("payload", (
    b"[]", b"null", b'{"a":[1,]}', b'{"a":NaN}', b'{"a":Infinity}',
    b'{"a":"\xff"}', b'{"a":["\xff"]}', '{"a":1}'.encode("utf-16"),
    b'\xef\xbb\xbf{"a":1}',
))
def test_invalid_object_or_encoding_never_commits_even_a_valid_batch_prefix(tmp_path, payload):
    journal = MmapOmsFactJournalV1(path=tmp_path / "facts.mmap", journal_id="j", capacity=4)
    try:
        with pytest.raises(MmapOmsFactJournalError):
            journal.append_batch((_fact("good"), replace(_fact("bad"), recovery_payload=payload)))
        scan = journal.scan()
        assert scan.clean and not scan.records
        assert journal.append_batch((_fact("after"),)).first_lsn == 1
    finally:
        journal.close()


def test_common_ascii_payload_is_validated_without_stdlib_object_construction(tmp_path):
    journal = MmapOmsFactJournalV1(path=tmp_path / "facts.mmap", journal_id="j", capacity=4)
    try:
        with patch(
            "xuanling_tradecore.oms.mmap_fact_journal_v1.json.loads",
            side_effect=AssertionError("reference parser used on common ASCII"),
        ):
            journal.append_batch((replace(_fact("seed"), recovery_payload=b'{"a":1}'),))
        assert journal.scan().clean
    finally:
        journal.close()

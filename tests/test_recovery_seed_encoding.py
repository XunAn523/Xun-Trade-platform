import json
from dataclasses import replace

import pytest
from xuanling_tradecore.oms.recovery import OmsRecoverySeedV1

from test_order_projection_store import admit, create


@pytest.mark.parametrize("text", ["ASCII", '"\\/\t\x00', "中文", "é𝄞", "\ud800", "z" * 256])
def test_seed_fast_build_preserves_existing_v1_canonical_hash_and_roundtrip(tmp_path, text):
    journal, book, store, tail = create(tmp_path)
    try:
        current = admit(book).order
        execution = replace(current.execution, execution_parameters={"tag": text, "a": "first"})
        seeded = OmsRecoverySeedV1.build(replace(current, execution=execution))
        reference = json.dumps(
            json.loads(seeded.encoded), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        assert seeded.encoded == reference
        assert OmsRecoverySeedV1.decode_and_verify(reference).encoded == reference
    finally:
        tail.close()
        store.close()
        journal.close()


def test_canonical_escape_compatibility_including_every_control_byte():
    from xuanling_tradecore.oms.recovery import _canonical_built_seed

    for code in range(128):
        value = {"z": None, "a": [chr(code), 0, -1, 2**80, True, {"x": chr(code)}]}
        reference = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        assert _canonical_built_seed(value) == reference

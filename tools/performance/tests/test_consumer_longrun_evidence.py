from dataclasses import replace
from hashlib import sha256
from unittest.mock import patch

import pytest
from perfbench.longrun_evidence import (
    RECORD,
    ConsumerEvidenceWriter,
    ConsumerTimingRecord,
    verify_consumer_evidence,
)


def record(n=0):
    return ConsumerTimingRecord(
        n, 1, 2, 3, 4, 5, 6, 100, n + 1, 0, 1, 0, 1, sha256(str(n).encode()).digest(), b"x" * 32
    )


def test_streaming_records_rotate_verify_and_never_accept_payload_fields(tmp_path):
    root = tmp_path / "raw"
    writer = ConsumerEvidenceWriter(root=root, segment_records=2)
    for n in range(7):
        writer.append(record(n))
    writer.close()
    assert verify_consumer_evidence(root) == (7, 7 * RECORD.size)
    assert len(list(root.glob("*.bin"))) == 4
    with pytest.raises(ValueError):
        writer.append(record(8))
    raw = b"".join(p.read_bytes() for p in root.glob("*.bin"))
    assert b"password" not in raw and b"cursor" not in raw
    path = root / "raw-000000.bin"
    path.write_bytes(b"tampered")
    with pytest.raises(ValueError):
        verify_consumer_evidence(root)


def test_quota_failure_is_retained_as_failure_not_a_valid_shorter_run(tmp_path):
    root = tmp_path / "raw"
    writer = ConsumerEvidenceWriter(root=root, segment_records=1, maximum_bytes=RECORD.size)
    writer.append(record())
    with pytest.raises(ValueError, match="quota"):
        writer.append(record(1))
    writer.close()
    with pytest.raises(ValueError, match="incomplete"):
        verify_consumer_evidence(root)


def test_flush_failure_never_seals_a_successful_manifest_or_leaks_file(tmp_path):
    root = tmp_path / "raw"
    writer = ConsumerEvidenceWriter(root=root)
    writer.append(record())
    handle = writer._file
    with patch("perfbench.longrun_evidence.os.fsync", side_effect=OSError("disk failure")):
        with pytest.raises(OSError):
            writer.close()
    assert handle.closed
    assert not (root / "manifest.json").exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"call_ns": 0},
        {"sent_ns": 2},
        {"sequence": True},
        {"journal_lsn": -1},
        {"request_digest": b"raw"},
        {"count": 2**32},
    ],
)
def test_record_validation_does_not_guess_missing_or_reverse_times(changes):
    with pytest.raises(ValueError):
        replace(record(), **changes).encode()

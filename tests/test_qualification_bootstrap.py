import os
from pathlib import Path

import pytest
from xuanling_tradecore.consumer.qualification_bootstrap import (
    initialize_new_qualification_runtime,
)


def parent(tmp_path):
    data = tmp_path / "data"
    data.mkdir(mode=0o700)
    return data / "persistent"


def test_only_absent_or_empty_runtime_initializes(tmp_path):
    root = parent(tmp_path)
    assert initialize_new_qualification_runtime(root) is True
    assert not root.exists()
    root.mkdir(mode=0o700)
    assert initialize_new_qualification_runtime(root) is True
    assert not list(root.iterdir())


def test_partial_runtime_is_not_silently_reset(tmp_path):
    root = parent(tmp_path)
    root.mkdir(mode=0o700)
    original = root / "partial-state"
    original.write_bytes(b"evidence")
    with pytest.raises(ValueError, match="partial"):
        initialize_new_qualification_runtime(root)
    assert original.read_bytes() == b"evidence"


def test_existing_source_enters_recovery_not_initialization(tmp_path):
    root = parent(tmp_path)
    root.mkdir(mode=0o700)
    (root / "identity").mkdir(mode=0o700)
    (root / "journal").mkdir(mode=0o700)
    manifest = root / "journal/segments.json"
    manifest.write_bytes(b"deliberately-invalid-to-be-verified-by-runtime")
    assert initialize_new_qualification_runtime(root) is False
    assert manifest.read_bytes().startswith(b"deliberately-invalid")


def test_relative_or_non_directory_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        initialize_new_qualification_runtime(Path("relative"))
    root = parent(tmp_path)
    root.write_text("not a directory")
    with pytest.raises(ValueError, match="unsafe"):
        initialize_new_qualification_runtime(root)


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and symlink gate")
def test_symlink_and_public_parent_are_rejected(tmp_path):
    root = parent(tmp_path)
    root.symlink_to(tmp_path / "missing", target_is_directory=True)
    with pytest.raises(ValueError, match="non-symlink"):
        initialize_new_qualification_runtime(root)
    root.unlink()
    root.parent.chmod(0o755)
    with pytest.raises(ValueError, match="private"):
        initialize_new_qualification_runtime(root)


def test_formal_unit_is_finite_manual_and_never_enables_sdk():
    unit = (
        Path(__file__).parents[1]
        / "deploy/systemd/xuanling-tradecore-longrun-qualification.service"
    ).read_text()
    assert "--initialize-if-new" in unit
    assert "--initialize " not in unit
    assert "KillMode=mixed" in unit and "TimeoutStopSec=75" in unit
    assert "RuntimeMaxSec=300" in unit and "Restart=no" in unit
    assert "Conflicts=xuanling-tradecore-consumer-uds.service" in unit
    assert "--role-cpus 64,65,66,67" in unit
    assert "[Install]" not in unit

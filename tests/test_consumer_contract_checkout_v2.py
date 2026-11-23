"""Consumer identities survive fresh Windows/Linux checkout and regeneration."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from tools.contracts import build_consumer_v2_golden as builder
from xuanling_tradecore.consumer.wire_v2 import load_contract_identity

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts" / "consumer" / "v2"


@pytest.mark.parametrize("autocrlf", ["true", "false"])
def test_fresh_checkout_preserves_declared_contract_identity(tmp_path, autocrlf):
    seed = tmp_path / "seed"
    seed.mkdir()
    shutil.copyfile(ROOT / ".gitattributes", seed / ".gitattributes")
    shutil.copytree(CONTRACT, seed / "contracts" / "consumer" / "v2")
    subprocess.run(["git", "init", "--quiet", str(seed)], check=True)
    subprocess.run(["git", "-C", str(seed), "-c", "core.autocrlf=false", "add", "."], check=True)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    subprocess.run(
        [
            "git",
            "-C",
            str(seed),
            "-c",
            "core.autocrlf=" + autocrlf,
            "checkout-index",
            "--all",
            "--prefix=" + checkout.as_posix() + "/",
        ],
        check=True,
    )
    copied = checkout / "contracts" / "consumer" / "v2"
    assert all(b"\r\n" not in path.read_bytes() for path in copied.rglob("*.json"))
    assert load_contract_identity(copied) == load_contract_identity(CONTRACT)


def test_rebuild_normalizes_old_worktrees_and_is_repeatable(tmp_path, monkeypatch):
    copied = tmp_path / "contracts"
    shutil.copytree(CONTRACT, copied)
    expected = load_contract_identity(CONTRACT)
    for path in copied.rglob("*.json"):
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    monkeypatch.setattr(builder, "CONTRACT_ROOT", copied)
    assert builder.main() == 0
    assert load_contract_identity(copied) == expected
    assert all(b"\r\n" not in path.read_bytes() for path in copied.rglob("*.json"))
    first = {path: path.read_bytes() for path in copied.rglob("*") if path.is_file()}
    assert builder.main() == 0
    assert all(path.read_bytes() == raw for path, raw in first.items())

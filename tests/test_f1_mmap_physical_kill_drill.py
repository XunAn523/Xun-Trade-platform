from __future__ import annotations

import platform
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from tools.acceptance.f1_mmap_physical_kill_drill import run


@pytest.mark.skipif(platform.system() != "Linux", reason="F1 physical SIGKILL drill is Linux-only")
def test_f1_mmap_physical_kill_drill_is_fail_closed_without_emt_io() -> None:
    with TemporaryDirectory() as temporary:
        report = run(root=Path(temporary))
    assert report["complete"]
    assert report["safety"] == {
        "linux_only": True,
        "sdk_loaded": False,
        "login": False,
        "query": False,
        "order_submit": False,
        "cancel_submit": False,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    assert len(report["cases"]) == 5
    assert all(
        item["child_terminated_by_sigkill"] and item["submit_rejected"] for item in report["cases"]
    )

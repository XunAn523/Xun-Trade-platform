from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.acceptance.r1_lifecycle_drill_v1 import run


def test_offline_lifecycle_drill_rejects_unsafe_transitions_and_stays_disarmed() -> None:
    with TemporaryDirectory() as directory:
        output = Path(directory) / "r1-lifecycle.json"
        report = run(output=output)
        persisted = json.loads(output.read_text(encoding="utf-8"))

    assert report == persisted
    assert report["conclusion"] == "OFFLINE_CONTROL_PLANE_PASS_DISARMED_NOT_READY"
    assert all(report["checks"].values())
    assert report["safety"]["emt_sdk_imported_before"] is False
    assert report["safety"]["emt_sdk_imported_after"] is False
    assert not report["safety"]["network_used"]
    assert not report["safety"]["broker_mutation"]

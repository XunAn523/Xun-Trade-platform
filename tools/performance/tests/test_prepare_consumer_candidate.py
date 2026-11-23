import json

import pytest
from tools.performance.prepare_consumer_candidate import prepare


def test_new_isolated_run_is_non_trading_and_not_a_qualification(tmp_path):
    root = tmp_path / "r"
    # Long pytest paths on Windows are not valid UNIX socket destinations;
    # filesystem creation rules and generated config are still tested there.
    from unittest.mock import patch

    with patch("tools.performance.prepare_consumer_candidate.os.fsencode", return_value=b"short"):
        result = prepare(run_root=root, peer_uid=1000, peer_gid=1000)
        assert result["measurement_started"] is False
        assert result["formal_qualification"] is False
        data = json.loads((root / "consumer.json").read_text())
        assert data["adapter_instance_id"] == "disabled-no-dispatch"
        assert data["runtime_profile"] == "LONG_RUNNING"
        assert data["max_requests_per_connection"] == 1
        assert data["journal_root"] == str(root / "data")
        with pytest.raises(ValueError):
            prepare(run_root=root, peer_uid=1000, peer_gid=1000)

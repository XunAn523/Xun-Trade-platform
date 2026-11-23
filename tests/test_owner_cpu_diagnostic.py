import pstats

import pytest
from xuanling_tradecore.consumer.owner_cpu_diagnostic import OwnerCpuDiagnostic


def test_cpu_profile_contains_aggregate_functions_and_never_overwrites(tmp_path):
    target = tmp_path / "owner.pstats"
    profile = OwnerCpuDiagnostic(target)
    profile.start()
    sum(range(1000))
    profile.close()
    stats = pstats.Stats(str(target))
    assert stats.total_calls > 0
    assert all(len(key) == 3 for key in stats.stats)
    second = OwnerCpuDiagnostic(target)
    second.start()
    with pytest.raises(FileExistsError):
        second.close()

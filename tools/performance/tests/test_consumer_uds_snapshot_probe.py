from __future__ import annotations

from consumer_uds_snapshot_probe import _bounded_queue_capacity, _percentile


def test_percentile_uses_nearest_rank() -> None:
    assert _percentile([], 0.95) is None
    assert _percentile([3.0, 1.0, 2.0, 4.0], 0.50) == 2.0
    assert _percentile([3.0, 1.0, 2.0, 4.0], 0.95) == 4.0
    assert _bounded_queue_capacity(32, 50) == 64

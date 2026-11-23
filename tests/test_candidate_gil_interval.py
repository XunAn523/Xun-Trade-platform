from __future__ import annotations

from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.long_running_candidate import configure_candidate_gil_switch


def test_default_interval_is_untouched_and_experiment_is_explicit():
    with patch(
        "xuanling_tradecore.consumer.long_running_candidate.sys.setswitchinterval"
    ) as setter:
        configure_candidate_gil_switch(None)
        setter.assert_not_called()
        for value in (100, 250, 500, 1000, 2000, 5000):
            configure_candidate_gil_switch(value)
            setter.assert_called_with(value / 1_000_000)


@pytest.mark.parametrize("value", [True, False, 0, -1, 99, 1500, 5001, 1000.0, "1000"])
def test_unreviewed_interval_is_rejected_before_changing_interpreter(value):
    with patch(
        "xuanling_tradecore.consumer.long_running_candidate.sys.setswitchinterval"
    ) as setter:
        with pytest.raises(ValueError):
            configure_candidate_gil_switch(value)
        setter.assert_not_called()

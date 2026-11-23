import pytest
from xuanling_tradecore.consumer.long_running_candidate import candidate_mock_batch_size


def test_candidate_mock_batch_is_bounded_and_defaults_to_existing_profile():
    assert candidate_mock_batch_size(None) == 128
    assert candidate_mock_batch_size("128") == 128
    assert candidate_mock_batch_size("32") == 32
    for value in ("", "1", "256", "032", "foo"):
        with pytest.raises(ValueError):
            candidate_mock_batch_size(value)

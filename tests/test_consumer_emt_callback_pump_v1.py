from threading import Event
from types import SimpleNamespace

import pytest
from xuanling_tradecore.consumer.emt_callback_pump_v1 import (
    EmtCallbackPumpErrorV1,
    ManagedEmtCallbackPumpV1,
)


def test_callback_failure_fences_target_and_latches_health():
    failed = Event()

    class Runtime:
        def drain_broker_events(self, *, wait_timeout_seconds):
            assert wait_timeout_seconds > 0
            raise RuntimeError("callback failure")

    runtime = Runtime()
    target = SimpleNamespace(
        worker=SimpleNamespace(runtime=runtime),
        on_execution_wakeup_failed=failed.set,
    )
    pump = ManagedEmtCallbackPumpV1(runtime=runtime, failure_target=target)
    pump.start()
    assert failed.wait(2)
    assert not pump.healthy()
    pump.close()
    with pytest.raises(EmtCallbackPumpErrorV1):
        pump.start()


def test_callback_target_must_share_exact_runtime():
    target = SimpleNamespace(worker=SimpleNamespace(runtime=object()))
    with pytest.raises(ValueError):
        ManagedEmtCallbackPumpV1(runtime=object(), failure_target=target)

"""Lifecycle guarantees independent of the vendor SDK and trading authority."""

import pytest
from xuanling_tradecore.consumer.emt_submit_composition_v1 import ManagedEmtExecutionSessionV1
from xuanling_tradecore.consumer.emt_submit_worker_v1 import EmtConsumerSubmitWorkerErrorV1
from xuanling_tradecore.consumer.service_v2 import ConsumerUnixServiceV2


class _Runtime:
    def __init__(self, *, fail_start=False, fail_stop=False):
        self.calls = []
        self.fail_start = fail_start
        self.fail_stop = fail_stop

    def start(self, observer, *, evaluated_wall_ns):
        self.calls.append(("start", observer, evaluated_wall_ns))
        if self.fail_start:
            raise RuntimeError("startup failed")

    def stop(self):
        self.calls.append(("stop",))
        if self.fail_stop:
            raise RuntimeError("shutdown failed")


def test_session_starts_once_and_closes_once():
    runtime = _Runtime()
    observer = object()
    session = ManagedEmtExecutionSessionV1(runtime=runtime, observer=observer, now_wall_ns=lambda: 7)
    assert session.runtime is runtime
    session.start()
    with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
        session.start()
    session.close()
    session.close()
    assert runtime.calls == [("start", observer, 7), ("stop",)]
    with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
        session.start()


@pytest.mark.parametrize("failure", ["start", "stop"])
def test_session_failure_is_latched_and_never_restarted(failure):
    runtime = _Runtime(fail_start=failure == "start", fail_stop=failure == "stop")
    session = ManagedEmtExecutionSessionV1(runtime=runtime, observer=object(), now_wall_ns=lambda: 7)
    if failure == "start":
        with pytest.raises(RuntimeError):
            session.start()
    else:
        session.start()
        with pytest.raises(RuntimeError):
            session.close()
    with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
        session.start()
    with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
        session.close()


def test_closing_unstarted_session_prevents_later_start():
    runtime = _Runtime()
    session = ManagedEmtExecutionSessionV1(runtime=runtime, observer=object(), now_wall_ns=lambda: 7)
    session.close()
    with pytest.raises(EmtConsumerSubmitWorkerErrorV1):
        session.start()
    assert runtime.calls == []


@pytest.mark.parametrize("owner_close_fails", [False, True])
def test_listener_start_failure_fences_owner_before_session_release(owner_close_fails):
    events = []

    class Runtime(_Runtime):
        def start(self, observer, *, evaluated_wall_ns):
            events.append("session-start")

        def stop(self):
            events.append("session-stop")

    class Owner:
        def start(self):
            events.append("owner-start")

        def close(self):
            events.append("owner-close")
            if owner_close_fails:
                raise RuntimeError("owner did not stop")

    class Listener:
        supports_request_reactor = True

        def start(self):
            events.append("listener-start")
            raise RuntimeError("listener failed")

    session = ManagedEmtExecutionSessionV1(runtime=Runtime(), observer=object(), now_wall_ns=lambda: 7)
    service = ConsumerUnixServiceV2(
        Listener(), request_reactor=True, managed_components=(session, Owner())
    )
    with pytest.raises(RuntimeError):
        service.start()
    assert events[:4] == ["session-start", "owner-start", "listener-start", "owner-close"]
    assert ("session-stop" in events) is not owner_close_fails

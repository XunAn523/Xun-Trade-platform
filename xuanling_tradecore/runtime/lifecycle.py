"""Fail-closed runtime lifecycle orchestration.

This module owns process shutdown ordering only.  It deliberately does not
construct adapters or move account state to READY: composition supplies the
already-acquired ownership fences and the bounded worker participants.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from enum import Enum

from ..domain._validation import require_nonnegative, require_text


class RuntimeLifecycleStateV1(str, Enum):
    CREATED = "CREATED"
    RUNNING_DISARMED = "RUNNING_DISARMED"
    STOPPING_DISARMED = "STOPPING_DISARMED"
    STOPPED_DISARMED = "STOPPED_DISARMED"
    FAILED_DISARMED = "FAILED_DISARMED"


class RuntimeLifecycleErrorV1(RuntimeError):
    """A lifecycle transition was unsafe or failed while stopping."""


class RuntimeShutdownCoordinatorV1:
    """Stop admission before workers, then release ownership only at the end."""

    def __init__(
        self,
        *,
        stop_new_admissions: Callable[[str], None],
        drain_workers: Sequence[Callable[[int], None]],
        release_ownership_fences: Sequence[Callable[[], None]],
    ) -> None:
        self._stop_new_admissions = stop_new_admissions
        self._drain_workers = tuple(drain_workers)
        self._release_ownership_fences = tuple(release_ownership_fences)
        self._state = RuntimeLifecycleStateV1.CREATED

    @property
    def state(self) -> RuntimeLifecycleStateV1:
        return self._state

    def start_disarmed(self) -> None:
        """Enter the only automatic start state; no account is armed here."""

        if self._state is not RuntimeLifecycleStateV1.CREATED:
            raise RuntimeLifecycleErrorV1("runtime can start only once")
        self._state = RuntimeLifecycleStateV1.RUNNING_DISARMED

    def request_stop(self, *, reason: str, deadline_monotonic_ns: int) -> None:
        """Execute the bounded stop chain, retaining the disarmed state on failure."""

        reason = require_text(reason, "runtime stop reason", max_length=160)
        require_nonnegative(deadline_monotonic_ns, "runtime shutdown deadline")
        if self._state is RuntimeLifecycleStateV1.STOPPED_DISARMED:
            return
        if self._state is not RuntimeLifecycleStateV1.RUNNING_DISARMED:
            raise RuntimeLifecycleErrorV1("runtime is not running in the disarmed state")

        self._state = RuntimeLifecycleStateV1.STOPPING_DISARMED
        failure: BaseException | None = None
        try:
            self._stop_new_admissions(reason)
            for drain_worker in self._drain_workers:
                drain_worker(deadline_monotonic_ns)
        except BaseException as exc:
            failure = exc
        finally:
            for release_fence in self._release_ownership_fences:
                try:
                    release_fence()
                except BaseException as exc:
                    if failure is None:
                        failure = exc
        if failure is not None:
            self._state = RuntimeLifecycleStateV1.FAILED_DISARMED
            raise RuntimeLifecycleErrorV1("runtime shutdown chain failed") from failure
        self._state = RuntimeLifecycleStateV1.STOPPED_DISARMED

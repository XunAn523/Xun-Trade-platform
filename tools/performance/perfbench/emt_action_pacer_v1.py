"""Conservative single-process EMT order/cancel action pacing.

This is not an account-wide or interprocess rate limiter. It is suitable only
when an external ownership fence guarantees that all account-side order and
cancel actions enter this one pacer. It does not schedule or submit orders.
"""

from __future__ import annotations

from threading import Lock
from time import monotonic_ns, sleep
from typing import Callable, TypeVar

_T = TypeVar("_T")


class EmtActionPacerErrorV1(RuntimeError):
    """The isolated action lane cannot prove its local cadence."""


class EmtActionPacerV1:
    """Serialize actions and separate their starts by at least 1e9/rate ns.

    The next action is measured from the *previous call's return*, not its
    intended wake-up time. Thus delayed or preempted calls cannot catch up in
    a burst. A failed action also consumes its slot. This conservative rule
    may achieve less than the configured rate and never promises 4 seconds.
    """

    def __init__(
        self,
        *,
        actions_per_second: int,
        now_ns: Callable[[], int] = monotonic_ns,
        sleep_seconds: Callable[[float], None] = sleep,
    ) -> None:
        if type(actions_per_second) is not int or not 1 <= actions_per_second <= 499:
            raise ValueError("EMT action rate must be in 1..499 per second")
        self._gap_ns = (1_000_000_000 + actions_per_second - 1) // actions_per_second
        self._clock = now_ns
        self._sleep = sleep_seconds
        self._lock = Lock()
        self._last_finished_ns: int | None = None
        self._failed = False

    def call(self, action: Callable[[], _T]) -> _T:
        """Perform exactly one action; never retry an exception or rejection."""

        with self._lock:
            if self._failed:
                raise EmtActionPacerErrorV1("EMT action clock is untrusted")
            now = self._read_clock()
            if self._last_finished_ns is not None:
                target = self._last_finished_ns + self._gap_ns
                while now < target:
                    self._sleep((target - now) / 1_000_000_000)
                    later = self._read_clock()
                    if later <= now:
                        self._failed = True
                        raise EmtActionPacerErrorV1("EMT pacing clock did not advance")
                    now = later
            try:
                return action()
            finally:
                finished = self._read_clock()
                if finished < now:
                    self._failed = True
                    raise EmtActionPacerErrorV1("EMT pacing clock moved backwards")
                self._last_finished_ns = finished

    def _read_clock(self) -> int:
        value = self._clock()
        if type(value) is not int or value < 0 or (
            self._last_finished_ns is not None and value < self._last_finished_ns
        ):
            self._failed = True
            raise EmtActionPacerErrorV1("EMT pacing clock is invalid")
        return value

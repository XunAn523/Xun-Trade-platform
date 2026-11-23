"""Explicit fixed role CPUs, validated once before candidate threads exist."""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import Callable, Generator, ParamSpec, TypeVar, cast

P = ParamSpec("P")
T = TypeVar("T")


def validate_role_cpus(
    cpus: tuple[int, ...] | None,
    read_cpus: tuple[int, ...] | None = None,
    background_cpus: tuple[int, ...] | None = None,
) -> None:
    if cpus is None:
        if read_cpus is not None or background_cpus is not None:
            raise ValueError("read owners require explicit fixed role CPUs")
        return
    if len(cpus) not in (3, 4) or any(type(cpu) is not int or cpu < 0 for cpu in cpus):
        raise ValueError(
            "candidate requires reactor, projection, cold and optional segment CPU IDs"
        )
    if read_cpus is not None:
        if not 1 <= len(read_cpus) <= 2 or any(
            type(cpu) is not int or cpu < 0 for cpu in read_cpus
        ):
            raise ValueError("candidate supports one or two fixed read owner CPUs")
        cpus = (*cpus, *read_cpus)
    if background_cpus is not None:
        if not 1 <= len(background_cpus) <= 2 or any(
            type(cpu) is not int or cpu < 0 for cpu in background_cpus
        ):
            raise ValueError("candidate supports one or two fixed background CPUs")
        cpus = (*cpus, *background_cpus)
    allowed = os.sched_getaffinity(0)
    if not set(cpus) <= allowed:
        raise ValueError("candidate role CPU is outside its existing allowed set")
    physical: list[tuple[int, int]] = []
    for cpu in cpus:
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        physical.append(
            (int((root / "physical_package_id").read_text()), int((root / "core_id").read_text()))
        )
    if len(set(physical)) != len(cpus):
        raise ValueError("candidate roles overlap physical/SMT cores")


def set_thread_affinity_exact(cpus: set[int]) -> None:
    os.sched_setaffinity(0, cpus)
    if os.sched_getaffinity(0) != cpus:
        raise RuntimeError("candidate CPU grant changed or a role CPU is unavailable")


@contextmanager
def temporary_thread_affinity(cpus: tuple[int, ...] | None) -> Generator[None, None, None]:
    if cpus is None:
        yield
        return
    previous = os.sched_getaffinity(0)
    try:
        set_thread_affinity_exact(set(cpus))
        yield
    finally:
        set_thread_affinity_exact(previous)


def preserve_candidate_affinity(method: Callable[P, T]) -> Callable[P, T]:
    """A failed constructor must not leave its caller pinned to a role CPU.

    Optional background mode also restores the caller after successful setup;
    start/serve then enter their explicit fixed thread-role contexts.
    """

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        options = cast(dict[str, object], kwargs)
        if options.get("role_cpus") is None or sys.platform != "linux":
            return method(*args, **kwargs)
        previous = os.sched_getaffinity(0)
        try:
            result = method(*args, **kwargs)
        except BaseException:
            set_thread_affinity_exact(previous)
            raise
        if options.get("background_cpus") is not None:
            set_thread_affinity_exact(previous)
        return result

    return wrapped

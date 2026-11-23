"""Startup-frozen, bounded reactor candidate; no load-driven parameter changes."""

from __future__ import annotations

from dataclasses import dataclass, fields


@dataclass(frozen=True, slots=True)
class FixedReactorBudgets:
    accepts: int = 32
    reads: int = 64
    writes: int = 64
    ready: int = 24
    dispatches: int = 24
    phase_ns: int = 1_000_000
    dispatch_ns: int = 2_000_000
    read_bytes: int = 4 * 1024**2
    write_bytes: int = 4 * 1024**2

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            maximum = (
                128
                if field.name in {"accepts", "reads", "writes", "ready", "dispatches"}
                else (4 * 1024**2 if field.name.endswith("bytes") else 5_000_000)
            )
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("invalid fixed reactor budget")


@dataclass(slots=True)
class ReactorIoTurn:
    policy: FixedReactorBudgets
    accepts: int = 0
    accept_ns: int = 0
    reads: int = 0
    read_ns: int = 0
    read_bytes: int = 0
    writes: int = 0
    write_ns: int = 0
    write_bytes: int = 0

    def may_accept(self) -> bool:
        return self.accepts < self.policy.accepts and self.accept_ns < self.policy.phase_ns

    def receive_capacity(self) -> int:
        if self.reads >= self.policy.reads or self.read_ns >= self.policy.phase_ns:
            return 0
        return max(0, min(65536, self.policy.read_bytes - self.read_bytes))

    def send_capacity(self) -> int:
        if self.writes >= self.policy.writes or self.write_ns >= self.policy.phase_ns:
            return 0
        return max(0, self.policy.write_bytes - self.write_bytes)

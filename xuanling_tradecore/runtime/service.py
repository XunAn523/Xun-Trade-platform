"""Signal-aware, deliberately disarmed runtime process entry primitives."""

from __future__ import annotations

import os
import signal
import socket
from collections.abc import Callable
from threading import Event
from time import monotonic_ns

from .lifecycle import RuntimeShutdownCoordinatorV1


class SystemdWatchdogNotifierV1:
    """Best-effort systemd notify/watchdog sender outside every hot path."""

    def __init__(self) -> None:
        self._socket_path = os.environ.get("NOTIFY_SOCKET", "")
        raw_interval = os.environ.get("WATCHDOG_USEC", "")
        try:
            interval_us = int(raw_interval)
        except ValueError:
            interval_us = 0
        self._interval_seconds = max(0.1, interval_us / 2_000_000) if interval_us > 0 else 0.0

    @property
    def enabled(self) -> bool:
        return bool(self._socket_path and self._interval_seconds)

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    def ready(self) -> None:
        self._send("READY=1\nSTATUS=DISARMED")

    def ping(self) -> None:
        self._send("WATCHDOG=1\nSTATUS=DISARMED")

    def stopping(self) -> None:
        self._send("STOPPING=1\nSTATUS=STOPPING_DISARMED")

    def _send(self, payload: str) -> None:
        if not self._socket_path:
            return
        address = (
            "\0" + self._socket_path[1:] if self._socket_path.startswith("@") else self._socket_path
        )
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client:
                client.connect(address)
                client.send(payload.encode("ascii"))
        except OSError:
            # Watchdog delivery must never make the disarmed process tradable.
            return


class DisarmedRuntimeProcessV1:
    """Wait for SIGTERM/SIGINT without loading accounts, SDKs, or credentials."""

    def __init__(self, coordinator: RuntimeShutdownCoordinatorV1) -> None:
        self._coordinator = coordinator
        self._stop_requested = Event()

    def request_stop(self, _signal_number: int, _frame: object) -> None:
        self._stop_requested.set()

    def run(self, *, once: bool = False, wait: Callable[[], None] | None = None) -> None:
        self._coordinator.start_disarmed()
        watchdog = SystemdWatchdogNotifierV1()
        watchdog.ready()
        if not once:
            signal.signal(signal.SIGTERM, self.request_stop)
            signal.signal(signal.SIGINT, self.request_stop)
            if wait is None:
                while not self._stop_requested.wait(
                    timeout=watchdog.interval_seconds if watchdog.enabled else None
                ):
                    watchdog.ping()
            else:
                wait()
        watchdog.stopping()
        self._coordinator.request_stop(
            reason="process-stop", deadline_monotonic_ns=monotonic_ns() + 45_000_000_000
        )

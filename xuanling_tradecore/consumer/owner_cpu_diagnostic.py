"""Opt-in candidate-only thread CPU profiling, written only after shutdown."""

from __future__ import annotations

import cProfile
import marshal
import os
import pstats
from pathlib import Path
from time import thread_time_ns

from ..persistence.runtime_files import open_private


class OwnerCpuDiagnostic:
    """Collect aggregate function counts/times, never frame locals or payloads."""

    def __init__(self, output: Path) -> None:
        self._output = output
        self._profile = cProfile.Profile(timer=thread_time_ns, timeunit=1e-9)
        self._running = False

    def start(self) -> None:
        if self._running:
            raise RuntimeError("owner CPU diagnostic already running")
        self._profile.enable()
        self._running = True

    def close(self) -> None:
        if not self._running:
            raise RuntimeError("owner CPU diagnostic is not running")
        self._profile.disable()
        self._running = False
        stats = pstats.Stats(self._profile).stats
        descriptor = open_private(self._output, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        with os.fdopen(descriptor, "wb") as output:
            marshal.dump(stats, output)

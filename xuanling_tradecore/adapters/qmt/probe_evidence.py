"""Bounded disarmed lifecycle evidence, without QMT actions or activation.

Timer samples prove observed spacing, not official scheduled time or trading
capability. No QMT API, network or activation is available here.
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass, field

from .native_engine_contract import QmtNativeFrameKind, QmtNativeFrameV1


class QmtProbeEvidenceError(RuntimeError):
    """The probe failed to establish a fresh, continuous native lifecycle."""


def decode_durable_native_frame(payload: bytes) -> QmtNativeFrameV1:
    if len(payload) != 512 or payload[-8:] != b"\x00" * 8:
        raise QmtProbeEvidenceError("invalid durable native frame")
    sequence = struct.unpack_from("<Q", payload)[0]
    return QmtNativeFrameV1.decode_committed(
        payload[:-8] + struct.pack("<Q", sequence), expected_sequence=sequence
    )


@dataclass
class QmtProbeEvidence:
    min_timer_samples: int = 3
    max_timer_gap_ns: int = 5_000_000_000
    expect_stop: bool = False
    stop_quiet_ns: int = 1_000_000_000
    max_samples: int = 100_000
    init_qpc_ns: int | None = field(default=None, init=False)
    stop_received_ns: int | None = field(default=None, init=False)
    _last_sequence: int | None = field(default=None, init=False)
    _last_received_ns: int | None = field(default=None, init=False)
    _last_qpc_ns: int | None = field(default=None, init=False)
    _timer_ns: list[int] = field(default_factory=list[int], init=False)

    def __post_init__(self) -> None:
        if not 2 <= self.min_timer_samples <= self.max_samples or self.max_timer_gap_ns <= 0:
            raise QmtProbeEvidenceError("invalid timer evidence bounds")
        if self.stop_quiet_ns <= 0:
            raise QmtProbeEvidenceError("invalid stop observation bound")

    def observe(self, frame: QmtNativeFrameV1, *, received_ns: int) -> dict[str, object]:
        if self.stop_received_ns is not None:
            raise QmtProbeEvidenceError("native event arrived after MODEL_STOP")
        if self._last_sequence is not None and frame.sequence != self._last_sequence + 1:
            raise QmtProbeEvidenceError("native event sequence gap")
        if frame.kind != QmtNativeFrameKind.HEALTH:
            raise QmtProbeEvidenceError("native fault or non-health frame during PROBE")
        qpc_ns = frame.timestamps_ns[0]
        if qpc_ns <= 0 or (self._last_qpc_ns is not None and qpc_ns <= self._last_qpc_ns):
            raise QmtProbeEvidenceError("non-monotonic native QPC sample")
        self.check_alive(received_ns)
        tick: int | None = None
        if frame.payload == b"PROBE_INIT":
            if self.init_qpc_ns is not None:
                raise QmtProbeEvidenceError("multiple initializations in one probe window")
            self.init_qpc_ns = qpc_ns
            label = "PROBE_INIT"
        elif frame.payload.startswith(b"PROBE_TIMER") and len(frame.payload) == 19:
            if self.init_qpc_ns is None:
                raise QmtProbeEvidenceError("timer arrived without a fresh PROBE_INIT")
            tick = struct.unpack("<Q", frame.payload[11:])[0]
            if tick != len(self._timer_ns) + 1:
                raise QmtProbeEvidenceError("native timer counter gap")
            if len(self._timer_ns) >= self.max_samples:
                raise QmtProbeEvidenceError("timer evidence capacity exceeded")
            previous = self._timer_ns[-1] if self._timer_ns else self.init_qpc_ns
            if qpc_ns - previous > self.max_timer_gap_ns:
                raise QmtProbeEvidenceError("native timer gap exceeded")
            self._timer_ns.append(qpc_ns)
            label = "PROBE_TIMER"
        elif frame.payload == b"MODEL_STOP":
            if not self.expect_stop or not self._timer_ns:
                raise QmtProbeEvidenceError("unexpected or premature MODEL_STOP")
            self.stop_received_ns = received_ns
            label = "MODEL_STOP"
        else:
            raise QmtProbeEvidenceError("unrecognized PROBE health frame")
        self._last_sequence = frame.sequence
        self._last_received_ns = received_ns
        self._last_qpc_ns = qpc_ns
        return {
            "native_kind": int(frame.kind),
            "probe_label": label,
            "native_sequence": frame.sequence,
            "timer_tick": tick,
            "native_qpc_ns": qpc_ns,
            "received_monotonic_ns": received_ns,
            "scheduled_ns": None,
        }

    def check_alive(self, now_ns: int) -> None:
        if (
            self.stop_received_ns is None
            and self._last_received_ns is not None
            and now_ns - self._last_received_ns > self.max_timer_gap_ns
        ):
            raise QmtProbeEvidenceError("native timer stalled or monitor could not keep up")

    def finish(self, *, now_ns: int) -> dict[str, object]:
        self.check_alive(now_ns)
        if self.init_qpc_ns is None or len(self._timer_ns) < self.min_timer_samples:
            raise QmtProbeEvidenceError("fresh PROBE_INIT and sufficient timer samples required")
        if self.expect_stop and (
            self.stop_received_ns is None or now_ns - self.stop_received_ns < self.stop_quiet_ns
        ):
            raise QmtProbeEvidenceError("MODEL_STOP and its quiet observation window required")
        intervals = sorted(b - a for a, b in zip(self._timer_ns, self._timer_ns[1:]))

        def percentile(percent: int) -> int:
            return intervals[max(0, math.ceil(len(intervals) * percent / 100) - 1)]

        return {
            "acceptance": "PROBE_INIT_TIMER_STOP" if self.expect_stop else "PROBE_INIT_TIMER",
            "timer_samples": len(self._timer_ns),
            "first_timer_after_init_ns": self._timer_ns[0] - self.init_qpc_ns,
            "observed_interval_ns": {
                "p50": percentile(50),
                "p95": percentile(95),
                "p99": percentile(99),
                "p100": percentile(100),
            },
            "scheduled_time_evidence": "UNAVAILABLE",
            "capability_accepted": False,
        }

"""Bridge transport lifecycle guards shared by mTLS and test implementations.

Socket/TLS setup is intentionally injected by the Windows deployment package.
This module provides the fail-closed lifecycle guard used by the Linux Adapter
and deterministic in-process acceptance fixtures; it never impersonates QMT.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .bridge_protocol import QmtBridgeDelivery, QmtBridgeDeliverySink, QmtBridgeError


class QmtBridgeConnection(Protocol):
    """One authenticated bridge connection after mTLS validation."""

    def open(self, sink: QmtBridgeDeliverySink) -> None:
        """Attach one durable inbound delivery sink."""

        ...

    def close(self) -> None:
        """Close streams and invalidate the delivery sink."""

        ...


@dataclass(frozen=True)
class QmtBridgeTransportSnapshot:
    """Local lifecycle state that contains no endpoint or credential material."""

    opened: bool
    deliveries: int


class QmtBridgeLifecycle:
    """Single-sink guard for bridge implementations.

    The concrete mTLS transport owns framing and certificates.  It must call
    :meth:`deliver` only after its event record is Edge-durable, and it must
    not reuse the instance after :meth:`close` without a fresh :meth:`open`.
    """

    def __init__(self) -> None:
        self._sink: QmtBridgeDeliverySink | None = None
        self._deliveries = 0

    def open(self, sink: QmtBridgeDeliverySink) -> None:
        if self._sink is not None:
            raise QmtBridgeError("QMT bridge delivery sink is already attached")
        self._sink = sink

    def close(self) -> None:
        self._sink = None

    def deliver(self, delivery: QmtBridgeDelivery) -> None:
        sink = self._sink
        if sink is None:
            raise QmtBridgeError("QMT bridge delivered without an attached sink")
        self._deliveries += 1
        sink(delivery)

    def snapshot(self) -> QmtBridgeTransportSnapshot:
        return QmtBridgeTransportSnapshot(opened=self._sink is not None, deliveries=self._deliveries)

"""DISARMED-only composition of a V2 runtime and an EMT session shell.

This module deliberately has no SDK loader, login method, or account
transition.  It prevents the prepared V2 callback runtime from being silently
replaced by a V1 bridge before the separately controlled session lifecycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Callable

from .fastspi_v2_runtime import FastSpiV2RuntimeBinding, FastSpiV2RuntimeBuildError
from .fastspi_v2_worker import (
    FastSpiV2BrokerEventNormalizerPort,
    FastSpiV2BrokerEventObserverPort,
    FastSpiV2CallbackPipelineWorker,
)
from .gateway import CallbackPipeline
from .session import EmtSession, SessionSettings


class FastSpiV2SessionOwnerError(RuntimeError):
    """A V2 session shell cannot be safely prepared in the current state."""


@dataclass(frozen=True)
class FastSpiV2SessionOwnerSnapshot:
    """No-secret status for a session owner that has not started an API."""

    account_id: str
    account_profile: str
    session_shell_prepared: bool


class FastSpiV2SessionOwner:
    """Keep one verified V2 runtime paired with at most one NEW session shell."""

    def __init__(
        self,
        *,
        runtime_binding: FastSpiV2RuntimeBinding,
        settings: SessionSettings,
    ) -> None:
        runtime_binding.config.validate()
        settings.validate()
        if settings.account != runtime_binding.config.account_id:
            raise FastSpiV2SessionOwnerError("fastspi v2 session account identity mismatch")
        if settings.account_profile != runtime_binding.config.account_profile:
            raise FastSpiV2SessionOwnerError("fastspi v2 session account profile mismatch")
        if runtime_binding.gap_latch.active:
            raise FastSpiV2SessionOwnerError(
                "fastspi v2 session cannot be prepared with an active callback gap"
            )
        self._runtime_binding = runtime_binding
        self._settings = settings
        self._session: EmtSession | None = None
        self._lock = Lock()

    def snapshot(self) -> FastSpiV2SessionOwnerSnapshot:
        with self._lock:
            return FastSpiV2SessionOwnerSnapshot(
                account_id=self._runtime_binding.config.account_id,
                account_profile=self._runtime_binding.config.account_profile,
                session_shell_prepared=self._session is not None,
            )

    def build_callback_worker(
        self,
        *,
        callback_pipeline: CallbackPipeline,
        broker_event_normalizer: FastSpiV2BrokerEventNormalizerPort | None = None,
        broker_event_observer: FastSpiV2BrokerEventObserverPort | None = None,
        monotonic_clock_ns: Callable[[], int] | None = None,
    ) -> FastSpiV2CallbackPipelineWorker:
        """Build the post-ring worker from the exact paired V2 runtime."""

        if self._runtime_binding.gap_latch.active:
            raise FastSpiV2SessionOwnerError(
                "fastspi v2 callback worker is blocked by an active callback gap"
            )
        try:
            return self._runtime_binding.build_callback_worker(
                callback_pipeline=callback_pipeline,
                broker_event_normalizer=broker_event_normalizer,
                broker_event_observer=broker_event_observer,
                monotonic_clock_ns=monotonic_clock_ns,
            )
        except FastSpiV2RuntimeBuildError as error:
            raise FastSpiV2SessionOwnerError(
                "fastspi v2 runtime cannot build its controlled callback worker"
            ) from error

    def prepare_session_shell(self) -> EmtSession:
        """Create exactly one NEW session object without invoking ``start``."""

        with self._lock:
            if self._session is not None:
                raise FastSpiV2SessionOwnerError("fastspi v2 session shell was already prepared")
            if self._runtime_binding.gap_latch.active:
                raise FastSpiV2SessionOwnerError(
                    "fastspi v2 session cannot be prepared with an active callback gap"
                )
            session = EmtSession.from_prebuilt_trader_spi_type(
                trader_spi_type=self._runtime_binding.trader_spi_type,
                settings=self._settings,
            )
            self._session = session
            return session

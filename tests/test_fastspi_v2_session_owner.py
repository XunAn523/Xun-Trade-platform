"""DISARMED-only V2 session-shell composition tests."""

from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.fastspi_v2_equivalence import legacy_emt_fastspi_v2_equivalence_matrix
from emt_gateway.fastspi_v2_runtime import FastSpiV2RuntimeBinding, FastSpiV2RuntimeConfig
from emt_gateway.fastspi_v2_session_owner import (
    FastSpiV2SessionOwner,
    FastSpiV2SessionOwnerError,
)
from emt_gateway.session import SessionSettings, SessionState
from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI
from xuanling_tradecore.native.fastspi_v2 import (
    FastSpiRuntimeSelection,
    FastSpiV2CallbackKind,
    FastSpiV2CallbackKindRegistry,
    FastSpiVersion,
)


class _TraderApiBase:
    instances = 0

    def __init__(self) -> None:
        _TraderApiBase.instances += 1


@dataclass
class _Ring:
    native_available: bool = True


@dataclass
class _GapLatch:
    active: bool = False
    producers: list[tuple[int, str]] = field(default_factory=list)
    callback_kinds: list[tuple[int, str]] = field(default_factory=list)

    def register_producer(self, *, producer_id: int, label: str) -> None:
        self.producers.append((producer_id, label))

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None:
        self.callback_kinds.append((callback_kind, label))

    def record_native_rejection(
        self,
        *,
        producer_id: int,
        callback_seq: int,
        callback_kind: int,
        observed_monotonic_ns: int,
    ) -> bool:
        return True


class _Ingress:
    def offer(self, **kwargs: object) -> bool:
        return True

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        return None


class FastSpiV2SessionOwnerTests(unittest.TestCase):
    def setUp(self) -> None:
        _TraderApiBase.instances = 0
        matrix = legacy_emt_fastspi_v2_equivalence_matrix()
        self.selection = FastSpiRuntimeSelection(
            version=FastSpiVersion.V2,
            equivalence_certificate_sha256=matrix.certificate_sha256,
        )
        self.config = FastSpiV2RuntimeConfig(
            account_id="emt_sim_03",
            account_profile="ordinary",
            account_lifecycle="DISARMED",
            producer_id=7,
            producer_label="emt-spi-7",
            ring_capacity=2,
            maximum_producers=1,
        )

    def _binding(self, *, latch: _GapLatch | None = None) -> FastSpiV2RuntimeBinding:
        return FastSpiV2RuntimeBinding(
            config=self.config,
            selection=self.selection,
            ring=_Ring(),
            gap_latch=latch or _GapLatch(),
            ingress=_Ingress(),
            callback_kinds=FastSpiV2CallbackKindRegistry(
                tuple(
                    FastSpiV2CallbackKind(code=code, name=name)
                    for code, name in enumerate(OFFICIAL_TRADER_SPI, start=1)
                )
            ),
            trader_spi_type=_TraderApiBase,
        )

    def _settings(self, root: Path, *, account: str = "emt_sim_03") -> SessionSettings:
        return SessionSettings(
            host="synthetic-host",
            port=19088,
            account=account,
            password="synthetic-password",
            client_id=81,
            sdk_log_dir=root / "sdk-log",
            account_profile="ordinary",
        )

    def test_prepares_one_new_session_shell_without_api_or_filesystem_effects(self) -> None:
        with TemporaryDirectory() as directory:
            log_dir = Path(directory) / "sdk-log"
            owner = FastSpiV2SessionOwner(
                runtime_binding=self._binding(),
                settings=self._settings(Path(directory)),
            )
            self.assertFalse(owner.snapshot().session_shell_prepared)
            session = owner.prepare_session_shell()
            self.assertEqual(SessionState.NEW, session.snapshot().state)
            self.assertTrue(owner.snapshot().session_shell_prepared)
            self.assertEqual(0, _TraderApiBase.instances)
            self.assertFalse(log_dir.exists())
            with self.assertRaisesRegex(FastSpiV2SessionOwnerError, "already prepared"):
                owner.prepare_session_shell()

    def test_rejects_mismatched_account_or_a_latched_runtime(self) -> None:
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(FastSpiV2SessionOwnerError, "account identity"):
                FastSpiV2SessionOwner(
                    runtime_binding=self._binding(),
                    settings=self._settings(Path(directory), account="different-account"),
                )
            with self.assertRaisesRegex(FastSpiV2SessionOwnerError, "active callback gap"):
                FastSpiV2SessionOwner(
                    runtime_binding=self._binding(latch=_GapLatch(active=True)),
                    settings=self._settings(Path(directory)),
                )


if __name__ == "__main__":
    unittest.main()

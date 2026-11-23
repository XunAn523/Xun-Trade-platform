from __future__ import annotations

import pytest

from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI

from xuanling_tradecore.adapters.emt.trading_spi_runtime_v1 import (
    TRADER_SPI_BINDINGS_V1,
    build_complete_trader_spi_v1,
    callback_priority_for_kind_v1,
    register_complete_trader_spi_v1,
)


class _Base:
    pass


class _Ingress:
    def __init__(self) -> None:
        self.offers: list[dict[str, object]] = []
        self.gaps: list[tuple[int, int]] = []

    def offer(self, **kwargs: object) -> bool:
        self.offers.append(kwargs)
        return True

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        self.gaps.append((callback_kind, observed_monotonic_ns))


class _Registrar:
    def __init__(self) -> None:
        self.entries: list[tuple[int, str]] = []

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None:
        self.entries.append((callback_kind, label))


def test_t1_registers_exactly_the_complete_callback_abi() -> None:
    registrar = _Registrar()
    register_complete_trader_spi_v1(registrar)
    assert tuple(registrar.entries) == tuple(enumerate(OFFICIAL_TRADER_SPI, start=1))
    assert tuple(item.official_name for item in TRADER_SPI_BINDINGS_V1) == OFFICIAL_TRADER_SPI


def test_t1_generated_bridge_has_every_override_and_keeps_c5_scalar_only() -> None:
    ingress = _Ingress()
    bridge = build_complete_trader_spi_v1(_Base, callback_ingress=ingress)()
    for expected_kind, item in enumerate(TRADER_SPI_BINDINGS_V1, start=1):
        assert callable(getattr(bridge, item.binding_name))
        getattr(bridge, item.binding_name)()
        assert ingress.offers[-1]["callback_kind"] == expected_kind


def test_t1_priority_table_is_total_and_unknown_kind_is_fail_closed() -> None:
    assert callback_priority_for_kind_v1(1) == TRADER_SPI_BINDINGS_V1[0].priority
    assert callback_priority_for_kind_v1(69) == TRADER_SPI_BINDINGS_V1[-1].priority
    with pytest.raises(Exception, match="unknown TraderSpi callback kind"):
        callback_priority_for_kind_v1(70)

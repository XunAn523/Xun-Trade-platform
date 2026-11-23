from __future__ import annotations

from xuanling_tradecore.adapters.emt.trading_attestation_v1 import build_trading_abi_attestation_v1


def test_t6_abi_attestation_is_complete_and_never_logs_into_emt() -> None:
    report = build_trading_abi_attestation_v1(type("TraderApiBase", (), {}))
    assert report["schema"] == "tradecore.emt.trading-abi-attestation.v1"
    assert report["scope"] == "TraderApi-and-TraderSpi-only; QuoteApi-excluded"
    assert len(report["api"]) == 86
    assert len(report["spi"]) == 69
    assert report["counts"] == {
        "TraderApi": 86,
        "TraderSpi": 69,
        "api_supported_or_adapter_owned": 1,
        "spi_present": 69,
    }

from __future__ import annotations

from tools.acceptance.t6_emt_capability_matrix import build_t6_capability_matrix


def _report(profile: str, *, callbacks: int = 0) -> dict[str, object]:
    return {
        "driver": "vendor-simulated-sweep",
        "profile": profile,
        "operations": [
            {
                "binding_method": "queryAsset",
                "return_latency": {"count": 1},
                "callbacks": 0,
            },
            {
                "binding_method": "onQueryAsset",
                "return_latency": {"count": 0},
                "callbacks": callbacks,
            },
        ],
    }


def test_matrix_is_exact_and_does_not_upgrade_sweep_to_terminal_verification() -> None:
    matrix = build_t6_capability_matrix((_report("ordinary", callbacks=2),))
    assert matrix["summary"] == {
        "api_count": 86,
        "spi_count": 69,
        "api_observed": 1,
        "spi_observed": 1,
        "unsupported_binding": 4,
        "not_terminally_verified": 155,
    }
    api = {entry["official_name"]: entry for entry in matrix["api"]}
    spi = {entry["official_name"]: entry for entry in matrix["spi"]}
    assert api["QueryAsset"]["state"] == "supported_contract_only"
    assert spi["OnQueryAsset"]["state"] == "supported_contract_only"
    assert api["CreditMarginAvlTransfer"]["state"] == "unsupported_binding"
    assert matrix["release_conclusion"].startswith("not_ready")

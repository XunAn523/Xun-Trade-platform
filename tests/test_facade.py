from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_ROOT = ROOT / "tools" / "performance"
if str(PERFORMANCE_ROOT) not in sys.path:
    sys.path.insert(0, str(PERFORMANCE_ROOT))

from emt_gateway.capabilities import OFFICIAL_TRADER_API, inspect_trader_api, python_api_name
from emt_gateway.facade import (
    EmtTraderFacade,
    LifecycleOwnedError,
    SideEffectPermit,
    SideEffectPermitError,
    UnsupportedInterfaceError,
    operation_specs,
    query_completion_for,
)
from emt_gateway.query_registry import QueryCompletion, QueryRegistry
from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI
from perfbench.catalog import TRADER_API


class FacadeBinding:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []


def _make_method(name: str):
    def method(self: FacadeBinding, *args: object) -> int:
        self.calls.append((name, args))
        return 0

    return method


for _official_name in OFFICIAL_TRADER_API:
    if _official_name != "RegisterSpi":
        setattr(FacadeBinding, python_api_name(_official_name), _make_method(_official_name))


class FacadeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.api = FacadeBinding()
        self.registry = QueryRegistry()
        self.facade = EmtTraderFacade(
            self.api,
            session_id=42,
            capability_report=inspect_trader_api(self.api),
            query_registry=self.registry,
        )

    def test_route_table_covers_the_official_86_name_catalog(self) -> None:
        self.assertEqual(TRADER_API, tuple(spec.official_name for spec in operation_specs()))
        self.assertEqual(86, len(operation_specs()))
        self.assertTrue(
            all(
                spec.callback_name in OFFICIAL_TRADER_SPI
                for spec in operation_specs()
                if spec.callback_name is not None
            )
        )

    def test_direct_lookup_and_mutation_use_the_proven_session_argument_order(self) -> None:
        lookup = self.facade.invoke("IsServerRestart")
        self.assertEqual(0, lookup.return_value)
        self.assertEqual(("IsServerRestart", (42,)), self.api.calls[-1])

        with self.assertRaises(SideEffectPermitError):
            self.facade.invoke("FundTransfer", business_args=({"opaque": "not retained"},))
        self.facade.invoke(
            "FundTransfer",
            business_args=({"opaque": "not retained"},),
            side_effect_permit=SideEffectPermit.simulated_fixture(),
        )
        self.assertEqual(("FundTransfer", ({"opaque": "not retained"}, 42)), self.api.calls[-1])
        with self.assertRaisesRegex(Exception, "durable command workflow"):
            self.facade.invoke(
                "FundTransfer",
                business_args=({},),
                side_effect_permit=SideEffectPermit.simulated_fixture(),
                trace_call=True,
            )

    def test_query_owns_request_id_and_appends_session_then_request_id(self) -> None:
        handle = self.facade.invoke_query("QueryAsset", correlation_id="query-facade")
        self.assertEqual("OnQueryAsset", handle.callback_name)
        self.assertEqual(("QueryAsset", (42, handle.request_id)), self.api.calls[-1])

    def test_single_response_binding_callbacks_have_explicit_terminality_contracts(self) -> None:
        for api_name in (
            "QueryFund",
            "QueryOtherServerFund",
            "QueryCreditFundInfo",
            "QueryCreditAssetDebtInfo",
            "QueryCreditExcessStock",
            "QueryMulCreditExcessStock",
            "QueryCreditFundExtraInfo",
            "QueryOptionCombinedExecPosition",
            "QueryHKConnectIndicativeExchangeRateInfo",
        ):
            self.assertEqual(QueryCompletion.SINGLE, query_completion_for(api_name))
        self.assertEqual(QueryCompletion.LAST, query_completion_for("QueryOrders"))
        self.assertEqual(QueryCompletion.LAST, query_completion_for("QueryYesterdayAsset"))
        with self.assertRaisesRegex(Exception, "not a callback-driven query"):
            query_completion_for("GetApiVersion")

    def test_lifecycle_and_missing_binding_methods_fail_explicitly(self) -> None:
        with self.assertRaises(LifecycleOwnedError):
            self.facade.invoke("SetCpuAffinity", business_args=(1, 2))

        class Partial:
            pass

        partial = EmtTraderFacade(Partial(), session_id=42)
        with self.assertRaises(UnsupportedInterfaceError):
            partial.invoke("GetTradingDay")

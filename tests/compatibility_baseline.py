"""P0 compatibility baseline for the existing gateway hot paths.

This manifest deliberately references the tests that describe the behaviour
already relied on by the current gateway.  M0--M3 may add wrappers and new
domain modules, but must keep these cases green unless a reviewed compatibility
migration explicitly replaces a baseline entry.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BaselineTest:
    """A stable, fully-qualified unittest case and its runtime requirement."""

    test_id: str
    requires_postgres: bool = False


@dataclass(frozen=True)
class CompatibilityBaselineModule:
    """The source module and behavioural cases protected by the P0 baseline."""

    component: str
    source_module: str
    test_module: str
    tests: tuple[BaselineTest, ...]


COMPATIBILITY_BASELINE: tuple[CompatibilityBaselineModule, ...] = (
    CompatibilityBaselineModule(
        component="OrderGateway",
        source_module="emt_gateway.gateway",
        test_module="tests.test_gateway",
        tests=(
            BaselineTest(
                "tests.test_gateway.OrderGatewayTests."
                "test_order_is_durable_before_native_and_same_id_never_replays"
            ),
            BaselineTest(
                "tests.test_gateway.OrderGatewayTests."
                "test_call_started_exception_is_unknown_and_same_id_is_not_called_again"
            ),
            BaselineTest(
                "tests.test_gateway.OrderGatewayTests."
                "test_cancel_requires_native_mapping_and_uses_its_own_callback_correlation"
            ),
            BaselineTest(
                "tests.test_gateway.OrderGatewayTests."
                "test_journal_failure_fails_closed_before_the_native_boundary"
            ),
            BaselineTest(
                "tests.test_gateway.OrderGatewayTests."
                "test_callback_overflow_disarms_instead_of_silent_drop"
            ),
            BaselineTest(
                "tests.test_gateway.OrderGatewayTests."
                "test_unavailable_postgres_does_not_block_command_or_callback_ack"
            ),
        ),
    ),
    CompatibilityBaselineModule(
        component="FileJournal",
        source_module="emt_gateway.journal",
        test_module="tests.test_journal",
        tests=(
            BaselineTest(
                "tests.test_journal.FileJournalTests."
                "test_append_is_recovered_with_identical_identity_and_monotonic_lsn"
            ),
            BaselineTest(
                "tests.test_journal.FileJournalTests."
                "test_open_truncates_an_incomplete_tail_and_keeps_verified_prefix"
            ),
            BaselineTest(
                "tests.test_journal.FileJournalTests.test_open_truncates_a_crc_corrupt_frame"
            ),
            BaselineTest(
                "tests.test_journal.FileJournalTests.test_group_commit_returns_only_durable_records"
            ),
            BaselineTest(
                "tests.test_journal.FileJournalTests."
                "test_group_commit_prioritises_pre_native_evidence_without_dropping_outcome"
            ),
        ),
    ),
    CompatibilityBaselineModule(
        component="TraderSpi Bridge",
        source_module="emt_gateway.spi_bridge",
        test_module="tests.test_spi_bridge",
        tests=(
            BaselineTest(
                "tests.test_spi_bridge.SpiBridgeTests."
                "test_registers_all_official_callback_overrides"
            ),
            BaselineTest(
                "tests.test_spi_bridge.SpiBridgeTests."
                "test_copies_only_order_and_operation_ids_into_ingress"
            ),
            BaselineTest(
                "tests.test_spi_bridge.SpiBridgeTests."
                "test_normalizes_order_and_trade_facts_without_retaining_native_payloads"
            ),
            BaselineTest(
                "tests.test_spi_bridge.SpiBridgeTests."
                "test_query_callbacks_forward_only_request_and_terminal_metadata"
            ),
            BaselineTest(
                "tests.test_spi_bridge.SpiBridgeTests."
                "test_deferred_ingress_drops_pre_session_replay_then_forwards_once_attached"
            ),
        ),
    ),
    CompatibilityBaselineModule(
        component="QueryRegistry",
        source_module="emt_gateway.query_registry",
        test_module="tests.test_query_registry",
        tests=(
            BaselineTest(
                "tests.test_query_registry.QueryRegistryTests."
                "test_terminal_query_joins_active_and_callback_c0_to_c7_paths"
            ),
            BaselineTest(
                "tests.test_query_registry.QueryRegistryTests."
                "test_multicast_query_waits_for_last_callback_before_c6_and_ack"
            ),
            BaselineTest(
                "tests.test_query_registry.QueryRegistryTests."
                "test_completed_query_exposes_only_payload_free_semantic_facts"
            ),
            BaselineTest(
                "tests.test_query_registry.QueryRegistryTests."
                "test_request_id_cools_before_reuse_and_nonzero_return_completes_rejected"
            ),
            BaselineTest(
                "tests.test_query_registry.QueryRegistryTests."
                "test_timeout_retires_only_expired_requests_unless_session_is_forced_closed"
            ),
        ),
    ),
    CompatibilityBaselineModule(
        component="AsyncPostgresOrderLedger",
        source_module="emt_gateway.order_ledger",
        test_module="tests.test_order_ledger",
        tests=(
            BaselineTest(
                "tests.test_order_ledger.AsyncPostgresOrderLedgerFailureTests."
                "test_connection_failure_is_background_only"
            ),
            BaselineTest(
                "tests.test_order_ledger.PostgresOrderLedgerTests."
                "test_trade_deduplication_and_projection_rebuild_preserve_continuity",
                requires_postgres=True,
            ),
            BaselineTest(
                "tests.test_order_ledger.PostgresOrderLedgerTests."
                "test_async_projector_persists_facts_in_background",
                requires_postgres=True,
            ),
        ),
    ),
)

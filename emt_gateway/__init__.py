"""EMT gateway core.

This package intentionally contains no import of the vendor binding.  The
production binding is injected through the narrow protocol in ``native_api``.
"""

from .capabilities import (
    OFFICIAL_TRADER_API,
    CapabilityEntry,
    CapabilityReport,
    CapabilityStatus,
    inspect_trader_api,
    require_direct_capabilities,
)
from .facade import (
    EmtTraderFacade,
    FacadeError,
    LifecycleOwnedError,
    NativeInvocation,
    OperationSpec,
    SideEffectPermit,
    SideEffectPermitError,
    UnsupportedInterfaceError,
    operation_specs,
)
from .fastspi import FastSpiMpscRing, FastSpiUnavailable
from .fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeError,
    FastSpiV2CancelOutcomeObserver,
)
from .fastspi_v2_domain import (
    FastSpiV2BrokerEventNormalizer,
    FastSpiV2CallbackDomainIdentity,
    FastSpiV2DomainEventConfig,
    FastSpiV2DomainNormalizationError,
)
from .fastspi_v2_outcome import (
    FastSpiV2BrokerEventObserverMultiplexer,
    FastSpiV2NativeOutcomeError,
    FastSpiV2NativeOutcomeObserver,
    FastSpiV2OutcomeResolution,
)
from .fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from .fastspi_v2_runtime import (
    FastSpiV2RuntimeBinding,
    FastSpiV2RuntimeBuildError,
    FastSpiV2RuntimeConfig,
    build_fastspi_v2_runtime_binding,
)
from .fastspi_v2_session_owner import (
    FastSpiV2SessionOwner,
    FastSpiV2SessionOwnerError,
    FastSpiV2SessionOwnerSnapshot,
)
from .gateway import (
    CallbackDelivery,
    CancelIntent,
    CommandResult,
    CommandState,
    GatewayError,
    OrderGateway,
    OrderIntent,
)
from .journal import (
    FileJournal,
    InMemoryJournal,
    JournalAppendTiming,
    JournalFailure,
    JournalRecord,
)
from .journal_archive import AsyncFileJournalArchive, JournalArchiveSink, JournalArchiveSnapshot
from .linux_attestation import EmtLinuxBindingAttestation, collect_linux_binding_attestation
from .native_api import EmtOrderAdapter, NativeAdapterError
from .operational_telemetry import (
    BoundedOperationalTelemetry,
    OperationalTelemetryEvent,
    OtlpGrpcMetricExporter,
    TelemetryError,
    TelemetryExportPort,
    TelemetryStats,
)
from .order_ledger import (
    AsyncLedgerStatus,
    AsyncPostgresOrderLedger,
    BookHealth,
    BookStatus,
    LedgerCallbackFact,
    LedgerCallbackKind,
    LedgerEvent,
    LedgerEventKind,
    LedgerOrderIntent,
    OrderLedgerError,
    OrderState,
    OrderView,
    PostgresOrderLedger,
)
from .platform_loader import (
    EmtPlatformLoader,
    PlatformLoadError,
    SdkLoadSettings,
    is_project_venv_runtime,
)
from .query_registry import (
    QueryCompletion,
    QueryHandle,
    QueryRegistry,
    QueryRegistryError,
    QuerySemanticObservation,
)
from .recovery import (
    CommandCustody,
    EncryptedReplayStore,
    RecoverableCommand,
    RecoveryDisposition,
    RecoveryError,
    recovery_plan,
)
from .session import EmtSession, SessionError, SessionSettings
from .tradecore_mmap_journal import (
    EmtCancelRoute,
    EmtJournaledBrokerEventObserver,
    EmtMmapNativeCallJournal,
    EmtMmapNativeCallJournalError,
    EmtNativeRouteResolver,
    EmtSubmitRoute,
)
from .tradecore_read_only import (
    EmtLoadedSdkProvider,
    EmtPlatformReadOnlySessionFactory,
    EmtReadOnlyPlatformSnapshot,
    EmtTradeCoreReadOnlySessionError,
)

__all__ = [
    "CallbackDelivery",
    "AsyncLedgerStatus",
    "AsyncFileJournalArchive",
    "AsyncPostgresOrderLedger",
    "BookHealth",
    "BookStatus",
    "build_fastspi_v2_runtime_binding",
    "CapabilityEntry",
    "CapabilityReport",
    "CapabilityStatus",
    "CancelIntent",
    "CommandResult",
    "CommandState",
    "CommandCustody",
    "EmtPlatformLoader",
    "EmtPlatformReadOnlySessionFactory",
    "EmtCancelRoute",
    "EmtJournaledBrokerEventObserver",
    "EmtNativeRouteResolver",
    "EmtMmapNativeCallJournal",
    "EmtMmapNativeCallJournalError",
    "EmtSubmitRoute",
    "EmtOrderAdapter",
    "EmtSession",
    "EmtLoadedSdkProvider",
    "EmtLinuxBindingAttestation",
    "EmtTraderFacade",
    "EmtReadOnlyPlatformSnapshot",
    "EmtTradeCoreReadOnlySessionError",
    "EncryptedReplayStore",
    "FacadeError",
    "FileJournal",
    "FastSpiMpscRing",
    "FastSpiUnavailable",
    "FastSpiV2RuntimeBinding",
    "FastSpiV2RuntimeBuildError",
    "FastSpiV2RuntimeConfig",
    "FastSpiV2BrokerEventNormalizer",
    "FastSpiV2BrokerEventObserverMultiplexer",
    "FastSpiV2CallbackDomainIdentity",
    "FastSpiV2DomainEventConfig",
    "FastSpiV2DomainNormalizationError",
    "FastSpiV2CancelOutcomeDirectoryConfig",
    "FastSpiV2CancelOutcomeError",
    "FastSpiV2CancelOutcomeObserver",
    "FastSpiV2NativeOutcomeError",
    "FastSpiV2NativeOutcomeObserver",
    "FastSpiV2OutcomeResolution",
    "FastSpiV2RouteOutcomeDirectory",
    "FastSpiV2RouteOutcomeDirectoryConfig",
    "FastSpiV2SessionOwner",
    "FastSpiV2SessionOwnerError",
    "FastSpiV2SessionOwnerSnapshot",
    "GatewayError",
    "InMemoryJournal",
    "inspect_trader_api",
    "collect_linux_binding_attestation",
    "JournalFailure",
    "JournalArchiveSink",
    "JournalArchiveSnapshot",
    "JournalAppendTiming",
    "JournalRecord",
    "NativeAdapterError",
    "BoundedOperationalTelemetry",
    "OperationalTelemetryEvent",
    "OtlpGrpcMetricExporter",
    "NativeInvocation",
    "OperationSpec",
    "PlatformLoadError",
    "QueryCompletion",
    "QueryHandle",
    "QueryRegistry",
    "QueryRegistryError",
    "QuerySemanticObservation",
    "OrderGateway",
    "OrderLedgerError",
    "OrderIntent",
    "OrderState",
    "OrderView",
    "OFFICIAL_TRADER_API",
    "require_direct_capabilities",
    "RecoverableCommand",
    "RecoveryDisposition",
    "RecoveryError",
    "recovery_plan",
    "SdkLoadSettings",
    "is_project_venv_runtime",
    "SessionError",
    "SessionSettings",
    "SideEffectPermit",
    "SideEffectPermitError",
    "LifecycleOwnedError",
    "operation_specs",
    "UnsupportedInterfaceError",
    "TelemetryError",
    "TelemetryExportPort",
    "TelemetryStats",
    "LedgerEvent",
    "LedgerEventKind",
    "LedgerCallbackFact",
    "LedgerCallbackKind",
    "LedgerOrderIntent",
    "PostgresOrderLedger",
]

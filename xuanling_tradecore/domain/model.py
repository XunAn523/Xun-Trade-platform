"""Vendor-neutral immutable domain model for Xuanling TradeCore M0.

These values define the OMS/EMS language before any execution, persistence,
or transport implementation exists.  They are intentionally free of SDK,
database, filesystem, and clock dependencies so they can be validated in an
environment where no broker binding is installed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping

from ._validation import (
    DomainValidationError,
    freeze_nonnegative_int_mapping,
    freeze_text_mapping,
    freeze_texts,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)


def _empty_text_mapping() -> Mapping[str, str]:
    """Create a precisely typed empty map for immutable domain summaries."""

    return {}


def _empty_nonnegative_int_mapping() -> Mapping[str, int]:
    """Create a precisely typed empty fixed-integer quantity map."""

    return {}


class AccountLifecycle(str, Enum):
    """Account lifecycle; simulator authorization is distinct from READY."""

    BOOTSTRAP = "BOOTSTRAP"
    RECOVERING = "RECOVERING"
    DISARMED = "DISARMED"
    READY = "READY"
    SIMULATOR_ACTIVE = "SIMULATOR_ACTIVE"


class ReconciliationStatus(str, Enum):
    """Whether broker evidence is required or has been validated."""

    NOT_REQUIRED = "NOT_REQUIRED"
    REQUIRED = "REQUIRED"
    RUNNING = "RUNNING"
    VALIDATED = "VALIDATED"


class AdmissionGateKind(str, Enum):
    """A versioned reason that prevents new order admission."""

    KILL_SWITCH = "KILL_SWITCH"
    OPERATOR_STOP = "OPERATOR_STOP"
    JOURNAL_CAPACITY = "JOURNAL_CAPACITY"
    JOURNAL_INTEGRITY = "JOURNAL_INTEGRITY"
    CALLBACK_GAP = "CALLBACK_GAP"
    RISK_STALE = "RISK_STALE"
    SESSION_OR_ADAPTER = "SESSION_OR_ADAPTER"
    RECONCILIATION = "RECONCILIATION"


class NativeOutcomeRace(str, Enum):
    """The durable ordering state between a native return and callback."""

    PENDING = "PENDING"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    NATIVE_OBSERVED_FROM_CALLBACK = "NATIVE_OBSERVED_FROM_CALLBACK"
    NATIVE_RETURN_OBSERVED = "NATIVE_RETURN_OBSERVED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"


class OrderOrigin(str, Enum):
    """The authorized source category for an OMS order."""

    STRATEGY = "STRATEGY"
    MANUAL = "MANUAL"
    EXTERNAL_ATTACHED = "EXTERNAL_ATTACHED"
    COMPATIBILITY = "COMPATIBILITY"


class AttributionState(str, Enum):
    """Whether a broker order is safely attributable to a TradeCore binding."""

    OWN = "OWN"
    FOREIGN_SAME_ACCOUNT = "FOREIGN_SAME_ACCOUNT"
    UNKNOWN = "UNKNOWN"
    UNATTRIBUTED = "UNATTRIBUTED"
    ATTACHED = "ATTACHED"


class ErrorCategory(str, Enum):
    """Normalized, payload-free error categories exposed across adapters."""

    VALIDATION = "VALIDATION"
    ADMISSION = "ADMISSION"
    RISK = "RISK"
    SESSION_OR_ADAPTER = "SESSION_OR_ADAPTER"
    VENUE_REJECTED = "VENUE_REJECTED"
    VENUE_UNAVAILABLE = "VENUE_UNAVAILABLE"
    CALLBACK_GAP = "CALLBACK_GAP"
    RECONCILIATION = "RECONCILIATION"
    UNKNOWN = "UNKNOWN"


class OmsOrderState(str, Enum):
    """Monotonic OMS order states; transitions will be implemented in M1."""

    ACCEPTED = "ACCEPTED"
    WORKING = "WORKING"
    CANCEL_PENDING = "CANCEL_PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"


class ConsumerEventKind(str, Enum):
    """Consumer-visible event families."""

    ORDER = "ORDER"
    ACCOUNT_STATUS = "ACCOUNT_STATUS"
    ACCOUNT_RISK = "ACCOUNT_RISK"


class BrokerEventKind(str, Enum):
    """Normalized event families supplied by a BrokerAdapter."""

    ORDER = "ORDER"
    TRADE = "TRADE"
    CANCEL_ERROR = "CANCEL_ERROR"
    QUERY = "QUERY"
    PAPER = "PAPER"


class BrokerSourceKind(str, Enum):
    """The broker-side origin used when constructing a stable source key."""

    CALLBACK = "CALLBACK"
    QUERY = "QUERY"
    RECOVERY_REPLAY = "RECOVERY_REPLAY"
    PAPER_MATCH = "PAPER_MATCH"


@dataclass(frozen=True)
class TradingDayEpoch:
    """A venue-account trading-day scope that prevents day-crossing identity reuse."""

    value: str

    def __post_init__(self) -> None:
        require_text(self.value, "trading_day_epoch", max_length=64)


@dataclass(frozen=True)
class ConsumerApiVersion:
    """A major/minor Consumer API schema version."""

    major: int
    minor: int = 0

    def __post_init__(self) -> None:
        require_positive(self.major, "consumer API major version")
        require_nonnegative(self.minor, "consumer API minor version")

    def is_compatible_with(self, requested: ConsumerApiVersion) -> bool:
        """Return whether this server version can serve the requested version."""

        return self.major == requested.major and self.minor >= requested.minor


@dataclass(frozen=True)
class ConsumerPrincipal:
    """An internal consumer identity; personal fields are excluded from repr."""

    consumer_id: str
    login_name: str = field(repr=False)
    legal_name: str = field(repr=False)
    active: bool = True

    def __post_init__(self) -> None:
        require_text(self.consumer_id, "consumer_id")
        require_text(self.login_name, "login_name", max_length=128)
        require_text(self.legal_name, "legal_name", max_length=128)


@dataclass(frozen=True)
class ConsumerAccountBinding:
    """The authorized connection from one consumer to one venue account."""

    binding_id: str
    binding_version: int
    consumer_id: str
    consumer_account_id: str
    account_id: str
    account_display_name: str
    venue_id: str
    adapter_instance_id: str
    active: bool = True

    def __post_init__(self) -> None:
        require_text(self.binding_id, "consumer account binding_id")
        require_positive(self.binding_version, "consumer account binding_version")
        require_text(self.consumer_id, "consumer_id")
        require_text(self.consumer_account_id, "consumer_account_id")
        require_text(self.account_id, "account_id")
        require_text(self.account_display_name, "account_display_name", max_length=128)
        require_text(self.venue_id, "venue_id")
        require_text(self.adapter_instance_id, "adapter_instance_id")


@dataclass(frozen=True)
class ConsumerStrategyBinding:
    """The authorized relationship between a consumer account and one strategy."""

    binding_id: str
    binding_version: int
    consumer_id: str
    consumer_account_id: str
    consumer_account_binding_id: str
    strategy_id: str
    active: bool = True

    def __post_init__(self) -> None:
        require_text(self.binding_id, "consumer strategy binding_id")
        require_positive(self.binding_version, "consumer strategy binding_version")
        require_text(self.consumer_id, "consumer_id")
        require_text(self.consumer_account_id, "consumer_account_id")
        require_text(self.consumer_account_binding_id, "consumer_account_binding_id")
        require_text(self.strategy_id, "strategy_id")


@dataclass(frozen=True)
class AttributionBinding:
    """The immutable binding identity carried by order and callback evidence."""

    attribution_binding_id: str
    consumer_account_binding_id: str
    consumer_strategy_binding_id: str | None
    consumer_id: str
    consumer_account_id: str
    strategy_id: str | None
    binding_version: int

    def __post_init__(self) -> None:
        require_text(self.attribution_binding_id, "attribution_binding_id")
        require_text(self.consumer_account_binding_id, "consumer_account_binding_id")
        require_text(self.consumer_id, "consumer_id")
        require_text(self.consumer_account_id, "consumer_account_id")
        require_positive(self.binding_version, "attribution binding_version")
        if self.consumer_strategy_binding_id is None:
            if self.strategy_id is not None:
                raise DomainValidationError("strategy_id requires a consumer_strategy_binding_id")
        else:
            require_text(self.consumer_strategy_binding_id, "consumer_strategy_binding_id")
            if self.strategy_id is None:
                raise DomainValidationError("consumer_strategy_binding_id requires a strategy_id")
            require_text(self.strategy_id, "strategy_id")


@dataclass(frozen=True)
class ConsumerTrace:
    """Immutable consumer-to-strategy lineage attached to a business intent."""

    consumer_id: str
    consumer_account_id: str
    account_id: str
    strategy_id: str | None
    strategy_order_id: str | None
    attribution_binding_id: str
    binding_version: int
    order_origin: OrderOrigin

    def __post_init__(self) -> None:
        require_text(self.consumer_id, "consumer_id")
        require_text(self.consumer_account_id, "consumer_account_id")
        require_text(self.account_id, "account_id")
        require_text(self.attribution_binding_id, "attribution_binding_id")
        require_positive(self.binding_version, "consumer trace binding_version")
        if self.order_origin in {OrderOrigin.STRATEGY, OrderOrigin.COMPATIBILITY}:
            if self.strategy_id is None or self.strategy_order_id is None:
                raise DomainValidationError(
                    "strategy and compatibility orders require strategy identity"
                )
        if self.strategy_id is not None:
            require_text(self.strategy_id, "strategy_id")
        if self.strategy_order_id is not None:
            require_text(self.strategy_order_id, "strategy_order_id")


@dataclass(frozen=True)
class AdmissionGate:
    """A durable, versioned new-order admission closure reason."""

    gate_id: str
    kind: AdmissionGateKind
    version: int
    source_reference: str
    first_observed_monotonic_ns: int
    last_observed_monotonic_ns: int
    operator_id: str | None = None
    approval_reference: str | None = None

    def __post_init__(self) -> None:
        require_text(self.gate_id, "admission gate_id")
        require_positive(self.version, "admission gate version")
        require_text(self.source_reference, "admission gate source_reference")
        require_nonnegative(self.first_observed_monotonic_ns, "admission gate first time")
        require_nonnegative(self.last_observed_monotonic_ns, "admission gate last time")
        if self.last_observed_monotonic_ns < self.first_observed_monotonic_ns:
            raise DomainValidationError("admission gate last time precedes first time")
        if self.operator_id is not None:
            require_text(self.operator_id, "admission gate operator_id")
        if self.approval_reference is not None:
            require_text(self.approval_reference, "admission gate approval_reference")

    @property
    def is_recovery_gate(self) -> bool:
        """Whether this gate requires recovery/reconciliation rather than a simple clear."""

        return self.kind in {
            AdmissionGateKind.JOURNAL_INTEGRITY,
            AdmissionGateKind.CALLBACK_GAP,
            AdmissionGateKind.SESSION_OR_ADAPTER,
            AdmissionGateKind.RECONCILIATION,
        }


@dataclass(frozen=True)
class AvailabilityObjective:
    """Per-account recovery and replica-loss budget, expressed in milliseconds."""

    objective_id: str
    version: int
    physical_media_rpo_ms: int
    local_recovery_budget_ms: int
    broker_wait_budget_ms: int
    recovery_e2e_budget_ms: int

    def __post_init__(self) -> None:
        require_text(self.objective_id, "availability objective_id")
        require_positive(self.version, "availability objective version")
        require_nonnegative(self.physical_media_rpo_ms, "physical media RPO")
        require_positive(self.local_recovery_budget_ms, "local recovery budget")
        require_positive(self.broker_wait_budget_ms, "broker wait budget")
        require_positive(self.recovery_e2e_budget_ms, "recovery end-to-end budget")
        if self.recovery_e2e_budget_ms < (
            self.local_recovery_budget_ms + self.broker_wait_budget_ms
        ):
            raise DomainValidationError("recovery end-to-end budget is smaller than its phases")


@dataclass(frozen=True)
class ReleaseState:
    """The monotonic, release-manifest state held in RuntimeControlRoot."""

    release_epoch: int
    artifact_hash: str
    release_manifest_hash: str

    def __post_init__(self) -> None:
        require_positive(self.release_epoch, "release_epoch")
        require_sha256(self.artifact_hash, "artifact_hash")
        require_sha256(self.release_manifest_hash, "release_manifest_hash")


@dataclass(frozen=True)
class BundleTrustPolicy:
    """The offline trust policy for versioned control and business bundles."""

    policy_id: str
    policy_epoch: int
    trusted_signer_ids: frozenset[str]
    revoked_signer_ids: frozenset[str]
    allowed_bundle_types: frozenset[str]

    def __post_init__(self) -> None:
        require_text(self.policy_id, "bundle trust policy_id")
        require_positive(self.policy_epoch, "bundle trust policy_epoch")
        trusted = freeze_texts(self.trusted_signer_ids, "trusted_signer_ids")
        revoked = freeze_texts(self.revoked_signer_ids, "revoked_signer_ids")
        allowed = freeze_texts(self.allowed_bundle_types, "allowed_bundle_types")
        if not trusted:
            raise DomainValidationError("bundle trust policy requires at least one trusted signer")
        if not allowed:
            raise DomainValidationError("bundle trust policy requires allowed bundle types")
        if trusted & revoked:
            raise DomainValidationError("a signer cannot be trusted and revoked")
        object.__setattr__(self, "trusted_signer_ids", trusted)
        object.__setattr__(self, "revoked_signer_ids", revoked)
        object.__setattr__(self, "allowed_bundle_types", allowed)


@dataclass(frozen=True)
class SchemaLifecyclePolicy:
    """Reader/writer compatibility and retention policy for one schema family."""

    schema_name: str
    current_version: int
    minimum_reader_version: int
    minimum_writer_version: int
    retention_seconds: int

    def __post_init__(self) -> None:
        require_text(self.schema_name, "schema_name")
        require_positive(self.current_version, "schema current_version")
        require_positive(self.minimum_reader_version, "schema minimum_reader_version")
        require_positive(self.minimum_writer_version, "schema minimum_writer_version")
        require_positive(self.retention_seconds, "schema retention_seconds")
        if self.minimum_reader_version > self.current_version:
            raise DomainValidationError("schema minimum reader version exceeds current version")
        if self.minimum_writer_version > self.current_version:
            raise DomainValidationError("schema minimum writer version exceeds current version")


@dataclass(frozen=True)
class ConsumerEventRetentionPolicy:
    """Retention policy for cursor replay and asynchronous archival."""

    policy_id: str
    version: int
    event_retention_seconds: int
    cursor_ttl_seconds: int
    archive_required: bool

    def __post_init__(self) -> None:
        require_text(self.policy_id, "consumer event retention policy_id")
        require_positive(self.version, "consumer event retention policy version")
        require_positive(self.event_retention_seconds, "event retention seconds")
        require_positive(self.cursor_ttl_seconds, "cursor ttl seconds")
        if self.cursor_ttl_seconds > self.event_retention_seconds:
            raise DomainValidationError("cursor ttl cannot exceed event retention")


@dataclass(frozen=True)
class VenueOrderIdentity:
    """Adapter-neutral mapping between client and venue-native order identities."""

    venue_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    client_order_ref: str | None = None
    venue_order_id: str | None = None
    venue_cancel_id: str | None = None

    def __post_init__(self) -> None:
        require_text(self.venue_id, "venue_id")
        require_text(self.adapter_instance_id, "adapter_instance_id")
        require_text(self.adapter_client_scope, "adapter_client_scope")
        identities = (self.client_order_ref, self.venue_order_id, self.venue_cancel_id)
        if all(value is None for value in identities):
            raise DomainValidationError(
                "venue order identity requires at least one native identifier"
            )
        for field_name, value in (
            ("client_order_ref", self.client_order_ref),
            ("venue_order_id", self.venue_order_id),
            ("venue_cancel_id", self.venue_cancel_id),
        ):
            if value is not None:
                require_text(value, field_name)


@dataclass(frozen=True)
class BrokerCapabilities:
    """Explicit adapter capability declaration; the core must never guess it."""

    capability_profile_id: str
    version: int
    venue_id: str
    supports_cash: bool
    supports_margin_buy: bool
    supports_short_sell: bool
    supports_pagination: bool
    supports_public_stream_replay: bool
    echoes_client_order_ref: bool
    has_stable_venue_order_id: bool
    has_stable_trade_id: bool
    supports_complete_recovery_queries: bool
    supports_negative_confirmation: bool
    supports_paper_matching: bool = False

    def __post_init__(self) -> None:
        require_text(self.capability_profile_id, "capability_profile_id")
        require_positive(self.version, "capability version")
        require_text(self.venue_id, "venue_id")

    @property
    def supports_automatic_recovery_validation(self) -> bool:
        """Return the strict minimum capability for automatic recovery validation."""

        return (
            self.echoes_client_order_ref
            and self.has_stable_venue_order_id
            and self.supports_complete_recovery_queries
            and self.supports_negative_confirmation
        )


@dataclass(frozen=True)
class OmsOrder:
    """A durable OMS order fact; transition and persistence are deferred to M1."""

    oms_order_id: str
    account_id: str
    trading_day: TradingDayEpoch
    consumer_trace: ConsumerTrace
    intent_hash: str
    order_origin: OrderOrigin
    state: OmsOrderState
    created_monotonic_ns: int
    order_revision: int = 0

    def __post_init__(self) -> None:
        require_text(self.oms_order_id, "oms_order_id")
        require_text(self.account_id, "account_id")
        require_sha256(self.intent_hash, "intent_hash")
        require_nonnegative(self.created_monotonic_ns, "order created time")
        require_nonnegative(self.order_revision, "order_revision")
        if self.consumer_trace.account_id != self.account_id:
            raise DomainValidationError("OMS order account does not match consumer trace")
        if self.consumer_trace.order_origin != self.order_origin:
            raise DomainValidationError("OMS order origin does not match consumer trace")


@dataclass(frozen=True)
class ExecutionInstruction:
    """The immutable OMS-to-EMS instruction, independent of broker SDK fields."""

    execution_id: str
    oms_order_id: str
    account_id: str
    trading_day: TradingDayEpoch
    consumer_trace: ConsumerTrace
    symbol: str
    market: str
    side: str
    price_type: str
    business_type: str
    quantity: int
    protection_price_ticks: int
    limit_price_ticks: int | None = None
    execution_parameters: Mapping[str, str] = field(default_factory=_empty_text_mapping)

    def __post_init__(self) -> None:
        require_text(self.execution_id, "execution_id")
        require_text(self.oms_order_id, "oms_order_id")
        require_text(self.account_id, "account_id")
        require_text(self.symbol, "symbol", max_length=64)
        require_text(self.market, "market", max_length=64)
        require_text(self.side, "side", max_length=64)
        require_text(self.price_type, "price_type", max_length=64)
        require_text(self.business_type, "business_type", max_length=64)
        require_positive(self.quantity, "execution quantity")
        require_nonnegative(self.protection_price_ticks, "protection_price_ticks")
        if self.limit_price_ticks is not None:
            require_nonnegative(self.limit_price_ticks, "limit_price_ticks")
        if self.consumer_trace.account_id != self.account_id:
            raise DomainValidationError("execution account does not match consumer trace")
        object.__setattr__(
            self,
            "execution_parameters",
            freeze_text_mapping(self.execution_parameters, "execution_parameters"),
        )


@dataclass(frozen=True)
class CancelRequest:
    """A request to cancel one known execution through the normal EMS chain."""

    cancel_request_id: str
    oms_order_id: str
    execution_id: str
    account_id: str
    consumer_trace: ConsumerTrace
    reason: str
    requested_monotonic_ns: int

    def __post_init__(self) -> None:
        require_text(self.cancel_request_id, "cancel_request_id")
        require_text(self.oms_order_id, "oms_order_id")
        require_text(self.execution_id, "execution_id")
        require_text(self.account_id, "account_id")
        require_text(self.reason, "cancel reason", max_length=256)
        require_nonnegative(self.requested_monotonic_ns, "cancel requested time")
        if self.consumer_trace.account_id != self.account_id:
            raise DomainValidationError("cancel account does not match consumer trace")


@dataclass(frozen=True)
class CallbackGapEvidence:
    """A conservative description of a callback-ingress gap requiring reconciliation."""

    evidence_id: str
    account_id: str
    producer_id: str
    first_rejected_sequence: int
    last_rejected_sequence: int
    rejected_count: int
    observed_monotonic_ns: int

    def __post_init__(self) -> None:
        require_text(self.evidence_id, "callback gap evidence_id")
        require_text(self.account_id, "callback gap account_id")
        require_text(self.producer_id, "callback gap producer_id")
        require_nonnegative(self.first_rejected_sequence, "callback gap first sequence")
        require_nonnegative(self.last_rejected_sequence, "callback gap last sequence")
        require_positive(self.rejected_count, "callback gap rejected_count")
        require_nonnegative(self.observed_monotonic_ns, "callback gap observed time")
        if self.last_rejected_sequence < self.first_rejected_sequence:
            raise DomainValidationError("callback gap last sequence precedes first sequence")
        if self.rejected_count > self.last_rejected_sequence - self.first_rejected_sequence + 1:
            raise DomainValidationError("callback gap count exceeds its sequence range")


@dataclass(frozen=True)
class RecoveryPlan:
    """The explicit, read-only recovery scope given to a BrokerAdapter."""

    recovery_run_id: str
    account_id: str
    venue_id: str
    adapter_instance_id: str
    trading_day: TradingDayEpoch
    journal_id: str
    expected_journal_lsn: int
    callback_gaps: tuple[CallbackGapEvidence, ...] = ()

    def __post_init__(self) -> None:
        require_text(self.recovery_run_id, "recovery_run_id")
        require_text(self.account_id, "recovery account_id")
        require_text(self.venue_id, "recovery venue_id")
        require_text(self.adapter_instance_id, "recovery adapter_instance_id")
        require_text(self.journal_id, "recovery journal_id")
        require_nonnegative(self.expected_journal_lsn, "recovery expected journal LSN")
        callback_gaps = tuple(self.callback_gaps)
        gap_ids = [gap.evidence_id for gap in callback_gaps]
        if len(gap_ids) != len(set(gap_ids)):
            raise DomainValidationError("recovery plan contains duplicate callback gap evidence")
        if any(gap.account_id != self.account_id for gap in callback_gaps):
            raise DomainValidationError("callback gap evidence belongs to another account")
        object.__setattr__(self, "callback_gaps", callback_gaps)


@dataclass(frozen=True)
class BrokerRecoveryEvidence:
    """Normalized, completeness-explicit recovery evidence returned by an adapter."""

    evidence_id: str
    recovery_run_id: str
    account_id: str
    venue_id: str
    adapter_instance_id: str
    trading_day: TradingDayEpoch
    callback_replay_complete: bool
    orders_complete: bool
    trades_complete: bool
    working_orders_complete: bool
    risk_snapshot_complete: bool
    negative_confirmation_complete: bool
    callback_gaps: tuple[CallbackGapEvidence, ...] = ()
    unresolved_order_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_text(self.evidence_id, "broker recovery evidence_id")
        require_text(self.recovery_run_id, "broker recovery run_id")
        require_text(self.account_id, "broker recovery account_id")
        require_text(self.venue_id, "broker recovery venue_id")
        require_text(self.adapter_instance_id, "broker recovery adapter_instance_id")
        unresolved = tuple(
            require_text(order_id, "unresolved_order_id") for order_id in self.unresolved_order_ids
        )
        if len(unresolved) != len(set(unresolved)):
            raise DomainValidationError("recovery evidence repeats an unresolved order")
        callback_gaps = tuple(self.callback_gaps)
        if any(gap.account_id != self.account_id for gap in callback_gaps):
            raise DomainValidationError("recovery callback gap belongs to another account")
        object.__setattr__(self, "callback_gaps", callback_gaps)
        object.__setattr__(self, "unresolved_order_ids", unresolved)

    @property
    def is_complete(self) -> bool:
        """Return true only for evidence sufficient to attempt recovery validation."""

        return (
            self.callback_replay_complete
            and self.orders_complete
            and self.trades_complete
            and self.working_orders_complete
            and self.risk_snapshot_complete
            and self.negative_confirmation_complete
            and not self.callback_gaps
            and not self.unresolved_order_ids
        )


@dataclass(frozen=True)
class RecoveryRun:
    """A durable recovery outcome; M3 decides whether it can release an account."""

    recovery_run_id: str
    account_id: str
    started_monotonic_ns: int
    completed_monotonic_ns: int | None
    reconciliation_status: ReconciliationStatus
    broker_recovery_evidence_id: str | None

    def __post_init__(self) -> None:
        require_text(self.recovery_run_id, "recovery_run_id")
        require_text(self.account_id, "recovery run account_id")
        require_nonnegative(self.started_monotonic_ns, "recovery started time")
        if self.completed_monotonic_ns is not None:
            require_nonnegative(self.completed_monotonic_ns, "recovery completed time")
            if self.completed_monotonic_ns < self.started_monotonic_ns:
                raise DomainValidationError("recovery completion precedes its start")
        if self.reconciliation_status is ReconciliationStatus.VALIDATED:
            if self.completed_monotonic_ns is None or self.broker_recovery_evidence_id is None:
                raise DomainValidationError(
                    "validated recovery requires completion and broker recovery evidence"
                )
        if self.broker_recovery_evidence_id is not None:
            require_text(self.broker_recovery_evidence_id, "broker_recovery_evidence_id")


@dataclass(frozen=True)
class BrokerEvent:
    """A normalized adapter event with a stable, versioned source key."""

    event_id: str
    schema_version: int
    source_key_schema_version: int
    source_key: str
    event_kind: BrokerEventKind
    source_kind: BrokerSourceKind
    venue_id: str
    adapter_instance_id: str
    account_id: str
    trading_day: TradingDayEpoch
    received_monotonic_ns: int
    venue_order_identity: VenueOrderIdentity | None = None
    execution_id: str | None = None
    cancel_request_id: str | None = None
    trade_key: str | None = None
    filled_qty: int | None = None
    leaves_qty: int | None = None
    error_category: ErrorCategory | None = None
    normalized_summary: Mapping[str, str] = field(default_factory=_empty_text_mapping)

    def __post_init__(self) -> None:
        require_text(self.event_id, "broker event_id")
        require_positive(self.schema_version, "broker event schema_version")
        require_positive(self.source_key_schema_version, "source_key_schema_version")
        require_text(self.source_key, "source_key", max_length=256)
        require_text(self.venue_id, "broker event venue_id")
        require_text(self.adapter_instance_id, "broker event adapter_instance_id")
        require_text(self.account_id, "broker event account_id")
        require_nonnegative(self.received_monotonic_ns, "broker event received time")
        if self.venue_order_identity is not None:
            if self.venue_order_identity.venue_id != self.venue_id:
                raise DomainValidationError("broker event venue identity has another venue")
            if self.venue_order_identity.adapter_instance_id != self.adapter_instance_id:
                raise DomainValidationError("broker event identity has another adapter instance")
        for field_name, value in (
            ("execution_id", self.execution_id),
            ("cancel_request_id", self.cancel_request_id),
            ("trade_key", self.trade_key),
        ):
            if value is not None:
                require_text(value, field_name)
        if self.event_kind is BrokerEventKind.TRADE and self.trade_key is None:
            raise DomainValidationError("trade broker event requires trade_key")
        if self.event_kind is BrokerEventKind.CANCEL_ERROR and self.cancel_request_id is None:
            raise DomainValidationError("cancel error broker event requires cancel_request_id")
        if self.filled_qty is not None:
            require_nonnegative(self.filled_qty, "broker event filled_qty")
        if self.leaves_qty is not None:
            require_nonnegative(self.leaves_qty, "broker event leaves_qty")
        object.__setattr__(
            self,
            "normalized_summary",
            freeze_text_mapping(self.normalized_summary, "broker event normalized_summary"),
        )


@dataclass(frozen=True)
class RiskSnapshot:
    """A versioned, bounded risk input snapshot for future reservation decisions."""

    risk_snapshot_id: str
    account_id: str
    trading_day: TradingDayEpoch
    rule_bundle_hash: str
    captured_monotonic_ns: int
    expires_monotonic_ns: int
    available_cash_minor: int
    available_margin_minor: int
    sellable_quantities: Mapping[str, int] = field(default_factory=_empty_nonnegative_int_mapping)
    borrowable_quantities: Mapping[str, int] = field(default_factory=_empty_nonnegative_int_mapping)

    def __post_init__(self) -> None:
        require_text(self.risk_snapshot_id, "risk_snapshot_id")
        require_text(self.account_id, "risk snapshot account_id")
        require_sha256(self.rule_bundle_hash, "risk snapshot rule_bundle_hash")
        require_nonnegative(self.captured_monotonic_ns, "risk snapshot captured time")
        require_nonnegative(self.expires_monotonic_ns, "risk snapshot expiry time")
        if self.expires_monotonic_ns <= self.captured_monotonic_ns:
            raise DomainValidationError("risk snapshot expiry must be after capture time")
        require_nonnegative(self.available_cash_minor, "available_cash_minor")
        require_nonnegative(self.available_margin_minor, "available_margin_minor")
        object.__setattr__(
            self,
            "sellable_quantities",
            freeze_nonnegative_int_mapping(self.sellable_quantities, "sellable_quantities"),
        )
        object.__setattr__(
            self,
            "borrowable_quantities",
            freeze_nonnegative_int_mapping(self.borrowable_quantities, "borrowable_quantities"),
        )

    def is_fresh_at(self, monotonic_ns: int) -> bool:
        """Return whether this snapshot can still be used at the supplied monotonic time."""

        require_nonnegative(monotonic_ns, "risk snapshot comparison time")
        return monotonic_ns < self.expires_monotonic_ns


@dataclass(frozen=True)
class DecisionContext:
    """Immutable risk and pricing evidence used when an OMS accepts one intent.

    The context is a business fact, not an execution result.  It deliberately
    keeps only versioned model identifiers and fixed-point values, so a later
    Journal reader can explain why a reservation was created without retaining
    a mutable rule object or a raw consumer payload.
    """

    decision_context_id: str
    account_id: str
    trading_day: TradingDayEpoch
    risk_snapshot_id: str
    risk_rule_bundle_hash: str
    protection_price_ticks: int
    fee_model_version: str
    margin_model_version: str
    reservation_model_version: str
    decided_monotonic_ns: int
    decision_bundle_hash: str | None = None
    decision_bundle_epoch: int | None = None

    def __post_init__(self) -> None:
        require_text(self.decision_context_id, "decision context id")
        require_text(self.account_id, "decision context account_id")
        require_text(self.risk_snapshot_id, "decision context risk_snapshot_id")
        require_sha256(self.risk_rule_bundle_hash, "decision context risk_rule_bundle_hash")
        require_nonnegative(self.protection_price_ticks, "decision context protection price")
        require_text(self.fee_model_version, "decision context fee model version")
        require_text(self.margin_model_version, "decision context margin model version")
        require_text(self.reservation_model_version, "decision context reservation model version")
        require_nonnegative(self.decided_monotonic_ns, "decision context decided time")
        if (self.decision_bundle_hash is None) != (self.decision_bundle_epoch is None):
            raise DomainValidationError(
                "decision context bundle hash and epoch must be present together"
            )
        if self.decision_bundle_hash is not None:
            require_sha256(self.decision_bundle_hash, "decision context decision_bundle_hash")
            require_positive(self.decision_bundle_epoch, "decision context decision_bundle_epoch")


@dataclass(frozen=True)
class ConsumerEventCursor:
    """The unsigned domain representation of an account-scoped consumer cursor."""

    consumer_id: str
    consumer_account_id: str
    account_id: str
    journal_id: str
    stream_generation: int
    journal_lsn: int
    schema_version: int

    def __post_init__(self) -> None:
        require_text(self.consumer_id, "cursor consumer_id")
        require_text(self.consumer_account_id, "cursor consumer_account_id")
        require_text(self.account_id, "cursor account_id")
        require_text(self.journal_id, "cursor journal_id")
        require_positive(self.stream_generation, "cursor stream_generation")
        require_nonnegative(self.journal_lsn, "cursor journal_lsn")
        require_positive(self.schema_version, "cursor schema_version")


@dataclass(frozen=True)
class ConsumerEvent:
    """Base consumer event preserving Journal lineage and binding identity."""

    consumer_event_id: str
    event_kind: ConsumerEventKind
    schema_version: int
    account_id: str
    trading_day: TradingDayEpoch
    journal_id: str
    journal_lsn: int
    consumer_trace: ConsumerTrace

    def __post_init__(self) -> None:
        require_text(self.consumer_event_id, "consumer_event_id")
        require_positive(self.schema_version, "consumer event schema_version")
        require_text(self.account_id, "consumer event account_id")
        require_text(self.journal_id, "consumer event journal_id")
        require_nonnegative(self.journal_lsn, "consumer event journal_lsn")
        if self.consumer_trace.account_id != self.account_id:
            raise DomainValidationError("consumer event account does not match consumer trace")


@dataclass(frozen=True)
class ConsumerOrderEvent(ConsumerEvent):
    """A consumer-visible OMS order state event."""

    oms_order_id: str
    execution_id: str | None
    order_state: OmsOrderState
    order_revision: int
    venue_order_identity: VenueOrderIdentity | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.event_kind is not ConsumerEventKind.ORDER:
            raise DomainValidationError("consumer order event must use ORDER event kind")
        require_text(self.oms_order_id, "consumer order oms_order_id")
        require_nonnegative(self.order_revision, "consumer order revision")
        if self.execution_id is not None:
            require_text(self.execution_id, "consumer order execution_id")


@dataclass(frozen=True)
class ConsumerAccountStatusEvent(ConsumerEvent):
    """A consumer-visible account lifecycle and reconciliation state event."""

    lifecycle: AccountLifecycle
    reconciliation_status: ReconciliationStatus
    gates: tuple[AdmissionGate, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.event_kind is not ConsumerEventKind.ACCOUNT_STATUS:
            raise DomainValidationError("account status event must use ACCOUNT_STATUS event kind")
        gates = tuple(self.gates)
        gate_ids = [gate.gate_id for gate in gates]
        if len(gate_ids) != len(set(gate_ids)):
            raise DomainValidationError("account status event repeats an admission gate")
        gate_kinds = {gate.kind for gate in gates}
        if self.lifecycle is AccountLifecycle.READY:
            if self.reconciliation_status is not ReconciliationStatus.VALIDATED or gates:
                raise DomainValidationError("READY requires VALIDATED reconciliation and no gates")
        if self.lifecycle is AccountLifecycle.SIMULATOR_ACTIVE and (
            self.reconciliation_status is not ReconciliationStatus.NOT_REQUIRED or gates
        ):
            raise DomainValidationError("SIMULATOR_ACTIVE requires NOT_REQUIRED and no gates")
        if self.reconciliation_status is ReconciliationStatus.REQUIRED:
            if AdmissionGateKind.RECONCILIATION not in gate_kinds:
                raise DomainValidationError(
                    "REQUIRED reconciliation requires a RECONCILIATION gate"
                )
        if self.lifecycle is AccountLifecycle.RECOVERING:
            if self.reconciliation_status is not ReconciliationStatus.RUNNING:
                raise DomainValidationError("RECOVERING requires RUNNING reconciliation")
            if AdmissionGateKind.RECONCILIATION not in gate_kinds:
                raise DomainValidationError("RECOVERING requires a RECONCILIATION gate")
        object.__setattr__(self, "gates", gates)


@dataclass(frozen=True)
class ConsumerAccountRiskEvent(ConsumerEvent):
    """A consumer-visible, payload-limited account risk status event."""

    risk_snapshot_id: str | None
    error_category: ErrorCategory
    summary: Mapping[str, str] = field(default_factory=_empty_text_mapping)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.event_kind is not ConsumerEventKind.ACCOUNT_RISK:
            raise DomainValidationError("account risk event must use ACCOUNT_RISK event kind")
        if self.risk_snapshot_id is not None:
            require_text(self.risk_snapshot_id, "risk event risk_snapshot_id")
        object.__setattr__(self, "summary", freeze_text_mapping(self.summary, "risk event summary"))


@dataclass(frozen=True)
class ConsumerAuditTrail:
    """A non-authoritative pointer chain to durable Journal and control evidence."""

    audit_id: str
    consumer_id: str
    account_id: str
    oms_order_id: str | None
    execution_id: str | None
    journal_id: str
    journal_lsn: int
    action: str
    decision_context_hash: str | None = None

    def __post_init__(self) -> None:
        require_text(self.audit_id, "consumer audit_id")
        require_text(self.consumer_id, "consumer audit consumer_id")
        require_text(self.account_id, "consumer audit account_id")
        require_text(self.journal_id, "consumer audit journal_id")
        require_nonnegative(self.journal_lsn, "consumer audit journal_lsn")
        require_text(self.action, "consumer audit action", max_length=128)
        if self.oms_order_id is not None:
            require_text(self.oms_order_id, "consumer audit oms_order_id")
        if self.execution_id is not None:
            require_text(self.execution_id, "consumer audit execution_id")
        if self.decision_context_hash is not None:
            require_sha256(self.decision_context_hash, "consumer audit decision_context_hash")

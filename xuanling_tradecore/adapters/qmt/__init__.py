"""QMT Adapter contracts, Edge bridge implementation, and no-SDK fixtures."""

from .bridge_adapter import (
    QmtBrokerAdapter,
    QmtBrokerAdapterError,
    QmtBrokerAdapterSnapshot,
    QmtMutationUnavailableError,
)
from .bridge_normalizer import QmtBridgeEventConflictError, QmtBridgeEventNormalizer
from .bridge_protocol import (
    QmtBridgeCancelReceipt,
    QmtBridgeDelivery,
    QmtBridgeDeliverySink,
    QmtBridgeError,
    QmtBridgeEventDelivery,
    QmtBridgeGapDelivery,
    QmtBridgeHealth,
    QmtBridgeNativeEvent,
    QmtBridgePort,
    QmtBridgeSubmitReceipt,
    QmtSubmitCommand,
)
from .bridge_transport import QmtBridgeLifecycle, QmtBridgeTransportSnapshot
from .bridge_wire import (
    QmtBridgeWireError,
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)
from .capability import (
    QmtCapabilityActivation,
    QmtCapabilityError,
    QmtCapabilityManifestV1,
    QmtEd25519CapabilityVerifierV1,
)
from .edge_command import QmtEdgeCommandError, QmtEdgeCommandKind, QmtEdgeCommandV1
from .edge_config import (
    QmtEdgeConfigError,
    QmtEdgeConfigV1,
    QmtNativePolicyV1,
    QmtNativeSubmitRouteV1,
)
from .edge_controller import (
    QmtEdgeCommandAcceptance,
    QmtEdgeCommandAdmissionError,
    QmtEdgeCommandController,
)
from .edge_event_endpoint import QmtEdgeEventEndpoint
from .edge_native_encoder import QmtEdgeNativeEncoder, QmtEdgeNativeEncodingError
from .edge_runtime import (
    MIN_QMT_EDGE_SQLITE_VERSION,
    QmtEdgeRuntime,
    QmtEdgeRuntimeSnapshot,
    require_supported_qmt_edge_sqlite,
)
from .edge_service import QmtEdgeService, QmtEdgeServiceError, QmtEdgeServiceSnapshot
from .edge_store import (
    QmtEdgeIdempotencyConflict,
    QmtEdgeOutboxRecord,
    QmtEdgeRequest,
    QmtEdgeStore,
    QmtEdgeStoreError,
)
from .event_wire import QmtEventStreamConfig, QmtEventWireCodec, QmtEventWireError
from .fake import FakeQmtBrokerAdapter, FakeQmtBrokerAdapterError, FakeQmtBrokerAdapterSnapshot
from .mapping import QmtMappingRegistry, load_builtin_qmt_fake_manifest
from .mtls_bridge import QmtMtlsBridgeConfig, QmtMtlsBridgeError, QmtMtlsBridgePort
from .mtls_channel import (
    QmtMtlsChannelError,
    QmtMtlsClientConfig,
    QmtMtlsConnection,
    QmtMtlsServerConfig,
    create_qmt_mtls_server_context,
    verify_qmt_mtls_client,
)
from .native_engine_client import (
    QmtNativeEngineClient,
    QmtNativeEngineClientError,
    QmtNativePublishResult,
)
from .native_engine_contract import (
    QmtNativeContractError,
    QmtNativeFrameKind,
    QmtNativeFrameV1,
    QmtRingHeaderV1,
    qmt_native_abi_digest,
)
from .native_payload import (
    QmtNativeCancelPayloadV1,
    QmtNativeQueryPayloadV1,
    QmtNativeSubmitPayloadV1,
)
from .profile import QmtAdapterProfile
from .provisioning_edge_v1 import QmtEdgeCredentialReceiverV1
from .route_token import QmtOrderRemarkCodec, qmt_route_token_binding_v1
from .windows_ring import (
    QmtNativeAtomicLibrary,
    QmtNativeRing,
    QmtNativeRingEmpty,
    QmtNativeRingFull,
    QmtNativeRingSnapshot,
)

__all__ = [
    "QmtEdgeEventEndpoint",
    "QmtEventStreamConfig",
    "QmtEventWireCodec",
    "QmtEventWireError",
    "QmtBrokerAdapter",
    "QmtBrokerAdapterError",
    "QmtBrokerAdapterSnapshot",
    "QmtCapabilityActivation",
    "QmtCapabilityError",
    "QmtCapabilityManifestV1",
    "QmtEd25519CapabilityVerifierV1",
    "QmtBridgeCancelReceipt",
    "QmtBridgeDelivery",
    "QmtBridgeDeliverySink",
    "QmtBridgeError",
    "QmtBridgeEventConflictError",
    "QmtBridgeEventDelivery",
    "QmtBridgeEventNormalizer",
    "QmtBridgeGapDelivery",
    "QmtBridgeHealth",
    "QmtBridgeLifecycle",
    "QmtBridgeNativeEvent",
    "QmtBridgePort",
    "QmtBridgeSubmitReceipt",
    "QmtBridgeTransportSnapshot",
    "QmtBridgeWireError",
    "QmtBridgeWireFrame",
    "QmtBridgeWireKind",
    "QmtEdgeIdempotencyConflict",
    "QmtEdgeConfigError",
    "QmtEdgeConfigV1",
    "QmtEdgeCommandError",
    "QmtEdgeCommandKind",
    "QmtEdgeCommandV1",
    "QmtEdgeCommandAcceptance",
    "QmtEdgeCommandAdmissionError",
    "QmtEdgeCommandController",
    "QmtEdgeNativeEncoder",
    "QmtEdgeNativeEncodingError",
    "QmtEdgeOutboxRecord",
    "QmtEdgeRequest",
    "QmtEdgeRuntime",
    "QmtEdgeRuntimeSnapshot",
    "QmtEdgeService",
    "QmtEdgeServiceError",
    "QmtEdgeServiceSnapshot",
    "QmtEdgeStore",
    "QmtEdgeStoreError",
    "QmtNativeContractError",
    "QmtNativeEngineClient",
    "QmtNativeEngineClientError",
    "QmtNativeCancelPayloadV1",
    "QmtNativeAtomicLibrary",
    "QmtNativeFrameKind",
    "QmtNativeFrameV1",
    "QmtNativeQueryPayloadV1",
    "QmtNativePublishResult",
    "QmtNativePolicyV1",
    "QmtNativeRing",
    "QmtNativeRingEmpty",
    "QmtNativeRingFull",
    "QmtNativeRingSnapshot",
    "QmtNativeSubmitPayloadV1",
    "QmtNativeSubmitRouteV1",
    "QmtRingHeaderV1",
    "QmtMutationUnavailableError",
    "QmtMtlsChannelError",
    "QmtMtlsBridgeConfig",
    "QmtMtlsBridgeError",
    "QmtMtlsBridgePort",
    "QmtMtlsClientConfig",
    "QmtMtlsConnection",
    "QmtMtlsServerConfig",
    "QmtSubmitCommand",
    "canonical_wire_payload",
    "create_qmt_mtls_server_context",
    "decode_canonical_wire_payload",
    "MIN_QMT_EDGE_SQLITE_VERSION",
    "require_supported_qmt_edge_sqlite",
    "verify_qmt_mtls_client",
    "qmt_native_abi_digest",
    "FakeQmtBrokerAdapter",
    "FakeQmtBrokerAdapterError",
    "FakeQmtBrokerAdapterSnapshot",
    "QmtAdapterProfile",
    "QmtEdgeCredentialReceiverV1",
    "QmtMappingRegistry",
    "QmtOrderRemarkCodec",
    "load_builtin_qmt_fake_manifest",
    "qmt_route_token_binding_v1",
]

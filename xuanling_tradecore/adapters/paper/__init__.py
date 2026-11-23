"""TradeCore-only PaperBroker connector/profile boundary."""

from .adapter import (
    PaperBrokerAdapter,
    PaperBrokerAdapterError,
    PaperBrokerAdapterSnapshot,
    PaperBrokerMutationUnavailableError,
)
from .connector import (
    PaperBrokerCancelReceipt,
    PaperBrokerConnectorError,
    PaperBrokerHealth,
    PaperBrokerNativeEvent,
    PaperBrokerOrderRequest,
    PaperBrokerSessionPort,
    PaperBrokerSubmitReceipt,
)
from .dependency import (
    PaperBrokerDependencyError,
    PaperBrokerDependencyManifest,
    PaperBrokerDeploymentMode,
    PaperBrokerRuntimeIdentity,
    load_paperbroker_dependency_manifest_json,
)
from .mapping import PaperBrokerMappingRegistry, load_builtin_paperbroker_contract_fixture
from .normalizer import PaperBrokerEventConflictError, PaperBrokerEventNormalizer
from .profile import PaperBrokerAdapterProfile

__all__ = [
    "PaperBrokerAdapter",
    "PaperBrokerAdapterError",
    "PaperBrokerAdapterProfile",
    "PaperBrokerAdapterSnapshot",
    "PaperBrokerCancelReceipt",
    "PaperBrokerConnectorError",
    "PaperBrokerDependencyError",
    "PaperBrokerDependencyManifest",
    "PaperBrokerDeploymentMode",
    "PaperBrokerEventConflictError",
    "PaperBrokerEventNormalizer",
    "PaperBrokerHealth",
    "PaperBrokerMappingRegistry",
    "PaperBrokerMutationUnavailableError",
    "PaperBrokerNativeEvent",
    "PaperBrokerOrderRequest",
    "PaperBrokerRuntimeIdentity",
    "PaperBrokerSessionPort",
    "PaperBrokerSubmitReceipt",
    "load_paperbroker_dependency_manifest_json",
    "load_builtin_paperbroker_contract_fixture",
]

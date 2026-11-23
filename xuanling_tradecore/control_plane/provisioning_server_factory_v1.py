"""Explicit, no-secret composition boundary for the Linux control listener.

Site deployment code owns protected paths, TLS material, recipient private
keys and the actual validator/relay integrations.  This module deliberately
does not invent a default, read environment variables or fall back to an
in-memory provider.  It verifies that only durable repository/provider
implementations can be wired into the HTTP/2 mTLS listener.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .postgres_provisioning_v1 import (
    PostgresProvisioningApprovalRegistryV1,
    PostgresProvisioningRegistryV1,
)
from .provisioning_approval_v1 import ProvisioningApprovalRegistryV1
from .provisioning_contracts_v1 import ProvisioningErrorV1
from .provisioning_http_v1 import MtlsPeerVerifierV1, ProvisioningHttpRouterV1
from .provisioning_registry_v1 import ProvisioningRegistryV1
from .provisioning_secret_v1 import InMemorySecretProviderV1, SecretProviderV1
from .provisioning_server_v1 import (
    ProvisioningHttp2ServerConfigV1,
    ProvisioningHttp2ServerV1,
    build_provisioning_server_ssl_context_v1,
)
from .provisioning_service_v1 import ProvisioningControlServiceV1


@dataclass(frozen=True)
class ProvisioningTlsFilesV1:
    """Protected path references supplied by root-owned deployment code."""

    certificate_file: str
    private_key_file: str
    client_ca_file: str


def build_linux_provisioning_server_v1(
    *,
    service: ProvisioningControlServiceV1,
    peer_verifier: MtlsPeerVerifierV1,
    approvals: ProvisioningApprovalRegistryV1,
    registry: ProvisioningRegistryV1,
    secret_providers: Mapping[str, SecretProviderV1],
    listener: ProvisioningHttp2ServerConfigV1,
    tls_files: ProvisioningTlsFilesV1,
) -> ProvisioningHttp2ServerV1:
    """Compose a production-shaped server without hidden test fallbacks.

    The validated components are passed explicitly so the host-owned factory
    can select a KMS/HSM provider or the Linux protected-file provider.  A
    caller cannot accidentally expose the test-only in-memory registry or
    provider by invoking the normal deployment entrypoint.
    """

    if not isinstance(registry, PostgresProvisioningRegistryV1):
        raise ProvisioningErrorV1(
            "CONTROL_DEPLOYMENT", "Linux control server requires PostgreSQL registry"
        )
    if not isinstance(approvals, PostgresProvisioningApprovalRegistryV1):
        raise ProvisioningErrorV1(
            "CONTROL_DEPLOYMENT", "Linux control server requires PostgreSQL approvals"
        )
    if not secret_providers:
        raise ProvisioningErrorV1("CONTROL_DEPLOYMENT", "Linux control server has no secret provider")
    for provider in secret_providers.values():
        if isinstance(provider, InMemorySecretProviderV1) or provider.provider_kind.startswith(
            "IN_MEMORY"
        ):
            raise ProvisioningErrorV1(
                "CONTROL_DEPLOYMENT", "Linux control server rejects test secret provider"
            )
    router = ProvisioningHttpRouterV1(
        service=service,
        peer_verifier=peer_verifier,
        approvals=approvals,
    )
    ssl_context = build_provisioning_server_ssl_context_v1(
        certificate_file=tls_files.certificate_file,
        private_key_file=tls_files.private_key_file,
        client_ca_file=tls_files.client_ca_file,
    )
    return ProvisioningHttp2ServerV1(router=router, config=listener, ssl_context=ssl_context)

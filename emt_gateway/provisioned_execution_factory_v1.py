"""Credential-free composition of a provisioned EMT execution factory.

The Strategy/Consumer/EMS data plane never imports this module.  A privileged
runtime bootstrapper supplies only an already activated profile revision and
an opaque ``SecretReferenceV1``.  This module borrows material for one short
lease, constructs the outer-platform session settings, invokes the supplied
native factory builder, and then releases its local reference.  It does not
persist, expose, serialize, or log broker credentials.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hmac import compare_digest
from pathlib import Path
from threading import Lock
from time import time_ns
from typing import Callable, Protocol, TypeVar

from xuanling_tradecore.adapters.emt import (
    EmtBrokerEventIngress,
    EmtExecutionSdkSession,
    EmtQueryCallbackIngress,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileRevisionV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ProfileStateV1,
    ProvisioningErrorV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import (
    SecretLeasePurposeV1,
    SecretProviderV1,
)

from .session import SessionSettings


class ProvisionedEmtExecutionFactoryErrorV1(RuntimeError):
    """A provisioned EMT session cannot safely be opened."""


class _OpenedExecutionFactoryV1(Protocol):
    """The small outer-platform surface needed after a credential lease."""

    def open(
        self,
        *,
        query_callback_ingress: EmtQueryCallbackIngress,
        broker_event_ingress: EmtBrokerEventIngress,
    ) -> EmtExecutionSdkSession:
        """Open one native execution session after both observers attach."""

        ...


_OpenedFactory = TypeVar("_OpenedFactory", bound=_OpenedExecutionFactoryV1)


@dataclass(frozen=True)
class ProvisionedEmtExecutionProfileV1:
    """Non-secret runtime inputs for one activated EMT profile revision."""

    profile: AdapterProfileRevisionV1
    sdk_log_dir: Path = field(repr=False)
    expected_client_id: int
    account_profile: str = "ordinary"
    local_ip: str = field(default="", repr=False)
    public_topic_resume_type: int = 2
    heartbeat_seconds: int = 10
    software_version: str = "emt-gateway/0.1"
    sdk_log_level: int = 0
    sdk_receive_cpu: int | None = None
    sdk_process_cpu: int | None = None

    def __post_init__(self) -> None:
        if self.profile.identity.adapter_kind is not AdapterKindV1.EMT:
            raise ProvisionedEmtExecutionFactoryErrorV1("provisioned profile is not EMT")
        if self.profile.state is not ProfileStateV1.ACTIVE_DISARMED:
            raise ProvisionedEmtExecutionFactoryErrorV1(
                "EMT execution requires an ACTIVE_DISARMED profile"
            )
        if self.account_profile not in {"ordinary", "credit", "option"}:
            raise ProvisionedEmtExecutionFactoryErrorV1("EMT account profile is invalid")
        if type(self.expected_client_id) is not int or not 1 <= self.expected_client_id <= 255:
            raise ProvisionedEmtExecutionFactoryErrorV1("EMT expected client scope is invalid")


class ProvisionedEmtExecutionSessionFactoryV1:
    """Open exactly one native EMT session from one scope-bound secret lease.

    The factory is deliberately single-use.  Reconnect/restart needs a new
    bootstrap and a new lease so stale material cannot silently remain active.
    ``factory_builder`` is outer-platform code, normally a constructor for
    :class:`EmtPlatformExecutionSessionFactory`; it is never called by a
    strategy, Consumer, OMS, or EMS worker.
    """

    _LEASE_TTL_NS = 15_000_000_000

    def __init__(
        self,
        *,
        runtime_profile: ProvisionedEmtExecutionProfileV1,
        secret_provider: SecretProviderV1,
        factory_builder: Callable[[SessionSettings], _OpenedFactory],
        now_wall_ns: Callable[[], int] = time_ns,
    ) -> None:
        if not callable(factory_builder) or not callable(now_wall_ns):
            raise ProvisionedEmtExecutionFactoryErrorV1("EMT provisioned factory dependency is invalid")
        self._runtime_profile = runtime_profile
        self._secret_provider = secret_provider
        self._factory_builder = factory_builder
        self._now_wall_ns = now_wall_ns
        self._opened = False
        self._lock = Lock()

    @property
    def profile_id(self) -> str:
        """Return the non-secret profile identity used by this factory."""

        return self._runtime_profile.profile.identity.profile_id

    def open(
        self,
        *,
        query_callback_ingress: EmtQueryCallbackIngress,
        broker_event_ingress: EmtBrokerEventIngress,
    ) -> EmtExecutionSdkSession:
        """Create a session using the matching short lease exactly once."""

        with self._lock:
            if self._opened:
                raise ProvisionedEmtExecutionFactoryErrorV1(
                    "provisioned EMT execution factory is already consumed"
                )
            self._opened = True
        profile = self._runtime_profile.profile
        purpose = SecretLeasePurposeV1(
            adapter_instance_id=profile.identity.adapter_instance_id,
            account_scope=profile.identity.profile_id,
            operation="EMT_EXECUTION_SESSION_OPEN",
        )
        try:
            with self._secret_provider.lease(
                reference=profile.secret_reference,
                purpose=purpose,
                now_wall_ns=self._now_wall_ns(),
                ttl_wall_ns=self._LEASE_TTL_NS,
            ) as lease:
                material = lease.borrow(now_wall_ns=self._now_wall_ns())
                settings = self._settings_from_material(material)
                native_factory = self._factory_builder(settings)
                return native_factory.open(
                    query_callback_ingress=query_callback_ingress,
                    broker_event_ingress=broker_event_ingress,
                )
        except ProvisioningErrorV1 as exc:
            raise ProvisionedEmtExecutionFactoryErrorV1(
                "EMT provisioned secret lease is unavailable"
            ) from exc
        except ProvisionedEmtExecutionFactoryErrorV1:
            raise
        except Exception as exc:
            raise ProvisionedEmtExecutionFactoryErrorV1(
                "EMT provisioned execution session startup failed"
            ) from exc

    def _settings_from_material(self, material: object) -> SessionSettings:
        profile = self._runtime_profile.profile
        if not isinstance(material, BrokerCredentialMaterialV1) or not (
            compare_digest(material.credential_digest(), profile.material_digest.credential_digest)
            and compare_digest(
                material.connection.endpoint_digest(), profile.material_digest.endpoint_digest
            )
        ):
            raise ProvisionedEmtExecutionFactoryErrorV1(
                "EMT leased credential material does not match the activated profile"
            )
        broker_account = getattr(material, "broker_account", None)
        password = getattr(material, "password", None)
        connection = getattr(material, "connection", None)
        raw_client_id = getattr(material, "client_id", None)
        if (
            not isinstance(broker_account, str)
            or not isinstance(password, str)
            or not isinstance(connection, BrokerConnectionV1)
            or connection.kind != "BROKER_TCP"
            or not isinstance(raw_client_id, str)
            or not raw_client_id.isdecimal()
        ):
            raise ProvisionedEmtExecutionFactoryErrorV1("EMT provisioned secret material is invalid")
        client_id = int(raw_client_id)
        if not 1 <= client_id <= 255:
            raise ProvisionedEmtExecutionFactoryErrorV1("EMT provisioned client scope is invalid")
        if client_id != self._runtime_profile.expected_client_id:
            raise ProvisionedEmtExecutionFactoryErrorV1("EMT provisioned client scope does not match")
        settings = SessionSettings(
            host=connection.host,
            port=connection.port,
            account=broker_account,
            password=password,
            client_id=client_id,
            sdk_log_dir=self._runtime_profile.sdk_log_dir,
            account_profile=self._runtime_profile.account_profile,
            local_ip=self._runtime_profile.local_ip,
            public_topic_resume_type=self._runtime_profile.public_topic_resume_type,
            heartbeat_seconds=self._runtime_profile.heartbeat_seconds,
            software_version=self._runtime_profile.software_version,
            sdk_log_level=self._runtime_profile.sdk_log_level,
            sdk_receive_cpu=self._runtime_profile.sdk_receive_cpu,
            sdk_process_cpu=self._runtime_profile.sdk_process_cpu,
        )
        settings.validate()
        return settings

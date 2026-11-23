from __future__ import annotations

import unittest
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.platform_loader import LoadedSdk
from emt_gateway.session import (
    EmtSession,
    SessionError,
    SessionSettings,
    SessionState,
    normalize_emt_trading_day,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ProvisioningErrorV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import (
    InMemorySecretProviderV1,
    SecretLeasePurposeV1,
)


class IngressSpy:
    def receive(self, *args: object, **kwargs: object) -> str:
        return "callback-correlation"


class FakeTraderApi:
    instances: list["FakeTraderApi"] = []
    login_result = 51

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        type(self).instances.append(self)

    def createTraderApi(self, client_id: int, log_dir: str, log_level: int) -> None:
        self.calls.append(("create", client_id, log_dir, log_level))

    def setSoftwareVersion(self, version: str) -> None:
        self.calls.append(("software", version))

    def setHeartBeatInterval(self, seconds: int) -> None:
        self.calls.append(("heartbeat", seconds))

    def subscribePublicTopic(self, resume_type: int) -> None:
        self.calls.append(("subscribe", resume_type))

    def setCpuAffinity(self, receive_cpu: int, process_cpu: int) -> None:
        self.calls.append(("cpu_affinity", receive_cpu, process_cpu))

    def login(
        self, host: str, port: int, account: str, password: str, protocol: int, local_ip: str
    ) -> int:
        self.calls.append(("login", host, port, protocol, local_ip))
        return type(self).login_result

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "20260825"

    def getApiLastError(self) -> dict[str, int]:
        return {"error_id": 88}

    def logout(self, session_id: int) -> None:
        self.calls.append(("logout", session_id))

    def release(self) -> None:
        self.calls.append(("release",))


class CredentialDigestTraderApi(FakeTraderApi):
    """Stub native API that preserves only a digest of Login inputs.

    This lets the test prove exact account/endpoint/password propagation
    without putting fixture secrets in captured call records or test output.
    """

    instances: list["CredentialDigestTraderApi"] = []
    login_call_count = 0
    login_argument_digest: str | None = None

    def login(
        self, host: str, port: int, account: str, password: str, protocol: int, local_ip: str
    ) -> int:
        canonical = "\0".join((host, str(port), account, password, str(protocol), local_ip))
        CredentialDigestTraderApi.login_argument_digest = sha256(canonical.encode("utf-8")).hexdigest()
        CredentialDigestTraderApi.login_call_count += 1
        self.calls.append(("login", "redacted"))
        return CredentialDigestTraderApi.login_result


class SessionTests(unittest.TestCase):
    def setUp(self) -> None:
        FakeTraderApi.instances = []
        FakeTraderApi.login_result = 51
        CredentialDigestTraderApi.instances = []
        CredentialDigestTraderApi.login_result = 51
        CredentialDigestTraderApi.login_call_count = 0
        CredentialDigestTraderApi.login_argument_digest = None

    @staticmethod
    def _settings(path: Path) -> SessionSettings:
        return SessionSettings(
            host="test-host",
            port=19088,
            account="test-account",
            password="test-password",
            client_id=81,
            sdk_log_dir=path,
        )

    def test_start_uses_required_prelogin_order_and_close_releases_session(self) -> None:
        with TemporaryDirectory() as directory:
            session = EmtSession(
                trader_api_base=FakeTraderApi,
                callback_ingress=IngressSpy(),
                settings=self._settings(Path(directory) / "sdk-log"),
            )
            snapshot = session.start()
            api = FakeTraderApi.instances[0]

            self.assertEqual(SessionState.READY, snapshot.state)
            self.assertEqual(51, snapshot.session_id)
            self.assertEqual("2.27.0.0", snapshot.api_version)
            self.assertEqual("2026-08-25", snapshot.trading_day)
            self.assertEqual(
                ["create", "software", "heartbeat", "subscribe", "login"],
                [call[0] for call in api.calls],
            )
            session.close()
            self.assertEqual(["logout", "release"], [call[0] for call in api.calls[-2:]])
            self.assertEqual(SessionState.CLOSED, session.snapshot().state)

    def test_normalizes_vendor_compact_trading_day_and_rejects_invalid_values(self) -> None:
        self.assertEqual("2026-08-25", normalize_emt_trading_day("20260825"))
        self.assertEqual("2026-08-25", normalize_emt_trading_day("2026-08-25"))
        with self.assertRaisesRegex(SessionError, "invalid trading day"):
            normalize_emt_trading_day("not-a-day")

    def test_optional_sdk_cpu_affinity_is_applied_only_after_login(self) -> None:
        with TemporaryDirectory() as directory:
            settings = replace(
                self._settings(Path(directory) / "sdk-log"),
                sdk_receive_cpu=17,
                sdk_process_cpu=18,
            )
            session = EmtSession(
                trader_api_base=FakeTraderApi,
                callback_ingress=IngressSpy(),
                settings=settings,
            )
            session.start()
            api = FakeTraderApi.instances[0]
            self.assertEqual(
                ["create", "software", "heartbeat", "subscribe", "login", "cpu_affinity"],
                [call[0] for call in api.calls],
            )
            self.assertEqual(("cpu_affinity", 17, 18), api.calls[-1])
            session.close()

    def test_optional_sdk_cpu_affinity_requires_a_complete_distinct_pair(self) -> None:
        with TemporaryDirectory() as directory:
            settings = self._settings(Path(directory) / "sdk-log")
            with self.assertRaisesRegex(SessionError, "configured together"):
                replace(settings, sdk_receive_cpu=17).validate()
            with self.assertRaisesRegex(SessionError, "must be distinct"):
                replace(settings, sdk_receive_cpu=17, sdk_process_cpu=17).validate()

    def test_zero_login_keeps_the_session_non_trading_and_releases_api(self) -> None:
        FakeTraderApi.login_result = 0
        with TemporaryDirectory() as directory:
            session = EmtSession(
                trader_api_base=FakeTraderApi,
                callback_ingress=IngressSpy(),
                settings=self._settings(Path(directory) / "sdk-log"),
            )
            with self.assertRaises(SessionError):
                session.start()
            api = FakeTraderApi.instances[0]
            self.assertIn(("release",), api.calls)
            self.assertEqual(SessionState.FAILED, session.snapshot().state)

    def test_explicit_relogin_uses_the_original_session_settings(self) -> None:
        with TemporaryDirectory() as directory:
            session = EmtSession(
                trader_api_base=FakeTraderApi,
                callback_ingress=IngressSpy(),
                settings=self._settings(Path(directory) / "sdk-log"),
            )
            session.start()
            snapshot = session.relogin_after_disconnect()
            api = FakeTraderApi.instances[0]

            self.assertEqual(SessionState.READY, snapshot.state)
            self.assertEqual(51, snapshot.session_id)
            self.assertEqual(2, [call[0] for call in api.calls].count("login"))
            self.assertEqual(("login", "test-host", 19088, 1, ""), api.calls[-1])

    def test_failed_relogin_latches_the_session_non_trading(self) -> None:
        with TemporaryDirectory() as directory:
            session = EmtSession(
                trader_api_base=FakeTraderApi,
                callback_ingress=IngressSpy(),
                settings=self._settings(Path(directory) / "sdk-log"),
            )
            session.start()
            FakeTraderApi.login_result = 0

            with self.assertRaisesRegex(SessionError, "relogin failed"):
                session.relogin_after_disconnect()
            self.assertEqual(SessionState.FAILED, session.snapshot().state)
            with self.assertRaises(SessionError):
                _ = session.api

    def test_constructs_a_session_from_the_platform_loader_result(self) -> None:
        with TemporaryDirectory() as directory:
            module = type("LoadedModule", (), {"TraderApi": FakeTraderApi})
            session = EmtSession.from_loaded_sdk(
                LoadedSdk(library_dir=Path(directory), module=module),
                callback_ingress=IngressSpy(),
                settings=self._settings(Path(directory) / "sdk-log"),
            )
            self.assertEqual(SessionState.NEW, session.snapshot().state)

    def test_scope_bound_credential_lease_reaches_only_the_redacted_login_stub(self) -> None:
        """Prove the control-plane lease to EMT login boundary with fake material only.

        This is deliberately a no-network/no-SDK test.  It does not assert a
        real broker login; it asserts that the exact material released for the
        matching Adapter/profile scope reaches the outer native login call,
        while diagnostics retain no plaintext input.
        """

        material = BrokerCredentialMaterialV1(
            broker_account="stub-account-001",
            password="stub-password-never-log",
            client_id="94",
            connection=BrokerConnectionV1("BROKER_TCP", "198.51.100.71", 19000),
        )
        provider = InMemorySecretProviderV1(provider_kind="EMT_STUB_TEST")
        reference = provider.store(
            target="linux-emt-secret-provider/stub-host",
            material=material,
            adapter_instance_id="emt-stub-instance",
            account_scope="profile-emt-stub",
            idempotency_key="credential-login-stub-1",
        )
        purpose = SecretLeasePurposeV1(
            adapter_instance_id="emt-stub-instance",
            account_scope="profile-emt-stub",
            operation="EMT_SESSION_OPEN",
        )
        with TemporaryDirectory() as directory:
            with provider.lease(
                reference=reference,
                purpose=purpose,
                now_wall_ns=1,
                ttl_wall_ns=1_000_000_000,
            ) as lease:
                borrowed = lease.borrow(now_wall_ns=2)
                settings = SessionSettings(
                    host=borrowed.connection.host,
                    port=borrowed.connection.port,
                    account=borrowed.broker_account,
                    password=borrowed.password,
                    client_id=int(borrowed.client_id or "0"),
                    sdk_log_dir=Path(directory) / "sdk-log",
                )
                session = EmtSession(
                    trader_api_base=CredentialDigestTraderApi,
                    callback_ingress=IngressSpy(),
                    settings=settings,
                )
                self.assertNotIn("stub-account-001", repr(settings))
                self.assertNotIn("stub-password-never-log", repr(settings))
                self.assertNotIn("198.51.100.71", repr(settings))
                self.assertNotIn("stub-password-never-log", repr(lease))
                self.assertEqual(SessionState.READY, session.start().state)
                self.assertNotIn("stub-account-001", repr(session.snapshot()))
                session.close()

        expected = sha256(
            "\0".join(
                (
                    "198.51.100.71",
                    "19000",
                    "stub-account-001",
                    "stub-password-never-log",
                    "1",
                    "",
                )
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(1, CredentialDigestTraderApi.login_call_count)
        self.assertEqual(expected, CredentialDigestTraderApi.login_argument_digest)
        self.assertNotIn("stub-account-001", repr(CredentialDigestTraderApi.instances[0].calls))
        self.assertNotIn("stub-password-never-log", repr(CredentialDigestTraderApi.instances[0].calls))
        with self.assertRaises(ProvisioningErrorV1) as error:
            provider.lease(
                reference=reference,
                purpose=SecretLeasePurposeV1(
                    adapter_instance_id="wrong-instance",
                    account_scope="profile-emt-stub",
                    operation="EMT_SESSION_OPEN",
                ),
                now_wall_ns=3,
                ttl_wall_ns=1_000_000_000,
            )
        self.assertEqual("CONTROL_SECRET_UNAVAILABLE", error.exception.code)
        self.assertEqual(1, CredentialDigestTraderApi.login_call_count)

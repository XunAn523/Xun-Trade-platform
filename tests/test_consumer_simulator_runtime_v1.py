from __future__ import annotations

import json
import os
import stat
import sys
import time
from pathlib import Path
from threading import Thread, Timer
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.consumer_runtime_control_v1 import LinuxRootOwnedNamespaceFileV1
from emt_gateway.consumer_simulator_runtime_v1 import (
    build_simulator_runtime_v1,
    load_simulator_runtime_config_v1,
)
from emt_gateway.simulator_provision_v1 import build_simulator_documents_v1
from xuanling_tradecore.adapters.emt import FileNamespaceReleaseStateStoreV1
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
)
from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.ems import SessionSupervisor, SessionSupervisorError

from test_tradecore_execution_session import _Provider, _TraderBase

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux UDS and namespace locks")


@pytest.mark.parametrize("wrong_secret", [False, True])
def test_complete_simulator_bootstrap_with_real_uds_and_fake_vendor(tmp_path, monkeypatch, wrong_secret):
    public, runtime_root, socket_root, credentials = (tmp_path / name for name in ("public", "runtime", "socket", "credentials"))
    for directory in (public, runtime_root, credentials):
        directory.mkdir(mode=0o700)
    socket_root.mkdir(mode=0o750)
    material = BrokerCredentialMaterialV1("fake", "fake-password", BrokerConnectionV1("BROKER_TCP", "127.0.0.1", 1234), "123")
    day = TradingDayEpoch("2026-10-31")
    now = time.time_ns()
    docs, state = build_simulator_documents_v1(
        material=material, trading_day=day, config_root=public, runtime_root=runtime_root,
        socket_path=socket_root / "c.sock", contract_root=Path(__file__).parents[1] / "contracts/consumer/v2",
        sdk_root=tmp_path / "sdk", sdk_hashes={}, release_hash="a" * 64,
        private_key=Ed25519PrivateKey.generate(), now_wall_ns=now, expires_wall_ns=now + 3_600_000_000_000,
        allowed_peers=[[os.geteuid(), os.getegid()]], capacity=32,
    )
    for name, document in docs.items():
        (public / name).write_text(json.dumps(document), encoding="utf-8")
    FileNamespaceReleaseStateStoreV1(runtime_root / "namespace-state.json").commit(state)
    secret = credentials / "emt-simulator-account"
    secret.write_text(json.dumps({"broker_account": "fake", "password": "wrong" if wrong_secret else "fake-password", "client_id": "123", "host": "127.0.0.1", "port": 1234}), encoding="utf-8")
    secret.chmod(0o600)
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(credentials))
    # Protected-file readers have their own OS tests. This integration owns
    # only an unprivileged temporary directory, but verifies real signatures.
    monkeypatch.setattr(LinuxRootOwnedNamespaceFileV1, "read", lambda self: self._path.read_text(encoding="utf-8"))
    import emt_gateway.consumer_simulator_host_inputs_v1 as host_inputs
    real_lstat = Path.lstat
    ancestors = set(runtime_root.parents)
    monkeypatch.setattr(Path, "lstat", lambda path: (
        SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=os.geteuid())
        if path in ancestors else real_lstat(path)
    ))
    assert host_inputs is not None
    calls = []
    timers = []

    class FakeApi(_TraderBase):
        def getTradingDay(self):
            return "20261031"

        def insertOrder(self, request, session):
            calls.append(request["order_client_id"])
            timer = Timer(0.02, lambda: self.onOrderEvent({
                "order_emt_id": 5001, "order_client_id": request["order_client_id"],
                "order_status": 4, "qty_traded": 0, "qty_left": request["quantity"],
            }))
            timers.append(timer)
            timer.start()
            return 5001

    config = load_simulator_runtime_config_v1(public / "runtime.json")
    service, inputs, worker, pump, observer, resources, fence = build_simulator_runtime_v1(config, sdk_provider=_Provider(FakeApi))
    runner = None
    try:
        if wrong_secret:
            with pytest.raises(Exception):
                service.start()
            assert calls == []
            return
        service.start()
        simulator_supervisor = worker.worker.runtime._supervisor
        assert not simulator_supervisor._mapping.manifest.scope_ownership.uniqueness_probe_succeeded
        assert not simulator_supervisor._mapping.runtime.route_token_probe_succeeded
        normal = SessionSupervisor(
            binding=worker.worker.runtime.binding, activated_mapping=simulator_supervisor._mapping,
            ownership_fence=fence, adapter_factory=lambda: pytest.fail("normal gate invoked factory"),
        )
        with pytest.raises(SessionSupervisorError):
            normal.start(observer, evaluated_wall_ns=time.time_ns())
        runner = Thread(target=service.serve_forever)
        runner.start()
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, 5))

        def call(operation, payload, request_id="test"):
            return client.call({"message_type": "request", "schema_version": 2, "operation": operation, "request_id": request_id, "payload": payload})

        account = call("ListAccounts", {})["payload"]["accounts"][0]
        assert account["lifecycle"] == "SIMULATOR_ACTIVE"
        result = call("SubmitSignal", {
            "consumer_account_id": "emt-sim-ordinary", "strategy_id": "consumer-sim-strategy",
            "strategy_order_id": "test-one", "trading_day": day.value, "symbol": "000001", "market": "SZ",
            "side": "BUY", "price_type": "LIMIT", "business_type": "CASH", "quantity": 100,
            "protection_price_ticks": 1039, "limit_price_ticks": 1039,
        })
        assert result["status"] == "ACCEPTED", result
        order_id = result["payload"]["oms_order_id"]
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            order = call("GetOrder", {"oms_order_id": order_id})
            if order["payload"]["order"]["payload"]["order_state"] == "WORKING":
                break
            time.sleep(0.01)
        else:
            pytest.fail("callback did not project through the real runtime")
        assert len(calls) == 1 and not worker.failed and pump.healthy() and observer.gap_count == 0
        plan = call("CreateReplayPartitions", {"consumer_account_id": "emt-sim-ordinary", "partition_count": 1})
        token = plan["payload"]["partitions"][0]["cursor"]
        events = call("PollEvents", {"cursor": token, "limit": 100})["payload"]["events"]
        assert any(item["payload"].get("order_state") == "WORKING" for item in events)
        assert call("CancelOrder", {"oms_order_id": order_id, "reason": "disabled"})["code"] == "UNAUTHORIZED"
        assert len(calls) == 1
    finally:
        for timer in timers:
            timer.join(2)
        if runner is not None:
            service.stop()
            runner.join(5)
            assert not runner.is_alive()
        for resource in resources:
            resource.close()
        fence.release()
    with pytest.raises(FileExistsError):
        build_simulator_runtime_v1(config, sdk_provider=_Provider(FakeApi))

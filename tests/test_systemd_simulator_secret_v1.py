from __future__ import annotations

import json
import os
import stat
import struct
import sys
from types import SimpleNamespace

import pytest
from emt_gateway.systemd_simulator_secret_v1 import SystemdSimulatorSecretProviderV1
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    ProvisioningErrorV1,
    SecretReferenceV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import SecretLeasePurposeV1

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux credential DAC")


def setup_provider(tmp_path):
    root = tmp_path / "credentials"
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    path = root / "emt-simulator"
    path.write_text(json.dumps({
        "broker_account": "sample-account", "password": "synthetic-secret-sentinel",
        "client_id": "101", "host": "127.0.0.1", "port": 1234,
    }), encoding="utf-8")
    os.chmod(path, 0o600)
    reference = SecretReferenceV1("SYSTEMD_SIMULATOR_CREDENTIAL_V1", "reference", "sim-target")
    provider = SystemdSimulatorSecretProviderV1(
        credential_directory=root, credential_name=path.name, reference=reference,
        adapter_instance_id="adapter", account_scope="profile",
    )
    purpose = SecretLeasePurposeV1("adapter", "profile", "EMT_EXECUTION_SESSION_OPEN")
    return provider, path, reference, purpose


def test_systemd_secret_scope_one_shot_and_expiry(tmp_path):
    provider, _path, reference, purpose = setup_provider(tmp_path)
    with pytest.raises(ProvisioningErrorV1):
        provider.lease(
            reference=reference, purpose=SecretLeasePurposeV1("other", "profile", purpose.operation),
            now_wall_ns=10, ttl_wall_ns=20,
        )
    lease = provider.lease(reference=reference, purpose=purpose, now_wall_ns=10, ttl_wall_ns=20)
    assert lease.borrow(now_wall_ns=11).credential_digest()
    assert "synthetic-secret-sentinel" not in repr(lease)
    with pytest.raises(ProvisioningErrorV1):
        lease.borrow(now_wall_ns=30)
    with pytest.raises(ProvisioningErrorV1):
        provider.lease(reference=reference, purpose=purpose, now_wall_ns=11, ttl_wall_ns=20)
    lease.close()


@pytest.mark.parametrize("problem", ["public", "symlink", "duplicate", "extra"])
def test_systemd_secret_rejects_unsafe_or_ambiguous_input_without_leak(tmp_path, problem):
    provider, path, reference, purpose = setup_provider(tmp_path)
    if problem == "public":
        os.chmod(path, 0o644)
    elif problem == "symlink":
        target = path.with_name("target")
        path.rename(target)
        path.symlink_to(target)
    elif problem == "duplicate":
        path.write_text('{"password":"synthetic-secret-sentinel","password":"x"}', encoding="utf-8")
    else:
        data = json.loads(path.read_text(encoding="utf-8"))
        path.write_text(json.dumps({**data, "extra": "synthetic-secret-sentinel"}), encoding="utf-8")
    with pytest.raises(ProvisioningErrorV1) as raised:
        provider.lease(reference=reference, purpose=purpose, now_wall_ns=10, ttl_wall_ns=20)
    assert "synthetic-secret-sentinel" not in str(raised.value)
    with pytest.raises(ProvisioningErrorV1):
        provider.lease(reference=reference, purpose=purpose, now_wall_ns=10, ttl_wall_ns=20)


def test_systemd_exact_named_user_acl_rejects_any_additional_principal(monkeypatch):
    meta = SimpleNamespace(st_mode=stat.S_IFREG | 0o440, st_uid=0, st_gid=0)
    entries = [
        (1, 4, 0xFFFFFFFF), (2, 4, os.geteuid()), (4, 0, 0xFFFFFFFF),
        (16, 4, 0xFFFFFFFF), (32, 0, 0xFFFFFFFF),
    ]
    def encode(rows):
        return struct.pack("<I", 2) + b"".join(struct.pack("<HHI", *row) for row in rows)
    monkeypatch.setattr(os, "getxattr", lambda _fd, _name: encode(entries))
    assert SystemdSimulatorSecretProviderV1._protected(3, meta, is_directory=False)
    entries.insert(2, (2, 4, os.geteuid() + 1))
    assert not SystemdSimulatorSecretProviderV1._protected(3, meta, is_directory=False)

"""Root-only, write-once provisioning for an explicitly authorized EMT simulator.

Reads one ordinary simulated account, creates protected systemd credentials
and signed administrative namespace documents, and never opens an SDK or
sends an order. Probe flags remain false. Existing instances are not reset.
"""

from __future__ import annotations

import argparse
import grp
import json
import os
import pwd
import secrets
import sys
import time
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.platform_loader import sdk_library_dir, verify_sdk_hashes
from emt_gateway.simulator_provision_v1 import build_simulator_documents_v1
from xuanling_tradecore.adapters.emt import FileNamespaceReleaseStateStoreV1
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
)
from xuanling_tradecore.domain import TradingDayEpoch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("accounts-file", "config-root", "runtime-root", "sdk-root", "sdk-hashes-file", "contract-root", "socket-path"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--release-hash", required=True)
    parser.add_argument("--service-user", default="xuanling")
    parser.add_argument("--service-group", default="tradecore-testers")
    parser.add_argument("--consumer-users", default="xuanling,fengyu,yangxue")
    parser.add_argument("--client-id", type=int, default=123)
    parser.add_argument("--capacity", type=int, default=10_000)
    parser.add_argument("--expires-hours", type=int, default=168)
    args = parser.parse_args()
    if sys.platform != "linux" or os.geteuid() != 0:
        parser.error("provisioning requires Linux root")
    if not 1 <= args.client_id <= 127 or not 16 <= args.capacity <= 100_000 or not 1 <= args.expires_hours <= 168:
        parser.error("invalid simulator bounds")
    if len(args.release_hash) != 64 or any(c not in "0123456789abcdef" for c in args.release_hash):
        parser.error("release hash must be SHA-256")
    for path in (args.config_root, args.runtime_root, args.sdk_root, args.socket_path, args.contract_root):
        if not path.is_absolute() or ".." in path.parts:
            parser.error("all deployment paths must be absolute")
    if args.config_root.exists() or args.runtime_root.exists():
        parser.error("refusing to overwrite or reset an existing simulator instance")
    user = pwd.getpwnam(args.service_user)
    group = grp.getgrnam(args.service_group)
    peers = {(user.pw_uid, user.pw_gid), (user.pw_uid, group.gr_gid)}
    for name in args.consumer_users.split(","):
        account = pwd.getpwnam(name)
        peers.add((account.pw_uid, account.pw_gid))
    source = json.loads(args.accounts_file.read_text(encoding="utf-8-sig"))
    ordinary, gateway = source["ordinary"], source["gateway"]
    material = BrokerCredentialMaterialV1(
        ordinary["account"], ordinary["password"],
        BrokerConnectionV1("BROKER_TCP", gateway["host"], gateway["port"]), str(args.client_id),
    )
    sdk_hashes = json.loads(args.sdk_hashes_file.read_text(encoding="utf-8"))
    if not sdk_hashes:
        parser.error("SDK hashes must not be empty")
    verify_sdk_hashes(sdk_library_dir(args.sdk_root), sdk_hashes)
    key, now = Ed25519PrivateKey.generate(), time.time_ns()
    range_start = 100_000_000 + secrets.randbelow(700_000_000 - args.capacity)
    documents, state = build_simulator_documents_v1(
        material=material, trading_day=TradingDayEpoch(args.trading_day),
        config_root=args.config_root, runtime_root=args.runtime_root, socket_path=args.socket_path,
        contract_root=args.contract_root, sdk_root=args.sdk_root, sdk_hashes=sdk_hashes,
        release_hash=args.release_hash, private_key=key, now_wall_ns=now,
        expires_wall_ns=now + args.expires_hours * 3_600_000_000_000,
        allowed_peers=[list(item) for item in sorted(peers)], capacity=args.capacity,
        client_range_start=range_start,
    )
    args.config_root.mkdir(mode=0o755, parents=False)
    args.runtime_root.mkdir(mode=0o700, parents=False)
    os.chown(args.runtime_root, user.pw_uid, group.gr_gid)
    for name, document in documents.items():
        path = args.config_root / name
        with path.open("x", encoding="utf-8") as handle:
            json.dump(document, handle, separators=(",", ":"), sort_keys=True)
        path.chmod(0o644)
    state_store = FileNamespaceReleaseStateStoreV1(args.runtime_root / "namespace-state.json")
    state_store.commit(state)
    os.chown(args.runtime_root / "namespace-state.json", user.pw_uid, group.gr_gid)
    credential_root = args.config_root / "credentials"
    credential_root.mkdir(mode=0o700)
    credential_path = credential_root / "emt-simulator-account.json"
    with credential_path.open("x", encoding="utf-8") as handle:
        json.dump({"broker_account": material.broker_account, "password": material.password,
                   "client_id": material.client_id, "host": material.connection.host, "port": material.connection.port}, handle)
    credential_path.chmod(0o600)
    key_path = credential_root / "operator-signing-key.bin"
    with key_path.open("xb") as handle:
        handle.write(key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption()))
    key_path.chmod(0o600)
    print(json.dumps({"status": "PROVISIONED_SIMULATOR_ONLY", "credentials_logged": False,
                      "probe_flags_changed": False, "consumer_account_id": "emt-sim-ordinary",
                      "strategy_id": "consumer-sim-strategy", "trading_day": args.trading_day}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

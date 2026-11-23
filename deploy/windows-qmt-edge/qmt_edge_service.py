"""Run the Windows Edge mTLS command service in a dedicated CPython 3.10 venv."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from threading import Event
from typing import cast

from xuanling_tradecore.adapters.qmt.edge_config import QmtEdgeConfigV1
from xuanling_tradecore.adapters.qmt.edge_runtime import QmtEdgeRuntime
from xuanling_tradecore.adapters.qmt.edge_service import QmtCommandServiceOwner, QmtEdgeService
from xuanling_tradecore.adapters.qmt.mtls_channel import QmtMtlsServerConfig


def _load_service_config(path: Path) -> QmtMtlsServerConfig:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("QMT Edge service configuration cannot be read") from error
    if not isinstance(raw, dict) or raw.get("schema") != "TradeCoreQmtEdgeServiceConfigV1":
        raise RuntimeError("QMT Edge service configuration schema is invalid")
    fields = cast(dict[str, object], raw)
    required = {
        "schema",
        "edge_config_path",
        "listen_host",
        "listen_port",
        "ca_certificate_path",
        "certificate_path",
        "private_key_path",
        "expected_client_certificate_sha256",
    }
    if set(fields) != required:
        raise RuntimeError("QMT Edge service configuration fields are incomplete")
    host = fields["listen_host"]
    port = fields["listen_port"]
    if not isinstance(host, str) or host.startswith("REPLACE_") or type(port) is not int:
        raise RuntimeError("QMT Edge service endpoint is invalid")
    for name in ("ca_certificate_path", "certificate_path", "private_key_path"):
        value = fields[name]
        if not isinstance(value, str) or value.startswith("REPLACE_"):
            raise RuntimeError("QMT Edge service certificate path is invalid")
    expected = fields["expected_client_certificate_sha256"]
    if not isinstance(expected, str) or expected.startswith("REPLACE_"):
        raise RuntimeError("QMT Edge expected client certificate digest is invalid")
    return QmtMtlsServerConfig(
        host=host,
        port=port,
        ca_certificate_path=Path(cast(str, fields["ca_certificate_path"])),
        certificate_path=Path(cast(str, fields["certificate_path"])),
        private_key_path=Path(cast(str, fields["private_key_path"])),
        expected_client_certificate_sha256=expected,
    )


def _edge_config_path(path: Path) -> Path:
    raw = json.loads(path.read_text(encoding="utf-8"))
    value = cast(dict[str, object], raw).get("edge_config_path")
    if not isinstance(value, str) or value.startswith("REPLACE_"):
        raise RuntimeError("QMT Edge service edge_config_path is invalid")
    return Path(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="TradeCore QMT Windows Edge mTLS command service")
    parser.add_argument("--service-config", required=True, type=Path)
    arguments = parser.parse_args()
    service: QmtEdgeService | None = None
    try:
        config = QmtEdgeConfigV1.load(_edge_config_path(arguments.service_config))
        service = QmtEdgeService(
            owner_factory=lambda: QmtCommandServiceOwner(QmtEdgeRuntime.open(config)),
            tls=_load_service_config(arguments.service_config),
        )
        service.start()
    except (RuntimeError, OSError, ValueError) as error:
        print(json.dumps({"status": "SERVICE_BLOCKED", "reason": str(error)}, sort_keys=True))
        return 2
    stopping = Event()
    signal.signal(signal.SIGINT, lambda _signal, _frame: stopping.set())
    signal.signal(signal.SIGTERM, lambda _signal, _frame: stopping.set())
    print(json.dumps({"status": "SERVICE_LISTENING", "mode": config.mode}, sort_keys=True))
    try:
        stopping.wait()
    finally:
        service.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Explicit, probe-only deployment entry; never loads native rings or policies."""

from __future__ import annotations

import argparse
import ipaddress
import json
import signal
from pathlib import Path
from threading import Event
from typing import cast

from .edge_runtime import require_supported_qmt_edge_sqlite
from .edge_service import QmtEdgeService
from .mtls_channel import QmtMtlsClientConfig, QmtMtlsServerConfig
from .transport_probe import QmtTransportProbeConfig, QmtTransportProbeOwner, run_transport_probe


def load_config(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError("QMT probe configuration must be an object")
    fields = cast(dict[str, object], raw)
    expected = {
        "schema",
        "role",
        "mode",
        "local_host",
        "peer_host",
        "port",
        "scope_digest",
        "outbox_epoch",
        "event_count",
        "database_path",
        "ca_certificate_path",
        "certificate_path",
        "private_key_path",
        "expected_peer_certificate_sha256",
    }
    if set(fields) != expected or fields["schema"] != "TradeCoreQmtTransportProbeConfigV1":
        raise ValueError("QMT probe configuration schema/fields differ")
    if fields["role"] not in ("WINDOWS_EDGE", "LINUX_CLIENT") or fields["mode"] != "DISARMED":
        raise ValueError("QMT transport probe requires DISARMED and an explicit role")
    for name in expected - {"port", "event_count"}:
        value = fields[name]
        if not isinstance(value, str) or not value or "REPLACE_" in value:
            raise ValueError("QMT probe configuration has an unfilled text field")
    for name in ("local_host", "peer_host"):
        address = ipaddress.IPv4Address(cast(str, fields[name]))
        if address.is_unspecified or address.is_multicast:
            raise ValueError("QMT probe requires a concrete unicast interface")
    if type(fields["port"]) is not int or not 1 <= fields["port"] <= 65535:
        raise ValueError("QMT probe port is invalid")
    for name in ("database_path", "ca_certificate_path", "certificate_path", "private_key_path"):
        value = Path(cast(str, fields[name]))
        fields[name] = value if value.is_absolute() else path.resolve().parent / value
    return fields


def main() -> int:
    parser = argparse.ArgumentParser(description="QMT dual mTLS DISARMED transport probe")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--interrupt-after", type=int)
    parser.add_argument(
        "--interrupt-phase", choices=("after_journal", "after_ack"), default="after_journal"
    )
    args = parser.parse_args()
    service: QmtEdgeService | None = None
    try:
        fields = load_config(args.config)
        config = QmtTransportProbeConfig(
            cast(str, fields["scope_digest"]),
            cast(str, fields["outbox_epoch"]),
            cast(int, fields["event_count"]),
        )
        local, peer = cast(str, fields["local_host"]), cast(str, fields["peer_host"])
        port = cast(int, fields["port"])
        ca = cast(Path, fields["ca_certificate_path"])
        certificate = cast(Path, fields["certificate_path"])
        key = cast(Path, fields["private_key_path"])
        pin = cast(str, fields["expected_peer_certificate_sha256"])
        database = cast(Path, fields["database_path"])
        if fields["role"] == "LINUX_CLIENT":
            result = run_transport_probe(
                tls=QmtMtlsClientConfig(peer, port, ca, certificate, key, pin, peer, 5.0, local),
                config=config,
                journal_path=database,
                interrupt_after=args.interrupt_after,
                interrupt_phase=args.interrupt_phase,
            )
            print(json.dumps(result, sort_keys=True), flush=True)
            return 0 if result["status"] == "TRANSPORT_PROBE_COMPLETE" else 3
        if args.interrupt_after is not None:
            raise ValueError("Fault interruption belongs to the Linux probe client")
        require_supported_qmt_edge_sqlite()
        service = QmtEdgeService(
            owner_factory=lambda: QmtTransportProbeOwner(database, config),
            tls=QmtMtlsServerConfig(local, port, ca, certificate, key, pin),
            allowed_peer_host=peer,
        )
        stopping = Event()
        signal.signal(signal.SIGINT, lambda _signal, _frame: stopping.set())
        signal.signal(signal.SIGTERM, lambda _signal, _frame: stopping.set())
        service.start()
        print(
            json.dumps(
                {
                    "status": "TRANSPORT_PROBE_LISTENING",
                    "mode": "DISARMED",
                    "probe_only": True,
                    "mutation_allowed": False,
                    "capability_accepted": False,
                    "port": port,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        stopping.wait()
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        # Do not emit paths, certificate material or peer payloads on failure.
        print(
            json.dumps({"status": "TRANSPORT_PROBE_BLOCKED", "error_type": type(error).__name__}),
            flush=True,
        )
        return 2
    finally:
        if service is not None:
            service.stop()


if __name__ == "__main__":
    raise SystemExit(main())

"""Offline contracts for QMT P0 evidence and the fixed native-ring ABI."""

from __future__ import annotations

import base64
import hashlib
import re
import socket
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.qmt import (
    QmtAdapterProfile,
    QmtBridgeWireError,
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    QmtCapabilityActivation,
    QmtCapabilityError,
    QmtCapabilityManifestV1,
    QmtEd25519CapabilityVerifierV1,
    QmtEdgeCommandAcceptance,
    QmtEdgeCommandError,
    QmtEdgeCommandV1,
    QmtEdgeConfigError,
    QmtEdgeConfigV1,
    QmtEdgeStoreError,
    QmtNativeContractError,
    QmtNativeFrameKind,
    QmtNativeFrameV1,
    QmtNativeRing,
    QmtNativeRingEmpty,
    QmtNativeRingFull,
    QmtRingHeaderV1,
    canonical_wire_payload,
    decode_canonical_wire_payload,
    load_builtin_qmt_fake_manifest,
    qmt_native_abi_digest,
    require_supported_qmt_edge_sqlite,
)
from xuanling_tradecore.adapters.qmt.bridge_wire import receive_wire_frame, send_wire_frame
from xuanling_tradecore.adapters.qmt.edge_wire_endpoint import (
    QmtEdgeHealthProjection,
    QmtEdgeWireEndpoint,
)


def _digest(character: str) -> str:
    return character * 64


def _mapping():
    manifest = load_builtin_qmt_fake_manifest()
    runtime = RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("qmt-fake-account-1", True, False, False),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )
    return QmtAdapterProfile(manifest).activate(runtime=runtime, activated_at_unix_s=0)


def _signed_manifest(
    private_key: Ed25519PrivateKey, *, edge_digest: str
) -> QmtCapabilityManifestV1:
    mapping = _mapping()
    ownership = mapping.manifest.scope_ownership
    binding = mapping.manifest.route_token_binding
    assert ownership is not None and binding is not None
    unsigned = QmtCapabilityManifestV1(
        manifest_id="qmt-p0-test",
        signer_id="qmt-p0-signer",
        adapter_instance_id=ownership.adapter_instance_id,
        adapter_client_scope=ownership.adapter_client_scope,
        mapping_manifest_hash=mapping.manifest.manifest_hash,
        edge_instance_digest=edge_digest,
        qmt_build_digest=_digest("a"),
        embedded_python_version="3.6",
        native_abi_digest=qmt_native_abi_digest(),
        route_binding_digest=binding.manifest_hash,
        evidence_digest=_digest("b"),
        valid_from_wall_ns=10,
        valid_until_wall_ns=100,
        capabilities=frozenset(
            {
                "embedded_python_36",
                "run_time",
                "quick_trade_2",
                "single_native_caller",
                "native_mapping_interlocked",
                "route_identity_echo",
                "order_callback",
                "query_complete",
            }
        ),
        signature="unsigned",
    )
    return replace(
        unsigned,
        signature=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode("ascii"),
    )


class QmtCapabilityTests(unittest.TestCase):
    def test_signed_scope_bound_capability_activates(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        edge_digest = _digest("c")
        manifest = _signed_manifest(private_key, edge_digest=edge_digest)
        activation = QmtCapabilityActivation.activate(
            manifest=manifest,
            verifier=QmtEd25519CapabilityVerifierV1(
                {"qmt-p0-signer": private_key.public_key().public_bytes_raw()},
                revoked_signer_ids=frozenset(),
            ),
            mapping=_mapping(),
            expected_edge_instance_digest=edge_digest,
            now_wall_ns=11,
        )
        self.assertEqual(activation.manifest.manifest_digest, manifest.manifest_digest)

    def test_capability_manifest_rejects_tampered_signature_and_missing_probe(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        edge_digest = _digest("c")
        manifest = _signed_manifest(private_key, edge_digest=edge_digest)
        verifier = QmtEd25519CapabilityVerifierV1(
            {"qmt-p0-signer": private_key.public_key().public_bytes_raw()},
            revoked_signer_ids=frozenset(),
        )
        with self.assertRaisesRegex(QmtCapabilityError, "verification"):
            QmtCapabilityActivation.activate(
                manifest=replace(manifest, evidence_digest=_digest("d")),
                verifier=verifier,
                mapping=_mapping(),
                expected_edge_instance_digest=edge_digest,
                now_wall_ns=11,
            )
        with self.assertRaisesRegex(QmtCapabilityError, "lacks required"):
            incomplete = replace(manifest, capabilities=manifest.capabilities - {"query_complete"})
            signed = replace(
                incomplete,
                signature=base64.b64encode(private_key.sign(incomplete.canonical_payload())).decode(
                    "ascii"
                ),
            )
            QmtCapabilityActivation.activate(
                manifest=signed,
                verifier=verifier,
                mapping=_mapping(),
                expected_edge_instance_digest=edge_digest,
                now_wall_ns=11,
            )


class QmtNativeContractTests(unittest.TestCase):
    def test_header_and_committed_frame_round_trip(self) -> None:
        header = QmtRingHeaderV1(
            scope_digest=b"s" * 32,
            generation=b"g" * 16,
            slot_size=512,
            capacity=8,
        )
        self.assertEqual(QmtRingHeaderV1.unpack(header.pack()), header)
        frame = QmtNativeFrameV1(
            sequence=3,
            kind=QmtNativeFrameKind.SUBMIT,
            request_digest=b"r" * 32,
            intent_digest=b"i" * 32,
            timestamps_ns=(1, 2, 3, 4),
            payload=b"fixed-binary-payload",
        )
        raw = bytearray(frame.encode_uncommitted())
        raw[-8:] = (3).to_bytes(8, "little")
        self.assertEqual(QmtNativeFrameV1.decode_committed(bytes(raw), expected_sequence=3), frame)

    def test_frame_rejects_crc_and_commit_corruption(self) -> None:
        frame = QmtNativeFrameV1(
            sequence=3,
            kind=QmtNativeFrameKind.QUERY,
            request_digest=b"r" * 32,
            intent_digest=b"i" * 32,
            timestamps_ns=(0, 0, 0, 0),
            payload=b"payload",
        )
        raw = bytearray(frame.encode_uncommitted())
        raw[120] ^= 1
        raw[-8:] = (3).to_bytes(8, "little")
        with self.assertRaisesRegex(QmtNativeContractError, "CRC"):
            QmtNativeFrameV1.decode_committed(bytes(raw), expected_sequence=3)
        raw = bytearray(frame.encode_uncommitted())
        raw[-8:] = (4).to_bytes(8, "little")
        with self.assertRaisesRegex(QmtNativeContractError, "commit"):
            QmtNativeFrameV1.decode_committed(bytes(raw), expected_sequence=3)

    def test_file_backed_ring_is_bounded_and_commits_last(self) -> None:
        header = QmtRingHeaderV1(
            scope_digest=b"s" * 32,
            generation=b"g" * 16,
            slot_size=512,
            capacity=2,
        )
        with tempfile.TemporaryDirectory() as directory:
            ring = QmtNativeRing.create_file(
                Path(directory) / "command.ring", header, test_only_unsafe_fallback=True
            )
            try:
                for sequence in (1, 2):
                    ring.publish(
                        QmtNativeFrameV1(
                            sequence=sequence,
                            kind=QmtNativeFrameKind.SUBMIT,
                            request_digest=bytes([sequence]) * 32,
                            intent_digest=b"i" * 32,
                            timestamps_ns=(1, 2, 3, 4),
                            payload=b"payload",
                        )
                    )
                self.assertEqual(ring.snapshot().depth, 2)
                with self.assertRaises(QmtNativeRingFull):
                    ring.publish(
                        QmtNativeFrameV1(
                            sequence=3,
                            kind=QmtNativeFrameKind.SUBMIT,
                            request_digest=b"r" * 32,
                            intent_digest=b"i" * 32,
                            timestamps_ns=(0, 0, 0, 0),
                            payload=b"payload",
                        )
                    )
                self.assertEqual(ring.consume().sequence, 1)
                self.assertEqual(ring.consume().sequence, 2)
                with self.assertRaises(QmtNativeRingEmpty):
                    ring.consume()
            finally:
                ring.close()


class QmtBridgeWireTests(unittest.TestCase):
    def test_wire_frame_and_canonical_payload_round_trip(self) -> None:
        payload = canonical_wire_payload({"kind": "HELLO", "scope": "digest", "cursor": 0})
        frame = QmtBridgeWireFrame(QmtBridgeWireKind.HELLO, payload)
        decoded = QmtBridgeWireFrame.decode(frame.encode())
        self.assertEqual(decoded, frame)
        self.assertEqual(decode_canonical_wire_payload(decoded.payload), {"kind": "HELLO", "scope": "digest", "cursor": 0})

    def test_wire_rejects_payload_tampering_and_noncanonical_json(self) -> None:
        frame = QmtBridgeWireFrame(QmtBridgeWireKind.ACK, b'{"cursor":1}')
        raw = bytearray(frame.encode())
        raw[-1] ^= 1
        with self.assertRaisesRegex(QmtBridgeWireError, "hash"):
            QmtBridgeWireFrame.decode(bytes(raw))
        with self.assertRaisesRegex(QmtBridgeWireError, "canonical"):
            decode_canonical_wire_payload(b'{"b":1, "a":2}')

    def test_wire_can_cross_a_fragmented_socket_stream(self) -> None:
        left, right = socket.socketpair()
        try:
            frame = QmtBridgeWireFrame(QmtBridgeWireKind.HEALTH, b'{"ready":true}')
            send_wire_frame(left, frame)
            self.assertEqual(receive_wire_frame(right), frame)
        finally:
            left.close()
            right.close()


class QmtEdgeDeploymentTests(unittest.TestCase):
    def test_bigqmt_probe_strategy_loader_pins_the_current_engine(self) -> None:
        root = Path(__file__).resolve().parents[1]
        loader = (
            root
            / "deploy"
            / "windows-qmt-edge"
            / "bigqmt_strategy"
            / "tradecore_qmt_p0_probe_strategy.py"
        )
        engine = root / "deploy" / "windows-qmt-edge" / "qmt_native_engine.py"
        source = loader.read_text(encoding="utf-8")
        match = re.search(r'EXPECTED_ENGINE_SHA256 = "([0-9a-f]{64})"', source)
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.group(1), hashlib.sha256(engine.read_bytes()).hexdigest())
        self.assertIn('CONFIG_PATH = r"D:\\projects\\TradeCore\\qmt-edge\\config\\edge.json"', source)
        self.assertIn('config.get("mode") != "PROBE"', source)
        self.assertIn('config.get("allow_mutation") is not False', source)

    def test_sqlite_gate_rejects_unfixed_runtime_and_accepts_official_backports(self) -> None:
        with self.assertRaisesRegex(QmtEdgeStoreError, "official WAL-reset-fixed"):
            require_supported_qmt_edge_sqlite((3, 40, 1))
        with self.assertRaisesRegex(QmtEdgeStoreError, "official WAL-reset-fixed"):
            require_supported_qmt_edge_sqlite((3, 49, 1))
        self.assertEqual(require_supported_qmt_edge_sqlite((3, 44, 6)), (3, 44, 6))
        self.assertEqual(require_supported_qmt_edge_sqlite((3, 50, 7)), (3, 50, 7))
        self.assertEqual(require_supported_qmt_edge_sqlite((3, 51, 3)), (3, 51, 3))

    def test_probe_config_remains_disarmed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "edge.json"
            path.write_text(
                """{
                    \"schema\": \"TradeCoreQmtEdgeConfigV1\",
                    \"scope_digest_hex\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",
                    \"generation_hex\": \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",
                    \"command_ring_path\": \"C:/edge/command.ring\",
                    \"event_ring_path\": \"C:/edge/event.ring\",
                    \"atomic_dll_path\": \"C:/edge/tradecore_qmt_atomic.dll\",
                    \"database_path\": \"C:/edge/qmt.sqlite3\",
                    \"ring_capacity\": 8,
                    \"sqlite_synchronous\": \"NORMAL\",
                    \"qmt_timer_period\": \"100nMilliSecond\",
                    \"qmt_timer_start_time\": \"2019-10-14 13:20:00\",
                    \"max_drain\": 1,
                    \"mode\": \"PROBE\",
                    \"allow_mutation\": false,
                    \"callback_attributes\": {
                        \"event_key\": \"event_key\", \"client_order_ref\": \"remark\",
                        \"venue_order_id\": \"order_id\", \"trade_key\": \"trade_id\",
                        \"filled_qty\": \"filled\", \"leaves_qty\": \"leaves\",
                        \"error_code\": \"error_code\"
                    }
                }""",
                encoding="utf-8",
            )
            self.assertTrue(QmtEdgeConfigV1.load(path).is_disarmed)
            data = path.read_text(encoding="utf-8").replace('"allow_mutation": false', '"allow_mutation": true')
            path.write_text(data, encoding="utf-8")
            with self.assertRaises(QmtEdgeConfigError):
                QmtEdgeConfigV1.load(path)

    def test_edge_command_rejects_noncanonical_cross_scope_record(self) -> None:
        raw = {
            "schema": "QmtEdgeCommandV1",
            "kind": "QUERY",
            "request_id": "query-1",
            "scope_digest": "a" * 64,
            "account_id": "account-1",
            "adapter_instance_id": "adapter-1",
            "adapter_client_scope": "scope-1",
            "fields": {"query_kind": "orders", "page_size": 100},
        }
        command = QmtEdgeCommandV1.from_mapping(raw)
        self.assertEqual(command.canonical_bytes, canonical_wire_payload(raw))
        raw["scope_digest"] = "a" * 63
        with self.assertRaises(QmtEdgeCommandError):
            QmtEdgeCommandV1.from_mapping(raw)

    def test_wire_endpoint_fences_hello_scope_then_returns_acceptance(self) -> None:
        class _Acceptor:
            def accept_and_publish(self, command: QmtEdgeCommandV1) -> QmtEdgeCommandAcceptance:
                return QmtEdgeCommandAcceptance(
                    request_id=command.request_id,
                    command_sequence=17,
                    accepted_monotonic_ns=19,
                    state="PUBLISH_ATTEMPTED",
                )

        endpoint = QmtEdgeWireEndpoint(
            scope_digest="a" * 64,
            command_acceptor=_Acceptor(),
            health=QmtEdgeHealthProjection(ready=True, detail_code="READY"),
        )
        hello = QmtBridgeWireFrame(
            QmtBridgeWireKind.HELLO,
            canonical_wire_payload(
                {"schema": "QmtEdgeHelloV1", "role": "COMMAND", "scope_digest": "a" * 64}
            ),
        )
        self.assertEqual(endpoint.handle(hello, hello_complete=False).kind, QmtBridgeWireKind.HEALTH)
        command = QmtEdgeCommandV1.from_mapping(
            {
                "schema": "QmtEdgeCommandV1",
                "kind": "QUERY",
                "request_id": "query-1",
                "scope_digest": "a" * 64,
                "account_id": "account-1",
                "adapter_instance_id": "adapter-1",
                "adapter_client_scope": "scope-1",
                "fields": {"query_kind": "orders", "page_size": 100},
            }
        )
        response = endpoint.handle(
            QmtBridgeWireFrame(QmtBridgeWireKind.COMMAND, command.canonical_bytes),
            hello_complete=True,
        )
        self.assertEqual(response.kind, QmtBridgeWireKind.ACCEPTED)
        self.assertEqual(decode_canonical_wire_payload(response.payload)["command_sequence"], 17)


if __name__ == "__main__":
    unittest.main()

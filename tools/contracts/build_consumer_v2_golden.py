"""Rebuild deterministic ConsumerGateway V2 golden frames and asset hashes."""

from __future__ import annotations

import json
import struct
from hashlib import sha256
from pathlib import Path

from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = ROOT / "contracts" / "consumer" / "v2"


def _tree_hash(paths: tuple[Path, ...], *, normalize_text: bool = False) -> str:
    digest = sha256()
    for path in sorted(paths):
        relative = path.relative_to(CONTRACT_ROOT).as_posix().encode("utf-8")
        content = path.read_bytes()
        if normalize_text:
            content = content.replace(b"\r\n", b"\n")
        digest.update(struct.pack(">I", len(relative)))
        digest.update(relative)
        digest.update(struct.pack(">Q", len(content)))
        digest.update(content)
    return digest.hexdigest()


def main() -> int:
    # Attributes govern fresh checkouts, not already existing CRLF files.
    # Rebuild canonical LF assets as well as their normalized text identities.
    for path in sorted(CONTRACT_ROOT.rglob("*.json")):
        original = path.read_bytes()
        normalized = original.replace(b"\r\n", b"\n")
        if normalized != original:
            path.write_bytes(normalized)
    source = CONTRACT_ROOT / "fixtures" / "positive" / "submit-signal.request.json"
    payload = json.loads(source.read_text(encoding="utf-8"))
    golden_directory = CONTRACT_ROOT / "golden"
    golden_directory.mkdir(parents=True, exist_ok=True)
    (golden_directory / "submit-signal.request.bin").write_bytes(
        encode_frame(MessageKind.REQUEST, 0x0102030405060708, payload)
    )
    hashes = {
        "schema_hash": _tree_hash(
            tuple((CONTRACT_ROOT / "schemas").glob("*.json")),
            normalize_text=True,
        ),
        "golden_vectors_hash": _tree_hash(tuple(golden_directory.glob("*.bin"))),
        "compatibility_matrix_hash": sha256(
            (CONTRACT_ROOT / "compatibility-matrix.json")
            .read_bytes()
            .replace(b"\r\n", b"\n")
        ).hexdigest(),
    }
    (CONTRACT_ROOT / "schema-hashes.json").write_text(
        json.dumps(hashes, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

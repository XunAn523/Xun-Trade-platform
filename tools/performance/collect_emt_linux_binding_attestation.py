"""Emit the M2-E-02H Linux EMT static-binding attestation on its target host."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping

from emt_gateway.linux_attestation import collect_linux_binding_attestation
from emt_gateway.platform_loader import PlatformLoadError


def load_expected_library_hashes(path: Path) -> Mapping[str, str]:
    """Load a standalone, non-secret mapping of SDK file names to SHA-256."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise PlatformLoadError("EMT SDK hash manifest is unreadable") from exc
    if not isinstance(value, dict) or not value:
        raise PlatformLoadError("EMT SDK hash manifest must be a non-empty object")
    hashes: dict[str, str] = {}
    for name, digest in value.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            raise PlatformLoadError("EMT SDK hash manifest entries must be string pairs")
        hashes[name] = digest
    return hashes


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect a sanitized Linux project-.venv CPython 3.10 EMT binding attestation"
    )
    parser.add_argument("--sdk-root", type=str, required=True)
    parser.add_argument("--expected-hashes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    attestation = collect_linux_binding_attestation(
        sdk_root=args.sdk_root,
        expected_library_hashes=load_expected_library_hashes(args.expected_hashes),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(attestation.as_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"EMT_LINUX_BINDING_ATTESTATION={attestation.result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

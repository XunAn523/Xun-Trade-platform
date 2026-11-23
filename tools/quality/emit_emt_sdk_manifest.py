"""Emit a payload-free EMT SDK binary version and SHA-256 manifest.

The vendor SDK is supplied outside Git.  This tool deliberately does not
import it: importing may load a native extension or start vendor logging.  A
reviewed Linux native runner supplies the SDK path and vendor version, then
archives the resulting manifest as CI evidence.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

_BINARY_SUFFIXES = frozenset({".dll", ".pyd", ".so"})


def build_manifest(*, sdk_root: Path, sdk_version: str) -> dict[str, Any]:
    """Return a deterministic inventory of vendor native binaries by hash."""

    if not sdk_root.is_dir():
        raise ValueError("sdk_root must be an existing directory")
    if not sdk_version.strip():
        raise ValueError("sdk_version is required")
    binaries = [
        path
        for path in sorted(sdk_root.rglob("*"))
        if path.is_file()
        and (path.suffix.casefold() in _BINARY_SUFFIXES or ".so." in path.name.casefold())
    ]
    if not binaries:
        raise ValueError("sdk_root contains no native EMT SDK binary")
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sdk_version": sdk_version.strip(),
        "files": [
            {
                "path": path.relative_to(sdk_root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for path in binaries
        ],
    }


def write_manifest(*, manifest: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as binary:
        while chunk := binary.read(1_048_576):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="emit EMT SDK binary manifest")
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--sdk-version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        manifest = build_manifest(sdk_root=args.sdk_root, sdk_version=args.sdk_version)
        write_manifest(manifest=manifest, output=args.output)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

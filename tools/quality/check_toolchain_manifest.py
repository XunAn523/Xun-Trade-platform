"""Verify the versioned Linux build-tool boundary without downloading tools."""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping


class ToolchainManifestError(ValueError):
    """A runner does not match its reviewed toolchain manifest."""


def load_manifest(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ToolchainManifestError("cannot read toolchain manifest") from exc
    if not isinstance(value, Mapping) or value.get("schema_version") != 1:
        raise ToolchainManifestError("unsupported toolchain manifest version")
    return value


def verify_manifest(
    manifest: Mapping[str, Any], *, require_performance_tools: bool = False
) -> None:
    platform_spec, tools = manifest.get("platform"), manifest.get("tools")
    if not isinstance(platform_spec, Mapping) or not isinstance(tools, Mapping):
        raise ToolchainManifestError("toolchain manifest is missing platform or tools")
    expected = (
        platform_spec.get("system"),
        platform_spec.get("machine"),
        platform_spec.get("python_implementation"),
        platform_spec.get("python_minor"),
    )
    actual = (
        platform.system(),
        platform.machine(),
        platform.python_implementation(),
        f"{sys.version_info.major}.{sys.version_info.minor}",
    )
    if expected != actual:
        raise ToolchainManifestError(
            f"runner platform mismatch: expected={expected} actual={actual}"
        )
    uv = tools.get("uv")
    if not isinstance(uv, Mapping) or not isinstance(uv.get("version"), str):
        raise ToolchainManifestError("toolchain manifest has no uv version")
    _require_version("uv", str(uv["version"]))
    git = tools.get("git")
    if not isinstance(git, Mapping) or git.get("required") is not True:
        raise ToolchainManifestError("toolchain manifest must require git")
    _require_version("git", None)
    if require_performance_tools:
        perf = tools.get("perf")
        if (
            not isinstance(perf, Mapping)
            or perf.get("required_for_performance_evidence") is not True
        ):
            raise ToolchainManifestError("toolchain manifest must require perf for evidence")
        _require_version("perf", None)


def _require_version(command: str, expected: str | None) -> None:
    path = shutil.which(command)
    if path is None:
        raise ToolchainManifestError(f"required tool is unavailable: {command}")
    try:
        output = subprocess.run(
            [path, "--version"], check=True, capture_output=True, text=True, timeout=3
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ToolchainManifestError(f"cannot inspect tool version: {command}") from exc
    if expected is not None and expected not in output.split():
        raise ToolchainManifestError(f"tool version mismatch for {command}: {output}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="verify reviewed Linux build toolchain")
    parser.add_argument(
        "--manifest", type=Path, default=Path("tools/quality/toolchain-manifest.v1.json")
    )
    parser.add_argument("--require-performance-tools", action="store_true")
    args = parser.parse_args(argv)
    try:
        verify_manifest(
            load_manifest(args.manifest), require_performance_tools=args.require_performance_tools
        )
    except ToolchainManifestError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

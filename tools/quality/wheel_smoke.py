"""Build and install the wheel with lock-pinned runtime dependencies, outside source imports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from zipfile import ZipFile

from .run_acceptance import ROOT, revision, run_bounded


def snapshot_sources(destination: Path) -> str:
    """Copy only packaging inputs, excluding ignored incremental build outputs."""
    names = (
        subprocess.check_output(["git", "ls-files", "-coz", "--exclude-standard"], cwd=ROOT)
        .decode("utf-8")
        .split("\0")
    )
    roots = {"xuntrade_omsems", "xuanling_tradecore", "emt_gateway", "native", "contracts"}
    files = {"pyproject.toml", "setup.py", "README.md", "MANIFEST.in", "CMakeLists.txt"}
    digest = hashlib.sha256()
    for name in sorted(set(names) - {""}):
        path = Path(name)
        if path.parts[0] not in roots and name not in files:
            continue
        source = (ROOT / path).resolve()
        if not source.is_relative_to(ROOT) or not source.is_file():
            continue
        target = destination / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        digest.update(name.encode("utf-8") + b"\0" + source.read_bytes())
    return digest.hexdigest()


def smoke(output: Path) -> dict:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    uv = [sys.executable, "-m", "uv"]
    # In CI uv is bootstrapped into the setup-python interpreter, outside .venv.
    probe = subprocess.run([*uv, "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if probe.returncode:
        uv = ["uv"]
    commands = [
        [
            *uv,
            "export",
            "--locked",
            "--no-dev",
            "--no-emit-project",
            "--output-file",
            str(output / "runtime.txt"),
        ],
        [*uv, "build", "--wheel", "--python", sys.executable, "--out-dir", str(output / "dist")],
    ]
    source = revision()
    fresh_source = Path(tempfile.mkdtemp(prefix="clean-source-", dir=output)).resolve()
    assert fresh_source.is_relative_to(output)
    source_fingerprint = snapshot_sources(fresh_source)
    commands[1].append(str(fresh_source))
    for i, command in enumerate(commands):
        result = run_bounded(command, output / f"prepare-{i}.log", 300)
        if result["status"] != "passed":
            raise RuntimeError(f"wheel preparation failed: {result}")
    wheels = list((output / "dist").glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("wheel directory must contain exactly one artifact")
    wheel = wheels[0]
    with ZipFile(wheel) as archive:
        names = archive.namelist()
        if any(name.startswith("xuanling_tradecore/consumer/emt_") for name in names):
            raise RuntimeError("wheel contains stale pre-migration Consumer modules")
        if "xuanling_tradecore/compatibility/emt_consumer/emt_submit_worker_v1.py" not in names:
            raise RuntimeError("wheel is missing the current EMT composition namespace")
    environment = Path(tempfile.mkdtemp(prefix="wheel-env-", dir=output)).resolve()
    assert environment.is_relative_to(output)
    result = run_bounded(
        [*uv, "venv", "--python", sys.executable, str(environment)], output / "venv.log", 120
    )
    if result["status"] != "passed":
        raise RuntimeError(f"wheel environment failed: {result}")
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    result = run_bounded(
        [
            *uv,
            "pip",
            "install",
            "--python",
            str(python),
            "--requirement",
            str(output / "runtime.txt"),
            "--no-deps",
            str(wheel),
        ],
        output / "install.log",
        300,
    )
    if result["status"] != "passed":
        raise RuntimeError(f"wheel installation failed: {result}")
    script = output / "verify_installed.py"
    script.write_text(
        """import importlib, json, pathlib, sys
from xuntrade_omsems import compute
from xuntrade_omsems.cli import main
environment=pathlib.Path(sys.prefix).resolve()
module=importlib.import_module("xuntrade_omsems._native")
assert pathlib.Path(module.__file__).resolve().is_relative_to(environment)
assert "site-packages" in str(module.__file__)
assert compute.backend_info()["cxx_standard"] == 202002
assert compute.backend_info()["engine"] == "cpp20"
importlib.import_module("emt_gateway._fastspi")
if sys.platform == "linux": importlib.import_module("xuanling_tradecore._fastspi_v2")
assert main(["self-check", "--require-native"]) == 0
print(json.dumps({"installed_native":module.__file__,"prefix":sys.prefix,"compiler_standard":202002}))
""",
        encoding="utf-8",
    )
    # -I excludes source/PYTHONPATH. The venv has only runtime requirements and the built wheel.
    result = run_bounded(
        [str(python), "-I", str(script)],
        output / "smoke.log",
        60,
        env={**os.environ, "XUNTRADE_BACKEND": "native", "CC": "unavailable", "CXX": "unavailable"},
        cwd=environment,
    )
    payload = {
        "source": source,
        "source_snapshot_sha256": source_fingerprint,
        "wheel": wheel.name,
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "status": result["status"],
        "isolated_environment": str(environment),
        "runtime_requirements_sha256": hashlib.sha256(
            (output / "runtime.txt").read_bytes()
        ).hexdigest(),
        "verification": result,
    }
    (output / "wheel-report.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "build/acceptance/wheel")
    args = parser.parse_args()
    result = smoke(args.output_dir)
    print(json.dumps(result))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""One versioned test manifest for local verification and CI, with bounded isolation."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import signal
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = Path(__file__).with_name("test_suites.json")


def load_suite(name: str) -> dict:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if payload["schema_version"] != 1:
        raise ValueError("unsupported test manifest")
    suite = payload["suites"][name]
    files = suite["files"]
    if not files or len(files) != len(set(files)):
        raise ValueError("empty/duplicate suite files")
    for name in files:
        path = (ROOT / name).resolve()
        if not path.is_relative_to(ROOT) or not path.is_file() or path.suffix != ".py":
            raise ValueError(f"invalid test path: {name}")
    return suite


def revision() -> dict:
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT))
    return {
        "commit": sha,
        "working_tree_dirty": dirty,
        "lock_sha256": hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest(),
        "test_manifest_sha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),
    }


def classify_skip(reason: str, backend: str) -> tuple[str, str]:
    text = reason.lower()
    if "postgres" in text:
        return (
            "postgres",
            "Provide an isolated PostgreSQL test database and EMT_GATEWAY_TEST_POSTGRES_DSN.",
        )
    if "linux" in text or "posix" in text:
        return "platform", "Run the same manifest on Linux with the required POSIX/native features."
    if "native" in text and backend == "python":
        return "reference_not_applicable", "Run native_core to exercise the C++ implementation."
    if "sdk" in text:
        return (
            "sdk",
            "Provide the matching official SDK and an explicitly configured isolated integration environment.",
        )
    return (
        "unclassified",
        "Review this node's prerequisite; an unclassified skip fails the acceptance runner.",
    )


class ResultCollector:
    def __init__(self, backend: str):
        self.backend = backend
        self.nodes: dict[str, dict] = {}

    def pytest_runtest_logreport(self, report):
        node = self.nodes.setdefault(
            report.nodeid, {"nodeid": report.nodeid, "duration_seconds": 0}
        )
        node["duration_seconds"] += report.duration
        if report.failed:
            node["outcome"] = "failed"
        elif report.skipped:
            reason = (
                str(report.longrepr[2])
                if isinstance(report.longrepr, tuple)
                else str(report.longrepr)
            )
            category, prerequisite = classify_skip(reason, self.backend)
            node.update(
                outcome="skipped", reason=reason, category=category, prerequisite=prerequisite
            )
        elif report.when == "call" and node.get("outcome") != "failed":
            node["outcome"] = "passed"


def run_bounded(
    command: list[str],
    log: Path,
    timeout_seconds: float,
    *,
    env: dict | None = None,
    cwd: Path = ROOT,
) -> dict:
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 3600:
        raise ValueError("timeout must be positive")
    log.parent.mkdir(parents=True, exist_ok=True)
    options = (
        {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        if os.name == "nt"
        else {"start_new_session": True}
    )
    started = time.monotonic()
    with log.open("wb") as stream:
        process = subprocess.Popen(
            command, stdout=stream, stderr=subprocess.STDOUT, cwd=cwd, env=env, **options
        )
        try:
            code = process.wait(timeout=timeout_seconds)
            status = "passed" if code == 0 else "failed"
        except subprocess.TimeoutExpired:
            status, code = "timed_out", None
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait(timeout=10)
    return {
        "status": status,
        "returncode": code,
        "elapsed_seconds": time.monotonic() - started,
        "log": str(log),
        "timeout_seconds": timeout_seconds,
    }


def worker(suite_name: str, output: Path, files: list[str]) -> int:
    import pytest

    suite = load_suite(suite_name)
    collector = ResultCollector(suite["backend"])
    result = pytest.main(
        [
            "-q",
            "-rs",
            "--strict-markers",
            "-o",
            "faulthandler_timeout=15",
            "-o",
            f"cache_dir={output / 'cache'}",
            "--junitxml",
            str(output / "junit.xml"),
            *files,
        ],
        plugins=[collector],
    )
    nodes = list(collector.nodes.values())
    unclassified = [n for n in nodes if n.get("category") == "unclassified"]
    payload = {
        "suite": suite_name,
        "files": files,
        "backend": suite["backend"],
        "environment": {"platform": platform.platform(), "python": platform.python_version()},
        "source": revision(),
        "pytest_exitcode": int(result),
        "counts": dict(Counter(n.get("outcome", "incomplete") for n in nodes)),
        "skips": [n for n in nodes if n.get("outcome") == "skipped"],
        "tests": nodes,
        "unclassified_skips": len(unclassified),
    }
    (output / "results.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return int(result) if result else int(bool(unclassified))


def run_suite(name: str, output: Path, *, timeout_seconds=600, per_file=False) -> dict:
    suite = load_suite(name)
    output.mkdir(parents=True, exist_ok=True)
    groups = [[f] for f in suite["files"]] if per_file else [suite["files"]]
    runs = []
    for number, files in enumerate(groups):
        target = output / f"run-{number:03d}-{uuid4().hex}"
        target.mkdir()
        command = [
            sys.executable,
            "-m",
            "tools.quality.run_acceptance",
            "--suite",
            name,
            "--output-dir",
            str(target),
            "--worker",
            "--files",
            *files,
        ]
        result = run_bounded(
            command,
            target / "pytest.log",
            timeout_seconds,
            env={**os.environ, "XUNTRADE_BACKEND": suite["backend"], "PYTHONUTF8": "1"},
        )
        result["files"] = files
        result_path = target / "results.json"
        if result_path.exists():
            result["details"] = json.loads(result_path.read_text(encoding="utf-8"))
        elif result["status"] == "passed":
            result["status"] = "failed"
            result["reason"] = "worker exited without a result artifact"
        runs.append(result)
    payload = {
        "suite": name,
        "source": revision(),
        "description": suite["description"],
        "status": "passed" if all(r["status"] == "passed" for r in runs) else "failed",
        "runs": runs,
    }
    (output / "summary.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


def catalog(output: Path) -> None:
    suites = json.loads(MANIFEST.read_text(encoding="utf-8"))["suites"]
    paths = sorted(
        {
            p
            for base in ("tests", "tools/performance/tests", "tools/acceptance/tests")
            for p in (ROOT / base).rglob("test_*.py")
        }
    )
    rows = [
        {
            "file": p.relative_to(ROOT).as_posix(),
            "suites": [
                name
                for name, suite in suites.items()
                if p.relative_to(ROOT).as_posix() in suite["files"]
            ],
        }
        for p in paths
    ]
    for row in rows:
        row["scope"] = "versioned_manifest" if row["suites"] else "not_yet_in_release_core"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps({"source": revision(), "files": rows}, indent=2) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="native_core")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "build/acceptance/native_core")
    parser.add_argument("--timeout-seconds", type=float, default=600)
    parser.add_argument("--per-file", action="store_true")
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--files", nargs="*", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.catalog:
        catalog(args.catalog)
        return 0
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.worker:
        selected = args.files or load_suite(args.suite)["files"]
        if not set(selected) <= set(load_suite(args.suite)["files"]):
            raise ValueError("worker files are outside the versioned manifest")
        return worker(args.suite, args.output_dir, selected)
    result = run_suite(
        args.suite, args.output_dir, timeout_seconds=args.timeout_seconds, per_file=args.per_file
    )
    print(
        json.dumps(
            {"suite": args.suite, "status": result["status"], "output": str(args.output_dir)}
        )
    )
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

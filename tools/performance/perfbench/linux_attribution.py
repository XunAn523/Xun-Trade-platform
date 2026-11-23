"""Payload-free Linux ``perf``/``ftrace`` attribution preflight evidence.

This command intentionally never starts, signals, or attaches to a trading
process.  It records whether a separately approved benchmark window *could*
collect the required host evidence.  Raw traces remain operator-managed files
outside Git, the Journal, and normal performance reports.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

ATTRIBUTION_PROFILE_VERSION = 1
ATTRIBUTION_PROFILES: Mapping[str, Mapping[str, object]] = {
    "c0-c7-linux-v1": {
        "profile_version": ATTRIBUTION_PROFILE_VERSION,
        "perf_stat_events": ("task-clock", "context-switches", "cpu-migrations"),
        "ftrace_events": ("sched:sched_switch", "sched:sched_wakeup", "block:block_rq_complete"),
        "maximum_capture_seconds": 60,
        "raw_trace_policy": "operator_retained_outside_git_and_business_journal",
    }
}


class LinuxAttributionError(ValueError):
    """The host cannot produce a qualified attribution preflight manifest."""


def collect_preflight(*, profile_name: str, journal_path: Path) -> dict[str, Any]:
    """Collect a bounded, payload-free manifest without changing host state."""

    if profile_name not in ATTRIBUTION_PROFILES:
        raise LinuxAttributionError(f"unknown attribution profile: {profile_name}")
    if platform.system() != "Linux":
        raise LinuxAttributionError("Linux attribution preflight requires Linux")
    if not journal_path.exists():
        raise LinuxAttributionError("journal_path must exist for attribution preflight")
    profile = ATTRIBUTION_PROFILES[profile_name]
    affinity = sorted(os.sched_getaffinity(0))
    perf_path = shutil.which("perf")
    tracefs = next(
        (
            path
            for path in (Path("/sys/kernel/tracing"), Path("/sys/kernel/debug/tracing"))
            if path.is_dir()
        ),
        None,
    )
    mount = _mount_evidence(journal_path)
    evidence = {
        "schema_version": 1,
        "evidence_kind": "linux_attribution_preflight",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "capture_mode": "preflight_only",
        "profile": {"name": profile_name, **profile},
        "host": {
            "system": platform.system(),
            "machine": platform.machine(),
            "kernel_release": platform.release(),
            "clocksource": _read_text(
                Path("/sys/devices/system/clocksource/clocksource0/current_clocksource")
            ),
            "process_cpu_affinity": affinity,
            "cpu_governor": _first_governor(),
            "perf_event_paranoid": _read_text(Path("/proc/sys/kernel/perf_event_paranoid")),
        },
        "journal_storage": mount,
        "tools": {
            "perf": _tool_evidence(perf_path, ("--version",)),
            "ftrace": {
                "available": tracefs is not None,
                "tracefs_kind": "available" if tracefs is not None else "unavailable",
                "required_events": list(profile["ftrace_events"]),
            },
        },
        "raw_trace_policy": profile["raw_trace_policy"],
    }
    evidence["evidence_id"] = _evidence_id(evidence)
    return evidence


def write_manifest(*, manifest: Mapping[str, Any], output: Path) -> None:
    """Atomically write a public, payload-free attribution manifest."""

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output)


def _mount_evidence(path: Path) -> dict[str, str]:
    try:
        result = subprocess.run(
            ["findmnt", "--json", "--target", str(path.resolve())],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
        filesystems = json.loads(result.stdout).get("filesystems", [])
        item = filesystems[0] if isinstance(filesystems, list) and filesystems else {}
        if not isinstance(item, Mapping):
            item = {}
        source = str(item.get("source", ""))
        return {
            "filesystem": str(item.get("fstype", "unknown")),
            "mount_target": str(item.get("target", "unknown")),
            "mount_options": str(item.get("options", "unknown")),
            "storage_identifier_hash": _hash_text(source) if source else "unknown",
        }
    except (OSError, ValueError, subprocess.SubprocessError):
        return {
            "filesystem": "unknown",
            "mount_target": "unknown",
            "mount_options": "unknown",
            "storage_identifier_hash": "unknown",
        }


def _tool_evidence(path: str | None, arguments: tuple[str, ...]) -> dict[str, str | bool]:
    if path is None:
        return {"available": False, "version": "unavailable"}
    try:
        result = subprocess.run(
            [path, *arguments], check=True, capture_output=True, text=True, timeout=3
        )
        version = (result.stdout or result.stderr).strip().splitlines()[0][:160]
        return {"available": True, "version": version}
    except (OSError, subprocess.SubprocessError):
        return {"available": False, "version": "unavailable"}


def _first_governor() -> str:
    candidates = sorted(Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor"))
    return _read_text(candidates[0]) if candidates else "unavailable"


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()[:160] or "unavailable"
    except OSError:
        return "unavailable"


def _hash_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _evidence_id(manifest: Mapping[str, Any]) -> str:
    stable = dict(manifest)
    stable.pop("generated_at_utc", None)
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(stable, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="collect Linux perf/ftrace attribution preflight evidence"
    )
    parser.add_argument("--profile", choices=sorted(ATTRIBUTION_PROFILES), default="c0-c7-linux-v1")
    parser.add_argument("--journal-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        write_manifest(
            manifest=collect_preflight(profile_name=args.profile, journal_path=args.journal_path),
            output=args.output,
        )
    except LinuxAttributionError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

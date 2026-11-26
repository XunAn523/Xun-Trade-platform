"""Reject common credential material before it becomes a tracked artifact.

The scanner is intentionally conservative: it reports only a path, line and
stable rule identifier, never the matched value.  It supplements human review
and the schema contracts; it does not claim to discover every possible secret.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

_TEXT_SUFFIXES = frozenset(
    {".cfg", ".conf", ".ini", ".json", ".md", ".py", ".sh", ".toml", ".txt", ".yaml", ".yml"}
)
_MAX_TEXT_BYTES = 2 * 1024 * 1024
_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("SCS001", re.compile(r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----")),
    (
        "SCS002",
        re.compile(
            r"(?i)\b(?:api[_-]?key|access[_-]?key|password|passwd|client[_-]?secret|"
            r"bearer[_-]?token|auth(?:entication)?[_-]?token)\b"
            r"\s*(?:=|:)\s*([\"'])(?!<[^>]+>|redacted\b|example\b|dummy\b|changeme\b|"
            r"none\b|null\b|synthetic[-_]|test[-_]|stub[-_]|sim[-_]|emt_gateway_test\b)"
            r"[a-z0-9+/=_-]{12,}\1"
        ),
    ),
    ("SCS003", re.compile(r"(?i)(?:https?|ssh)://[^\s/:@]+:[^\s@]+@")),
    ("SCS004", re.compile(r"(?i)\bauthorization\s*:\s*(?:bearer|basic)\s+\S{8,}")),
)


@dataclass(frozen=True)
class SensitiveContentFinding:
    """A payload-free sensitive-content finding."""

    path: Path
    line: int
    rule_id: str


def scan_text(*, path: Path, text: str) -> tuple[SensitiveContentFinding, ...]:
    """Return payload-free findings for one decoded text file."""

    findings: list[SensitiveContentFinding] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        for rule_id, pattern in _RULES:
            if pattern.search(line):
                findings.append(SensitiveContentFinding(path, line_number, rule_id))
    return tuple(findings)


def scan_paths(paths: Iterable[Path]) -> tuple[SensitiveContentFinding, ...]:
    """Scan supported small text files and skip binary or generated artifacts."""

    findings: list[SensitiveContentFinding] = []
    for path in paths:
        if not path.is_file() or path.suffix.lower() not in _TEXT_SUFFIXES:
            continue
        try:
            if path.stat().st_size > _MAX_TEXT_BYTES:
                continue
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        findings.extend(scan_text(path=path, text=text))
    return tuple(findings)


def tracked_or_untracked_paths(root: Path) -> tuple[Path, ...]:
    """Return Git-tracked plus non-ignored untracked paths without shell parsing."""

    completed = subprocess.run(
        ["git", "ls-files", "-coz", "--exclude-standard"],
        cwd=root,
        check=False,
        capture_output=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise RuntimeError("cannot enumerate repository files for sensitive-content scan")
    return tuple(root / name for name in completed.stdout.split("\0") if name)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="scan repository text for likely credential material"
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--paths", type=Path, nargs="+", help="explicit paths, for targeted validation"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the scanner and keep any output free of detected values."""

    arguments = _parse_args(sys.argv[1:] if argv is None else argv)
    root = arguments.root.resolve()
    paths = (
        tuple(
            (root / path).resolve() if not path.is_absolute() else path.resolve()
            for path in arguments.paths
        )
        if arguments.paths
        else tracked_or_untracked_paths(root)
    )
    findings = scan_paths(paths)
    for finding in findings:
        try:
            display_path = finding.path.relative_to(root).as_posix()
        except ValueError:
            display_path = finding.path.name
        print(
            f"{finding.rule_id}:{display_path}:{finding.line}: likely credential material",
            file=sys.stderr,
        )
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())

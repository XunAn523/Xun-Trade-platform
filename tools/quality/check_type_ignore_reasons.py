"""Reject unexplained ``type: ignore`` comments in reviewed Python code.

An ignore can be necessary at a dynamic vendor or test boundary, but it must
name the checker's diagnostic code and state why the local type model cannot
express the operation.  This keeps a future strict-Pyright expansion auditable.
"""

from __future__ import annotations

import re
import sys
from io import StringIO
from pathlib import Path
from tokenize import COMMENT, generate_tokens

ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOTS = tuple(
    ROOT / name
    for name in ("xuntrade_omsems", "xuanling_tradecore", "emt_gateway", "tests", "tools")
)
_IGNORE = re.compile(r"#\s*type:\s*ignore\[[^\]]+\]\s+#\s*(?P<reason>\S.*)\s*$")


def collect_violations() -> tuple[str, ...]:
    """Return source locations whose type ignore lacks a code or reason."""

    violations: list[str] = []
    for source_root in SOURCE_ROOTS:
        for path in sorted(source_root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            for token in generate_tokens(StringIO(source).readline):
                if token.type != COMMENT or "type: ignore" not in token.string:
                    continue
                if _IGNORE.fullmatch(token.string) is None:
                    relative = path.relative_to(ROOT).as_posix()
                    violations.append(
                        f"{relative}:{token.start[0]}: type: ignore needs a diagnostic code and reason"
                    )
    return tuple(violations)


def main() -> int:
    """Print violations and return a non-zero code when the contract fails."""

    violations = collect_violations()
    if not violations:
        return 0
    print("\n".join(violations), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

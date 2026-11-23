"""Write fail-closed V1.1 post-run evidence for a completed EMT C0--C7 burst."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from tools.performance.benchmark_evidence_v11 import (
    BenchmarkEvidenceV11Error,
    write_emt_gate_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        paths = write_emt_gate_evidence(
            profile_path=args.profile, report_path=args.report, output_dir=args.output_dir
        )
        result = {"status": "PASS", "evidence": {gate: path.name for gate, path in paths.items()}}
    except BenchmarkEvidenceV11Error as error:
        result = {"status": "BLOCKED", "error": str(error)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

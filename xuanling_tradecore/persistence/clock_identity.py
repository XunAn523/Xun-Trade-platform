"""Conservative identity for comparing Linux monotonic evidence timestamps."""

from __future__ import annotations

import json
import sys
from hashlib import sha256
from pathlib import Path


def monotonic_clock_identity() -> str | None:
    """Same boot AND time namespace/offsets; unknown is never a matching clock.

    Only an opaque digest is exported, not a machine ID, hostname or PID.
    This proves a clock domain label, not event provenance or clock accuracy.
    """
    if sys.platform != "linux":
        return None
    try:
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        namespace = Path("/proc/self/ns/time").stat()
        offsets = Path("/proc/self/timens_offsets").read_text()
        if not 1 <= len(boot) <= 128 or len(offsets) > 4096:
            return None
        return sha256(
            json.dumps(
                (boot, namespace.st_dev, namespace.st_ino, offsets), separators=(",", ":")
            ).encode()
        ).hexdigest()
    except (OSError, UnicodeError):
        return None

"""Client-side plan evidence; never invoked by the server's request hot path.

This checks the public bounds, not token signatures or durable checkpoints.
Persist the original cursor privately; exported evidence contains only its hash.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any, cast

from ..domain import ConsumerEventPositionV2


def build_replay_partition_evidence_v2(
    plan: Mapping[str, Any], *, expected_after_position: str | None = None
) -> dict[str, Any]:
    """Validate a COMPLETED CreateReplayPartitions payload and redact its tokens.

    Missing fields are different from a present null BEGIN. Do not substitute
    start_position/end_position aliases or use this function to validate PollEvents.
    An empty plan is valid but cannot satisfy a nonempty performance profile.
    """
    for name in (
        "journal_id",
        "stream_generation",
        "event_count",
        "high_watermark_position",
        "partitions",
    ):
        if name not in plan:
            raise ValueError(f"partition plan missing {name}")
    if not isinstance(plan["journal_id"], str) or not plan["journal_id"]:
        raise ValueError("invalid journal_id")
    generation, total = plan["stream_generation"], plan["event_count"]
    if type(generation) is not int or generation < 1:
        raise ValueError("invalid stream_generation")
    if type(total) is not int or total < 0:
        raise ValueError("invalid event_count")
    partitions = plan["partitions"]
    if not isinstance(partitions, list):
        raise ValueError("invalid partitions")
    entries = cast(list[object], partitions)
    if len(entries) > 64:
        raise ValueError("invalid partitions")
    # Suffix plans begin at the preceding DURABLE exhausted cursor, not BEGIN.
    # This is a metadata comparison only, never permission to forge a cursor.
    previous = expected_after_position
    previous_position = (
        None
        if previous is None
        else ConsumerEventPositionV2.from_external_id(previous)
    )
    count = 0
    rows: list[dict[str, Any]] = []
    token_hashes: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise ValueError("invalid partition entry")
        part = cast(Mapping[str, Any], entry)
        for name in (
            "partition_id",
            "after_position",
            "high_watermark_position",
            "event_count",
            "cursor",
        ):
            if name not in part:
                raise ValueError(f"partition missing {name}")
        if type(part["partition_id"]) is not int or part["partition_id"] != index:
            raise ValueError("partition_id must follow plan order")
        if part["after_position"] != previous:
            raise ValueError("partition BEGIN/adjacency mismatch")
        high = ConsumerEventPositionV2.from_external_id(part["high_watermark_position"])
        if previous_position is not None and high <= previous_position:
            raise ValueError("partition end must strictly increase")
        amount, token = part["event_count"], part["cursor"]
        if type(amount) is not int or amount < 1:
            raise ValueError("partition event_count must be positive")
        if not isinstance(token, str) or not token:
            raise ValueError("partition cursor must be nonempty")
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        if digest in token_hashes:
            raise ValueError("partition cursors must be distinct")
        token_hashes.add(digest)
        rows.append(
            {
                "partition_id": index,
                "after_position": previous,
                "after_position_is_begin": previous is None,
                "high_watermark_position": part["high_watermark_position"],
                "event_count": amount,
                "cursor_sha256": digest,
            }
        )
        count += amount
        previous, previous_position = part["high_watermark_position"], high
    if count != total or plan["high_watermark_position"] != (previous if entries else None):
        raise ValueError("partition total/final watermark mismatch")
    return {
        "evidence_schema": "consumer-replay-partition-evidence-v1",
        "journal_id": plan["journal_id"],
        "stream_generation": generation,
        "event_count": total,
        "high_watermark_position": previous if entries else None,
        "interval_after_position": expected_after_position,
        "interval_semantics": "(after_position, high_watermark_position]",
        "bounds_valid": True,
        "cursor_signature_verified": False,
        "checkpoint_evidence": False,
        "partitions": rows,
    }

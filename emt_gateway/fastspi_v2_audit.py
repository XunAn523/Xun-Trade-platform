"""Static C5-boundary audit for the V2 Python TraderSpi bridge.

The audit is intentionally run by tests or release checks, never by an SDK
callback.  It prevents a maintenance change from quietly pulling domain-object
creation or synchronous work back into the C5 forwarding method.
"""

from __future__ import annotations

import ast
from pathlib import Path


class FastSpiV2C5AuditError(RuntimeError):
    """The V2 bridge source contains a forbidden C5 operation."""


_FORBIDDEN_CALLS = frozenset(
    {
        "uuid4",
        "dataclass",
        "replace",
        "CallbackRecord",
        "CallbackPipeline",
        "JournalAppend",
        "append",
        "append_batch",
        "record_metric",
        "on_enqueued",
        "on_durable",
        "on_ack",
        "execute",
        "connect",
        "send",
        "recv",
    }
)


def assert_fastspi_v2_c5_source_constrained() -> None:
    """Reject forbidden calls and dictionary literals in V2 bridge C5 source."""

    source_path = Path(__file__).with_name("spi_bridge_v2.py")
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    bridge_method = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "_forward_callback"
        ),
        None,
    )
    if bridge_method is None:
        raise FastSpiV2C5AuditError("fastspi v2 bridge forwarding method is missing")
    forbidden: list[str] = []
    for node in ast.walk(bridge_method):
        if isinstance(node, ast.Dict):
            forbidden.append("dictionary literal")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            else:
                continue
            if name in _FORBIDDEN_CALLS:
                forbidden.append(name)
    if forbidden:
        raise FastSpiV2C5AuditError(
            "fastspi v2 C5 source uses forbidden operations: " + ", ".join(sorted(set(forbidden)))
        )

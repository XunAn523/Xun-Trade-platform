"""Execution-algorithm planning contracts.

Algorithms are deliberately pure planners: they can only derive immutable
``ExecutionInstruction`` child intents.  OMS reservation and EMS Guard remain
the only paths that can authorize a child for adapter dispatch.
"""

from .planning import (
    AlgorithmChildInstruction,
    AlgorithmPlan,
    ExecutionAlgorithm,
    ExecutionAlgorithmError,
    MaxQuantitySliceAlgorithm,
    SliceAlgorithmSpec,
)
from .scheduling_v1 import (
    AlgorithmAuditFactV1,
    AlgorithmAuditPortV1,
    AlgorithmChildStateV1,
    AlgorithmRunControllerV1,
    AlgorithmRunSnapshotV1,
    AlgorithmRunStateV1,
    ExecutionAlgorithmSelectorV1,
    PovAlgorithmV1,
    PovScheduleBucketV1,
    PovSpecV1,
    TargetPositionAlgorithmV1,
    TargetPositionSpecV1,
    TwapAlgorithmV1,
    TwapSpecV1,
    VolumeScheduleBucketV1,
    VwapAlgorithmV1,
    VwapSpecV1,
)

__all__ = [
    "AlgorithmChildInstruction",
    "AlgorithmPlan",
    "ExecutionAlgorithm",
    "ExecutionAlgorithmError",
    "MaxQuantitySliceAlgorithm",
    "SliceAlgorithmSpec",
    "AlgorithmAuditFactV1",
    "AlgorithmAuditPortV1",
    "AlgorithmChildStateV1",
    "AlgorithmRunControllerV1",
    "AlgorithmRunSnapshotV1",
    "AlgorithmRunStateV1",
    "ExecutionAlgorithmSelectorV1",
    "PovAlgorithmV1",
    "PovScheduleBucketV1",
    "PovSpecV1",
    "TargetPositionAlgorithmV1",
    "TargetPositionSpecV1",
    "TwapAlgorithmV1",
    "TwapSpecV1",
    "VolumeScheduleBucketV1",
    "VwapAlgorithmV1",
    "VwapSpecV1",
]

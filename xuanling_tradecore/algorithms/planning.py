"""Pure, auditable child-instruction planning for M5 execution algorithms."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol

from ..domain import ExecutionInstruction
from ..domain._validation import require_positive, require_text


class ExecutionAlgorithmError(ValueError):
    """An algorithm plan cannot prove safe child-instruction lineage."""


@dataclass(frozen=True)
class SliceAlgorithmSpec:
    """Versioned input for a deterministic maximum-quantity slicing plan."""

    algorithm_id: str
    algorithm_version: int
    max_child_quantity: int

    def __post_init__(self) -> None:
        require_text(self.algorithm_id, "execution algorithm_id", max_length=128)
        require_positive(self.algorithm_version, "execution algorithm_version")
        require_positive(self.max_child_quantity, "execution max_child_quantity")


@dataclass(frozen=True)
class AlgorithmChildInstruction:
    """One child that retains its parent OMS identity and attribution evidence."""

    parent_execution_id: str
    child_index: int
    instruction: ExecutionInstruction

    def __post_init__(self) -> None:
        require_text(self.parent_execution_id, "algorithm parent_execution_id")
        if self.child_index < 1:
            raise ExecutionAlgorithmError("algorithm child_index must be positive")
        if self.instruction.execution_id == self.parent_execution_id:
            raise ExecutionAlgorithmError("algorithm child execution_id must differ from parent")


@dataclass(frozen=True)
class AlgorithmPlan:
    """Immutable execution plan; it is not an authorization or dispatch record."""

    algorithm_id: str
    algorithm_version: int
    parent_instruction: ExecutionInstruction
    children: tuple[AlgorithmChildInstruction, ...]

    def __post_init__(self) -> None:
        require_text(self.algorithm_id, "algorithm plan algorithm_id", max_length=128)
        require_positive(self.algorithm_version, "algorithm plan algorithm_version")
        if not self.children:
            raise ExecutionAlgorithmError("algorithm plan must have at least one child")
        child_ids = tuple(child.instruction.execution_id for child in self.children)
        if len(child_ids) != len(set(child_ids)):
            raise ExecutionAlgorithmError("algorithm plan repeats child execution_id")
        if tuple(child.child_index for child in self.children) != tuple(
            range(1, len(self.children) + 1)
        ):
            raise ExecutionAlgorithmError("algorithm plan child indexes must be contiguous")
        total_quantity = 0
        for child in self.children:
            if child.parent_execution_id != self.parent_instruction.execution_id:
                raise ExecutionAlgorithmError("algorithm child belongs to another parent")
            instruction = child.instruction
            parent = self.parent_instruction
            if (
                instruction.oms_order_id != parent.oms_order_id
                or instruction.account_id != parent.account_id
                or instruction.trading_day != parent.trading_day
                or instruction.consumer_trace != parent.consumer_trace
                or instruction.symbol != parent.symbol
                or instruction.market != parent.market
                or instruction.side != parent.side
                or instruction.price_type != parent.price_type
                or instruction.business_type != parent.business_type
                or instruction.protection_price_ticks != parent.protection_price_ticks
                or instruction.limit_price_ticks != parent.limit_price_ticks
            ):
                raise ExecutionAlgorithmError(
                    "algorithm child changes immutable OMS/EMS routing or protection fields"
                )
            total_quantity += instruction.quantity
        if total_quantity != self.parent_instruction.quantity:
            raise ExecutionAlgorithmError("algorithm children must preserve parent quantity")


class ExecutionAlgorithm(Protocol):
    """A planner that returns child instructions only; it has no adapter surface."""

    def plan(self, parent: ExecutionInstruction) -> AlgorithmPlan:
        """Derive immutable children for later OMS/EMS admission."""

        ...


class MaxQuantitySliceAlgorithm:
    """Deterministically split one parent into bounded contiguous child quantities."""

    def __init__(self, spec: SliceAlgorithmSpec) -> None:
        self._spec = spec

    @property
    def spec(self) -> SliceAlgorithmSpec:
        return self._spec

    def plan(self, parent: ExecutionInstruction) -> AlgorithmPlan:
        remaining = parent.quantity
        children: list[AlgorithmChildInstruction] = []
        child_index = 1
        while remaining:
            quantity = min(remaining, self._spec.max_child_quantity)
            child_execution_id = f"{parent.execution_id}:child:{child_index}"
            if len(child_execution_id) > 256:
                raise ExecutionAlgorithmError("algorithm child execution_id exceeds 256 characters")
            parameters = dict(parent.execution_parameters)
            parameters.update(
                {
                    "algorithm_id": self._spec.algorithm_id,
                    "algorithm_version": str(self._spec.algorithm_version),
                    "algorithm_parent_execution_id": parent.execution_id,
                    "algorithm_child_index": str(child_index),
                }
            )
            child = replace(
                parent,
                execution_id=child_execution_id,
                quantity=quantity,
                execution_parameters=parameters,
            )
            children.append(AlgorithmChildInstruction(parent.execution_id, child_index, child))
            remaining -= quantity
            child_index += 1
        return AlgorithmPlan(
            algorithm_id=self._spec.algorithm_id,
            algorithm_version=self._spec.algorithm_version,
            parent_instruction=parent,
            children=tuple(children),
        )

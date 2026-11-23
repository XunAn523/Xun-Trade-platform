"""Fail-closed feature contract for the callback direct-write A/B candidate.

The contract intentionally does not dispatch on the vendor SDK callback
thread.  A composition may select ``DIRECT_CANDIDATE`` only after it has
proved that a scope-local *drain* worker is the sole callback-Journal writer.
The default is the established ingress -> SPSC target-lane control path.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CallbackDirectWriteError(RuntimeError):
    """A callback direct-write profile would weaken writer ownership evidence."""


class CallbackPersistenceProfileV1(str, Enum):
    CONTROL = "control"
    DIRECT_CANDIDATE = "direct-candidate"


@dataclass(frozen=True)
class CallbackDirectWriteEligibilityV1:
    """Static proof required before selecting the candidate for one scope."""

    scope_id: str
    disarmed_simulated: bool
    sealed_route_map: bool
    source_scope_equals_target_scope: bool
    sole_drain_writer: bool
    bounded_projector_inbox: bool
    callback_journal_capacity_available: bool

    def __post_init__(self) -> None:
        if not self.scope_id:
            raise CallbackDirectWriteError("callback direct-write scope is invalid")

    @property
    def eligible(self) -> bool:
        return (
            self.disarmed_simulated
            and self.sealed_route_map
            and self.source_scope_equals_target_scope
            and self.sole_drain_writer
            and self.bounded_projector_inbox
            and self.callback_journal_capacity_available
        )


def select_callback_persistence_profile_v1(
    *,
    requested: CallbackPersistenceProfileV1,
    eligibility: CallbackDirectWriteEligibilityV1,
) -> CallbackPersistenceProfileV1:
    """Select control or reject an unsafe candidate without silent fallback.

    An explicit candidate request that lacks all ownership and bounded-inbox
    proofs is rejected rather than silently changing a performance experiment
    back to control.  Normal startup requests ``CONTROL`` and needs no proof.
    """

    if requested is CallbackPersistenceProfileV1.CONTROL:
        return requested
    if not eligibility.eligible:
        raise CallbackDirectWriteError(
            "callback direct-write requires DISARMED simulation, sealed same-scope route, "
            "one drain writer, bounded projector inbox and Journal capacity"
        )
    return requested

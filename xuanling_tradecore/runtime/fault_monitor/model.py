"""Payload-limited fault facts; business payloads never enter this channel."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType
from typing import Mapping

from ...domain._validation import DomainValidationError, require_nonnegative, require_text


class OperationalFaultSeverityV1(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


def _empty_summary() -> Mapping[str, str]:
    return MappingProxyType({})


@dataclass(frozen=True)
class OperationalFaultV1:
    """One bounded diagnostic fact safe for a lossy asynchronous monitor."""

    sequence: int
    category: str
    severity: OperationalFaultSeverityV1
    observed_monotonic_ns: int
    account_id: str | None = None
    summary: Mapping[str, str] = field(default_factory=_empty_summary)

    def __post_init__(self) -> None:
        require_nonnegative(self.sequence, "operational fault sequence")
        require_text(self.category, "operational fault category", max_length=128)
        require_nonnegative(self.observed_monotonic_ns, "operational fault observed time")
        if self.account_id is not None:
            require_text(self.account_id, "operational fault account", max_length=128)
        values = dict(self.summary)
        if len(values) > 16:
            raise DomainValidationError("operational fault summary has too many fields")
        for key, value in values.items():
            require_text(key, "operational fault summary key", max_length=64)
            require_text(value, "operational fault summary value", max_length=256)
        object.__setattr__(self, "summary", MappingProxyType(values))

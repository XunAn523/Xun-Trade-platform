"""Protected public-key trust inputs for a simulator namespace release."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceVerifier,
    namespace_trust_anchor_hash_v1,
)
from xuanling_tradecore.domain._validation import require_text

from .consumer_runtime_control_v1 import LinuxRootOwnedNamespaceFileV1


class SimulatorNamespaceTrustErrorV1(RuntimeError):
    """Public namespace trust input is invalid."""


@dataclass(frozen=True)
class SimulatorNamespaceTrustV1:
    public_keys: Mapping[str, bytes]
    revoked_signer_ids: frozenset[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "public_keys", MappingProxyType(dict(self.public_keys)))

    @property
    def trust_anchor_hash(self) -> str:
        return namespace_trust_anchor_hash_v1(
            self.public_keys, revoked_signer_ids=self.revoked_signer_ids
        )

    def verifier(self) -> EmtOrderClientNamespaceVerifier:
        return EmtOrderClientNamespaceVerifier(
            self.public_keys, revoked_signer_ids=self.revoked_signer_ids
        )


def parse_simulator_namespace_trust_v1(text: str) -> SimulatorNamespaceTrustV1:
    def unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate trust field")
            result[key] = value
        return result

    try:
        document = json.loads(text, object_pairs_hook=unique_pairs)
        if (
            not isinstance(document, dict)
            or set(document) != {"schema_version", "trusted_public_keys", "revoked_signer_ids"}
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
        ):
            raise ValueError("invalid namespace trust schema")
        rows, revoked = document["trusted_public_keys"], document["revoked_signer_ids"]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 128 or not isinstance(revoked, list):
            raise ValueError("invalid namespace trust list")
        keys: dict[str, bytes] = {}
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"signer_id", "public_key_b64"}:
                raise ValueError("invalid namespace trust key")
            signer = require_text(row["signer_id"], "namespace signer", max_length=128)
            if signer in keys or not isinstance(row["public_key_b64"], str):
                raise ValueError("duplicate or invalid namespace signer")
            raw = base64.b64decode(row["public_key_b64"], validate=True)
            if len(raw) != 32:
                raise ValueError("namespace signer key is not Ed25519")
            keys[signer] = raw
        revocations = tuple(require_text(item, "revoked signer", max_length=128) for item in revoked)
        if len(revocations) > 128 or len(set(revocations)) != len(revocations):
            raise ValueError("invalid namespace revocations")
        return SimulatorNamespaceTrustV1(keys, frozenset(revocations))
    except (TypeError, ValueError, binascii.Error) as exc:
        raise SimulatorNamespaceTrustErrorV1("namespace trust input cannot be parsed") from exc


def load_simulator_namespace_trust_v1(path: Path) -> SimulatorNamespaceTrustV1:
    return parse_simulator_namespace_trust_v1(LinuxRootOwnedNamespaceFileV1(path).read())

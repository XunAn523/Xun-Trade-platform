import base64
import json

import pytest
from emt_gateway.consumer_simulator_namespace_trust_v1 import (
    SimulatorNamespaceTrustErrorV1,
    parse_simulator_namespace_trust_v1,
)
from xuanling_tradecore.adapters.emt import namespace_trust_anchor_hash_v1


def document():
    return {
        "schema_version": 1,
        "trusted_public_keys": [{"signer_id": "signer", "public_key_b64": base64.b64encode(b"x" * 32).decode()}],
        "revoked_signer_ids": [],
    }


def test_namespace_trust_is_immutable_and_matches_authoritative_hash():
    trust = parse_simulator_namespace_trust_v1(json.dumps(document()))
    assert trust.trust_anchor_hash == namespace_trust_anchor_hash_v1(
        {"signer": b"x" * 32}, revoked_signer_ids=frozenset()
    )
    with pytest.raises(TypeError):
        trust.public_keys["other"] = b"x" * 32
    assert trust.verifier() is not None


@pytest.mark.parametrize("case", ["duplicate", "short-key", "bad-base64", "duplicate-signer", "duplicate-revocation", "extra", "bool-schema"])
def test_namespace_trust_rejects_ambiguous_or_invalid_input(case):
    data = document()
    if case == "short-key":
        data["trusted_public_keys"][0]["public_key_b64"] = base64.b64encode(b"short").decode()
    elif case == "bad-base64":
        data["trusted_public_keys"][0]["public_key_b64"] = "bad!"
    elif case == "duplicate-signer":
        data["trusted_public_keys"].append(data["trusted_public_keys"][0])
    elif case == "duplicate-revocation":
        data["revoked_signer_ids"] = ["signer", "signer"]
    elif case == "extra":
        data["secret"] = "not-allowed"
    elif case == "bool-schema":
        data["schema_version"] = True
    text = json.dumps(data)
    if case == "duplicate":
        text = text[:-1] + ', "schema_version": 1}'
    with pytest.raises(SimulatorNamespaceTrustErrorV1):
        parse_simulator_namespace_trust_v1(text)

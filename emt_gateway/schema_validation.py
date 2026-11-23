"""Versioned JSON Schema contracts for configuration and persisted artifacts.

Schemas protect configuration-load and report-persistence boundaries only.  They
are deliberately not invoked on the C0-C7 command/callback hot paths, whose
domain objects have already passed their own immutable validation.
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import files
from typing import Mapping

from jsonschema import Draft202012Validator


class SchemaContractError(ValueError):
    """An instance does not satisfy its versioned public contract."""


_SCHEMA_FILES = {
    "performance-report-v1": "performance-report-v1.schema.json",
    "controlled-order-fixture-v1": "controlled-order-fixture-v1.schema.json",
    "mutation-fixture-v1": "mutation-fixture-v1.schema.json",
}


def schema_names() -> tuple[str, ...]:
    """Return the stable names of schemas distributed with this package."""

    return tuple(sorted(_SCHEMA_FILES))


@lru_cache(maxsize=None)
def schema_validator(schema_name: str) -> Draft202012Validator:
    """Load and meta-validate one bundled Draft 2020-12 schema."""

    try:
        filename = _SCHEMA_FILES[schema_name]
    except KeyError as exc:
        raise SchemaContractError(f"unknown schema contract: {schema_name}") from exc
    try:
        text = files("emt_gateway").joinpath("schemas", "v1", filename).read_text(encoding="utf-8")
        schema = json.loads(text)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise SchemaContractError(f"cannot load schema contract: {schema_name}") from exc
    if not isinstance(schema, Mapping):
        raise SchemaContractError(f"schema contract is not an object: {schema_name}")
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate_schema_instance(schema_name: str, instance: object) -> None:
    """Raise a bounded, payload-free error for the first schema violation."""

    validator = schema_validator(schema_name)
    errors = sorted(validator.iter_errors(instance), key=lambda error: list(error.absolute_path))
    if not errors:
        return
    error = errors[0]
    location = ".".join(str(part) for part in error.absolute_path) or "$"
    raise SchemaContractError(f"{schema_name} rejected {location}: {error.message}")

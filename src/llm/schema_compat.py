"""Validate that a JSON schema is compatible with Stage 4 vLLM usage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.common.config import load_json


ALLOWED_TOP_LEVEL_FIELDS = ("source", "targets", "event_type")
FORBIDDEN_SCHEMA_KEYS = {"anyOf", "oneOf", "allOf", "nullable"}


class SchemaCompatibilityError(ValueError):
    """Raised when a schema violates the Stage 4 compatibility rules."""


@dataclass(frozen=True)
class SchemaCompatibilityResult:
    """Outcome of the vLLM schema compatibility precheck."""

    is_compatible: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    normalized_schema: dict[str, Any] = field(default_factory=dict)


def load_stage4_schema(schema_path: str) -> dict[str, Any]:
    """Load the Stage 4 JSON schema from disk."""
    return load_json(schema_path)


def _walk_schema(node: Any, path: str, errors: list[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            next_path = f"{path}.{key}" if path else key
            if key in FORBIDDEN_SCHEMA_KEYS:
                errors.append(f"{next_path} uses forbidden keyword '{key}'.")
            if key == "type" and value == "null":
                errors.append(f"{next_path} uses forbidden type 'null'.")
            _walk_schema(value, next_path, errors)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _walk_schema(value, f"{path}[{index}]", errors)


def _validate_top_level_shape(schema: dict[str, Any], errors: list[str], warnings: list[str]) -> None:
    if schema.get("type") != "object":
        errors.append("Top-level schema type must be 'object'.")
    if schema.get("additionalProperties") is not False:
        errors.append("Top-level schema must set additionalProperties to false.")

    properties = schema.get("properties")
    if not isinstance(properties, dict):
        errors.append("Top-level schema must define an object-valued properties field.")
        return

    property_names = tuple(properties.keys())
    if property_names != ALLOWED_TOP_LEVEL_FIELDS:
        errors.append(
            "Top-level properties must appear exactly as "
            f"{list(ALLOWED_TOP_LEVEL_FIELDS)}; got {list(property_names)}."
        )

    required = schema.get("required")
    if required != list(ALLOWED_TOP_LEVEL_FIELDS):
        errors.append(
            "Top-level required fields must be "
            f"{list(ALLOWED_TOP_LEVEL_FIELDS)}; got {required!r}."
        )

    source_schema = properties.get("source", {})
    if source_schema.get("type") != "string":
        errors.append("properties.source must use type 'string'.")

    event_type_schema = properties.get("event_type", {})
    if event_type_schema.get("type") != "string":
        errors.append("properties.event_type must use type 'string'.")
    enum_values = event_type_schema.get("enum")
    if not isinstance(enum_values, list) or not enum_values or not all(isinstance(item, str) for item in enum_values):
        errors.append("properties.event_type.enum must be a non-empty string list.")

    targets_schema = properties.get("targets", {})
    if targets_schema.get("type") != "array":
        errors.append("properties.targets must use type 'array'.")
    items_schema = targets_schema.get("items")
    if not isinstance(items_schema, dict) or items_schema.get("type") != "string":
        errors.append("properties.targets.items must use type 'string'.")
    if targets_schema.get("minItems", 0) < 0:
        warnings.append("properties.targets.minItems should not be negative.")


def validate_vllm_schema(schema: dict[str, Any]) -> SchemaCompatibilityResult:
    """Check whether a schema satisfies the Stage 4 structured-output constraints."""
    errors: list[str] = []
    warnings: list[str] = []
    _walk_schema(schema, "", errors)
    _validate_top_level_shape(schema, errors, warnings)
    return SchemaCompatibilityResult(
        is_compatible=not errors,
        errors=errors,
        warnings=warnings,
        normalized_schema=schema,
    )


def assert_vllm_schema_compatible(schema: dict[str, Any]) -> dict[str, Any]:
    """Raise when the supplied schema is not compatible."""
    result = validate_vllm_schema(schema)
    if not result.is_compatible:
        joined_errors = "; ".join(result.errors)
        raise SchemaCompatibilityError(f"Stage 4 schema compatibility precheck failed: {joined_errors}")
    return result.normalized_schema

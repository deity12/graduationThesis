"""Structured output decoding helpers for Stage 4 extraction."""

from __future__ import annotations

import json
from typing import Any


ALLOWED_OUTPUT_FIELDS = {"source", "targets", "event_type"}


class StructuredDecodeError(ValueError):
    """Raised when structured JSON cannot be decoded into the Stage 4 shape."""


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_targets(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        candidate = _normalize_optional_string(value)
        return [candidate] if candidate else []
    if not isinstance(value, list):
        raise StructuredDecodeError(f"targets must be a list[str], got {type(value).__name__}.")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        candidate = _normalize_optional_string(item)
        if candidate is None or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def decode_structured_output(raw_output: str | dict[str, Any]) -> dict[str, Any]:
    """Decode a guided JSON payload and convert empty strings back to Python None."""
    if isinstance(raw_output, str):
        text = raw_output.strip()
        if not text:
            raise StructuredDecodeError("The model returned an empty response string.")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise StructuredDecodeError(f"Failed to parse structured JSON: {exc}") from exc
    elif isinstance(raw_output, dict):
        payload = dict(raw_output)
    else:
        raise StructuredDecodeError(f"Unsupported raw output type: {type(raw_output).__name__}.")

    if not isinstance(payload, dict):
        raise StructuredDecodeError("The decoded structured output must be a JSON object.")

    unknown_fields = sorted(set(payload) - ALLOWED_OUTPUT_FIELDS)
    if unknown_fields:
        raise StructuredDecodeError(f"Unexpected structured-output fields: {unknown_fields}.")

    return {
        "source": _normalize_optional_string(payload.get("source", "")),
        "targets": _normalize_targets(payload.get("targets", [])),
        "event_type": _normalize_optional_string(payload.get("event_type", "")),
    }

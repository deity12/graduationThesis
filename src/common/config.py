"""Minimal config loading helpers for stage 0."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_project_root() -> Path:
    """Return the repository root path."""
    return PROJECT_ROOT


def resolve_from_root(path: str | Path) -> Path:
    """Resolve a project-relative path against the repository root."""
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file from an absolute or project-relative path."""
    resolved_path = resolve_from_root(path)
    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise TypeError(f"Expected a mapping in YAML config: {resolved_path}")
    return payload


def load_json(path: str | Path) -> dict[str, Any]:
    """Load a JSON file from an absolute or project-relative path."""
    resolved_path = resolve_from_root(path)
    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise TypeError(f"Expected a mapping in JSON config: {resolved_path}")
    return payload

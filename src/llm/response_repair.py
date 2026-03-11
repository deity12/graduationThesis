"""Conservative helpers for normalizing structured JSON text."""

from __future__ import annotations


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3:
            return "\n".join(lines[1:-1]).strip()
    return stripped


def _extract_first_json_object(text: str) -> str:
    depth = 0
    start = -1
    in_string = False
    escape = False

    for index, char in enumerate(text):
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            if depth == 0:
                start = index
            depth += 1
            continue
        if char == "}":
            if depth == 0:
                continue
            depth -= 1
            if depth == 0 and start >= 0:
                return text[start : index + 1].strip()

    return text.strip()


def repair_structured_json_text(raw_content: str) -> str:
    """Remove trivial wrappers around a JSON object without altering keys or values."""
    stripped = _strip_code_fence(str(raw_content or ""))
    if not stripped:
        return ""
    return _extract_first_json_object(stripped)

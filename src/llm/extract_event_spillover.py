"""Stage 4 single-article spillover extraction pipeline."""

from __future__ import annotations

import json
from typing import Any

from src.llm.prompts import build_stage4_messages
from src.llm.structured_decode import decode_structured_output
from src.llm.vllm_client import VLLMClient


class MockExtractionClient:
    """Deterministic local mock client used when vLLM is unavailable."""

    def generate_structured_json(
        self,
        messages: list[dict[str, str]],
        schema: dict[str, Any],
        seed: int | None = None,
    ) -> dict[str, Any]:
        del messages, schema, seed
        return {
            "raw_content": json.dumps({"source": "", "targets": [], "event_type": "other"}),
            "request_mode": "mock",
            "response": {"mode": "mock"},
        }


def extract_event_spillover(
    news_row: dict[str, Any],
    schema: dict[str, Any],
    client: VLLMClient | MockExtractionClient,
    seed: int | None = None,
) -> dict[str, Any]:
    """Run one structured extraction request and normalize the decoded payload."""
    event_type_schema = schema.get("properties", {}).get("event_type", {})
    event_types = list(event_type_schema.get("enum", []))
    messages = build_stage4_messages(news_row, event_types=event_types)
    raw_result = client.generate_structured_json(messages=messages, schema=schema, seed=seed)
    decoded = decode_structured_output(raw_result["raw_content"])

    return {
        "news_id": news_row.get("news_id", ""),
        "published_date": news_row.get("published_date", ""),
        "source_ticker": news_row.get("source_ticker", ""),
        "source_company_name": news_row.get("source_company_name", ""),
        "request_mode": raw_result.get("request_mode", ""),
        "raw_content": raw_result.get("raw_content", ""),
        "extracted_source": decoded["source"],
        "extracted_targets": decoded["targets"],
        "extracted_event_type": decoded["event_type"],
        "is_valid_source": decoded["source"] is not None,
        "has_valid_targets": bool(decoded["targets"]),
        "valid_target_count": len(decoded["targets"]),
    }

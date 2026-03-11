"""Batch helpers for Stage 5 full extraction."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.llm.prompts import build_stage4_messages
from src.llm.response_repair import repair_structured_json_text
from src.llm.structured_decode import decode_structured_output
from src.mapping.target_resolver import TargetResolver


RAW_CACHE_COLUMNS = [
    "news_id",
    "shard_id",
    "batch_id",
    "published_date",
    "cutoff_date",
    "source_ticker",
    "source_company_name",
    "request_mode",
    "raw_content",
    "repaired_content",
    "response_repaired",
    "raw_content_sha256",
    "status",
    "error_type",
    "error_message",
    "seed",
    "run_mode",
    "processed_at_utc",
]

PARSED_CACHE_COLUMNS = [
    "news_id",
    "shard_id",
    "batch_id",
    "published_date",
    "cutoff_date",
    "is_warmup",
    "in_evaluation_window",
    "source_ticker",
    "source_company_name",
    "request_mode",
    "extracted_source",
    "extracted_targets",
    "extracted_event_type",
    "is_valid_source",
    "has_valid_targets",
    "valid_target_count",
    "resolved_target_tickers",
    "unresolved_targets",
    "resolved_target_count",
    "has_resolved_targets",
    "self_loop_targets_dropped",
    "target_resolution_json",
    "raw_content_sha256",
    "seed",
    "run_mode",
]

FAILURE_COLUMNS = [
    "news_id",
    "shard_id",
    "batch_id",
    "published_date",
    "cutoff_date",
    "source_ticker",
    "source_company_name",
    "request_mode",
    "raw_content_sha256",
    "error_type",
    "error_message",
    "seed",
    "run_mode",
]


@dataclass(frozen=True)
class BatchExtractResult:
    """In-memory result of one processed batch."""

    raw_frame: pd.DataFrame
    parsed_frame: pd.DataFrame
    failure_frame: pd.DataFrame
    stats: dict[str, Any]


def _frame_from_records(records: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(records, columns=columns)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = str(value or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _build_mock_result(
    news_row: dict[str, Any],
    seed: int | None,
    mock_targets: list[str],
    mock_event_types: list[str],
) -> dict[str, Any]:
    choices = [str(item).strip() for item in mock_targets if str(item).strip()]
    event_types = [str(item).strip() for item in mock_event_types if str(item).strip()]
    seed_value = int(seed or 0)
    if not choices:
        choices = ["Apple", "Microsoft"]
    if not event_types:
        event_types = ["other"]

    first_target = choices[seed_value % len(choices)]
    second_target = choices[(seed_value + 1) % len(choices)]
    payload = {
        "source": str(news_row.get("source_company_name", "") or news_row.get("source_ticker", "") or "").strip(),
        "targets": _dedupe_keep_order([first_target, second_target]),
        "event_type": event_types[seed_value % len(event_types)],
    }
    return {
        "raw_content": json.dumps(payload, ensure_ascii=False),
        "request_mode": "mock",
        "response": {"mode": "mock", "seed": seed_value},
    }


def extract_batch(
    *,
    records: list[dict[str, Any]],
    schema: dict[str, Any],
    client: Any,
    target_resolver: TargetResolver,
    shard_id: str,
    batch_id: int,
    run_mode: str,
    seed_base: int,
    drop_self_loops: bool,
    mock_targets: list[str],
    mock_event_types: list[str],
    mock_fail_news_ids: set[str],
) -> BatchExtractResult:
    """Process one batch and return raw/parsed/failure frames plus stats."""
    raw_records: list[dict[str, Any]] = []
    parsed_records: list[dict[str, Any]] = []
    failure_records: list[dict[str, Any]] = []
    event_types = list(schema.get("properties", {}).get("event_type", {}).get("enum", []))

    for index, news_row in enumerate(records):
        news_id = str(news_row.get("news_id", "")).strip()
        seed = seed_base + index
        request_mode = ""
        raw_content = ""
        raw_content_sha256 = ""
        processed_at_utc = datetime.now(timezone.utc).isoformat()
        cutoff_date = str(news_row.get("published_date", "")).strip()

        try:
            if news_id in mock_fail_news_ids:
                raise RuntimeError(f"Configured mock failure for news_id={news_id}")

            if run_mode == "mock":
                raw_result = _build_mock_result(
                    news_row=news_row,
                    seed=seed,
                    mock_targets=mock_targets,
                    mock_event_types=mock_event_types,
                )
            else:
                messages = build_stage4_messages(news_row, event_types=event_types)
                raw_result = client.generate_structured_json(messages=messages, schema=schema, seed=seed)

            request_mode = str(raw_result.get("request_mode", "")).strip()
            raw_content = str(raw_result.get("raw_content", "") or "")
            repaired_content = repair_structured_json_text(raw_content)
            raw_content_sha256 = _sha256_text(raw_content)
            decoded = decode_structured_output(repaired_content)

            resolutions = target_resolver.resolve_many(decoded["targets"])
            resolved_target_tickers = [
                str(item.get("resolved_ticker", "")).strip()
                for item in resolutions
                if bool(item.get("is_resolved")) and str(item.get("resolved_ticker", "")).strip()
            ]
            unresolved_targets = [
                str(item.get("input_target", "")).strip()
                for item in resolutions
                if not bool(item.get("is_resolved")) and str(item.get("status", "")) != "empty"
            ]
            resolved_target_tickers = _dedupe_keep_order(resolved_target_tickers)
            unresolved_targets = _dedupe_keep_order(unresolved_targets)

            self_loop_targets_dropped = 0
            source_ticker = str(news_row.get("source_ticker", "")).strip()
            if drop_self_loops and source_ticker:
                filtered_targets = [ticker for ticker in resolved_target_tickers if ticker != source_ticker]
                self_loop_targets_dropped = len(resolved_target_tickers) - len(filtered_targets)
                resolved_target_tickers = filtered_targets

            raw_records.append(
                {
                    "news_id": news_id,
                    "shard_id": shard_id,
                    "batch_id": batch_id,
                    "published_date": cutoff_date,
                    "cutoff_date": cutoff_date,
                    "source_ticker": source_ticker,
                    "source_company_name": str(news_row.get("source_company_name", "")).strip(),
                    "request_mode": request_mode,
                    "raw_content": raw_content,
                    "repaired_content": repaired_content,
                    "response_repaired": repaired_content != raw_content.strip(),
                    "raw_content_sha256": raw_content_sha256,
                    "status": "success",
                    "error_type": "",
                    "error_message": "",
                    "seed": seed,
                    "run_mode": run_mode,
                    "processed_at_utc": processed_at_utc,
                }
            )
            parsed_records.append(
                {
                    "news_id": news_id,
                    "shard_id": shard_id,
                    "batch_id": batch_id,
                    "published_date": cutoff_date,
                    "cutoff_date": cutoff_date,
                    "is_warmup": bool(news_row.get("is_warmup", False)),
                    "in_evaluation_window": bool(news_row.get("in_evaluation_window", False)),
                    "source_ticker": source_ticker,
                    "source_company_name": str(news_row.get("source_company_name", "")).strip(),
                    "request_mode": request_mode,
                    "extracted_source": decoded["source"],
                    "extracted_targets": decoded["targets"],
                    "extracted_event_type": decoded["event_type"],
                    "is_valid_source": decoded["source"] is not None,
                    "has_valid_targets": bool(decoded["targets"]),
                    "valid_target_count": len(decoded["targets"]),
                    "resolved_target_tickers": resolved_target_tickers,
                    "unresolved_targets": unresolved_targets,
                    "resolved_target_count": len(resolved_target_tickers),
                    "has_resolved_targets": bool(resolved_target_tickers),
                    "self_loop_targets_dropped": self_loop_targets_dropped,
                    "target_resolution_json": json.dumps(resolutions, ensure_ascii=False),
                    "raw_content_sha256": raw_content_sha256,
                    "seed": seed,
                    "run_mode": run_mode,
                }
            )
        except Exception as exc:
            error_type = type(exc).__name__
            error_message = str(exc)
            if raw_content:
                raw_content_sha256 = _sha256_text(raw_content)
            raw_records.append(
                {
                    "news_id": news_id,
                    "shard_id": shard_id,
                    "batch_id": batch_id,
                    "published_date": cutoff_date,
                    "cutoff_date": cutoff_date,
                    "source_ticker": str(news_row.get("source_ticker", "")).strip(),
                    "source_company_name": str(news_row.get("source_company_name", "")).strip(),
                    "request_mode": request_mode,
                    "raw_content": raw_content,
                    "repaired_content": "",
                    "response_repaired": False,
                    "raw_content_sha256": raw_content_sha256,
                    "status": "failed",
                    "error_type": error_type,
                    "error_message": error_message,
                    "seed": seed,
                    "run_mode": run_mode,
                    "processed_at_utc": processed_at_utc,
                }
            )
            failure_records.append(
                {
                    "news_id": news_id,
                    "shard_id": shard_id,
                    "batch_id": batch_id,
                    "published_date": cutoff_date,
                    "cutoff_date": cutoff_date,
                    "source_ticker": str(news_row.get("source_ticker", "")).strip(),
                    "source_company_name": str(news_row.get("source_company_name", "")).strip(),
                    "request_mode": request_mode,
                    "raw_content_sha256": raw_content_sha256,
                    "error_type": error_type,
                    "error_message": error_message,
                    "seed": seed,
                    "run_mode": run_mode,
                }
            )

    raw_frame = _frame_from_records(raw_records, RAW_CACHE_COLUMNS)
    parsed_frame = _frame_from_records(parsed_records, PARSED_CACHE_COLUMNS)
    failure_frame = _frame_from_records(failure_records, FAILURE_COLUMNS)
    stats = {
        "input_rows": len(records),
        "success_rows": len(parsed_frame),
        "failure_rows": len(failure_frame),
        "resolved_target_rows": int(parsed_frame["has_resolved_targets"].sum()) if not parsed_frame.empty else 0,
        "resolved_edge_count": int(parsed_frame["resolved_target_count"].sum()) if not parsed_frame.empty else 0,
    }
    return BatchExtractResult(
        raw_frame=raw_frame,
        parsed_frame=parsed_frame,
        failure_frame=failure_frame,
        stats=stats,
    )

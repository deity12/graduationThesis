"""Stage 5 full extraction runner with batch cache, checkpoint and resume support."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds

from src.analysis.stage5_report import build_stage5_summary, write_stage5_summary
from src.common.config import load_yaml, resolve_from_root
from src.common.io import ensure_parent_dir
from src.llm.batch_extract import extract_batch
from src.llm.cache_manager import Stage5CacheManager
from src.llm.schema_compat import assert_vllm_schema_compatible, load_stage4_schema, validate_vllm_schema
from src.llm.vllm_client import VLLMClient, VLLMClientConfig
from src.mapping.target_resolver import TargetResolver, TargetResolverConfig


DEFAULT_STAGE5_CONFIG = "configs/llm/full_extract_2016_2023.yaml"

INPUT_COLUMNS = [
    "news_id",
    "published_date",
    "title",
    "body",
    "summary_lsa",
    "summary_luhn",
    "summary_textrank",
    "summary_lexrank",
    "source_ticker",
    "source_company_name",
    "is_mapped",
    "is_warmup",
    "in_evaluation_window",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_STAGE5_CONFIG)
    parser.add_argument("--mode", default="")
    parser.add_argument("--seed", type=int, default=-1)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--small-smoke", action="store_true")
    parser.add_argument("--max-shards", type=int, default=-1)
    parser.add_argument("--max-batches-per-shard", type=int, default=-1)
    return parser.parse_args()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_model_identifier(model_name: str) -> bool:
    candidate = Path(model_name)
    if candidate.exists():
        return True
    parts = [part for part in model_name.split("/") if part]
    return len(parts) >= 2 and " " not in model_name


def _check_nvidia_smi() -> tuple[bool, bool]:
    try:
        completed = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return False, False
    return True, completed.returncode == 0


def _resolve_model_name(model_name: str) -> str:
    return str(model_name).strip()


def _build_vllm_client(config: dict[str, Any], runtime_payload: dict[str, Any]) -> VLLMClient:
    execution = config["stage5"]["execution"]
    runtime = runtime_payload["runtime"]
    return VLLMClient(
        VLLMClientConfig(
            base_url=str(execution["base_url"]),
            api_key=str(execution["api_key"]),
            model_name=_resolve_model_name(str(runtime["model_name"])),
            timeout_seconds=float(execution["timeout_seconds"]),
            temperature=float(runtime["temperature"]),
            top_p=float(runtime["top_p"]),
            max_output_tokens=int(runtime["max_output_tokens"]),
            request_mode=str(execution["request_mode"]),
        )
    )


def run_preflight(
    config: dict[str, Any],
    runtime_payload: dict[str, Any],
    client: VLLMClient,
    schema: dict[str, Any],
) -> dict[str, Any]:
    """Run only local-safe preflight checks before any real inference."""
    nvidia_smi_available, gpu_available = _check_nvidia_smi()
    vllm_import_available = importlib.util.find_spec("vllm") is not None
    model_name = str(runtime_payload["runtime"]["model_name"])
    model_identifier_parseable = _parse_model_identifier(model_name)
    http_preflight = client.preflight()
    allow_live_minimal_call = bool(config["stage5"]["execution"]["allow_live_minimal_call"])
    minimal_call_checked = False
    minimal_call_error: str | None = None
    structured_output_ready = bool(http_preflight.ready)
    if structured_output_ready and allow_live_minimal_call:
        minimal_call_checked = True
        structured_output_ready, minimal_call_error = client.run_minimal_structured_output_check(schema)
    return {
        "nvidia_smi_available": nvidia_smi_available,
        "gpu_available": gpu_available,
        "vllm_import_available": vllm_import_available,
        "model_identifier_parseable": model_identifier_parseable,
        "server_reachable": http_preflight.server_reachable,
        "model_resolved": http_preflight.model_resolved,
        "structured_output_ready": structured_output_ready,
        "ready_for_real_inference": (
            gpu_available
            and vllm_import_available
            and model_identifier_parseable
            and structured_output_ready
        ),
        "hosted_models": http_preflight.hosted_models,
        "http_error": http_preflight.error,
        "allow_live_minimal_call": allow_live_minimal_call,
        "minimal_call_checked": minimal_call_checked,
        "minimal_call_error": minimal_call_error,
    }


def _month_range(start_date: str, end_date: str) -> list[str]:
    months = pd.period_range(start=start_date[:7], end=end_date[:7], freq="M")
    return [period.strftime("%Y-%m") for period in months]


def _month_bounds(shard_id: str, end_date: str) -> tuple[str, str]:
    month_start = f"{shard_id}-01"
    month_end = (pd.Period(shard_id, freq="M").to_timestamp(how="end")).strftime("%Y-%m-%d")
    return month_start, min(month_end, end_date)


def _load_stage4_gate(stage4_report_path: str, require_gate: bool) -> dict[str, Any]:
    report_path = resolve_from_root(stage4_report_path)
    if not report_path.exists():
        if require_gate:
            raise FileNotFoundError(f"Stage 4 report required but not found: {report_path}")
        return {
            "stage4_formally_complete": False,
            "can_enter_stage5": False,
            "source_path": str(report_path),
        }
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    stage_gate = dict(payload.get("stage_gate", {}))
    stage_gate["source_path"] = str(report_path)
    if require_gate and not bool(stage_gate.get("can_enter_stage5", False)):
        raise RuntimeError(f"Stage 4 gate does not allow Stage 5: {report_path}")
    return stage_gate


def _load_shard_frame(
    *,
    input_path: str,
    shard_id: str,
    end_date: str,
    require_mapped_source: bool,
    require_source_ticker: bool,
    row_limit: int | None,
) -> pd.DataFrame:
    month_start, month_end = _month_bounds(shard_id, end_date=end_date)
    dataset = ds.dataset(resolve_from_root(input_path), format="parquet")
    filter_expr = (ds.field("published_date") >= month_start) & (ds.field("published_date") <= month_end)
    if require_mapped_source:
        filter_expr = filter_expr & (ds.field("is_mapped") == True)
    table = dataset.to_table(columns=INPUT_COLUMNS, filter=filter_expr)
    frame = table.to_pandas()
    if frame.empty:
        return frame
    frame["published_date"] = frame["published_date"].fillna("").astype(str)
    frame["source_ticker"] = frame["source_ticker"].fillna("").astype(str)
    frame["source_company_name"] = frame["source_company_name"].fillna("").astype(str)
    if require_source_ticker:
        frame = frame[frame["source_ticker"].ne("")]
    frame = frame.sort_values(["published_date", "news_id"]).reset_index(drop=True)
    if row_limit is not None and row_limit >= 0:
        frame = frame.head(row_limit)
    return frame


def _atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    target = ensure_parent_dir(resolve_from_root(path))
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temp_path.replace(target)


def _initialize_checkpoint(
    *,
    config_path: str,
    config: dict[str, Any],
    run_mode: str,
    stage4_gate: dict[str, Any],
    shards: list[str],
) -> dict[str, Any]:
    return {
        "created_at_utc": _utc_now(),
        "updated_at_utc": _utc_now(),
        "config_path": config_path,
        "run_mode": run_mode,
        "stage4_gate": stage4_gate,
        "input_path": config["stage5"]["inputs"]["news_source_mapped"],
        "filters": dict(config["stage5"]["filters"]),
        "shards": {
            shard_id: {
                "status": "pending",
                "last_completed_batch_index": -1,
                "total_rows": 0,
                "total_batches": 0,
                "processed_rows": 0,
                "success_rows": 0,
                "failure_rows": 0,
                "resolved_edge_count": 0,
                "updated_at_utc": _utc_now(),
            }
            for shard_id in shards
        },
        "totals": {
            "planned_shards": len(shards),
            "completed_shards": 0,
            "processed_rows": 0,
            "success_rows": 0,
            "failure_rows": 0,
            "resolved_edge_count": 0,
        },
    }


def _load_or_create_checkpoint(
    *,
    checkpoint_path: str,
    config_path: str,
    config: dict[str, Any],
    run_mode: str,
    stage4_gate: dict[str, Any],
    shards: list[str],
    overwrite: bool,
) -> dict[str, Any]:
    resolved = resolve_from_root(checkpoint_path)
    if resolved.exists() and not overwrite:
        checkpoint = json.loads(resolved.read_text(encoding="utf-8"))
        for shard_id in shards:
            checkpoint.setdefault("shards", {}).setdefault(
                shard_id,
                {
                    "status": "pending",
                    "last_completed_batch_index": -1,
                    "total_rows": 0,
                    "total_batches": 0,
                    "processed_rows": 0,
                    "success_rows": 0,
                    "failure_rows": 0,
                    "resolved_edge_count": 0,
                    "updated_at_utc": _utc_now(),
                },
            )
        checkpoint.setdefault("totals", {}).setdefault("planned_shards", len(shards))
        checkpoint["totals"]["planned_shards"] = len(shards)
        checkpoint["updated_at_utc"] = _utc_now()
        return checkpoint
    return _initialize_checkpoint(
        config_path=config_path,
        config=config,
        run_mode=run_mode,
        stage4_gate=stage4_gate,
        shards=shards,
    )


def _apply_cli_limits(
    *,
    shards: list[str],
    small_smoke: bool,
    max_shards: int,
    config: dict[str, Any],
) -> tuple[list[str], int | None, int | None]:
    smoke = config["stage5"]["smoke"]
    row_limit = None
    batch_limit_per_shard = None
    selected_shards = list(shards)

    if small_smoke:
        selected_shards = selected_shards[: int(smoke["shard_limit"])]
        batch_limit_per_shard = int(smoke["batch_limit_per_shard"])
        row_limit = int(smoke["row_limit_per_shard"])

    if max_shards is not None and max_shards > 0:
        selected_shards = selected_shards[:max_shards]

    return selected_shards, row_limit, batch_limit_per_shard


def _recompute_totals(checkpoint: dict[str, Any]) -> None:
    shard_values = list(checkpoint.get("shards", {}).values())
    checkpoint["totals"] = {
        "planned_shards": len(shard_values),
        "completed_shards": sum(1 for shard in shard_values if shard.get("status") == "completed"),
        "processed_rows": sum(int(shard.get("processed_rows", 0)) for shard in shard_values),
        "success_rows": sum(int(shard.get("success_rows", 0)) for shard in shard_values),
        "failure_rows": sum(int(shard.get("failure_rows", 0)) for shard in shard_values),
        "resolved_edge_count": sum(int(shard.get("resolved_edge_count", 0)) for shard in shard_values),
    }


def run_stage5_full_extract(
    *,
    config_path: str = DEFAULT_STAGE5_CONFIG,
    mode: str | None = None,
    seed: int | None = None,
    overwrite: bool = False,
    small_smoke: bool = False,
    max_shards: int | None = None,
    max_batches_per_shard: int | None = None,
) -> dict[str, Any]:
    """Execute the Stage 5 extraction flow."""
    config = load_yaml(config_path)
    runtime_payload = load_yaml(config["stage5"]["runtime_config"])
    schema = load_stage4_schema(config["stage5"]["schema_path"])
    schema_result_obj = validate_vllm_schema(schema)
    schema_result = {
        "is_compatible": schema_result_obj.is_compatible,
        "errors": schema_result_obj.errors,
        "warnings": schema_result_obj.warnings,
    }
    assert_vllm_schema_compatible(schema)

    if seed is not None and seed >= 0:
        config["stage5"]["execution"]["seed"] = int(seed)

    resolved_mode = mode or config["stage5"]["execution"]["mode"]
    stage4_gate = _load_stage4_gate(
        config["stage5"]["inputs"]["stage4_report_json"],
        require_gate=bool(config["stage5"]["inputs"]["require_stage4_gate"]),
    )

    filters = config["stage5"]["filters"]
    shard_ids = _month_range(str(filters["start_date"]), str(filters["end_date"]))
    selected_shards, row_limit, smoke_batch_limit = _apply_cli_limits(
        shards=shard_ids,
        small_smoke=small_smoke,
        max_shards=max_shards if max_shards is not None and max_shards > 0 else -1,
        config=config,
    )
    batch_limit_per_shard = (
        max_batches_per_shard if max_batches_per_shard is not None and max_batches_per_shard > 0 else smoke_batch_limit
    )

    cache_manager = Stage5CacheManager(
        raw_dir=config["stage5"]["cache"]["raw_dir"],
        parsed_dir=config["stage5"]["cache"]["parsed_dir"],
        failure_dir=config["stage5"]["cache"]["failure_dir"],
    )
    checkpoint_path = config["stage5"]["outputs"]["checkpoint_json"]
    checkpoint = _load_or_create_checkpoint(
        checkpoint_path=checkpoint_path,
        config_path=config_path,
        config=config,
        run_mode=resolved_mode,
        stage4_gate=stage4_gate,
        shards=selected_shards,
        overwrite=overwrite,
    )

    target_resolver = TargetResolver(
        TargetResolverConfig(
            alias_seed_csv=config["stage5"]["inputs"]["alias_seed_csv"],
            company_alias_min_length=int(config["stage5"]["target_resolution"]["company_alias_min_length"]),
            allow_exact_ticker=bool(config["stage5"]["target_resolution"]["allow_exact_ticker"]),
        )
    )

    preflight: dict[str, Any] | None = None
    client: Any = None
    if resolved_mode == "real":
        client = _build_vllm_client(config=config, runtime_payload=runtime_payload)
        preflight = run_preflight(config=config, runtime_payload=runtime_payload, client=client, schema=schema)
        if not preflight["ready_for_real_inference"]:
            raise RuntimeError("Stage 5 preflight failed. Refusing to run real full extraction.")
    elif resolved_mode == "dry_run":
        preflight = None
    elif resolved_mode != "mock":
        raise ValueError(f"Unsupported Stage 5 mode: {resolved_mode}")

    output_paths = {
        "checkpoint_json": config["stage5"]["outputs"]["checkpoint_json"],
        "summary_json": config["stage5"]["outputs"]["summary_json"],
        "summary_markdown": config["stage5"]["outputs"]["summary_markdown"],
        "failures_parquet": config["stage5"]["outputs"]["failures_parquet"],
        "extractions_parquet": config["stage5"]["outputs"]["extractions_parquet"],
        "edges_parquet": config["stage5"]["outputs"]["edges_parquet"],
    }
    cache_paths = {
        "raw_dir": config["stage5"]["cache"]["raw_dir"],
        "parsed_dir": config["stage5"]["cache"]["parsed_dir"],
        "failure_dir": config["stage5"]["cache"]["failure_dir"],
    }

    if resolved_mode == "dry_run":
        for shard_id in selected_shards:
            shard_frame = _load_shard_frame(
                input_path=config["stage5"]["inputs"]["news_source_mapped"],
                shard_id=shard_id,
                end_date=str(filters["end_date"]),
                require_mapped_source=bool(filters["require_mapped_source"]),
                require_source_ticker=bool(filters["require_source_ticker"]),
                row_limit=row_limit,
            )
            checkpoint["shards"][shard_id]["total_rows"] = int(len(shard_frame))
            checkpoint["shards"][shard_id]["total_batches"] = int(
                (len(shard_frame) + int(config["stage5"]["execution"]["batch_size"]) - 1)
                / int(config["stage5"]["execution"]["batch_size"])
            ) if len(shard_frame) > 0 else 0
            checkpoint["shards"][shard_id]["status"] = "planned"
            checkpoint["shards"][shard_id]["updated_at_utc"] = _utc_now()
        _recompute_totals(checkpoint)
        checkpoint["updated_at_utc"] = _utc_now()
        _atomic_write_json(checkpoint_path, checkpoint)
        summary = build_stage5_summary(
            config_path=config_path,
            run_mode=resolved_mode,
            checkpoint=checkpoint,
            preflight=preflight,
            stage4_gate=stage4_gate,
            output_paths=output_paths,
            cache_paths=cache_paths,
            validation_note="Dry-run only. No mock or real LLM extraction was executed.",
        )
        write_stage5_summary(summary, json_path=output_paths["summary_json"], markdown_path=output_paths["summary_markdown"])
        summary = build_stage5_summary(
            config_path=config_path,
            run_mode=resolved_mode,
            checkpoint=checkpoint,
            preflight=preflight,
            stage4_gate=stage4_gate,
            output_paths=output_paths,
            cache_paths=cache_paths,
            validation_note="Dry-run only. No mock or real LLM extraction was executed.",
        )
        write_stage5_summary(summary, json_path=output_paths["summary_json"], markdown_path=output_paths["summary_markdown"])
        return {
            "mode": resolved_mode,
            "checkpoint_json": output_paths["checkpoint_json"],
            "summary_json": output_paths["summary_json"],
            "summary_markdown": output_paths["summary_markdown"],
            "schema_compatibility": schema_result,
        }

    batch_size = int(config["stage5"]["execution"]["batch_size"])
    execution_seed = int(config["stage5"]["execution"]["seed"])
    mock_targets = list(config["stage5"]["mock"]["targets"])
    mock_event_types = list(config["stage5"]["mock"]["event_types"])
    mock_fail_news_ids = {str(item) for item in config["stage5"]["mock"]["fail_news_ids"]}
    drop_self_loops = bool(config["stage5"]["target_resolution"]["drop_self_loops"])

    for shard_id in selected_shards:
        shard_state = checkpoint["shards"][shard_id]
        if shard_state.get("status") == "completed":
            continue

        shard_frame = _load_shard_frame(
            input_path=config["stage5"]["inputs"]["news_source_mapped"],
            shard_id=shard_id,
            end_date=str(filters["end_date"]),
            require_mapped_source=bool(filters["require_mapped_source"]),
            require_source_ticker=bool(filters["require_source_ticker"]),
            row_limit=row_limit,
        )
        total_rows = int(len(shard_frame))
        total_batches = int((total_rows + batch_size - 1) / batch_size) if total_rows > 0 else 0
        shard_state["total_rows"] = total_rows
        shard_state["total_batches"] = total_batches
        shard_state["status"] = "completed" if total_rows == 0 else "in_progress"
        shard_state["updated_at_utc"] = _utc_now()
        _recompute_totals(checkpoint)
        checkpoint["updated_at_utc"] = _utc_now()
        _atomic_write_json(checkpoint_path, checkpoint)

        if total_rows == 0:
            continue

        for batch_id in range(total_batches):
            if batch_limit_per_shard is not None and batch_id >= batch_limit_per_shard:
                break
            if batch_id <= int(shard_state.get("last_completed_batch_index", -1)) and cache_manager.batch_complete(shard_id, batch_id):
                continue

            batch_start = batch_id * batch_size
            batch_end = min(batch_start + batch_size, total_rows)
            batch_records = shard_frame.iloc[batch_start:batch_end].to_dict(orient="records")
            result = extract_batch(
                records=batch_records,
                schema=schema,
                client=client,
                target_resolver=target_resolver,
                shard_id=shard_id,
                batch_id=batch_id,
                run_mode=resolved_mode,
                seed_base=execution_seed + batch_start,
                drop_self_loops=drop_self_loops,
                mock_targets=mock_targets,
                mock_event_types=mock_event_types,
                mock_fail_news_ids=mock_fail_news_ids,
            )
            cache_manager.write_batch(
                shard_id=shard_id,
                batch_id=batch_id,
                raw_frame=result.raw_frame,
                parsed_frame=result.parsed_frame,
                failure_frame=result.failure_frame,
                overwrite=overwrite,
            )

            shard_state["last_completed_batch_index"] = batch_id
            shard_state["processed_rows"] = int(shard_state.get("processed_rows", 0)) + int(result.stats["input_rows"])
            shard_state["success_rows"] = int(shard_state.get("success_rows", 0)) + int(result.stats["success_rows"])
            shard_state["failure_rows"] = int(shard_state.get("failure_rows", 0)) + int(result.stats["failure_rows"])
            shard_state["resolved_edge_count"] = int(shard_state.get("resolved_edge_count", 0)) + int(
                result.stats["resolved_edge_count"]
            )
            shard_state["updated_at_utc"] = _utc_now()
            if batch_id + 1 == total_batches or (
                batch_limit_per_shard is not None and batch_id + 1 == batch_limit_per_shard
            ):
                shard_state["status"] = "completed"
            _recompute_totals(checkpoint)
            checkpoint["updated_at_utc"] = _utc_now()
            _atomic_write_json(checkpoint_path, checkpoint)

    parsed_stats = cache_manager.consolidate_kind("parsed", output_paths["extractions_parquet"])
    failure_stats = cache_manager.consolidate_kind("failure", output_paths["failures_parquet"])
    edges_frame = cache_manager.build_edges(output_paths["extractions_parquet"])
    ensure_parent_dir(resolve_from_root(output_paths["edges_parquet"]))
    edges_frame.to_parquet(resolve_from_root(output_paths["edges_parquet"]), index=False)

    validation_note = (
        "Only local mock extraction was executed. Real vLLM inference on the server remains pending."
        if resolved_mode == "mock"
        else "Real vLLM extraction executed through the configured server endpoint."
    )
    summary = build_stage5_summary(
        config_path=config_path,
        run_mode=resolved_mode,
        checkpoint=checkpoint,
        preflight=preflight,
        stage4_gate=stage4_gate,
        output_paths=output_paths,
        cache_paths=cache_paths,
        validation_note=validation_note,
    )
    write_stage5_summary(summary, json_path=output_paths["summary_json"], markdown_path=output_paths["summary_markdown"])
    summary = build_stage5_summary(
        config_path=config_path,
        run_mode=resolved_mode,
        checkpoint=checkpoint,
        preflight=preflight,
        stage4_gate=stage4_gate,
        output_paths=output_paths,
        cache_paths=cache_paths,
        validation_note=validation_note,
    )
    write_stage5_summary(summary, json_path=output_paths["summary_json"], markdown_path=output_paths["summary_markdown"])
    return {
        "mode": resolved_mode,
        "checkpoint_json": output_paths["checkpoint_json"],
        "summary_json": output_paths["summary_json"],
        "summary_markdown": output_paths["summary_markdown"],
        "extractions_parquet": output_paths["extractions_parquet"],
        "edges_parquet": output_paths["edges_parquet"],
        "failures_parquet": output_paths["failures_parquet"],
        "parsed_cache_files": parsed_stats["file_count"],
        "parsed_rows": parsed_stats["row_count"],
        "failure_rows": failure_stats["row_count"],
        "edge_rows": int(len(edges_frame)),
        "schema_compatibility": schema_result,
    }


def main() -> int:
    args = parse_args()
    seed = None if args.seed < 0 else args.seed
    result = run_stage5_full_extract(
        config_path=args.config,
        mode=args.mode or None,
        seed=seed,
        overwrite=args.overwrite,
        small_smoke=args.small_smoke,
        max_shards=None if args.max_shards < 0 else args.max_shards,
        max_batches_per_shard=None if args.max_batches_per_shard < 0 else args.max_batches_per_shard,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

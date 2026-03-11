"""Stage 4 feasibility runner for local mock and server execution."""

from __future__ import annotations

import argparse
import importlib.util
import random
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.analysis.stage4_report import build_stage4_report, write_stage4_report
from src.common.config import load_yaml, resolve_from_root
from src.common.io import ensure_parent_dir
from src.llm.extract_event_spillover import MockExtractionClient, extract_event_spillover
from src.llm.schema_compat import assert_vllm_schema_compatible, load_stage4_schema, validate_vllm_schema
from src.llm.vllm_client import VLLMClient, VLLMClientConfig


DEFAULT_STAGE4_CONFIG = "configs/llm/stage4_feasibility_2016.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_STAGE4_CONFIG)
    parser.add_argument("--mode", default="")
    parser.add_argument("--seed", type=int, default=-1)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


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
    """保持 vLLM served model id 一致。

    vLLM `serve` 时会把传入的 model tag 原样作为 `/v1/models` 的 id（例如 `models/Qwen2.5-...`）。
    因此这里不应把 `models/...` 转为绝对路径，否则 preflight 的 model_resolved 以及后续请求都会对不上。
    """
    return str(model_name).strip()


def _build_vllm_client(config: dict[str, Any], runtime_payload: dict[str, Any]) -> VLLMClient:
    execution = config["stage4"]["execution"]
    runtime = runtime_payload["runtime"]
    model_name = _resolve_model_name(str(runtime["model_name"]))
    client_config = VLLMClientConfig(
        base_url=str(execution["base_url"]),
        api_key=str(execution["api_key"]),
        model_name=model_name,
        timeout_seconds=float(execution["timeout_seconds"]),
        temperature=float(runtime["temperature"]),
        top_p=float(runtime["top_p"]),
        max_output_tokens=int(runtime["max_output_tokens"]),
        request_mode=str(execution["request_mode"]),
    )
    return VLLMClient(client_config)


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
    allow_live_minimal_call = bool(config["stage4"]["execution"]["allow_live_minimal_call"])
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


def prepare_official_sample(config: dict[str, Any]) -> dict[str, Any]:
    """Stream 2015Q3-2016 mapped news and reservoir-sample the 2016 official subset."""
    sampling = config["stage4"]["sampling"]
    news_path = resolve_from_root(config["stage4"]["inputs"]["news_source_mapped"])
    parquet_file = pq.ParquetFile(news_path)

    randomizer = random.Random(int(sampling["seed"]))
    reservoir: list[dict[str, Any]] = []
    rows_seen = 0
    warmup_count = 0
    official_count = 0
    official_month_counter: Counter[str] = Counter()

    columns = [
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

    for batch in parquet_file.iter_batches(batch_size=int(sampling["batch_size"]), columns=columns):
        frame = batch.to_pandas()
        if frame.empty:
            continue
        frame["published_date"] = frame["published_date"].fillna("").astype(str)
        frame["source_ticker"] = frame["source_ticker"].fillna("").astype(str)
        frame = frame[frame["is_mapped"].eq(True) & frame["source_ticker"].ne("")]
        frame = frame[frame["published_date"].between(str(sampling["warmup_start"]), str(sampling["official_end"]))]
        if frame.empty:
            continue

        warmup_rows = frame[frame["published_date"].between(str(sampling["warmup_start"]), str(sampling["warmup_end"]))]
        warmup_count += int(len(warmup_rows))

        official_rows = frame[frame["published_date"].between(str(sampling["official_start"]), str(sampling["official_end"]))]
        if official_rows.empty:
            continue

        official_count += int(len(official_rows))
        official_month_counter.update(official_rows["published_date"].str.slice(0, 7))

        for row in official_rows.to_dict(orient="records"):
            rows_seen += 1
            if len(reservoir) < int(sampling["sample_size"]):
                reservoir.append(row)
                continue
            swap_index = randomizer.randint(1, rows_seen)
            if swap_index <= int(sampling["sample_size"]):
                reservoir[swap_index - 1] = row

    reservoir = sorted(reservoir, key=lambda row: (row["published_date"], row["news_id"]))
    sample_frame = pd.DataFrame(reservoir)
    return {
        "official_sample_frame": sample_frame,
        "warmup_candidate_count": warmup_count,
        "official_candidate_count": official_count,
        "official_sample_size": int(len(sample_frame)),
        "official_sample_requested": int(sampling["sample_size"]),
        "official_months_present": sorted(official_month_counter),
        "official_monthly_counts": dict(sorted(official_month_counter.items())),
        "research_universe": "SP500",
        "sampling_population_note": "Only rows already mapped into the SP500 research universe are sampled.",
    }


def _compute_formal_metrics(results_frame: pd.DataFrame, sample_summary: dict[str, Any]) -> dict[str, Any]:
    if results_frame.empty:
        return {
            "source_mapping_ratio": 0.0,
            "valid_target_ratio": 0.0,
            "avg_valid_target_count": 0.0,
            "monthly_avg_out_degree": 0.0,
            "news_density_2016": 0.0,
            "active_source_nodes": 0,
            "coverage_conclusion": "insufficient",
        }

    monthly_unique_targets = (
        results_frame.assign(month=results_frame["published_date"].astype(str).str.slice(0, 7))
        .explode("extracted_targets")
        .dropna(subset=["extracted_targets"])
        .groupby(["month", "source_ticker"])["extracted_targets"]
        .nunique()
    )

    return {
        "source_mapping_ratio": round(float(results_frame["is_valid_source"].mean()), 6),
        "valid_target_ratio": round(float(results_frame["has_valid_targets"].mean()), 6),
        "avg_valid_target_count": round(float(results_frame["valid_target_count"].mean()), 6),
        "monthly_avg_out_degree": round(float(monthly_unique_targets.mean()), 6) if not monthly_unique_targets.empty else 0.0,
        "news_density_2016": round(sample_summary["official_candidate_count"] / 12.0, 6),
        "active_source_nodes": int(results_frame.loc[results_frame["has_valid_targets"], "source_ticker"].nunique()),
        "coverage_conclusion": "sufficient" if results_frame["has_valid_targets"].any() else "insufficient",
    }


def _write_dataframe(frame: pd.DataFrame, output_path: str, overwrite: bool) -> str:
    target = ensure_parent_dir(output_path)
    if target.exists() and not overwrite:
        raise FileExistsError(f"Output already exists, rerun with --overwrite: {target}")
    frame.to_parquet(target, index=False)
    return str(target)


def run_stage4_feasibility(
    config_path: str = DEFAULT_STAGE4_CONFIG,
    mode: str | None = None,
    seed: int | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Execute the local-safe Stage 4 feasibility flow."""
    config = load_yaml(config_path)
    runtime_payload = load_yaml(config["stage4"]["runtime_config"])
    schema = load_stage4_schema(config["stage4"]["schema_path"])
    schema_result_obj = validate_vllm_schema(schema)
    schema_result = {
        "is_compatible": schema_result_obj.is_compatible,
        "errors": schema_result_obj.errors,
        "warnings": schema_result_obj.warnings,
    }
    assert_vllm_schema_compatible(schema)

    if seed is not None and seed >= 0:
        config["stage4"]["sampling"]["seed"] = int(seed)

    client = _build_vllm_client(config=config, runtime_payload=runtime_payload)
    preflight = run_preflight(config=config, runtime_payload=runtime_payload, client=client, schema=schema)
    sample_summary = prepare_official_sample(config)

    outputs = config["stage4"]["outputs"]
    official_sample_path = _write_dataframe(
        sample_summary["official_sample_frame"],
        outputs["official_sample_parquet"],
        overwrite=overwrite,
    )

    resolved_mode = mode or config["stage4"]["execution"]["mode"]
    execution_summary: dict[str, Any] = {
        "mode": resolved_mode,
        "local_mock_executed": False,
        "formal_run_complete": False,
        "results_path": "",
        "processed_rows": 0,
        "formal_metrics": None,
        "execution_note": "",
    }

    if resolved_mode == "mock":
        mock_client = MockExtractionClient()
        local_mock_size = int(config["stage4"]["execution"]["local_mock_size"])
        records = sample_summary["official_sample_frame"].head(local_mock_size).to_dict(orient="records")
        results = [
            extract_event_spillover(
                news_row=row,
                schema=schema,
                client=mock_client,
                seed=int(config["stage4"]["sampling"]["seed"]) + index,
            )
            for index, row in enumerate(records)
        ]
        result_frame = pd.DataFrame(results)
        results_path = _write_dataframe(result_frame, outputs["local_mock_results_parquet"], overwrite=overwrite)
        execution_summary.update(
            {
                "local_mock_executed": True,
                "results_path": results_path,
                "processed_rows": len(result_frame),
                "execution_note": (
                    "The official 100-row 2016 sample has been prepared, but only a small local mock subset "
                    "was processed. Real 1-row smoke plus 100-row formal extraction remains pending on the server."
                ),
            }
        )
    elif resolved_mode == "real":
        if not preflight["ready_for_real_inference"]:
            raise RuntimeError("Local environment preflight failed. Refusing to run real Stage 4 inference.")
        smoke_size = int(config["stage4"]["sampling"]["smoke_size"])
        smoke_records = sample_summary["official_sample_frame"].head(smoke_size).to_dict(orient="records")
        smoke_results = [
            extract_event_spillover(
                news_row=row,
                schema=schema,
                client=client,
                seed=int(config["stage4"]["sampling"]["seed"]) + index,
            )
            for index, row in enumerate(smoke_records)
        ]
        smoke_frame = pd.DataFrame(smoke_results)
        _write_dataframe(smoke_frame, outputs["smoke_results_parquet"], overwrite=overwrite)

        records = sample_summary["official_sample_frame"].head(int(config["stage4"]["sampling"]["sample_size"])).to_dict(
            orient="records"
        )
        formal_results = [
            extract_event_spillover(
                news_row=row,
                schema=schema,
                client=client,
                seed=int(config["stage4"]["sampling"]["seed"]) + smoke_size + index,
            )
            for index, row in enumerate(records)
        ]
        result_frame = pd.DataFrame(formal_results)
        results_path = _write_dataframe(result_frame, outputs["formal_results_parquet"], overwrite=overwrite)
        execution_summary.update(
            {
                "local_mock_executed": False,
                "formal_run_complete": len(smoke_frame) == len(smoke_records) and len(result_frame) == len(records) and len(records) > 0,
                "results_path": results_path,
                "processed_rows": len(result_frame),
                "formal_metrics": _compute_formal_metrics(result_frame, sample_summary),
                "smoke_results_path": outputs["smoke_results_parquet"],
                "execution_note": "Real server-side smoke and formal extraction completed.",
            }
        )
    else:
        raise ValueError(f"Unsupported Stage 4 mode: {resolved_mode}")

    sample_summary = {key: value for key, value in sample_summary.items() if key != "official_sample_frame"}
    sample_summary["official_sample_path"] = official_sample_path

    report = build_stage4_report(
        config_path=config_path,
        run_mode=resolved_mode,
        schema_result=schema_result,
        preflight=preflight,
        sample_summary=sample_summary,
        execution_summary=execution_summary,
    )
    report_paths = write_stage4_report(
        report=report,
        json_path=outputs["report_json"],
        markdown_path=outputs["report_markdown"],
    )
    return {
        "mode": resolved_mode,
        "report_json": report_paths["json_path"],
        "report_markdown": report_paths["markdown_path"],
        "official_sample_path": official_sample_path,
        "results_path": execution_summary["results_path"],
        "ready_for_real_inference": preflight["ready_for_real_inference"],
    }


def main() -> int:
    args = parse_args()
    seed = None if args.seed < 0 else args.seed
    result = run_stage4_feasibility(
        config_path=args.config,
        mode=args.mode or None,
        seed=seed,
        overwrite=args.overwrite,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

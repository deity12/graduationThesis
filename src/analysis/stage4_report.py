"""Stage 4 feasibility report generation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.io import ensure_parent_dir
from src.common.config import load_yaml, resolve_from_root


def _json_default(value: Any) -> Any:
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def compute_can_enter_stage5(formal_metrics: dict[str, Any] | None) -> bool:
    """Stage-5 admission gate derived from Stage-4 formal metrics.

    Project thresholds (fixed by spec):
    - source mapping ratio >= 0.80
    - valid target ratio >= 0.25
    - monthly avg out-degree >= 1.0
    - coverage conclusion == 'sufficient'
    """
    if not formal_metrics:
        return False
    try:
        source_mapping_ratio = float(formal_metrics.get("source_mapping_ratio"))
        valid_target_ratio = float(formal_metrics.get("valid_target_ratio"))
        monthly_avg_out_degree = float(formal_metrics.get("monthly_avg_out_degree"))
        coverage_conclusion = str(formal_metrics.get("coverage_conclusion", "")).strip().lower()
    except (TypeError, ValueError):
        return False

    return (
        source_mapping_ratio >= 0.80
        and valid_target_ratio >= 0.25
        and monthly_avg_out_degree >= 1.0
        and coverage_conclusion == "sufficient"
    )


def recompute_stage_gate(report: dict[str, Any]) -> dict[str, Any]:
    """Recompute stage gate fields in-place from existing report payload."""
    stage_gate = dict(report.get("stage_gate", {}))
    formal_complete = bool(stage_gate.get("stage4_formally_complete", report.get("execution_summary", {}).get("formal_run_complete", False)))
    stage_gate["stage4_formally_complete"] = formal_complete
    stage_gate["can_enter_stage5"] = bool(formal_complete) and compute_can_enter_stage5(report.get("formal_metrics"))
    report["stage_gate"] = stage_gate
    return report


def _format_utc_mtime(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


def build_file_manifest(path: str) -> dict[str, Any]:
    """Lightweight, reproducible file manifest for audit and lineage."""
    candidate = Path(resolve_from_root(path) if not os.path.isabs(path) else path)
    if not candidate.exists():
        return {
            "path": str(path),
            "resolved_path": str(candidate),
            "available": False,
            "unavailable_reason": "path_not_found",
        }

    stat = candidate.stat()
    manifest: dict[str, Any] = {
        "path": str(path),
        "resolved_path": str(candidate),
        "available": True,
        "size_bytes": int(stat.st_size),
        "mtime_utc": _format_utc_mtime(stat.st_mtime),
    }

    # Optional parquet metadata (kept lightweight).
    if candidate.suffix == ".parquet":
        try:
            import pyarrow.parquet as pq  # local import to avoid hard dependency at import time

            pf = pq.ParquetFile(candidate)
            num_rows = int(pf.metadata.num_rows) if pf.metadata is not None else None
            num_row_groups = int(pf.metadata.num_row_groups) if pf.metadata is not None else None
            schema_names = pf.schema_arrow.names
            manifest.update(
                {
                    "parquet_num_rows": num_rows,
                    "parquet_num_row_groups": num_row_groups,
                    "parquet_num_columns": int(len(schema_names)),
                    "parquet_columns_head": schema_names[:25],
                }
            )
        except Exception as exc:  # pragma: no cover (best-effort metadata)
            manifest["parquet_metadata_error"] = f"{type(exc).__name__}: {exc}"

    # Minimal fingerprint: stable hash of key manifest fields.
    fingerprint_source = json.dumps(
        {
            "resolved_path": manifest["resolved_path"],
            "size_bytes": manifest.get("size_bytes"),
            "mtime_utc": manifest.get("mtime_utc"),
            "parquet_num_rows": manifest.get("parquet_num_rows"),
            "parquet_num_row_groups": manifest.get("parquet_num_row_groups"),
        },
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    manifest["fingerprint_sha256"] = hashlib.sha256(fingerprint_source).hexdigest()
    return manifest


def build_stage4_sample_lineage(
    *,
    stage4_config_path: str,
    official_sample_parquet: str,
    direct_input_parquet: str,
    upstream_news_normalized_parquet: str,
) -> dict[str, Any]:
    """Build Stage-4 sample lineage and input metadata without rerunning extraction."""
    stage4_config = load_yaml(stage4_config_path)["stage4"]
    sampling = stage4_config["sampling"]

    lineage: dict[str, Any] = {
        "sampling_window": {
            "warmup_start": str(sampling["warmup_start"]),
            "warmup_end": str(sampling["warmup_end"]),
            "official_start": str(sampling["official_start"]),
            "official_end": str(sampling["official_end"]),
        },
        "sampling_seed": int(sampling["seed"]) if "seed" in sampling else None,
        "paths": {
            "official_sample_parquet": str(official_sample_parquet),
            "direct_input_news_source_mapped_parquet": str(direct_input_parquet),
            "upstream_news_normalized_parquet": str(upstream_news_normalized_parquet),
        },
        "manifests": {
            "official_sample_parquet": build_file_manifest(str(official_sample_parquet)),
            "direct_input_news_source_mapped_parquet": build_file_manifest(str(direct_input_parquet)),
            "upstream_news_normalized_parquet": build_file_manifest(str(upstream_news_normalized_parquet)),
        },
        "lineage_statement": {
            "direct_input": "data/processed/news_source_mapped.parquet",
            "upstream_main_table": "data/interim/news_normalized.parquet",
            "standard_wording": (
                "Stage4 official sample is directly sampled from data/processed/news_source_mapped.parquet. "
                "The sampled news_id are 100% traceable in data/interim/news_normalized.parquet with key fields consistent, "
                "and source_file is restricted to the official dual-source corpus."
            ),
        },
        "traceability_check": {
            "available": False,
            "unavailable_reason": "not_checked",
        },
    }

    # Best-effort traceability validation on the 100-row sample.
    try:
        import pandas as pd
        import pyarrow.dataset as ds

        sample_path = resolve_from_root(str(official_sample_parquet))
        official = pd.read_parquet(sample_path, columns=["news_id", "published_date", "title"])
        ids = official["news_id"].astype(str).tolist()

        norm_ds = ds.dataset(resolve_from_root(str(upstream_news_normalized_parquet)), format="parquet")
        norm = (
            norm_ds.to_table(
                columns=["news_id", "published_date", "title", "source_file"],
                filter=ds.field("news_id").isin(ids),
            )
            .to_pandas()
        )

        merged = official.merge(norm, on="news_id", how="left", suffixes=("_sample", "_normalized"))
        matched_ratio = float(merged["source_file"].notna().mean())
        published_date_equal_ratio = float(
            (merged["published_date_sample"].astype(str) == merged["published_date_normalized"].astype(str)).mean()
        )
        title_equal_ratio = float((merged["title_sample"].astype(str) == merged["title_normalized"].astype(str)).mean())
        source_file_unique = sorted(set(norm["source_file"].dropna().astype(str)))
        lineage["traceability_check"] = {
            "available": True,
            "matched_ratio_in_news_normalized": round(matched_ratio, 6),
            "published_date_equal_ratio": round(published_date_equal_ratio, 6),
            "title_equal_ratio": round(title_equal_ratio, 6),
            "source_file_unique": source_file_unique,
            "source_file_dual_source_only": set(source_file_unique).issubset({"All_external.csv", "nasdaq_exteral_data.csv"}),
        }
    except Exception as exc:  # pragma: no cover (best-effort audit)
        lineage["traceability_check"] = {
            "available": False,
            "unavailable_reason": f"{type(exc).__name__}: {exc}",
        }

    return lineage


def build_stage4_report(
    config_path: str,
    run_mode: str,
    schema_result: dict[str, Any],
    preflight: dict[str, Any],
    sample_summary: dict[str, Any],
    execution_summary: dict[str, Any],
) -> dict[str, Any]:
    """Build the serialized Stage 4 report payload."""
    formal_run_complete = bool(execution_summary.get("formal_run_complete", False))
    can_start_server_validation = bool(schema_result["is_compatible"]) and sample_summary.get("official_sample_size", 0) > 0

    formal_metrics = execution_summary.get("formal_metrics")
    if not formal_run_complete:
        formal_metrics = {
            "source_mapping_ratio": None,
            "valid_target_ratio": None,
            "avg_valid_target_count": None,
            "monthly_avg_out_degree": None,
            "news_density_2016": None,
            "active_source_nodes": None,
            "coverage_conclusion": "pending_server_validation",
        }

    decision_order = [
        "若 2016 覆盖足够，按 2016-2023 / 4 split 继续。",
        "若 2016 覆盖不足，先尝试 semantic_window_days: 63 -> 126，或改为季度更新。",
        "若扩窗后 2016 仍不足但 2017 足够，则降级到 2017-2023 / 3 split。",
        "只有上述三级都不满足，才建议暂停项目主线。",
    ]

    can_enter_stage5 = bool(formal_run_complete) and compute_can_enter_stage5(formal_metrics)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": config_path,
        "run_mode": run_mode,
        "config_roles": {
            "runtime_config": "configs/llm/stage4_feasibility_2016.yaml drives the runner and sampling behavior.",
            "experiment_config": "configs/experiment/stage4_feasibility.yaml is metadata only.",
        },
        "data_scope": {
            "research_universe": "SP500",
            "news_source_policy": "Only news rows mapped into the SP500 research universe are eligible for Stage 4 sampling.",
            "warmup_policy": "2015-09-01 to 2015-12-31 may initialize windows but never enter the official 100-row denominator.",
            "official_window": "2016-01-01 to 2016-12-31",
        },
        "schema_compatibility": schema_result,
        "preflight": preflight,
        "sample_summary": sample_summary,
        "execution_summary": execution_summary,
        "formal_metrics": formal_metrics,
        "decision_order": decision_order,
        "stage_gate": {
            "can_start_server_stage4_validation": can_start_server_validation,
            "stage4_formally_complete": formal_run_complete,
            "can_enter_stage5": can_enter_stage5,
        },
    }


def render_stage4_report_markdown(report: dict[str, Any]) -> str:
    """Render a human-readable Stage 4 markdown report."""
    preflight = report["preflight"]
    sample_summary = report["sample_summary"]
    execution = report["execution_summary"]
    metrics = report["formal_metrics"]
    stage_gate = report["stage_gate"]
    lineage = report.get("sample_lineage", {})

    lines = [
        "# Stage 4 Feasibility Report",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Run mode: {report['run_mode']}",
        f"- Config: `{report['config_path']}`",
        "",
        "## Preflight",
        "",
        f"- NVIDIA GPU available: {preflight['gpu_available']}",
        f"- nvidia-smi available: {preflight['nvidia_smi_available']}",
        f"- vLLM import available: {preflight['vllm_import_available']}",
        f"- Model identifier parseable: {preflight['model_identifier_parseable']}",
        f"- Server reachable: {preflight['server_reachable']}",
        f"- Hosted model resolved: {preflight['model_resolved']}",
        f"- Structured-output minimal call ready: {preflight['structured_output_ready']}",
        f"- Minimal call checked live: {preflight['minimal_call_checked']}",
        f"- Ready for real inference: {preflight['ready_for_real_inference']}",
        "",
        "## Sampling",
        "",
        "- Official sampled set means the fixed 100-row 2016 feasibility sample prepared for server validation.",
        "- Local mock processed set means only a small local subset used to verify code paths without real vLLM inference.",
        f"- Warm-up candidate count (2015-09-01 to 2015-12-31): {sample_summary['warmup_candidate_count']}",
        f"- Official candidate count (2016 only): {sample_summary['official_candidate_count']}",
        f"- Official sampled rows: {sample_summary['official_sample_size']}",
        f"- Local mock processed rows: {execution['processed_rows'] if execution['local_mock_executed'] else 0}",
        f"- Official sampled month coverage: {sample_summary['official_months_present']}",
        "",
        "## Sample Lineage (Audit)",
        "",
        f"- Official sample parquet: `{lineage.get('paths', {}).get('official_sample_parquet', '')}`",
        f"- Direct input parquet: `{lineage.get('paths', {}).get('direct_input_news_source_mapped_parquet', '')}`",
        f"- Upstream main table parquet: `{lineage.get('paths', {}).get('upstream_news_normalized_parquet', '')}`",
        f"- Sampling window: {lineage.get('sampling_window', {})}",
        f"- Sampling seed: {lineage.get('sampling_seed', None)}",
        f"- Lineage statement: {lineage.get('lineage_statement', {}).get('standard_wording', '')}",
        "",
        "### Traceability Check",
        "",
        f"- Available: {lineage.get('traceability_check', {}).get('available', False)}",
        f"- Matched ratio in upstream table: {lineage.get('traceability_check', {}).get('matched_ratio_in_news_normalized', None)}",
        f"- published_date equal ratio: {lineage.get('traceability_check', {}).get('published_date_equal_ratio', None)}",
        f"- title equal ratio: {lineage.get('traceability_check', {}).get('title_equal_ratio', None)}",
        f"- source_file unique: {lineage.get('traceability_check', {}).get('source_file_unique', None)}",
        f"- source_file dual-source only: {lineage.get('traceability_check', {}).get('source_file_dual_source_only', None)}",
        "",
        "## Execution",
        "",
        f"- Local mock executed: {execution['local_mock_executed']}",
        f"- Formal run complete: {execution['formal_run_complete']}",
        f"- Results path: {execution['results_path']}",
        f"- Smoke results path: {execution.get('smoke_results_path', '')}",
        f"- Execution note: {execution.get('execution_note', '')}",
        "",
        "## Formal Metrics",
        "",
        f"- Source mapping ratio: {metrics['source_mapping_ratio']}",
        f"- Valid target ratio: {metrics['valid_target_ratio']}",
        f"- Average valid target count: {metrics['avg_valid_target_count']}",
        f"- Monthly average out-degree: {metrics['monthly_avg_out_degree']}",
        f"- 2016 news density: {metrics['news_density_2016']}",
        f"- Active source nodes: {metrics['active_source_nodes']}",
        f"- Coverage conclusion: {metrics['coverage_conclusion']}",
        "",
        "## Decision Order",
        "",
    ]
    lines.extend([f"- {item}" for item in report["decision_order"]])
    lines.extend(
        [
            "",
            "## Stage Gate",
            "",
            f"- Can start server Stage 4 validation: {stage_gate['can_start_server_stage4_validation']}",
            f"- Stage 4 formally complete: {stage_gate['stage4_formally_complete']}",
            f"- Can enter Stage 5: {stage_gate['can_enter_stage5']}",
            "",
            "## Universe Scope",
            "",
            f"- Research universe: {report['data_scope']['research_universe']}",
            f"- News source policy: {report['data_scope']['news_source_policy']}",
        ]
    )
    return "\n".join(lines) + "\n"


def write_stage4_report(report: dict[str, Any], json_path: str, markdown_path: str) -> dict[str, str]:
    """Persist the Stage 4 report as JSON and Markdown."""
    json_target = ensure_parent_dir(json_path)
    markdown_target = ensure_parent_dir(markdown_path)
    json_target.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=_json_default), encoding="utf-8")
    markdown_target.write_text(render_stage4_report_markdown(report), encoding="utf-8")
    return {"json_path": str(json_target), "markdown_path": str(markdown_target)}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 4 report utilities (gate recompute).")
    parser.add_argument("--input", default="outputs/stage4/stage4_report.json")
    parser.add_argument("--stage4-config", default="")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--output-md", default="")
    parser.add_argument("--rewrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    input_path = args.input
    payload = json.loads(ensure_parent_dir(input_path).read_text(encoding="utf-8"))
    updated = recompute_stage_gate(payload)

    if args.rewrite:
        stage4_config_path = args.stage4_config or updated.get("config_path") or "configs/llm/stage4_feasibility_2016.yaml"
        stage4_cfg = load_yaml(stage4_config_path)["stage4"]
        official_sample_path = (
            updated.get("sample_summary", {}).get("official_sample_path")
            or stage4_cfg.get("outputs", {}).get("official_sample_parquet")
            or "outputs/stage4/official_sample_2016.parquet"
        )
        direct_input_path = stage4_cfg.get("inputs", {}).get("news_source_mapped", "data/processed/news_source_mapped.parquet")
        upstream_path = "data/interim/news_normalized.parquet"
        updated["sample_lineage"] = build_stage4_sample_lineage(
            stage4_config_path=stage4_config_path,
            official_sample_parquet=str(official_sample_path),
            direct_input_parquet=str(direct_input_path),
            upstream_news_normalized_parquet=str(upstream_path),
        )
        updated["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
        json_path = args.output_json or input_path
        md_path = args.output_md or input_path.replace(".json", ".md")
        write_stage4_report(updated, json_path=json_path, markdown_path=md_path)
        print({"json": json_path, "md": md_path})
        return 0

    print(updated.get("stage_gate", {}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

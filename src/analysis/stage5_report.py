"""Stage 5 summary report generation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.analysis.stage4_report import build_file_manifest
from src.common.io import ensure_parent_dir


def build_stage5_summary(
    *,
    config_path: str,
    run_mode: str,
    checkpoint: dict[str, Any],
    preflight: dict[str, Any] | None,
    stage4_gate: dict[str, Any],
    output_paths: dict[str, str],
    cache_paths: dict[str, str],
    validation_note: str,
) -> dict[str, Any]:
    """Build the serialized Stage 5 summary payload."""
    manifests = {
        "checkpoint_json": build_file_manifest(output_paths["checkpoint_json"]),
        "summary_json": build_file_manifest(output_paths["summary_json"]),
        "extractions_parquet": build_file_manifest(output_paths["extractions_parquet"]),
        "edges_parquet": build_file_manifest(output_paths["edges_parquet"]),
        "failures_parquet": build_file_manifest(output_paths["failures_parquet"]),
        "raw_cache_dir": {"path": cache_paths["raw_dir"]},
        "parsed_cache_dir": {"path": cache_paths["parsed_dir"]},
        "failure_cache_dir": {"path": cache_paths["failure_dir"]},
    }
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": config_path,
        "run_mode": run_mode,
        "stage4_gate": stage4_gate,
        "preflight": preflight,
        "checkpoint": checkpoint,
        "output_paths": output_paths,
        "cache_paths": cache_paths,
        "manifests": manifests,
        "validation_note": validation_note,
    }


def render_stage5_summary_markdown(summary: dict[str, Any]) -> str:
    """Render the Stage 5 summary report in Markdown."""
    checkpoint = summary["checkpoint"]
    totals = checkpoint.get("totals", {})
    lines = [
        "# Stage 5 Full Extraction Summary",
        "",
        f"- Generated at (UTC): {summary['generated_at_utc']}",
        f"- Config: `{summary['config_path']}`",
        f"- Run mode: {summary['run_mode']}",
        "",
        "## Stage Gate",
        "",
        f"- Stage 4 formally complete: {summary['stage4_gate'].get('stage4_formally_complete')}",
        f"- Can enter Stage 5: {summary['stage4_gate'].get('can_enter_stage5')}",
        "",
        "## Totals",
        "",
        f"- Planned shards: {totals.get('planned_shards', 0)}",
        f"- Completed shards: {totals.get('completed_shards', 0)}",
        f"- Processed rows: {totals.get('processed_rows', 0)}",
        f"- Success rows: {totals.get('success_rows', 0)}",
        f"- Failure rows: {totals.get('failure_rows', 0)}",
        f"- Resolved edge count: {totals.get('resolved_edge_count', 0)}",
        "",
        "## Outputs",
        "",
        f"- Checkpoint: `{summary['output_paths']['checkpoint_json']}`",
        f"- Extractions: `{summary['output_paths']['extractions_parquet']}`",
        f"- Edges: `{summary['output_paths']['edges_parquet']}`",
        f"- Failures: `{summary['output_paths']['failures_parquet']}`",
        "",
        "## Validation Boundary",
        "",
        f"- {summary['validation_note']}",
    ]
    return "\n".join(lines) + "\n"


def write_stage5_summary(summary: dict[str, Any], json_path: str, markdown_path: str) -> dict[str, str]:
    """Persist the Stage 5 summary as JSON and Markdown."""
    json_target = ensure_parent_dir(json_path)
    markdown_target = ensure_parent_dir(markdown_path)
    json_target.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    markdown_target.write_text(render_stage5_summary_markdown(summary), encoding="utf-8")
    return {"json_path": str(json_target), "markdown_path": str(markdown_target)}

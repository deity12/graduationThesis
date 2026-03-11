from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.llm.stage5_full_extract import run_stage5_full_extract
from src.mapping.target_resolver import TargetResolver, TargetResolverConfig


def test_target_resolver_resolves_exact_alias_and_ticker() -> None:
    resolver = TargetResolver(
        TargetResolverConfig(
            alias_seed_csv="data/raw/mapping/company_alias_seed.csv",
            company_alias_min_length=4,
            allow_exact_ticker=True,
        )
    )
    alias_result = resolver.resolve("Apple")
    ticker_result = resolver.resolve("MSFT")

    assert alias_result["is_resolved"] is True
    assert alias_result["resolved_ticker"] == "AAPL"
    assert alias_result["match_type"] == "exact_alias"
    assert ticker_result["is_resolved"] is True
    assert ticker_result["resolved_ticker"] == "MSFT"
    assert ticker_result["match_type"] == "exact_ticker"


def test_stage5_mock_runner_supports_checkpoint_resume_and_failure_capture(tmp_path: Path) -> None:
    news_frame = pd.DataFrame(
        [
            {
                "news_id": "warmup-1",
                "published_date": "2015-09-02",
                "title": "Warmup 1",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "AAPL",
                "source_company_name": "Apple Inc.",
                "is_mapped": True,
                "is_warmup": True,
                "in_evaluation_window": False,
            },
            {
                "news_id": "warmup-2",
                "published_date": "2015-09-21",
                "title": "Warmup 2",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "MSFT",
                "source_company_name": "Microsoft Corporation",
                "is_mapped": True,
                "is_warmup": True,
                "in_evaluation_window": False,
            },
            {
                "news_id": "eval-1",
                "published_date": "2016-01-05",
                "title": "Eval 1",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "NVDA",
                "source_company_name": "NVIDIA Corporation",
                "is_mapped": True,
                "is_warmup": False,
                "in_evaluation_window": True,
            },
            {
                "news_id": "eval-fail",
                "published_date": "2016-01-06",
                "title": "Eval fail",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "AMZN",
                "source_company_name": "Amazon.com, Inc.",
                "is_mapped": True,
                "is_warmup": False,
                "in_evaluation_window": True,
            },
        ]
    )
    news_path = tmp_path / "news_source_mapped.parquet"
    news_frame.to_parquet(news_path, index=False)

    stage4_report_path = tmp_path / "stage4_report.json"
    stage4_report_path.write_text(
        json.dumps({"stage_gate": {"stage4_formally_complete": True, "can_enter_stage5": True}}, ensure_ascii=False),
        encoding="utf-8",
    )

    config_path = tmp_path / "stage5.yaml"
    config_path.write_text(
        "\n".join(
            [
                "stage5:",
                "  inputs:",
                f"    stage4_report_json: \"{stage4_report_path.as_posix()}\"",
                "    require_stage4_gate: true",
                f"    news_source_mapped: \"{news_path.as_posix()}\"",
                "    alias_seed_csv: \"data/raw/mapping/company_alias_seed.csv\"",
                "  schema_path: \"configs/llm/spillover_schema_v2_vllm_compatible.json\"",
                "  runtime_config: \"configs/llm/runtime_qwen25_32b_awq.yaml\"",
                "  filters:",
                "    start_date: \"2015-09-01\"",
                "    warmup_end: \"2015-12-31\"",
                "    evaluation_start: \"2016-01-01\"",
                "    end_date: \"2016-01-31\"",
                "    require_mapped_source: true",
                "    require_source_ticker: true",
                "  execution:",
                "    mode: \"mock\"",
                "    seed: 7",
                "    batch_size: 2",
                "    base_url: \"http://127.0.0.1:8000\"",
                "    api_key: \"-\"",
                "    timeout_seconds: 30",
                "    request_mode: \"structured_outputs\"",
                "    allow_live_minimal_call: false",
                "  target_resolution:",
                "    company_alias_min_length: 4",
                "    allow_exact_ticker: true",
                "    drop_self_loops: true",
                "  smoke:",
                "    shard_limit: 1",
                "    batch_limit_per_shard: 1",
                "    row_limit_per_shard: 8",
                "  mock:",
                "    targets:",
                "      - \"Apple\"",
                "      - \"Microsoft\"",
                "      - \"NVIDIA\"",
                "    event_types:",
                "      - \"partnership\"",
                "      - \"other\"",
                "    fail_news_ids:",
                "      - \"eval-fail\"",
                "  cache:",
                f"    raw_dir: \"{(tmp_path / 'cache' / 'raw').as_posix()}\"",
                f"    parsed_dir: \"{(tmp_path / 'cache' / 'parsed').as_posix()}\"",
                f"    failure_dir: \"{(tmp_path / 'cache' / 'failure').as_posix()}\"",
                "  outputs:",
                f"    checkpoint_json: \"{(tmp_path / 'outputs' / 'stage5_checkpoint.json').as_posix()}\"",
                f"    failures_parquet: \"{(tmp_path / 'outputs' / 'stage5_failures.parquet').as_posix()}\"",
                f"    summary_json: \"{(tmp_path / 'outputs' / 'stage5_summary.json').as_posix()}\"",
                f"    summary_markdown: \"{(tmp_path / 'outputs' / 'stage5_summary.md').as_posix()}\"",
                f"    extractions_parquet: \"{(tmp_path / 'outputs' / 'spillover_extractions.parquet').as_posix()}\"",
                f"    edges_parquet: \"{(tmp_path / 'outputs' / 'spillover_edges.parquet').as_posix()}\"",
            ]
        ),
        encoding="utf-8",
    )

    first_result = run_stage5_full_extract(config_path=str(config_path))
    second_result = run_stage5_full_extract(config_path=str(config_path))

    extractions = pd.read_parquet(first_result["extractions_parquet"])
    failures = pd.read_parquet(first_result["failures_parquet"])
    edges = pd.read_parquet(first_result["edges_parquet"])
    checkpoint = json.loads(Path(first_result["checkpoint_json"]).read_text(encoding="utf-8"))

    assert len(extractions) == 3
    assert len(failures) == 1
    assert len(edges) > 0
    assert set(checkpoint["shards"].keys()) == {"2015-09", "2015-10", "2015-11", "2015-12", "2016-01"}
    assert checkpoint["totals"]["processed_rows"] == 4
    assert checkpoint["totals"]["success_rows"] == 3
    assert checkpoint["totals"]["failure_rows"] == 1
    assert not (edges["source_ticker"] == edges["target_ticker"]).any()

    resumed_extractions = pd.read_parquet(second_result["extractions_parquet"])
    resumed_failures = pd.read_parquet(second_result["failures_parquet"])
    assert len(resumed_extractions) == len(extractions)
    assert len(resumed_failures) == len(failures)

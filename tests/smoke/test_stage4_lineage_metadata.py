from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analysis.stage4_report import build_stage4_sample_lineage


def test_stage4_lineage_builds_manifests_and_traceability(tmp_path: Path) -> None:
    # Build a minimal upstream normalized parquet.
    normalized = pd.DataFrame(
        [
            {"news_id": "n1", "published_date": "2016-01-01", "title": "t1", "source_file": "All_external.csv"},
            {"news_id": "n2", "published_date": "2016-01-02", "title": "t2", "source_file": "nasdaq_exteral_data.csv"},
        ]
    )
    normalized_path = tmp_path / "news_normalized.parquet"
    normalized.to_parquet(normalized_path, index=False)

    # Direct input parquet (news_source_mapped) can be a stub for this unit test.
    mapped_path = tmp_path / "news_source_mapped.parquet"
    normalized.assign(is_mapped=True, source_ticker="AAPL").to_parquet(mapped_path, index=False)

    # Official sample parquet with a subset of ids.
    official_sample = pd.DataFrame(
        [
            {"news_id": "n1", "published_date": "2016-01-01", "title": "t1"},
            {"news_id": "n2", "published_date": "2016-01-02", "title": "t2"},
        ]
    )
    official_path = tmp_path / "official_sample_2016.parquet"
    official_sample.to_parquet(official_path, index=False)

    # Minimal stage4 config for sampling params.
    cfg_path = tmp_path / "stage4.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                "stage4:",
                "  inputs:",
                f"    news_source_mapped: \"{mapped_path.as_posix()}\"",
                "  sampling:",
                "    warmup_start: \"2015-09-01\"",
                "    warmup_end: \"2015-12-31\"",
                "    official_start: \"2016-01-01\"",
                "    official_end: \"2016-12-31\"",
                "    seed: 7",
            ]
        ),
        encoding="utf-8",
    )

    lineage = build_stage4_sample_lineage(
        stage4_config_path=cfg_path.as_posix(),
        official_sample_parquet=official_path.as_posix(),
        direct_input_parquet=mapped_path.as_posix(),
        upstream_news_normalized_parquet=normalized_path.as_posix(),
    )

    assert lineage["sampling_seed"] == 7
    assert lineage["manifests"]["official_sample_parquet"]["available"] is True
    assert lineage["traceability_check"]["available"] is True
    assert lineage["traceability_check"]["matched_ratio_in_news_normalized"] == 1.0
    assert lineage["traceability_check"]["source_file_dual_source_only"] is True


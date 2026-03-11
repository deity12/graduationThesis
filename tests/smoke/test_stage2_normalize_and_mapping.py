from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.dates import load_protocol_window
from src.data.build_universe import build_universe_daily
from src.data.normalize_news import _collapse_chunk_candidates, normalize_news_dataset, standardize_news_chunk
from src.data.normalize_prices import normalize_prices_dataset
from src.mapping.source_mapper import map_news_sources


SAMPLE_TICKERS = {"AAPL", "A", "ABBV", "CPAY", "RVTY", "SPG", "SYF", "TDG", "WM", "WMB"}


def test_stage2_pipeline_smoke(tmp_path: Path) -> None:
    news_path = tmp_path / "news_normalized.parquet"
    prices_path = tmp_path / "prices_daily.parquet"
    universe_path = tmp_path / "universe_daily.parquet"
    mapped_news_path = tmp_path / "news_source_mapped.parquet"

    news_stats = normalize_news_dataset(
        output_path=news_path,
        chunk_size=500,
        max_rows_per_file=1000,
        overwrite=True,
    )
    assert news_stats["rows_written"] > 0
    news_frame = pd.read_parquet(news_path)
    assert {"news_id", "published_at", "ticker_raw", "title", "version", "source_priority"}.issubset(set(news_frame.columns))
    assert {"All_external.csv", "nasdaq_exteral_data.csv"}.issubset(set(news_frame["source_file"].unique()))
    assert pd.Timestamp(news_frame["published_at"].max()).year >= 2023

    prices_stats = normalize_prices_dataset(
        output_path=prices_path,
        tickers=SAMPLE_TICKERS,
        overwrite=True,
    )
    assert prices_stats["ticker_count"] == len(SAMPLE_TICKERS)
    prices_frame = pd.read_parquet(prices_path)
    assert {"date", "ticker", "instrument_type", "data_confidence", "is_warmup", "in_evaluation_window"}.issubset(
        set(prices_frame.columns)
    )
    assert set(SAMPLE_TICKERS).issubset(set(prices_frame.loc[prices_frame["instrument_type"] == "equity", "ticker"]))

    universe_stats = build_universe_daily(
        prices_path=prices_path,
        output_path=universe_path,
        overwrite=True,
    )
    assert universe_stats["ticker_count"] >= len(SAMPLE_TICKERS)
    universe_frame = pd.read_parquet(universe_path)
    assert {"date", "ticker", "has_price_data", "is_warmup", "in_evaluation_window", "version"}.issubset(
        set(universe_frame.columns)
    )

    mapping_stats = map_news_sources(
        news_path=news_path,
        output_path=mapped_news_path,
        batch_size=500,
        overwrite=True,
    )
    assert mapping_stats["rows_written"] == len(news_frame)
    mapped_news_frame = pd.read_parquet(mapped_news_path)
    assert {
        "source_ticker",
        "mapping_method",
        "mapping_confidence",
        "is_mapped",
        "in_evaluation_window",
    }.issubset(set(mapped_news_frame.columns))
    assert mapped_news_frame["is_mapped"].sum() > 0

    warmup_prices = prices_frame[(prices_frame["instrument_type"] == "equity") & (prices_frame["date"] < "2016-01-01")]
    assert not warmup_prices.empty
    assert warmup_prices["is_warmup"].all()
    assert not warmup_prices["in_evaluation_window"].any()

    warmup_universe = universe_frame[universe_frame["date"] < "2016-01-01"]
    assert not warmup_universe.empty
    assert warmup_universe["is_warmup"].all()
    assert not warmup_universe["in_evaluation_window"].any()


def test_news_standardization_priority_and_boundary_flags() -> None:
    protocol_window = load_protocol_window()
    news_config = {
        "raw_columns": {
            "published_at": "Date",
            "title": "Article_title",
            "ticker_raw": "Stock_symbol",
            "url": "Url",
            "publisher_raw": "Publisher",
            "author_raw": "Author",
            "body": "Article",
            "summary_lsa": "Lsa_summary",
            "summary_luhn": "Luhn_summary",
            "summary_textrank": "Textrank_summary",
            "summary_lexrank": "Lexrank_summary",
        }
    }
    raw_chunk = pd.DataFrame(
        [
            {
                "Date": "2015-09-01 10:00:00 UTC",
                "Article_title": "Duplicate row",
                "Stock_symbol": "A",
                "Url": "https://example.com/a",
                "Publisher": "Pub",
                "Author": "",
                "Article": "",
                "Lsa_summary": "",
                "Luhn_summary": "",
                "Textrank_summary": "",
                "Lexrank_summary": "",
                "source_file": "All_external.csv",
                "source_row_number": 2,
            },
            {
                "Date": "2015-09-01 10:00:00 UTC",
                "Article_title": "Duplicate row",
                "Stock_symbol": "A",
                "Url": "https://example.com/a",
                "Publisher": "",
                "Author": "",
                "Article": "Body wins",
                "Lsa_summary": "",
                "Luhn_summary": "",
                "Textrank_summary": "",
                "Lexrank_summary": "",
                "source_file": "nasdaq_exteral_data.csv",
                "source_row_number": 1,
            },
            {
                "Date": "2024-01-01 00:00:00 UTC",
                "Article_title": "Out of window",
                "Stock_symbol": "A",
                "Url": "https://example.com/b",
                "Publisher": "",
                "Author": "",
                "Article": "Future row",
                "Lsa_summary": "",
                "Luhn_summary": "",
                "Textrank_summary": "",
                "Lexrank_summary": "",
                "source_file": "nasdaq_exteral_data.csv",
                "source_row_number": 3,
            },
        ]
    )
    standardized = standardize_news_chunk(
        raw_chunk=raw_chunk,
        news_config=news_config,
        dataset_version="stage2_v1",
        protocol_window=protocol_window,
        source_priority_lookup={"All_external.csv": 2, "nasdaq_exteral_data.csv": 1},
        source_order_lookup={"All_external.csv": 0, "nasdaq_exteral_data.csv": 1},
    )
    collapsed = _collapse_chunk_candidates(standardized)

    assert len(standardized) == 3
    warmup_rows = standardized[standardized["published_date"] == "2015-09-01"]
    assert warmup_rows["is_warmup"].all()
    assert not warmup_rows["in_evaluation_window"].any()
    future_row = standardized[standardized["published_date"] == "2024-01-01"].iloc[0]
    assert bool(future_row["is_warmup"]) is False
    assert bool(future_row["in_evaluation_window"]) is False
    assert len(collapsed) == 2
    kept = collapsed.iloc[0]
    assert kept["body"] == "Body wins"
    assert kept["source_file"] == "nasdaq_exteral_data.csv"


def test_news_dual_source_synthetic_smoke(tmp_path: Path) -> None:
    fnspid_root = tmp_path / "fnspid"
    fnspid_root.mkdir()
    common_columns = [
        "Date",
        "Article_title",
        "Stock_symbol",
        "Url",
        "Publisher",
        "Author",
        "Article",
        "Lsa_summary",
        "Luhn_summary",
        "Textrank_summary",
        "Lexrank_summary",
    ]
    all_frame = pd.DataFrame(
        [
            ["2015-09-01 00:00:00 UTC", "Warmup", "A", "https://example.com/warm", "Pub", "", "", "", "", "", ""],
            ["2020-06-11 10:00:00 UTC", "Overlap", "A", "https://example.com/dup", "Pub", "", "", "", "", "", ""],
        ],
        columns=common_columns,
    )
    nasdaq_frame = pd.DataFrame(
        [
            [0, "2020-06-11 10:00:00 UTC", "Overlap", "A", "https://example.com/dup", "", "", "body", "", "", "", ""],
            [1, "2024-01-01 00:00:00 UTC", "Tail", "A", "https://example.com/tail", "", "", "tail body", "", "", "", ""],
        ],
        columns=["Unnamed: 0", *common_columns],
    )
    all_frame.to_csv(fnspid_root / "All_external.csv", index=False)
    nasdaq_frame.to_csv(fnspid_root / "nasdaq_exteral_data.csv", index=False)

    paths_config = tmp_path / "paths.yaml"
    paths_config.write_text(
        "\n".join(
            [
                "version: 1",
                "project_root: \".\"",
                "paths:",
                f"  raw_fnspid_root: \"{fnspid_root.as_posix()}\"",
                "  data_root: \"data\"",
                "  raw_data_root: \"data/raw\"",
                "  raw_prices_root: \"data/raw/prices\"",
                "  raw_prices_stocks_root: \"data/raw/prices/stocks\"",
                "  raw_prices_market_root: \"data/raw/prices/market\"",
                "  raw_universe_root: \"data/raw/universe\"",
                "  raw_mapping_root: \"data/raw/mapping\"",
                "  interim_data_root: \"data/interim\"",
                "  processed_data_root: \"data/processed\"",
                "  outputs_root: \"outputs\"",
                "  logs_root: \"logs\"",
            ]
        ),
        encoding="utf-8",
    )
    stage2_config = tmp_path / "stage2.yaml"
    stage2_config.write_text(
        "\n".join(
            [
                "stage2:",
                "  dataset_version: \"stage2_v1\"",
                "  protocol_config: \"configs/data/walk_forward_2016_2023.yaml\"",
                "  outputs:",
                "    news_normalized: \"data/interim/news_normalized.parquet\"",
                "    news_normalized_all_only_backup: \"data/interim/news_normalized_v1_all_only.parquet\"",
                "    prices_daily: \"data/processed/prices_daily.parquet\"",
                "    universe_daily: \"data/processed/universe_daily.parquet\"",
                "    news_source_mapped: \"data/processed/news_source_mapped.parquet\"",
                "  news:",
                "    chunk_size: 10",
                "    source_files:",
                "      - \"All_external.csv\"",
                "      - \"nasdaq_exteral_data.csv\"",
                "    source_priority:",
                "      All_external.csv: 2",
                "      nasdaq_exteral_data.csv: 1",
                "    raw_columns:",
                "      published_at: \"Date\"",
                "      title: \"Article_title\"",
                "      ticker_raw: \"Stock_symbol\"",
                "      url: \"Url\"",
                "      publisher_raw: \"Publisher\"",
                "      author_raw: \"Author\"",
                "      body: \"Article\"",
                "      summary_lsa: \"Lsa_summary\"",
                "      summary_luhn: \"Luhn_summary\"",
                "      summary_textrank: \"Textrank_summary\"",
                "      summary_lexrank: \"Lexrank_summary\"",
                "    optional_columns:",
                "      - \"Unnamed: 0\"",
            ]
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "synthetic_news.parquet"
    stats = normalize_news_dataset(
        paths_config_path=str(paths_config),
        stage2_config_path=str(stage2_config),
        output_path=output_path,
        overwrite=True,
    )
    assert stats["rows_written"] == 3
    frame = pd.read_parquet(output_path)
    assert {"All_external.csv", "nasdaq_exteral_data.csv"} == set(frame["source_file"].unique())
    assert frame["published_at"].max() > pd.Timestamp("2023-12-31", tz="UTC")
    warmup = frame.loc[frame["title"] == "Warmup"].iloc[0]
    assert bool(warmup["is_warmup"]) is True
    assert bool(warmup["in_evaluation_window"]) is False

    mapped_path = tmp_path / "synthetic_news_source_mapped.parquet"
    mapping_stats = map_news_sources(
        news_path=output_path,
        output_path=mapped_path,
        overwrite=True,
    )
    assert mapping_stats["rows_written"] == len(frame)
    mapped_frame = pd.read_parquet(mapped_path)
    assert mapped_frame["is_mapped"].all()
    assert mapped_frame["is_mapped"].mean() >= 1.0
    mapped_warmup = mapped_frame.loc[mapped_frame["title"] == "Warmup"].iloc[0]
    assert bool(mapped_warmup["is_warmup"]) is True
    assert bool(mapped_warmup["in_evaluation_window"]) is False
    mapped_tail = mapped_frame.loc[mapped_frame["title"] == "Tail"].iloc[0]
    assert mapped_tail["published_at"] > pd.Timestamp("2023-12-31", tz="UTC")
    assert bool(mapped_tail["in_evaluation_window"]) is False

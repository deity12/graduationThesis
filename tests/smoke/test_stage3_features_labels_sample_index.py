from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.build_forward_returns import build_forward_returns
from src.data.build_feature_panel import build_feature_panel
from src.data.normalize_prices import normalize_prices_dataset
from src.training.sample_index import build_sample_index


SAMPLE_TICKERS = {"AAPL", "A", "ABBV", "CPAY", "RVTY", "SPG", "SYF", "TDG", "WM", "WMB"}


def test_stage3_pipeline_smoke(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices_daily.parquet"
    feature_path = tmp_path / "feature_panel_v1.parquet"
    forward_path = tmp_path / "forward_returns.parquet"
    sample_index_path = tmp_path / "sample_index.parquet"
    news_path = tmp_path / "mapped_news.parquet"

    normalize_prices_dataset(
        output_path=prices_path,
        tickers=SAMPLE_TICKERS,
        overwrite=True,
    )
    news_frame = pd.DataFrame(
        [
            {"published_date": "2016-01-04", "source_ticker": "AAPL", "is_mapped": True},
            {"published_date": "2016-01-04", "source_ticker": "AAPL", "is_mapped": True},
            {"published_date": "2019-03-01", "source_ticker": "ABBV", "is_mapped": True},
            {"published_date": "2019-03-01", "source_ticker": "", "is_mapped": False},
        ]
    )
    news_frame.to_parquet(news_path, index=False)

    feature_stats = build_feature_panel(
        prices_path=prices_path,
        output_path=feature_path,
        tickers=SAMPLE_TICKERS,
        overwrite=True,
    )
    assert feature_stats["rows_written"] > 0
    feature_frame = pd.read_parquet(feature_path)
    assert {
        "as_of_date",
        "ticker",
        "ret_1",
        "ret_5",
        "ret_20",
        "vol_5",
        "vol_20",
        "volume_zscore",
        "hl_spread",
        "oc_return",
        "feat_mkt_ret_1d",
        "feature_complete",
    }.issubset(set(feature_frame.columns))
    assert feature_frame["as_of_date"].max() <= "2023-12-31"

    label_stats = build_forward_returns(
        prices_path=prices_path,
        output_path=forward_path,
        tickers=SAMPLE_TICKERS,
        overwrite=True,
    )
    assert label_stats["rows_written"] > 0
    label_frame = pd.read_parquet(forward_path)
    assert {"as_of_date", "ticker", "label_ret_1d", "label_start_date", "label_end_date"}.issubset(
        set(label_frame.columns)
    )
    assert (pd.to_datetime(label_frame["label_start_date"]) > pd.to_datetime(label_frame["as_of_date"])).all()

    sample_stats = build_sample_index(
        feature_path=feature_path,
        forward_returns_path=forward_path,
        news_path=news_path,
        output_path=sample_index_path,
        tickers=SAMPLE_TICKERS,
        split_filter={"S1"},
        overwrite=True,
    )
    assert sample_stats["rows_written"] > 0
    assert "S1" in sample_stats["split_counts"]
    sample_frame = pd.read_parquet(sample_index_path)
    assert {"split", "partition", "as_of_date", "ticker", "label", "cutoff_date"}.issubset(set(sample_frame.columns))
    assert set(sample_frame["split"].unique()) == {"S1"}
    assert sample_frame["as_of_date"].min() >= "2016-01-01"
    assert (sample_frame["cutoff_date"] == sample_frame["as_of_date"]).all()
    assert sample_frame.loc[
        (sample_frame["as_of_date"] == "2016-01-04") & (sample_frame["ticker"] == "AAPL"),
        "mapped_news_count_1d",
    ].max() == 2

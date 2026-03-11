from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.common.config import get_project_root, load_yaml


PROJECT_ROOT = get_project_root()


def test_universe_file_exists_and_has_required_columns() -> None:
    paths_config = load_yaml("configs/paths.yaml")
    universe_config = load_yaml("configs/data/universe.yaml")

    universe_path = PROJECT_ROOT / paths_config["paths"]["raw_universe_root"] / universe_config["universe"]["output_filename"]
    assert universe_path.is_file()

    universe_frame = pd.read_csv(universe_path, dtype=str).fillna("")
    required_columns = {
        "ticker",
        "price_ticker",
        "company_name",
        "gics_sector",
        "gics_sub_industry",
        "headquarters",
        "date_added",
        "cik",
        "founded",
    }
    assert required_columns.issubset(set(universe_frame.columns))
    assert len(universe_frame) >= 400


def test_sample_stock_price_files_are_readable() -> None:
    paths_config = load_yaml("configs/paths.yaml")
    universe_config = load_yaml("configs/data/universe.yaml")

    universe_path = PROJECT_ROOT / paths_config["paths"]["raw_universe_root"] / universe_config["universe"]["output_filename"]
    stocks_root = PROJECT_ROOT / paths_config["paths"]["raw_prices_stocks_root"]
    manifest_path = stocks_root / universe_config["prices"]["stock_manifest_filename"]
    universe_frame = pd.read_csv(universe_path, dtype=str).fillna("")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    total_count = len(universe_frame)
    assert manifest["requested_ticker_count"] == total_count
    assert manifest["downloaded_count"] + manifest["skipped_count"] + manifest["failed_count"] == total_count

    sample_tickers = universe_frame.head(3)["ticker"].tolist()
    for ticker in sample_tickers:
        stock_path = stocks_root / f"{ticker}.csv"
        assert stock_path.is_file(), f"Missing stock price file: {stock_path}"
        stock_frame = pd.read_csv(stock_path)
        assert {"date", "adj_close", "close", "volume"}.issubset(set(stock_frame.columns))
        assert len(stock_frame) > 1000


def test_market_index_file_exists() -> None:
    paths_config = load_yaml("configs/paths.yaml")
    universe_config = load_yaml("configs/data/universe.yaml")

    market_path = (
        PROJECT_ROOT
        / paths_config["paths"]["raw_prices_market_root"]
        / universe_config["prices"]["market_output_filename"]
    )
    metadata_path = (
        PROJECT_ROOT
        / paths_config["paths"]["raw_prices_market_root"]
        / universe_config["prices"]["market_metadata_filename"]
    )

    assert market_path.is_file()
    market_frame = pd.read_csv(market_path)
    assert {"date", "adj_close", "close"}.issubset(set(market_frame.columns))
    assert len(market_frame) > 1000

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["market_symbol"] == "^GSPC"


def test_alias_seed_exists_and_is_non_empty() -> None:
    paths_config = load_yaml("configs/paths.yaml")
    universe_config = load_yaml("configs/data/universe.yaml")

    alias_path = PROJECT_ROOT / paths_config["paths"]["raw_mapping_root"] / universe_config["mapping"]["output_filename"]
    metadata_path = PROJECT_ROOT / paths_config["paths"]["raw_mapping_root"] / universe_config["mapping"]["metadata_filename"]

    assert alias_path.is_file()
    alias_frame = pd.read_csv(alias_path, dtype={"ticker": str}).fillna("")
    assert {"ticker", "company_name", "alias", "alias_normalized", "alias_type", "is_primary"}.issubset(
        set(alias_frame.columns)
    )
    assert len(alias_frame) > 0
    assert alias_frame["ticker"].nunique() >= 400

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["row_count"] == len(alias_frame)

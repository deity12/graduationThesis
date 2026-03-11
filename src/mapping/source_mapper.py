"""Map normalized news rows to the fixed source universe."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.common.config import load_yaml, resolve_from_root
from src.common.io import ParquetChunkWriter
from src.mapping.alias_table import normalize_alias_text


DEFAULT_PATHS_CONFIG = "configs/paths.yaml"
DEFAULT_STAGE2_CONFIG = "configs/data/stage2_normalization.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths-config", default=DEFAULT_PATHS_CONFIG)
    parser.add_argument("--config", default=DEFAULT_STAGE2_CONFIG)
    parser.add_argument("--news-path", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def _load_mapping_inputs(paths_config_path: str, stage2_config_path: str) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    paths_config = load_yaml(paths_config_path)
    stage2_config = load_yaml(stage2_config_path)["stage2"]
    universe_config = load_yaml("configs/data/universe.yaml")

    universe_path = resolve_from_root(paths_config["paths"]["raw_universe_root"]) / universe_config["universe"]["output_filename"]
    alias_path = resolve_from_root(paths_config["paths"]["raw_mapping_root"]) / universe_config["mapping"]["output_filename"]
    universe_frame = pd.read_csv(universe_path, dtype=str).fillna("")
    alias_frame = pd.read_csv(alias_path, dtype=str).fillna("")
    return stage2_config, universe_frame, alias_frame


def _build_alias_records(alias_frame: pd.DataFrame, mapping_config: dict[str, Any]) -> list[dict[str, str]]:
    alias_frame = alias_frame.copy()
    alias_frame["alias_normalized"] = alias_frame["alias_normalized"].astype(str)
    alias_frame = alias_frame[alias_frame["alias_normalized"].str.len() >= int(mapping_config["alias_min_length"])]
    alias_frame["alias_length"] = alias_frame["alias_normalized"].str.len()
    alias_frame = alias_frame.sort_values(["alias_length", "is_primary"], ascending=[False, False])
    return alias_frame[["ticker", "price_ticker", "company_name", "alias_normalized"]].to_dict(orient="records")


def _map_alias_fallback(
    frame: pd.DataFrame,
    alias_records: list[dict[str, str]],
    mapping_config: dict[str, Any],
) -> pd.DataFrame:
    unresolved = frame[frame["source_ticker"].eq("")].copy()
    if unresolved.empty:
        return frame

    alias_scan_columns = list(mapping_config["alias_scan_columns"])
    scan_limit = int(mapping_config["max_alias_scan_rows"])
    unresolved = unresolved.head(scan_limit).copy()
    normalized_text = unresolved[alias_scan_columns].fillna("").astype(str).agg(" ".join, axis=1).map(normalize_alias_text)

    for index, text in normalized_text.items():
        if not text:
            continue
        for alias_record in alias_records:
            if alias_record["alias_normalized"] in text:
                frame.at[index, "source_ticker"] = alias_record["ticker"]
                frame.at[index, "source_price_ticker"] = alias_record["price_ticker"]
                frame.at[index, "source_company_name"] = alias_record["company_name"]
                frame.at[index, "mapping_method"] = "alias_text"
                frame.at[index, "mapping_confidence"] = "medium"
                frame.at[index, "is_mapped"] = True
                break
    return frame


def map_news_batch(
    frame: pd.DataFrame,
    stage2_config: dict[str, Any],
    ticker_lookup: dict[str, str],
    price_ticker_lookup: dict[str, str],
    legacy_lookup: dict[str, str],
    ticker_to_price: dict[str, str],
    ticker_to_company: dict[str, str],
    alias_records: list[dict[str, str]],
) -> pd.DataFrame:
    mapped = frame.copy()
    ticker_raw_key = mapped["ticker_raw"].fillna("").astype(str).str.upper().str.strip()
    mapped["source_ticker"] = ticker_raw_key.map(ticker_lookup).fillna("")
    mapped["mapping_method"] = mapped["source_ticker"].map(lambda value: "ticker_raw" if value else "")

    missing_direct = mapped["source_ticker"].eq("")
    mapped.loc[missing_direct, "source_ticker"] = ticker_raw_key[missing_direct].map(price_ticker_lookup).fillna("")
    mapped.loc[missing_direct & mapped["source_ticker"].ne(""), "mapping_method"] = "price_ticker"

    missing_after_price = mapped["source_ticker"].eq("")
    mapped.loc[missing_after_price, "source_ticker"] = ticker_raw_key[missing_after_price].map(legacy_lookup).fillna("")
    mapped.loc[missing_after_price & mapped["source_ticker"].ne(""), "mapping_method"] = "legacy_ticker"

    mapped["source_price_ticker"] = mapped["source_ticker"].map(ticker_to_price).fillna("")
    mapped["source_company_name"] = mapped["source_ticker"].map(ticker_to_company).fillna("")
    mapped["mapping_confidence"] = mapped["mapping_method"].map(
        {"ticker_raw": "high", "price_ticker": "high", "legacy_ticker": "high"}
    ).fillna("")
    mapped["is_mapped"] = mapped["source_ticker"].ne("")

    mapped = _map_alias_fallback(mapped, alias_records, stage2_config["mapping"])
    mapped["is_mapped"] = mapped["source_ticker"].ne("")
    mapped["mapping_confidence"] = mapped["mapping_confidence"].mask(mapped["mapping_confidence"].eq(""), "none")
    mapped["version"] = stage2_config["dataset_version"]
    return mapped


def map_news_sources(
    paths_config_path: str = DEFAULT_PATHS_CONFIG,
    stage2_config_path: str = DEFAULT_STAGE2_CONFIG,
    news_path: str | Path | None = None,
    output_path: str | Path | None = None,
    batch_size: int = 50000,
    overwrite: bool = False,
) -> dict[str, Any]:
    stage2_config, universe_frame, alias_frame = _load_mapping_inputs(paths_config_path, stage2_config_path)
    news_parquet_path = resolve_from_root(news_path or stage2_config["outputs"]["news_normalized"])
    output = resolve_from_root(output_path or stage2_config["outputs"]["news_source_mapped"])
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output already exists, rerun with overwrite=True: {output}")

    ticker_lookup = {ticker.upper(): ticker for ticker in universe_frame["ticker"]}
    price_ticker_lookup = {ticker.upper(): mapped for ticker, mapped in zip(universe_frame["price_ticker"], universe_frame["ticker"])}
    legacy_lookup = {legacy.upper(): current for legacy, current in stage2_config["mapping"]["legacy_ticker_to_current"].items()}
    ticker_to_price = dict(zip(universe_frame["ticker"], universe_frame["price_ticker"]))
    ticker_to_company = dict(zip(universe_frame["ticker"], universe_frame["company_name"]))
    alias_records = _build_alias_records(alias_frame, stage2_config["mapping"])

    stats = {"rows_read": 0, "rows_written": 0, "rows_mapped": 0}
    parquet_file = pq.ParquetFile(news_parquet_path)
    with ParquetChunkWriter(output) as writer:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            frame = batch.to_pandas()
            stats["rows_read"] += len(frame)
            mapped = map_news_batch(
                frame=frame,
                stage2_config=stage2_config,
                ticker_lookup=ticker_lookup,
                price_ticker_lookup=price_ticker_lookup,
                legacy_lookup=legacy_lookup,
                ticker_to_price=ticker_to_price,
                ticker_to_company=ticker_to_company,
                alias_records=alias_records,
            )
            writer.write(mapped)
            stats["rows_written"] += len(mapped)
            stats["rows_mapped"] += int(mapped["is_mapped"].sum())
    return stats


def main() -> int:
    args = parse_args()
    stats = map_news_sources(
        paths_config_path=args.paths_config,
        stage2_config_path=args.config,
        news_path=args.news_path or None,
        output_path=args.output or None,
        batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

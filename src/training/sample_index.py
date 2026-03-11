"""Build stage 3 sample_index skeleton without graph dates."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.common.config import load_yaml
from src.common.io import ParquetChunkWriter
from src.common.leakage_guard import (
    filter_frame_by_cutoff,
    validate_feature_frame,
    validate_forward_returns_frame,
    validate_sample_index_frame,
)
from src.data.build_feature_panel import build_feature_panel
from src.data.build_forward_returns import build_forward_returns
from src.data.stage3_common import (
    DEFAULT_PATHS_CONFIG,
    DEFAULT_STAGE2_CONFIG,
    DEFAULT_STAGE3_CONFIG,
    load_stage3_runtime,
    parse_split_filter,
    parse_ticker_filter,
    resolve_cutoff_date,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths-config", default=DEFAULT_PATHS_CONFIG)
    parser.add_argument("--stage2-config", default=DEFAULT_STAGE2_CONFIG)
    parser.add_argument("--stage3-config", default=DEFAULT_STAGE3_CONFIG)
    parser.add_argument("--feature-path", default="")
    parser.add_argument("--forward-returns-path", default="")
    parser.add_argument("--news-path", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--tickers", default="")
    parser.add_argument("--splits", default="")
    parser.add_argument("--cutoff-date", default="")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def _load_feature_frame(path: Path, cutoff_date: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame = filter_frame_by_cutoff(frame, "as_of_date", cutoff_date, "feature_panel")
    validate_feature_frame(frame, cutoff_date)
    return frame


def _load_forward_frame(path: Path, cutoff_date: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame = filter_frame_by_cutoff(frame, "as_of_date", cutoff_date, "forward_returns")
    frame = filter_frame_by_cutoff(frame, "label_end_date", cutoff_date, "forward_returns.label_end_date")
    validate_forward_returns_frame(frame, cutoff_date)
    return frame


def _build_news_metadata(news_path: Path, batch_size: int) -> pd.DataFrame:
    if not news_path.is_file():
        return pd.DataFrame(columns=["as_of_date", "ticker", "mapped_news_count_1d", "has_mapped_news"])

    counts: dict[tuple[str, str], int] = defaultdict(int)
    parquet_file = pq.ParquetFile(news_path)
    for batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=["published_date", "source_ticker", "is_mapped"],
    ):
        frame = batch.to_pandas()
        frame = frame[frame["is_mapped"].eq(True)].copy()
        frame["source_ticker"] = frame["source_ticker"].fillna("").astype(str).str.upper()
        frame["published_date"] = frame["published_date"].fillna("").astype(str)
        frame = frame[frame["source_ticker"].ne("") & frame["published_date"].ne("")]
        if frame.empty:
            continue
        grouped = frame.groupby(["published_date", "source_ticker"], as_index=False).size()
        for row in grouped.itertuples(index=False):
            counts[(row.published_date, row.source_ticker)] += int(row.size)

    if not counts:
        return pd.DataFrame(columns=["as_of_date", "ticker", "mapped_news_count_1d", "has_mapped_news"])

    metadata = pd.DataFrame(
        [
            {
                "as_of_date": as_of_date,
                "ticker": ticker,
                "mapped_news_count_1d": count,
                "has_mapped_news": count > 0,
            }
            for (as_of_date, ticker), count in counts.items()
        ]
    )
    return metadata.sort_values(["as_of_date", "ticker"]).reset_index(drop=True)


def _iter_split_parts(protocol_config_path: str, split_filter: set[str] | None) -> list[tuple[str, str, str, str]]:
    protocol_config = load_yaml(protocol_config_path)
    parts: list[tuple[str, str, str, str]] = []
    for split in protocol_config["splits"]:
        split_name = str(split["name"]).upper()
        if split_filter is not None and split_name not in split_filter:
            continue
        for partition in ("train", "val", "test"):
            part_window = split[partition]
            parts.append((split_name, partition, part_window["start"], part_window["end"]))
    return parts


def build_sample_index(
    paths_config_path: str = DEFAULT_PATHS_CONFIG,
    stage2_config_path: str = DEFAULT_STAGE2_CONFIG,
    stage3_config_path: str = DEFAULT_STAGE3_CONFIG,
    feature_path: str | Path | None = None,
    forward_returns_path: str | Path | None = None,
    news_path: str | Path | None = None,
    output_path: str | Path | None = None,
    tickers: set[str] | None = None,
    split_filter: set[str] | None = None,
    cutoff_date: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    runtime = load_stage3_runtime(
        paths_config_path=paths_config_path,
        stage2_config_path=stage2_config_path,
        stage3_config_path=stage3_config_path,
        feature_panel_path=feature_path,
        forward_returns_path=forward_returns_path,
        news_path=news_path,
        sample_index_path=output_path,
    )
    cutoff = resolve_cutoff_date(cutoff_date, runtime.protocol_window)
    output = runtime.sample_index_path
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output already exists, rerun with overwrite=True: {output}")

    if not runtime.feature_panel_path.exists():
        build_feature_panel(
            paths_config_path=paths_config_path,
            stage2_config_path=stage2_config_path,
            stage3_config_path=stage3_config_path,
            output_path=runtime.feature_panel_path,
            tickers=tickers,
            cutoff_date=cutoff,
            overwrite=False,
        )
    if not runtime.forward_returns_path.exists():
        build_forward_returns(
            paths_config_path=paths_config_path,
            stage2_config_path=stage2_config_path,
            stage3_config_path=stage3_config_path,
            output_path=runtime.forward_returns_path,
            tickers=tickers,
            cutoff_date=cutoff,
            overwrite=False,
        )

    feature_frame = _load_feature_frame(runtime.feature_panel_path, cutoff)
    forward_frame = _load_forward_frame(runtime.forward_returns_path, cutoff)
    if tickers is not None:
        feature_frame = feature_frame[feature_frame["ticker"].isin(tickers)].copy()
        forward_frame = forward_frame[forward_frame["ticker"].isin(tickers)].copy()

    required_feature_columns = list(runtime.stage3_config["features"]["required_columns"])
    label_column = runtime.stage3_config["labels"]["label_column"]
    base = feature_frame[
        ["as_of_date", "ticker", "feature_complete", "feature_window_end_date", *required_feature_columns]
    ].merge(
        forward_frame[["as_of_date", "ticker", label_column, "label_start_date", "label_end_date"]],
        on=["as_of_date", "ticker"],
        how="inner",
    )
    base = base[base["feature_complete"].eq(True)].copy()
    base["cutoff_date"] = base["as_of_date"]
    base["label"] = base[label_column]
    base["label_name"] = label_column

    news_metadata = _build_news_metadata(
        runtime.news_source_mapped_path,
        int(runtime.stage3_config["sample_index"]["news_metadata_batch_size"]),
    )
    if not news_metadata.empty:
        base = base.merge(news_metadata, on=["as_of_date", "ticker"], how="left")
    else:
        base["mapped_news_count_1d"] = 0
        base["has_mapped_news"] = False
    base["mapped_news_count_1d"] = pd.to_numeric(base["mapped_news_count_1d"], errors="coerce").fillna(0).astype("int64")
    base["has_mapped_news"] = base["has_mapped_news"].eq(True)

    split_parts = _iter_split_parts(runtime.stage3_config["protocol_config"], split_filter)
    rows_written = 0
    split_counts: dict[str, int] = defaultdict(int)
    with ParquetChunkWriter(output) as writer:
        for split_name, partition, start_date, end_date in split_parts:
            part_frame = base[base["as_of_date"].between(start_date, end_date, inclusive="both")].copy()
            if part_frame.empty:
                continue
            part_frame["split"] = split_name
            part_frame["partition"] = partition
            part_frame = part_frame[
                [
                    "split",
                    "partition",
                    "as_of_date",
                    "ticker",
                    "cutoff_date",
                    "feature_window_end_date",
                    "label_name",
                    "label",
                    "label_start_date",
                    "label_end_date",
                    "mapped_news_count_1d",
                    "has_mapped_news",
                ]
            ].sort_values(["as_of_date", "ticker"]).reset_index(drop=True)
            validate_sample_index_frame(
                part_frame,
                cutoff_date=cutoff,
                evaluation_start=runtime.protocol_window.evaluation_start,
            )
            writer.write(part_frame)
            rows_written += len(part_frame)
            split_counts[split_name] += len(part_frame)

    return {
        "rows_written": int(rows_written),
        "split_counts": dict(split_counts),
        "cutoff_date": cutoff,
        "news_metadata_rows": int(len(news_metadata)),
    }


def main() -> int:
    args = parse_args()
    stats = build_sample_index(
        paths_config_path=args.paths_config,
        stage2_config_path=args.stage2_config,
        stage3_config_path=args.stage3_config,
        feature_path=args.feature_path or None,
        forward_returns_path=args.forward_returns_path or None,
        news_path=args.news_path or None,
        output_path=args.output or None,
        tickers=parse_ticker_filter(args.tickers),
        split_filter=parse_split_filter(args.splits),
        cutoff_date=args.cutoff_date or None,
        overwrite=args.overwrite,
    )
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

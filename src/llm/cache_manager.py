"""Filesystem cache helpers for Stage 5 extraction batches."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.config import resolve_from_root
from src.common.io import ParquetChunkWriter, ensure_parent_dir
from src.llm.batch_extract import FAILURE_COLUMNS, PARSED_CACHE_COLUMNS, RAW_CACHE_COLUMNS


EDGE_COLUMNS = [
    "news_id",
    "published_date",
    "cutoff_date",
    "is_warmup",
    "in_evaluation_window",
    "source_ticker",
    "target_ticker",
    "event_type",
    "shard_id",
    "batch_id",
    "run_mode",
]


class Stage5CacheManager:
    """Manage raw/parsed/failure cache files for resumable Stage 5 execution."""

    def __init__(self, raw_dir: str, parsed_dir: str, failure_dir: str) -> None:
        self.raw_dir = ensure_parent_dir(resolve_from_root(raw_dir) / ".gitkeep").parent
        self.parsed_dir = ensure_parent_dir(resolve_from_root(parsed_dir) / ".gitkeep").parent
        self.failure_dir = ensure_parent_dir(resolve_from_root(failure_dir) / ".gitkeep").parent

    def _batch_path(self, root: Path, shard_id: str, batch_id: int) -> Path:
        return root / f"month={shard_id}" / f"batch_{batch_id:06d}.parquet"

    def raw_batch_path(self, shard_id: str, batch_id: int) -> Path:
        return self._batch_path(self.raw_dir, shard_id, batch_id)

    def parsed_batch_path(self, shard_id: str, batch_id: int) -> Path:
        return self._batch_path(self.parsed_dir, shard_id, batch_id)

    def failure_batch_path(self, shard_id: str, batch_id: int) -> Path:
        return self._batch_path(self.failure_dir, shard_id, batch_id)

    def batch_complete(self, shard_id: str, batch_id: int) -> bool:
        return self.raw_batch_path(shard_id, batch_id).exists() and self.parsed_batch_path(shard_id, batch_id).exists()

    def write_batch(
        self,
        *,
        shard_id: str,
        batch_id: int,
        raw_frame: pd.DataFrame,
        parsed_frame: pd.DataFrame,
        failure_frame: pd.DataFrame,
        overwrite: bool,
    ) -> dict[str, str]:
        raw_path = ensure_parent_dir(self.raw_batch_path(shard_id, batch_id))
        parsed_path = ensure_parent_dir(self.parsed_batch_path(shard_id, batch_id))
        failure_path = self.failure_batch_path(shard_id, batch_id)

        for path in (raw_path, parsed_path):
            if path.exists() and not overwrite:
                raise FileExistsError(f"Cache batch already exists: {path}")

        raw_frame.to_parquet(raw_path, index=False)
        parsed_frame.to_parquet(parsed_path, index=False)

        written_failure_path = ""
        if failure_path.exists() and overwrite:
            failure_path.unlink()
        if not failure_frame.empty:
            ensure_parent_dir(failure_path)
            failure_frame.to_parquet(failure_path, index=False)
            written_failure_path = str(failure_path)
        return {
            "raw_path": str(raw_path),
            "parsed_path": str(parsed_path),
            "failure_path": written_failure_path,
        }

    def iter_batch_files(self, kind: str) -> list[Path]:
        if kind == "raw":
            root = self.raw_dir
        elif kind == "parsed":
            root = self.parsed_dir
        elif kind == "failure":
            root = self.failure_dir
        else:
            raise ValueError(f"Unsupported cache kind: {kind}")
        return sorted(root.rglob("*.parquet"))

    def consolidate_kind(self, kind: str, output_path: str) -> dict[str, int]:
        files = self.iter_batch_files(kind)
        rows_written = 0
        target = resolve_from_root(output_path)
        if kind == "raw":
            empty_columns = RAW_CACHE_COLUMNS
        elif kind == "parsed":
            empty_columns = PARSED_CACHE_COLUMNS
        elif kind == "failure":
            empty_columns = FAILURE_COLUMNS
        else:
            raise ValueError(f"Unsupported cache kind: {kind}")

        if not files:
            ensure_parent_dir(target)
            pd.DataFrame(columns=empty_columns).to_parquet(target, index=False)
            return {"file_count": 0, "row_count": 0}

        with ParquetChunkWriter(target) as writer:
            for file_path in files:
                frame = pd.read_parquet(file_path)
                writer.write(frame)
                rows_written += len(frame)
        return {"file_count": len(files), "row_count": rows_written}

    def build_edges(self, parsed_output_path: str) -> pd.DataFrame:
        parsed_path = resolve_from_root(parsed_output_path)
        if not parsed_path.exists():
            return pd.DataFrame(columns=EDGE_COLUMNS)
        frame = pd.read_parquet(parsed_path)
        if frame.empty:
            return pd.DataFrame(columns=EDGE_COLUMNS)
        exploded = frame.copy()
        exploded = exploded.explode("resolved_target_tickers")
        exploded = exploded.dropna(subset=["resolved_target_tickers"])
        exploded["resolved_target_tickers"] = exploded["resolved_target_tickers"].astype(str).str.strip()
        exploded = exploded[exploded["resolved_target_tickers"].ne("")]
        exploded = exploded.rename(
            columns={
                "resolved_target_tickers": "target_ticker",
                "extracted_event_type": "event_type",
            }
        )
        return exploded[EDGE_COLUMNS].reset_index(drop=True)

"""IO helpers shared by stage 2 normalization modules."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def ensure_parent_dir(path: str | Path) -> Path:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def write_json(path: str | Path, payload: Any) -> None:
    resolved = ensure_parent_dir(path)
    resolved.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class ParquetChunkWriter:
    """Append pandas chunks into one parquet file."""

    def __init__(self, path: str | Path) -> None:
        self.path = ensure_parent_dir(path)
        self._writer: pq.ParquetWriter | None = None

    def write(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, table.schema)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> "ParquetChunkWriter":
        return self

    def __exit__(self, exc_type: object, exc: object, exc_tb: object) -> None:
        self.close()

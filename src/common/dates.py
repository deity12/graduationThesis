"""Date helpers for stage 2 normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.common.config import load_yaml


DEFAULT_PROTOCOL_CONFIG = "configs/data/walk_forward_2016_2023.yaml"


@dataclass(frozen=True)
class ProtocolWindow:
    """Fixed warm-up and evaluation boundaries."""

    warmup_start: pd.Timestamp
    warmup_end: pd.Timestamp
    evaluation_start: pd.Timestamp
    evaluation_end: pd.Timestamp

    @property
    def inclusive_start(self) -> pd.Timestamp:
        return self.warmup_start

    @property
    def inclusive_end(self) -> pd.Timestamp:
        return self.evaluation_end


def load_protocol_window(config_path: str = DEFAULT_PROTOCOL_CONFIG) -> ProtocolWindow:
    payload = load_yaml(config_path)["protocol"]
    return ProtocolWindow(
        warmup_start=pd.Timestamp(payload["warmup_start"], tz="UTC"),
        warmup_end=pd.Timestamp(payload["warmup_end"], tz="UTC"),
        evaluation_start=pd.Timestamp(payload["evaluation_start"], tz="UTC"),
        evaluation_end=pd.Timestamp(payload["evaluation_end"], tz="UTC"),
    )


def parse_timestamp_utc(values: Any) -> pd.Series:
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    if isinstance(parsed, pd.Series):
        return parsed
    return pd.Series(parsed)


def normalize_date(values: Any) -> pd.Series:
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    if not isinstance(parsed, pd.Series):
        parsed = pd.Series(parsed)
    return parsed.dt.strftime("%Y-%m-%d")


def within_stage2_window(timestamps: pd.Series, window: ProtocolWindow) -> pd.Series:
    normalized = pd.to_datetime(timestamps, utc=True, errors="coerce").dt.normalize()
    return normalized.between(
        window.inclusive_start.normalize(),
        window.inclusive_end.normalize(),
        inclusive="both",
    ).fillna(False)


def warmup_mask(date_values: pd.Series, window: ProtocolWindow) -> pd.Series:
    dates = pd.to_datetime(date_values, utc=True, errors="coerce").dt.normalize()
    return dates.between(
        window.warmup_start.normalize(),
        window.warmup_end.normalize(),
        inclusive="both",
    ).fillna(False)


def evaluation_mask(date_values: pd.Series, window: ProtocolWindow) -> pd.Series:
    dates = pd.to_datetime(date_values, utc=True, errors="coerce").dt.normalize()
    return dates.between(
        window.evaluation_start.normalize(),
        window.evaluation_end.normalize(),
        inclusive="both",
    ).fillna(False)

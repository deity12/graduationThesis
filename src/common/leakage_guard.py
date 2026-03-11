"""Common stage 3 leakage guard helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd


class LeakageError(ValueError):
    """Raised when a dataframe violates the cutoff-date protocol."""


def normalize_cutoff_date(cutoff_date: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(cutoff_date)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.normalize()


def normalize_date_series(values: Any) -> pd.Series:
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    if not isinstance(parsed, pd.Series):
        parsed = pd.Series(parsed)
    normalized = parsed.dt.normalize()
    if normalized.isna().any():
        raise LeakageError("Encountered invalid date values while applying leakage guards.")
    return normalized


def _comparison_mask(left: pd.Series, right: pd.Series, relation: str) -> pd.Series:
    operations = {
        "<": left < right,
        "<=": left <= right,
        "==": left == right,
        ">=": left >= right,
        ">": left > right,
    }
    if relation not in operations:
        raise ValueError(f"Unsupported date relation: {relation}")
    return operations[relation]


def assert_column_not_after_cutoff(
    frame: pd.DataFrame,
    date_column: str,
    cutoff_date: Any,
    context: str,
    allow_equal: bool = True,
) -> None:
    if frame.empty:
        return
    cutoff = normalize_cutoff_date(cutoff_date)
    series = normalize_date_series(frame[date_column])
    invalid_mask = series > cutoff if allow_equal else series >= cutoff
    if not invalid_mask.any():
        return
    offending = frame.loc[invalid_mask, [date_column]].head(3).to_dict(orient="records")
    raise LeakageError(
        f"{context} contains rows after cutoff_date={cutoff.strftime('%Y-%m-%d')}: {offending}"
    )


def assert_column_not_before(
    frame: pd.DataFrame,
    date_column: str,
    lower_bound: Any,
    context: str,
    allow_equal: bool = True,
) -> None:
    if frame.empty:
        return
    lower = normalize_cutoff_date(lower_bound)
    series = normalize_date_series(frame[date_column])
    invalid_mask = series < lower if allow_equal else series <= lower
    if not invalid_mask.any():
        return
    offending = frame.loc[invalid_mask, [date_column]].head(3).to_dict(orient="records")
    raise LeakageError(
        f"{context} contains rows before lower bound={lower.strftime('%Y-%m-%d')}: {offending}"
    )


def assert_date_column_relation(
    frame: pd.DataFrame,
    left_column: str,
    right_column: str,
    relation: str,
    context: str,
) -> None:
    if frame.empty:
        return
    left = normalize_date_series(frame[left_column])
    right = normalize_date_series(frame[right_column])
    valid_mask = _comparison_mask(left, right, relation)
    invalid_mask = ~valid_mask.fillna(False)
    if not invalid_mask.any():
        return
    offending = frame.loc[invalid_mask, [left_column, right_column]].head(3).to_dict(orient="records")
    raise LeakageError(f"{context} violates {left_column} {relation} {right_column}: {offending}")


def filter_frame_by_cutoff(
    frame: pd.DataFrame,
    date_column: str,
    cutoff_date: Any,
    context: str,
    allow_equal: bool = True,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    cutoff = normalize_cutoff_date(cutoff_date)
    series = normalize_date_series(frame[date_column])
    mask = series <= cutoff if allow_equal else series < cutoff
    filtered = frame.loc[mask].copy()
    assert_column_not_after_cutoff(filtered, date_column, cutoff, context, allow_equal=allow_equal)
    return filtered


def validate_feature_frame(frame: pd.DataFrame, cutoff_date: Any) -> None:
    assert_column_not_after_cutoff(frame, "as_of_date", cutoff_date, "feature_panel.as_of_date")
    assert_date_column_relation(
        frame,
        "feature_window_end_date",
        "as_of_date",
        "==",
        "feature_panel.feature_window_end_date",
    )


def validate_forward_returns_frame(frame: pd.DataFrame, cutoff_date: Any) -> None:
    assert_column_not_after_cutoff(frame, "as_of_date", cutoff_date, "forward_returns.as_of_date")
    assert_column_not_after_cutoff(frame, "label_end_date", cutoff_date, "forward_returns.label_end_date")
    assert_date_column_relation(
        frame,
        "as_of_date",
        "label_start_date",
        "<",
        "forward_returns.label_start_date",
    )
    assert_date_column_relation(
        frame,
        "label_start_date",
        "label_end_date",
        "<=",
        "forward_returns.label_end_date",
    )


def validate_sample_index_frame(
    frame: pd.DataFrame,
    cutoff_date: Any,
    evaluation_start: Any,
) -> None:
    assert_column_not_after_cutoff(frame, "as_of_date", cutoff_date, "sample_index.as_of_date")
    assert_column_not_before(frame, "as_of_date", evaluation_start, "sample_index.as_of_date")
    assert_date_column_relation(frame, "as_of_date", "cutoff_date", "==", "sample_index.cutoff_date")
    assert_date_column_relation(frame, "as_of_date", "label_start_date", "<", "sample_index.label_start_date")
    assert_date_column_relation(frame, "label_start_date", "label_end_date", "<=", "sample_index.label_end_date")

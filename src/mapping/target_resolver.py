"""Resolve extracted target entities into the fixed research universe."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.common.config import resolve_from_root
from src.mapping.alias_table import normalize_alias_text


@dataclass(frozen=True)
class TargetResolverConfig:
    """Configuration for exact-match target resolution."""

    alias_seed_csv: str
    company_alias_min_length: int = 4
    allow_exact_ticker: bool = True


class TargetResolver:
    """Resolve model-extracted target strings to SP500 tickers."""

    def __init__(self, config: TargetResolverConfig) -> None:
        self.config = config
        alias_path = resolve_from_root(config.alias_seed_csv)
        alias_frame = pd.read_csv(alias_path, dtype=str).fillna("")
        alias_frame["ticker_upper"] = alias_frame["ticker"].astype(str).str.upper().str.strip()
        alias_frame["alias_normalized"] = alias_frame["alias_normalized"].astype(str)
        alias_frame["alias_type"] = alias_frame["alias_type"].astype(str)

        ticker_rows = (
            alias_frame.sort_values(["ticker_upper", "is_primary"], ascending=[True, False])
            .drop_duplicates(subset=["ticker_upper"], keep="first")
        )
        self._ticker_lookup = {
            row["ticker_upper"]: {
                "ticker": row["ticker"],
                "price_ticker": row.get("price_ticker", ""),
                "company_name": row.get("company_name", ""),
            }
            for _, row in ticker_rows.iterrows()
        }

        alias_candidates = alias_frame[alias_frame["alias_type"].ne("ticker")].copy()
        alias_candidates = alias_candidates[
            alias_candidates["alias_normalized"].str.len() >= int(config.company_alias_min_length)
        ]
        grouped = alias_candidates.groupby("alias_normalized", dropna=False)
        self._alias_lookup = {
            alias_normalized: group[["ticker", "price_ticker", "company_name", "alias_type"]].to_dict(orient="records")
            for alias_normalized, group in grouped
            if alias_normalized
        }

    def resolve(self, candidate: str) -> dict[str, Any]:
        """Resolve one extracted target string."""
        raw_value = str(candidate or "").strip()
        if not raw_value:
            return {
                "input_target": raw_value,
                "normalized_target": "",
                "status": "empty",
                "is_resolved": False,
                "resolved_ticker": "",
                "resolved_price_ticker": "",
                "resolved_company_name": "",
                "match_type": "",
            }

        upper = raw_value.upper()
        if self.config.allow_exact_ticker and upper in self._ticker_lookup:
            resolved = self._ticker_lookup[upper]
            return {
                "input_target": raw_value,
                "normalized_target": normalize_alias_text(raw_value),
                "status": "resolved",
                "is_resolved": True,
                "resolved_ticker": resolved["ticker"],
                "resolved_price_ticker": resolved["price_ticker"],
                "resolved_company_name": resolved["company_name"],
                "match_type": "exact_ticker",
            }

        normalized = normalize_alias_text(raw_value)
        if not normalized:
            return {
                "input_target": raw_value,
                "normalized_target": "",
                "status": "empty",
                "is_resolved": False,
                "resolved_ticker": "",
                "resolved_price_ticker": "",
                "resolved_company_name": "",
                "match_type": "",
            }

        matches = self._alias_lookup.get(normalized, [])
        unique_matches: dict[str, dict[str, Any]] = {}
        for match in matches:
            ticker = str(match.get("ticker", "")).strip()
            if ticker and ticker not in unique_matches:
                unique_matches[ticker] = {
                    "ticker": ticker,
                    "price_ticker": str(match.get("price_ticker", "")).strip(),
                    "company_name": str(match.get("company_name", "")).strip(),
                }

        if len(unique_matches) == 1:
            resolved = next(iter(unique_matches.values()))
            return {
                "input_target": raw_value,
                "normalized_target": normalized,
                "status": "resolved",
                "is_resolved": True,
                "resolved_ticker": resolved["ticker"],
                "resolved_price_ticker": resolved["price_ticker"],
                "resolved_company_name": resolved["company_name"],
                "match_type": "exact_alias",
            }

        if len(unique_matches) > 1:
            return {
                "input_target": raw_value,
                "normalized_target": normalized,
                "status": "ambiguous",
                "is_resolved": False,
                "resolved_ticker": "",
                "resolved_price_ticker": "",
                "resolved_company_name": "",
                "match_type": "ambiguous_alias",
            }

        return {
            "input_target": raw_value,
            "normalized_target": normalized,
            "status": "unresolved",
            "is_resolved": False,
            "resolved_ticker": "",
            "resolved_price_ticker": "",
            "resolved_company_name": "",
            "match_type": "no_match",
        }

    def resolve_many(self, candidates: list[str]) -> list[dict[str, Any]]:
        """Resolve many extracted target strings in order."""
        return [self.resolve(candidate) for candidate in candidates]

"""Generate an initial alias seed table from the fixed universe snapshot."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.common.config import load_yaml, resolve_from_root


DEFAULT_PATHS_CONFIG = "configs/paths.yaml"
DEFAULT_DATA_CONFIG = "configs/data/universe.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths-config", default=DEFAULT_PATHS_CONFIG)
    parser.add_argument("--config", default=DEFAULT_DATA_CONFIG)
    parser.add_argument("--force", action="store_true", help="Regenerate existing alias output.")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_alias_text(value: str) -> str:
    lowered = value.lower().replace("&", " and ")
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def strip_suffixes(company_name: str, suffixes: set[str]) -> str:
    tokens = re.findall(r"[A-Za-z0-9&]+", company_name)
    while tokens and tokens[-1].lower() in suffixes:
        tokens.pop()
    return " ".join(tokens).strip()


def punctuation_stripped_variant(company_name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9& ]+", " ", company_name)
    return re.sub(r"\s+", " ", cleaned).strip()


def generate_alias_rows(universe_frame: pd.DataFrame, suffixes: set[str], include_ticker_alias: bool) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()

    def add_alias(row: pd.Series, alias: str, alias_type: str, is_primary: bool) -> None:
        alias = alias.strip()
        if not alias:
            return
        alias_normalized = normalize_alias_text(alias)
        key = (row["ticker"], alias_normalized)
        if not alias_normalized or key in seen_keys:
            return
        seen_keys.add(key)
        records.append(
            {
                "ticker": row["ticker"],
                "price_ticker": row.get("price_ticker", ""),
                "company_name": row["company_name"],
                "cik": row.get("cik", ""),
                "alias": alias,
                "alias_normalized": alias_normalized,
                "alias_type": alias_type,
                "is_primary": is_primary,
            }
        )

    for _, row in universe_frame.iterrows():
        company_name = str(row["company_name"]).strip()
        add_alias(row, company_name, "company_name", True)

        stripped = strip_suffixes(company_name, suffixes)
        if stripped and stripped != company_name:
            add_alias(row, stripped, "company_name_no_suffix", False)

        punctuation_variant = punctuation_stripped_variant(company_name)
        if punctuation_variant and punctuation_variant != company_name:
            add_alias(row, punctuation_variant, "company_name_no_punctuation", False)

        ampersand_variant = company_name.replace("&", "and")
        if ampersand_variant != company_name:
            add_alias(row, ampersand_variant, "company_name_ampersand", False)

        if include_ticker_alias:
            add_alias(row, str(row["ticker"]).strip().upper(), "ticker", False)

    alias_frame = pd.DataFrame.from_records(records).sort_values(["ticker", "alias_type", "alias"]).reset_index(drop=True)
    return alias_frame


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    paths_config = load_yaml(args.paths_config)
    data_config = load_yaml(args.config)

    universe_root = resolve_from_root(paths_config["paths"]["raw_universe_root"])
    mapping_root = resolve_from_root(paths_config["paths"]["raw_mapping_root"])
    universe_file = universe_root / data_config["universe"]["output_filename"]
    output_file = mapping_root / data_config["mapping"]["output_filename"]
    metadata_file = mapping_root / data_config["mapping"]["metadata_filename"]

    if output_file.exists() and not args.force and data_config["download"]["skip_existing"]:
        print(f"Alias seed already exists, skipping: {output_file}")
        return 0

    if not universe_file.exists():
        raise FileNotFoundError(f"Universe file not found. Run download_universe.py first: {universe_file}")

    universe_frame = pd.read_csv(universe_file, dtype=str).fillna("")
    suffixes = {suffix.lower() for suffix in data_config["mapping"]["corporate_suffixes_to_strip"]}
    alias_frame = generate_alias_rows(
        universe_frame,
        suffixes=suffixes,
        include_ticker_alias=bool(data_config["mapping"]["include_ticker_alias"]),
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    alias_frame.to_csv(output_file, index=False)

    metadata = {
        "source_name": "derived_from_fixed_universe",
        "source_version": "stage1_alias_seed_v1",
        "created_at": utc_now_iso(),
        "universe_file": str(universe_file),
        "row_count": int(len(alias_frame)),
        "ticker_count": int(alias_frame["ticker"].nunique()),
        "notes": [
            "This file is an initial alias seed generated from company_name and ticker.",
            "Manual review and later enrichment remain out of scope for stage 1.",
        ],
    }
    write_json(metadata_file, metadata)
    print(f"Wrote alias seed to {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

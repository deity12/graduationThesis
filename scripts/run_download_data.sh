#!/usr/bin/env bash
set -euo pipefail

python -m src.data.download_universe
python -m src.data.download_prices
python -m src.mapping.alias_table

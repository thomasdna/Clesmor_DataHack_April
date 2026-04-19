#!/usr/bin/env python3
"""
Download Places OS rows from Hugging Face (remote parquet scan) for each global-café metro:
country ISO + bounding box → ``data/interim/places_<metro>.parquet`` (canonical columns).

Requires HF_TOKEN and network. Release ``--dt`` must match a folder under the dataset
(e.g. ``2026-04-14``).

Examples::

  python scripts/fetch_global_cafe_metros_remote.py --dt 2026-04-14
  python scripts/fetch_global_cafe_metros_remote.py --dt 2026-04-14 --metros singapore,seoul --force
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.cities import BUILD_CITY_PROFILES, GLOBAL_CAFE_METRO_COUNTRY, parse_global_cafe_metros
from src.data.hf_places_remote import fetch_metro_subset_from_hf, load_hf_token


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dt", required=True, help="Release date folder, e.g. 2026-04-14")
    ap.add_argument(
        "--interim-dir",
        type=Path,
        default=Path("data/interim"),
        help="Output directory for places_<metro>.parquet",
    )
    ap.add_argument(
        "--metros",
        default="all",
        help="Comma-separated metro keys (default: all six GLOBAL_CAFE_DEMO_KEYS)",
    )
    ap.add_argument("--force", action="store_true", help="Overwrite existing parquet files")
    ap.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap per metro (0 = no cap)",
    )
    args = ap.parse_args()

    try:
        keys = parse_global_cafe_metros(args.metros)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    token = load_hf_token()

    for key in keys:

        out_path = args.interim_dir / f"places_{key}.parquet"
        if out_path.exists() and not args.force:
            print(f"Skip (exists): {out_path}")
            continue

        country = GLOBAL_CAFE_METRO_COUNTRY[key]
        bbox = BUILD_CITY_PROFILES[key].bbox
        print(f"Fetching {key} ({country}) bbox={bbox}…")
        n = fetch_metro_subset_from_hf(
            args.dt,
            token,
            country,
            bbox,
            out_path,
            max_rows=args.max_rows,
        )
        print(f"  Wrote {out_path} rows={n:,}")


if __name__ == "__main__":
    main()

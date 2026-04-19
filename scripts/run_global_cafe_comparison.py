#!/usr/bin/env python3
"""
Global café “opportunity structure” comparison across six metros.

This does **not** estimate dollar ROI or true economic opportunity cost (rent, labor,
permits, COGS). It compares **relative POI-based opportunity** using the same
café taxonomy and scoring weights everywhere, with AU-only layers disabled so
every metro is judged on POI structure only.

Data: for each metro key, expects `data/interim/places_<key>.parquet`, built from
the relevant country Foursquare OS extract, e.g.:

  ./.venv/bin/python scripts/build_city_subset.py --city sydney --in data/interim/places_au.parquet
  ./.venv/bin/python scripts/build_city_subset.py --city singapore --in data/interim/places_sg.parquet
  ...

Scores are computed **once** on the concatenated H3 cells so min–max normalization
is global (comparable across cities). Missing metro files are skipped with a warning.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.business_taxonomies import BUSINESS_TEMPLATES
from src.config.cities import BUILD_CITY_PROFILES, GLOBAL_CAFE_DEMO_KEYS, parse_global_cafe_metros
from src.features.area_units import AreaUnit
from src.features.build_opportunity_features import FeatureBuildInputs, build_area_features
from src.scoring.opportunity_score import score_area_features


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--interim-dir",
        type=Path,
        default=Path("data/interim"),
        help="Directory containing places_<metro>.parquet",
    )
    ap.add_argument(
        "--categories",
        type=Path,
        default=Path("data/interim/categories_clean.parquet"),
        help="Categories parquet for taxonomy join",
    )
    ap.add_argument(
        "--min-active-poi",
        type=int,
        default=80,
        help="Minimum active_poi_count per H3 cell to include in metro summary stats",
    )
    ap.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="How many top cells per metro to summarize (median score)",
    )
    ap.add_argument(
        "--metros",
        default="all",
        help="Comma-separated metro keys (e.g. singapore) or 'all'. Scores only those parquets; "
        "normalization is across loaded metros.",
    )
    args = ap.parse_args()

    try:
        selected_metros = parse_global_cafe_metros(args.metros)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    tpl = BUSINESS_TEMPLATES["cafe"]
    area_unit = AreaUnit(kind="h3", h3_resolution=8)

    frames: list[pl.DataFrame] = []
    missing: list[str] = []

    for key in selected_metros:
        path = args.interim_dir / f"places_{key}.parquet"
        if not path.exists():
            missing.append(str(path))
            continue
        feats = build_area_features(
            FeatureBuildInputs(
                places_sydney_path=path,
                categories_clean_path=args.categories,
                taxonomy=tpl.taxonomy,
                template_key="cafe",
                area_unit=area_unit,
                include_transit_access=False,
                include_market_fit=False,
                include_rent_affordability=False,
                include_suburb_labels=False,
            )
        )
        frames.append(feats.with_columns(pl.lit(key).alias("metro_key")))

    if not frames:
        raise SystemExit(
            "No metro parquet files found for --metros selection. Build subsets first, e.g.\n"
            + "\n".join(f"  - {args.interim_dir}/places_{k}.parquet" for k in selected_metros)
        )

    if missing:
        print("Warning: missing (skipped):", file=sys.stderr)
        for m in missing:
            print(f"  {m}", file=sys.stderr)

    combined = pl.concat(frames)
    ranked = score_area_features(combined, tpl.weights)

    out_dir = Path("data/exports")
    out_dir.mkdir(parents=True, exist_ok=True)
    ranked_path = out_dir / "global_cafe_areas_ranked.parquet"
    ranked.write_parquet(ranked_path)

    # Per-metro summary: median of top-N scores among cells with enough POIs.
    summary_rows: list[dict] = []
    for key in selected_metros:
        path = args.interim_dir / f"places_{key}.parquet"
        if not path.exists():
            summary_rows.append(
                {
                    "metro_key": key,
                    "label": BUILD_CITY_PROFILES[key].label,
                    "status": "missing_parquet",
                    "n_cells_qualified": 0,
                    "median_top_score": None,
                    "max_score": None,
                }
            )
            continue
        sub = ranked.filter((pl.col("metro_key") == key) & (pl.col("active_poi_count") >= args.min_active_poi))
        if sub.height == 0:
            summary_rows.append(
                {
                    "metro_key": key,
                    "label": BUILD_CITY_PROFILES[key].label,
                    "status": "no_qualified_cells",
                    "n_cells_qualified": 0,
                    "median_top_score": None,
                    "max_score": None,
                }
            )
            continue
        top = sub.sort("opportunity_score", descending=True).head(args.top_n)
        summary_rows.append(
            {
                "metro_key": key,
                "label": BUILD_CITY_PROFILES[key].label,
                "status": "ok",
                "n_cells_qualified": sub.height,
                "median_top_score": float(top["opportunity_score"].median()),
                "max_score": float(top["opportunity_score"].max()),
            }
        )

    summary = pl.DataFrame(summary_rows).sort("median_top_score", descending=True, nulls_last=True)
    csv_path = out_dir / "global_cafe_metro_summary.csv"
    summary.write_csv(csv_path)

    print("Wrote:", ranked_path)
    print("Wrote:", csv_path)
    print()
    print(summary)


if __name__ == "__main__":
    main()

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import polars as pl

import argparse

from src.config.business_taxonomies import BUSINESS_TEMPLATES
from src.config.cities import CITIES
from src.features.area_units import AreaUnit
from src.features.build_opportunity_features import FeatureBuildInputs, build_area_features
from src.scoring.opportunity_score import OpportunityWeights, score_area_features, top_reasons
from scripts.build_market_fit_assets import main as build_market_fit_assets


def main() -> None:
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("data/exports").mkdir(parents=True, exist_ok=True)
    Path("docs").mkdir(parents=True, exist_ok=True)
    Path("data/raw/nsw_transport").mkdir(parents=True, exist_ok=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--template", default="gym", help="Business template key (gym, cafe, clinic, coworking)")
    ap.add_argument("--all", action="store_true", help="Build all templates (demo mode)")
    ap.add_argument("--city", default="sydney", choices=sorted(CITIES.keys()), help="City profile key")
    args = ap.parse_args()

    keys = list(BUSINESS_TEMPLATES.keys()) if args.all else [args.template]
    city = CITIES[args.city]
    places_path = Path(f"data/interim/places_{city.key}.parquet")
    if not places_path.exists():
        raise SystemExit(
            f"Missing {places_path}. Build it with: ./.venv/bin/python scripts/build_city_subset.py --city {city.key}"
        )

    for k in keys:
        if k not in BUSINESS_TEMPLATES:
            raise SystemExit(f"Unknown template: {k}. Available: {sorted(BUSINESS_TEMPLATES.keys())}")
        tpl = BUSINESS_TEMPLATES[k]
        features = build_area_features(
            FeatureBuildInputs(
                places_sydney_path=places_path,
                template_key=k,
                taxonomy=tpl.taxonomy,
                area_unit=AreaUnit(kind="h3", h3_resolution=8),
                include_transit_access=True,
                include_market_fit=True,
                include_rent_affordability=True,
            )
        )
        features_path = Path(f"data/processed/{city.key}_area_features_{k}.parquet")
        features.write_parquet(features_path)

        ranked = score_area_features(features, tpl.weights)

        # Guardrails for demo stability: avoid tiny cells dominating normalization.
        ranked = ranked.filter(pl.col("active_poi_count") >= 200)
        ranked = ranked.with_columns(
            pl.struct(ranked.columns)
            .map_elements(lambda r: "; ".join(top_reasons(r)), return_dtype=pl.Utf8)
            .alias("reasons_top3")
        )
        ranked_path = Path(f"data/processed/{city.key}_ranked_areas_{k}.parquet")
        ranked.write_parquet(ranked_path)

        top_csv = Path(f"data/exports/top_ranked_areas_{city.key}_{k}.csv")
        ranked.head(50).write_csv(top_csv)

    # Docs previews (gym-focused snapshot for consistency)
    (Path("docs") / "feature_summary.md").write_text(
        "# Feature summary (Sydney)\n\n"
        "- This file is a lightweight preview.\n"
        "- Use `data/processed/sydney_area_features_<template>.parquet` for template-specific outputs.\n"
    )
    (Path("docs") / "top_areas_preview.md").write_text(
        "# Top areas preview (H3)\n\n"
        "Run `scripts/run_build_features.py --all` and inspect `data/exports/top_ranked_areas_<template>.csv`.\n"
    )

    # Optional: build SA2-level market fit GeoJSON for choropleth map in the app.
    try:
        build_market_fit_assets(city_key=city.key)
    except Exception as e:
        # Do not fail the entire build for a demo-optional artifact.
        print("Warning: market fit SA2 assets build failed:", e)

    print("Wrote:")
    for k in keys:
        print(" -", f"data/processed/{city.key}_area_features_{k}.parquet")
        print(" -", f"data/processed/{city.key}_ranked_areas_{k}.parquet")
        print(" -", f"data/exports/top_ranked_areas_{city.key}_{k}.csv")
    print(" - docs/feature_summary.md")
    print(" - docs/top_areas_preview.md")


if __name__ == "__main__":
    main()


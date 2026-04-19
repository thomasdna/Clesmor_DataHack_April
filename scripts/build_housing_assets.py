from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.geo import SYDNEY_BBOX
from src.features.dcj_housing_market import (
    DCJHousingMarketConfig,
    load_dcj_rent_by_postcode,
    load_dcj_sales_by_postcode,
)
from src.features.poa_sa2_correspondence import build_poa_to_sa2_correspondence


def _weighted_mean(value: pl.Expr, weight: pl.Expr) -> pl.Expr:
    return (value * weight).sum() / pl.when(weight.sum() <= 0).then(1.0).otherwise(weight.sum())


def build_housing_by_sa2() -> pl.DataFrame:
    """
    Build SA2-level housing market signals from DCJ postcode tables.

    Notes:
    - Rent/sales are published by postcode. We map postcode→SA2 via POA centroid as a demo-friendly approximation.
    - Aggregation uses weighted mean where counts are available; otherwise unweighted mean.
    """
    rent = load_dcj_rent_by_postcode(DCJHousingMarketConfig())
    sales = load_dcj_sales_by_postcode(DCJHousingMarketConfig())

    # Statistically safer mapping than centroid: MB-count-based POA→SA2 correspondence from ABS allocation files.
    poa_sa2 = build_poa_to_sa2_correspondence()

    rent_sa2 = (
        rent.join(poa_sa2, on="poa_code_2021", how="inner")
        .group_by("sa2_code_2021")
        .agg(
            [
                _weighted_mean(
                    pl.col("median_weekly_rent_total").fill_null(0.0),
                    (pl.col("bonds_lodged_total").fill_null(0) * pl.col("mb_ratio_from_poa_to_sa2")),
                ).alias("median_weekly_rent_total_w"),
                (pl.col("bonds_lodged_total").fill_null(0) * pl.col("mb_ratio_from_poa_to_sa2")).sum().alias(
                    "bonds_lodged_total_w"
                ),
            ]
        )
    )

    sales_sa2 = (
        sales.join(poa_sa2, on="poa_code_2021", how="inner")
        .group_by("sa2_code_2021")
        .agg(
            [
                _weighted_mean(
                    pl.col("median_sale_price_total").fill_null(0.0),
                    (pl.col("sales_total").fill_null(0) * pl.col("mb_ratio_from_poa_to_sa2")),
                ).alias("median_sale_price_total_w"),
                (pl.col("sales_total").fill_null(0) * pl.col("mb_ratio_from_poa_to_sa2")).sum().alias("sales_total_w"),
            ]
        )
    )

    out = rent_sa2.join(sales_sa2, on="sa2_code_2021", how="full")
    return out


def main() -> None:
    # Load the existing SA2 geojson (market fit) and attach housing properties.
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="sydney", help="City key (sydney, melbourne)")
    args = ap.parse_args()

    sa2_geo = Path(f"data/processed/{args.city}_sa2_market_fit.geojson")
    if not sa2_geo.exists():
        raise SystemExit(
            f"Missing {sa2_geo}. Run: ./.venv/bin/python scripts/build_market_fit_assets.py (via run_build_features) first."
        )

    fc = json.loads(sa2_geo.read_text(encoding="utf-8"))
    housing = build_housing_by_sa2()
    hmap = (
        {int(r["sa2_code_2021"]): r for r in housing.filter(pl.col("sa2_code_2021").is_not_null()).to_dicts()}
        if housing.height
        else {}
    )

    # Attach to properties
    for f in fc.get("features", []):
        p = f.get("properties", {})
        code = p.get("sa2_code_2021")
        row = hmap.get(int(code)) if code is not None else None
        if row:
            p["median_weekly_rent_total"] = row.get("median_weekly_rent_total_w")
            p["bonds_lodged_total"] = row.get("bonds_lodged_total_w")
            p["median_sale_price_total"] = row.get("median_sale_price_total_w")
            p["sales_total"] = row.get("sales_total_w")
        else:
            p["median_weekly_rent_total"] = None
            p["bonds_lodged_total"] = None
            p["median_sale_price_total"] = None
            p["sales_total"] = None

    out_path = Path(f"data/processed/{args.city}_sa2_housing_market.geojson")
    out_path.write_text(json.dumps(fc), encoding="utf-8")
    print("Wrote:", out_path)


if __name__ == "__main__":
    main()


from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.cities import BUILD_CITY_PROFILES


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="sydney", choices=sorted(BUILD_CITY_PROFILES.keys()))
    ap.add_argument("--in", dest="in_path", default="data/interim/places_au.parquet")
    ap.add_argument("--out", dest="out_path", default=None)
    args = ap.parse_args()

    city = BUILD_CITY_PROFILES[args.city]
    in_path = Path(args.in_path)
    if not in_path.exists():
        raise SystemExit(f"Missing input parquet: {in_path}")

    out_path = Path(args.out_path) if args.out_path else Path(f"data/interim/places_{city.key}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    b = city.bbox
    lf = pl.scan_parquet(str(in_path))
    # column names are canonicalized in our interim artifacts: lat/lon
    lf = lf.filter(
        (pl.col("lat") >= b.min_lat)
        & (pl.col("lat") <= b.max_lat)
        & (pl.col("lon") >= b.min_lon)
        & (pl.col("lon") <= b.max_lon)
    )
    df = lf.collect(engine="streaming")
    df.write_parquet(out_path)
    print("Wrote:", out_path, "rows:", df.height)


if __name__ == "__main__":
    main()


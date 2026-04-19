import argparse
from pathlib import Path

import polars as pl


AU_BBOX = {
    "min_lat": -44.0,
    "max_lat": -10.0,
    "min_lon": 112.0,
    "max_lon": 154.0,
}


def _pick_col(cols: list[str], candidates: list[str]) -> str | None:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="Release date, e.g. 2026-04-14")
    parser.add_argument("--raw-root", default="data/raw", help="Raw root folder")
    parser.add_argument("--out-places", default="data/au_places.parquet")
    parser.add_argument("--out-categories", default="data/categories.parquet")
    args = parser.parse_args()

    raw_root = Path(args.raw_root)
    places_dir = raw_root / "release" / f"dt={args.dt}" / "places" / "parquet"
    cats_dir = raw_root / "release" / f"dt={args.dt}" / "categories" / "parquet"

    if not places_dir.exists():
        raise SystemExit(f"Places parquet not found at {places_dir}. Run download script first.")

    place_files = sorted(places_dir.glob("*.parquet"))
    if not place_files:
        place_files = sorted(places_dir.rglob("*.parquet"))
    if not place_files:
        raise SystemExit(f"No parquet files found under {places_dir}")

    lf = pl.scan_parquet([str(p) for p in place_files])
    cols = lf.collect_schema().names()

    lat_col = _pick_col(cols, ["latitude", "lat"])
    lon_col = _pick_col(cols, ["longitude", "lon", "lng"])
    country_col = _pick_col(cols, ["country", "country_code", "iso_country"])

    if not (lat_col and lon_col):
        raise SystemExit(
            f"Could not find lat/lon columns. Available columns include: {cols[:50]} ..."
        )

    if country_col:
        au_filter = pl.col(country_col).cast(pl.Utf8).str.to_uppercase() == "AU"
    else:
        au_filter = (
            (pl.col(lat_col) >= AU_BBOX["min_lat"])
            & (pl.col(lat_col) <= AU_BBOX["max_lat"])
            & (pl.col(lon_col) >= AU_BBOX["min_lon"])
            & (pl.col(lon_col) <= AU_BBOX["max_lon"])
        )

    out_places = Path(args.out_places)
    out_places.parent.mkdir(parents=True, exist_ok=True)

    # Keep all columns initially; we can slim later once the app logic stabilizes.
    print("Filtering places to Australia (this may take a bit)...")
    lf.filter(au_filter).sink_parquet(str(out_places))
    print(f"Wrote {out_places}")

    if cats_dir.exists():
        cat_files = sorted(cats_dir.glob("*.parquet"))
        if not cat_files:
            cat_files = sorted(cats_dir.rglob("*.parquet"))
        if cat_files:
            out_cats = Path(args.out_categories)
            out_cats.parent.mkdir(parents=True, exist_ok=True)
            print("Combining categories parquet...")
            pl.scan_parquet([str(p) for p in cat_files]).sink_parquet(str(out_cats))
            print(f"Wrote {out_cats}")
        else:
            print("No categories parquet files found; skipping categories output.")
    else:
        print("Categories folder not found; skipping categories output.")


if __name__ == "__main__":
    main()


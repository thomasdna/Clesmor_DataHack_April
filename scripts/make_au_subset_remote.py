import argparse
import os
from pathlib import Path
import shutil
import random

from dotenv import load_dotenv
from huggingface_hub import HfFileSystem

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq


AU_BBOX = {
    "min_lat": -44.0,
    "max_lat": -10.0,
    "min_lon": 112.0,
    "max_lon": 154.0,
}


def _pick(schema: pa.Schema, candidates: list[str]) -> str | None:
    names = set(schema.names)
    for c in candidates:
        if c in names:
            return c
    return None


def _hf_dataset(root: str, token: str) -> tuple[HfFileSystem, pafs.FileSystem]:
    fs = HfFileSystem(token=token)
    arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(fs))
    return fs, arrow_fs


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="Release date, e.g. 2026-04-14")
    parser.add_argument("--out-places", default="data/au_places.parquet")
    parser.add_argument("--out-categories", default="data/categories.parquet")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap for faster prototyping (0 = no cap).",
    )
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN")
    if not token:
        raise SystemExit("HF_TOKEN missing. Add it to .env or export it in your shell.")

    root = "datasets/foursquare/fsq-os-places"
    places_dir = f"{root}/release/dt={args.dt}/places/parquet"
    cats_dir = f"{root}/release/dt={args.dt}/categories/parquet"

    fs, arrow_fs = _hf_dataset(root, token)

    # Build a dataset over all parquet shards (remote, range-read via fsspec).
    place_files = [x["name"] for x in fs.ls(places_dir) if x["type"] == "file" and x["name"].endswith(".parquet")]
    if not place_files:
        raise SystemExit(f"No parquet files found at {places_dir}")

    places = ds.dataset(place_files, filesystem=arrow_fs, format="parquet")
    schema = places.schema

    lat_col = _pick(schema, ["latitude", "lat"])
    lon_col = _pick(schema, ["longitude", "lon", "lng"])
    country_col = _pick(schema, ["country", "country_code", "iso_country"])

    if not (lat_col and lon_col):
        raise SystemExit(f"Could not find lat/lon columns in schema. Columns: {schema.names[:50]} ...")

    # Filter expression (prefer explicit country, fallback to bbox).
    expr = (
        (ds.field(lat_col) >= AU_BBOX["min_lat"])
        & (ds.field(lat_col) <= AU_BBOX["max_lat"])
        & (ds.field(lon_col) >= AU_BBOX["min_lon"])
        & (ds.field(lon_col) <= AU_BBOX["max_lon"])
    )
    if country_col:
        # Some releases may use lowercase/uppercase; normalize by comparing uppercase if possible.
        # Arrow doesn't have uppercase for all types; keep simple equality + bbox to be safe.
        expr = expr & (ds.field(country_col) == "AU")

    # Select a lean set of columns for the MVP.
    keep = []
    for c in ["fsq_place_id", "name", lat_col, lon_col, country_col, "categories", "fsq_category_ids", "category_ids"]:
        if c and c in schema.names and c not in keep:
            keep.append(c)
    # Ensure lat/lon included.
    for c in [lat_col, lon_col]:
        if c not in keep:
            keep.append(c)

    out_places = Path(args.out_places)
    out_places.parent.mkdir(parents=True, exist_ok=True)
    # If a previous run created a directory at this path, remove it.
    if out_places.exists() and out_places.is_dir():
        shutil.rmtree(out_places)

    if args.max_rows and args.max_rows > 0:
        # Important: scanning in file order can bias geography. Shuffle shards so the cap
        # yields a more representative Australia sample (incl. east coast).
        rng = random.Random(42)
        shuffled = list(place_files)
        rng.shuffle(shuffled)

        batches: list[pa.RecordBatch] = []
        total = 0
        for f in shuffled:
            part = ds.dataset([f], filesystem=arrow_fs, format="parquet")
            scanner = part.scanner(filter=expr, columns=keep, batch_size=128 * 1024)
            for b in scanner.to_batches():
                batches.append(b)
                total += b.num_rows
                if total >= args.max_rows:
                    break
            if total >= args.max_rows:
                break

        table = pa.Table.from_batches(batches)
        if table.num_rows > args.max_rows:
            table = table.slice(0, args.max_rows)
    else:
        scanner = places.scanner(filter=expr, columns=keep, batch_size=128 * 1024)
        table = scanner.to_table()

    # Write a single-file parquet (Streamlit app expects a file path).
    single_file = out_places if out_places.suffix == ".parquet" else out_places.with_suffix(".parquet")
    pq.write_table(table, str(single_file))

    print(f"Wrote {single_file} rows={table.num_rows:,} cols={len(table.column_names)}")

    # Categories (single parquet file typically).
    try:
        cat_files = [x["name"] for x in fs.ls(cats_dir) if x["type"] == "file" and x["name"].endswith(".parquet")]
        if cat_files:
            cats = ds.dataset(cat_files, filesystem=arrow_fs, format="parquet").to_table()
            out_cats = Path(args.out_categories)
            out_cats.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(cats, str(out_cats))
            print(f"Wrote {out_cats} rows={cats.num_rows:,}")
    except Exception as e:
        print(f"Skipping categories due to: {e}")


if __name__ == "__main__":
    main()


"""
Remote Hugging Face Foursquare OS Places helpers (range-read parquet shards).
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import polars as pl

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem

from src.config.geo import BBox
from src.data.schema import canonical_places_lazyframe


def _pick(schema: pa.Schema, candidates: list[str]) -> str | None:
    names = set(schema.names)
    for c in candidates:
        if c in names:
            return c
    return None


def hf_places_dataset(dt: str, token: str) -> tuple[ds.Dataset, pa.Schema, str, str, str | None]:
    """Scan all place parquet shards for release ``dt`` (remote)."""
    root = "datasets/foursquare/fsq-os-places"
    places_dir = f"{root}/release/dt={dt}/places/parquet"

    fs = HfFileSystem(token=token)
    arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(fs))

    place_files = [x["name"] for x in fs.ls(places_dir) if x["type"] == "file" and x["name"].endswith(".parquet")]
    if not place_files:
        raise FileNotFoundError(f"No parquet files under {places_dir}")

    places = ds.dataset(place_files, filesystem=arrow_fs, format="parquet")
    schema = places.schema
    lat_col = _pick(schema, ["latitude", "lat"])
    lon_col = _pick(schema, ["longitude", "lon", "lng"])
    country_col = _pick(schema, ["country", "country_code", "iso_country"])
    if not lat_col or not lon_col:
        raise RuntimeError(f"Missing lat/lon in schema. Got: {schema.names[:40]}")
    return places, schema, lat_col, lon_col, country_col


def _keep_columns(schema: pa.Schema, lat_col: str, lon_col: str, country_col: str | None) -> list[str]:
    keep: list[str] = []
    for c in ["fsq_place_id", "name", lat_col, lon_col, country_col, "categories", "fsq_category_ids", "category_ids"]:
        if c and c in schema.names and c not in keep:
            keep.append(c)
    for c in [lat_col, lon_col]:
        if c not in keep:
            keep.append(c)
    return keep


def metro_bbox_filter(
    country_iso2: str,
    bbox: BBox,
    lat_col: str,
    lon_col: str,
    country_col: str | None,
) -> ds.Expression:
    expr = (
        (ds.field(lat_col) >= bbox.min_lat)
        & (ds.field(lat_col) <= bbox.max_lat)
        & (ds.field(lon_col) >= bbox.min_lon)
        & (ds.field(lon_col) <= bbox.max_lon)
    )
    if country_col:
        expr = expr & (ds.field(country_col) == country_iso2)
    return expr


def arrow_table_to_canonical_parquet(table: pa.Table, out_path: Path) -> int:
    """Write Arrow table to a temp parquet, then re-export canonical place columns (lat/lon/place_id)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if table.num_rows == 0:
        pl.DataFrame(
            {
                "place_id": pl.Series([], dtype=pl.Utf8),
                "name": pl.Series([], dtype=pl.Utf8),
                "lat": pl.Series([], dtype=pl.Float64),
                "lon": pl.Series([], dtype=pl.Float64),
                "country": pl.Series([], dtype=pl.Utf8),
                "category_ids": pl.Series([], dtype=pl.Null),
            }
        ).write_parquet(out_path)
        return 0

    fd, raw = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    tmp_path = Path(raw)
    try:
        pq.write_table(table, str(tmp_path))
        lf, _ = canonical_places_lazyframe(tmp_path)
        df = lf.collect()
        df.write_parquet(out_path)
        return df.height
    finally:
        tmp_path.unlink(missing_ok=True)


def fetch_metro_subset_from_hf(
    dt: str,
    token: str,
    country_iso2: str,
    bbox: BBox,
    out_path: Path,
    *,
    max_rows: int = 0,
) -> int:
    """
    Filter remote Places OS shards by country + bbox; write canonical ``places_*.parquet``.

    Returns row count written.
    """
    places, schema, lat_col, lon_col, country_col = hf_places_dataset(dt, token)
    keep = _keep_columns(schema, lat_col, lon_col, country_col)
    expr = metro_bbox_filter(country_iso2, bbox, lat_col, lon_col, country_col)
    scanner = places.scanner(filter=expr, columns=keep, batch_size=128 * 1024)
    table = scanner.to_table()
    if max_rows and table.num_rows > max_rows:
        table = table.slice(0, max_rows)
    return arrow_table_to_canonical_parquet(table, Path(out_path))


def load_hf_token() -> str:
    load_dotenv()
    token = os.getenv("HF_TOKEN")
    if not token:
        raise RuntimeError("HF_TOKEN missing. Set it in .env for Hugging Face dataset access.")
    return token

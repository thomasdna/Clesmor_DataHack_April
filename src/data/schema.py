from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class PlacesSchema:
    place_id: str
    name: str
    lat: str
    lon: str
    country: str | None
    category_ids: str | None  # list column


def inspect_places_schema(parquet_path: Path) -> PlacesSchema:
    lf = pl.scan_parquet(str(parquet_path))
    cols = lf.collect_schema().names()
    s = set(cols)

    def pick(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in s:
                return c
        return None

    place_id = pick(["fsq_place_id", "place_id", "id"])
    name = pick(["name"])
    lat = pick(["latitude", "lat"])
    lon = pick(["longitude", "lon", "lng"])
    country = pick(["country", "country_code", "iso_country"])
    category_ids = pick(["fsq_category_ids", "category_ids"])

    missing = [k for k, v in {"place_id": place_id, "name": name, "lat": lat, "lon": lon}.items() if not v]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Available columns: {cols}")

    return PlacesSchema(
        place_id=place_id, name=name, lat=lat, lon=lon, country=country, category_ids=category_ids
    )


def canonical_places_lazyframe(parquet_path: Path) -> tuple[pl.LazyFrame, PlacesSchema]:
    schema = inspect_places_schema(parquet_path)
    lf = pl.scan_parquet(str(parquet_path)).select(
        [
            pl.col(schema.place_id).alias("place_id"),
            pl.col(schema.name).alias("name"),
            pl.col(schema.lat).alias("lat"),
            pl.col(schema.lon).alias("lon"),
            (pl.col(schema.country).alias("country") if schema.country else pl.lit(None).alias("country")),
            (pl.col(schema.category_ids).alias("category_ids") if schema.category_ids else pl.lit(None).alias("category_ids")),
        ]
    )
    return lf, schema


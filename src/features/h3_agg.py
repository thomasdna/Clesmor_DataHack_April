from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import h3
import polars as pl


@dataclass(frozen=True)
class H3AggResult:
    areas: pl.DataFrame
    area_category_counts: pl.DataFrame  # h3, category_id, cnt
    area_pois: pl.DataFrame  # h3, place_id, name, lat, lon, category_id


def add_h3(df: pl.DataFrame, h3_res: int) -> pl.DataFrame:
    cells = [h3.latlng_to_cell(lat, lon, h3_res) for lat, lon in zip(df["lat"], df["lon"])]
    return df.with_columns(pl.Series("h3", cells))


def explode_categories(df: pl.DataFrame) -> pl.DataFrame:
    # category_ids is list[str] or null
    if "category_ids" not in df.columns:
        return df.with_columns(pl.lit(None).alias("category_id"))
    return (
        df.with_columns(pl.col("category_ids").fill_null(pl.lit([])).alias("category_ids"))
        .explode("category_ids")
        .rename({"category_ids": "category_id"})
    )


def aggregate_by_h3(
    places: pl.DataFrame,
    h3_res: int,
    keep_poi_rows: bool = True,
) -> H3AggResult:
    df = add_h3(places, h3_res)

    areas = (
        df.group_by("h3")
        .agg(
            [
                pl.len().alias("poi_count"),
                pl.col("lat").mean().alias("lat_mean"),
                pl.col("lon").mean().alias("lon_mean"),
            ]
        )
        .sort("poi_count", descending=True)
    )

    exploded = explode_categories(df.select(["h3", "place_id", "name", "lat", "lon", "category_ids"]))
    area_category_counts = (
        exploded.filter(pl.col("category_id").is_not_null())
        .group_by(["h3", "category_id"])
        .agg(pl.len().alias("cnt"))
    )

    area_pois = exploded if keep_poi_rows else pl.DataFrame()
    return H3AggResult(areas=areas, area_category_counts=area_category_counts, area_pois=area_pois)


def sum_counts_for_categories(
    area_category_counts: pl.DataFrame, category_ids: Iterable[str], out_col: str
) -> pl.DataFrame:
    ids = list(category_ids)
    if not ids:
        return area_category_counts.select(["h3"]).unique().with_columns(pl.lit(0).alias(out_col))

    return (
        area_category_counts.filter(pl.col("category_id").is_in(ids))
        .group_by("h3")
        .agg(pl.col("cnt").sum().alias(out_col))
    )


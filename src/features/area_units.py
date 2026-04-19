from __future__ import annotations

from dataclasses import dataclass

import h3
import polars as pl


@dataclass(frozen=True)
class AreaUnit:
    kind: str  # "h3" | "locality"
    h3_resolution: int | None = None
    locality_col: str | None = None


def assign_h3(places: pl.DataFrame, resolution: int) -> pl.DataFrame:
    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(places["lat"], places["lon"])]
    return places.with_columns(pl.Series("area_id", cells))


def assign_locality(places: pl.DataFrame, locality_col: str = "locality") -> pl.DataFrame:
    if locality_col not in places.columns:
        raise ValueError(f"Locality aggregation requested but `{locality_col}` is missing.")
    return places.with_columns(pl.col(locality_col).cast(pl.Utf8).alias("area_id"))


def assign_area_unit(places: pl.DataFrame, unit: AreaUnit) -> pl.DataFrame:
    if unit.kind == "h3":
        if unit.h3_resolution is None:
            raise ValueError("H3 unit requires h3_resolution")
        return assign_h3(places, unit.h3_resolution)
    if unit.kind == "locality":
        return assign_locality(places, locality_col=unit.locality_col or "locality")
    raise ValueError(f"Unknown area unit kind: {unit.kind}")


from __future__ import annotations

from dataclasses import dataclass

import polars as pl

try:
    import h3
except Exception:  # pragma: no cover
    h3 = None


@dataclass(frozen=True)
class TransitAccessConfig:
    h3_resolution: int = 8
    # We treat entrances as higher-value access points than generic facilities.
    entrance_weight: float = 2.0
    facility_weight: float = 1.0
    bus_shelter_weight: float = 0.5


def assign_transport_h3(transport_points: pl.DataFrame, h3_resolution: int) -> pl.DataFrame:
    if h3 is None:
        raise RuntimeError("h3 package is required to assign transport points to H3.")

    def to_h3(lat: float, lon: float) -> str | None:
        try:
            return h3.latlng_to_cell(lat, lon, h3_resolution)
        except Exception:
            return None

    return transport_points.with_columns(
        pl.struct(["lat", "lon"])
        .map_elements(lambda r: to_h3(r["lat"], r["lon"]), return_dtype=pl.Utf8)
        .alias("h3")
    ).drop_nulls(["h3"])


def build_transit_access_by_h3(
    transport_points: pl.DataFrame, cfg: TransitAccessConfig = TransitAccessConfig()
) -> pl.DataFrame:
    """
    Builds a simple, demo-stable H3-level accessibility proxy:
    - counts of station entrances, facilities, bus shelters per H3 cell
    - weighted sum -> log1p -> minmax normalized to 0..1
    """
    tp = assign_transport_h3(transport_points, cfg.h3_resolution)

    # Normalize kind names into buckets
    kind = pl.col("kind").cast(pl.Utf8).fill_null("facility").str.to_lowercase()
    is_entrance = kind.str.contains("entrance") | kind.eq("station_entrance")
    is_bus_shelter = kind.str.contains("bus") & kind.str.contains("shelter")
    is_facility = ~(is_entrance | is_bus_shelter)

    agg = tp.group_by("h3").agg(
        [
            is_entrance.sum().alias("station_entrance_count"),
            is_facility.sum().alias("pt_facility_count"),
            is_bus_shelter.sum().alias("bus_shelter_count"),
        ]
    )

    agg = agg.with_columns(
        (
            cfg.entrance_weight * pl.col("station_entrance_count")
            + cfg.facility_weight * pl.col("pt_facility_count")
            + cfg.bus_shelter_weight * pl.col("bus_shelter_count")
        )
        .log1p()
        .alias("transit_access_raw")
    )

    mn = pl.col("transit_access_raw").min()
    mx = pl.col("transit_access_raw").max()
    agg = agg.with_columns(
        pl.when((mx - mn) <= 1e-9)
        .then(0.0)
        .otherwise((pl.col("transit_access_raw") - mn) / (mx - mn))
        .alias("transit_access_score")
    )

    return agg


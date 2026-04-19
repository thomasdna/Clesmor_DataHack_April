from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl


@dataclass(frozen=True)
class TransportPoint:
    source: str
    lat: float
    lon: float
    name: str | None = None
    kind: str | None = None


def _as_float(s: object) -> float | None:
    try:
        if s is None:
            return None
        return float(s)  # type: ignore[arg-type]
    except Exception:
        return None


def load_station_entrances_csv(path: Path) -> pl.DataFrame:
    """
    Station entrances CSV (2018) has lat/lon columns but naming can vary.
    We detect latitude/longitude by common column names.
    """
    df = pl.read_csv(path, ignore_errors=True)
    cols = {c.lower(): c for c in df.columns}
    lat_col = cols.get("latitude") or cols.get("lat") or cols.get("y")
    lon_col = cols.get("longitude") or cols.get("lon") or cols.get("long") or cols.get("x")
    if not lat_col or not lon_col:
        raise ValueError(f"Could not detect lat/lon columns in {path}. Columns: {df.columns}")
    name_col = cols.get("station") or cols.get("stationname") or cols.get("station_name") or cols.get("name")

    out = df.select(
        [
            pl.lit("station_entrance").alias("kind"),
            pl.lit("tfnsw_station_entrances").alias("source"),
            pl.col(lat_col).cast(pl.Float64).alias("lat"),
            pl.col(lon_col).cast(pl.Float64).alias("lon"),
            (pl.col(name_col).cast(pl.Utf8) if name_col else pl.lit(None).cast(pl.Utf8)).alias("name"),
        ]
    ).drop_nulls(["lat", "lon"])
    return out


def load_location_facilities_csv(path: Path) -> pl.DataFrame:
    """
    Public Transport - Location Facilities and Operators (CSV).
    We keep only rows with coordinates.
    """
    df = pl.read_csv(path, ignore_errors=True)
    cols = {c.lower(): c for c in df.columns}
    lat_col = cols.get("latitude") or cols.get("lat")
    lon_col = cols.get("longitude") or cols.get("lon") or cols.get("long")
    if not lat_col or not lon_col:
        raise ValueError(f"Could not detect lat/lon columns in {path}. Columns: {df.columns}")
    name_col = cols.get("location") or cols.get("locationname") or cols.get("stop_name") or cols.get("name")
    kind_col = cols.get("modename") or cols.get("mode") or cols.get("transportmode") or cols.get("locationtype")

    out = df.select(
        [
            (pl.col(kind_col).cast(pl.Utf8) if kind_col else pl.lit("facility")).alias("kind"),
            pl.lit("tfnsw_location_facilities").alias("source"),
            pl.col(lat_col).cast(pl.Float64).alias("lat"),
            pl.col(lon_col).cast(pl.Float64).alias("lon"),
            (pl.col(name_col).cast(pl.Utf8) if name_col else pl.lit(None).cast(pl.Utf8)).alias("name"),
        ]
    ).drop_nulls(["lat", "lon"])
    return out


def load_bus_shelters_geojson(path: Path) -> pl.DataFrame:
    """
    City of Sydney bus shelters GeoJSON from ArcGIS.
    """
    obj = json.loads(path.read_text(encoding="utf-8"))
    feats = obj.get("features", [])
    rows: list[dict] = []
    for f in feats:
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates")
        if not coords or not isinstance(coords, list) or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        lat_f = _as_float(lat)
        lon_f = _as_float(lon)
        if lat_f is None or lon_f is None:
            continue
        props = f.get("properties") or {}
        name = props.get("LOCATION") or props.get("Name") or props.get("name")
        rows.append(
            {
                "kind": "bus_shelter",
                "source": "city_sydney_bus_shelters",
                "lat": lat_f,
                "lon": lon_f,
                "name": str(name) if name is not None else None,
            }
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame({"kind": [], "source": [], "lat": [], "lon": [], "name": []})


def load_all_transport_points(raw_dir: Path = Path("data/raw/nsw_transport")) -> pl.DataFrame:
    parts: list[pl.DataFrame] = []
    p1 = raw_dir / "station_entrances_2018.csv"
    p2 = raw_dir / "location_facilities.csv"
    p3 = raw_dir / "city_sydney_bus_shelters.geojson"

    if p1.exists():
        parts.append(load_station_entrances_csv(p1))
    if p2.exists():
        parts.append(load_location_facilities_csv(p2))
    if p3.exists():
        parts.append(load_bus_shelters_geojson(p3))

    if not parts:
        return pl.DataFrame({"kind": [], "source": [], "lat": [], "lon": [], "name": []})

    df = pl.concat(parts, how="diagonal_relaxed")
    return df.drop_nulls(["lat", "lon"])


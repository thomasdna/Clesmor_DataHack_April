from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

try:
    import shapely.geometry as geom
    import shapely.prepared as prep
except Exception:  # pragma: no cover
    geom = None
    prep = None

try:
    import shapefile  # pyshp
except Exception:  # pragma: no cover
    shapefile = None


@dataclass(frozen=True)
class SuburbConfig:
    # ABS Suburbs and Localities (SAL) 2021 boundaries
    sal_shp_path: Path = Path("data/raw/abs/sal_2021_shp/SAL_2021_AUST_GDA2020.shp")


def assign_sal_to_h3_areas(
    areas: pl.DataFrame,
    cfg: SuburbConfig = SuburbConfig(),
    lat_col: str = "lat_mean",
    lon_col: str = "lon_mean",
) -> pl.DataFrame:
    """
    Spatial join: assign each H3 centroid to an ABS SAL (Suburbs and Localities) polygon.
    Output columns:
      - sal_code_2021
      - sal_name_2021
    """
    if shapefile is None or geom is None or prep is None:
        raise RuntimeError("Missing dependencies for SAL spatial join. Install shapely and pyshp.")

    if not cfg.sal_shp_path.exists():
        return areas.with_columns(
            [pl.lit(None).cast(pl.Utf8).alias("sal_code_2021"), pl.lit(None).cast(pl.Utf8).alias("sal_name_2021")]
        )

    sf = shapefile.Reader(str(cfg.sal_shp_path))
    fields = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
    idx_code = fields.index("SAL_CODE21") if "SAL_CODE21" in fields else None
    idx_name = fields.index("SAL_NAME21") if "SAL_NAME21" in fields else None
    if idx_code is None or idx_name is None:
        return areas.with_columns(
            [pl.lit(None).cast(pl.Utf8).alias("sal_code_2021"), pl.lit(None).cast(pl.Utf8).alias("sal_name_2021")]
        )

    polys: list[tuple[str, str, tuple[float, float, float, float], object]] = []
    for sr in sf.iterShapeRecords():
        shp = sr.shape
        if shp is None or getattr(shp, "shapeType", None) == 0:
            continue
        rec = sr.record
        try:
            code = str(rec[idx_code])
        except Exception:
            continue
        name = str(rec[idx_name])
        bbox = (float(shp.bbox[0]), float(shp.bbox[1]), float(shp.bbox[2]), float(shp.bbox[3]))
        try:
            poly = geom.shape(shp.__geo_interface__)
            polys.append((code, name, bbox, prep.prep(poly)))
        except Exception:
            continue

    rows = areas.select(["h3", lat_col, lon_col]).to_dicts()
    out = []
    for r in rows:
        lat = r.get(lat_col)
        lon = r.get(lon_col)
        if lat is None or lon is None:
            out.append({"h3": r["h3"], "sal_code_2021": None, "sal_name_2021": None})
            continue
        pt = geom.Point(float(lon), float(lat))
        hit_code = None
        hit_name = None
        for code, name, bbox, ppoly in polys:
            minx, miny, maxx, maxy = bbox
            if float(lon) < minx or float(lon) > maxx or float(lat) < miny or float(lat) > maxy:
                continue
            if ppoly.contains(pt):
                hit_code = code
                hit_name = name
                break
        out.append({"h3": r["h3"], "sal_code_2021": hit_code, "sal_name_2021": hit_name})

    return areas.join(pl.DataFrame(out), on="h3", how="left")


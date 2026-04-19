from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from src.features.market_fit import MarketFitConfig, assign_sa2_to_h3_areas

try:
    import shapefile  # pyshp
except Exception as e:  # pragma: no cover
    raise SystemExit("Missing dependency pyshp. Install: pip install pyshp") from e

try:
    from shapely.geometry import shape
except Exception as e:  # pragma: no cover
    raise SystemExit("Missing dependency shapely. Install: pip install shapely") from e


@dataclass(frozen=True)
class PostcodeToSA2Config:
    poa_shp_path: Path = Path(
        "data/raw/abs_boundaries/POA_2021_AUST_GDA2020_SHP/POA_2021_AUST_GDA2020.shp"
    )


def build_poa_centroids(cfg: PostcodeToSA2Config = PostcodeToSA2Config()) -> pl.DataFrame:
    """
    Returns:
    - poa_code_2021 (Int64)
    - lat (Float64)
    - lon (Float64)

    Note: This uses polygon centroid; POAs can span multiple SA2s.
    For the demo we use centroid→SA2 as a practical approximation.
    """
    if not cfg.poa_shp_path.exists():
        return pl.DataFrame(
            {
                "poa_code_2021": pl.Series([], dtype=pl.Int64),
                "lat": pl.Series([], dtype=pl.Float64),
                "lon": pl.Series([], dtype=pl.Float64),
            }
        )

    sf = shapefile.Reader(str(cfg.poa_shp_path))
    fields = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
    idx_code = fields.index("POA_CODE21") if "POA_CODE21" in fields else None
    if idx_code is None:
        raise RuntimeError("Could not find POA_CODE21 in POA shapefile attributes.")

    rows = []
    for sr in sf.iterShapeRecords():
        shp = sr.shape
        if shp is None or getattr(shp, "shapeType", None) == 0:
            continue
        rec = sr.record
        try:
            code = int(rec[idx_code])
        except Exception:
            continue
        geom = shape(shp.__geo_interface__)
        c = geom.centroid
        rows.append({"poa_code_2021": code, "lon": float(c.x), "lat": float(c.y)})

    return pl.DataFrame(rows)


def map_postcodes_to_sa2(poa_centroids: pl.DataFrame) -> pl.DataFrame:
    """
    Uses the existing SA2 point-in-polygon assignment to attach SA2 codes to POA centroids.
    """
    if poa_centroids.height == 0:
        return pl.DataFrame(
            {
                "poa_code_2021": pl.Series([], dtype=pl.Int64),
                "sa2_code_2021": pl.Series([], dtype=pl.Int64),
                "sa2_name_2021": pl.Series([], dtype=pl.Utf8),
            }
        )

    # Reuse the existing function by shaping the input like the H3 centroid table.
    tmp = poa_centroids.rename({"poa_code_2021": "h3", "lat": "lat_mean", "lon": "lon_mean"})
    assigned = assign_sa2_to_h3_areas(tmp, cfg=MarketFitConfig(), lat_col="lat_mean", lon_col="lon_mean")
    return assigned.select(
        [
            pl.col("h3").cast(pl.Int64, strict=False).alias("poa_code_2021"),
            pl.col("sa2_code_2021").cast(pl.Int64, strict=False),
            pl.col("sa2_name_2021").cast(pl.Utf8),
        ]
    )


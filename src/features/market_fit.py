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
class MarketFitConfig:
    # ABS SEIFA 2021 (SA2) file already downloaded in Task 5 pipeline.
    seifa_xlsx_path: Path = Path("data/raw/abs/seifa_sa2_2021.xlsx")
    sa2_shp_path: Path = Path("data/raw/abs/sa2_2021_shp/SA2_2021_AUST_GDA2020.shp")
    # Use IRSAD as the primary affluence proxy (advantage/disadvantage combined).
    # Also include IER (economic resources) as a secondary component.
    w_irsad: float = 0.7
    w_ier: float = 0.3


def load_seifa_sa2(config: MarketFitConfig = MarketFitConfig()) -> pl.DataFrame:
    """
    Load SEIFA 2021 SA2 summary from Table 1:
      - IRSD score/decile
      - IRSAD score/decile
      - IER score/decile
      - IEO score/decile
      - Usual Resident Population
    """
    import pandas as pd

    if not config.seifa_xlsx_path.exists():
        return pl.DataFrame()

    # Column names in this ABS table are stable but verbose.
    code = "2021 Statistical Area Level 2  (SA2) 9-Digit Code"
    name = "2021 Statistical Area Level 2 (SA2) Name "
    pdf = pd.read_excel(config.seifa_xlsx_path, sheet_name="Table 1", header=5)
    if code not in pdf.columns or name not in pdf.columns:
        return pl.DataFrame()

    # Drop non-data footer rows (e.g., copyright) by requiring numeric SA2 code.
    pdf[code] = pd.to_numeric(pdf[code], errors="coerce")
    pdf = pdf[pdf[code].notna()].copy()

    # Coerce numeric fields that may contain '-' into numbers/NaN.
    for c in [
        "Score",
        "Decile",
        "Score.1",
        "Decile.1",
        "Score.2",
        "Decile.2",
        "Score.3",
        "Decile.3",
        "Usual Resident Population",
    ]:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

    # Keep only the columns we use to avoid mixed-type conversion issues.
    keep = [
        code,
        name,
        "Score",
        "Decile",
        "Score.1",
        "Decile.1",
        "Score.2",
        "Decile.2",
        "Score.3",
        "Decile.3",
        "Usual Resident Population",
    ]
    pdf = pdf[[c for c in keep if c in pdf.columns]]

    df = pl.from_pandas(pdf)

    out = df.select(
        [
            pl.col(code).cast(pl.Int64).alias("sa2_code_2021"),
            pl.col(name).cast(pl.Utf8).str.strip_chars().alias("sa2_name_2021"),
            pl.col("Score").cast(pl.Float64).alias("seifa_irsd_score"),
            pl.col("Decile").cast(pl.Int64).alias("seifa_irsd_decile"),
            pl.col("Score.1").cast(pl.Float64).alias("seifa_irsad_score"),
            pl.col("Decile.1").cast(pl.Int64).alias("seifa_irsad_decile"),
            pl.col("Score.2").cast(pl.Float64).alias("seifa_ier_score"),
            pl.col("Decile.2").cast(pl.Int64).alias("seifa_ier_decile"),
            pl.col("Score.3").cast(pl.Float64).alias("seifa_ieo_score"),
            pl.col("Decile.3").cast(pl.Int64).alias("seifa_ieo_decile"),
            pl.col("Usual Resident Population").cast(pl.Int64).alias("sa2_population"),
        ]
    ).drop_nulls(["sa2_code_2021"])

    return out


def _safe_minmax(expr: pl.Expr) -> pl.Expr:
    mn = expr.min()
    mx = expr.max()
    return pl.when((mx - mn) <= 1e-9).then(0.0).otherwise((expr - mn) / (mx - mn))


def build_market_fit_by_sa2(seifa: pl.DataFrame, cfg: MarketFitConfig = MarketFitConfig()) -> pl.DataFrame:
    """
    Market fit is an affluence proxy (SEIFA) + economic resources proxy.
    We output a 0..1 score with components.
    """
    if seifa.height == 0:
        return pl.DataFrame()

    df = seifa.with_columns(
        [
            _safe_minmax(pl.col("seifa_irsad_score")).alias("n_irsad"),
            _safe_minmax(pl.col("seifa_ier_score")).alias("n_ier"),
        ]
    ).with_columns(
        (cfg.w_irsad * pl.col("n_irsad") + cfg.w_ier * pl.col("n_ier")).alias("market_fit_score")
    )
    return df.select(
        [
            "sa2_code_2021",
            "sa2_name_2021",
            "sa2_population",
            "seifa_irsad_score",
            "seifa_irsad_decile",
            "seifa_ier_score",
            "seifa_ier_decile",
            "market_fit_score",
        ]
    )


def assign_sa2_to_h3_areas(
    areas: pl.DataFrame,
    cfg: MarketFitConfig = MarketFitConfig(),
    lat_col: str = "lat_mean",
    lon_col: str = "lon_mean",
) -> pl.DataFrame:
    """
    Spatial join: assign each H3 area centroid to an SA2 polygon.
    Uses pyshp + shapely (no GDAL dependency).
    """
    if shapefile is None or geom is None or prep is None:
        raise RuntimeError("Missing dependencies for SA2 spatial join. Install shapely and pyshp.")
    if not cfg.sa2_shp_path.exists():
        return areas.with_columns(
            [pl.lit(None).cast(pl.Int64).alias("sa2_code_2021"), pl.lit(None).cast(pl.Utf8).alias("sa2_name_2021")]
        )

    sf = shapefile.Reader(str(cfg.sa2_shp_path))
    fields = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
    idx_code = fields.index("SA2_CODE21") if "SA2_CODE21" in fields else None
    idx_name = fields.index("SA2_NAME21") if "SA2_NAME21" in fields else None
    if idx_code is None or idx_name is None:
        return areas.with_columns(
            [pl.lit(None).cast(pl.Int64).alias("sa2_code_2021"), pl.lit(None).cast(pl.Utf8).alias("sa2_name_2021")]
        )

    # Build prepared polygons and bbox for fast candidate filtering.
    polys: list[tuple[int, str, tuple[float, float, float, float], object]] = []
    for sr in sf.iterShapeRecords():
        rec = sr.record
        try:
            sa2_code = int(rec[idx_code])
        except Exception:
            # Some boundary files contain placeholder rows (e.g., 'ZZZZZZZZZ').
            continue
        sa2_name = str(rec[idx_name])
        shp = sr.shape
        if shp is None or getattr(shp, "shapeType", None) == 0:
            # NullShape
            continue
        # shp.bbox = [minx, miny, maxx, maxy]
        bbox = (float(shp.bbox[0]), float(shp.bbox[1]), float(shp.bbox[2]), float(shp.bbox[3]))
        try:
            poly = geom.shape(shp.__geo_interface__)
            polys.append((sa2_code, sa2_name, bbox, prep.prep(poly)))
        except Exception:
            continue

    # Assign per centroid.
    rows = areas.select(["h3", lat_col, lon_col]).to_dicts()
    out = []
    for r in rows:
        lat = r.get(lat_col)
        lon = r.get(lon_col)
        if lat is None or lon is None:
            out.append({"h3": r["h3"], "sa2_code_2021": None, "sa2_name_2021": None})
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
        out.append({"h3": r["h3"], "sa2_code_2021": hit_code, "sa2_name_2021": hit_name})

    return areas.join(pl.DataFrame(out), on="h3", how="left")


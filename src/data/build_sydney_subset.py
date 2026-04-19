from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config.geo import SYDNEY_BBOX
from src.config.settings import Settings
from src.data.load_places import count_rows

logger = logging.getLogger(__name__)


def _sydney_filter_strategy(lf: pl.LazyFrame, settings: Settings) -> pl.LazyFrame:
    cols = set(lf.collect_schema().names())

    # Strategy A: locality == "Sydney"
    if "locality" in cols:
        lf_a = lf.filter(pl.col("locality") == settings.city_name)
        return lf_a

    # Strategy B: NSW + locality in Greater Sydney list (extensible)
    if "region" in cols and "locality" in cols:
        allowed = {settings.city_name} | settings.sydney_localities
        lf_b = lf.filter((pl.col("region") == "NSW") & (pl.col("locality").is_in(sorted(allowed))))
        return lf_b

    # Fallback: bbox
    b = SYDNEY_BBOX
    return lf.filter(
        (pl.col("lat") >= b.min_lat)
        & (pl.col("lat") <= b.max_lat)
        & (pl.col("lon") >= b.min_lon)
        & (pl.col("lon") <= b.max_lon)
    )


def build_places_sydney(settings: Settings, places_au_path: Path, out_path: Path | None = None) -> Path:
    out = out_path or (settings.data_interim_dir / "places_sydney.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)

    lf = pl.scan_parquet(str(places_au_path))
    count_rows(lf, "places_au_input")

    lf = _sydney_filter_strategy(lf, settings)
    count_rows(lf, "places_sydney_filtered")

    logger.info("Writing Sydney subset to %s", out)
    lf.sink_parquet(str(out))
    return out


from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config.settings import Settings
from src.data.load_places import count_rows, load_places_canonical_lazy

logger = logging.getLogger(__name__)


AU_BBOX = {"min_lat": -44.0, "max_lat": -10.0, "min_lon": 112.0, "max_lon": 154.0}


def build_places_au(settings: Settings, out_path: Path | None = None) -> Path:
    out = out_path or (settings.data_interim_dir / "places_au.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)

    lf = load_places_canonical_lazy(settings)
    count_rows(lf, "places_input")

    # Filter by country if present; otherwise fallback to bbox.
    if "country" in lf.collect_schema().names():
        lf = lf.filter(pl.col("country") == settings.country_code)
        count_rows(lf, f"filter_country_{settings.country_code}")
    else:
        b = AU_BBOX
        lf = lf.filter(
            (pl.col("lat") >= b["min_lat"])
            & (pl.col("lat") <= b["max_lat"])
            & (pl.col("lon") >= b["min_lon"])
            & (pl.col("lon") <= b["max_lon"])
        )
        count_rows(lf, "filter_bbox_AU")

    # Closed / unresolved handling (only if columns exist)
    cols = set(lf.collect_schema().names())
    if "date_closed" in cols:
        lf = lf.filter(pl.col("date_closed").is_null())
        count_rows(lf, "filter_open_only")

    if "unresolved_flags" in cols:
        # Drop clearly invalid items if flags include these keywords.
        bad = ["duplicate", "delete", "privatevenue", "doesnt_exist"]
        lf = lf.filter(~pl.any_horizontal([pl.col("unresolved_flags").cast(pl.Utf8).str.contains(b) for b in bad]))
        count_rows(lf, "filter_unresolved_flags")

    logger.info("Writing AU subset to %s", out)
    lf.sink_parquet(str(out))
    return out


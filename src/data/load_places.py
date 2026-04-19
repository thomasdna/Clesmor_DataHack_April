from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config.settings import Settings
from src.data.schema import canonical_places_lazyframe, inspect_places_schema

logger = logging.getLogger(__name__)


def scan_places(path: Path) -> pl.LazyFrame:
    """
    Scan a Parquet file or a directory of Parquet shards.
    Uses Polars lazy scanning for efficient filtering.
    """
    if not path.exists():
        raise FileNotFoundError(f"Places parquet not found at: {path}")

    if path.is_dir():
        lf = pl.scan_parquet(str(path / "*.parquet"))
    else:
        lf = pl.scan_parquet(str(path))

    schema = lf.collect_schema()
    logger.info("Places schema columns (%d): %s", len(schema.names()), schema.names())
    return lf


def load_places_canonical_lazy(settings: Settings) -> pl.LazyFrame:
    """
    Returns a canonical LazyFrame with stable column names:
    place_id, name, lat, lon, country, category_ids
    """
    p = settings.places_parquet_path
    schema = inspect_places_schema(p)
    logger.info("Discovered required places columns: %s", schema)
    lf, _ = canonical_places_lazyframe(p)
    return lf


def count_rows(lf: pl.LazyFrame, label: str) -> int:
    n = lf.select(pl.len()).collect().item()
    logger.info("%s row_count=%d", label, n)
    return int(n)


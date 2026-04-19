from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config.settings import Settings

logger = logging.getLogger(__name__)


def scan_categories(path: Path) -> pl.LazyFrame:
    if not path.exists():
        raise FileNotFoundError(f"Categories parquet not found at: {path}")
    lf = pl.scan_parquet(str(path))
    schema = lf.collect_schema()
    logger.info("Categories schema columns (%d): %s", len(schema.names()), schema.names())
    return lf


def load_categories_df(settings: Settings) -> pl.DataFrame:
    df = pl.read_parquet(str(settings.categories_parquet_path))
    logger.info("Loaded categories rows=%d cols=%d", df.height, len(df.columns))
    return df


from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import polars as pl

from src.config.settings import Settings

logger = logging.getLogger(__name__)


KEEP_COLS = [
    "category_id",
    "category_name",
    "category_label",
    "level1_category_id",
    "level1_category_name",
    "level2_category_id",
    "level2_category_name",
]


def prepare_categories(settings: Settings, out_path: Path | None = None) -> Path:
    out = out_path or (settings.data_interim_dir / "categories_clean.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)

    df = pl.read_parquet(str(settings.categories_parquet_path))
    cols = set(df.columns)
    keep = [c for c in KEEP_COLS if c in cols]

    clean = df.select(keep).unique(subset=["category_id"])
    clean.write_parquet(str(out))
    logger.info("Wrote categories_clean rows=%d cols=%d to %s", clean.height, len(clean.columns), out)
    return out


def explode_category_ids(df: pl.DataFrame, col: str = "category_ids") -> pl.DataFrame:
    if col not in df.columns:
        return df.with_columns(pl.lit(None).alias("category_id"))
    return df.with_columns(pl.col(col).fill_null(pl.lit([])).alias(col)).explode(col).rename({col: "category_id"})


def join_categories(
    exploded: pl.DataFrame,
    categories_clean: pl.DataFrame,
    how: str = "left",
) -> pl.DataFrame:
    if "category_id" not in exploded.columns:
        raise ValueError("Expected a `category_id` column to join on.")
    return exploded.join(categories_clean, on="category_id", how=how)


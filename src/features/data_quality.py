from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class DataQualityWeights:
    # Penalties (subtracted)
    unresolved_flag_penalty: float = 0.25
    missing_coords_penalty: float = 0.40
    missing_name_penalty: float = 0.20
    missing_category_penalty: float = 0.15


def derived_data_quality_score(pois: pl.DataFrame, weights: DataQualityWeights = DataQualityWeights()) -> pl.Series:
    """
    POI-level transparent score in [0, 1], using only columns that exist.
    If a column doesn't exist, it contributes no penalty/bonus.
    """
    score = pl.lit(1.0)

    if "lat" in pois.columns and "lon" in pois.columns:
        score = score - weights.missing_coords_penalty * (
            (pl.col("lat").is_null() | pl.col("lon").is_null()).cast(pl.Float64)
        )

    if "name" in pois.columns:
        score = score - weights.missing_name_penalty * pl.col("name").is_null().cast(pl.Float64)

    if "category_id" in pois.columns:
        score = score - weights.missing_category_penalty * pl.col("category_id").is_null().cast(pl.Float64)

    if "unresolved_flags" in pois.columns:
        score = score - weights.unresolved_flag_penalty * pl.col("unresolved_flags").is_not_null().cast(pl.Float64)

    # Clamp to [0,1]
    return pl.when(score < 0).then(0.0).when(score > 1).then(1.0).otherwise(score).alias("data_quality_score")


def data_quality_by_area(area_pois: pl.DataFrame) -> pl.DataFrame:
    """
    Area-level score: mean POI score per H3 cell.
    Expects `h3` column and optionally `unresolved_flags`.
    """
    df = area_pois
    if "data_quality_score" not in df.columns:
        df = df.with_columns(derived_data_quality_score(df))
    return df.group_by("h3").agg(pl.col("data_quality_score").mean()).rename({"data_quality_score": "data_quality_score"})


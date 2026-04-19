from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class ScoreWeights:
    competitor_weight: float = 1.0
    complement_weight: float = 1.0
    density_weight: float = 0.0
    data_quality_weight: float = 0.5


def score_areas(
    areas: pl.DataFrame,
    competitor_counts: pl.DataFrame,  # h3, competitor_count
    complement_counts: pl.DataFrame,  # h3, complement_count
    data_quality: pl.DataFrame,  # h3, data_quality_score
    weights: ScoreWeights,
) -> pl.DataFrame:
    df = areas.join(competitor_counts, on="h3", how="left").join(complement_counts, on="h3", how="left").join(
        data_quality, on="h3", how="left"
    )
    df = df.with_columns(
        [
            pl.col("competitor_count").fill_null(0),
            pl.col("complement_count").fill_null(0),
            pl.col("data_quality_score").fill_null(0.0),
        ]
    )

    # Normalize counts lightly using log1p so CBD doesn't dominate purely by scale.
    df = df.with_columns(
        [
            (pl.col("competitor_count").log1p()).alias("competitor_log"),
            (pl.col("complement_count").log1p()).alias("complement_log"),
            (pl.col("poi_count").log1p()).alias("density_log"),
        ]
    )

    df = df.with_columns(
        (
            weights.complement_weight * pl.col("complement_log")
            - weights.competitor_weight * pl.col("competitor_log")
            + weights.density_weight * pl.col("density_log")
            + weights.data_quality_weight * pl.col("data_quality_score")
        ).alias("opportunity_score")
    )

    return df.sort("opportunity_score", descending=True)


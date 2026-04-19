from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from src.data.load_housing_affordability import HousingAffordabilityConfig, load_raid_2021_by_sa2


@dataclass(frozen=True)
class RentAffordabilityConfig:
    """
    Config for turning Census-derived rental stress into a simple 0..1 score.

    We define:
    - rent_stress_share = rent_stress_households / total_renting_households
    - rent_affordability_score = 1 - rent_stress_share

    Interpretation:
    - Higher rent_affordability_score = lower rental stress (more affordable on this proxy).
    """

    min_total_renting_households: int = 200


def build_rent_affordability_by_sa2(
    cfg: RentAffordabilityConfig = RentAffordabilityConfig(),
    src_cfg: HousingAffordabilityConfig = HousingAffordabilityConfig(),
) -> pl.DataFrame:
    base = load_raid_2021_by_sa2(src_cfg)
    if base.height == 0:
        return pl.DataFrame(
            {
                "sa2_code_2021": pl.Series([], dtype=pl.Int64),
                "rent_stress_share": pl.Series([], dtype=pl.Float64),
                "rent_affordability_score": pl.Series([], dtype=pl.Float64),
            }
        )

    eps = 1.0
    out = base.with_columns(
        [
            (pl.col("rent_stress_households") / (pl.col("total_renting_households") + eps))
            .clip(0.0, 1.0)
            .alias("rent_stress_share"),
        ]
    ).with_columns((1.0 - pl.col("rent_stress_share")).alias("rent_affordability_score"))

    # Guardrail: if SA2 has too few renting households, treat as missing/uncertain.
    out = out.with_columns(
        pl.when(pl.col("total_renting_households") < cfg.min_total_renting_households)
        .then(pl.lit(None).cast(pl.Float64))
        .otherwise(pl.col("rent_affordability_score"))
        .alias("rent_affordability_score")
    )

    return out.select(["sa2_code_2021", "rent_stress_share", "rent_affordability_score"])


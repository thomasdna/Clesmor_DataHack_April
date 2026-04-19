from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class OpportunityWeights:
    complementarity: float = 1.2
    commercial_activity: float = 0.8
    diversity: float = 0.6
    direct_competition: float = 1.0
    saturation: float = 0.7
    data_quality: float = 0.5
    transit_access: float = 0.4
    market_fit: float = 0.6
    rent_affordability: float = 0.35


def _safe_minmax(s: pl.Expr) -> pl.Expr:
    # Min-max normalize with guard for constant columns.
    mn = s.min()
    mx = s.max()
    return pl.when((mx - mn) <= 1e-9).then(0.0).otherwise((s - mn) / (mx - mn))


def score_area_features(features: pl.DataFrame, w: OpportunityWeights = OpportunityWeights()) -> pl.DataFrame:
    """
    Transparent score with breakdown. Features are expected to include:
    - complementarity_ratio
    - commercial_activity_proxy
    - unique_category_count
    - competitor_density_proxy
    - saturation_proxy
    - mean_data_quality_score
    """
    df = features.clone()

    # Stabilize heavy tails.
    df = df.with_columns(
        [
            pl.col("commercial_activity_proxy").log1p().alias("commercial_activity_log"),
            pl.col("unique_category_count").log1p().alias("diversity_log"),
            pl.col("competitor_density_proxy").log1p().alias("competition_log"),
            pl.col("saturation_proxy").log1p().alias("saturation_log"),
        ]
    )

    # Normalize (0..1)
    norm_cols = [
        _safe_minmax(pl.col("complementarity_ratio")).alias("n_complementarity"),
        _safe_minmax(pl.col("commercial_activity_log")).alias("n_activity"),
        _safe_minmax(pl.col("diversity_log")).alias("n_diversity"),
        _safe_minmax(pl.col("competition_log")).alias("n_competition"),
        _safe_minmax(pl.col("saturation_log")).alias("n_saturation"),
        _safe_minmax(pl.col("mean_data_quality_score")).alias("n_quality"),
    ]
    if "transit_access_score" in df.columns:
        norm_cols.append(_safe_minmax(pl.col("transit_access_score")).alias("n_transit"))
    else:
        norm_cols.append(pl.lit(0.0).alias("n_transit"))
    if "market_fit_score" in df.columns:
        norm_cols.append(_safe_minmax(pl.col("market_fit_score")).alias("n_market_fit"))
    else:
        norm_cols.append(pl.lit(0.0).alias("n_market_fit"))
    if "rent_affordability_score" in df.columns:
        norm_cols.append(_safe_minmax(pl.col("rent_affordability_score").fill_null(0.0)).alias("n_rent_affordability"))
    else:
        norm_cols.append(pl.lit(0.0).alias("n_rent_affordability"))
    df = df.with_columns(norm_cols)

    df = df.with_columns(
        [
            (w.complementarity * pl.col("n_complementarity")).alias("score_complementarity"),
            (w.commercial_activity * pl.col("n_activity")).alias("score_activity"),
            (w.diversity * pl.col("n_diversity")).alias("score_diversity"),
            (-w.direct_competition * pl.col("n_competition")).alias("score_competition"),
            (-w.saturation * pl.col("n_saturation")).alias("score_saturation"),
            (w.data_quality * pl.col("n_quality")).alias("score_quality"),
            (w.transit_access * pl.col("n_transit")).alias("score_transit_access"),
            (w.market_fit * pl.col("n_market_fit")).alias("score_market_fit"),
            (w.rent_affordability * pl.col("n_rent_affordability")).alias("score_rent_affordability"),
        ]
    )

    df = df.with_columns(
        (
            pl.col("score_complementarity")
            + pl.col("score_activity")
            + pl.col("score_diversity")
            + pl.col("score_competition")
            + pl.col("score_saturation")
            + pl.col("score_quality")
            + pl.col("score_transit_access")
            + pl.col("score_market_fit")
            + pl.col("score_rent_affordability")
        ).alias("opportunity_score")
    )

    return df.sort("opportunity_score", descending=True)


def top_reasons(row: dict) -> list[str]:
    parts = {
        "Strong complements": row.get("score_complementarity", 0.0),
        "High commercial activity": row.get("score_activity", 0.0),
        "High category diversity": row.get("score_diversity", 0.0),
        "Low competition": -row.get("score_competition", 0.0),
        "Lower saturation": -row.get("score_saturation", 0.0),
        "Good data coverage": row.get("score_quality", 0.0),
        "Strong transit access": row.get("score_transit_access", 0.0),
        "Strong market fit": row.get("score_market_fit", 0.0),
        "Lower rental stress (proxy)": row.get("score_rent_affordability", 0.0),
    }
    return [k for k, _ in sorted(parts.items(), key=lambda kv: kv[1], reverse=True)[:3]]


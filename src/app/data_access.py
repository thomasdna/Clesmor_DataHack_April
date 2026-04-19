from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import streamlit as st

from src.app.state import AppInputs
from src.scoring.opportunity_score import OpportunityWeights, score_area_features, top_reasons


class AppDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class AppData:
    ranked: "pl.DataFrame"


def _strategy_weights(mode: str) -> OpportunityWeights:
    if mode == "low competition":
        return OpportunityWeights(
            complementarity=0.8,
            commercial_activity=0.7,
            diversity=0.5,
            direct_competition=1.4,
            saturation=1.0,
            data_quality=0.4,
        )
    if mode == "ecosystem-first":
        return OpportunityWeights(
            complementarity=1.5,
            commercial_activity=0.8,
            diversity=0.8,
            direct_competition=0.8,
            saturation=0.5,
            data_quality=0.5,
        )
    return OpportunityWeights()


def _pick_processed_paths(area_unit: str) -> tuple[Path, Path | None]:
    ranked = Path("data/processed/sydney_ranked_areas.parquet")
    features = Path("data/processed/sydney_area_features.parquet")

    if area_unit == "locality":
        ranked_local = Path("data/processed/sydney_ranked_localities.parquet")
        features_local = Path("data/processed/sydney_locality_features.parquet")
        if ranked_local.exists():
            ranked = ranked_local
        if features_local.exists():
            features = features_local

    return ranked, features


@st.cache_data(show_spinner=False)
def _load_ranked_df(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def _load_features_df(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)


def _ensure_required_columns(df: pl.DataFrame) -> pl.DataFrame:
    required = {
        "area_id",
        "opportunity_score",
        "competitor_count",
        "complementary_count",
        "commercial_activity_proxy",
        "saturation_proxy",
        "unique_category_count",
        "mean_data_quality_score",
        "active_poi_count",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise AppDataError(f"Processed ranked data is missing required columns: {missing}")
    return df


def _maybe_add_reasons(df: pl.DataFrame) -> pl.DataFrame:
    if "brief_reason" in df.columns:
        return df
    if "reasons_top3" in df.columns:
        return df.with_columns(pl.col("reasons_top3").alias("brief_reason"))
    return df.with_columns(pl.lit("").alias("brief_reason"))


def _sample_fallback() -> pl.DataFrame:
    # Minimal demo fallback to keep the UI usable if processed outputs are missing.
    return pl.DataFrame(
        {
            "area_id": ["demo_area_1", "demo_area_2", "demo_area_3"],
            "area_name": ["Inner West (demo)", "Eastern Suburbs (demo)", "Lower North Shore (demo)"],
            "opportunity_score": [0.82, 0.74, 0.69],
            "competitor_count": [12, 18, 10],
            "complementary_count": [40, 52, 33],
            "commercial_activity_proxy": [820, 950, 610],
            "saturation_proxy": [0.14, 0.21, 0.12],
            "unique_category_count": [120, 160, 110],
            "mean_data_quality_score": [0.80, 0.76, 0.78],
            "active_poi_count": [900, 1100, 700],
            "brief_reason": [
                "Strong wellness ecosystem with moderate competition.",
                "High activity but competition is elevated—validate differentiation.",
                "Balanced activity + complements with lower saturation risk.",
            ],
        }
    )


def _re_score_from_features(features: pl.DataFrame, inputs: AppInputs) -> pl.DataFrame:
    w = _strategy_weights(inputs.strategy_mode)
    ranked = score_area_features(features, w=w)
    ranked = ranked.with_columns(
        pl.struct(ranked.columns)
        .map_elements(lambda r: "; ".join(top_reasons(r)), return_dtype=pl.Utf8)
        .alias("brief_reason")
    )
    return ranked


def load_app_data(inputs: AppInputs) -> AppData:
    ranked_path, features_path = _pick_processed_paths(inputs.area_unit)

    if ranked_path.exists():
        df = _load_ranked_df(str(ranked_path))
        df = _ensure_required_columns(df)
        df = _maybe_add_reasons(df)
    elif features_path is not None and features_path.exists():
        features = _load_features_df(str(features_path))
        df = _re_score_from_features(features=features, inputs=inputs)
        df = _ensure_required_columns(df)
    else:
        df = _sample_fallback()

    # Normalize naming for UI.
    if "area_name" not in df.columns:
        df = df.with_columns(pl.col("area_id").cast(pl.Utf8).alias("area_name"))

    # Guardrails (decision support stability).
    df = df.filter(
        (pl.col("active_poi_count") >= inputs.min_active_pois)
        & (pl.col("mean_data_quality_score") >= inputs.min_data_quality)
    )
    if df.height == 0:
        raise AppDataError(
            "No areas passed the current guardrails. Try lowering minimum data quality / minimum active POIs."
        )

    # If strategy mode changed and we loaded pre-ranked file, allow lightweight re-scoring if features exist.
    if features_path is not None and features_path.exists():
        features = _load_features_df(str(features_path))
        rescored = _re_score_from_features(features=features, inputs=inputs)
        if set(df.columns).issubset(set(rescored.columns)):
            df = rescored

    df = df.sort("opportunity_score", descending=True).with_row_index("rank", offset=1)
    return AppData(ranked=df)


"""
Build a tidy table of all known ranked-area fields for Streamlit (one row per metric).
"""
from __future__ import annotations

from typing import Any

import polars as pl

# (group, display label, parquet column, format)
# fmt: float2, float3, float4, int0, str, pct1
METRIC_ORDER: list[tuple[str, str, str, str]] = [
    ("Score", "Opportunity score", "opportunity_score", "float3"),
    ("Activity", "Active POIs", "poi_count", "int0"),
    ("Activity", "Competitors", "competitor_count", "int0"),
    ("Activity", "Complements", "complement_count", "int0"),
    ("Activity", "Data quality (mean)", "data_quality_score", "float2"),
    ("Structure", "Competitor density proxy", "competitor_density_proxy", "float4"),
    ("Structure", "Complementarity ratio", "complementarity_ratio", "float4"),
    ("Structure", "Commercial activity proxy", "commercial_activity_proxy", "float1"),
    ("Structure", "Saturation proxy", "saturation_proxy", "float4"),
    ("Structure", "Unique category count (sum)", "unique_category_count", "float1"),
    ("Structure", "Commercial other (count)", "commercial_other_count", "int0"),
    ("Transit", "Transit access score", "transit_access_score", "float2"),
    ("Transit", "Station entrances (nearby)", "station_entrance_count", "int0"),
    ("Transit", "PT facilities (nearby)", "pt_facility_count", "int0"),
    ("Transit", "Bus shelters (nearby)", "bus_shelter_count", "int0"),
    ("Market (SA2)", "Market fit score", "market_fit_score", "float2"),
    ("Market (SA2)", "SEIFA IRSAD decile", "seifa_irsad_decile", "int0"),
    ("Market (SA2)", "SA2 population", "sa2_population", "int0"),
    ("Market (SA2)", "SA2 code", "sa2_code_2021", "str"),
    ("Market (SA2)", "SA2 name", "sa2_name_2021", "str"),
    ("Rent (SA2)", "Rent affordability score", "rent_affordability_score", "float2"),
    ("Rent (SA2)", "Rental stress share", "rent_stress_share", "float2"),
    ("Labels", "Suburb (SAL)", "sal_name_2021", "str"),
    ("Labels", "SAL code", "sal_code_2021", "str"),
    ("Geo", "Latitude (centroid)", "lat_mean", "float4"),
    ("Geo", "Longitude (centroid)", "lon_mean", "float4"),
    # Normalized inputs (0–1) used before weighting
    ("Normalized inputs", "n complementarity", "n_complementarity", "float2"),
    ("Normalized inputs", "n activity", "n_activity", "float2"),
    ("Normalized inputs", "n diversity", "n_diversity", "float2"),
    ("Normalized inputs", "n competition", "n_competition", "float2"),
    ("Normalized inputs", "n saturation", "n_saturation", "float2"),
    ("Normalized inputs", "n quality", "n_quality", "float2"),
    ("Normalized inputs", "n transit", "n_transit", "float2"),
    ("Normalized inputs", "n market fit", "n_market_fit", "float2"),
    ("Normalized inputs", "n rent affordability", "n_rent_affordability", "float2"),
    # Weighted contributions (same units as opportunity_score pieces)
    ("Score parts", "Complements", "score_complementarity", "float3"),
    ("Score parts", "Activity", "score_activity", "float3"),
    ("Score parts", "Diversity", "score_diversity", "float3"),
    ("Score parts", "Competition (penalty)", "score_competition", "float3"),
    ("Score parts", "Saturation (penalty)", "score_saturation", "float3"),
    ("Score parts", "Data quality", "score_quality", "float3"),
    ("Score parts", "Transit access", "score_transit_access", "float3"),
    ("Score parts", "Market fit", "score_market_fit", "float3"),
    ("Score parts", "Rent affordability", "score_rent_affordability", "float3"),
]


def _fmt(v: Any, fmt: str) -> str:
    if v is None:
        return "—"
    try:
        if fmt == "str":
            s = str(v).strip()
            return s if s else "—"
        if fmt == "int0":
            return f"{int(round(float(v))):,}"
        if fmt == "float1":
            return f"{float(v):,.1f}"
        if fmt == "float2":
            return f"{float(v):.2f}"
        if fmt == "float3":
            return f"{float(v):.3f}"
        if fmt == "float4":
            return f"{float(v):.4f}"
        if fmt == "pct1":
            return f"{float(v) * 100:.1f}%"
    except (TypeError, ValueError):
        return str(v)
    return str(v)


def metrics_table_for_area_row(row: dict[str, Any]) -> pl.DataFrame:
    """Return Group | Metric | Value | Field for every column present in ``row``."""
    out: list[dict[str, str]] = []
    keys_present = set(row.keys())
    for group, label, field, fmt in METRIC_ORDER:
        if field not in keys_present:
            continue
        out.append(
            {
                "Group": group,
                "Metric": label,
                "Value": _fmt(row.get(field), fmt),
                "Field": field,
            }
        )
    return pl.DataFrame(out)


def safe_float(row: dict[str, Any], key: str) -> float | None:
    """None if missing or null; do not coerce to 0.0."""
    if key not in row:
        return None
    v = row.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


_REDUNDANT_PAIR = (
    ("complementary_count", "complement_count"),
    ("mean_data_quality_score", "data_quality_score"),
    ("active_poi_count", "poi_count"),
)


def ordered_columns_for_tables(df: pl.DataFrame) -> list[str]:
    """
    Column order for Overview / Compare: human-readable labels first, then metrics in METRIC_ORDER,
    then reasons + id, then any remaining columns (excluding redundant raw duplicates).
    """
    cols_set = set(df.columns)
    drop: set[str] = set()
    for old, new in _REDUNDANT_PAIR:
        if old in cols_set and new in cols_set:
            drop.add(old)

    out: list[str] = []
    for c in ("sal_name_2021", "sa2_name_2021"):
        if c in cols_set and c not in drop:
            out.append(c)

    for _, _, field, _ in METRIC_ORDER:
        if field in cols_set and field not in drop and field not in out:
            out.append(field)

    for c in ("reasons_top3", "h3"):
        if c in cols_set and c not in out:
            out.append(c)

    rest = sorted(c for c in cols_set if c not in out and c not in drop and c != "area_id")
    out.extend(rest)
    return out


def map_point_select(top: pl.DataFrame) -> list:
    """Expressions for Overview map layer + tooltip (all tooltip keys present, nullable)."""
    out: list = [
        pl.col("lat_mean").alias("lat"),
        pl.col("lon_mean").alias("lon"),
        pl.col("opportunity_score"),
        pl.col("competitor_count"),
        pl.col("complement_count"),
        pl.col("poi_count"),
        pl.col("h3"),
    ]
    optional: list[tuple[str, pl.DataType]] = [
        ("sal_name_2021", pl.Utf8),
        ("sa2_name_2021", pl.Utf8),
        ("transit_access_score", pl.Float64),
        ("market_fit_score", pl.Float64),
        ("rent_affordability_score", pl.Float64),
    ]
    for name, dtype in optional:
        if name in top.columns:
            out.append(pl.col(name))
        else:
            out.append(pl.lit(None).cast(dtype).alias(name))
    return out

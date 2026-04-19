from __future__ import annotations

from pathlib import Path

import pydeck as pdk
import polars as pl
import streamlit as st

from src.app.state import CompareState

try:
    import h3
except Exception:  # pragma: no cover
    h3 = None


def _with_centroids(df: pl.DataFrame) -> pl.DataFrame:
    if "lat" in df.columns and "lon" in df.columns:
        return df
    if "lat_mean" in df.columns and "lon_mean" in df.columns:
        return df.rename({"lat_mean": "lat", "lon_mean": "lon"})

    if h3 is None:
        return df.with_columns([pl.lit(None).cast(pl.Float64).alias("lat"), pl.lit(None).cast(pl.Float64).alias("lon")])

    def _lat(area_id: str) -> float | None:
        try:
            lat, _lon = h3.cell_to_latlng(area_id)
            return float(lat)
        except Exception:
            return None

    def _lon(area_id: str) -> float | None:
        try:
            _lat, lon = h3.cell_to_latlng(area_id)
            return float(lon)
        except Exception:
            return None

    return df.with_columns(
        [
            pl.col("area_id").map_elements(_lat, return_dtype=pl.Float64).alias("lat"),
            pl.col("area_id").map_elements(_lon, return_dtype=pl.Float64).alias("lon"),
        ]
    )


def _score_colors(scores: list[float]) -> list[list[int]]:
    if not scores:
        return []
    mn = min(scores)
    mx = max(scores)
    denom = (mx - mn) if (mx - mn) > 1e-9 else 1.0
    out: list[list[int]] = []
    for s in scores:
        t = (s - mn) / denom
        r = int(220 - 160 * t)
        g = int(60 + 160 * t)
        b = 80
        out.append([r, g, b, 150])
    return out


def render_map(ranked: pl.DataFrame, area_unit: str, selected_area_id: str | None) -> str | None:
    st.subheader("Opportunity map")
    st.caption("Color = opportunity score (red → green). Tooltip shows the main drivers.")

    df = ranked.head(500)  # keep rendering fast
    df = _with_centroids(df)
    if df.select(pl.col("lat").is_null().all()).item() is True:
        st.info("Map centroid columns are missing and H3 centroid fallback is unavailable. Showing table-only mode.")
        return None

    view_df = df.select(
        [
            "area_id",
            "area_name",
            "opportunity_score",
            "competitor_count",
            "complementary_count",
            "commercial_activity_proxy",
            "mean_data_quality_score",
            "lat",
            "lon",
        ]
    )
    rows = view_df.to_dicts()
    colors = _score_colors([float(r["opportunity_score"]) for r in rows])
    for r, c in zip(rows, colors, strict=False):
        r["fill"] = c

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=rows,
        get_position="[lon, lat]",
        get_radius="complementary_count",
        radius_scale=3.0,
        radius_min_pixels=4,
        radius_max_pixels=30,
        get_fill_color="fill",
        pickable=True,
    )

    view = pdk.ViewState(latitude=-33.8688, longitude=151.2093, zoom=10.5)
    tooltip = {
        "text": (
            "{area_name}\n"
            "Score: {opportunity_score}\n"
            "Competitors: {competitor_count}\n"
            "Complements: {complementary_count}\n"
            "Activity proxy: {commercial_activity_proxy}\n"
            "Data quality: {mean_data_quality_score}"
        )
    }

    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip))

    # Click-to-select isn’t reliably supported by Streamlit+Pydeck in all environments;
    # we keep selection via the table control (right panel).
    return None


def render_top_candidates_table(
    ranked: pl.DataFrame, area_unit: str, selected_area_id: str | None, pinned_area_ids: list[str]
) -> tuple[str | None, list[str]]:
    st.subheader("Top candidates")
    st.caption("Select an area to drive the explanation. Pin 2–3 areas for Compare.")

    df = ranked.head(50)
    options = df.get_column("area_id").to_list()
    default_idx = 0
    if selected_area_id in options:
        default_idx = options.index(selected_area_id)

    sel = st.selectbox("Selected area", options=options, index=default_idx)

    c1, c2 = st.columns([0.55, 0.45])
    with c1:
        if st.button("Pin selected", use_container_width=True):
            pins = list(pinned_area_ids)
            if sel not in pins:
                pins.append(sel)
            pinned_area_ids = pins[:3]
    with c2:
        if st.button("Clear pins", use_container_width=True):
            pinned_area_ids = []

    table = df.select(
        [
            "rank",
            "area_name",
            "opportunity_score",
            "competitor_count",
            "complementary_count",
            "commercial_activity_proxy",
            "saturation_proxy",
            "brief_reason",
        ]
    )
    st.dataframe(table, use_container_width=True, hide_index=True)
    return sel, pinned_area_ids


def render_selected_area_card(row, area_unit: str) -> None:
    st.subheader("Selected area")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Opportunity score", f"{float(row['opportunity_score']):.3f}")
    c2.metric("Competitors", int(row["competitor_count"]))
    c3.metric("Complements", int(row["complementary_count"]))
    c4.metric("Data quality", f"{float(row['mean_data_quality_score']):.2f}")


def render_compare_table(ranked: pl.DataFrame, area_unit: str, compare_state: CompareState) -> None:
    st.subheader("Compare 2–3 areas")
    default = compare_state.pinned_ids or ranked.head(2).get_column("area_id").to_list()
    picks = st.multiselect(
        "Pick areas",
        options=ranked.get_column("area_id").to_list(),
        default=default,
        max_selections=3,
    )
    compare_state.pinned_ids = picks

    if len(picks) < 2:
        st.info("Pin or select at least 2 areas to compare.")
        return

    comp = ranked.filter(pl.col("area_id").is_in(picks)).select(
        [
            "area_name",
            "opportunity_score",
            "competitor_count",
            "complementary_count",
            "commercial_activity_proxy",
            "unique_category_count",
            "saturation_proxy",
            "mean_data_quality_score",
            "brief_reason",
        ]
    )
    st.dataframe(comp, use_container_width=True, hide_index=True)

    c1, c2 = st.columns([0.55, 0.45])
    with c1:
        if st.button("Export compare set (CSV)", use_container_width=True):
            out_dir = "data/exports"
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            out_path = f"{out_dir}/compare_set.csv"
            comp.write_csv(out_path)
            st.success(f"Exported: {out_path}")
    with c2:
        st.caption("Tip: pin areas from the Top candidates panel to keep Compare stable.")


from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import streamlit as st

StrategyMode = Literal["balanced growth", "low competition", "ecosystem-first"]
AreaUnit = Literal["h3", "locality"]


@dataclass
class AppInputs:
    city: str = "Sydney"
    business_type: str = "Gym"
    area_unit: AreaUnit = "h3"
    strategy_mode: StrategyMode = "balanced growth"
    min_data_quality: float = 0.60
    min_active_pois: int = 200
    selected_area_id: str | None = None
    top_k: int = 10
    judge_mode: bool = True


@dataclass
class CompareState:
    pinned_ids: list[str]


def get_compare_state() -> CompareState:
    if "compare_state" not in st.session_state:
        st.session_state["compare_state"] = CompareState(pinned_ids=[])
    return st.session_state["compare_state"]


def get_inputs_from_sidebar() -> AppInputs:
    with st.sidebar:
        st.subheader("Demo controls")
        judge_mode = st.toggle(
            "Judge mode (recommended)",
            value=True,
            help="Applies a clean, stable preset for fast demos. You can still override settings below.",
        )
        st.caption("One-click hero states:")
        hero_cols = st.columns(3)
        with hero_cols[0]:
            if st.button("Map", use_container_width=True):
                st.session_state["hero_preset"] = "map"
        with hero_cols[1]:
            if st.button("Why", use_container_width=True):
                st.session_state["hero_preset"] = "why"
        with hero_cols[2]:
            if st.button("Compare", use_container_width=True):
                st.session_state["hero_preset"] = "compare"

        st.divider()
        st.subheader("Setup")
        city = st.text_input("City", value="Sydney", disabled=True)
        business_type = st.text_input("Business type", value="Gym", disabled=True)

        # Defaults tuned for demo stability.
        default_area_unit = "h3"
        default_strategy = "balanced growth"
        default_min_q = 0.60
        default_min_active = 200
        default_top_k = 10

        preset = st.session_state.get("hero_preset")
        if judge_mode:
            if preset == "map":
                default_strategy = "balanced growth"
                default_top_k = 10
            elif preset == "why":
                default_strategy = "ecosystem-first"
                default_top_k = 10
            elif preset == "compare":
                default_strategy = "balanced growth"
                default_top_k = 12

        area_unit = st.selectbox(
            "Area unit",
            options=["h3", "locality"],
            index=0 if default_area_unit == "h3" else 1,
            help="H3 is recommended for clean aggregation. Locality is optional if you’ve generated locality-level outputs.",
        )
        strategy_mode = st.selectbox(
            "Strategy mode",
            options=["balanced growth", "low competition", "ecosystem-first"],
            index=["balanced growth", "low competition", "ecosystem-first"].index(default_strategy),
        )
        st.divider()
        st.subheader("Guardrails")
        min_data_quality = st.slider("Minimum data quality", 0.0, 1.0, float(default_min_q), 0.05)
        min_active_pois = st.slider("Minimum active POIs per area", 0, 3000, int(default_min_active), 50)
        top_k = st.slider("Top candidates", 5, 30, int(default_top_k))

    return AppInputs(
        city=city,
        business_type=business_type,
        area_unit=area_unit,  # type: ignore[assignment]
        strategy_mode=strategy_mode,  # type: ignore[assignment]
        min_data_quality=float(min_data_quality),
        min_active_pois=int(min_active_pois),
        top_k=int(top_k),
        judge_mode=bool(judge_mode),
    )


def sync_default_selection(inputs: AppInputs, ranked) -> None:
    if ranked is None or ranked.height == 0:
        inputs.selected_area_id = None
        return
    if inputs.selected_area_id is None:
        inputs.selected_area_id = str(ranked.select("area_id").row(0)[0])


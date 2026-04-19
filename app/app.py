from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st
import polars as pl

# Ensure `src/` is importable when Streamlit runs from different cwd.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app.data_access import AppData, AppDataError, load_app_data
from src.app.explanations import build_explanation_card, demo_narrative_markdown
from src.app.layout import (
    apply_global_styles,
    render_header,
    render_landing_panel,
    render_missing_data_panel,
)
from src.app.state import (
    AppInputs,
    CompareState,
    get_compare_state,
    get_inputs_from_sidebar,
    sync_default_selection,
)
from src.app.components import (
    render_compare_table,
    render_map,
    render_selected_area_card,
    render_top_candidates_table,
)


def _render_app(data: AppData, inputs: AppInputs) -> None:
    apply_global_styles()
    render_header()

    # Landing/setup panel
    render_landing_panel(inputs)

    # Main content
    tab_map, tab_why, tab_compare = st.tabs(["Opportunity map", "Why this location?", "Compare"])

    with tab_map:
        left, right = st.columns([0.58, 0.42], gap="large")

        with left:
            selection = render_map(
                ranked=data.ranked,
                area_unit=inputs.area_unit,
                selected_area_id=inputs.selected_area_id,
            )
            if selection is not None:
                inputs.selected_area_id = selection

        with right:
            pinned = get_compare_state().pinned_ids
            selected, pinned = render_top_candidates_table(
                ranked=data.ranked,
                area_unit=inputs.area_unit,
                selected_area_id=inputs.selected_area_id,
                pinned_area_ids=pinned,
            )
            inputs.selected_area_id = selected
            get_compare_state().pinned_ids = pinned

        if inputs.judge_mode:
            st.caption(
                "Judge mode is ON: selection is driven from the Top Candidates panel for stability "
                "(map remains hoverable with full tooltips)."
            )

    with tab_why:
        if inputs.selected_area_id is None:
            st.info("Select an area from the map or the Top Candidates table to see the explanation.")
        else:
            picked = data.ranked.filter(pl.col("area_id") == inputs.selected_area_id)
            if picked.height == 0:
                st.warning("Selected area is not in the current filtered ranking.")
            else:
                row = picked.row(0, named=True)
                render_selected_area_card(row=row, area_unit=inputs.area_unit)
                build_explanation_card(row=row, strategy_mode=inputs.strategy_mode)

    with tab_compare:
        cs: CompareState = get_compare_state()
        render_compare_table(ranked=data.ranked, area_unit=inputs.area_unit, compare_state=cs)

    with st.sidebar:
        with st.expander("Demo narrative mode", expanded=False):
            st.markdown(demo_narrative_markdown(), unsafe_allow_html=False)


def main() -> None:
    st.set_page_config(page_title="Expansion Copilot — Sydney gyms", layout="wide")

    inputs = get_inputs_from_sidebar()

    try:
        data = load_app_data(inputs=inputs)
    except AppDataError as e:
        apply_global_styles()
        render_header()
        render_missing_data_panel(message=str(e))
        render_missing_data_panel(message="Tip: run `./scripts/run_build_features.py` first to generate `data/processed/*`.")
        return

    sync_default_selection(inputs=inputs, ranked=data.ranked)
    _render_app(data=data, inputs=inputs)


if __name__ == "__main__":
    main()


from __future__ import annotations

import streamlit as st

from src.app.state import AppInputs


def apply_global_styles() -> None:
    st.markdown(
        """
<style>
  .tagline { font-size: 0.98rem; opacity: 0.85; margin-top: -0.25rem; }
  .closing { font-size: 0.95rem; opacity: 0.85; margin-top: 0.5rem; }
  .kpi-card { padding: 0.75rem 0.9rem; border: 1px solid rgba(49,51,63,0.2); border-radius: 12px; }
  .muted { opacity: 0.75; }
</style>
""",
        unsafe_allow_html=True,
    )


def render_header() -> None:
    st.title("Expansion Copilot — Sydney gyms")
    st.markdown('<div class="tagline">Turn place data into smarter expansion decisions.</div>', unsafe_allow_html=True)
    st.caption("For expansion prioritization, not prediction.")


def render_landing_panel(inputs: AppInputs) -> None:
    with st.container(border=True):
        left, right = st.columns([0.7, 0.3], gap="large")
        with left:
            st.subheader("What this answers")
            st.write("Where should I open my next gym in Sydney, and why?")
            st.caption(
                "This is decision support for expansion prioritization. It’s a proxy-based ranking, not demand forecasting."
            )
        with right:
            st.subheader("Current setup")
            st.write(f"**City**: {inputs.city}")
            st.write(f"**Business**: {inputs.business_type}")
            st.write(f"**Area unit**: {inputs.area_unit}")
            st.write(f"**Strategy mode**: {inputs.strategy_mode}")

    st.markdown(
        '<div class="closing">Businesses do not need more map data. They need fewer bad location decisions.</div>',
        unsafe_allow_html=True,
    )


def render_missing_data_panel(message: str) -> None:
    st.error(message)


from __future__ import annotations

import streamlit as st


def _percent(x: float) -> str:
    return f"{x*100:.0f}%"


def build_explanation_card(row, strategy_mode: str) -> None:
    """
    Product-style explanation derived from already-computed features.
    Deterministic (no LLM). The Streamlit hackathon app (`app/streamlit_app.py`) can optionally
    call OpenAI for an extra narrative via ``src/app/llm_narrative.py`` when enabled in the sidebar.
    """
    activity = float(row.get("commercial_activity_proxy", 0.0))
    comp = float(row.get("competitor_count", 0.0))
    compl = float(row.get("complementary_count", 0.0))
    sat = float(row.get("saturation_proxy", 0.0))
    qual = float(row.get("mean_data_quality_score", 0.0))
    div = float(row.get("unique_category_count", 0.0))

    summary = (
        "This area scores highly because it shows strong surrounding commercial activity and a supportive "
        "wellness ecosystem, while competition remains moderate relative to activity."
    )
    if strategy_mode == "low competition":
        summary = (
            "This area stands out under a low-competition strategy because competitor intensity is lower than peers, "
            "while activity remains sufficient to support a new opening."
        )
    if strategy_mode == "ecosystem-first":
        summary = (
            "This area stands out under an ecosystem-first strategy because complements and commercial activity are "
            "both strong, improving the odds of sustained member acquisition."
        )

    st.subheader("Why this location?")
    st.write(summary)

    with st.container(border=True):
        c1, c2 = st.columns([0.55, 0.45], gap="large")

        with c1:
            st.markdown("**Pros**")
            st.write(f"- Strong activity proxy (≈ {activity:,.0f} active POIs in-area).")
            st.write(f"- Supportive ecosystem ({compl:,.0f} complementary venues).")
            st.write(f"- Category diversity (≈ {div:,.0f} unique categories) suggests mixed-use catchment.")
            st.write(f"- Data quality is solid (≈ {qual:.2f}).")

        with c2:
            st.markdown("**Risks**")
            st.write(f"- Direct competition exists ({comp:,.0f} competitors).")
            st.write(f"- Saturation proxy (≈ {sat:.3f}) may indicate tighter share-of-wallet dynamics.")
            st.write("- Area-unit artifacts: boundaries may not match real neighborhoods or trade areas.")

    with st.container(border=True):
        st.markdown("**What to validate next (in real life)**")
        st.write("- Verify foot traffic and member demographics near candidate sites (not measured here).")
        st.write("- Identify differentiation vs. nearby competitors (class mix, price point, brand).")
        st.write("- Check site availability, parking/transit access, and local planning constraints.")


def demo_narrative_markdown() -> str:
    return (
        "**User**: Gym-chain expansion manager / growth lead\n\n"
        "**Question**: Where should I open next in Sydney, and why?\n\n"
        "**How the score works (high level)**:\n"
        "- Rewards: complement ecosystem, commercial activity proxy, category diversity, data quality\n"
        "- Penalizes: direct competition and saturation proxy\n\n"
        "**Limitations**:\n"
        "- This is **decision support**, not demand prediction or revenue forecasting\n"
        "- Activity is a **proxy** based on POI presence, not verified foot traffic\n"
        "- Area units are approximations (H3/locality) and may not match real trade areas\n"
    )


from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pydeck as pdk
import polars as pl
import streamlit as st

# Ensure `src/` is importable when Streamlit runs from different cwd.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.cities import CITIES
from src.config.geo import SYDNEY_BBOX
from src.data.categories import complement_groups, gym_competitor_category_ids, load_category_index
from src.data.schema import canonical_places_lazyframe
from src.features.h3_agg import aggregate_by_h3, sum_counts_for_categories
from src.scoring.data_quality import compute_data_quality_by_area
from src.scoring.opportunity import ScoreWeights, score_areas
from src.data.load_transport import load_all_transport_points
from src.config.business_taxonomies import BUSINESS_TEMPLATES
from src.app.llm_narrative import (
    generate_deterministic_area_narrative,
    generate_openai_area_narrative,
    metrics_payload_from_area_row,
    reload_openai_env,
)
from src.app.methodology import (
    render_housing_tab_notes,
    render_main_methodology_expander,
    render_market_tab_notes,
    render_transit_tab_notes,
)
from src.app.area_metrics import map_point_select, metrics_table_for_area_row, ordered_columns_for_tables


@st.cache_data(ttl=7200, show_spinner="Generating AI narrative…")
def _cached_openai_narrative(payload_json: str, area_id: str, business_label: str, city_label: str) -> str:
    return generate_openai_area_narrative(
        metrics=json.loads(payload_json),
        city_label=city_label,
        business_label=business_label,
        area_id=area_id,
    )


# Defaults now point at fast interim artifacts produced by Task 2.
DATA_DEFAULT = Path("data/interim/places_sydney.parquet")
CATEGORIES_DEFAULT = Path("data/interim/categories_clean.parquet")
RANKED_DEFAULT = Path("data/processed/sydney_ranked_areas_gym.parquet")
SA2_MARKET_FIT_GEOJSON = Path("data/processed/sydney_sa2_market_fit.geojson")
SA2_HOUSING_GEOJSON = Path("data/processed/sydney_sa2_housing_market.geojson")


def _load_city_places(data_path: Path, bbox) -> pl.DataFrame:
    # The interim file can already be Sydney-filtered, but we keep a bbox guardrail.
    lf, _ = canonical_places_lazyframe(data_path)
    b = bbox
    return (
        lf.filter(
            (pl.col("lat") >= b.min_lat)
            & (pl.col("lat") <= b.max_lat)
            & (pl.col("lon") >= b.min_lon)
            & (pl.col("lon") <= b.max_lon)
        )
        .collect(engine="streaming")
    )


@st.cache_data(show_spinner=False)
def cached_category_index(path: str) -> dict:
    idx = load_category_index(Path(path))
    return {"id_to_name": idx.id_to_name, "id_to_label": idx.id_to_label}


@st.cache_data(show_spinner=False)
def cached_city_places(path: str, bbox_key: tuple[float, float, float, float]) -> pl.DataFrame:
    from src.config.geo import BBox

    b = BBox(min_lat=bbox_key[0], max_lat=bbox_key[1], min_lon=bbox_key[2], max_lon=bbox_key[3])
    return _load_city_places(Path(path), b)


@st.cache_data(show_spinner=False)
def cached_h3_agg(path: str, h3_res: int, bbox_key: tuple[float, float, float, float]) -> dict:
    from src.config.geo import BBox

    b = BBox(min_lat=bbox_key[0], max_lat=bbox_key[1], min_lon=bbox_key[2], max_lon=bbox_key[3])
    df = _load_city_places(Path(path), b)
    res = aggregate_by_h3(df, h3_res=h3_res, keep_poi_rows=True)
    return {
        "areas": res.areas,
        "area_category_counts": res.area_category_counts,
        "area_pois": res.area_pois,
    }


@st.cache_data(show_spinner=False)
def cached_transport_points() -> pl.DataFrame:
    # Reads from data/raw/nsw_transport/* if present; returns empty DF if missing.
    return load_all_transport_points()


def _kind_color(kind: str | None) -> list[int]:
    k = (kind or "").lower()
    if "entrance" in k:
        return [0, 180, 255, 180]  # blue
    if "bus" in k and "shelter" in k:
        return [255, 195, 0, 180]  # yellow
    return [180, 180, 180, 140]  # grey


@st.cache_data(show_spinner=False)
def cached_sa2_market_fit_geojson(path: str, file_mtime: float) -> dict | None:
    p = Path(path)
    if not p.exists():
        return None
    import json

    return json.loads(p.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def cached_geojson(path: str, file_mtime: float) -> dict | None:
    p = Path(path)
    if not p.exists():
        return None
    import json

    return json.loads(p.read_text(encoding="utf-8"))


def _norm01(s: pl.Series) -> pl.Series:
    mn = float(s.min()) if s.len() else 0.0
    mx = float(s.max()) if s.len() else 0.0
    denom = (mx - mn) if (mx - mn) > 1e-9 else 1.0
    return (s - mn) / denom


def _match_category_ids(idx_dict: dict, keywords: list[str]) -> set[int]:
    kws = [k.strip().lower() for k in keywords if k and k.strip()]
    if not kws:
        return set()
    out: set[int] = set()
    id_to_name = idx_dict.get("id_to_name", {})
    id_to_label = idx_dict.get("id_to_label", {})
    for k, name in id_to_name.items():
        txt = f"{name} | {id_to_label.get(k, '')}".lower()
        if any(kw in txt for kw in kws):
            try:
                out.add(int(k))
            except Exception:
                continue
    return out


def _categories_preview_df(idx_dict: dict, ids: list[int]) -> pl.DataFrame:
    id_to_name = idx_dict.get("id_to_name", {})
    id_to_label = idx_dict.get("id_to_label", {})
    rows = []
    for i in ids[:250]:
        rows.append(
            {
                "category_id": int(i),
                "category_name": id_to_name.get(i),
                "category_label": id_to_label.get(i),
            }
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame({"category_id": [], "category_name": [], "category_label": []})


@st.cache_data(show_spinner=False)
def cached_categories_df(categories_path: str) -> pl.DataFrame:
    p = Path(categories_path)
    if not p.exists():
        return pl.DataFrame({"category_id": [], "category_name": [], "category_label": []})
    return pl.read_parquet(str(p), columns=["category_id", "category_name", "category_label"])


def _category_search_df(categories_df: pl.DataFrame, query: str, limit: int = 50) -> pl.DataFrame:
    q = (query or "").strip().lower()
    if not q:
        return categories_df.head(0)
    return (
        categories_df.with_columns(
            pl.concat_str(
                [pl.col("category_name").fill_null(""), pl.lit(" | "), pl.col("category_label").fill_null("")],
                separator="",
            )
            .str.to_lowercase()
            .alias("_txt")
        )
        .filter(pl.col("_txt").str.contains(q))
        .select(["category_id", "category_name", "category_label"])
        .head(limit)
    )


def _top_categories_in_area(area_category_counts: pl.DataFrame, idx_dict: dict, h3_id: str, n: int = 10) -> pl.DataFrame:
    df = (
        area_category_counts.filter(pl.col("h3") == h3_id)
        .sort("cnt", descending=True)
        .head(n)
        .with_columns(
            pl.col("category_id")
            .map_elements(lambda x: idx_dict["id_to_name"].get(x, x), return_dtype=pl.Utf8)
            .alias("category_name")
        )
        .select(["category_name", "cnt"])
    )
    return df


def _decision_checklist(template_key: str) -> dict:
    base = {
        "validate_next": [
            "Do a site walk at peak times (weekday AM/PM + weekend).",
            "Check access: transit, parking, and pedestrian visibility.",
            "Review the closest competitors: pricing, positioning, and differentiation.",
        ],
        "red_flags": [
            "Very high direct competition within a short walk.",
            "Low data coverage / sparse POI context (treat as uncertain).",
            "Access constraints (poor transit / hard parking) inconsistent with the concept.",
        ],
        "go_no_go": [
            "Go if the concept can differentiate and the area has strong supporting ecosystem.",
            "No-go if competition is intense and you cannot win on price/offer/location convenience.",
        ],
    }

    if template_key == "cafe":
        base["validate_next"] = [
            "Measure morning footfall and coffee queue density (7–10am weekdays).",
            "Check nearby office/education demand and weekend activity.",
            "Assess co-tenancy: does the surrounding mix drive repeat visits?",
        ]
        base["red_flags"] = [
            "Too many comparable cafés within a 2–3 minute walk.",
            "Low morning activity proxy relative to the rest of Sydney.",
            "Poor frontage/visibility or no natural stopping points.",
        ]
        base["go_no_go"] = [
            "Go if you can own a clear niche (speed, specialty, seating, brand) and access is strong.",
            "No-go if you’re competing on commodity coffee in a saturated strip.",
        ]

    if template_key == "clinic":
        base["validate_next"] = [
            "Check referral ecosystem (pharmacies, gyms, allied health nearby).",
            "Confirm parking + accessibility (patients often require convenience).",
            "Validate competitor specialization gaps (e.g., sports physio vs general).",
        ]
        base["red_flags"] = [
            "Specialist-heavy area with entrenched incumbents.",
            "Low accessibility for short appointments (parking/transit).",
            "Mismatch between market fit and intended price point.",
        ]
        base["go_no_go"] = [
            "Go if the area has complements + access and you can offer a clear specialty.",
            "No-go if acquisition relies on price-only competition in a crowded cluster.",
        ]

    if template_key == "coworking":
        base["validate_next"] = [
            "Check weekday daytime activity and transit access.",
            "Assess nearby cafés/food options (member convenience).",
            "Validate competition: are you selling price, community, or premium space?",
        ]
        base["red_flags"] = [
            "Weak weekday activity proxy (area is mostly residential/off-peak).",
            "Poor transit access for commuters.",
            "High existing supply of similar coworking offerings nearby.",
        ]
        base["go_no_go"] = [
            "Go if transit + daytime ecosystem support daily usage and you can differentiate.",
            "No-go if you cannot sustain occupancy vs entrenched operators.",
        ]

    if template_key == "gym":
        base["validate_next"] = [
            "Validate catchment: commute patterns + nearby residential/office mix.",
            "Audit competitor offerings (class mix, price points, peak capacity).",
            "Confirm access: transit + parking + safe walking routes.",
        ]
        base["red_flags"] = [
            "Very high competitor density with similar positioning.",
            "Low complement ecosystem (weak wellness/health adjacency).",
            "Operational constraints: parking, space, local noise restrictions.",
        ]
        base["go_no_go"] = [
            "Go if complements + activity are strong and competition is moderate for your positioning.",
            "No-go if saturation is high and differentiation is unclear.",
        ]

    return base

@st.cache_data(show_spinner=False)
def cached_custom_rank(
    categories_path: str,
    data_path: str,
    h3_res: int,
    competitor_keywords_csv: str,
    complement_keywords_csv: str,
    exclude_keywords_csv: str,
) -> dict:
    """
    Custom (beta) business ranking.
    Uses:
    - area_category_counts + keyword match over category labels to build competitor/complement category-id sets
    - area_pois -> data quality
    - joins in transit_access_score + market_fit_score from gym-ranked output if available (these are business-agnostic)
    """
    idx_dict = cached_category_index(categories_path)
    city_key = st.session_state.get("demo_city", "sydney")
    city = CITIES.get(city_key, CITIES["sydney"])
    bbox_key = (city.bbox.min_lat, city.bbox.max_lat, city.bbox.min_lon, city.bbox.max_lon)
    agg = cached_h3_agg(data_path, h3_res, bbox_key)
    areas = agg["areas"]
    area_category_counts = agg["area_category_counts"]
    area_pois = agg["area_pois"]

    comp_kws = [x for x in competitor_keywords_csv.split(",")]
    compl_kws = [x for x in complement_keywords_csv.split(",")]
    excl_kws = [x for x in exclude_keywords_csv.split(",")]

    comp_ids = _match_category_ids(idx_dict, comp_kws)
    compl_ids = _match_category_ids(idx_dict, compl_kws)
    excl_ids = _match_category_ids(idx_dict, excl_kws)
    # Exclusions override.
    comp_ids = comp_ids - excl_ids
    compl_ids = compl_ids - excl_ids

    competitor_counts = sum_counts_for_categories(area_category_counts, comp_ids, out_col="competitor_count")
    complement_counts = sum_counts_for_categories(area_category_counts, compl_ids, out_col="complement_count")
    dq = compute_data_quality_by_area(area_pois)

    scored = score_areas(
        areas=areas,
        competitor_counts=competitor_counts,
        complement_counts=complement_counts,
        data_quality=dq,
        weights=ScoreWeights(competitor_weight=1.2, complement_weight=1.0, density_weight=0.0, data_quality_weight=0.5),
    )
    # Conform to downstream guardrail expectations used by the main demo:
    # - active_poi_count is used for filtering; in this lightweight custom mode we treat poi_count as "active".
    scored = scored.with_columns(pl.col("poi_count").alias("active_poi_count"))

    # Attach business-agnostic layers if available.
    ctx = None
    gym_ranked = Path(f"data/processed/{city.key}_ranked_areas_gym.parquet")
    if gym_ranked.exists():
        gym_cols = pl.read_parquet(gym_ranked).columns
        keep = ["transit_access_score", "market_fit_score", "seifa_irsad_decile"]
        keep = [c for c in keep if c in gym_cols]
        ctx = pl.read_parquet(gym_ranked).select(
            [
                pl.col("area_id").alias("h3"),
                *keep,
            ]
        )
    if ctx is not None:
        scored = scored.join(ctx, on="h3", how="left")

    # Ensure fields exist for UI/tooltips even if context join is missing.
    if "transit_access_score" not in scored.columns:
        scored = scored.with_columns(pl.lit(None).cast(pl.Float64).alias("transit_access_score"))
    if "market_fit_score" not in scored.columns:
        scored = scored.with_columns(pl.lit(None).cast(pl.Float64).alias("market_fit_score"))
    if "seifa_irsad_decile" not in scored.columns:
        scored = scored.with_columns(pl.lit(None).cast(pl.Int64).alias("seifa_irsad_decile"))

    # Lightweight reasons for custom mode (deterministic).
    scored = scored.with_columns(
        pl.concat_str(
            [
                pl.when(pl.col("complement_log") > pl.col("complement_log").median())
                .then(pl.lit("Strong complements"))
                .otherwise(pl.lit("")),
                pl.lit("; "),
                pl.when(pl.col("density_log") > pl.col("density_log").median())
                .then(pl.lit("High activity"))
                .otherwise(pl.lit("")),
                pl.lit("; "),
                pl.when(pl.col("competitor_log") < pl.col("competitor_log").median())
                .then(pl.lit("Lower competition"))
                .otherwise(pl.lit("")),
            ],
            separator="",
        )
        .str.replace_all(r"(^;\\s+)|(;\\s+;\\s+)+|(;\\s+$)", "")
        .alias("reasons_top3")
    )

    return {
        "scored": scored,
        "comp_ids": sorted(list(comp_ids))[:2000],
        "compl_ids": sorted(list(compl_ids))[:2000],
        "excl_ids": sorted(list(excl_ids))[:2000],
    }

def main() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env")
        load_dotenv()  # current working directory (fallback if Streamlit was started elsewhere)
    except Exception:
        pass

    st.set_page_config(page_title="Expansion Copilot — Sydney", layout="wide")
    with st.sidebar:
        st.header("City")
        st.selectbox("Demo city", options=list(CITIES.keys()), index=0, key="demo_city")
    city = CITIES.get(st.session_state.get("demo_city", "sydney"), CITIES["sydney"])
    data_default = Path(f"data/interim/places_{city.key}.parquet")
    ranked_default = Path(f"data/processed/{city.key}_ranked_areas_gym.parquet")
    sa2_market_geo = Path(f"data/processed/{city.key}_sa2_market_fit.geojson")
    sa2_housing_geo = Path(f"data/processed/{city.key}_sa2_housing_market.geojson")

    # Streamlit persists `data_path` / `ranked_path` across reruns. If the user switches city,
    # old paths can still point at the previous city (e.g. Sydney parquet + Melbourne bbox → 0 POIs).
    if st.session_state.get("_last_demo_city") != city.key:
        st.session_state["_last_demo_city"] = city.key
        st.session_state["data_path"] = str(data_default)
        tpl_now = st.session_state.get("business_template", "gym")
        if tpl_now != "custom (beta)":
            st.session_state["ranked_path"] = str(
                Path(f"data/processed/{city.key}_ranked_areas_{tpl_now}.parquet")
            )
        st.session_state.pop("selected_h3", None)
        st.session_state["pinned"] = []

    tpl_key = st.session_state.get("business_template", "gym")
    tpl_label = (
        BUSINESS_TEMPLATES[tpl_key].label
        if tpl_key in BUSINESS_TEMPLATES
        else ("Custom business" if tpl_key == "custom (beta)" else "Business")
    )
    st.title(f"Expansion Copilot — {city.label} ({tpl_label})")
    st.caption("Turn place data into smarter expansion decisions.")
    st.caption(f"Answer: Where should I open my next {tpl_label.lower()} in {city.label}, and why?")

    global_summary = Path("data/exports/global_cafe_metro_summary.csv")
    with st.expander("Global café comparison (POI-only, six metros)", expanded=False):
        st.markdown(
            "**Café fabric index (demo):** same POI taxonomy and scoring everywhere—use it to **shortlist** "
            "markets, not to pick capital deployment (no rent, labor, or capex). "
            "**Fill data:** set `HF_TOKEN`, preview with "
            "`python scripts/bootstrap_global_cafe_demo.py --dry-run --dt <release>`, "
            "then fetch **one metro at a time** e.g. "
            "`python scripts/bootstrap_global_cafe_demo.py --dt <release> --metros singapore`. "
            "Sydney can be built from `places_au.parquet` without HF. "
            "Or run `python scripts/run_global_cafe_comparison.py --metros all` after parquets exist."
        )
        if global_summary.exists():
            gdf = pl.read_csv(global_summary)
            n_ok = gdf.filter(pl.col("status") == "ok").height
            n_total = gdf.height
            st.caption(f"Metros with data: **{n_ok} / {n_total}** (others need country extracts + bbox subset).")
            ready = gdf.filter(pl.col("status") == "ok")
            if ready.height > 0:
                st.markdown("**Ranked metros (POI layer only)**")
                st.dataframe(ready, use_container_width=True, hide_index=True)
            need = gdf.filter(pl.col("status") == "missing_parquet")
            if need.height > 0:
                st.markdown("**Still needed** — subset Foursquare OS per country, then:")
                st.code(
                    "python scripts/build_city_subset.py --city <metro> --in data/interim/places_<CC>.parquet\n"
                    "python scripts/run_global_cafe_comparison.py",
                    language="bash",
                )
                st.dataframe(
                    need.select(["metro_key", "label", "status"]),
                    use_container_width=True,
                    hide_index=True,
                )
            other = gdf.filter(pl.col("status") == "no_qualified_cells")
            if other.height > 0:
                st.warning("Some metros have parquet but no cells above the activity threshold—lower `--min-active-poi` in the script if needed.")
                st.dataframe(other, use_container_width=True, hide_index=True)
        else:
            st.info(f"No export yet at `{global_summary}` — run the script after preparing metro parquet files.")

    render_main_methodology_expander(expanded=False)

    with st.sidebar:
        st.header("Demo controls")
        judge_mode = st.toggle(
            "Judge mode (recommended)",
            value=True,
            help="Applies stable defaults for a smooth demo. You can still override settings below.",
            key="demo_judge_mode",
        )
        st.caption("One-click hero states:")
        hero_cols = st.columns(3)
        with hero_cols[0]:
            if st.button("Map", use_container_width=True, key="hero_map"):
                st.session_state["hero_preset"] = "map"
        with hero_cols[1]:
            if st.button("Why", use_container_width=True, key="hero_why"):
                st.session_state["hero_preset"] = "why"
        with hero_cols[2]:
            if st.button("Compare", use_container_width=True, key="hero_compare"):
                st.session_state["hero_preset"] = "compare"

        # Apply demo preset values before rendering the rest of the widgets.
        preset = st.session_state.get("hero_preset")
        if judge_mode:
            # Defaults tuned for stability (avoid tiny cells & low-quality noise).
            st.session_state.setdefault("sc_top_k", 30)
            st.session_state.setdefault("sc_min_active", 200)
            st.session_state.setdefault("sc_min_quality", 0.60)
            st.session_state.setdefault("sc_h3_res", 8)
            st.session_state.setdefault("sc_competitor_mode", "strict")

            # Hero-state specific tweaks.
            if preset == "map":
                st.session_state["sc_top_k"] = 30
            elif preset == "why":
                # Make "Why here?" read more ecosystem-led.
                st.session_state["sc_complement_w"] = 1.3
                st.session_state["sc_competitor_w"] = 1.0
                st.session_state["sc_quality_w"] = 0.7
            elif preset == "compare":
                st.session_state["sc_top_k"] = 40

        st.divider()
        st.header("Business")
        business_template = st.selectbox(
            "Business type",
            options=["gym", "cafe", "clinic", "coworking", "custom (beta)"],
            index=0,
            help="Template-based MVP. Each business type has its own competitor/complement taxonomy and scoring weights.",
            key="business_template",
        )

        # Keep ranked_path synced to the selected template unless user overrides it in Advanced.
        # Streamlit widgets with the same key keep old values, so we explicitly update on template change.
        if st.session_state.get("_last_business_template") != business_template:
            st.session_state["_last_business_template"] = business_template
            if business_template != "custom (beta)":
                st.session_state["ranked_path"] = str(
                    Path(f"data/processed/{city.key}_ranked_areas_{business_template}.parquet")
                )
            # Reset selection to avoid stale selected_h3 pointing to a removed row.
            st.session_state.pop("selected_h3", None)
            st.session_state["pinned"] = []

        st.divider()
        st.header("AI narrative")
        st.toggle(
            "OpenAI summary on “Why here?”",
            value=False,
            help="Sends only numeric scores and counts (no raw POI lists). Set OPENAI_API_KEY in the environment.",
            key="use_openai_narrative",
        )
        st.caption(
            "You always get a **built-in narrative** from the same scores. OpenAI adds optional prose when the API works "
            "(needs HTTPS to api.openai.com; connection issues are usually network/VPN—not billing)."
        )
        if st.button("Clear AI narrative cache", help="Retry after fixing .env or network.", key="clear_openai_cache"):
            _cached_openai_narrative.clear()
            st.success("Cache cleared. Try “Why here?” again.")

        st.divider()
        st.header("Strategy")
        top_k = st.slider("Top areas to rank", 10, 100, int(st.session_state.get("sc_top_k", 30)), key="sc_top_k")
        positioning = st.selectbox(
            "Positioning",
            options=["balanced", "premium", "value"],
            index=0,
            help="Premium favors higher market-fit areas; Value relaxes that constraint.",
            key="strategy_positioning",
        )
        min_active = st.slider(
            "Guardrail: min active POIs per area",
            0,
            3000,
            int(st.session_state.get("sc_min_active", 200)),
            50,
            key="sc_min_active",
        )
        min_quality = st.slider(
            "Guardrail: min mean data quality",
            0.0,
            1.0,
            float(st.session_state.get("sc_min_quality", 0.60)),
            0.05,
            key="sc_min_quality",
        )

        # Advanced controls kept out of the judge's first 20 seconds.
        with st.expander("Advanced settings", expanded=not judge_mode):
            st.subheader("Data")
            data_path = st.text_input("Places parquet", value=str(data_default), key="data_path")
            categories_path = st.text_input("Categories parquet", value=str(CATEGORIES_DEFAULT), key="categories_path")
            h3_res = st.slider(
                "Area granularity (H3 resolution)",
                min_value=6,
                max_value=10,
                value=int(st.session_state.get("sc_h3_res", 8)),
                key="sc_h3_res",
            )
            ranked_path = st.text_input(
                "Precomputed ranked areas (optional)",
                value=str(
                    Path(
                        f"data/processed/{city.key}_ranked_areas_{business_template}.parquet"
                        if business_template != "custom (beta)"
                        else f"data/processed/{city.key}_ranked_areas_gym.parquet"
                    )
                ),
                key="ranked_path",
            )

            st.divider()
            st.subheader("Scoring weights")
            competitor_w = st.slider(
                "Competitor penalty",
                0.0,
                3.0,
                float(st.session_state.get("sc_competitor_w", 1.2)),
                0.1,
                key="sc_competitor_w",
            )
            complement_w = st.slider(
                "Complement boost",
                0.0,
                3.0,
                float(st.session_state.get("sc_complement_w", 1.0)),
                0.1,
                key="sc_complement_w",
            )
            quality_w = st.slider(
                "Data quality weight",
                0.0,
                2.0,
                float(st.session_state.get("sc_quality_w", 0.6)),
                0.1,
                key="sc_quality_w",
            )
            density_w = st.slider(
                "General density weight",
                0.0,
                1.5,
                float(st.session_state.get("sc_density_w", 0.0)),
                0.1,
                key="sc_density_w",
            )
        # Provide safe defaults if Advanced wasn't opened yet.
        data_path = st.session_state.get("data_path", str(data_default))
        categories_path = st.session_state.get("categories_path", str(CATEGORIES_DEFAULT))
        h3_res = int(st.session_state.get("sc_h3_res", 8))
        ranked_path = st.session_state.get("ranked_path", str(ranked_default))
        competitor_w = float(st.session_state.get("sc_competitor_w", 1.2))
        complement_w = float(st.session_state.get("sc_complement_w", 1.0))
        quality_w = float(st.session_state.get("sc_quality_w", 0.6))
        density_w = float(st.session_state.get("sc_density_w", 0.0))

        st.divider()
        st.header("Competitor definition")
        if st.session_state.get("business_template", "gym") == "gym":
            competitor_mode = st.radio(
                "Gym competitor mode",
                options=["strict", "broad"],
                index=0,
                help="Strict = gyms/fitness/health clubs only. Broad = includes yoga/pilates/boxing/crossfit etc.",
                key="sc_competitor_mode",
            )
        else:
            competitor_mode = "strict"
            st.caption("Competitor mode is gym-specific; other business types use template/custom taxonomy.")

        if business_template == "custom (beta)":
            st.divider()
            st.subheader("Custom business (beta)")
            st.caption(
                "How it works: we match your keywords against Foursquare category names/labels, then count those categories per area. "
                "If you get 0 matches, use the search below to discover the exact category wording."
            )
            custom_comp = st.text_input("Competitor keywords", value="cafe, coffee", key="custom_comp_kw")
            custom_compl = st.text_input("Complement keywords", value="office, transit, station, retail", key="custom_compl_kw")
            custom_excl = st.text_input("Exclusion keywords (optional)", value="", key="custom_excl_kw")

            with st.expander("Category search (find the right keywords)", expanded=False):
                cats_df = cached_categories_df(str(st.session_state.get("categories_path", str(CATEGORIES_DEFAULT))))
                q = st.text_input("Search category labels", value="bubble", key="custom_cat_search")
                matches = _category_search_df(cats_df, q, limit=60)
                if matches.height == 0:
                    st.caption("No matches. Try shorter terms (e.g., `tea`, `drink`, `dessert`, `juice`, `cafe`).")
                else:
                    st.dataframe(matches, use_container_width=True, hide_index=True)

            with st.expander("Matched categories preview", expanded=False):
                idx_preview = cached_category_index(str(st.session_state.get("categories_path", str(CATEGORIES_DEFAULT))))
                res_preview = cached_custom_rank(
                    categories_path=str(st.session_state.get("categories_path", str(CATEGORIES_DEFAULT))),
                    data_path=str(st.session_state.get("data_path", str(DATA_DEFAULT))),
                    h3_res=int(st.session_state.get("sc_h3_res", 8)),
                    competitor_keywords_csv=str(st.session_state.get("custom_comp_kw", "")),
                    complement_keywords_csv=str(st.session_state.get("custom_compl_kw", "")),
                    exclude_keywords_csv=str(st.session_state.get("custom_excl_kw", "")),
                )
                comp_ids = res_preview.get("comp_ids", [])
                compl_ids = res_preview.get("compl_ids", [])
                excl_ids = res_preview.get("excl_ids", [])

                c1, c2, c3 = st.columns(3)
                c1.metric("Competitor categories", len(comp_ids))
                c2.metric("Complement categories", len(compl_ids))
                c3.metric("Excluded categories", len(excl_ids))

                st.markdown("**Competitor matches**")
                st.dataframe(_categories_preview_df(idx_preview, comp_ids), use_container_width=True, hide_index=True)
                st.markdown("**Complement matches**")
                st.dataframe(_categories_preview_df(idx_preview, compl_ids), use_container_width=True, hide_index=True)
                if excl_ids:
                    st.markdown("**Exclusions**")
                    st.dataframe(_categories_preview_df(idx_preview, excl_ids), use_container_width=True, hide_index=True)

        with st.expander("Demo narrative mode", expanded=False):
            st.markdown(
                "**User**: Gym-chain expansion manager / growth lead\n\n"
                "**Question**: Where should I open next in Sydney, and why?\n\n"
                "**How the score works (high level)**:\n"
                "- Rewards: complement ecosystem, commercial activity proxy, category diversity, data quality, transit access, market fit\n"
                "- Penalizes: direct competition and saturation proxy\n\n"
                "**Limitations**:\n"
                "- This is **decision support**, not revenue or demand prediction\n"
                "- Activity is a **proxy** based on POI presence, not verified foot traffic\n"
                "- Market fit uses SEIFA as an affluence proxy, not a guarantee of demand\n"
                "- Area boundaries are approximations (H3) and not trade areas\n"
            )

    # Load
    idx_dict = cached_category_index(categories_path)
    idx = load_category_index(Path(categories_path))  # lightweight (1278 rows)
    bbox_key = (city.bbox.min_lat, city.bbox.max_lat, city.bbox.min_lon, city.bbox.max_lon)
    places = cached_city_places(data_path, bbox_key)
    if places.height == 0:
        st.error(
            f"No POIs found in the {city.label} bounding box for this local dataset. "
            "Regenerate `data/au_places.parquet` with a larger `--max-rows`, or run "
            f"`./.venv/bin/python scripts/build_city_subset.py --city {city.key}`."
        )
        st.stop()

    agg = cached_h3_agg(data_path, h3_res, bbox_key)
    areas = agg["areas"]
    area_category_counts = agg["area_category_counts"]
    area_pois = agg["area_pois"]

    # Category sets (used only for the live fallback scoring path).
    gym_ids = gym_competitor_category_ids(idx, mode=competitor_mode)
    comp_groups = complement_groups(idx)

    # Simple UI to toggle complement groups (gym fallback only).
    with st.sidebar:
        st.subheader("Complement groups")
        if st.session_state.get("business_template", "gym") != "gym":
            st.caption("Complement groups are gym-specific in the live fallback mode.")
            enabled_groups = list(comp_groups.keys())
        else:
            enabled_groups = []
            for g in comp_groups.keys():
                if st.checkbox(g, value=True, key=f"cg_{g}"):
                    enabled_groups.append(g)
        complement_ids = set().union(*[comp_groups[g] for g in enabled_groups]) if enabled_groups else set()

    competitor_counts = sum_counts_for_categories(area_category_counts, gym_ids, out_col="competitor_count")
    complement_counts = sum_counts_for_categories(area_category_counts, complement_ids, out_col="complement_count")
    data_quality = compute_data_quality_by_area(area_pois)

    # If precomputed ranked areas exist, prefer them (fast, stable, includes reasons).
    # Prefer template-specific ranked outputs for speed and correctness.
    tpl = st.session_state.get("business_template", "gym")
    if tpl == "custom (beta)":
        res = cached_custom_rank(
            categories_path=str(categories_path),
            data_path=str(data_path),
            h3_res=int(h3_res),
            competitor_keywords_csv=str(st.session_state.get("custom_comp_kw", "")),
            complement_keywords_csv=str(st.session_state.get("custom_compl_kw", "")),
            exclude_keywords_csv=str(st.session_state.get("custom_excl_kw", "")),
        )
        scored = res["scored"]
        # Conform to downstream expected names.
        scored = scored.rename({"poi_count": "poi_count"}).with_columns(
            [pl.lit("").alias("reasons_top3")] if "reasons_top3" not in scored.columns else []
        )
    else:
        ranked_file = Path(ranked_path)
        if not ranked_file.exists():
            ranked_file = Path(f"data/processed/{city.key}_ranked_areas_{tpl}.parquet")
        if ranked_file.exists():
            scored = pl.read_parquet(ranked_file).rename({"area_id": "h3"})
        else:
            scored = None
        # Ensure expected columns exist for UI; otherwise fall back.
        expected = {"h3", "opportunity_score", "competitor_count", "complementary_count", "active_poi_count", "mean_data_quality_score"}
        if expected.issubset(set(scored.columns)):
            scored = scored.with_columns(
                [
                    pl.col("complementary_count").alias("complement_count"),
                    pl.col("mean_data_quality_score").alias("data_quality_score"),
                    pl.col("active_poi_count").alias("poi_count"),
                ]
            )
        else:
            scored = None
    # (scored may already be set for custom)

    if scored is None:
        scored = score_areas(
            areas=areas,
            competitor_counts=competitor_counts,
            complement_counts=complement_counts,
            data_quality=data_quality,
            weights=ScoreWeights(
                competitor_weight=competitor_w,
                complement_weight=complement_w,
                density_weight=density_w,
                data_quality_weight=quality_w,
            ),
        ).with_columns(
            [
                pl.col("poi_count").alias("active_poi_count"),
                pl.lit(None).cast(pl.Utf8).alias("reasons_top3"),
            ]
        )

    # Guardrails
    scored = scored.filter((pl.col("active_poi_count") >= min_active) & (pl.col("data_quality_score") >= min_quality))
    # Positioning filter (simple + explainable)
    if positioning == "premium" and "market_fit_score" in scored.columns:
        scored = scored.filter(pl.col("market_fit_score") >= 0.60)
    # Ensure we have centroids for mapping (precomputed ranked file may not include them).
    if "lat_mean" not in scored.columns or "lon_mean" not in scored.columns:
        scored = scored.join(areas.select(["h3", "lat_mean", "lon_mean"]), on="h3", how="left")
    top = scored.head(top_k)

    # Sticky shortlist state
    if "pinned" not in st.session_state:
        st.session_state["pinned"] = []
    if "selected_h3" not in st.session_state:
        st.session_state["selected_h3"] = top["h3"].to_list()[0] if top.height else None
    if "shortlist_notes" not in st.session_state:
        st.session_state["shortlist_notes"] = {}
    if "shortlist_status" not in st.session_state:
        st.session_state["shortlist_status"] = {}

    # Calibration: how many POIs match competitor definition in Sydney overall?
    with st.sidebar:
        with st.expander("Calibration (dataset sanity)", expanded=False):
            total_pois = int(places.height)
            total_competitor_pois = int(
                area_pois.filter(pl.col("category_id").is_in(list(gym_ids))).select(pl.len()).item()
            )
            st.caption(f"POIs in city subset: **{total_pois:,}**")
            st.caption(f"POIs counted as gym competitors: **{total_competitor_pois:,}**")

    # Layout
    tab_overview, tab_detail, tab_compare, tab_transit, tab_market, tab_housing = st.tabs(
        ["Overview", "Why here?", "Compare", "Transit", "Market fit (SA2)", "Housing"]
    )

    with tab_overview:
        left, right = st.columns([0.58, 0.42], gap="large")

        with left:
            st.subheader("Candidate areas (map)")
            map_df = top.select(map_point_select(top)).to_pandas()

            # Color by opportunity score (red -> green).
            s_min = float(map_df["opportunity_score"].min())
            s_max = float(map_df["opportunity_score"].max())
            denom = (s_max - s_min) if (s_max - s_min) > 1e-9 else 1.0
            map_df["score_norm"] = (map_df["opportunity_score"] - s_min) / denom
            map_df["fill_r"] = (220 - 160 * map_df["score_norm"]).astype(int)
            map_df["fill_g"] = (60 + 160 * map_df["score_norm"]).astype(int)
            map_df["fill_b"] = 80
            map_df["fill_a"] = 150

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df,
                get_position="[lon, lat]",
                get_radius="complement_count",
                radius_scale=3.0,
                radius_min_pixels=4,
                radius_max_pixels=30,
                get_fill_color="[fill_r, fill_g, fill_b, fill_a]",
                pickable=True,
            )
            view_state = pdk.ViewState(latitude=city.map_center_lat, longitude=city.map_center_lon, zoom=city.map_zoom)
            st.pydeck_chart(
                pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    tooltip={
                        "text": "Suburb (SAL): {sal_name_2021}\nSA2: {sa2_name_2021}\nScore: {opportunity_score}\n"
                        "Competitors: {competitor_count}\nComplements: {complement_count}\n"
                        "Transit access: {transit_access_score}\nMarket fit: {market_fit_score}\n"
                        "Rent affordability: {rent_affordability_score}"
                    },
                )
            )

            st.caption("Circle size = complements; color = opportunity score (red → green).")

        with right:
            st.subheader("Ranked shortlist")
            st.caption("Tip: select an area, then click **Pin** to add it to Compare.")
            # Human-readable labels (SAL + SA2). Keep H3 hidden unless debugging.
            sal_map = {}
            sa2_map = {}
            if "sal_name_2021" in top.columns:
                sal_map = dict(zip(top["h3"].to_list(), top["sal_name_2021"].to_list(), strict=False))
            if "sa2_name_2021" in top.columns:
                sa2_map = dict(zip(top["h3"].to_list(), top["sa2_name_2021"].to_list(), strict=False))

            def _fmt_area(x: str) -> str:
                sal = sal_map.get(x)
                sa2 = sa2_map.get(x)
                if sal and sa2:
                    return f"{sal} • {sa2}"
                if sal:
                    return str(sal)
                if sa2:
                    return str(sa2)
                return "Unknown area"
            selected_overview = st.selectbox(
                "Selected area (for pinning)",
                options=top["h3"].to_list(),
                index=0,
                key="sel_overview",
                format_func=_fmt_area,
            )
            col_pin, col_clear = st.columns([0.5, 0.5])
            with col_pin:
                if st.button("Pin selected area"):
                    pins = list(st.session_state["pinned"])
                    if selected_overview not in pins:
                        pins.append(selected_overview)
                        st.session_state["pinned"] = pins[:3]
            with col_clear:
                if st.button("Clear pins"):
                    st.session_state["pinned"] = []

            st.dataframe(
                top.select(ordered_columns_for_tables(top)),
                use_container_width=True,
                hide_index=True,
            )

            with st.expander("Advanced / debug", expanded=False):
                st.caption("H3 ids are hidden by default for judges. Use this only for traceability/exports.")
                st.dataframe(
                    top.select(
                        [
                            "h3",
                            *(
                                ["sal_name_2021"]
                                if "sal_name_2021" in top.columns
                                else []
                            ),
                            *(
                                ["sa2_name_2021"]
                                if "sa2_name_2021" in top.columns
                                else []
                            ),
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            if st.button("Export shortlist to CSV"):
                out = Path("data/exports")
                out.mkdir(parents=True, exist_ok=True)
                out_path = out / "shortlist.csv"
                top.write_csv(out_path)
                st.success(f"Exported to {out_path}")

            st.divider()
            st.subheader("Shortlist workflow")
            pins_now = list(st.session_state.get("pinned", []))
            if not pins_now:
                st.caption("Pin 1–3 areas to create a shortlist with notes and next steps.")
            else:
                for pid in pins_now:
                    title = pid
                    try:
                        trow = top.filter(pl.col("h3") == pid).select(["opportunity_score"]).to_dicts()
                        if trow:
                            title = f"{pid} (score {float(trow[0]['opportunity_score']):.2f})"
                    except Exception:
                        pass
                    with st.expander(title, expanded=False):
                        st.session_state["shortlist_status"].setdefault(pid, "Investigate")
                        st.session_state["shortlist_notes"].setdefault(pid, "")
                        st.selectbox(
                            "Status",
                            ["Investigate", "Visit", "Contact broker", "Defer"],
                            key=f"sl_status_{pid}",
                            index=["Investigate", "Visit", "Contact broker", "Defer"].index(
                                st.session_state["shortlist_status"].get(pid, "Investigate")
                            ),
                        )
                        # Sync selectbox to dict
                        st.session_state["shortlist_status"][pid] = st.session_state.get(f"sl_status_{pid}", "Investigate")
                        st.text_area(
                            "Notes",
                            key=f"sl_notes_{pid}",
                            value=st.session_state["shortlist_notes"].get(pid, ""),
                            height=90,
                            placeholder="e.g., Check lease availability near station; validate competitor class mix; visit peak times.",
                        )
                        st.session_state["shortlist_notes"][pid] = st.session_state.get(f"sl_notes_{pid}", "")

                if st.button("Export shortlist + notes (CSV)", use_container_width=True):
                    out = Path("data/exports")
                    out.mkdir(parents=True, exist_ok=True)
                    out_path = out / "shortlist_with_notes.csv"
                    base = top.filter(pl.col("h3").is_in(pins_now))
                    extra = pl.DataFrame(
                        {
                            "h3": pins_now,
                            "shortlist_status": [st.session_state["shortlist_status"].get(x, "") for x in pins_now],
                            "shortlist_notes": [st.session_state["shortlist_notes"].get(x, "") for x in pins_now],
                        }
                    )
                    base.join(extra, on="h3", how="left").write_csv(out_path)
                    st.success(f"Exported to {out_path}")

    with tab_detail:
        st.subheader("Why this area?")
        default_selected = st.session_state.get("selected_h3") or (top["h3"].to_list()[0] if top.height else None)
        selected = st.selectbox(
            "Select an area",
            options=top["h3"].to_list(),
            index=top["h3"].to_list().index(default_selected) if default_selected in top["h3"].to_list() else 0,
            format_func=_fmt_area,
        )
        st.session_state["selected_h3"] = selected
        row = top.filter(pl.col("h3") == selected).to_dicts()[0]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Opportunity score", f"{row['opportunity_score']:.3f}")
        # Label competitor metric by selected business type
        bt = st.session_state.get("business_template", "gym")
        bt_label = BUSINESS_TEMPLATES[bt].label if bt in BUSINESS_TEMPLATES else ("Custom business" if bt == "custom (beta)" else "Competitors")
        c2.metric(f"{bt_label.split('/')[0].strip()} competitors", int(row["competitor_count"]))
        c3.metric("Complements", int(row["complement_count"]))
        c4.metric("Data quality", f"{row['data_quality_score']:.2f}")

        st.divider()
        st.subheader("Complete metrics (from dataset)")
        st.caption(
            "Every value we have for this area: transit score plus station / PT / bus shelter counts, SA2 market and rent fields, and each score component. "
            "Missing values show as —."
        )
        st.dataframe(metrics_table_for_area_row(row), use_container_width=True, hide_index=True)

        if st.session_state.get("use_openai_narrative"):
            bt_key = st.session_state.get("business_template", "gym")
            bt_lbl = (
                BUSINESS_TEMPLATES[bt_key].label
                if bt_key in BUSINESS_TEMPLATES
                else ("Custom business" if bt_key == "custom (beta)" else "Business")
            )
            payload = metrics_payload_from_area_row(row)
            built_in = generate_deterministic_area_narrative(
                payload,
                city_label=city.label,
                business_label=bt_lbl,
                area_id=str(selected),
            )

            if not reload_openai_env():
                st.markdown("**Area narrative (built-in)**")
                st.caption("No `OPENAI_API_KEY` in `.env`—showing rule-based text from the same metrics.")
                st.markdown(built_in)
                st.info(
                    "Add **OPENAI_API_KEY** to `Data_Hack_April/.env`, save, restart Streamlit for optional OpenAI prose."
                )
            else:
                openai_text: str | None = None
                api_error: str | None = None
                try:
                    payload_json = json.dumps(payload, sort_keys=True, default=str)
                    openai_text = _cached_openai_narrative(payload_json, str(selected), bt_lbl, city.label)
                except Exception as e:
                    api_error = str(e)

                if openai_text:
                    st.markdown("**AI narrative (OpenAI)**")
                    st.caption("Generated from the metrics above; does not replace scoring. Validate on site.")
                    st.write(openai_text)
                    with st.expander("Offline narrative (same numbers, no API)", expanded=False):
                        st.markdown(built_in)
                else:
                    st.markdown("**Area narrative (built-in)**")
                    st.caption(
                        "OpenAI did not return text—using rule-based prose from the **same** scores so the demo keeps working."
                    )
                    st.markdown(built_in)
                    with st.expander("Why OpenAI failed (troubleshooting)", expanded=False):
                        st.markdown(api_error or "Unknown error.")
                    r1, r2 = st.columns([1, 2])
                    with r1:
                        if st.button("Retry OpenAI", key="retry_openai_narrative"):
                            _cached_openai_narrative.clear()
                            st.rerun()
                    with r2:
                        st.caption("Clears the narrative cache after you fix network, billing, or `.env`.")

        if row.get("reasons_top3"):
            st.markdown("**Top reasons (auto)**")
            for r in str(row["reasons_top3"]).split(";"):
                rr = r.strip()
                if rr:
                    st.write(f"- {rr}")

        with st.expander("Advanced / debug (area identifiers)", expanded=False):
            st.write(f"**H3**: `{selected}`")
            if row.get("sal_name_2021"):
                st.write(f"**SAL**: {row.get('sal_name_2021')}")
            if row.get("sa2_name_2021"):
                st.write(f"**SA2**: {row.get('sa2_name_2021')}")

        st.markdown("**Top 10 business categories in this area**")
        try:
            topcats = _top_categories_in_area(area_category_counts, idx_dict, selected, n=10)
            if topcats.height == 0:
                st.caption("No category counts available for this area.")
            else:
                st.dataframe(topcats, use_container_width=True, hide_index=True)
        except Exception:
            st.caption("Category breakdown unavailable in this run.")

        # Score breakdown bars (uses precomputed components when available).
        st.markdown("**Score breakdown**")
        ranked_row = None
        try:
            ranked_row = scored.filter(pl.col("h3") == selected)
        except Exception:
            ranked_row = None
        if ranked_row is not None and ranked_row.height > 0:
            cols = [
                ("Complements", "score_complementarity"),
                ("Activity", "score_activity"),
                ("Diversity", "score_diversity"),
                ("Transit access", "score_transit_access"),
                ("Market fit", "score_market_fit"),
                ("Rent affordability", "score_rent_affordability"),
                ("Data quality", "score_quality"),
                ("Competition (penalty)", "score_competition"),
                ("Saturation (penalty)", "score_saturation"),
            ]
            available = [(label, c) for (label, c) in cols if c in ranked_row.columns]
            if available:
                rows = []
                for label, c in available:
                    raw = ranked_row.select(pl.col(c)).item()
                    v = 0.0 if raw is None else float(raw)
                    rows.append({"factor": label, "contribution": v})
                bdf = pl.DataFrame(rows).sort("contribution", descending=True)
                st.bar_chart(bdf.to_pandas().set_index("factor"))
                st.caption("Positive bars increase score; negative bars decrease score.")
            else:
                st.caption("Breakdown not available for this run (missing `score_*` columns in ranked output).")
        else:
            st.caption("Breakdown not available for this selection.")

        st.markdown("**Decision checklist (what to do next)**")
        tplk = st.session_state.get("business_template", "gym")
        if tplk == "custom (beta)":
            tplk = "gym"
        ck = _decision_checklist(tplk)
        cA, cB, cC = st.columns(3, gap="large")
        with cA:
            st.markdown("**Validate next**")
            for x in ck["validate_next"]:
                st.write(f"- {x}")
        with cB:
            st.markdown("**Red flags**")
            for x in ck["red_flags"]:
                st.write(f"- {x}")
        with cC:
            st.markdown("**Go / No-go**")
            for x in ck["go_no_go"]:
                st.write(f"- {x}")

        # Competitor examples are implemented for gym template only (category-id based).
        if st.session_state.get("business_template", "gym") == "gym":
            pois_cell = area_pois.filter(pl.col("h3") == selected)
            comp_pois = (
                pois_cell.filter(pl.col("category_id").is_in(list(gym_ids)))
                .select(["name", "place_id", "category_id"])
                .unique()
                .head(20)
            )
            st.markdown("**Competitor examples (in this area)**")
            if comp_pois.height == 0:
                st.write("None found in this cell (based on current competitor category definition).")
            else:
                st.dataframe(
                    comp_pois.with_columns(
                        pl.col("category_id")
                        .map_elements(lambda x: idx_dict["id_to_name"].get(x, x), return_dtype=pl.Utf8)
                        .alias("category_name")
                    ).select(["name", "category_name"]),
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.markdown("**Competitor examples (in this area)**")
            st.caption("In v1, competitor examples are shown for the Gym template only. Rankings still use the selected business template.")

        # Complement breakdown by group
        st.markdown("**Complement breakdown (counts in this area)**")
        if st.session_state.get("business_template", "gym") == "gym":
            breakdown = []
            counts_cell = area_category_counts.filter(pl.col("h3") == selected)
            for g, ids in comp_groups.items():
                if not ids:
                    continue
                cnt = (
                    counts_cell.filter(pl.col("category_id").is_in(list(ids)))
                    .select(pl.col("cnt").sum())
                    .item()
                )
                breakdown.append({"group": g, "count": int(cnt or 0)})
            st.dataframe(pl.DataFrame(breakdown).sort("count", descending=True), use_container_width=True, hide_index=True)
        else:
            st.caption("Complement group breakdown is gym-specific in v1. Rankings still use the selected template taxonomy.")

    with tab_compare:
        st.subheader("Compare 2–3 areas")
        default_pins = st.session_state.get("pinned") or top["h3"].to_list()[:2]
        picks = st.multiselect(
            "Pick areas",
            options=top["h3"].to_list(),
            default=default_pins,
            max_selections=3,
            format_func=_fmt_area,
        )
        if len(picks) < 2:
            st.info("Select at least 2 areas to compare.")
        else:
            comp = top.filter(pl.col("h3").is_in(picks)).select(ordered_columns_for_tables(top))
            st.dataframe(comp, use_container_width=True, hide_index=True)

            if st.button("Export comparison to CSV"):
                out = Path("data/exports")
                out.mkdir(parents=True, exist_ok=True)
                out_path = out / "comparison.csv"
                comp.write_csv(out_path)
                st.success(f"Exported to {out_path}")

    with tab_transit:
        st.subheader("Transit & transport context (Theme 3)")
        st.caption(
            "Points come from **NSW open transport data** (station entrances, facilities, City of Sydney bus shelters)—"
            "see *Data sources & methodology* above. The **transit access score** is a proximity-style proxy; "
            "it is **not** Opal fares, peak loads, tap-on/off, or ridership."
        )
        render_transit_tab_notes()

        tp = cached_transport_points()
        if tp.height == 0:
            st.info(
                "No transport datasets found in `data/raw/nsw_transport/`. "
                "Run `./.venv/bin/python scripts/download_public_datasets.py` then rebuild features."
            )
        else:
            # Build point layer
            points = (
                tp.select(["lat", "lon", "kind", "source", "name"])
                .with_columns(
                    pl.col("kind")
                    .map_elements(lambda k: _kind_color(k), return_dtype=pl.List(pl.Int64))
                    .alias("rgba")
                )
                .to_dicts()
            )

            point_layer = pdk.Layer(
                "ScatterplotLayer",
                data=points,
                get_position="[lon, lat]",
                get_fill_color="rgba",
                get_radius=60,
                radius_min_pixels=2,
                radius_max_pixels=8,
                pickable=True,
            )

            # Overlay the transit_access_score by area (if present in ranked outputs)
            if "transit_access_score" in scored.columns:
                overlay_df = scored.select(
                    [
                        pl.col("lat_mean").alias("lat"),
                        pl.col("lon_mean").alias("lon"),
                        pl.col("transit_access_score"),
                        pl.col("h3"),
                    ]
                ).to_pandas()
                overlay_df["t"] = overlay_df["transit_access_score"].fillna(0.0)
                overlay_df["fill_r"] = (220 - 160 * overlay_df["t"]).astype(int)
                overlay_df["fill_g"] = (60 + 160 * overlay_df["t"]).astype(int)
                overlay_df["fill_b"] = 220
                overlay_df["fill_a"] = 90

                overlay_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=overlay_df,
                    get_position="[lon, lat]",
                    get_fill_color="[fill_r, fill_g, fill_b, fill_a]",
                    get_radius=250,
                    radius_min_pixels=6,
                    radius_max_pixels=22,
                    pickable=True,
                )
                layers = [overlay_layer, point_layer]
                tooltip = {
                    "text": "Kind: {kind}\nName: {name}\nSource: {source}\n\n(Hover area overlay)\nH3: {h3}\nTransit score: {transit_access_score}"
                }
            else:
                layers = [point_layer]
                tooltip = {"text": "Kind: {kind}\nName: {name}\nSource: {source}"}

            view_state = pdk.ViewState(latitude=city.map_center_lat, longitude=city.map_center_lon, zoom=city.map_zoom)
            st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, tooltip=tooltip))

            st.markdown("**Legend**")
            st.write("- **Blue**: train station entrances")
            st.write("- **Yellow**: City of Sydney bus shelters")
            st.write("- **Grey**: other public transport facilities")
            if "transit_access_score" in scored.columns:
                st.write("- **Large translucent circles**: area-level transit access score (higher = greener)")

    with tab_market:
        st.subheader("Market fit (SA2 demographics proxy)")
        st.caption(
            "Choropleths join **ABS 2021 SA2** boundaries to our H3 scores. **Market fit** uses **SEIFA 2021**; "
            "**rent affordability** uses Census **rental stress (RAID-style)** at SA2. "
            "We **do not** use Census QuickStats by **postcode (POA)** here—geography is **SA2**, not POA2026."
        )
        render_market_tab_notes()

        mtime = sa2_market_geo.stat().st_mtime if sa2_market_geo.exists() else 0.0
        fc = cached_sa2_market_fit_geojson(str(sa2_market_geo), mtime)
        if fc is None:
            st.info(
                f"Missing `{sa2_market_geo}`. "
                f"Re-run `./.venv/bin/python scripts/run_build_features.py --city {city.key}` to generate it."
            )
        else:
            show_h3_overlay = st.toggle(
                "Overlay top opportunity areas (H3)",
                value=True,
                help="Shows the top-ranked H3 cells on top of the SA2 choropleth for judge-friendly alignment checks.",
            )
            layer_metric = st.radio(
                "SA2 layer",
                options=[
                    "Market fit (SEIFA proxy)",
                    "Rent affordability (rental stress proxy)",
                    "Renter rate (Census 2021, SA2)",
                ],
                horizontal=True,
                help=(
                    "Market fit uses SEIFA. Rent affordability uses Census RAID: share of renting households paying >30% of income "
                    "(lower stress = higher score). Renter rate uses ABS Census 2021 tenure totals (share of households renting)."
                ),
            )
            tooltip_mode = st.radio(
                "Tooltip mode",
                options=["SA2 (market fit)", "H3 (opportunity overlay)"],
                horizontal=True,
                help="Deck has one tooltip template. Choose which layer you want to read while hovering.",
            )

            geo_layer = pdk.Layer(
                "GeoJsonLayer",
                data=fc,
                opacity=0.35,
                stroked=True,
                filled=True,
                get_line_color=[255, 255, 255, 120],
                get_fill_color="properties.fill",
                pickable=True,
                auto_highlight=True,
            )

            # Precompute colors inside the GeoJSON (stable + fast in Deck).
            for f in fc.get("features", []):
                p = f.get("properties", {})
                if layer_metric.startswith("Market"):
                    key = "market_fit_score"
                elif layer_metric.startswith("Rent affordability"):
                    key = "rent_affordability_score"
                else:
                    key = "renter_share_2021"
                val = p.get(key)
                if val is None:
                    p["fill"] = [180, 180, 180, 90]  # N/A
                else:
                    t = float(val or 0.0)
                    t = max(0.0, min(1.0, t))
                    # yellow-green palette (higher = greener)
                    r = int(240 - 130 * t)
                    g = int(210 + 25 * t)
                    b = int(95 - 40 * t)
                    p["fill"] = [r, g, b, 170]

            view_state = pdk.ViewState(latitude=city.map_center_lat, longitude=city.map_center_lon, zoom=max(9.4, city.map_zoom - 0.7))
            layers = [geo_layer]

            # Overlay H3 opportunities (centroids) for alignment check.
            if show_h3_overlay:
                overlay = scored
                # Keep it fast: show top 80 after guardrails.
                overlay = overlay.head(80)
                ov = overlay.select(
                    [
                        pl.col("lat_mean").alias("lat"),
                        pl.col("lon_mean").alias("lon"),
                        pl.col("opportunity_score"),
                        pl.col("competitor_count"),
                        pl.col("complement_count"),
                        (pl.col("transit_access_score") if "transit_access_score" in overlay.columns else pl.lit(None)).alias(
                            "transit_access_score"
                        ),
                        (pl.col("market_fit_score") if "market_fit_score" in overlay.columns else pl.lit(None)).alias(
                            "market_fit_score"
                        ),
                        pl.col("h3"),
                    ]
                ).to_pandas()
                s_min = float(ov["opportunity_score"].min())
                s_max = float(ov["opportunity_score"].max())
                denom = (s_max - s_min) if (s_max - s_min) > 1e-9 else 1.0
                ov["t"] = (ov["opportunity_score"] - s_min) / denom
                ov["fill_r"] = (220 - 160 * ov["t"]).astype(int)
                ov["fill_g"] = (60 + 160 * ov["t"]).astype(int)
                ov["fill_b"] = 90
                ov["fill_a"] = 180

                h3_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=ov,
                    get_position="[lon, lat]",
                    get_fill_color="[fill_r, fill_g, fill_b, fill_a]",
                    get_radius="complement_count",
                    radius_scale=2.5,
                    radius_min_pixels=4,
                    radius_max_pixels=18,
                    pickable=True,
                )
                layers.append(h3_layer)

            if tooltip_mode == "H3 (opportunity overlay)":
                tooltip = {
                    "text": (
                        "H3: {h3}\n"
                        "Opportunity: {opportunity_score}\n"
                        "Complements: {complement_count}\n"
                        "Competitors: {competitor_count}\n"
                        "Transit: {transit_access_score}\n"
                        "Market fit: {market_fit_score}"
                    )
                }
            else:
                tooltip = {
                    "text": (
                        "SA2: {sa2_name_2021}\n"
                        "Market fit: {market_fit_score}\n"
                        "Rent affordability: {rent_affordability_score}\n"
                        "Rental stress share: {rent_stress_share}\n"
                        "Renter share (2021): {renter_share_2021}\n"
                        "IRSAD decile: {seifa_irsad_decile}\n"
                        "Population: {sa2_population}"
                    )
                }
            st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, tooltip=tooltip))

            st.markdown("**How to read this**")
            if layer_metric.startswith("Market"):
                st.write("- The **SA2 background** is market fit (SEIFA proxy).")
            elif layer_metric.startswith("Rent affordability"):
                st.write("- The **SA2 background** is rent affordability (Census RAID proxy; higher = lower rental stress).")
            else:
                st.write("- The **SA2 background** is renter share (ABS Census 2021; higher = more households renting).")
            st.write("- The **H3 overlay** shows top opportunity cells (color = opportunity score; size = complements).")
            st.write("- Grey SA2s are **N/A** (no SEIFA row for that polygon).")

            # Table view for judges
            rows = [f.get("properties", {}) for f in fc.get("features", [])]
            df = pl.DataFrame(rows)
            # Backward-compatible with older GeoJSONs that don't yet include rent proxy fields.
            if "rent_affordability_score" not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("rent_affordability_score"))
            if "rent_stress_share" not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("rent_stress_share"))
            if "renter_share_2021" not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("renter_share_2021"))

            df = df.select(
                [
                    pl.col("sa2_name_2021").alias("SA2"),
                    pl.col("market_fit_score").cast(pl.Float64).alias("market_fit_score"),
                    pl.col("rent_affordability_score").cast(pl.Float64).alias("rent_affordability_score"),
                    pl.col("rent_stress_share").cast(pl.Float64).alias("rent_stress_share"),
                    pl.col("renter_share_2021").cast(pl.Float64).alias("renter_share_2021"),
                    pl.col("seifa_irsad_decile").cast(pl.Int64).alias("irsad_decile"),
                    pl.col("sa2_population").cast(pl.Int64).alias("population"),
                ]
            )
            df = df.filter(pl.col("market_fit_score").is_not_null())
            left, right = st.columns(2, gap="large")
            with left:
                st.markdown("**Top 10 SA2s (market fit)**")
                st.dataframe(df.sort("market_fit_score", descending=True).head(10), hide_index=True, use_container_width=True)
            with right:
                st.markdown("**Bottom 10 SA2s (market fit)**")
                st.dataframe(df.sort("market_fit_score").head(10), hide_index=True, use_container_width=True)

            if df.filter(pl.col("rent_affordability_score").is_not_null()).height > 0:
                st.markdown("**Rent affordability (proxy) quick view**")
                c1, c2 = st.columns(2, gap="large")
                with c1:
                    st.markdown("Top 10 SA2s (more affordable)")
                    st.dataframe(
                        df.filter(pl.col("rent_affordability_score").is_not_null())
                        .sort("rent_affordability_score", descending=True)
                        .select(["SA2", "rent_affordability_score", "rent_stress_share", "population"])
                        .head(10),
                        hide_index=True,
                        use_container_width=True,
                    )
                with c2:
                    st.markdown("Bottom 10 SA2s (less affordable)")
                    st.dataframe(
                        df.filter(pl.col("rent_affordability_score").is_not_null())
                        .sort("rent_affordability_score")
                        .select(["SA2", "rent_affordability_score", "rent_stress_share", "population"])
                        .head(10),
                        hide_index=True,
                        use_container_width=True,
                    )

            # Simple alignment insight for judges.
            if "market_fit_score" in scored.columns:
                top_n = 50 if scored.height >= 50 else scored.height
                top_slice = scored.head(top_n)
                mf_med = float(top_slice.select(pl.col("market_fit_score").median()).item())
                dec_med = int(top_slice.select(pl.col("seifa_irsad_decile").median()).item())
                st.caption(f"Alignment check: median market fit among top {top_n} opportunities = **{mf_med:.2f}** (median IRSAD decile **{dec_med}**).")

    with tab_housing:
        st.subheader("Housing signals (rent + buy price)")
        st.caption(
            "**DCJ** tables (postcode → aggregated to SA2) give **residential** median rent and sale price when the file is present; "
            "**Census 2021** gives renter share at SA2. This is **housing-market context**, not commercial shop rent per m²."
        )
        render_housing_tab_notes()

        if not sa2_housing_geo.exists():
            st.info(
                f"Missing `{sa2_housing_geo}`. "
                f"Run `./.venv/bin/python scripts/build_housing_assets.py --city {city.key}` to generate it."
            )
        else:
            metric = st.radio(
                "Housing layer",
                options=["Renter rate (Census 2021)", "Median weekly rent (DCJ)", "Median sale price (DCJ)"],
                horizontal=True,
            )
            mtime_h = sa2_housing_geo.stat().st_mtime if sa2_housing_geo.exists() else 0.0
            fc_h = cached_geojson(str(sa2_housing_geo), mtime_h)
            if fc_h is None:
                st.warning("Could not load housing geojson.")
            else:
                # Compute fill colors based on chosen metric.
                key = (
                    "renter_share_2021"
                    if metric.startswith("Renter")
                    else ("median_weekly_rent_total" if "rent" in metric.lower() else "median_sale_price_total")
                )
                # Estimate min/max from the geojson itself (ignores nulls).
                vals = []
                for f in fc_h.get("features", []):
                    v = f.get("properties", {}).get(key)
                    if v is None:
                        continue
                    try:
                        vals.append(float(v))
                    except Exception:
                        continue
                vmin = min(vals) if vals else 0.0
                vmax = max(vals) if vals else 1.0
                denom = (vmax - vmin) if (vmax - vmin) > 1e-9 else 1.0

                for f in fc_h.get("features", []):
                    p = f.get("properties", {})
                    v = p.get(key)
                    if v is None:
                        p["fill"] = [180, 180, 180, 90]
                        continue
                    try:
                        t = (float(v) - vmin) / denom
                    except Exception:
                        p["fill"] = [180, 180, 180, 90]
                        continue
                    t = max(0.0, min(1.0, t))
                    # blue → purple → red (higher = hotter)
                    r = int(60 + 180 * t)
                    g = int(120 - 80 * t)
                    b = int(220 - 120 * t)
                    p["fill"] = [r, g, b, 170]

                geo_layer = pdk.Layer(
                    "GeoJsonLayer",
                    data=fc_h,
                    opacity=0.35,
                    stroked=True,
                    filled=True,
                    get_line_color=[255, 255, 255, 120],
                    get_fill_color="properties.fill",
                    pickable=True,
                    auto_highlight=True,
                )

                tooltip = {
                    "text": (
                        "SA2: {sa2_name_2021}\n"
                        "Renter share (2021): {renter_share_2021}\n"
                        "Median weekly rent: {median_weekly_rent_total}\n"
                        "Median sale price: {median_sale_price_total}\n"
                        "Sales count: {sales_total}\n"
                        "Bonds lodged: {bonds_lodged_total}"
                    )
                }
                view_state = pdk.ViewState(latitude=city.map_center_lat, longitude=city.map_center_lon, zoom=max(9.4, city.map_zoom - 0.7))
                st.pydeck_chart(pdk.Deck(layers=[geo_layer], initial_view_state=view_state, tooltip=tooltip))

                st.markdown("**How to read this**")
                st.write("- Grey SA2s are **N/A** (missing value for this layer).")
                st.write("- DCJ rent/sales are published by **postcode** and aggregated to SA2 for the demo.")

    st.caption("Businesses do not need more map data. They need fewer bad location decisions.")


if __name__ == "__main__":
    main()


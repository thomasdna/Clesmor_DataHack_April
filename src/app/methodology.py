"""
User-facing copy: where data comes from, which geographies apply, and what we do not claim.

Keep wording cautious—this is decision support, not a forecast.
"""
from __future__ import annotations

import streamlit as st

# --- Main panel (full methodology) ------------------------------------------

MAIN_METHODOLOGY = """
### What this demo uses

**Places & competition (core)**  
- **Foursquare Places OS** (open dataset): business locations, coordinates, and category taxonomy.  
- We infer **competitor / complement / activity** signals from categories and counts. This is **not** foot traffic, sales, or commuter demand.

**Geography**  
- **H3** cells: where we aggregate POIs and compute opportunity scores.  
- **ABS Statistical Area Level 2 (SA2)**: where we join **Census and SEIFA** layers.  
- We **do not** use [Census QuickStats by postcode (POA)](https://www.abs.gov.au/census/find-census-data/quickstats/2021/POA2026) in this app. If you need **postcode-native** reporting, that would be a separate join—not implemented here.

---

### Transport (New South Wales)

**What is integrated**  
Open data is loaded from local files under `data/raw/nsw_transport/` (see `scripts/download_public_datasets.py`):

| Layer | Role in the demo |
|------|-------------------|
| **Train station entrances** (CSV) | Points near entries; supports a **distance / density–style** transit access proxy. |
| **Public transport location facilities** (CSV) | Additional stops/facilities with coordinates. |
| **City of Sydney bus shelters** (GeoJSON) | Bus stop–related points in the City of Sydney LGA. |

**What is *not* integrated (by design for this MVP)**  
- **Opal fares** — we do not model ticket price or “cheapest route.”  
- **Peak train load estimates** — we do not use crowding or capacity.  
- **Opal tap-on / tap-off** — we do not use temporal ridership or OD matrices.  

Those datasets are valuable for **operations and revenue timing** (e.g. peak hours), but they add heavy ETL, time alignment, and risk of over-interpreting aggregates. They are **optional future work**, not required to explain “where might a site fit the map?”

**Interpretation**  
The **transit access score** is a **location-based proxy** (nearby infrastructure), **not** ridership or revenue.

---

### Demographics & “market fit” (ABS)

**What is integrated**  
- **SEIFA 2021 at SA2** (Socio-Economic Indexes for Areas): used as an **affluence / resources proxy** for **market fit** (scores and deciles in outputs). Source table is read from `data/raw/abs/` in the feature pipeline (`src/features/market_fit.py`).  
- **Census rental affordability (RAID-style) at SA2**: share of renting households under rental stress; drives **rent affordability proxy** (`src/features/rent_affordability.py`).  
- **Census 2021 tenure (G37 / household renting) at SA2**: **renter rate** where loaded.

**What we do not claim**  
- This is **not** median personal income from QuickStats by POA.  
- **SEIFA** summarises area-level disadvantage/advantage; it is a **planning proxy**, not a statement about any individual.

---

### Housing & rent (optional assets)

- **NSW DCJ Rent & Sales Report** (quarterly tables): **median weekly rent** and **median sale price** are available when you place the published spreadsheet under `data/raw/housing_market/` and run the housing asset script. Values are **residential** market context aggregated from **postcode** to **SA2** for mapping.  
- This is **not** the same as **commercial shop rent** (per m²) from a retail lease market feed. We do **not** integrate a dedicated commercial-rent-by-postcode series.

---

### Global café comparison (separate export)

- Uses **Foursquare POIs only** (café taxonomy, POI-only scoring). **No** ABS, **no** NSW transport layers—by design for cross-country fairness.

---

### Optional AI narrative

- If enabled in the sidebar, **OpenAI** turns the **same numeric features** into prose. It does **not** see raw POI lists or external APIs. Validate any narrative on site.

---

### Limitations (all layers)

- POI presence ≠ demand; categories ≠ sales.  
- SA2 boundaries ≠ trade areas; H3 cells ≠ council or postcode.  
- Scores are **transparent weighted combinations** for **shortlisting**, not optimal site selection or ROI.
"""


TRANSIT_TAB = """
**Sources (TfNSW & related open data)**  
Train **station entrances**, **location facilities**, and **City of Sydney bus shelters** are loaded from files produced by `scripts/download_public_datasets.py` (see dataset URLs in that script).

**What the map shows**  
Coloured points = infrastructure locations. When present, the **large translucent circles** show the **transit access score** by H3 (a proximity-style proxy from those points).

**Not included**  
Opal fares, peak crowding, and tap-on/tap-off data are **not** in this demo—see **Data sources & methodology** on the main page.
"""


MARKET_TAB = """
**ABS geography**  
Choropleth and tables use **SA2 (2021)** polygons joined to scored areas by **H3 centroid in polygon**.

**Market fit**  
Derived from **SEIFA 2021** (IRSAD / related scores at SA2). Higher **market fit score** = stronger area-level socio-economic resources / advantage **on that index**, not a brand or revenue forecast.

**Rent affordability**  
Uses **Census RAID-style** rental stress at SA2: share of renting households paying more than 30% of income on rent. The score is **1 − stress share** (with guardrails). **Not** median rent dollars from QuickStats POA.

**Renter rate**  
Share of households renting (Census 2021), SA2.

**Not used here**  
[Census QuickStats for POA2026](https://www.abs.gov.au/census/find-census-data/quickstats/2021/POA2026)—we standardise on **SA2** for these overlays.
"""


HOUSING_TAB = """
**DCJ Rent & Sales**  
When the NSW **DCJ** quarterly rent and sales tables are available locally, **median weekly rent** and **median sale price** are aggregated from **postcode** to **SA2** for choropleths.

**Census**  
**Renter share** comes from ABS Census 2021 household tenure at SA2.

**Caveat**  
These layers describe **housing market context** (residential). They are **not** commercial lease rates for retail floorspace.
"""


def render_main_methodology_expander(*, expanded: bool = False) -> None:
    """Full methodology: call once near the top of the main page."""
    with st.expander("Data sources, geography & limitations", expanded=expanded):
        st.markdown(MAIN_METHODOLOGY)


def render_transit_tab_notes() -> None:
    with st.expander("What this tab includes (transport data)", expanded=False):
        st.markdown(TRANSIT_TAB)


def render_market_tab_notes() -> None:
    with st.expander("What this tab includes (ABS / SA2)", expanded=False):
        st.markdown(MARKET_TAB)


def render_housing_tab_notes() -> None:
    with st.expander("What this tab includes (housing data)", expanded=False):
        st.markdown(HOUSING_TAB)

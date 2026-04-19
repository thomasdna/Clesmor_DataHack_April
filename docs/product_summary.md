# Expansion Copilot — Product Summary (Demo)

**Tagline**: Turn place data into smarter expansion decisions.  
**Closing line**: Businesses do not need more map data. They need fewer bad location decisions.  

## 1) What this product does

Expansion Copilot helps a user answer:

**“Where should I open my next business in Sydney, and why?”**

It’s a **decision-support** tool (not demand/revenue prediction) that ranks candidate areas using:

- local ecosystem signals (POI-based proxies)
- competition pressure
- complementary venue ecosystem
- accessibility (transit proxy)
- market fit (SEIFA proxy)
- data quality guardrails

## 2) Supported business modes

### Template-based (fast + reliable)
- Gym / boutique fitness
- Café / coffee shop
- Allied health clinic
- Coworking space

Each template has:
- a curated taxonomy (competitors / complements / exclusions)
- default scoring weights
- precomputed ranked outputs for fast switching

### Custom (beta)
User enters **competitor/complement/exclusion keywords**.
We match against Foursquare category names/labels and generate a ranking.

Guardrail: this is keyword-based matching, so we provide:
- category search + suggestions
- “matched categories preview” tables

## 3) Data sources (public / open)

### Foursquare Open Source Places (Parquet)
Used for:
- POI universe in Sydney
- category mapping for competitors/complements
- activity/density proxies and diversity

### NSW transport open data (Theme 3)
Used for **transit accessibility proxy**:
- Train station entrance locations (CSV)
- Public transport location facilities (CSV)
- City of Sydney bus shelters (GeoJSON)

### ABS SEIFA 2021 (SA2)
Used for **market fit proxy**:
- IRSAD (advantage/disadvantage)
- IER (economic resources)
- SA2 population (context)

## 4) Core features (area-level)

We aggregate POIs into areas (v1 uses H3 resolution 8).

Key outputs per area:
- `active_poi_count`
- `competitor_count`
- `complementary_count`
- `commercial_activity_proxy`
- `unique_category_count`
- `mean_data_quality_score`
- `transit_access_score` (0–1)
- `market_fit_score` (0–1)

### 4.1 Data quality score (POI-level, 0..1)

Let base score be:
\[
q = 1
\]

Then subtract penalties if those columns exist:
\[
q = 1
 - 0.40\cdot \mathbb{1}[\text{lat or lon missing}]
 - 0.20\cdot \mathbb{1}[\text{name missing}]
 - 0.15\cdot \mathbb{1}[\text{category missing}]
 - 0.25\cdot \mathbb{1}[\text{unresolved\_flags present}]
\]
Clamp to \([0,1]\).

Area-level:
\[
\text{mean\_data\_quality\_score} = \frac{1}{N}\sum_{i=1}^{N} q_i
\]

### 4.2 Competition + complement proxies

Using \(\varepsilon=1\):

\[
\text{competitor\_density\_proxy}=\frac{\text{competitor\_count}}{\text{active\_poi\_count}+\varepsilon}
\]
\[
\text{complementarity\_ratio}=\frac{\text{complementary\_count}}{\text{active\_poi\_count}+\varepsilon}
\]
\[
\text{commercial\_activity\_proxy}=\text{active\_poi\_count}
\]
\[
\text{saturation\_proxy}=\text{competitor\_density\_proxy}\cdot (1-\text{complementarity\_ratio})
\]

### 4.3 Transit accessibility proxy (0..1)

Per H3 cell, aggregate transport points:
- station entrances
- public transport facilities
- bus shelters

Compute raw:
\[
r = \log(1 + 2.0\cdot entrances + 1.0\cdot facilities + 0.5\cdot bus\_shelters)
\]
Then min–max normalize to \(transit\_access\_score\in[0,1]\).

### 4.4 Market fit proxy (SEIFA, 0..1)

At SA2 level:
- normalize IRSAD and IER to 0–1
\[
market\_fit\_score = 0.7\cdot n_{irsad} + 0.3\cdot n_{ier}
\]

Join SA2 → H3 by centroid-in-polygon using ABS SA2 boundaries.

## 5) Opportunity Score (explainable)

We compute a weighted sum of normalized features (with log1p on heavy-tailed columns).

Default gym-like formulation:
\[
Score =
1.2n_{comp\_ecosystem}
 + 0.8n_{activity}
 + 0.6n_{diversity}
 - 1.0n_{competition}
 - 0.7n_{saturation}
 + 0.5n_{quality}
 + 0.4n_{transit}
 + 0.6n_{marketfit}
\]

Template mode changes the weights (e.g., café weights transit more).

We also store **component contributions** (`score_*`) so the UI can show a breakdown bar chart.

## 6) Demo UX (Streamlit)

### Overview tab
- Opportunity map (color by score, size by complements)
- Ranked shortlist table
- Pin/compare flow
- Shortlist workflow:
  - status (Investigate / Visit / Contact broker / Defer)
  - notes
  - export shortlist + notes

### Why here? tab
- KPI card (score, competitors, complements, quality)
- Transit + market fit callouts
- Top reasons (auto)
- Score breakdown bar chart
- Top 10 categories in area (location-first discovery)
- Competitor examples (gym template)

### Compare tab
- Side-by-side metrics for 2–3 pinned areas
- Export compare set

### Transit tab
- Transport points map + transit-score overlay

### Market fit (SA2) tab
- SA2 choropleth (SEIFA market fit proxy)
- Optional overlay of top opportunity H3 points
- Tooltip mode toggle (SA2 vs H3)
- Top/Bottom SA2 tables + alignment insight

### Demo controls
- Judge mode toggle + hero buttons (Map/Why/Compare)
- Advanced settings collapsed by default in judge mode

## 7) Key outputs and artifacts

Processed outputs:
- `data/processed/sydney_ranked_areas_<template>.parquet`
- `data/processed/sydney_area_features_<template>.parquet`
- `data/processed/sydney_sa2_market_fit.geojson`

Exports:
- `data/exports/top_ranked_areas_<template>.csv`
- `data/exports/shortlist_with_notes.csv`
- `data/exports/compare_set.csv`

Docs:
- `docs/transport_integration.md`
- `docs/market_fit_integration.md`
- `docs/competitive_landscape.md`

## 8) How to run (local)

Rebuild features (all templates):
```bash
./.venv/bin/python scripts/run_build_features.py --all
```

Run the demo:
```bash
./.venv/bin/streamlit run app/streamlit_app.py --server.port 8507
```

## 9) Limitations (explicit)

- Not revenue/demand prediction.
- Activity is a POI proxy, not verified footfall.
- Transit is an accessibility proxy (no ridership).
- Market fit uses SEIFA as a socioeconomic proxy.
- Area units (H3/SA2) are approximations and not true trade areas.


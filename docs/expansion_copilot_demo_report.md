# Expansion Copilot (Sydney Gyms) — Demo Build Report (Pre-Transport)

**Tagline**: Turn place data into smarter expansion decisions.  
**Core question**: *Where should I open my next gym in Sydney, and why?*  
**Scope**: Sydney, Australia • Boutique / mid‑market gyms • Decision support (not prediction)

---

## 1) What we built (so far)

### Product
- A Streamlit MVP that:
  - shows **ranked candidate areas**
  - visualizes them on a map (color = score)
  - provides a **“Why here?”** explanation panel with transparent drivers
  - supports **Compare** for 2–3 areas
  - includes **guardrails** and **judge-mode presets** for demo stability

### Data + feature pipeline (offline, reproducible)
- Reads Foursquare Open Source Places (Parquet) subset for Sydney.
- Builds an explainable feature table aggregated by area (H3).
- Computes an Opportunity Score with a transparent weighted formula.
- Writes **processed artifacts** consumed by the app (no recompute in the UI).

---

## 2) Repository structure (relevant parts)

- `app/streamlit_app.py`: current demo app (single-file Streamlit app, upgraded with demo controls)
- `app/app.py` + `src/app/*`: modular Streamlit MVP (cleaner structure, same data contract)
- `src/features/build_opportunity_features.py`: feature engineering for area-level metrics
- `src/scoring/opportunity_score.py`: scoring math (normalization, weights, breakdown)
- `scripts/run_build_features.py`: produces processed outputs
- `data/interim/`: Sydney subset + cleaned categories used to build features
- `data/processed/`: feature table + ranked areas (app reads these)

---

## 3) Data inputs (pre-transport)

### Foursquare Open Source Places (Sydney subset)
Inputs (from prior tasks):
- `data/interim/places_sydney.parquet`
- `data/interim/categories_clean.parquet`

We explicitly designed the pipeline to **not assume optional columns exist**. Where columns are missing (e.g., refresh timestamps), we use safe fallbacks or leave placeholders.

---

## 4) Core feature engineering (area-level)

We aggregate to an **area unit** (v1: H3 at resolution 8).

### 4.1 Place → role flags (competitor / complement / excluded)

We classify each place into business roles using a transparent gym taxonomy:
- competitors: gyms / yoga / pilates / martial arts (configurable)
- complements: healthy cafes, physio, wellness, parks, sports goods, etc.
- exclusions: categories that should not count toward active commercial ecosystem

Implementation: `src/features/category_mapping.py`  
Key design constraint: explode/join category IDs **without inflating places**.

**Method**
1. explode `category_ids` to one row per `(place_id, category_id)`
2. join to cleaned categories table for `category_name` and `category_label`
3. keyword-match into flags
4. group back to `place_id` using `any()` for boolean flags

Output per place:
- `is_competitor`, `is_complementary`, `is_commercial_other`, `is_excluded`
- `unique_category_count` (per place, later summed by area)

### 4.2 Derived POI Data Quality Score (0..1)

Implementation: `src/features/data_quality.py`

We compute a transparent POI score, clamped to \([0, 1]\), based on field presence:

Let base score be:
\[
q = 1.0
\]

Then subtract penalties if columns exist:
- missing coords: \(0.40\)
- missing name: \(0.20\)
- missing category: \(0.15\)
- unresolved flags present: \(0.25\)

Formally (boolean casts are 1 if true else 0):
\[
q = 1
 - 0.40\cdot \mathbb{1}[\text{lat or lon missing}]
 - 0.20\cdot \mathbb{1}[\text{name missing}]
 - 0.15\cdot \mathbb{1}[\text{category missing}]
 - 0.25\cdot \mathbb{1}[\text{unresolved\_flags present}]
\]
Then clamp:
\[
q := \min(1, \max(0, q))
\]

Area-level data quality is:
\[
\text{mean\_data\_quality\_score} = \frac{1}{N}\sum_{i=1}^{N} q_i
\]

### 4.3 Active POIs

In v1, we treat “active” as:
- not excluded by taxonomy (`~is_excluded`)
- (optionally) not closed if `date_closed` exists (not currently in our canonical inputs)

So:
\[
\text{is\_active} = \neg \text{is\_excluded}
\]

### 4.4 Aggregates per area (counts)

Implementation: `src/features/build_opportunity_features.py`

For each `area_id`:
- `total_poi_count`
- `active_poi_count`
- `competitor_count`
- `complementary_count`
- `commercial_other_count`
- `unique_category_count` (summed)
- `mean_data_quality_score` (mean)

### 4.5 Derived area features (transparent formulas)

We compute the core proxies:

**Competitor density proxy**
\[
\text{competitor\_density\_proxy}
=
\frac{\text{competitor\_count}}{\text{active\_poi\_count} + \varepsilon}
\]

**Complementarity ratio**
\[
\text{complementarity\_ratio}
=
\frac{\text{complementary\_count}}{\text{active\_poi\_count} + \varepsilon}
\]

**Commercial activity proxy**
\[
\text{commercial\_activity\_proxy} = \text{active\_poi\_count}
\]

**Saturation proxy**
\[
\text{saturation\_proxy}
=
\text{competitor\_density\_proxy}\cdot (1 - \text{complementarity\_ratio})
\]

Where \(\varepsilon = 1.0\) prevents division by zero.

`recent_refresh_share` is currently a placeholder because it depends on refresh timestamp columns that may not exist in the dataset we’re using.

---

## 5) Opportunity Score (math + implementation)

Implementation: `src/scoring/opportunity_score.py`

### 5.1 Stabilization (log1p)

For heavy-tailed signals we apply log transform:
- `commercial_activity_proxy_log = log(1 + commercial_activity_proxy)`
- `diversity_log = log(1 + unique_category_count)`
- `competition_log = log(1 + competitor_density_proxy)`
- `saturation_log = log(1 + saturation_proxy)`

### 5.2 Normalization (safe min–max)

For each signal \(x\) we compute:
\[
n(x)=
\begin{cases}
0 & \text{if } \max(x)-\min(x) \le 10^{-9}\\
\frac{x-\min(x)}{\max(x)-\min(x)} & \text{otherwise}
\end{cases}
\]

### 5.3 Weighted sum (transparent)

Default weights (`OpportunityWeights`):
- complementarity: \(+1.2\)
- commercial activity: \(+0.8\)
- diversity: \(+0.6\)
- direct competition: \(-1.0\)
- saturation: \(-0.7\)
- data quality: \(+0.5\)

Let normalized features be:
- \(n_c\) = `n_complementarity`
- \(n_a\) = `n_activity`
- \(n_d\) = `n_diversity`
- \(n_k\) = `n_competition`
- \(n_s\) = `n_saturation`
- \(n_q\) = `n_quality`

Then:
\[
\text{OpportunityScore}
=
1.2n_c
+0.8n_a
+0.6n_d
-1.0n_k
-0.7n_s
+0.5n_q
\]

### 5.4 Reasons (“built-in” explanation function)

We generate `reasons_top3` via `top_reasons(row)`:
- compute contribution components (e.g., `score_complementarity`, `score_activity`, etc.)
- return the top 3 labels by magnitude

This is deterministic and demo-safe (no LLM dependency).

---

## 6) Guardrails (demo stability + honesty)

In `scripts/run_build_features.py` we apply:
- `active_poi_count >= 200` to prevent tiny cells from dominating normalization.

In the Streamlit app we also expose:
- minimum active POIs
- minimum mean data quality

And we label outputs as **decision support**, not revenue/demand forecasting.

---

## 7) Processed outputs (what the app reads)

Produced by `scripts/run_build_features.py`:

- `data/processed/sydney_area_features.parquet`
- `data/processed/sydney_ranked_areas.parquet`
- `data/exports/top_ranked_areas.csv`
- docs previews:
  - `docs/feature_summary.md`
  - `docs/top_areas_preview.md`

The Streamlit demo is designed to **prefer precomputed ranked outputs** for performance.

---

## 8) Streamlit MVP (demo behavior)

### Required sections implemented
- Landing/setup panel (Sydney + Gym fixed for v1)
- Opportunity map (tooltip shows score drivers)
- Top candidates panel (table + pinning)
- “Why this location?” (metrics + narrative + examples)
- Compare mode (2–3 areas)
- Demo narrative mode (limitations + how score works)

### Judge-mode enhancements
- Judge Mode toggle + hero buttons (Map / Why / Compare)
- Stable presets for top_k and guardrails
- Clear tagline and closing line:
  - “Businesses do not need more map data. They need fewer bad location decisions.”

---

## 9) What changes when we add transport data (not implemented yet)

Transport data adds an **accessibility feature** (not demand):
- `transit_access_score` per area (0..1), derived from:
  - `station_entrance_count`
  - `pt_facility_count`
  - `bus_shelter_count`

We compute a weighted raw score then normalize to 0..1:
\[
r = \log(1 + 2.0\cdot entrances + 1.0\cdot facilities + 0.5\cdot bus\_shelters)
\]
\[
transit\_access\_score = \frac{r-\min(r)}{\max(r)-\min(r)}
\]

It enters the score as a new positive term (default weight \(0.4\)):
\[
\text{OpportunityScore}_{new}
= \text{OpportunityScore} + 0.4\cdot n(\text{transit\_access\_score})
\]

Guardrail: this is an **accessibility proxy**, not ridership, foot traffic, or demand prediction.


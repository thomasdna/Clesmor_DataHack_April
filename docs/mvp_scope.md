# Expansion Copilot — MVP Scope

## Core user persona
- **Primary**: Franchise expansion manager / growth lead (Sydney)
- **Context**: Needs a shortlist fast, needs an explanation for stakeholders

## Core question
**“Where should I open my next gym in Sydney, and why?”**

## Product scope (what we will build)

### 1) Ranked candidate areas
- Aggregate POIs into areas using **H3 hexes** (fast, explainable, boundary-free)
- Present a ranked list of the best candidate areas for a new gym

### 2) Map visualization
- Sydney map showing:
  - candidate area hexes / centroids
  - optional overlays: competitor POIs, key complements

### 3) Explanation panel (“Why this location?”)
For the selected area:
- **Competitors**: count + example POIs
- **Complements**: counts by group (e.g., transit, grocery, offices)
- **Data quality score**: derived from available fields (no assumptions)

### 4) Compare mode (2–3 areas)
Side-by-side comparison with:
- score breakdown
- competitor/complement mix
- data quality note

### 5) Export
- CSV export of ranked areas + explanation fields

## Non-goals (explicitly out of scope)
- Revenue forecasting / unit economics
- Property valuation / lease pricing
- Traffic simulation / drive-time routing
- Workforce, demographics, or census fusion (unless time remains)
- Generic tourism recommendations

## Data constraints & assumptions
- We **must** inspect the Parquet schema and use only columns that exist.
- If no explicit confidence field exists, we compute `data_quality_score` from:
  - null rates on key fields (lat/lon/name/category)
  - presence of category IDs
  - optional address fields (if present)

## MVP definition of “done”
- A Streamlit app that:
  - runs locally with a Sydney slice of data
  - shows a ranked list, a map, and an explanation panel
  - supports comparing 2–3 areas
  - exports results

# Expansion Copilot — MVP Scope

## Core user persona
- **Primary**: Franchise expansion manager / growth lead (Sydney)
- **Context**: Needs a shortlist fast, needs an explanation for stakeholders

## Core question
**“Where should I open my next gym in Sydney, and why?”**

## Product scope (what we will build)

### 1) Ranked candidate areas
- Aggregate POIs into areas using **H3 hexes** (fast, explainable, boundary-free)
- Present a ranked list of the best candidate areas for a new gym

### 2) Map visualization
- Sydney map showing:
  - candidate area hexes / centroids
  - optional overlays: competitor POIs, key complements

### 3) Explanation panel (“Why this location?”)
For the selected area:
- **Competitors**: count + example POIs
- **Complements**: counts by group (e.g., transit, grocery, offices)
- **Data quality score**: derived from available fields (no assumptions)

### 4) Compare mode (2–3 areas)
Side-by-side comparison with:
- score breakdown
- competitor/complement mix
- data quality note

### 5) Export
- CSV export of ranked areas + explanation fields

## Non-goals (explicitly out of scope)
- Revenue forecasting / unit economics
- Property valuation / lease pricing
- Traffic simulation / drive-time routing
- Workforce, demographics, or census fusion (unless time remains)
- Generic tourism recommendations

## Data constraints & assumptions
- We **must** inspect the Parquet schema and use only columns that exist.
- If no explicit confidence field exists, we compute `data_quality_score` from:
  - null rates on key fields (lat/lon/name/category)
  - presence of category IDs
  - optional address fields (if present)

## MVP definition of “done”
- A Streamlit app that:
  - runs locally with a Sydney slice of data
  - shows a ranked list, a map, and an explanation panel
  - supports comparing 2–3 areas
  - exports results


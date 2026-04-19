# Expansion Copilot — MVP Architecture

This is a **product-first** MVP architecture optimized for a working demo in 48 hours.

## Data ingestion layer
- **Source**: `foursquare/fsq-os-places` (Parquet)
- **Ingestion modes**
  - **Recommended**: `scripts/make_au_subset_remote.py` range-reads the HF Parquet and writes a local subset.
  - **Optional**: `scripts/download_fsq_release.py` downloads raw shards to `data/raw/...` for offline work.

## Filtering / schema inspection layer
- **Principle**: never assume columns.
- Implement `src/data/schema.py`:
  - load schema from `data/au_places.parquet`
  - detect canonical fields:
    - `place_id` (prefer `fsq_place_id`)
    - `name`
    - `latitude` / `longitude`
    - `category_ids` / `fsq_category_ids`
    - optional address fields if present
  - fail fast with a clear message if lat/lon missing

## Category mapping layer
- Input: `data/categories.parquet`
- Implement `src/data/categories.py`:
  - map **gym-related categories** to a set of IDs
  - define complement groups (transit, grocery, offices, schools, cafes, etc.)
  - provide label lookup for explanations

## Area aggregation layer
- Use **H3** to define “areas” without requiring suburb boundaries.
- Implement `src/features/h3_agg.py`:
  - convert POI lat/lon to H3 cell at chosen resolution (default \(r=8\))
  - aggregate metrics per cell:
    - total POIs
    - gym competitors
    - complements by group
    - centroids

## Scoring layer
- Implement `src/scoring/opportunity.py`:
  - `opportunity_score = complement_score - w * competitor_score`
  - weights locked for MVP and tunable in UI
- Implement `src/scoring/data_quality.py`:
  - derive `data_quality_score` from available fields (null-rate + presence checks)
  - never depend on a non-existent “confidence” field

## Streamlit UI layer
- Implement in `app/` (thin UI) calling `src/` modules:
  - **Controls**: H3 resolution, weights, competitor definition, complement groups
  - **Outputs**: ranked table + map + explanation panel + compare view

## Caching strategy
- Use Streamlit caching:
  - `st.cache_data` for loading parquet and category maps
  - cache H3 aggregation per (bbox, h3_res) and score params
- Keep recomputation under a few seconds for the demo.

## Export strategy
- Export ranked results to:
  - `data/exports/candidates.csv`
  - optionally `data/exports/candidates.parquet`
- Include explanation fields in export for stakeholder handoff.

## Fallback plan (if full dataset too large locally)
1. Use `--max-rows` to create a manageable local subset.
2. Limit to a **Sydney bounding box** early to reduce compute.
3. Reduce H3 resolution (larger hexes) to speed up aggregation.
4. If still slow: compute aggregates via DuckDB on Parquet directly.


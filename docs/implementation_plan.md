# Expansion Copilot — Implementation Plan (48 hours)

## Locked product plan (what we’re building)
- **Market**: Sydney
- **Category**: gyms / fitness studios
- **Outputs**: ranked areas + map + explanation + compare + export
- **Differentiator**: clear “Why here?” explanation

## Repo structure (target)
```
.
├── app/
├── data/
│   ├── raw/
│   ├── interim/
│   ├── processed/
│   └── exports/
├── docs/
├── notebooks/
├── src/
│   ├── config/
│   ├── data/
│   ├── features/
│   ├── scoring/
│   ├── app/
│   └── utils/
├── tests/
└── scripts/
```

## Milestone 1 (0–6h): Data + schema safety
- [ ] Ensure `scripts/make_au_subset_remote.py` produces `data/au_places.parquet` + `data/categories.parquet`
- [ ] Add `src/data/schema.py` to detect required columns and expose a canonical view:
  - `place_id`, `name`, `lat`, `lon`, `category_ids` (if present)
- [ ] Add a **Sydney bounding box** config in `src/config/geo.py`
- [ ] Add a quick validation script: row count in Sydney bbox, null counts on lat/lon

## Milestone 2 (6–18h): Aggregation + baseline scoring
- [ ] Implement `src/features/h3_agg.py`:
  - POI → H3 cell
  - per-cell aggregates: competitors, complements, totals
- [ ] Implement `src/scoring/opportunity.py`:
  - weights (UI-tunable)
  - produce a ranked table with score breakdown
- [ ] Implement `src/scoring/data_quality.py`:
  - derived score (null-rate + field presence)

## Milestone 3 (18–36h): Product UI (strong demo)
- [ ] Streamlit screens:
  - Overview: ranked areas + map
  - Detail: “Why here?” panel + nearby POIs
  - Compare: select 2–3 areas and compare breakdown
- [ ] Add exports to `data/exports/`
- [ ] Add caching so interactions stay <2s

## Milestone 4 (36–48h): Polish + narrative
- [ ] Tighten copy + labels (product-first language)
- [ ] Add guardrails for missing columns and small subsets
- [ ] Add `docs/demo_story.md` and run the 90-second demo script
- [ ] Record a backup demo video / screenshots

## Engineer checklist (immediately executable)
- [ ] (Data) Run:
  - `python scripts/make_au_subset_remote.py --dt 2026-04-14 --max-rows 2000000`
- [ ] (App) Run:
  - `streamlit run app/streamlit_app.py`
- [ ] (Dev) Implement `src/` modules in this order:
  1. `src/config/geo.py`
  2. `src/data/schema.py`
  3. `src/data/categories.py`
  4. `src/features/h3_agg.py`
  5. `src/scoring/data_quality.py`
  6. `src/scoring/opportunity.py`
  7. Update `app/streamlit_app.py` to use `src/` modules


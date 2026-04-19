# Expansion Copilot

Sharply scoped hackathon MVP to answer one question for a real user:

**“Where should I open my next boutique / mid‑market gym in Sydney, and why?”**

Built on the **Foursquare Open Source Places** dataset (`foursquare/fsq-os-places`).

## What the demo shows (MVP)

- **Ranked candidate areas** (Sydney) for a new gym
- **Map visualization** of candidate areas + key nearby POIs
- **“Why here?” explanation panel** (competitors, complements, data quality)
- **Compare mode** for 2–3 shortlisted areas
- **Export** results (CSV/Parquet) for handoff

## Primary persona

- **Franchise expansion manager / growth lead** evaluating suburbs/precincts in Sydney.

## Tech (locked)

- **Python 3.11**
- **Polars + DuckDB** for data work
- **Streamlit** for UI
- **Pydeck** for maps (H3 used where it speeds up aggregation/explainability)

## Repo structure

See `docs/implementation_plan.md` for the execution checklist and milestones.

## Quickstart

### 1) Create a venv

```bash
cd "Data_Hack_April"
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Add Hugging Face token

Create `Data_Hack_April/.env`:

```bash
HF_TOKEN=hf_...
```

### 3) Data foundation (Task 2)

#### A) Profile the real schema (writes to `docs/`)

```bash
python -m src.data.profile_schema
```

#### B) Build subsets + clean categories (writes to `data/interim/` and `docs/`)

```bash
python scripts/run_build_subsets.py
```

Outputs:
- `data/interim/categories_clean.parquet`
- `data/interim/places_au.parquet`
- `data/interim/places_sydney.parquet`
- `docs/schema_places.json`
- `docs/schema_categories.json`
- `docs/schema_summary.md`
- `docs/subset_build_report.md`

> Note: Sydney filtering uses locality/region fields if they exist; otherwise it falls back to a Sydney bounding box.

### 4) Run the app

```bash
streamlit run app/streamlit_app.py
```

### Tests

```bash
pytest -q
```

## Product docs (start here)

- `docs/product_brief.md`
- `docs/mvp_scope.md`
- `docs/architecture.md`
- `docs/implementation_plan.md`
- `docs/demo_story.md`


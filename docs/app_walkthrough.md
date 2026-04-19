# Expansion Copilot — App Walkthrough (MVP)

This MVP answers one question:

**“Where should I open my next gym in Sydney, and why?”**

## What data the app reads

The app is fast because it reads **only processed outputs**:

- `data/processed/sydney_ranked_areas.parquet` (preferred)
- or `data/processed/sydney_area_features.parquet` (fallback; re-scores in-app with strategy mode weights)

If neither file exists, the app runs in a small **sample fallback mode** so the demo UI still works.

## Page / sections

### Landing / setup panel

- Fixed scope for v1: **Sydney**, **Gym**
- Select:
  - **Area unit**: `h3` (recommended) or `locality` (only if you’ve generated locality outputs)
  - **Strategy mode**:
    - balanced growth
    - low competition
    - ecosystem-first
  - Guardrails:
    - minimum data quality
    - minimum active POIs per area

### Opportunity map

- Plots the top-ranked areas
- **Color**: opportunity score (red → green)
- **Tooltip** includes**:** area name, score, competitor count, complementary count, activity proxy, data quality

### Top candidates panel

- Sortable shortlist table with the key columns needed for the decision
- Selection drives the “Why this location?” panel
- “Pin selected” supports Compare mode

### Why this location?

For the selected area, the app shows:

- A concise product-style explanation
- A factor breakdown:
  - **Pros**
  - **Risks**
  - **What to validate next in real life**

### Compare

- Compare 2–3 pinned/selected areas side-by-side
- Shows the decision factors and a short reason line

### Demo narrative mode

An explainer expander in the sidebar:

- user
- question
- how the score works
- limitations / “decision support, not prediction”


# Opportunity score (MVP)

This score is **decision support**, not revenue prediction.
All components are **proxies** derived from POI composition.

## Inputs (per area)
- `active_poi_count`: proxy for activity (we treat “active” as not excluded; closed flags are used only if present in schema)
- `competitor_count`: direct competitors in the area (gym taxonomy)
- `complementary_count`: complementary ecosystem venues in the area
- `unique_category_count`: proxy for diversity
- `mean_data_quality_score`: coverage/quality proxy

Derived:
- `competitor_density_proxy = competitor_count / (active_poi_count + 1)`
- `complementarity_ratio = complementary_count / (active_poi_count + 1)`
- `commercial_activity_proxy = active_poi_count`
- `saturation_proxy = competitor_density_proxy * (1 - complementarity_ratio)`

## Scoring formula

We log-transform heavy-tailed features with `log1p`, then min-max normalize each component to \([0,1]\).

Opportunity Score =
- **+ complementarity**
- **+ commercial activity**
- **+ diversity**
- **− direct competition**
- **− saturation**
- **+ data quality**

Weights are configurable in `src/scoring/opportunity_score.py`.

## Guardrails
- This does **not** measure real demand or foot traffic.
- “Activity” is a proxy for mixed-use intensity, not visits.
- Results should be reviewed before any real-world expansion.


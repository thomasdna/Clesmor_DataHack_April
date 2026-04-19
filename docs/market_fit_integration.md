# Market fit integration (Task 5)

Goal: add a **demand/affluence proxy layer** to Expansion Copilot so the recommendation is not only about POI ecosystem + access, but also whether an area is likely to support the target gym positioning.

## Dataset (open)

We use the ABS SEIFA 2021 SA2 summary table:
- **IRSAD** (Index of Relative Socio-economic Advantage and Disadvantage) — primary affluence proxy
- **IER** (Index of Economic Resources) — secondary proxy
- plus SA2 population (context only)

File:
- `data/raw/abs/seifa_sa2_2021.xlsx` (ABS download)

## Geography

SEIFA is published by **SA2**. Our product aggregates to **H3**.

We join H3 areas to SA2 by:
- taking each H3 area centroid (mean lat/lon of POIs in that cell)
- doing a centroid-in-polygon lookup against ABS SA2 2021 boundaries (`SA2_2021_AUST_GDA2020.shp`)

This is intentionally simple and demo-stable.

## Market fit score (transparent)

Let:
- \(n_{irsad}\) be min–max normalized IRSAD score
- \(n_{ier}\) be min–max normalized IER score

Then:
\[
market\_fit\_score = 0.7\cdot n_{irsad} + 0.3\cdot n_{ier}
\]

## How it affects Opportunity Score

We add a positive term:
\[
OpportunityScore_{new} = OpportunityScore + w_m\cdot n(market\_fit\_score)
\]

Default \(w_m = 0.6\) (tunable later).

## UI / demo

- “Why here?” includes:
  - `Market fit (proxy)` and SEIFA decile
- Reasons can include “Strong market fit”

Guardrail: this remains decision support. SEIFA is a socioeconomic proxy, not a guarantee of gym demand.


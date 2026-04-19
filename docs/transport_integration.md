# Transport integration (Theme 3 add-on)

This document describes how we integrate NSW open transport datasets into Expansion Copilot **without turning the product into a transport dashboard**.

## Goal

Add a single defensible signal: **accessibility**.

This improves the gym expansion ranking by differentiating areas with similar POI ecosystem profiles but different ease-of-access for members.

## Datasets used (static points)

We start with low-maintenance, stable location point datasets:

- **Train station entrance locations** (CSV)  
  Used as high-signal access points.
- **Public transport location facilities** (CSV)  
  Includes stations/wharves/interchanges (broader coverage).
- **City of Sydney bus shelters** (GeoJSON)  
  Proxy for bus stop density in the CBD + inner city.

All are open data with CC-BY style licensing (see Data.NSW / TfNSW Open Data Hub metadata).

## Pipeline

1. Download to `data/raw/nsw_transport/`:
   - `scripts/download_public_datasets.py`
2. Load + canonicalize to a points table:
   - `src/data/load_transport.py` → columns: `kind, source, lat, lon, name`
3. Assign transport points to H3 (same resolution as the app):
   - `src/features/transit_access.py`
4. Aggregate to H3 features:
   - `station_entrance_count`
   - `pt_facility_count`
   - `bus_shelter_count`
   - `transit_access_score` (0–1)

## Transit access score (transparent)

We compute:

1. Weighted raw score:
   \[
   r = \log(1 + 2.0\cdot entrances + 1.0\cdot facilities + 0.5\cdot bus\_shelters)
   \]
2. Min–max normalize:
   \[
   transit\_access\_score = \frac{r-\min(r)}{\max(r)-\min(r)}
   \]

## How it affects Opportunity Score

We add a positive term (default weight 0.4):

\[
OpportunityScore_{new} = OpportunityScore + 0.4 \cdot n(transit\_access\_score)
\]

Guardrail: this is an **accessibility proxy**, not ridership, foot traffic, or demand prediction.

## Demo/UI changes

- Map tooltip shows `Transit access`
- Shortlist table includes `transit_access_score` if available
- “Why here?” includes a small transit accessibility callout


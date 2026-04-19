# Data quality logic (MVP)

Expansion Copilot does not assume any explicit `confidence` field exists in the dataset.
If no confidence field is present, we compute a **derived `data_quality_score`**.

## POI-level score (transparent)

Start with \(1.0\) and subtract penalties (only if those columns exist):

- Missing coordinates (`lat` or `lon` null): **−0.40**
- Missing name (`name` null): **−0.20**
- Missing category (`category_id` null): **−0.15**
- Unresolved flags present (`unresolved_flags` not null): **−0.25**

Then clamp to \([0, 1]\).

## Area-level score

For an H3 area, compute the **mean** POI-level score across POIs in the area.

## Why this is good enough for the hackathon

- It’s **explainable** and auditable.
- It adapts to schema reality (missing columns simply don’t contribute).
- It creates a stable signal for “coverage quality” without overengineering.


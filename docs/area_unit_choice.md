# Area unit choice (MVP)

We support two aggregation options:
1. **Locality/suburb** (human-friendly, but requires locality fields)
2. **H3 grid** (always available when lat/lon exist)

## What we recommend for the Sydney demo
**Use H3 first**.

Why:
- Our current `places_sydney.parquet` schema includes **lat/lon** but does **not** include `locality` / `postcode`.
- H3 lets us produce stable “areas” without needing boundary files.
- It’s fast, explainable, and consistent across releases.

## When to use locality/suburb
If future releases (or a richer subset build) include `locality` and `region`, locality aggregation becomes a good “presentation layer” for stakeholders.

## H3 resolution
For Sydney:
- Start with **H3 res 8** for a good balance (neighborhood-scale).
- Adjust if too coarse/fine for the demo.


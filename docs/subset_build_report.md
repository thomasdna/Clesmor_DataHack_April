# Subset Build Report

## Outputs
- **categories_clean**: `data/interim/categories_clean.parquet`
- **places_au**: `data/interim/places_au.parquet`
- **places_sydney**: `data/interim/places_sydney.parquet`

## Notes
- Sydney filtering uses a locality/region strategy if those fields exist; otherwise it falls back to a Sydney bounding box.

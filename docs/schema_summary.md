# Schema Summary

## Places
- **path**: `data/au_places.parquet`
- **columns**: 6

### Field presence (explicit checks)
- **latitude**: ✅ present
- **longitude**: ✅ present
- **country**: ✅ present
- **locality**: ❌ missing
- **region**: ❌ missing
- **postcode**: ❌ missing
- **date_refreshed**: ❌ missing
- **date_closed**: ❌ missing
- **fsq_category_ids**: ✅ present
- **fsq_category_labels**: ❌ missing
- **unresolved_flags**: ❌ missing
- **geom**: ❌ missing
- **bbox**: ❌ missing
- **confidence**: ❌ missing
- **confidence_score**: ❌ missing

### Columns
`fsq_place_id`, `name`, `latitude`, `longitude`, `country`, `fsq_category_ids`

## Categories
- **path**: `data/categories.parquet`
- **columns**: 16

### Field presence (explicit checks)
- **category_id**: ✅ present
- **category_name**: ✅ present
- **category_label**: ✅ present
- **level1_category_id**: ✅ present
- **level1_category_name**: ✅ present
- **level2_category_id**: ✅ present
- **level2_category_name**: ✅ present

### Columns
`category_id`, `category_level`, `category_name`, `category_label`, `level1_category_id`, `level1_category_name`, `level2_category_id`, `level2_category_name`, `level3_category_id`, `level3_category_name`, `level4_category_id`, `level4_category_name`, `level5_category_id`, `level5_category_name`, `level6_category_id`, `level6_category_name`

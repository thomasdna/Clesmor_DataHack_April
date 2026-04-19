from __future__ import annotations

import polars as pl

from src.features.data_quality import data_quality_by_area

def compute_data_quality_by_area(area_pois: pl.DataFrame) -> pl.DataFrame:
    """
    Derived data quality score in [0, 1] per H3 cell from available fields.
    No assumption of any explicit confidence column.
    """
    return data_quality_by_area(area_pois)


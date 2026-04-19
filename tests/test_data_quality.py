from __future__ import annotations

import polars as pl

from src.features.data_quality import derived_data_quality_score


def test_data_quality_score_clamps_and_penalizes_missing() -> None:
    df = pl.DataFrame(
        {
            "lat": [1.0, None],
            "lon": [2.0, 2.0],
            "name": ["x", None],
            "category_id": ["c", None],
        }
    )
    out = df.with_columns(derived_data_quality_score(df)).select("data_quality_score").to_series().to_list()
    assert 0.0 <= out[0] <= 1.0
    assert 0.0 <= out[1] <= 1.0
    assert out[0] > out[1]


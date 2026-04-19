from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class RenterRateConfig:
    """
    ABS 2021 Census G37 tenure (SA2) extracted for Sydney bbox.

    Source: Digital Atlas of Australia (ArcGIS feature service), queried and cached locally.
    """

    abs_g37_sydney_json: Path = Path("data/raw/housing_market/abs_g37_tenure_sa2_sydney.json")


def load_abs_g37_renter_rate_sa2(cfg: RenterRateConfig = RenterRateConfig()) -> pl.DataFrame:
    """
    Returns one row per SA2 with:
    - sa2_code_2021
    - sa2_name_2021
    - renter_households_2021 (r_tot_total)
    - total_households_2021 (total_total)
    - renter_share_2021 = renter / total
    """
    p = cfg.abs_g37_sydney_json
    if not p.exists():
        return pl.DataFrame(
            {
                "sa2_code_2021": pl.Series([], dtype=pl.Int64),
                "sa2_name_2021": pl.Series([], dtype=pl.Utf8),
                "renter_households_2021": pl.Series([], dtype=pl.Int64),
                "total_households_2021": pl.Series([], dtype=pl.Int64),
                "renter_share_2021": pl.Series([], dtype=pl.Float64),
            }
        )

    j = json.loads(p.read_text(encoding="utf-8"))
    feats = j.get("features", [])
    rows = [f.get("attributes", {}) for f in feats]
    if not rows:
        return pl.DataFrame(
            {
                "sa2_code_2021": pl.Series([], dtype=pl.Int64),
                "sa2_name_2021": pl.Series([], dtype=pl.Utf8),
                "renter_households_2021": pl.Series([], dtype=pl.Int64),
                "total_households_2021": pl.Series([], dtype=pl.Int64),
                "renter_share_2021": pl.Series([], dtype=pl.Float64),
            }
        )

    df = pl.DataFrame(rows).select(
        [
            pl.col("sa2_code_2021").cast(pl.Int64, strict=False),
            pl.col("sa2_name_2021").cast(pl.Utf8),
            pl.col("r_tot_total").cast(pl.Int64, strict=False).alias("renter_households_2021"),
            pl.col("total_total").cast(pl.Int64, strict=False).alias("total_households_2021"),
        ]
    )
    eps = 1.0
    df = df.with_columns(
        (pl.col("renter_households_2021") / (pl.col("total_households_2021") + eps))
        .clip(0.0, 1.0)
        .alias("renter_share_2021")
    )
    return df


from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class HousingAffordabilityConfig:
    """
    Inputs from Regional Data Hub (Census-derived).

    This is not "median rent". It's an affordability indicator:
    share of renting households whose rent is >30% of household income (rental stress proxy).
    """

    maid_raid_hied_2021_csv: Path = Path("data/raw/housing/2021_maid_raid_hied.csv")


def load_raid_2021_by_sa2(cfg: HousingAffordabilityConfig = HousingAffordabilityConfig()) -> pl.DataFrame:
    """
    Returns one row per SA2 with:
    - sa2_code_2021 (Int64)
    - sa2_name_2021 (Utf8)
    - rent_stress_households (Int64)
    - total_renting_households (Int64)

    Source file schema (wide): the "Total" column holds total households for that row.
    """
    p = cfg.maid_raid_hied_2021_csv
    if not p.exists():
        return pl.DataFrame(
            {
                "sa2_code_2021": pl.Series([], dtype=pl.Int64),
                "sa2_name_2021": pl.Series([], dtype=pl.Utf8),
                "rent_stress_households": pl.Series([], dtype=pl.Int64),
                "total_renting_households": pl.Series([], dtype=pl.Int64),
            }
        )

    # The CSV starts with a "Note:" line; skip until the header row.
    df = pl.read_csv(str(p), skip_rows=1, infer_schema_length=200, ignore_errors=True)

    # Filter to SA2 rows only.
    df = df.filter(pl.col("GEOGRAPHY_LEVEL") == "SA2 2021").with_columns(
        [
            pl.col("GEOGRAPHY_CODE_2021").cast(pl.Int64, strict=False).alias("sa2_code_2021"),
            pl.col("GEOGRAPHY_NAME_2021").cast(pl.Utf8).alias("sa2_name_2021"),
            pl.col("Total").cast(pl.Int64, strict=False).alias("_total"),
        ]
    )

    # Pull the two RAID rows we need.
    stress = (
        df.filter(
            (pl.col("Affordability_Indicator") == "RAID Rent Affordability Indicator")
            & (pl.col("Affordability_Indicator_Category") == "Households where rent payments are more than 30% of household income")
        )
        .select(["sa2_code_2021", "_total"])
        .rename({"_total": "rent_stress_households"})
    )
    total = (
        df.filter(
            (pl.col("Affordability_Indicator") == "RAID Rent Affordability Indicator")
            & (pl.col("Affordability_Indicator_Category") == "Total renting households")
        )
        .select(["sa2_code_2021", "_total", "sa2_name_2021"])
        .rename({"_total": "total_renting_households"})
    )

    out = total.join(stress, on="sa2_code_2021", how="left").with_columns(
        pl.col("rent_stress_households").fill_null(0).cast(pl.Int64)
    )
    return out


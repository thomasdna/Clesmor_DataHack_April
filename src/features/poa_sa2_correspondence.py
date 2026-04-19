from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import polars as pl


@dataclass(frozen=True)
class POAToSA2CorrespondenceConfig:
    """
    Build a POA_2021 → SA2_2021 correspondence using ABS ASGS 2021 allocation files.

    These allocation files are MB-based and (in this version) do not include a population/dwelling weighting column,
    so we use **Mesh Block counts** as a pragmatic weight:
      ratio = (#MBs in POA that fall in SA2) / (total #MBs in POA)

    This is more robust than a single centroid mapping, but still an approximation.
    """

    poa_alloc_xlsx: Path = Path("data/raw/housing_market/asgs2021_allocation/POA_2021_AUST.xlsx")
    mb_alloc_xlsx: Path = Path("data/raw/housing_market/asgs2021_allocation/MB_2021_AUST.xlsx")


def build_poa_to_sa2_correspondence(cfg: POAToSA2CorrespondenceConfig = POAToSA2CorrespondenceConfig()) -> pl.DataFrame:
    """
    Returns:
    - poa_code_2021 (Int64)
    - sa2_code_2021 (Int64)
    - mb_count (Int64)
    - mb_ratio_from_poa_to_sa2 (Float64)
    """
    if (not cfg.poa_alloc_xlsx.exists()) or (not cfg.mb_alloc_xlsx.exists()):
        return pl.DataFrame(
            {
                "poa_code_2021": pl.Series([], dtype=pl.Int64),
                "sa2_code_2021": pl.Series([], dtype=pl.Int64),
                "mb_count": pl.Series([], dtype=pl.Int64),
                "mb_ratio_from_poa_to_sa2": pl.Series([], dtype=pl.Float64),
            }
        )

    # Read the minimal columns from each sheet.
    poa = pd.read_excel(cfg.poa_alloc_xlsx, sheet_name=0, usecols=["MB_CODE_2021", "POA_CODE_2021"])
    mb = pd.read_excel(cfg.mb_alloc_xlsx, sheet_name=0, usecols=["MB_CODE_2021", "SA2_CODE_2021"])

    poa_df = pl.from_pandas(poa).rename({"MB_CODE_2021": "mb_code_2021", "POA_CODE_2021": "poa_code_2021"}).with_columns(
        [pl.col("mb_code_2021").cast(pl.Int64, strict=False), pl.col("poa_code_2021").cast(pl.Int64, strict=False)]
    )
    mb_df = pl.from_pandas(mb).rename({"MB_CODE_2021": "mb_code_2021", "SA2_CODE_2021": "sa2_code_2021"}).with_columns(
        [pl.col("mb_code_2021").cast(pl.Int64, strict=False), pl.col("sa2_code_2021").cast(pl.Int64, strict=False)]
    )

    joined = poa_df.join(mb_df, on="mb_code_2021", how="inner").filter(
        pl.col("poa_code_2021").is_not_null() & pl.col("sa2_code_2021").is_not_null()
    )

    pair = joined.group_by(["poa_code_2021", "sa2_code_2021"]).agg(pl.len().alias("mb_count"))
    totals = pair.group_by("poa_code_2021").agg(pl.col("mb_count").sum().alias("mb_total"))

    out = pair.join(totals, on="poa_code_2021", how="left").with_columns(
        (pl.col("mb_count") / pl.when(pl.col("mb_total") <= 0).then(1.0).otherwise(pl.col("mb_total"))).alias(
            "mb_ratio_from_poa_to_sa2"
        )
    )
    return out.select(["poa_code_2021", "sa2_code_2021", "mb_count", "mb_ratio_from_poa_to_sa2"])


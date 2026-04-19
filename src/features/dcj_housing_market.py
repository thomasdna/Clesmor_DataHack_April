from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import polars as pl


@dataclass(frozen=True)
class DCJHousingMarketConfig:
    """
    NSW DCJ Rent & Sales Report tables (quarterly).

    We use the 'Postcode' sheet which contains the latest quarter stats by NSW postcode.
    """

    dcj_sales_xlsx: Path = Path("data/raw/housing_market/dcj_sales_tables_june_2025.xlsx")
    dcj_rent_xlsx: Path = Path("data/raw/housing_market/dcj_rent_tables_sept_2025.xlsx")


def _find_header_row(df: pd.DataFrame, first_col_name: str) -> int:
    """
    The DCJ spreadsheets contain explanatory rows before the real header.
    Find the row index where the first column equals something like 'Postcode'.
    """
    col0 = df.iloc[:, 0].astype(str).str.strip().str.lower()
    target = first_col_name.strip().lower()
    hits = col0[col0 == target]
    if len(hits) == 0:
        # fallback: any row containing the target
        hits = col0[col0.str.contains(target, na=False)]
    if len(hits) == 0:
        raise ValueError(f"Could not find header row containing '{first_col_name}'.")
    return int(hits.index[0])


def _clean_numeric(x) -> float | None:
    if x is None:
        return None
    s = str(x).strip()
    if s in ("", "nan", "NaN", "-", "s", "(s)"):
        return None
    # remove commas and dollar signs
    s = s.replace(",", "").replace("$", "")
    try:
        return float(s)
    except Exception:
        return None


def load_dcj_rent_by_postcode(cfg: DCJHousingMarketConfig = DCJHousingMarketConfig()) -> pl.DataFrame:
    """
    Returns one row per postcode with best-effort:
    - poa_code_2021 (Int64)
    - median_weekly_rent_total (Float64)
    - bonds_lodged_total (Int64)
    """
    p = cfg.dcj_rent_xlsx
    if not p.exists():
        return pl.DataFrame(
            {
                "poa_code_2021": pl.Series([], dtype=pl.Int64),
                "median_weekly_rent_total": pl.Series([], dtype=pl.Float64),
                "bonds_lodged_total": pl.Series([], dtype=pl.Int64),
            }
        )

    raw = pd.read_excel(p, sheet_name="Postcode", header=None)
    hdr = _find_header_row(raw, "Postcode")
    df = pd.read_excel(p, sheet_name="Postcode", header=hdr)
    df.columns = [str(c).strip() for c in df.columns]

    # Standardize postcode column
    post_col = [c for c in df.columns if c.strip().lower() == "postcode"]
    if not post_col:
        post_col = [df.columns[0]]
    post_col = post_col[0]

    # Prefer the known DCJ column names.
    rent_col = None
    for c in df.columns:
        if "Median Weekly Rent" in c:
            rent_col = c
            break
    bonds_col = None
    for c in df.columns:
        if "New Bonds Lodged" in c:
            bonds_col = c
            break

    # Keep only the "Total / Total" rollup row per postcode if those columns exist.
    if "Dwelling Types" in df.columns and "Number of Bedrooms" in df.columns:
        df = df[
            (df["Dwelling Types"].astype(str).str.strip().str.lower() == "total")
            & (df["Number of Bedrooms"].astype(str).str.strip().str.lower() == "total")
        ]

    out = pd.DataFrame()
    out["poa_code_2021"] = pd.to_numeric(df[post_col], errors="coerce").astype("Int64")
    if rent_col is not None:
        out["median_weekly_rent_total"] = df[rent_col].map(_clean_numeric)
    else:
        out["median_weekly_rent_total"] = None
    if bonds_col is not None:
        out["bonds_lodged_total"] = pd.to_numeric(df[bonds_col].map(_clean_numeric), errors="coerce").astype("Int64")
    else:
        out["bonds_lodged_total"] = pd.NA

    out = out.dropna(subset=["poa_code_2021"])
    return pl.from_pandas(out)


def load_dcj_sales_by_postcode(cfg: DCJHousingMarketConfig = DCJHousingMarketConfig()) -> pl.DataFrame:
    """
    Returns one row per postcode with best-effort:
    - poa_code_2021 (Int64)
    - median_sale_price_total (Float64)
    - sales_total (Int64)
    """
    p = cfg.dcj_sales_xlsx
    if not p.exists():
        return pl.DataFrame(
            {
                "poa_code_2021": pl.Series([], dtype=pl.Int64),
                "median_sale_price_total": pl.Series([], dtype=pl.Float64),
                "sales_total": pl.Series([], dtype=pl.Int64),
            }
        )

    raw = pd.read_excel(p, sheet_name="Postcode", header=None)
    hdr = _find_header_row(raw, "Postcode")
    df = pd.read_excel(p, sheet_name="Postcode", header=hdr)
    df.columns = [str(c).strip() for c in df.columns]

    post_col = [c for c in df.columns if c.strip().lower() == "postcode"]
    if not post_col:
        post_col = [df.columns[0]]
    post_col = post_col[0]

    # Prefer known DCJ column names.
    med_col = None
    for c in df.columns:
        if "Median Sales Price" in c:
            med_col = c
            break
    cnt_col = None
    for c in df.columns:
        if c.strip() == "Sales\nNo." or c.strip().lower().startswith("sales"):
            cnt_col = c
            break

    # Keep only Total dwelling type (postcode-level rollup).
    dt_col = "Dwelling Type" if "Dwelling Type" in df.columns else ("Dwelling Types" if "Dwelling Types" in df.columns else None)
    if dt_col is not None:
        df = df[df[dt_col].astype(str).str.strip().str.lower() == "total"]

    out = pd.DataFrame()
    out["poa_code_2021"] = pd.to_numeric(df[post_col], errors="coerce").astype("Int64")
    if med_col is not None:
        # Values are in $'000s in the DCJ sheet.
        out["median_sale_price_total"] = df[med_col].map(_clean_numeric).map(lambda v: None if v is None else float(v) * 1000.0)
    else:
        out["median_sale_price_total"] = None
    if cnt_col is not None:
        out["sales_total"] = pd.to_numeric(df[cnt_col].map(_clean_numeric), errors="coerce").astype("Int64")
    else:
        out["sales_total"] = pd.NA

    out = out.dropna(subset=["poa_code_2021"])
    return pl.from_pandas(out)


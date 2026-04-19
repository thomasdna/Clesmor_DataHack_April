from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl


@dataclass(frozen=True)
class CategoryIndex:
    id_to_name: dict[str, str]
    id_to_label: dict[str, str]


def load_category_index(categories_parquet: Path) -> CategoryIndex:
    df = pl.read_parquet(str(categories_parquet), columns=["category_id", "category_name", "category_label"])
    id_to_name = dict(zip(df["category_id"].to_list(), df["category_name"].to_list(), strict=False))
    id_to_label = dict(zip(df["category_id"].to_list(), df["category_label"].to_list(), strict=False))
    return CategoryIndex(id_to_name=id_to_name, id_to_label=id_to_label)


def _match_ids_by_keywords(idx: CategoryIndex, keywords: Iterable[str]) -> set[str]:
    kws = [k.lower() for k in keywords]
    out: set[str] = set()
    for cid, name in idx.id_to_name.items():
        n = (name or "").lower()
        if any(k in n for k in kws):
            out.add(cid)
    return out


def gym_competitor_category_ids(idx: CategoryIndex, mode: str = "strict") -> set[str]:
    """
    Returns category IDs treated as direct competitors.

    - strict: gym/fitness/health club only (minimize false positives)
    - broad: include adjacent boutique fitness (yoga/pilates/boxing/etc.)
    """
    mode = mode.lower().strip()
    strict = _match_ids_by_keywords(idx, keywords=["gym", "fitness", "health club"])
    if mode == "strict":
        return strict

    broad = strict | _match_ids_by_keywords(
        idx,
        keywords=[
            "pilates",
            "yoga",
            "boxing",
            "martial",
            "crossfit",
            "personal training",
            "strength",
            "bootcamp",
        ],
    )
    return broad


def complement_groups(idx: CategoryIndex) -> dict[str, set[str]]:
    # Lightweight MVP grouping for explanations.
    return {
        "Transit": _match_ids_by_keywords(idx, ["train", "station", "bus", "tram", "metro", "ferry", "transit"]),
        "Grocery & Essentials": _match_ids_by_keywords(idx, ["supermarket", "grocery", "convenience", "pharmacy"]),
        "Food & Coffee": _match_ids_by_keywords(idx, ["cafe", "coffee", "restaurant", "bakery", "fast food"]),
        "Work & Study": _match_ids_by_keywords(idx, ["office", "cowork", "university", "college", "school"]),
        "Retail": _match_ids_by_keywords(idx, ["mall", "shopping", "department store", "clothing", "retail"]),
        "Parks": _match_ids_by_keywords(idx, ["park", "trail", "playground"]),
    }


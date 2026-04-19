from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import polars as pl

from src.config.business_taxonomies import BusinessTaxonomy, normalize_keywords
from src.data.prepare_categories import explode_category_ids, join_categories


@dataclass(frozen=True)
class PlaceRoles:
    place_id: str
    is_competitor: bool
    is_complementary: bool
    is_excluded: bool
    is_commercial_other: bool


def _contains_any(expr: pl.Expr, keywords: Iterable[str]) -> pl.Expr:
    kws = list(normalize_keywords(keywords))
    if not kws:
        return pl.lit(False)
    return pl.any_horizontal([expr.str.to_lowercase().str.contains(k) for k in kws])


def map_places_to_roles(
    places: pl.DataFrame,
    categories_clean: pl.DataFrame,
    taxonomy: BusinessTaxonomy,
) -> pl.DataFrame:
    """
    Returns one row per place_id with boolean role flags.

    Prevents explode/join inflation by aggregating back to place_id using any().
    Precedence (for human interpretation): excluded > competitor > complementary > commercial_other
    """
    exploded = explode_category_ids(places, col="category_ids")
    joined = join_categories(exploded, categories_clean, how="left")

    # Build a text field for matching.
    text = (
        pl.concat_str(
            [
                pl.col("category_name").fill_null(""),
                pl.lit(" | "),
                pl.col("category_label").fill_null(""),
            ]
        )
        .cast(pl.Utf8)
        .alias("category_text")
    )

    joined = joined.with_columns(text)

    is_excluded = _contains_any(pl.col("category_text"), taxonomy.exclusions_keywords).alias("is_excluded")
    is_competitor = _contains_any(pl.col("category_text"), taxonomy.competitor_keywords).alias("is_competitor")
    is_complementary = _contains_any(pl.col("category_text"), taxonomy.complementary_keywords).alias("is_complementary")
    is_other = _contains_any(pl.col("category_text"), taxonomy.commercial_other_keywords).alias("is_commercial_other")

    joined = joined.with_columns([is_excluded, is_competitor, is_complementary, is_other])

    by_place = (
        joined.group_by("place_id")
        .agg(
            [
                pl.col("is_excluded").any(),
                pl.col("is_competitor").any(),
                pl.col("is_complementary").any(),
                pl.col("is_commercial_other").any(),
                pl.col("category_id").drop_nulls().n_unique().alias("unique_category_count"),
            ]
        )
        .with_columns(
            # Exclusion overrides.
            pl.when(pl.col("is_excluded"))
            .then(True)
            .otherwise(pl.col("is_excluded"))
            .alias("is_excluded")
        )
    )

    return places.join(by_place, on="place_id", how="left").with_columns(
        [
            pl.col("is_excluded").fill_null(False),
            pl.col("is_competitor").fill_null(False),
            pl.col("is_complementary").fill_null(False),
            pl.col("is_commercial_other").fill_null(False),
            pl.col("unique_category_count").fill_null(0),
        ]
    )


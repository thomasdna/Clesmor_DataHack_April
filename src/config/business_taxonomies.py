from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from src.scoring.opportunity_score import OpportunityWeights


@dataclass(frozen=True)
class BusinessTaxonomy:
    """
    A transparent, editable taxonomy.

    We intentionally express taxonomy primarily as keyword rules against
    category_name / category_label (from categories_clean), because the exact
    IDs can change across releases.
    """

    competitor_keywords: tuple[str, ...]
    complementary_keywords: tuple[str, ...]
    exclusions_keywords: tuple[str, ...]

    # Optional “neutral” business activity keywords (not competitor/complement).
    commercial_other_keywords: tuple[str, ...] = ()


@dataclass(frozen=True)
class BusinessTemplate:
    """
    A product-facing template: taxonomy + default scoring weights.
    Keep this small and curated for demo reliability.
    """

    key: str
    label: str
    taxonomy: BusinessTaxonomy
    weights: OpportunityWeights


GYM_TAXONOMY = BusinessTaxonomy(
    competitor_keywords=(
        "gym",
        "fitness",
        "health club",
        "pilates",
        "yoga",
        "boxing",
        "martial",
        "crossfit",
        "personal training",
        "recreation center",
        "sports club",
    ),
    complementary_keywords=(
        "cafe",
        "coffee",
        "juice",
        "smoothie",
        "health food",
        "supermarket",
        "grocery",
        "pharmacy",
        "physio",
        "physical therapy",
        "massage",
        "chiropractor",
        "sports goods",
        "sporting goods",
        "wellness",
        "park",
        "trail",
    ),
    exclusions_keywords=(
        # MVP exclusions: leave mostly empty; add only when clearly harmful.
    ),
    commercial_other_keywords=(
        "restaurant",
        "retail",
        "shopping",
        "office",
        "cowork",
        "school",
        "university",
        "transit",
        "station",
    ),
)

CAFE_TAXONOMY = BusinessTaxonomy(
    competitor_keywords=(
        "cafe",
        "coffee",
        "espresso",
        "tea",
        "bakery",
        "brunch",
        "breakfast",
    ),
    complementary_keywords=(
        "office",
        "cowork",
        "university",
        "school",
        "park",
        "transit",
        "station",
        "shopping",
        "retail",
        "gym",
        "fitness",
    ),
    exclusions_keywords=(),
    commercial_other_keywords=("restaurant", "bar", "takeaway", "fast food"),
)

CLINIC_TAXONOMY = BusinessTaxonomy(
    # Allied health / clinic-style businesses
    competitor_keywords=(
        "physio",
        "physical therapy",
        "chiropractor",
        "dental",
        "dentist",
        "medical",
        "clinic",
        "podiatry",
        "optometrist",
    ),
    complementary_keywords=(
        "pharmacy",
        "supermarket",
        "shopping",
        "retail",
        "gym",
        "fitness",
        "yoga",
        "pilates",
        "park",
        "transit",
        "station",
    ),
    exclusions_keywords=(),
    commercial_other_keywords=("hospital", "specialist"),
)

COWORKING_TAXONOMY = BusinessTaxonomy(
    competitor_keywords=(
        "cowork",
        "co-work",
        "shared office",
        "office space",
        "serviced office",
        "workspace",
    ),
    complementary_keywords=(
        "cafe",
        "coffee",
        "restaurant",
        "transit",
        "station",
        "retail",
        "shopping",
        "gym",
        "fitness",
    ),
    exclusions_keywords=(),
    commercial_other_keywords=("office", "university"),
)


BUSINESS_TEMPLATES: dict[str, BusinessTemplate] = {
    "gym": BusinessTemplate(
        key="gym",
        label="Gym / boutique fitness",
        taxonomy=GYM_TAXONOMY,
        weights=OpportunityWeights(
            complementarity=1.2,
            commercial_activity=0.8,
            diversity=0.6,
            direct_competition=1.0,
            saturation=0.7,
            data_quality=0.5,
            transit_access=0.4,
            market_fit=0.6,
            rent_affordability=0.35,
        ),
    ),
    "cafe": BusinessTemplate(
        key="cafe",
        label="Café / coffee shop",
        taxonomy=CAFE_TAXONOMY,
        weights=OpportunityWeights(
            complementarity=0.9,
            commercial_activity=1.1,
            diversity=0.6,
            direct_competition=1.1,
            saturation=0.8,
            data_quality=0.4,
            transit_access=0.6,
            market_fit=0.5,
            rent_affordability=0.5,
        ),
    ),
    "clinic": BusinessTemplate(
        key="clinic",
        label="Allied health clinic",
        taxonomy=CLINIC_TAXONOMY,
        weights=OpportunityWeights(
            complementarity=1.0,
            commercial_activity=0.8,
            diversity=0.5,
            direct_competition=0.9,
            saturation=0.6,
            data_quality=0.5,
            transit_access=0.4,
            market_fit=0.7,
            rent_affordability=0.3,
        ),
    ),
    "coworking": BusinessTemplate(
        key="coworking",
        label="Coworking space",
        taxonomy=COWORKING_TAXONOMY,
        weights=OpportunityWeights(
            complementarity=0.9,
            commercial_activity=1.0,
            diversity=0.6,
            direct_competition=0.9,
            saturation=0.6,
            data_quality=0.4,
            transit_access=0.6,
            market_fit=0.6,
            rent_affordability=0.4,
        ),
    ),
}


def normalize_keywords(xs: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({x.strip().lower() for x in xs if x and x.strip()}))


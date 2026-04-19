from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from src.config.business_taxonomies import BusinessTaxonomy, GYM_TAXONOMY
from src.features.area_units import AreaUnit, assign_area_unit
from src.features.category_mapping import map_places_to_roles
from src.features.data_quality import derived_data_quality_score
from src.data.load_transport import load_all_transport_points
from src.features.transit_access import TransitAccessConfig, build_transit_access_by_h3
from src.features.market_fit import MarketFitConfig, build_market_fit_by_sa2, load_seifa_sa2, assign_sa2_to_h3_areas
from src.features.suburb_names import SuburbConfig, assign_sal_to_h3_areas
from src.features.rent_affordability import build_rent_affordability_by_sa2


@dataclass(frozen=True)
class FeatureBuildInputs:
    places_sydney_path: Path = Path("data/interim/places_sydney.parquet")
    categories_clean_path: Path = Path("data/interim/categories_clean.parquet")
    taxonomy: BusinessTaxonomy = GYM_TAXONOMY
    template_key: str = "gym"
    area_unit: AreaUnit = AreaUnit(kind="h3", h3_resolution=8)
    include_transit_access: bool = True
    include_market_fit: bool = True
    include_rent_affordability: bool = True
    # ABS SAL labels (Australia only); disable for global / non-AU metros to avoid heavy spatial work.
    include_suburb_labels: bool = True


def build_area_features(inputs: FeatureBuildInputs) -> pl.DataFrame:
    places = pl.read_parquet(str(inputs.places_sydney_path))
    categories = pl.read_parquet(str(inputs.categories_clean_path))

    # Role flags + category diversity per place.
    places_mapped = map_places_to_roles(places, categories, inputs.taxonomy)

    # Area assignment (H3 default).
    places_area = assign_area_unit(places_mapped, inputs.area_unit)

    # Derived POI data quality (transparent; only uses columns that exist).
    # We do not have unresolved_flags/date_refreshed in current schema; this will still work.
    places_area = places_area.with_columns(
        derived_data_quality_score(
            places_area.rename({"lat": "lat", "lon": "lon"})  # no-op; clarity
        )
    )

    # Active = not excluded (and not closed, if date_closed exists).
    active_expr = (~pl.col("is_excluded")).alias("is_active")
    places_area = places_area.with_columns(active_expr)

    # Area aggregates
    agg = places_area.group_by("area_id").agg(
        [
            pl.len().alias("total_poi_count"),
            pl.col("is_active").sum().alias("active_poi_count"),
            (pl.col("is_active") & pl.col("is_competitor")).sum().alias("competitor_count"),
            (pl.col("is_active") & pl.col("is_complementary")).sum().alias("complementary_count"),
            (pl.col("is_active") & pl.col("is_commercial_other")).sum().alias("commercial_other_count"),
            pl.col("unique_category_count").sum().alias("unique_category_count"),
            pl.col("data_quality_score").mean().alias("mean_data_quality_score"),
        ]
    )

    # Features (transparent formulas)
    eps = 1.0
    agg = agg.with_columns(
        [
            (pl.col("competitor_count") / (pl.col("active_poi_count") + eps)).alias("competitor_density_proxy"),
            (pl.col("complementary_count") / (pl.col("active_poi_count") + eps)).alias("complementarity_ratio"),
            (pl.col("active_poi_count")).alias("commercial_activity_proxy"),
        ]
    )
    agg = agg.with_columns(
        (pl.col("competitor_density_proxy") * (1 - pl.col("complementarity_ratio"))).alias("saturation_proxy")
    )

    # recent_refresh_share placeholder (schema-dependent)
    agg = agg.with_columns(pl.lit(None).cast(pl.Float64).alias("recent_refresh_share"))

    # Optional: transit accessibility proxy (H3 only)
    if inputs.include_transit_access and inputs.area_unit.kind == "h3":
        tp = load_all_transport_points()
        if tp.height > 0:
            transit = build_transit_access_by_h3(
                tp, cfg=TransitAccessConfig(h3_resolution=int(inputs.area_unit.h3_resolution or 8))
            ).rename({"h3": "area_id"})
            agg = agg.join(transit, on="area_id", how="left").with_columns(
                [
                    pl.col("transit_access_score").fill_null(0.0),
                    pl.col("station_entrance_count").fill_null(0),
                    pl.col("pt_facility_count").fill_null(0),
                    pl.col("bus_shelter_count").fill_null(0),
                ]
            )
        else:
            agg = agg.with_columns(
                [
                    pl.lit(0.0).alias("transit_access_score"),
                    pl.lit(0).alias("station_entrance_count"),
                    pl.lit(0).alias("pt_facility_count"),
                    pl.lit(0).alias("bus_shelter_count"),
                ]
            )

    # Optional: Market fit (SEIFA by SA2), joined via centroid-in-polygon.
    if inputs.include_market_fit and inputs.area_unit.kind == "h3":
        # Ensure centroids are present to assign SA2.
        if "lat_mean" in places_area.columns and "lon_mean" in places_area.columns:
            pass
        # Our `agg` produced by group-by does not keep centroids; use `places_area` to compute centroids.
        centroids = places_area.group_by("area_id").agg(
            [pl.col("lat").mean().alias("lat_mean"), pl.col("lon").mean().alias("lon_mean")]
        ).rename({"area_id": "h3"})

        areas_h3 = pl.DataFrame({"h3": agg.get_column("area_id")}).join(centroids, on="h3", how="left")
        areas_sa2 = assign_sa2_to_h3_areas(areas_h3, cfg=MarketFitConfig())

        seifa = load_seifa_sa2(MarketFitConfig())
        mf = build_market_fit_by_sa2(seifa, cfg=MarketFitConfig())
        if mf.height > 0:
            joined = areas_sa2.join(mf, on="sa2_code_2021", how="left")
            agg = agg.join(
                joined.select(
                    [
                        pl.col("h3").alias("area_id"),
                        "sa2_code_2021",
                        "sa2_name_2021",
                        "market_fit_score",
                        "seifa_irsad_decile",
                        "sa2_population",
                    ]
                ),
                on="area_id",
                how="left",
            ).with_columns(
                [
                    pl.col("market_fit_score").fill_null(0.0),
                    pl.col("seifa_irsad_decile").fill_null(0),
                ]
            )
        else:
            agg = agg.with_columns(
                [
                    pl.lit(None).cast(pl.Int64).alias("sa2_code_2021"),
                    pl.lit(None).cast(pl.Utf8).alias("sa2_name_2021"),
                    pl.lit(0.0).alias("market_fit_score"),
                    pl.lit(0).alias("seifa_irsad_decile"),
                    pl.lit(None).cast(pl.Int64).alias("sa2_population"),
                ]
            )

    # Optional: suburb/locality name (ABS SAL 2021) for human-readable UI labels.
    if inputs.include_suburb_labels and inputs.area_unit.kind == "h3":
        centroids = places_area.group_by("area_id").agg(
            [pl.col("lat").mean().alias("lat_mean"), pl.col("lon").mean().alias("lon_mean")]
        ).rename({"area_id": "h3"})
        areas_h3 = pl.DataFrame({"h3": agg.get_column("area_id")}).join(centroids, on="h3", how="left")
        sal = assign_sal_to_h3_areas(areas_h3, cfg=SuburbConfig())
        agg = agg.join(
            sal.select([pl.col("h3").alias("area_id"), "sal_name_2021", "sal_code_2021"]),
            on="area_id",
            how="left",
        )

    # Optional: Rent affordability proxy (Census-derived RAID at SA2), joined via centroid-in-polygon (already computed above).
    # This is NOT a median rent series; it's a rental stress proxy.
    if inputs.include_rent_affordability and inputs.area_unit.kind == "h3":
        # Ensure SA2 codes exist (assign_sa2_to_h3_areas is called in the market-fit block, but we can recompute cheaply).
        centroids = places_area.group_by("area_id").agg(
            [pl.col("lat").mean().alias("lat_mean"), pl.col("lon").mean().alias("lon_mean")]
        ).rename({"area_id": "h3"})
        areas_h3 = pl.DataFrame({"h3": agg.get_column("area_id")}).join(centroids, on="h3", how="left")
        areas_sa2 = assign_sa2_to_h3_areas(areas_h3, cfg=MarketFitConfig())

        rent = build_rent_affordability_by_sa2()
        if rent.height > 0:
            joined = areas_sa2.join(rent, on="sa2_code_2021", how="left")
            agg = agg.join(
                joined.select(
                    [
                        pl.col("h3").alias("area_id"),
                        "rent_stress_share",
                        "rent_affordability_score",
                    ]
                ),
                on="area_id",
                how="left",
            )
        else:
            agg = agg.with_columns(
                [
                    pl.lit(None).cast(pl.Float64).alias("rent_stress_share"),
                    pl.lit(None).cast(pl.Float64).alias("rent_affordability_score"),
                ]
            )

    return agg.sort("active_poi_count", descending=True)


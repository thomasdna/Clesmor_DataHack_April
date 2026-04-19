from __future__ import annotations

from pathlib import Path

import polars as pl

from src.config.settings import Settings
from src.data.build_au_subset import build_places_au
from src.data.build_sydney_subset import build_places_sydney
from src.data.profile_schema import write_schema_reports


def _make_places(tmp_path: Path) -> Path:
    df = pl.DataFrame(
        {
            "fsq_place_id": ["a", "b", "c"],
            "name": ["A", "B", "C"],
            "latitude": [-33.9, -37.8, 40.0],
            "longitude": [151.2, 144.9, -70.0],
            "country": ["AU", "AU", "US"],
            "fsq_category_ids": [["x"], ["y"], ["z"]],
        }
    )
    p = tmp_path / "places.parquet"
    df.write_parquet(p)
    return p


def _make_categories(tmp_path: Path) -> Path:
    df = pl.DataFrame(
        {
            "category_id": ["x", "y"],
            "category_level": [2, 2],
            "category_name": ["Gym", "Cafe"],
            "category_label": ["Sports > Gym", "Dining > Cafe"],
            "level1_category_id": ["l1", "l1"],
            "level1_category_name": ["Sports", "Dining"],
            "level2_category_id": ["l2", "l2"],
            "level2_category_name": ["Gym", "Cafe"],
        }
    )
    p = tmp_path / "categories.parquet"
    df.write_parquet(p)
    return p


def test_schema_reports_written(tmp_path: Path) -> None:
    places = _make_places(tmp_path)
    cats = _make_categories(tmp_path)
    docs = tmp_path / "docs"
    write_schema_reports(places, cats, docs_dir=docs)
    assert (docs / "schema_places.json").exists()
    assert (docs / "schema_categories.json").exists()
    assert (docs / "schema_summary.md").exists()


def test_au_and_sydney_filter_pipeline(tmp_path: Path) -> None:
    places = _make_places(tmp_path)
    cats = _make_categories(tmp_path)

    s = Settings(
        places_parquet_path=places,
        categories_parquet_path=cats,
        hf_token=None,
        hf_repo_id="foursquare/fsq-os-places",
        hf_release_dt="2026-04-14",
        data_interim_dir=tmp_path / "interim",
    )

    out_au = build_places_au(s)
    df_au = pl.read_parquet(out_au)
    assert df_au.height == 2

    out_syd = build_places_sydney(s, places_au_path=out_au)
    df_syd = pl.read_parquet(out_syd)
    # only the Sydney-ish row in bbox
    assert df_syd.height == 1


def test_metrics_payload_from_area_row() -> None:
    from src.app.llm_narrative import metrics_payload_from_area_row

    p = metrics_payload_from_area_row(
        {"opportunity_score": 1.5, "competitor_count": 3, "complement_count": 10, "reasons_top3": "a;b"}
    )
    assert p["opportunity_score"] == 1.5
    assert p["competitor_count"] == 3


def test_parse_global_cafe_metros() -> None:
    from src.config.cities import GLOBAL_CAFE_DEMO_KEYS, parse_global_cafe_metros

    assert parse_global_cafe_metros("all") == GLOBAL_CAFE_DEMO_KEYS
    assert parse_global_cafe_metros("singapore, seoul") == ("singapore", "seoul")


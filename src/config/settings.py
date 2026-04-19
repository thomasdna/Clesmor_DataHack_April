from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    # Inputs
    places_parquet_path: Path
    categories_parquet_path: Path

    # Optional Hugging Face source (for downloading raw shards / remote build)
    hf_token: str | None
    hf_repo_id: str
    hf_release_dt: str

    # Filters
    country_code: str = "AU"
    city_name: str = "Sydney"
    allowed_business_type: str = "gyms_fitness"

    # Output dirs
    data_raw_dir: Path = Path("data/raw")
    data_interim_dir: Path = Path("data/interim")
    data_processed_dir: Path = Path("data/processed")
    data_exports_dir: Path = Path("data/exports")

    # Sydney filter strategy tuning (extensible)
    sydney_localities_csv: str = ""  # comma-separated additional localities

    @property
    def sydney_localities(self) -> set[str]:
        vals = [v.strip() for v in self.sydney_localities_csv.split(",") if v.strip()]
        return {v for v in vals}


def load_settings(env_path: Path | None = None) -> Settings:
    if env_path:
        load_dotenv(str(env_path))
    else:
        load_dotenv()

    places_path = Path(os.getenv("PLACES_PARQUET_PATH", "data/au_places.parquet"))
    categories_path = Path(os.getenv("CATEGORIES_PARQUET_PATH", "data/categories.parquet"))

    return Settings(
        places_parquet_path=places_path,
        categories_parquet_path=categories_path,
        hf_token=os.getenv("HF_TOKEN") or None,
        hf_repo_id=os.getenv("HF_REPO_ID", "foursquare/fsq-os-places"),
        hf_release_dt=os.getenv("HF_RELEASE_DT", "2026-04-14"),
        country_code=os.getenv("COUNTRY_CODE", "AU"),
        city_name=os.getenv("CITY_NAME", "Sydney"),
        allowed_business_type=os.getenv("ALLOWED_BUSINESS_TYPE", "gyms_fitness"),
        data_raw_dir=Path(os.getenv("DATA_RAW_DIR", "data/raw")),
        data_interim_dir=Path(os.getenv("DATA_INTERIM_DIR", "data/interim")),
        data_processed_dir=Path(os.getenv("DATA_PROCESSED_DIR", "data/processed")),
        data_exports_dir=Path(os.getenv("DATA_EXPORTS_DIR", "data/exports")),
        sydney_localities_csv=os.getenv("SYDNEY_LOCALITIES", ""),
    )


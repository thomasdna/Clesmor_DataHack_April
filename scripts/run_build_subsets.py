from __future__ import annotations

import sys
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.settings import load_settings
from src.data.build_au_subset import build_places_au
from src.data.build_sydney_subset import build_places_sydney
from src.data.profile_schema import write_schema_reports
from src.data.prepare_categories import prepare_categories


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    settings = load_settings()
    settings.data_interim_dir.mkdir(parents=True, exist_ok=True)
    Path("docs").mkdir(parents=True, exist_ok=True)

    # 1) Schema profile
    if settings.places_parquet_path.exists() and settings.categories_parquet_path.exists():
        write_schema_reports(settings.places_parquet_path, settings.categories_parquet_path, docs_dir=Path("docs"))

    # 2) Prepare categories
    categories_clean = prepare_categories(settings)

    # 3) Build AU subset (if input is already AU, this is still safe)
    places_au = build_places_au(settings)

    # 4) Build Sydney subset
    places_syd = build_places_sydney(settings, places_au_path=places_au)

    # 5) Write report
    report = Path("docs/subset_build_report.md")
    report.write_text(
        f"""# Subset Build Report

## Outputs
- **categories_clean**: `{categories_clean}`
- **places_au**: `{places_au}`
- **places_sydney**: `{places_syd}`

## Notes
- Sydney filtering uses a locality/region strategy if those fields exist; otherwise it falls back to a Sydney bounding box.
"""
    )

    logging.info("Wrote %s", report)


if __name__ == "__main__":
    main()


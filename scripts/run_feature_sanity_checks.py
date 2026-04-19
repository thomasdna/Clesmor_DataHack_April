from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import matplotlib.pyplot as plt
import polars as pl


def _bar_plot(df: pl.DataFrame, x: str, y: str, title: str, out: Path) -> None:
    pdf = df.to_pandas()
    plt.figure(figsize=(10, 4))
    plt.bar(pdf[x].astype(str), pdf[y])
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out)
    plt.close()


def main() -> None:
    ranked_path = Path("data/processed/sydney_ranked_areas.parquet")
    if not ranked_path.exists():
        raise SystemExit("Missing ranked areas. Run: python scripts/run_build_features.py")

    df = pl.read_parquet(ranked_path)

    top20 = df.select(["area_id", "opportunity_score", "competitor_count", "complementary_count", "active_poi_count"]).head(20)
    sat20 = df.sort("saturation_proxy", descending=True).select(["area_id", "saturation_proxy", "competitor_count", "active_poi_count"]).head(20)
    comp20 = df.sort("complementarity_ratio", descending=True).select(["area_id", "complementarity_ratio", "complementary_count", "active_poi_count"]).head(20)

    print("\nTop 20 by opportunity_score")
    print(top20)
    print("\nTop 20 most saturated (proxy)")
    print(sat20)
    print("\nTop 20 highest complementarity_ratio")
    print(comp20)

    export_dir = Path("data/exports")
    export_dir.mkdir(parents=True, exist_ok=True)

    _bar_plot(top20, "area_id", "opportunity_score", "Top 20 opportunity score", export_dir / "top20_opportunity.png")
    _bar_plot(sat20, "area_id", "saturation_proxy", "Top 20 saturation proxy", export_dir / "top20_saturation.png")
    _bar_plot(comp20, "area_id", "complementarity_ratio", "Top 20 complementarity ratio", export_dir / "top20_complementarity.png")

    print("\nSaved plots to data/exports/:")
    print(" - top20_opportunity.png")
    print(" - top20_saturation.png")
    print(" - top20_complementarity.png")


if __name__ == "__main__":
    main()


from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.geo import SYDNEY_BBOX
from src.features.market_fit import MarketFitConfig, build_market_fit_by_sa2, load_seifa_sa2
from src.features.rent_affordability import build_rent_affordability_by_sa2
from src.features.renter_rate import load_abs_g37_renter_rate_sa2
from src.config.cities import CITIES

try:
    import shapefile  # pyshp
except Exception as e:  # pragma: no cover
    raise SystemExit("Missing dependency pyshp. Install: pip install pyshp") from e


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def build_sa2_market_fit_geojson(
    cfg: MarketFitConfig = MarketFitConfig(),
    bbox=SYDNEY_BBOX,
) -> dict:
    """
    Build a SA2 GeoJSON FeatureCollection filtered to Sydney bbox, with market-fit properties joined.
    Uses bbox filtering for speed + demo stability.
    """
    seifa = load_seifa_sa2(cfg)
    mf = build_market_fit_by_sa2(seifa, cfg=cfg)
    mf_map = {int(r["sa2_code_2021"]): r for r in mf.to_dicts()} if mf.height else {}

    rent = build_rent_affordability_by_sa2()
    rent_map = {int(r["sa2_code_2021"]): r for r in rent.to_dicts()} if rent.height else {}

    rr = load_abs_g37_renter_rate_sa2()
    rr_map = {int(r["sa2_code_2021"]): r for r in rr.to_dicts()} if rr.height else {}

    shp_path = cfg.sa2_shp_path
    if not shp_path.exists():
        raise FileNotFoundError(f"Missing SA2 shapefile: {shp_path}")

    sf = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
    idx_code = fields.index("SA2_CODE21") if "SA2_CODE21" in fields else None
    idx_name = fields.index("SA2_NAME21") if "SA2_NAME21" in fields else None
    if idx_code is None or idx_name is None:
        raise RuntimeError("Could not find SA2_CODE21/SA2_NAME21 in shapefile attributes.")

    min_lon, min_lat, max_lon, max_lat = bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat

    feats = []
    for sr in sf.iterShapeRecords():
        shp = sr.shape
        if shp is None or getattr(shp, "shapeType", None) == 0:
            continue
        bb = shp.bbox  # [minx, miny, maxx, maxy]
        if bb[2] < min_lon or bb[0] > max_lon or bb[3] < min_lat or bb[1] > max_lat:
            continue

        rec = sr.record
        try:
            code = int(rec[idx_code])
        except Exception:
            continue
        name = str(rec[idx_name])

        props = {
            "sa2_code_2021": code,
            "sa2_name_2021": name,
        }

        joined = mf_map.get(code)
        if joined:
            props.update(
                {
                    "market_fit_score": float(joined.get("market_fit_score") or 0.0),
                    "seifa_irsad_decile": int(joined.get("seifa_irsad_decile") or 0),
                    "sa2_population": int(joined.get("sa2_population") or 0),
                    "seifa_irsad_score": float(joined.get("seifa_irsad_score") or 0.0),
                    "seifa_ier_score": float(joined.get("seifa_ier_score") or 0.0),
                }
            )
        else:
            # Some SA2 polygons (airports/industrial/parks) can be missing SEIFA rows.
            # Use nulls so UI can treat as N/A (instead of incorrectly ranking them as "bottom").
            props.update(
                {
                    "market_fit_score": None,
                    "seifa_irsad_decile": None,
                    "sa2_population": None,
                    "seifa_irsad_score": None,
                    "seifa_ier_score": None,
                }
            )

        rj = rent_map.get(code)
        if rj:
            props.update(
                {
                    "rent_stress_share": float(rj.get("rent_stress_share") or 0.0),
                    "rent_affordability_score": (
                        None
                        if rj.get("rent_affordability_score") is None
                        else float(rj.get("rent_affordability_score") or 0.0)
                    ),
                }
            )
        else:
            props.update(
                {
                    "rent_stress_share": None,
                    "rent_affordability_score": None,
                }
            )

        rrate = rr_map.get(code)
        if rrate:
            props.update(
                {
                    "renter_share_2021": float(rrate.get("renter_share_2021") or 0.0),
                    "renter_households_2021": int(rrate.get("renter_households_2021") or 0),
                    "total_households_2021": int(rrate.get("total_households_2021") or 0),
                }
            )
        else:
            props.update(
                {
                    "renter_share_2021": None,
                    "renter_households_2021": None,
                    "total_households_2021": None,
                }
            )

        feats.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": shp.__geo_interface__,
            }
        )

    return {"type": "FeatureCollection", "features": feats}


def main(city_key: str = "sydney") -> None:
    city = CITIES.get(city_key, CITIES["sydney"])
    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir = Path("docs")
    docs_dir.mkdir(parents=True, exist_ok=True)

    fc = build_sa2_market_fit_geojson(bbox=city.bbox)
    out_geo = out_dir / f"{city.key}_sa2_market_fit.geojson"
    out_geo.write_text(json.dumps(fc), encoding="utf-8")

    # Also write a simple preview table for docs.
    rows = []
    for f in fc["features"]:
        p = f["properties"]
        if p.get("market_fit_score") is None:
            continue
        rows.append(
            {
                "sa2_name_2021": p.get("sa2_name_2021"),
                "market_fit_score": float(p.get("market_fit_score") or 0.0),
                "seifa_irsad_decile": int(p.get("seifa_irsad_decile") or 0),
                "sa2_population": int(p.get("sa2_population") or 0),
                "rent_affordability_score": p.get("rent_affordability_score"),
                "rent_stress_share": p.get("rent_stress_share"),
                "renter_share_2021": p.get("renter_share_2021"),
            }
        )
    df = pl.DataFrame(rows).sort("market_fit_score", descending=True)
    (docs_dir / "market_fit_preview.md").write_text(
        "# Market fit preview (SA2)\n\n"
        "Top 15 SA2s by market fit score:\n\n"
        + df.head(15).to_pandas().to_markdown(index=False)
        + "\n",
        encoding="utf-8",
    )

    print("Wrote:")
    print(" -", out_geo)
    print(" - docs/market_fit_preview.md")


if __name__ == "__main__":
    main()


"""
Download NSW open transport layers used by Expansion Copilot (train entrances, facilities, bus shelters).

These match the **Transport** section in `src/app/methodology.py` and the Transit tab in `app/streamlit_app.py`.
Opal fares / peak loads / tap data are intentionally out of scope for the MVP.
"""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DownloadSpec:
    name: str
    url: str
    out_path: Path


SPECS: list[DownloadSpec] = [
    DownloadSpec(
        name="Train station entrances (CSV)",
        url="https://opendata.transport.nsw.gov.au/data/dataset/3e875da4-5090-499f-9a1e-a566b6ef3559/resource/e9d0180b-d758-40d5-855a-955d1f0b733a/download/stationentrances2018.csv",
        out_path=Path("data/raw/nsw_transport/station_entrances_2018.csv"),
    ),
    DownloadSpec(
        name="Public transport location facilities (CSV)",
        url="https://opendata.transport.nsw.gov.au/data/dataset/25f006fd-d0fb-4a8e-bfda-7ea4033c1aeb/resource/e9d94351-f22d-46ea-b64d-10e7e238368a/download/locationfacilitydata.csv",
        out_path=Path("data/raw/nsw_transport/location_facilities.csv"),
    ),
    DownloadSpec(
        name="City of Sydney bus shelters (GeoJSON)",
        url="https://opendata.arcgis.com/datasets/3f216ec15ad0498091b335afee5b6537_0.geojson",
        out_path=Path("data/raw/nsw_transport/city_sydney_bus_shelters.geojson"),
    ),
]


def _download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as resp:
        return resp.read()


def main() -> None:
    out_dir = Path("data/raw/nsw_transport")
    out_dir.mkdir(parents=True, exist_ok=True)

    for spec in SPECS:
        spec.out_path.parent.mkdir(parents=True, exist_ok=True)
        if spec.out_path.exists():
            print(f"Skip (exists): {spec.out_path}")
            continue
        print(f"Downloading: {spec.name}")
        b = _download(spec.url)
        spec.out_path.write_bytes(b)
        print(f"Wrote: {spec.out_path} ({len(b):,} bytes)")

    # Basic sanity check for GeoJSON validity (optional).
    geo = Path("data/raw/nsw_transport/city_sydney_bus_shelters.geojson")
    if geo.exists():
        try:
            json.loads(geo.read_text(encoding="utf-8"))
        except Exception as e:
            raise SystemExit(f"GeoJSON parse failed: {geo}: {e}")


if __name__ == "__main__":
    main()


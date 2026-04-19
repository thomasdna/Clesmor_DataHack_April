from __future__ import annotations

from dataclasses import dataclass

from src.config.geo import BBox, SYDNEY_BBOX

# Defined here too so imports work even if an older `geo.py` omits `MELBOURNE_BBOX`.
MELBOURNE_BBOX = BBox(min_lat=-38.55, max_lat=-37.45, min_lon=144.35, max_lon=145.65)


@dataclass(frozen=True)
class CityProfile:
    key: str
    label: str
    bbox: BBox
    map_center_lat: float
    map_center_lon: float
    map_zoom: float


CITIES: dict[str, CityProfile] = {
    "sydney": CityProfile(
        key="sydney",
        label="Sydney",
        bbox=SYDNEY_BBOX,
        map_center_lat=-33.8688,
        map_center_lon=151.2093,
        map_zoom=10.5,
    ),
    "melbourne": CityProfile(
        key="melbourne",
        label="Melbourne (Greater)",
        bbox=MELBOURNE_BBOX,
        map_center_lat=-37.8136,
        map_center_lon=144.9631,
        map_zoom=10.2,
    ),
}

# Approximate metro bboxes for multi-city POI demos (Foursquare bbox filter).
# Not political boundaries — “greater metro” rectangles for subset builds.
GLOBAL_CAFE_METROS: dict[str, CityProfile] = {
    "singapore": CityProfile(
        key="singapore",
        label="Singapore (metro)",
        bbox=BBox(min_lat=1.15, max_lat=1.48, min_lon=103.55, max_lon=104.12),
        map_center_lat=1.3521,
        map_center_lon=103.8198,
        map_zoom=11.0,
    ),
    "saigon": CityProfile(
        key="saigon",
        label="Ho Chi Minh City (metro)",
        bbox=BBox(min_lat=10.65, max_lat=10.95, min_lon=106.55, max_lon=106.95),
        map_center_lat=10.7769,
        map_center_lon=106.7009,
        map_zoom=10.8,
    ),
    "san_francisco": CityProfile(
        key="san_francisco",
        label="San Francisco (city/county)",
        bbox=BBox(min_lat=37.70, max_lat=37.84, min_lon=-122.52, max_lon=-122.35),
        map_center_lat=37.7749,
        map_center_lon=-122.4194,
        map_zoom=11.5,
    ),
    "silicon_valley": CityProfile(
        key="silicon_valley",
        label="Silicon Valley (Santa Clara Valley)",
        bbox=BBox(min_lat=37.22, max_lat=37.50, min_lon=-122.15, max_lon=-121.82),
        map_center_lat=37.37,
        map_center_lon=-121.98,
        map_zoom=9.8,
    ),
    "seoul": CityProfile(
        key="seoul",
        label="Seoul (metro)",
        bbox=BBox(min_lat=37.40, max_lat=37.70, min_lon=126.85, max_lon=127.20),
        map_center_lat=37.5665,
        map_center_lon=126.9780,
        map_zoom=10.5,
    ),
}

# Union for scripts that build `places_<key>.parquet` (subset from country-level interim files).
BUILD_CITY_PROFILES: dict[str, CityProfile] = {**CITIES, **GLOBAL_CAFE_METROS}

# Six-way “global café” comparison (POI-only scoring; see `scripts/run_global_cafe_comparison.py`).
GLOBAL_CAFE_DEMO_KEYS: tuple[str, ...] = (
    "sydney",
    "singapore",
    "saigon",
    "san_francisco",
    "silicon_valley",
    "seoul",
)

# ISO 3166-1 alpha-2 for Foursquare `country` filter (remote fetch).
GLOBAL_CAFE_METRO_COUNTRY: dict[str, str] = {
    "sydney": "AU",
    "singapore": "SG",
    "saigon": "VN",
    "san_francisco": "US",
    "silicon_valley": "US",
    "seoul": "KR",
}


def parse_global_cafe_metros(spec: str) -> tuple[str, ...]:
    """
    Parse ``--metros``: comma-separated keys, or ``all`` for every global-café metro.
    Raises ``ValueError`` on unknown keys.
    """
    s = (spec or "").strip().lower()
    if not s or s == "all":
        return GLOBAL_CAFE_DEMO_KEYS
    keys = tuple(k.strip() for k in spec.split(",") if k.strip())
    valid = set(GLOBAL_CAFE_METRO_COUNTRY.keys())
    bad = [k for k in keys if k not in valid]
    if bad:
        raise ValueError(
            f"Unknown metro key(s): {bad}. Valid: {', '.join(GLOBAL_CAFE_DEMO_KEYS)}"
        )
    return keys

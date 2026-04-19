from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BBox:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


# Locked demo market.
SYDNEY_BBOX = BBox(min_lat=-34.20, max_lat=-33.55, min_lon=150.85, max_lon=151.45)

# Greater Melbourne (approx bbox for fast filtering; refined polygons can be added later).
MELBOURNE_BBOX = BBox(min_lat=-38.55, max_lat=-37.45, min_lon=144.35, max_lon=145.65)


#!/usr/bin/env python3
"""
One entrypoint for the global café demo:

1. **Sydney (no network)** — If ``places_sydney.parquet`` is missing but
   ``places_au.parquet`` exists, build it via ``build_city_subset.py`` (bbox).
2. **Other metros** — Hugging Face remote fetch (requires ``HF_TOKEN``),
   unless ``--no-fetch``.
3. **Compare** — Run ``run_global_cafe_comparison.py`` (unless ``--no-compare``).

Use ``--metros singapore`` (etc.) to fetch or score **one metro at a time**; use ``all`` for six.
``--dry-run`` prints the plan without network, builds, or scoring.

Categories should exist at ``data/interim/categories_clean.parquet``.

Examples::

  python scripts/bootstrap_global_cafe_demo.py --dt 2026-04-14 --dry-run
  python scripts/bootstrap_global_cafe_demo.py --dt 2026-04-14 --metros singapore
  python scripts/bootstrap_global_cafe_demo.py --dt 2026-04-14 --metros saigon,singapore
  python scripts/bootstrap_global_cafe_demo.py --no-fetch --metros all
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.cities import (
    BUILD_CITY_PROFILES,
    GLOBAL_CAFE_DEMO_KEYS,
    GLOBAL_CAFE_METRO_COUNTRY,
    parse_global_cafe_metros,
)
from src.data.hf_places_remote import fetch_metro_subset_from_hf, load_hf_token


def _build_sydney_from_au(interim: Path) -> bool:
    au = interim / "places_au.parquet"
    out = interim / "places_sydney.parquet"
    if not au.exists() or out.exists():
        return False
    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_city_subset.py"),
            "--city",
            "sydney",
            "--in",
            str(au),
            "--out",
            str(out),
        ],
        cwd=str(REPO_ROOT),
        check=True,
    )
    print(f"Built {out} from AU subset (bbox filter).")
    return True


def _dry_run(
    interim: Path,
    selected: tuple[str, ...],
    *,
    dt: str | None,
    no_fetch: bool,
    no_compare: bool,
    force: bool,
    max_rows: int,
) -> None:
    print("=== DRY RUN (no changes) ===\n")
    print(f"interim dir: {interim}")
    print(f"metros: {', '.join(selected)}\n")

    if "sydney" in selected:
        au = interim / "places_au.parquet"
        out = interim / "places_sydney.parquet"
        if out.exists() and not force:
            print(f"[sydney] skip — exists: {out}")
        elif au.exists():
            print(f"[sydney] would run: build_city_subset.py --city sydney --in {au} --out {out}")
        else:
            print(f"[sydney] would HF-fetch AU+Sydney bbox (needs places_au missing; use --dt) → {out}")
            if not dt:
                print("         (set --dt for the release folder when running for real)")

    if not no_fetch:
        if not dt:
            print("\nNote: --dt not set; HF steps below assume you pass e.g. --dt 2026-04-14 when running.\n")
        for key in selected:
            if key == "sydney":
                continue
            out_path = interim / f"places_{key}.parquet"
            if out_path.exists() and not force:
                print(f"[{key}] skip — exists: {out_path}")
                continue
            cc = GLOBAL_CAFE_METRO_COUNTRY[key]
            bbox = BUILD_CITY_PROFILES[key].bbox
            mr = f" max_rows={max_rows}" if max_rows else ""
            print(
                f"[{key}] would HF-fetch country={cc} bbox=({bbox.min_lat}, {bbox.max_lat}, "
                f"{bbox.min_lon}, {bbox.max_lon}) → {out_path}{mr}"
            )

    if not no_compare:
        m = ",".join(selected) if len(selected) < len(GLOBAL_CAFE_DEMO_KEYS) else "all"
        print(
            f"\nWould run: python scripts/run_global_cafe_comparison.py "
            f"--interim-dir {interim} --metros {m}"
        )
    else:
        print("\nWould skip comparison (--no-compare).")

    print("\n=== end dry run ===")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dt",
        default=None,
        help="Foursquare OS release dt= folder (required for HF fetch unless --no-fetch or --dry-run)",
    )
    ap.add_argument("--interim-dir", type=Path, default=Path("data/interim"))
    ap.add_argument("--no-fetch", action="store_true", help="Skip Hugging Face downloads (still builds Sydney from AU if possible)")
    ap.add_argument("--no-compare", action="store_true", help="Only prepare parquets; skip scoring CSV")
    ap.add_argument("--force", action="store_true", help="Re-fetch even if parquet exists")
    ap.add_argument("--max-rows", type=int, default=0, help="Cap rows per metro (HF fetch only; 0=all)")
    ap.add_argument(
        "--metros",
        default="all",
        help="Comma-separated metro keys (singapore, saigon, …) or 'all' (default: all six).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned steps only; no HF, no builds, no scoring.",
    )
    args = ap.parse_args()

    try:
        selected = parse_global_cafe_metros(args.metros)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    interim = args.interim_dir
    interim.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        _dry_run(
            interim,
            selected,
            dt=args.dt,
            no_fetch=args.no_fetch,
            no_compare=args.no_compare,
            force=args.force,
            max_rows=args.max_rows,
        )
        return

    if not args.no_fetch and not args.dt:
        raise SystemExit("--dt is required for Hugging Face fetch (e.g. 2026-04-14), or use --no-fetch / --dry-run")

    cats = interim / "categories_clean.parquet"
    if not cats.exists():
        print(
            f"Warning: missing {cats} — global comparison may fail. "
            "Generate with prepare_categories / run_build_subsets.",
            file=sys.stderr,
        )

    if "sydney" in selected:
        _build_sydney_from_au(interim)

    if not args.no_fetch:
        token = load_hf_token()
        for key in selected:
            out_path = interim / f"places_{key}.parquet"
            if out_path.exists() and not args.force:
                continue
            if key == "sydney" and (interim / "places_au.parquet").exists():
                _build_sydney_from_au(interim)
                continue
            country = GLOBAL_CAFE_METRO_COUNTRY[key]
            bbox = BUILD_CITY_PROFILES[key].bbox
            print(f"HF fetch: {key} ({country}) …")
            n = fetch_metro_subset_from_hf(
                args.dt,
                token,
                country,
                bbox,
                out_path,
                max_rows=args.max_rows,
            )
            print(f"  Wrote {out_path} rows={n:,}")

    if not args.no_compare:
        subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "run_global_cafe_comparison.py"),
                "--interim-dir",
                str(interim),
                "--metros",
                args.metros.strip() or "all",
            ],
            cwd=str(REPO_ROOT),
            check=True,
        )


if __name__ == "__main__":
    main()

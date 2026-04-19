from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class SchemaReport:
    path: str
    n_cols: int
    columns: list[str]
    dtypes: dict[str, str]
    field_presence: dict[str, bool]


FIELD_CHECKS_PLACES = [
    "latitude",
    "longitude",
    "country",
    "locality",
    "region",
    "postcode",
    "date_refreshed",
    "date_closed",
    "fsq_category_ids",
    "fsq_category_labels",
    "unresolved_flags",
    "geom",
    "bbox",
    "confidence",
    "confidence_score",
]

FIELD_CHECKS_CATEGORIES = [
    "category_id",
    "category_name",
    "category_label",
    "level1_category_id",
    "level1_category_name",
    "level2_category_id",
    "level2_category_name",
]


def _schema_report_for_parquet(parquet_path: Path, fields_to_check: list[str]) -> SchemaReport:
    lf = pl.scan_parquet(str(parquet_path))
    schema = lf.collect_schema()
    cols = schema.names()
    dtypes = {c: str(schema.get(c)) for c in cols}
    present = {f: (f in set(cols)) for f in fields_to_check}
    return SchemaReport(
        path=str(parquet_path),
        n_cols=len(cols),
        columns=cols,
        dtypes=dtypes,
        field_presence=present,
    )


def write_schema_reports(
    places_parquet: Path,
    categories_parquet: Path,
    docs_dir: Path = Path("docs"),
) -> None:
    docs_dir.mkdir(parents=True, exist_ok=True)

    places_report = _schema_report_for_parquet(places_parquet, FIELD_CHECKS_PLACES)
    cats_report = _schema_report_for_parquet(categories_parquet, FIELD_CHECKS_CATEGORIES)

    (docs_dir / "schema_places.json").write_text(json.dumps(asdict(places_report), indent=2))
    (docs_dir / "schema_categories.json").write_text(json.dumps(asdict(cats_report), indent=2))

    # Markdown summary (human readable)
    def fmt_presence(p: dict[str, bool]) -> str:
        lines = []
        for k, v in p.items():
            lines.append(f"- **{k}**: {'✅ present' if v else '❌ missing'}")
        return "\n".join(lines)

    md = f"""# Schema Summary

## Places
- **path**: `{places_report.path}`
- **columns**: {places_report.n_cols}

### Field presence (explicit checks)
{fmt_presence(places_report.field_presence)}

### Columns
{', '.join([f'`{c}`' for c in places_report.columns])}

## Categories
- **path**: `{cats_report.path}`
- **columns**: {cats_report.n_cols}

### Field presence (explicit checks)
{fmt_presence(cats_report.field_presence)}

### Columns
{', '.join([f'`{c}`' for c in cats_report.columns])}
"""
    (docs_dir / "schema_summary.md").write_text(md)


def main() -> None:
    # CLI-friendly entrypoint: uses default local paths.
    write_schema_reports(
        places_parquet=Path("data/au_places.parquet"),
        categories_parquet=Path("data/categories.parquet"),
        docs_dir=Path("docs"),
    )


if __name__ == "__main__":
    main()


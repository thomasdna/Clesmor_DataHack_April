from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer


def _md_to_paragraphs(md: str) -> list[str]:
    """
    Tiny, safe Markdown-ish to Paragraph chunks.
    We intentionally avoid complex Markdown conversion to keep the demo build robust.
    - Headings become bold lines
    - Bullets preserved as text
    - Inline math kept as plain text
    """
    lines = md.splitlines()
    out: list[str] = []
    buf: list[str] = []

    def flush() -> None:
        nonlocal buf
        if buf:
            out.append("<br/>".join([_escape(l) for l in buf]).strip())
            buf = []

    for line in lines:
        if line.strip() == "":
            flush()
            continue
        if line.startswith("#"):
            flush()
            level = len(line) - len(line.lstrip("#"))
            text = line.lstrip("#").strip()
            out.append(f"<b>{_escape(text)}</b>")
            if level <= 2:
                out.append("")  # small spacing paragraph
            continue
        buf.append(line)
    flush()
    return out


def _escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def build_pdf(md_path: Path, pdf_path: Path) -> None:
    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "Body",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10.2,
        leading=13.2,
        spaceAfter=6,
    )
    heading = ParagraphStyle(
        "Heading",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=16,
        spaceAfter=10,
        spaceBefore=10,
    )

    md = md_path.read_text(encoding="utf-8")
    chunks = _md_to_paragraphs(md)

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=2.0 * cm,
        rightMargin=2.0 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
        title="Expansion Copilot — Demo Build Report",
        author="Expansion Copilot",
    )

    story = []
    first = True
    for c in chunks:
        if c == "":
            story.append(Spacer(1, 6))
            continue
        # Heuristic: bold-only line treated as heading.
        if c.startswith("<b>") and c.endswith("</b>") and len(c) < 120:
            story.append(Paragraph(c, heading))
            continue
        if first:
            first = False
        story.append(Paragraph(c, body))
    doc.build(story)


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--md", default="docs/expansion_copilot_demo_report.md")
    ap.add_argument("--pdf", default="docs/expansion_copilot_demo_report.pdf")
    args = ap.parse_args()

    md_path = repo / args.md
    pdf_path = repo / args.pdf
    if not md_path.exists():
        raise SystemExit(f"Missing markdown source: {md_path}")
    build_pdf(md_path=md_path, pdf_path=pdf_path)
    print(f"Wrote {pdf_path}")


if __name__ == "__main__":
    main()


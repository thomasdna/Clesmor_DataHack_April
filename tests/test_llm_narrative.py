from __future__ import annotations

from src.app.llm_narrative import generate_deterministic_area_narrative


def test_deterministic_narrative_contains_key_metrics() -> None:
    text = generate_deterministic_area_narrative(
        {
            "opportunity_score": 1.5,
            "competitor_count": 3,
            "complement_count": 20,
            "reasons_top3": "Strong complements",
        },
        city_label="Sydney",
        business_label="Café",
        area_id="h3cell123",
    )
    assert "1.500" in text or "1.5" in text
    assert "Sydney" in text
    assert "Café" in text
    assert "Validate on site" in text

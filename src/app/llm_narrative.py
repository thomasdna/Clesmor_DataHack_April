"""
Optional OpenAI narrative layer: interprets the same deterministic area metrics as plain text.

Requires OPENAI_API_KEY. Does not replace scoring—only summarizes provided numbers.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv_from_repo() -> None:
    """Ensure `.env` next to the repo root is loaded (Streamlit cwd may differ)."""
    try:
        from dotenv import load_dotenv

        load_dotenv(_REPO_ROOT / ".env")
    except Exception:
        pass


def reload_openai_env() -> str | None:
    """
    Reload `.env` and return the trimmed API key if set.
    Call this in Streamlit before checking ``os.getenv`` so the key is visible after edits.
    """
    _load_dotenv_from_repo()
    k = (os.getenv("OPENAI_API_KEY") or "").strip()
    return k if k else None


def metrics_payload_from_area_row(row: dict[str, Any]) -> dict[str, Any]:
    """Subset of area row suitable for JSON + LLM (no raw POI lists)."""
    keys = [
        "opportunity_score",
        "competitor_count",
        "complement_count",
        "complementary_count",
        "active_poi_count",
        "commercial_activity_proxy",
        "saturation_proxy",
        "mean_data_quality_score",
        "data_quality_score",
        "transit_access_score",
        "market_fit_score",
        "rent_affordability_score",
        "rent_stress_share",
        "reasons_top3",
    ]
    out: dict[str, Any] = {}
    for k in keys:
        if k not in row:
            continue
        v = row[k]
        if hasattr(v, "item"):
            try:
                v = v.item()
            except Exception:
                v = float(v) if v is not None else None
        if v is not None:
            if k in ("opportunity_score", "mean_data_quality_score", "data_quality_score", "transit_access_score", "market_fit_score", "rent_affordability_score", "rent_stress_share", "saturation_proxy", "commercial_activity_proxy"):
                try:
                    out[k] = round(float(v), 6)
                except (TypeError, ValueError):
                    out[k] = str(v)
            elif k in ("competitor_count", "complement_count", "complementary_count", "active_poi_count"):
                try:
                    out[k] = int(v)
                except (TypeError, ValueError):
                    out[k] = str(v)
            else:
                out[k] = v
    if "complement_count" not in out and "complementary_count" in row:
        v = row["complementary_count"]
        try:
            out["complement_count"] = int(v if not hasattr(v, "item") else v.item())
        except Exception:
            pass
    return out


def generate_deterministic_area_narrative(
    metrics: dict[str, Any],
    *,
    city_label: str,
    business_label: str,
    area_id: str,
) -> str:
    """
    Rule-based narrative from the same metrics the LLM would see—no network.
    Used when OpenAI is unavailable so the UI stays usable.
    """
    parts: list[str] = []

    os_ = metrics.get("opportunity_score")
    comp = metrics.get("competitor_count")
    compl = metrics.get("complement_count")
    act = metrics.get("active_poi_count")
    dq = metrics.get("data_quality_score", metrics.get("mean_data_quality_score"))
    tr = metrics.get("transit_access_score")
    mf = metrics.get("market_fit_score")
    ra = metrics.get("rent_affordability_score")
    rs = metrics.get("rent_stress_share")
    reasons = metrics.get("reasons_top3")

    aid = str(area_id)
    aid_short = f"{aid[:18]}…" if len(aid) > 20 else aid
    parts.append(
        f"This **{business_label}** shortlist cell in **{city_label}** (area `{aid_short}`) "
        f"scores **{os_:.3f}** on the opportunity index, using POI-based proxies only—not revenue or foot traffic."
        if isinstance(os_, (int, float))
        else f"Area `{aid_short}` in **{city_label}** for **{business_label}** (scores from POI data only)."
    )

    line2 = []
    if comp is not None:
        line2.append(f"**{comp}** competitor-style POIs")
    if compl is not None:
        line2.append(f"**{compl}** complement POIs")
    if act is not None:
        line2.append(f"**{act}** active POIs (activity proxy)")
    if line2:
        parts.append("Surrounding mix: " + ", ".join(line2) + ".")

    extras = []
    if isinstance(dq, (int, float)):
        extras.append(f"data quality ≈ **{dq:.2f}**")
    if isinstance(tr, (int, float)):
        extras.append(f"transit access proxy ≈ **{tr:.2f}**")
    if isinstance(mf, (int, float)):
        extras.append(f"market fit proxy ≈ **{mf:.2f}**")
    if isinstance(ra, (int, float)):
        t = f"rent affordability proxy ≈ **{ra:.2f}**"
        if isinstance(rs, (int, float)):
            t += f" (rental stress share ≈ **{rs:.2f}**)"
        extras.append(t)
    if extras:
        parts.append("Context layers: " + "; ".join(extras) + ".")

    if reasons:
        parts.append(f"**Top score drivers (auto):** {reasons}")

    parts.append(
        "**Validate on site:** (1) observe weekday/weekend traffic near candidate streets; "
        "(2) compare your offer vs. nearby competitors; "
        "(3) confirm lease, access, and planning constraints."
    )

    return "\n\n".join(parts)


def generate_openai_area_narrative(
    *,
    metrics: dict[str, Any],
    city_label: str,
    business_label: str,
    area_id: str,
) -> str:
    """Call OpenAI Chat Completions with a strict analyst prompt. Raises on API/key errors."""
    _load_dotenv_from_repo()
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            f"OPENAI_API_KEY is not set. Add it to `{_REPO_ROOT / '.env'}` or export it before starting Streamlit."
        )

    model = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

    system = (
        "You are an analyst for retail and service-site expansion. You receive ONLY structured metrics "
        "for one map area (H3 cell). Rules: (1) Do not invent addresses, brands, foot traffic, or revenue. "
        "(2) Tie every claim to the numbers given. (3) Mention limitations: POI-based proxies, not demand forecasting. "
        "(4) Write 2–4 short paragraphs, plain language, professional tone. "
        "(5) End with one bullet list of 3 on-site validation steps."
    )
    user_obj = {
        "area_id": area_id,
        "city": city_label,
        "business_vertical": business_label,
        "metrics": metrics,
    }
    user = (
        "Explain why this area might or might not be a reasonable shortlist candidate for opening "
        f"a **{business_label}** in **{city_label}**, based solely on these metrics:\n\n"
        f"```json\n{json.dumps(user_obj, indent=2, default=str)}\n```"
    )

    from openai import APIConnectionError, APIStatusError, AuthenticationError, OpenAI, RateLimitError

    timeout_s = float(os.getenv("OPENAI_TIMEOUT", "120"))
    kwargs: dict[str, Any] = {"api_key": api_key, "timeout": timeout_s}
    base = (os.getenv("OPENAI_BASE_URL") or "").strip()
    if base:
        kwargs["base_url"] = base

    client = OpenAI(**kwargs)
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=600,
            temperature=0.35,
        )
    except AuthenticationError as e:
        raise RuntimeError(
            "OpenAI rejected this API key (401). Create a new key at "
            "https://platform.openai.com/api-keys and update OPENAI_API_KEY in `.env`, then restart Streamlit."
        ) from e
    except RateLimitError as e:
        raise RuntimeError(
            "OpenAI returned 429 (rate limit or insufficient quota). Your key works—the request reached OpenAI. "
            "Add billing or credits at https://platform.openai.com/account/billing (free tier may be exhausted). "
            f"Details: {e}"
        ) from e
    except APIConnectionError as e:
        raise RuntimeError(
            "Cannot reach OpenAI’s servers (network/TLS). This is usually not a bad key—it means the request "
            "never got a reply. Try: (1) confirm you can open https://platform.openai.com in a browser; "
            "(2) disable VPN or try another network; (3) check firewall/proxy blocking `api.openai.com`; "
            "(4) optional: set OPENAI_BASE_URL if your org uses a gateway. "
            f"Underlying error: {e}"
        ) from e
    except APIStatusError as e:
        raise RuntimeError(f"OpenAI API returned an error: {e}") from e
    except Exception as e:
        # Some httpx transport errors surface as plain Exception with message "Connection error."
        mod = getattr(type(e), "__module__", "") or ""
        name = type(e).__name__
        msg = (str(e) or "").strip().lower()
        if "httpx" in mod or "http" in mod.lower() or "connection" in msg or "connect" in name.lower():
            raise RuntimeError(
                "Your Mac could not complete HTTPS to api.openai.com (short message: "
                f"«{str(e)}»). This is a network/TLS path issue on this machine—not billing. "
                "Try: different Wi‑Fi or hotspot, disable VPN, check Little Snitch / firewall / corporate proxy. "
                "Confirm with: curl -sS -o /dev/null -w '%{http_code}\\n' https://api.openai.com/v1/models "
                "-H \"Authorization: Bearer $OPENAI_API_KEY\""
            ) from e
        raise

    text = (resp.choices[0].message.content or "").strip()
    if not text:
        raise RuntimeError("Empty response from OpenAI.")
    return text

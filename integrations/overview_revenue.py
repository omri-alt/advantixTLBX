"""
Keitaro revenue for overview dashboard (postback / conversion revenue from reports).

Uses ``KeitaroClient.build_report`` (``POST admin_api/v1/report/build``). Parses common
response shapes and metric keys (``revenue``, ``payout``, etc.).
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

# Prefer a single canonical revenue column when present (avoid double-counting).
_REV_KEYS_ORDER = (
    "revenue",
    "campaign_revenue",
    "sale_revenue",
    "conversions_revenue",
    "sales_revenue",
    "payout",
    "earn",
)


def _lower_keys(d: dict) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in d.items()}


def _row_revenue(row: dict) -> float:
    lk = _lower_keys(row)
    for k in _REV_KEYS_ORDER:
        if k in lk and lk[k] is not None:
            try:
                return float(lk[k])
            except (TypeError, ValueError):
                continue
    return 0.0


def _rows_from_report(report: Any) -> List[dict]:
    if not isinstance(report, dict):
        return []
    for k in ("rows", "data", "result", "body"):
        v = report.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return [x for x in v if isinstance(x, dict)]
    for k in ("total", "totals", "summary"):
        v = report.get(k)
        if isinstance(v, dict) and any(str(x).lower() in _REV_KEYS_ORDER for x in v.keys()):
            return [v]
    return []


def _extract_day_key(row: dict) -> str:
    lk = _lower_keys(row)
    for k in ("day", "date", "datetime", "click_date", "conversion_date"):
        if k in lk and lk[k] is not None:
            s = str(lk[k]).strip()
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return s[:10]
    return ""


def _report_payloads(d_from: date, d_to: date) -> List[Dict[str, Any]]:
    a = d_from.isoformat()
    b = d_to.isoformat()
    return [
        {
            "range": {"from": f"{a} 00:00:00", "to": f"{b} 23:59:59"},
            "grouping": ["day"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"from": a, "to": b},
            "grouping": ["day"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"interval": "custom", "from": f"{a} 00:00:00", "to": f"{b} 23:59:59"},
            "grouping": ["day"],
            "metrics": ["revenue", "conversions"],
        },
    ]


def _sum_revenue_from_report(report: Any, *, yesterday: Optional[date] = None) -> tuple[float, float]:
    """
    Returns (yesterday_revenue, mtd_total_revenue).
    If ``yesterday`` is None, yesterday_revenue is 0.0 and only mtd total is meaningful.
    """
    rows = _rows_from_report(report)

    mtd = 0.0
    yday = 0.0
    y_str = yesterday.isoformat() if yesterday else ""

    for row in rows:
        r = _row_revenue(row)
        mtd += r
        dk = _extract_day_key(row)
        if y_str and dk == y_str:
            yday += r

    return yday, mtd


def fetch_keitaro_revenue_overview(
    *,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    ``mtd_end`` should be yesterday (inclusive). Uses one MTD report when possible.

    Returns:
      ``{"yesterday": float|None, "mtd": float|None, "error": str|None}``
    """
    if mtd_start > mtd_end:
        return {"yesterday": 0.0, "mtd": 0.0, "error": None}

    if not (api_key or "").strip():
        return {"yesterday": None, "mtd": None, "error": "KEITARO_API_KEY not set"}

    client = KeitaroClient(base_url=base_url, api_key=api_key)
    last_err: str | None = None
    y_rev = 0.0
    mtd_rev = 0.0

    for payload in _report_payloads(mtd_start, mtd_end):
        try:
            report = client.build_report(payload)
            y_rev, mtd_rev = _sum_revenue_from_report(report, yesterday=yesterday)
            if yesterday < mtd_start or yesterday > mtd_end:
                y_rev = 0.0
            break
        except KeitaroClientError as e:
            last_err = str(e)
            logger.info("Keitaro report attempt failed: %s", last_err[:200])
        except Exception as e:
            last_err = str(e)
            logger.info("Keitaro report attempt failed: %s", last_err[:200])
    else:
        return {"yesterday": None, "mtd": None, "error": last_err or "Keitaro report failed"}

    if mtd_start <= yesterday <= mtd_end and y_rev == 0.0 and mtd_rev > 0.0:
        y_only = [
            {
                "range": {"from": f"{yesterday.isoformat()} 00:00:00", "to": f"{yesterday.isoformat()} 23:59:59"},
                "metrics": ["revenue", "conversions"],
            },
            {
                "range": {"from": yesterday.isoformat(), "to": yesterday.isoformat()},
                "metrics": ["revenue", "conversions"],
            },
        ]
        for payload in y_only:
            try:
                report_y = client.build_report(payload)
                y2, _ = _sum_revenue_from_report(report_y, yesterday=None)
                if y2 > 0:
                    y_rev = y2
                    break
                y3, _ = _sum_revenue_from_report(report_y, yesterday=yesterday)
                if y3 > 0:
                    y_rev = y3
                    break
            except Exception:
                continue

    return {"yesterday": round(y_rev, 4), "mtd": round(mtd_rev, 4), "error": None}


# Monetization-network rollup for the homepage affiliation widget.
AFFILIATION_ORDER: tuple[tuple[str, str], ...] = (
    ("kelkoo1", "Kelkoo 1"),
    ("kelkoo2", "Kelkoo 2"),
    ("kelkoo5", "Kelkoo 5"),
    ("kelkoo4", "Kelkoo 4"),
    ("yadore", "Yadore"),
    ("adexa", "Adexa"),
    ("shopnomix", "Shopnomix"),
    ("effinity", "Effinity"),
    ("flexoffers", "FlexOffers"),
    ("other", "Other"),
)

_FEED_KEY_TO_AFFIL = {
    "feed1": "kelkoo1",
    "feed2": "kelkoo2",
    "feed5": "kelkoo5",
    "feed8": "kelkoo4",
    "feed3": "yadore",
    "feed4": "adexa",
    "feed6": "shopnomix",
    "effinity": "effinity",
    "flexoffers": "flexoffers",
    "kelkoo1": "kelkoo1",
    "kelkoo2": "kelkoo2",
    "kelkoo5": "kelkoo5",
    "kelkoo4": "kelkoo4",
    "yadore": "yadore",
    "adexa": "adexa",
    "shopnomix": "shopnomix",
}

_AFFIL_IDS = {k for k, _ in AFFILIATION_ORDER}


def _scalar_name(row: dict, keys: tuple[str, ...]) -> str:
    lk = _lower_keys(row)
    for k in keys:
        v = lk.get(k)
        if v is None or v == "":
            continue
        if isinstance(v, dict):
            for nk in ("name", "value", "label", "title"):
                nv = v.get(nk)
                if nv is not None and str(nv).strip():
                    return str(nv).strip()
            continue
        s = str(v).strip()
        if s and s.lower() not in ("none", "null"):
            return s
    return ""


def _name_to_affiliation(name: str) -> Optional[str]:
    n = (name or "").lower()
    if not n:
        return None
    if "kelkoo4" in n or "feed8" in n or "feed 8" in n:
        return "kelkoo4"
    if "kelkoo5" in n or "feed5" in n or "feed 5" in n:
        return "kelkoo5"
    if "kelkoo2" in n or "feed2" in n or "feed 2" in n:
        return "kelkoo2"
    if "kelkoo1" in n or "feed1" in n or "feed 1" in n:
        return "kelkoo1"
    if "shopnomix" in n or "feed6" in n or "feed 6" in n:
        return "shopnomix"
    if "yadore" in n or "feed3" in n or "feed 3" in n or "yad feed" in n:
        return "yadore"
    if "adexa" in n:
        return "adexa"
    if "effinity" in n or "effiliation" in n:
        return "effinity"
    if "flexoffer" in n:
        return "flexoffers"
    return None


def _looks_like_feed_offer(name: str) -> bool:
    n = (name or "").lower()
    if not n:
        return False
    if n.startswith(("hub_", "blend_", "kl feed", "yad feed", "nipuhim", "blend-")):
        return True
    tokens = (
        "kelkoo",
        "adexa",
        "yadore",
        "shopnomix",
        "effinity",
        "flexoffer",
        "feed1",
        "feed2",
        "feed3",
        "feed4",
        "feed5",
        "feed6",
        "feed8",
        "feed 1",
        "feed 2",
        "feed 3",
        "feed 4",
        "feed 5",
        "feed 6",
        "feed 8",
    )
    return any(t in n for t in tokens)


def classify_affiliation(*, campaign_name: str = "", offer_name: str = "") -> str:
    """Map a Keitaro campaign/offer pair to a monetization-network affiliation id."""
    try:
        from integrations.keitaro_feed_balance import _offer_name_to_feed_key
    except Exception:
        _offer_name_to_feed_key = None  # type: ignore[assignment]

    if _looks_like_feed_offer(offer_name):
        if _offer_name_to_feed_key is not None:
            fk = _offer_name_to_feed_key(offer_name)
            mapped = _FEED_KEY_TO_AFFIL.get(fk or "")
            if mapped in _AFFIL_IDS:
                return mapped
        mapped = _name_to_affiliation(offer_name)
        if mapped:
            return mapped

    mapped = _name_to_affiliation(campaign_name)
    if mapped:
        return mapped
    mapped = _name_to_affiliation(offer_name)
    if mapped:
        return mapped
    return "other"


def _empty_affiliation_buckets() -> Dict[str, Dict[str, float]]:
    return {aid: {"yesterday": 0.0, "mtd": 0.0} for aid, _ in AFFILIATION_ORDER}


def _affiliation_payloads(d_from: date, d_to: date) -> List[Dict[str, Any]]:
    a = d_from.isoformat()
    b = d_to.isoformat()
    return [
        {
            "range": {"from": f"{a} 00:00:00", "to": f"{b} 23:59:59"},
            "grouping": ["campaign", "offer"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"from": a, "to": b},
            "grouping": ["campaign", "offer"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"from": f"{a} 00:00:00", "to": f"{b} 23:59:59"},
            "grouping": ["offer"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"from": a, "to": b},
            "grouping": ["offer"],
            "metrics": ["revenue", "conversions"],
        },
    ]


_AFFIL_REVENUE_SHARE: Dict[str, str] = {
    "kelkoo2": "kelkoo2",
    "kelkoo4": "kelkoo4",
    "kelkoo5": "kelkoo5",
}


def _affil_revenue_share(aid: str) -> float:
    """Net revenue share for an affiliation id (1.0 unless a sub-100% feed)."""
    feed_tag = _AFFIL_REVENUE_SHARE.get(aid)
    if not feed_tag:
        return 1.0
    try:
        from config import kelkoo_postback_revenue_share

        return kelkoo_postback_revenue_share(feed_tag=feed_tag)
    except Exception:
        return 1.0


def _sum_affiliation_from_report(
    report: Any,
    *,
    skip_aids: Optional[set[str]] = None,
) -> Dict[str, float]:
    skip = skip_aids or set()
    totals = {aid: 0.0 for aid, _ in AFFILIATION_ORDER}
    for row in _rows_from_report(report):
        offer = _scalar_name(row, ("offer", "offer_name", "offer_id"))
        campaign = _scalar_name(row, ("campaign", "campaign_name", "campaign_group"))
        aid = classify_affiliation(campaign_name=campaign, offer_name=offer)
        if aid not in totals:
            aid = "other"
        if aid in skip:
            continue
        rev = _row_revenue(row)
        share = _affil_revenue_share(aid)
        totals[aid] += rev * share
    return totals


# Kelkoo feeds: one publisher aggregated GET each (not Keitaro row rollups).
_KELKOO_AFFIL_API_TAGS: tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo4", "kelkoo5")


def _fetch_kelkoo_affil_from_publisher_api(
    tag: str,
    *,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
) -> Tuple[Optional[Dict[str, float]], Optional[str]]:
    """Single Kelkoo aggregated MTD call × postback share for one feed."""
    from integrations.kelkoo_api_revenue import fetch_kelkoo_feed_api_revenue

    try:
        data = fetch_kelkoo_feed_api_revenue(
            feed_tag=tag,
            yesterday=yesterday,
            mtd_start=mtd_start,
            mtd_end=mtd_end,
        )
    except Exception as e:
        return None, str(e)
    err = data.get("error")
    if err and data.get("mtd") is None:
        return None, str(err)
    return {
        "yesterday": float(data.get("yesterday") or 0.0),
        "mtd": float(data.get("mtd") or 0.0),
    }, None


def _fetch_affiliation_totals(
    client: KeitaroClient,
    d_from: date,
    d_to: date,
    *,
    skip_aids: Optional[set[str]] = None,
) -> tuple[Optional[Dict[str, float]], Optional[str]]:
    last_err: Optional[str] = None
    for payload in _affiliation_payloads(d_from, d_to):
        try:
            report = client.build_report(payload)
            return _sum_affiliation_from_report(report, skip_aids=skip_aids), None
        except KeitaroClientError as e:
            last_err = str(e)
            logger.info("Keitaro affiliation report attempt failed: %s", last_err[:200])
        except Exception as e:
            last_err = str(e)
            logger.info("Keitaro affiliation report attempt failed: %s", last_err[:200])
    return None, last_err or "Keitaro affiliation report failed"


def _affiliation_rows(buckets: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for aid, label in AFFILIATION_ORDER:
        b = buckets.get(aid) or {"yesterday": 0.0, "mtd": 0.0}
        y = round(float(b.get("yesterday") or 0.0), 4)
        m = round(float(b.get("mtd") or 0.0), 4)
        if y == 0.0 and m == 0.0:
            continue
        rows.append({"id": aid, "label": label, "yesterday": y, "mtd": m})
    return rows


def fetch_keitaro_affiliation_revenue(
    *,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Cumulative Keitaro conversion revenue by monetization affiliation.

    Window is first of month through yesterday (UTC), same as the overview MTD tiles.
    """
    empty_rows = _affiliation_rows(_empty_affiliation_buckets())
    if mtd_start > mtd_end:
        return {"yesterday": 0.0, "mtd": 0.0, "error": None, "rows": empty_rows}

    buckets = _empty_affiliation_buckets()
    kelkoo_skip = set(_KELKOO_AFFIL_API_TAGS)
    api_errors: List[str] = []

    # One Kelkoo aggregated GET per feed (parallel, ~5s each).
    with ThreadPoolExecutor(max_workers=len(_KELKOO_AFFIL_API_TAGS)) as pool:
        futs = {
            pool.submit(
                _fetch_kelkoo_affil_from_publisher_api,
                tag,
                yesterday=yesterday,
                mtd_start=mtd_start,
                mtd_end=mtd_end,
            ): tag
            for tag in _KELKOO_AFFIL_API_TAGS
        }
        for fut in as_completed(futs):
            tag = futs[fut]
            try:
                amounts, err = fut.result()
            except Exception as e:
                api_errors.append(f"{tag}: {e}")
                continue
            if err:
                api_errors.append(f"{tag}: {err}")
                continue
            if amounts:
                buckets[tag]["mtd"] = float(amounts.get("mtd") or 0.0)
                buckets[tag]["yesterday"] = float(amounts.get("yesterday") or 0.0)

    # Non-Kelkoo affiliations from Keitaro when configured (one report; kelkoo rows skipped).
    mtd_map: Dict[str, float] = {aid: 0.0 for aid, _ in AFFILIATION_ORDER}
    y_map: Dict[str, float] = {aid: 0.0 for aid, _ in AFFILIATION_ORDER}
    keitaro_err: Optional[str] = None
    if (api_key or "").strip():
        client = KeitaroClient(base_url=base_url, api_key=api_key)
        fetched, mtd_err = _fetch_affiliation_totals(client, mtd_start, mtd_end, skip_aids=kelkoo_skip)
        if fetched is None:
            keitaro_err = mtd_err
            logger.info("Keitaro affiliation MTD failed: %s", (mtd_err or "")[:200])
        else:
            mtd_map = fetched
        if mtd_start <= yesterday <= mtd_end:
            y_only, y_err = _fetch_affiliation_totals(
                client, yesterday, yesterday, skip_aids=kelkoo_skip
            )
            if y_only is None:
                logger.info("Keitaro affiliation yesterday split failed: %s", (y_err or "")[:200])
            else:
                y_map = y_only
    else:
        keitaro_err = "KEITARO_API_KEY not set"

    for aid, _ in AFFILIATION_ORDER:
        if aid in kelkoo_skip:
            continue
        buckets[aid]["mtd"] = float(mtd_map.get(aid) or 0.0)
        buckets[aid]["yesterday"] = float(y_map.get(aid) or 0.0)

    rows = _affiliation_rows(buckets)
    y_total = sum(float(r["yesterday"]) for r in rows)
    m_total = sum(float(r["mtd"]) for r in rows)
    err = None
    if api_errors:
        err = "; ".join(api_errors[:3])
    elif not rows and keitaro_err:
        err = keitaro_err
    return {
        "yesterday": round(y_total, 4),
        "mtd": round(m_total, 4),
        "error": err,
        "rows": rows,
    }

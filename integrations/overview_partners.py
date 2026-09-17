"""
Lean live Performance overview — partner totals only (no Keitaro rollups).

Revenue: one Kelkoo aggregated GET per feed (1/2/4/5); Yadore ``/v2/report/general``
per day (EUR, excluded from USD totals). Cost: Zeropark / Ecomnia / Trillion / SK
(account aggregate via ``SK_ACCOUNT_STATS_URL`` only — no per-campaign crawl).
"""
from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = ROOT / "runtime" / "overview_live.json"
CACHE_TTL_SEC = 15 * 60
_LOCK = threading.Lock()

KELKOO_ROWS: Tuple[Tuple[str, str], ...] = (
    ("kelkoo1", "Kelkoo 1"),
    ("kelkoo2", "Kelkoo 2"),
    ("kelkoo5", "Kelkoo 5"),
    ("kelkoo4", "Kelkoo 4"),
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def overview_period() -> Tuple[date, date, date]:
    now = datetime.now(timezone.utc).date()
    yesterday = now - timedelta(days=1)
    return yesterday, now.replace(day=1), yesterday


def _nz(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _row(id_: str, label: str, *, yesterday=None, mtd=None, error=None, source="") -> Dict[str, Any]:
    return {
        "id": id_,
        "label": label,
        "yesterday": None if yesterday is None else round(float(yesterday), 4),
        "mtd": None if mtd is None else round(float(mtd), 4),
        "error": error,
        "source": source,
    }


def _fetch_kelkoo(tag: str, label: str, yesterday: date, m0: date, m1: date) -> Dict[str, Any]:
    from integrations.kelkoo_api_revenue import fetch_kelkoo_feed_api_revenue

    try:
        data = fetch_kelkoo_feed_api_revenue(
            feed_tag=tag, yesterday=yesterday, mtd_start=m0, mtd_end=m1
        )
    except Exception as e:
        return _row(tag, label, error=str(e), source="kelkoo_aggregated")
    err = data.get("error")
    if err and data.get("mtd") is None:
        return _row(tag, label, error=str(err), source="kelkoo_aggregated")
    return _row(
        tag,
        label,
        yesterday=data.get("yesterday"),
        mtd=data.get("mtd"),
        error=None,
        source="kelkoo_aggregated",
    )


def _fetch_yadore(yesterday: date, m0: date, m1: date) -> Dict[str, Any]:
    """Sum Yadore ``/v2/report/general`` (per-day, EUR CPC revenue) for both placements."""
    from config import YADORE_API_KEY, YADORE_NO_COUPON_API_KEY
    from integrations.yadore import YADORE_BASE_URL, YadoreClientError
    import requests

    keys: List[Tuple[str, str]] = []
    if (YADORE_API_KEY or "").strip():
        keys.append(("coupon", YADORE_API_KEY.strip()))
    if (YADORE_NO_COUPON_API_KEY or "").strip():
        keys.append(("no_coupon", YADORE_NO_COUPON_API_KEY.strip()))
    if not keys:
        return _row("yadore", "Yadore", error="YADORE_API_KEY not set", source="yadore_report_general")

    days: List[date] = []
    cur = m0
    while cur <= m1:
        days.append(cur)
        cur += timedelta(days=1)
    if not days:
        return _row("yadore", "Yadore (EUR)", yesterday=0.0, mtd=0.0, source="yadore_report_general_eur")

    def one_day(api_key: str, day: date) -> float:
        endpoint = f"{YADORE_BASE_URL.rstrip('/')}/v2/report/general"
        headers = {"Accept": "application/json", "API-Key": api_key}
        r = requests.get(
            endpoint,
            headers=headers,
            params={"date": day.isoformat(), "format": "json"},
            timeout=45,
        )
        if r.status_code != 200:
            raise YadoreClientError(f"report/general HTTP {r.status_code}", status_code=r.status_code)
        data = r.json() if r.text else {}
        total = data.get("total") if isinstance(data, dict) else None
        if not isinstance(total, dict):
            return 0.0
        return float(total.get("revenue") or 0)

    y_key = yesterday.isoformat()
    by_day: Dict[str, float] = {d.isoformat(): 0.0 for d in days}
    errs: List[str] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {}
        for _name, key in keys:
            for d in days:
                futs[pool.submit(one_day, key, d)] = d.isoformat()
        for fut in as_completed(futs):
            day = futs[fut]
            try:
                by_day[day] = float(by_day.get(day) or 0.0) + float(fut.result() or 0.0)
            except Exception as e:
                errs.append(f"{day}: {e}")
    mtd = sum(by_day.values())
    y = float(by_day.get(y_key) or 0.0)
    return _row(
        "yadore",
        "Yadore (EUR)",
        yesterday=y,
        mtd=mtd,
        error=("; ".join(errs[:2])[:180] if errs else None),
        source="yadore_report_general_eur",
    )


def _fetch_cost(name: str, label: str, fn: Callable[..., Dict[str, Any]], yesterday: date, m0: date, m1: date) -> Dict[str, Any]:
    try:
        data = fn(yesterday=yesterday, mtd_start=m0, mtd_end=m1)
    except Exception as e:
        return _row(name, label, error=str(e), source=name)
    return _row(
        name,
        label,
        yesterday=data.get("yesterday"),
        mtd=data.get("mtd"),
        error=data.get("error"),
        source=name,
    )


def _fetch_sk_fast(yesterday: date, m0: date, m1: date) -> Dict[str, Any]:
    """Account-level SK only — never fall back to per-campaign crawl."""
    from config import SK_ACCOUNT_STATS_URL, SOURCEKNOWLEDGE_API_KEY
    from integrations.overview_costs import _sk_get_aggregate_spend

    if not (SOURCEKNOWLEDGE_API_KEY or "").strip():
        return _row("sourceknowledge", "SourceKnowledge", error="KEYSK not set", source="sk")
    if not (SK_ACCOUNT_STATS_URL or "").strip():
        return _row(
            "sourceknowledge",
            "SourceKnowledge",
            error="Set SK_ACCOUNT_STATS_URL for fast overview (per-campaign crawl disabled)",
            source="sk",
        )
    api_key = SOURCEKNOWLEDGE_API_KEY.strip()
    try:
        y = _sk_get_aggregate_spend(api_key, yesterday.isoformat(), yesterday.isoformat())
        m = _sk_get_aggregate_spend(api_key, m0.isoformat(), m1.isoformat()) if m0 <= m1 else 0.0
        if y is None and m is None:
            return _row(
                "sourceknowledge",
                "SourceKnowledge",
                error="SK account stats URL returned no spend",
                source="sk",
            )
        return _row(
            "sourceknowledge",
            "SourceKnowledge",
            yesterday=_nz(y),
            mtd=_nz(m),
            source="sk",
        )
    except Exception as e:
        return _row("sourceknowledge", "SourceKnowledge", error=str(e), source="sk")


def read_live_cache(*, max_age_sec: float = CACHE_TTL_SEC) -> Optional[Dict[str, Any]]:
    if not CACHE_PATH.is_file():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    as_of = str(data.get("as_of_utc") or "")
    try:
        ts = datetime.strptime(as_of, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age > max_age_sec:
            return None
    except ValueError:
        return None
    return data


def write_live_cache(data: Dict[str, Any]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(CACHE_PATH)


def build_overview_live(*, force: bool = False) -> Dict[str, Any]:
    """
    Parallel partner pulls. Typical: ~4 Kelkoo + 3–4 cost APIs + optional Yadore (~10–20s).
    """
    if not force:
        cached = read_live_cache()
        if cached is not None:
            out = dict(cached)
            out["cached"] = True
            return out

    with _LOCK:
        if not force:
            cached = read_live_cache()
            if cached is not None:
                out = dict(cached)
                out["cached"] = True
                return out

        yesterday, m0, m1 = overview_period()
        t0 = time.time()
        from integrations.overview_costs import (
            fetch_ecomnia_cost,
            fetch_trillion_cost,
            fetch_zeropark_cost,
        )

        jobs: Dict[Any, str] = {}
        revenue_rows: List[Dict[str, Any]] = []
        cost_rows: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=10) as pool:
            for tag, label in KELKOO_ROWS:
                jobs[pool.submit(_fetch_kelkoo, tag, label, yesterday, m0, m1)] = f"rev:{tag}"
            jobs[pool.submit(_fetch_yadore, yesterday, m0, m1)] = "rev:yadore"
            jobs[pool.submit(_fetch_cost, "zeropark", "Zeropark", fetch_zeropark_cost, yesterday, m0, m1)] = "cost:zp"
            jobs[pool.submit(_fetch_cost, "ecomnia", "Ecomnia", fetch_ecomnia_cost, yesterday, m0, m1)] = "cost:ec"
            jobs[pool.submit(_fetch_cost, "trillion", "Trillion", fetch_trillion_cost, yesterday, m0, m1)] = "cost:tr"
            jobs[pool.submit(_fetch_sk_fast, yesterday, m0, m1)] = "cost:sk"

            by_key: Dict[str, Dict[str, Any]] = {}
            for fut in as_completed(jobs):
                key = jobs[fut]
                try:
                    by_key[key] = fut.result()
                except Exception as e:
                    kind, sid = key.split(":", 1)
                    by_key[key] = _row(sid, sid, error=str(e), source=sid)

        for tag, label in KELKOO_ROWS:
            revenue_rows.append(by_key.get(f"rev:{tag}") or _row(tag, label, error="missing"))
        revenue_rows.append(by_key.get("rev:yadore") or _row("yadore", "Yadore", error="missing"))

        for cid, label in (
            ("zeropark", "Zeropark"),
            ("sourceknowledge", "SourceKnowledge"),
            ("ecomnia", "Ecomnia"),
            ("trillion", "Trillion"),
        ):
            key = {
                "zeropark": "cost:zp",
                "sourceknowledge": "cost:sk",
                "ecomnia": "cost:ec",
                "trillion": "cost:tr",
            }[cid]
            cost_rows.append(by_key.get(key) or _row(cid, label, error="missing"))

        rev_y = sum(
            _nz(r.get("yesterday"))
            for r in revenue_rows
            if r.get("yesterday") is not None and "eur" not in str(r.get("source") or "")
        )
        rev_m = sum(
            _nz(r.get("mtd"))
            for r in revenue_rows
            if r.get("mtd") is not None and "eur" not in str(r.get("source") or "")
        )
        cost_y = sum(_nz(r.get("yesterday")) for r in cost_rows if r.get("yesterday") is not None)
        cost_m = sum(_nz(r.get("mtd")) for r in cost_rows if r.get("mtd") is not None)

        payload = {
            "as_of_utc": _utc_now(),
            "elapsed_sec": round(time.time() - t0, 1),
            "cached": False,
            "ranges": {
                "yesterday": yesterday.isoformat(),
                "mtd_from": m0.isoformat(),
                "mtd_to": m1.isoformat(),
            },
            "revenue_rows": revenue_rows,
            "cost_rows": cost_rows,
            "totals": {
                "revenue_yesterday": round(rev_y, 4),
                "revenue_mtd": round(rev_m, 4),
                "cost_yesterday": round(cost_y, 4),
                "cost_mtd": round(cost_m, 4),
                "net_yesterday": round(rev_y - cost_y, 4),
                "net_mtd": round(rev_m - cost_m, 4),
            },
        }
        try:
            write_live_cache(payload)
        except OSError:
            logger.warning("Could not write overview live cache", exc_info=True)
        return payload

#!/usr/bin/env python3
"""One-shot: Yadore MX sales vs Keitaro, then optional SaleOur postbacks."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from integrations.daily_conversion_postbacks import (  # noqa: E402
    _yadore_conversion_to_sale,
    build_daily_postback_url,
)
from integrations.keitaro_conversions import (  # noqa: E402
    collect_sale_postback_keys,
    has_matching_sale_postback,
    normalize_payout,
)
from integrations.yadore import fetch_conversion_detail_clicks  # noqa: E402
from late_conversion_sales import SaleRow, _dedupe_sale_rows, send_postback_gets  # noqa: E402
from config import DAILY_CONVERSION_POSTBACK_SALE_STATUS  # noqa: E402


def _fetch_mx_sales(date_from: date, date_to: date) -> list[SaleRow]:
    from datetime import timedelta

    out: list[SaleRow] = []
    d = date_from
    while d <= date_to:
        day_s = d.isoformat()
        for conv in fetch_conversion_detail_clicks(day_s, markets=["mx"]):
            parsed = _yadore_conversion_to_sale(conv)
            if not parsed:
                continue
            sub_id, payout, merchant, market = parsed
            sale_dt = (str(conv.get("date") or day_s) or day_s)[:10]
            out.append(
                SaleRow(
                    feed="yadore",
                    sub_id=sub_id,
                    sale_date=sale_dt,
                    sale_value_usd=payout,
                    merchant=merchant,
                    country=market,
                    geo=market,
                )
            )
        d += timedelta(days=1)
    return _dedupe_sale_rows(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", default="2026-07-01")
    ap.add_argument("--to", dest="date_to", default="")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.apply and args.dry_run:
        print("Use --dry-run or --apply, not both")
        return 2
    dry_run = not args.apply

    date_from = date.fromisoformat(args.date_from[:10])
    if args.date_to:
        date_to = date.fromisoformat(args.date_to[:10])
    else:
        # through yesterday UTC
        date_to = datetime.now(timezone.utc).date()
        from datetime import timedelta

        date_to = date_to - timedelta(days=1)

    print(f"Fetching Yadore MX conversion/detail {date_from} .. {date_to} ...")
    rows = _fetch_mx_sales(date_from, date_to)
    print(f"Yadore MX sales (deduped): {len(rows)}")
    by_day = Counter(r.sale_date for r in rows)
    for day in sorted(by_day):
        print(f"  {day}: {by_day[day]}")

    today = datetime.now(timezone.utc).date()
    print(f"Fetching Keitaro SaleOur/LateSale keys {date_from} .. {today} ...")
    keys = collect_sale_postback_keys(date_from=date_from, date_to=today)

    missing: list[SaleRow] = []
    skipped = 0
    for r in rows:
        if has_matching_sale_postback(r.sub_id, r.sale_value_usd, keys):
            skipped += 1
        else:
            missing.append(r)

    print(f"Already in Keitaro: {skipped}")
    print(f"Missing in Keitaro: {len(missing)}")
    for r in missing[:30]:
        print(f"  {r.sale_date} {r.sub_id} payout={r.sale_value_usd} merchant={r.merchant}")
    if len(missing) > 30:
        print(f"  ... +{len(missing) - 30} more")

    report_path = ROOT / "data" / f"yadore_mx_missing_{date_from}_{date_to}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "yadore_mx_sales": len(rows),
        "skipped_keitaro": skipped,
        "missing": [
            {
                "sub_id": r.sub_id,
                "payout": normalize_payout(r.sale_value_usd),
                "sale_date": r.sale_date,
                "merchant": r.merchant,
                "country": r.country,
            }
            for r in missing
        ],
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {report_path}")

    if not missing:
        print("Nothing to fire.")
        return 0

    status = (DAILY_CONVERSION_POSTBACK_SALE_STATUS or "SaleOur").strip()
    # Yadore daily convention: SaleOur with payout=0
    urls = [
        build_daily_postback_url(subid=r.sub_id, payout="0", status=status)
        for r in missing
    ]
    print(f"mode={'dry-run' if dry_run else 'apply'} eligible={len(urls)} status={status}")
    for u in urls[:5]:
        print(" ", u)

    if dry_run:
        print("Dry-run only. Re-run with --apply to fire.")
        return 0

    ok = 0
    fail = 0
    failures = []
    for r, u in zip(missing, urls):
        pr = send_postback_gets([u])[0]
        code = pr.get("http_status")
        if code is not None and 200 <= int(code) < 400 and not pr.get("http_error"):
            ok += 1
        else:
            fail += 1
            failures.append({"sub_id": r.sub_id, "http_status": code, "error": pr.get("http_error")})
        if (ok + fail) % 25 == 0:
            print(f"  progress {ok + fail}/{len(urls)} ok={ok} fail={fail}")

    print(f"Done: sent_ok={ok} sent_fail={fail}")
    for f in failures[:20]:
        print(" FAIL:", f)
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

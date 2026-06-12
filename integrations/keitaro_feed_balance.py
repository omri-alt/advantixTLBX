"""
Keitaro multi-feed campaign optimizer — config + reports live in campaign ``notes``.

Bootstrap writes the config template; daily optimization (later) refreshes only the
auto-updated section below the divider.
"""
from __future__ import annotations

import csv
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

CONFIG_HEADER = "=== Feed balance config ==="
AUTO_DIVIDER = "--- auto-updated below (do not edit) ---"

_KNOWN_GEOS = frozenset(
    {
        "uk",
        "gb",
        "us",
        "fr",
        "de",
        "es",
        "it",
        "nl",
        "be",
        "at",
        "ch",
        "se",
        "no",
        "dk",
        "fi",
        "pl",
        "pt",
        "ie",
        "au",
        "ca",
        "cz",
        "hu",
        "ro",
        "gr",
    }
)

_DEFAULT_GROUPS = tuple(
    g.strip()
    for g in (os.getenv("KEITARO_FEED_BALANCE_GROUPS") or "Quality,effinity,flexoffers").split(",")
    if g.strip()
)

_SK_ADVERTISERS_CSV = Path(__file__).resolve().parents[1] / "sk_advertisers.csv"


@dataclass
class FeedBalanceConfig:
    url: str = ""
    geo: str = ""
    enabled: str = "no"

    @property
    def ready(self) -> bool:
        return bool(self.url.strip() and self.geo.strip() and self.enabled.strip().lower() in ("yes", "y", "true", "1"))


def has_feed_balance_config(notes: str) -> bool:
    return CONFIG_HEADER in (notes or "")


def parse_feed_balance_config(notes: str) -> FeedBalanceConfig:
    text = notes or ""
    if CONFIG_HEADER not in text:
        return FeedBalanceConfig()
    block = text.split(CONFIG_HEADER, 1)[1]
    if AUTO_DIVIDER in block:
        block = block.split(AUTO_DIVIDER, 1)[0]
    cfg = FeedBalanceConfig()
    for line in block.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if ":" not in s:
            continue
        key, val = s.split(":", 1)
        key = key.strip().lower()
        val = val.strip()
        if key == "url":
            cfg.url = val
        elif key == "geo":
            cfg.geo = val.lower()[:2] if val.lower() != "uk" else "uk"
        elif key == "enabled":
            cfg.enabled = val
    if cfg.geo == "gb":
        cfg.geo = "uk"
    return cfg


def build_bootstrap_notes(
    *,
    url: str = "",
    geo: str = "",
    enabled: str = "no",
    report: str = "Pending first optimization run.",
) -> str:
    url_line = url.strip() if url.strip() else ""
    geo_line = geo.strip().lower() if geo.strip() else ""
    if geo_line == "gb":
        geo_line = "uk"
    return (
        f"{CONFIG_HEADER}\n"
        f"url: {url_line}\n"
        f"geo: {geo_line}\n"
        f"enabled: {enabled}\n"
        f"\n"
        f"{AUTO_DIVIDER}\n"
        f"{report.strip()}\n"
    )


def merge_notes_preserve_config(existing_notes: str, new_report: str) -> str:
    """Replace only the auto section; keep user config block."""
    cfg = parse_feed_balance_config(existing_notes)
    return build_bootstrap_notes(url=cfg.url, geo=cfg.geo, enabled=cfg.enabled, report=new_report)


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _geo_from_domain_token(token: str) -> str:
    t = (token or "").strip().lower()
    m = re.search(r"\.([a-z]{2})$", t)
    if not m:
        return ""
    tld = m.group(1)
    if tld == "uk":
        return "uk"
    if tld in _KNOWN_GEOS:
        return tld
    return ""


def parse_brand_geo_from_campaign_name(name: str) -> Tuple[str, str]:
    """
  Parse ``Brand-geo`` or ``domain.tld - network`` style Keitaro campaign names.
  Returns ``(brand_slug, geo)`` — either may be empty.
    """
    raw = (name or "").strip()
    if not raw:
        return "", ""

    head = raw.split(" - ", 1)[0].strip() if " - " in raw else raw

    if re.search(r"\.[a-z]{2,}(?:/|$)", head, re.I):
        geo = _geo_from_domain_token(head.split("/")[0])
        return _slug(head.split("/")[0]), geo

    raw = head
    parts = [p for p in raw.split("-") if p.strip()]
    if len(parts) >= 2:
        tail = parts[-1].strip().lower()
        if tail in _KNOWN_GEOS:
            brand = "-".join(parts[:-1])
            geo = "uk" if tail == "gb" else tail[:2]
            return _slug(brand), geo
    return _slug(raw), ""


def _domain_from_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    if re.match(r"^[a-z0-9][-a-z0-9]*\.[a-z]{2,}$", t, re.I):
        return f"https://{t.lower()}"
    m = re.search(r"https?://[^\s\"']+", t, re.I)
    if m:
        return m.group(0).rstrip(".,)")
    m = re.search(r"url=([^&\"']+)", t, re.I)
    if m:
        u = unquote(m.group(1))
        if u.lower().startswith("http"):
            return u
        return f"https://{u.lstrip('/')}"
    return ""


def _load_sk_advertiser_index() -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    path = _SK_ADVERTISERS_CSV
    if not path.exists():
        return rows
    try:
        with path.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                name = str(row.get("name") or "")
                url = str(row.get("businessUrl") or row.get("businessurl") or "").strip()
                if not name or not url:
                    continue
                rows.append((_slug(name), name, url))
    except Exception as e:
        logger.warning("SK advertisers CSV read failed: %s", e)
    return rows


def infer_merchant_url(
    campaign_name: str,
    geo: str,
    *,
    offer_payloads: Optional[List[str]] = None,
    sk_index: Optional[List[Tuple[str, str, str]]] = None,
) -> str:
    """Best-effort merchant homepage for notes bootstrap."""
    name = (campaign_name or "").strip()
    domain_url = _domain_from_text(name.split(" - ", 1)[0] if " - " in name else name)
    if domain_url:
        return domain_url

    for payload in offer_payloads or []:
        u = _domain_from_text(payload or "")
        if u and "effiliation.com" not in u.lower() and "shopli.city" not in u.lower():
            host = (urlparse(u).hostname or "").lower()
            if host and not host.endswith(("kelkoogroup.net", "adexad.com", "v2i8b.com")):
                return u if u.startswith("http") else f"https://{u}"

    brand_slug, name_geo = parse_brand_geo_from_campaign_name(name)
    g = (geo or name_geo or "").lower()
    if g == "gb":
        g = "uk"

    index = sk_index if sk_index is not None else _load_sk_advertiser_index()
    if not brand_slug:
        return ""

    candidates: List[Tuple[int, str]] = []
    for slug, _disp, url in index:
        score = 0
        if brand_slug in slug or slug.startswith(brand_slug) or brand_slug.startswith(slug[: max(4, len(slug))]):
            score += 10
        if g and g in slug:
            score += 5
        if score > 0:
            candidates.append((score, url))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][1]


def _offer_name_to_feed_key(name: str) -> Optional[str]:
    n = (name or "").lower()
    if "fallback" in n:
        return None
    if "feed 1" in n or "feed1" in n or n.startswith("kl feed 1"):
        return "feed1"
    if "feed 2" in n or "feed2" in n or n.startswith("kl feed 2"):
        return "feed2"
    if "feed 5" in n or "feed5" in n or n.startswith("kl feed 5"):
        return "feed5"
    if "adexa" in n or "feed4" in n:
        return "feed4"
    if "yad" in n or "yadore" in n or "feed3" in n:
        return "feed3"
    if "shopnomix" in n or "feed6" in n:
        return "feed6"
    if "effinity" in n or "effiliation" in n:
        return "effinity"
    if "flexoffer" in n:
        return "flexoffers"
    return None


def _regular_flow_with_feeds(streams: List[Dict[str, Any]], client: KeitaroClient) -> Optional[Dict[str, Any]]:
    offer_cache: Dict[int, str] = {}
    for stream in streams:
        if (stream.get("type") or "").lower() != "regular":
            continue
        offers = stream.get("offers") or []
        feed_keys: List[str] = []
        for o in offers:
            oid = int(o.get("offer_id") or 0)
            if not oid:
                continue
            if oid not in offer_cache:
                try:
                    resp = client._session.get(client._api_path(f"offers/{oid}"), timeout=30)
                    offer_cache[oid] = str(resp.json().get("name") or "") if resp.ok else ""
                except Exception:
                    offer_cache[oid] = ""
            fk = _offer_name_to_feed_key(offer_cache[oid])
            if fk:
                feed_keys.append(fk)
        if len(set(feed_keys)) >= 1:
            return stream
    return None


def discover_feed_balance_campaigns(
    client: Optional[KeitaroClient] = None,
    *,
    groups: Optional[Tuple[str, ...]] = None,
) -> List[Dict[str, Any]]:
    """Campaigns in configured groups that have a regular flow with feed offers."""
    c = client or KeitaroClient()
    want_groups = set(groups or _DEFAULT_GROUPS)
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        batch = c.get_campaigns(offset=offset, limit=250)
        if not batch:
            break
        for camp in batch:
            grp = str(camp.get("group") or "").strip()
            if grp not in want_groups:
                continue
            cid = int(camp.get("id") or 0)
            if not cid:
                continue
            try:
                streams = c.get_streams(cid)
            except KeitaroClientError as e:
                logger.warning("streams %s: %s", cid, e)
                continue
            flow = _regular_flow_with_feeds(streams, c)
            if not flow:
                continue
            out.append(
                {
                    "id": cid,
                    "name": str(camp.get("name") or ""),
                    "group": grp,
                    "alias": str(camp.get("alias") or ""),
                    "stream_id": int(flow.get("id") or 0),
                    "notes": str(camp.get("notes") or ""),
                }
            )
        if len(batch) < 250:
            break
        offset += 250
    return out


def bootstrap_campaign_notes(
    client: Optional[KeitaroClient] = None,
    *,
    dry_run: bool = False,
    force: bool = False,
    campaign_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Write feed-balance note templates on campaigns that do not have one yet.

    Fills ``url`` / ``geo`` when inference succeeds; leaves blanks for manual entry.
    """
    c = client or KeitaroClient()
    sk_index = _load_sk_advertiser_index()
    campaigns = discover_feed_balance_campaigns(c)
    if campaign_ids is not None:
        want = {int(x) for x in campaign_ids}
        campaigns = [x for x in campaigns if int(x["id"]) in want]

    updated = 0
    skipped_has_config = 0
    skipped_no_change = 0
    errors = 0
    rows: List[Dict[str, Any]] = []

    for camp in campaigns:
        cid = int(camp["id"])
        try:
            full = c.get_campaign(cid)
        except KeitaroClientError as e:
            errors += 1
            rows.append({"campaignId": cid, "name": camp["name"], "error": str(e)})
            continue

        notes = str(full.get("notes") or "")
        if has_feed_balance_config(notes) and not force:
            skipped_has_config += 1
            rows.append({"campaignId": cid, "name": camp["name"], "action": "skip_has_config"})
            continue

        brand_slug, geo = parse_brand_geo_from_campaign_name(camp["name"])
        offer_payloads: List[str] = []
        try:
            streams = c.get_streams(cid)
            for s in streams:
                if (s.get("type") or "").lower() != "regular":
                    continue
                for o in s.get("offers") or []:
                    oid = int(o.get("offer_id") or 0)
                    if not oid:
                        continue
                    resp = c._session.get(c._api_path(f"offers/{oid}"), timeout=30)
                    if resp.ok:
                        body = resp.json()
                        offer_payloads.append(
                            str(body.get("action_payload") or body.get("url") or "")
                        )
        except Exception:
            pass

        url = infer_merchant_url(camp["name"], geo, offer_payloads=offer_payloads, sk_index=sk_index)
        new_notes = build_bootstrap_notes(url=url, geo=geo, enabled="no")
        if new_notes.strip() == notes.strip():
            skipped_no_change += 1
            rows.append({"campaignId": cid, "name": camp["name"], "action": "skip_unchanged", "geo": geo, "url": url})
            continue

        action = "dry_run" if dry_run else "updated"
        if not dry_run:
            try:
                c.update_campaign(cid, {"notes": new_notes})
                updated += 1
            except KeitaroClientError as e:
                errors += 1
                action = "error"
                rows.append({"campaignId": cid, "name": camp["name"], "action": action, "error": str(e)})
                continue

        rows.append(
            {
                "campaignId": cid,
                "name": camp["name"],
                "group": camp["group"],
                "action": action,
                "geo": geo,
                "url": url,
                "url_inferred": bool(url),
                "geo_inferred": bool(geo),
            }
        )

    return {
        "discovered": len(campaigns),
        "updated": updated,
        "skipped_has_config": skipped_has_config,
        "skipped_unchanged": skipped_no_change,
        "errors": errors,
        "rows": rows,
    }


_FEED_LABELS = {
    "feed1": "kelkoo1",
    "feed2": "kelkoo2",
    "feed5": "kelkoo5",
    "feed3": "yadore",
    "feed4": "adexa",
    "feed6": "shopnomix",
    "effinity": "effinity",
    "flexoffers": "flexoffers",
}


@dataclass
class FlowFeedOffer:
    offer_id: int
    offer_name: str
    feed_key: str
    share: int
    state: str


@dataclass
class FeedCheckResult:
    feed_key: str
    label: str
    in_flow: bool
    share: int
    offer_name: str
    found: Optional[bool]
    detail: str
    note: str = ""
    mode: str = ""
    operator_hint: str = ""
    keitaro_offer_url: str = ""


@dataclass
class CampaignCheckmonRow:
    campaign_id: int
    campaign_name: str
    group: str
    url: str
    geo: str
    enabled: str
    stream_id: int
    feed_results: List[FeedCheckResult] = field(default_factory=list)
    error: str = ""


def list_regular_flow_feed_offers(
    client: KeitaroClient,
    campaign_id: int,
) -> Tuple[Optional[int], List[FlowFeedOffer]]:
    """Regular flow stream id + mapped feed offers (non-fallback)."""
    streams = client.get_streams(int(campaign_id))
    offer_cache: Dict[int, str] = {}
    for stream in streams:
        if (stream.get("type") or "").lower() != "regular":
            continue
        sid = int(stream.get("id") or 0)
        out: List[FlowFeedOffer] = []
        for o in stream.get("offers") or []:
            oid = int(o.get("offer_id") or 0)
            if not oid:
                continue
            if oid not in offer_cache:
                try:
                    resp = client._session.get(client._api_path(f"offers/{oid}"), timeout=30)
                    offer_cache[oid] = str(resp.json().get("name") or "") if resp.ok else ""
                except Exception:
                    offer_cache[oid] = ""
            fk = _offer_name_to_feed_key(offer_cache[oid])
            if not fk:
                continue
            out.append(
                FlowFeedOffer(
                    offer_id=oid,
                    offer_name=offer_cache[oid],
                    feed_key=fk,
                    share=int(o.get("share") or 0),
                    state=str(o.get("state") or ""),
                )
            )
        if out:
            return sid, out
    return None, []


def _feed_results_from_checkmon(
    url: str,
    geo: str,
    flow_offers: List[FlowFeedOffer],
) -> List[FeedCheckResult]:
    from integrations.monetization_geo import shopnomix_feed_class, yadore_feed_class
    from monetization_check import _run_row_checks

    checks = _run_row_checks(url, geo)
    k1 = checks.get("k1") or {}
    k2 = checks.get("k2") or {}
    k5 = checks.get("k5") or {}
    y_nc = checks.get("ync") or {}
    y_c = checks.get("yc") or {}
    ax = checks.get("ax") or {}
    sn_tile = checks.get("sn_tile") or {}
    sn_coupons = checks.get("sn_coupons") or {}

    ax_mode = str(ax.get("mode") or "")
    ax_detail = str(ax.get("note") or "")
    if ax_mode == "smartlink":
        ax_extra = f"smartlink cpc={ax.get('estimated_cpc') or '?'}"
        if ax.get("smartlink_url"):
            ax_extra += f" golink={ax.get('smartlink_url')}"
    elif ax_mode == "links":
        ax_extra = str(ax.get("note") or "http_redirect")
    else:
        ax_extra = str(ax.get("note") or "")

    by_feed: Dict[str, Dict[str, Any]] = {
        "feed1": {"found": bool(k1.get("found")), "detail": str(k1.get("estimatedCpc") or ""), "note": ""},
        "feed2": {"found": bool(k2.get("found")), "detail": str(k2.get("estimatedCpc") or ""), "note": ""},
        "feed5": {"found": bool(k5.get("found")), "detail": str(k5.get("estimatedCpc") or ""), "note": ""},
        "feed4": {
            "found": bool(ax.get("found")),
            "detail": ax_extra,
            "note": ax_extra,
            "mode": ax_mode,
            "operator_hint": str(ax.get("operator_hint") or ""),
            "keitaro_offer_url": str(ax.get("keitaro_offer_url") or ""),
        },
        "feed3": {
            "found": bool(y_nc.get("found") or y_c.get("found")),
            "detail": f"nc={y_nc.get('estimatedCpc_amount', '')} {y_nc.get('estimatedCpc_currency', '')}".strip(),
            "note": yadore_feed_class(bool(y_nc.get("found")), bool(y_c.get("found"))),
        },
        "feed6": {
            "found": bool(sn_tile.get("found") or sn_coupons.get("found")),
            "detail": f"tile_epc={sn_tile.get('epc', '')} coupons_epc={sn_coupons.get('epc', '')}".strip(),
            "note": shopnomix_feed_class(bool(sn_tile.get("found")), bool(sn_coupons.get("found"))),
        },
        "effinity": {"found": None, "detail": "", "note": "no API check in KLblend yet"},
        "flexoffers": {"found": None, "detail": "", "note": "no API check in KLblend yet"},
    }

    seen: set[str] = set()
    results: List[FeedCheckResult] = []
    for fo in flow_offers:
        if fo.feed_key in seen:
            continue
        seen.add(fo.feed_key)
        info = by_feed.get(fo.feed_key) or {
            "found": None,
            "detail": "",
            "note": "unknown feed",
            "mode": "",
            "operator_hint": "",
            "keitaro_offer_url": "",
        }
        results.append(
            FeedCheckResult(
                feed_key=fo.feed_key,
                label=_FEED_LABELS.get(fo.feed_key, fo.feed_key),
                in_flow=True,
                share=fo.share,
                offer_name=fo.offer_name,
                found=info.get("found"),
                detail=str(info.get("detail") or ""),
                note=str(info.get("note") or ""),
                mode=str(info.get("mode") or ""),
                operator_hint=str(info.get("operator_hint") or ""),
                keitaro_offer_url=str(info.get("keitaro_offer_url") or ""),
            )
        )
    return results


def _adexa_golink_from_row(row: CampaignCheckmonRow) -> str:
    for fr in row.feed_results:
        if fr.mode == "smartlink" and fr.keitaro_offer_url:
            return fr.keitaro_offer_url.strip()
    return ""


def format_checkmon_report_block(row: CampaignCheckmonRow, *, ts: str, dry_run: bool = False) -> str:
    """Human-readable checkmon block (for notes or stdout)."""
    check_label = "Last checkmon dry-run" if dry_run else "Last checkmon"
    lines = [
        f"{check_label}: {ts} UTC",
        f"Campaign {row.campaign_id} ({row.campaign_name}) [{row.group}]",
        f"url: {row.url}",
        f"geo: {row.geo}",
    ]
    golink = _adexa_golink_from_row(row)
    if golink:
        lines.append(f"adexa_golink: {golink}")
    lines.extend(["", "Feed monetization (no weight changes):"])
    if row.error:
        lines.append(f"  ERROR: {row.error}")
        return "\n".join(lines)
    for fr in row.feed_results:
        if fr.found is True:
            status = "YES" if fr.mode != "smartlink" else "SMARTLINK"
        elif fr.found is False:
            status = "NO"
        else:
            status = "N/A"
        share_part = f"share={fr.share}%"
        extra = fr.detail or fr.note
        if extra:
            extra = f" ({extra})"
        lines.append(f"  {fr.label:10} {status:7}  {share_part:12}  {fr.offer_name}{extra}")
        if fr.mode == "smartlink" and fr.keitaro_offer_url:
            lines.append(f"             -> Keitaro golink offer: {fr.keitaro_offer_url}")
        if fr.operator_hint:
            lines.append(f"             -> {fr.operator_hint}")
    return "\n".join(lines)


def run_checkmon_audit_dry(
    client: Optional[KeitaroClient] = None,
    *,
    campaign_ids: Optional[List[int]] = None,
    require_url_geo: bool = True,
) -> Dict[str, Any]:
    """
    Run matchmaking-style monetization checks for feed-balance campaigns.

    Read-only — does not update Keitaro stream shares or notes.
    """
    c = client or KeitaroClient()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    campaigns = discover_feed_balance_campaigns(c)
    if campaign_ids is not None:
        want = {int(x) for x in campaign_ids}
        campaigns = [x for x in campaigns if int(x["id"]) in want]

    audit_rows: List[CampaignCheckmonRow] = []
    skipped_incomplete = 0

    for camp in campaigns:
        cid = int(camp["id"])
        try:
            full = c.get_campaign(cid)
        except KeitaroClientError as e:
            audit_rows.append(
                CampaignCheckmonRow(
                    campaign_id=cid,
                    campaign_name=str(camp.get("name") or ""),
                    group=str(camp.get("group") or ""),
                    url="",
                    geo="",
                    enabled="",
                    stream_id=0,
                    error=str(e),
                )
            )
            continue

        notes = str(full.get("notes") or "")
        cfg = parse_feed_balance_config(notes)
        if require_url_geo and (not cfg.url.strip() or not cfg.geo.strip()):
            skipped_incomplete += 1
            continue

        stream_id, flow_offers = list_regular_flow_feed_offers(c, cid)
        if not flow_offers:
            audit_rows.append(
                CampaignCheckmonRow(
                    campaign_id=cid,
                    campaign_name=str(full.get("name") or ""),
                    group=str(camp.get("group") or ""),
                    url=cfg.url,
                    geo=cfg.geo,
                    enabled=cfg.enabled,
                    stream_id=int(stream_id or 0),
                    error="no mapped feed offers on regular flow",
                )
            )
            continue

        try:
            feed_results = _feed_results_from_checkmon(cfg.url, cfg.geo, flow_offers)
        except Exception as e:
            audit_rows.append(
                CampaignCheckmonRow(
                    campaign_id=cid,
                    campaign_name=str(full.get("name") or ""),
                    group=str(camp.get("group") or ""),
                    url=cfg.url,
                    geo=cfg.geo,
                    enabled=cfg.enabled,
                    stream_id=int(stream_id or 0),
                    error=str(e),
                )
            )
            continue

        audit_rows.append(
            CampaignCheckmonRow(
                campaign_id=cid,
                campaign_name=str(full.get("name") or ""),
                group=str(camp.get("group") or ""),
                url=cfg.url,
                geo=cfg.geo,
                enabled=cfg.enabled,
                stream_id=int(stream_id or 0),
                feed_results=feed_results,
            )
        )

    flat: List[Dict[str, Any]] = []
    for row in audit_rows:
        if row.error and not row.feed_results:
            flat.append(
                {
                    "timestamp_utc": ts,
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "group": row.group,
                    "url": row.url,
                    "geo": row.geo,
                    "enabled": row.enabled,
                    "feed": "",
                    "share_pct": "",
                    "offer_name": "",
                    "monetized": "",
                    "detail": row.error,
                }
            )
            continue
        for fr in row.feed_results:
            flat.append(
                {
                    "timestamp_utc": ts,
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "group": row.group,
                    "url": row.url,
                    "geo": row.geo,
                    "enabled": row.enabled,
                    "feed": fr.label,
                    "share_pct": fr.share,
                    "offer_name": fr.offer_name,
                    "monetized": (
                        "yes"
                        if fr.found is True and fr.mode != "smartlink"
                        else "smartlink"
                        if fr.found is True and fr.mode == "smartlink"
                        else "no"
                        if fr.found is False
                        else "n/a"
                    ),
                    "detail": (fr.detail or fr.note or "").strip(),
                    "adexa_mode": fr.mode,
                    "keitaro_offer_url": fr.keitaro_offer_url,
                    "operator_hint": fr.operator_hint,
                }
            )

    return {
        "timestamp_utc": ts,
        "checked_campaigns": len(audit_rows),
        "skipped_incomplete_config": skipped_incomplete,
        "flat_rows": flat,
        "campaigns": audit_rows,
    }


def run_checkmon_update_notes(
    client: Optional[KeitaroClient] = None,
    *,
    campaign_ids: Optional[List[int]] = None,
    dry_run: bool = False,
    require_url_geo: bool = True,
) -> Dict[str, Any]:
    """
    Run per-feed monetization checks and write the report into campaign notes.

    Soft-launch scope: notes only — does not change stream shares or ``enabled``.
    """
    c = client or KeitaroClient()
    audit = run_checkmon_audit_dry(
        c,
        campaign_ids=campaign_ids,
        require_url_geo=require_url_geo,
    )
    ts = str(audit.get("timestamp_utc") or "")
    updated = 0
    skipped_no_config = 0
    skipped_unchanged = 0
    errors = 0
    rows: List[Dict[str, Any]] = []

    for row in audit.get("campaigns") or []:
        cid = int(row.campaign_id)
        if row.error and not row.feed_results:
            rows.append({"campaign_id": cid, "action": "check_error", "error": row.error})
            continue
        if require_url_geo and (not row.url.strip() or not row.geo.strip()):
            continue

        report = format_checkmon_report_block(row, ts=ts, dry_run=dry_run)
        try:
            full = c.get_campaign(cid)
        except KeitaroClientError as e:
            errors += 1
            rows.append({"campaign_id": cid, "action": "error", "error": str(e)})
            continue

        notes = str(full.get("notes") or "")
        if not has_feed_balance_config(notes):
            skipped_no_config += 1
            rows.append({"campaign_id": cid, "action": "skip_no_config"})
            continue

        new_notes = merge_notes_preserve_config(notes, report)
        if new_notes == notes:
            skipped_unchanged += 1
            rows.append({"campaign_id": cid, "action": "skip_unchanged"})
            continue

        if dry_run:
            updated += 1
            rows.append(
                {
                    "campaign_id": cid,
                    "action": "would_update",
                    "adexa_golink": _adexa_golink_from_row(row),
                }
            )
            continue

        try:
            c.update_campaign(cid, {"notes": new_notes})
            updated += 1
            rows.append(
                {
                    "campaign_id": cid,
                    "action": "updated",
                    "adexa_golink": _adexa_golink_from_row(row),
                }
            )
            logger.info("feed balance checkmon: updated notes for campaign %s", cid)
        except KeitaroClientError as e:
            errors += 1
            rows.append({"campaign_id": cid, "action": "error", "error": str(e)})

    return {
        **audit,
        "dry_run": dry_run,
        "notes_updated": updated,
        "notes_skipped_no_config": skipped_no_config,
        "notes_skipped_unchanged": skipped_unchanged,
        "notes_errors": errors,
        "notes_rows": rows,
    }


def write_checkmon_audit_csv(result: Dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = result.get("flat_rows") or []
    if not rows:
        path.write_text("timestamp_utc,campaign_id,campaign_name,group,url,geo,enabled,feed,share_pct,offer_name,monetized,detail\n", encoding="utf-8")
        return path
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return path

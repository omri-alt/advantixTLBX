"""
Assistance helpers: fetch Keitaro campaigns (e.g. test from UI) and clone one
to verify campaign creation / campaign_setup flow.
"""
import logging
from urllib.parse import quote
from typing import Any, Dict, List, Optional

from integrations.keitaro import KeitaroClient, KeitaroClientError
from config import KELKOO_ACCOUNT_ID, FEED1_KELKOO_ACCOUNT_ID

logger = logging.getLogger(__name__)


def get_campaigns_data(
    offset: int = 0,
    limit: int = 100,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch campaigns from Keitaro (e.g. your test campaign created in the UI).
    Returns list of campaign objects.
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    campaigns = client.get_campaigns(offset=offset, limit=limit)
    logger.info("Fetched %s campaigns (offset=%s, limit=%s)", len(campaigns), offset, limit)
    return campaigns


def find_campaign_by_alias_or_name(
    campaigns: List[Dict[str, Any]],
    alias: Optional[str] = None,
    name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return first campaign whose alias or name matches (case-insensitive)."""
    alias_lower = (alias or "").strip().lower()
    name_lower = (name or "").strip().lower()
    for c in campaigns:
        if alias_lower and (c.get("alias") or "").lower() == alias_lower:
            return c
        if name_lower and (c.get("name") or "").lower() == name_lower:
            return c
    return None


def clone_campaign_copy(
    campaign_id: int,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a copy of a campaign by id (POST clone endpoint).
    Use this to verify campaign creation by cloning your campaign_setup test campaign.
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    result = client.clone_campaign(campaign_id)
    logger.info("Cloned campaign id=%s -> new id=%s", campaign_id, result.get("id"))
    return result


def get_campaigns_then_clone_setup(
    alias_or_name: str = "campaign_setup",
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Assistance flow: fetch all campaigns, find one by alias/name (e.g. your test
    campaign_setup), then clone it. Returns the cloned campaign.
    Raises if no matching campaign or clone fails.
    """
    campaigns = get_campaigns_data(base_url=base_url, api_key=api_key)
    campaign = find_campaign_by_alias_or_name(campaigns, alias=alias_or_name, name=alias_or_name)
    if not campaign:
        raise ValueError(
            f"No campaign with alias or name '{alias_or_name}'. "
            f"Found {len(campaigns)} campaigns: {[c.get('alias') or c.get('name') for c in campaigns]}"
        )
    cid = campaign.get("id")
    if cid is None:
        raise ValueError(f"Campaign has no id: {campaign}")
    return clone_campaign_copy(int(cid), base_url=base_url, api_key=api_key)


def get_offers_data(
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all offers from Keitaro. Returns list of offer objects.
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    offers = client.get_offers()
    logger.info("Fetched %s offers", len(offers))
    return offers


def get_full_setup(
    campaign_alias_or_name: str,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch your current setup: one campaign (by alias/name), its flows, and all offers.
    Use the returned payloads to see real values for create/update.
    """
    campaigns = get_campaigns_data(base_url=base_url, api_key=api_key)
    campaign = find_campaign_by_alias_or_name(
        campaigns, alias=campaign_alias_or_name, name=campaign_alias_or_name
    )
    if not campaign:
        raise ValueError(
            f"No campaign with alias or name '{campaign_alias_or_name}'. "
            f"Found: {[c.get('alias') or c.get('name') for c in campaigns]}"
        )
    cid = campaign.get("id")
    if cid is None:
        raise ValueError(f"Campaign has no id: {campaign}")
    streams = get_campaign_streams(int(cid), base_url=base_url, api_key=api_key)
    offers = get_offers_data(base_url=base_url, api_key=api_key)
    return {
        "campaign": campaign,
        "streams": streams,
        "offers": offers,
    }


def get_campaign_streams(
    campaign_id: int,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get traffic flows (streams) for a campaign.
    Returns list of stream objects (id, type, name, position, weight, filters, triggers, landings, offers, ...).
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    streams = client.get_streams(campaign_id)
    logger.info("Fetched %s streams for campaign_id=%s", len(streams), campaign_id)
    return streams


def get_campaign_streams_by_alias(
    alias_or_name: str,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Find campaign by alias/name, then get its flows (streams).
    Use this for the test campaign we already have in the app.
    """
    campaigns = get_campaigns_data(base_url=base_url, api_key=api_key)
    campaign = find_campaign_by_alias_or_name(campaigns, alias=alias_or_name, name=alias_or_name)
    if not campaign:
        raise ValueError(
            f"No campaign with alias or name '{alias_or_name}'. "
            f"Found {len(campaigns)} campaigns: {[c.get('alias') or c.get('name') for c in campaigns]}"
        )
    cid = campaign.get("id")
    if cid is None:
        raise ValueError(f"Campaign has no id: {campaign}")
    return get_campaign_streams(int(cid), base_url=base_url, api_key=api_key)


try:
    from geos import SUPPORTED_GEOS, is_supported_geo, normalize_geo
except ImportError:
    SUPPORTED_GEOS = []
    def is_supported_geo(code: str) -> bool:
        return True
    def normalize_geo(code: str) -> str:
        return (code or "").strip().lower()


def _geo_for_api(code: str) -> str:
    """Normalize geo for our list (lowercase), then return uppercase for Keitaro filter payload."""
    c = normalize_geo(code)
    return c.upper() if c else ""


def add_country_flow(
    campaign_id: int,
    country_code: str,
    flow_name: str,
    offer_ids: Optional[List[int]] = None,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    position: Optional[int] = None,
    weight: int = 100,
    skip_if_exists: bool = True,
) -> Dict[str, Any]:
    """
    Add a flow like the Spain flow: country filter + offers (equal share).
    Uses schema "landings", action_type "http", type "regular".
    Country code from your geos list (e.g. uk, es, fr). If offer_ids is None, uses all offers.
    If skip_if_exists is True and a flow with the same name already exists in the campaign, returns
    that flow with "_skipped": True and does not create a duplicate.
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    if skip_if_exists:
        existing = client.get_streams(int(campaign_id))
        name_lower = (flow_name or "").strip().lower()
        for s in existing:
            if (s.get("name") or "").strip().lower() == name_lower:
                logger.info("Flow %r already exists (id=%s), skipping create", flow_name, s.get("id"))
                out = dict(s)
                out["_skipped"] = True
                return out
    geo_code = _geo_for_api(country_code)
    if not geo_code:
        raise ValueError(f"Invalid country code {country_code!r}. Supported: {SUPPORTED_GEOS}")
    if SUPPORTED_GEOS and normalize_geo(country_code) not in SUPPORTED_GEOS:
        logger.warning("Country code %r not in supported geos list: %s", country_code, SUPPORTED_GEOS)
    if offer_ids is None:
        offers_list = client.get_offers()
        if not offers_list:
            raise ValueError("No offers in tracker; create an offer first or pass offer_ids")
        offer_ids = [int(o.get("id")) for o in offers_list if o.get("id") is not None][:10]
    if not offer_ids:
        raise ValueError("No offer_ids to attach")
    # Equal share (integers that sum to 100)
    n = len(offer_ids)
    base_share = 100 // n
    remainder = 100 % n
    shares = [base_share + (1 if i < remainder else 0) for i in range(n)]
    offer_entries = [
        {"offer_id": oid, "state": "active", "share": share}
        for oid, share in zip(offer_ids, shares)
    ]
    # Filter: some Keitaro versions expect payload as string for create; response may return array
    filter_payload = [geo_code]
    payload = {
        "campaign_id": int(campaign_id),
        "type": "regular",
        "name": flow_name,
        "schema": "landings",
        "action_type": "http",
        "state": "active",
        "weight": weight,
        "filter_or": False,
        "collect_clicks": True,
        "offer_selection": "before_click",
        "filters": [
            {"name": "country", "mode": "accept", "payload": filter_payload}
        ],
        "offers": offer_entries,
    }
    if position is not None:
        payload["position"] = position
    result = client.create_stream(payload)
    logger.info("Created flow %s (country=%s, geo=%s) id=%s", flow_name, country_code, geo_code, result.get("id"))
    return result


def set_flow_country_filter(
    stream_id: int,
    country_code: str,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Set or fix the country filter on an existing flow. Use your geo code (e.g. uk, es).
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    geo_code = _geo_for_api(country_code)
    payload = {
        "filters": [
            {"name": "country", "mode": "accept", "payload": [geo_code]}
        ]
    }
    return client.update_stream(stream_id, payload)


# --- Geo offers: 3 offers per country (action_payload with geo + productUrl) ---

def build_offer_action_payload(
    geo: str,
    product_url: str,
    account_id: Optional[str] = None,
    feed: int = 1,
) -> str:
    """
    Build action_payload URL for a Kelkoo offer.
    feed=1: permanentLinkGo (Kelkoo) with account_id, {var10}, {subid}.
    feed=2: same rain shell as feed1-style Blend URLs; inner target is ``sidehustlerbaby.com/klk-merchant``
    with literal ``geo`` + URL-encoded ``merchantUrl`` + ``pub_click_id={subid}`` (Keitaro macro), matching
    ``https://shopli.city/rainotest?rain=https://sidehustlerbaby.com/klk-merchant?geo=...&merchantUrl=...``.
    """
    geo = (geo or "").strip().lower()[:2]
    encoded = quote(product_url or "https://example.com/placeholder", safe="")
    if feed == 2:
        return (
            "https://shopli.city/rainotest?rain=https://sidehustlerbaby.com/klk-merchant"
            f"?geo={geo}&merchantUrl={encoded}&pub_click_id={{subid}}"
        )
    acc = (account_id or FEED1_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID).strip()
    return (
        f"https://shopli.city/rainotest?rain=https://{geo}-go.kelkoogroup.net/permanentLinkGo"
        f"?country={geo}&id={acc}&merchantUrl={encoded}"
        f"&publisherSubId=shoplicity&ctrl_ab={{var10}}&publisherClickId={{subid}}"
    )


def ensure_geo_offers(
    geo: str,
    product_urls: Optional[List[str]] = None,
    *,
    account_id: Optional[str] = None,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    skip_if_exists: bool = True,
) -> List[int]:
    """
    Ensure 3 offers exist for this geo: {geo}_product1, {geo}_product2, {geo}_product3.
    Uses product_urls[0..2] for action_payload; if None, uses placeholders (update later).
    Returns list of 3 offer IDs in order.
    """
    geo = (geo or "").strip().lower()
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    defaults = [
        "https://example.com/placeholder_product1",
        "https://example.com/placeholder_product2",
        "https://example.com/placeholder_product3",
    ]
    urls = (product_urls or defaults)[:3]
    while len(urls) < 3:
        urls.append(defaults[len(urls)])
    names = [f"{geo}_product1", f"{geo}_product2", f"{geo}_product3"]
    existing = {o.get("name"): o for o in client.get_offers() if o.get("name")}
    ids = []
    for i, (name, product_url) in enumerate(zip(names, urls)):
        if skip_if_exists and name in existing:
            ids.append(int(existing[name]["id"]))
            logger.debug("Offer %s already exists id=%s", name, existing[name]["id"])
            continue
        action_payload = build_offer_action_payload(geo, product_url, account_id)
        payload = {
            "name": name,
            "action_type": "http",
            "action_payload": action_payload,
            "offer_type": "external",
            "affiliate_network_id": 0,
            "group_id": 0,
            "state": "active",
            "payout_value": 0,
            "payout_currency": "USD",
            "payout_type": "CPA",
            "payout_auto": True,
            "payout_upsell": True,
        }
        created = client.create_offer(payload)
        oid = created.get("id")
        if oid is None:
            raise ValueError(f"Create offer {name} did not return id: {created}")
        ids.append(int(oid))
        logger.info("Created offer %s id=%s", name, oid)
    return ids


def get_geo_offers_sorted(
    geo: str,
    *,
    feed_prefix: Optional[str] = None,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return offers for this geo (name feed1_uk_productN or uk_productN) sorted by product number."""
    geo = (geo or "").strip().lower()
    name_prefix = f"{feed_prefix}_{geo}_product" if feed_prefix else f"{geo}_product"
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    out = []
    for o in client.get_offers():
        name = (o.get("name") or "").strip()
        if name.startswith(name_prefix):
            suffix = name[len(name_prefix):]
            if suffix.isdigit():
                out.append((int(suffix), o))
    out.sort(key=lambda x: x[0])
    return [o for _, o in out]


def update_offer_action_payload(
    offer_id: int,
    action_payload: str,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Update an offer's action_payload (e.g. with new merchant URL)."""
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    return client.update_offer(offer_id, {"action_payload": action_payload})


def archive_offer(
    offer_id: int,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Archive an offer (moves to archive, does not permanently delete)."""
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    return client.archive_offer(offer_id)


def delete_offer(
    offer_id: int,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> None:
    """Permanently delete an offer (remove from flows first)."""
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    client.delete_offer(offer_id)


def remove_offer_best_effort(
    offer_id: int,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> bool:
    """
    Try several Keitaro API shapes to delete or archive an offer (already detached from flows).
    Returns True if the tracker accepted the removal.
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    return client.remove_offer_best_effort(offer_id)


def get_geo_offer_numbers(
    geo: str,
    *,
    feed_prefix: Optional[str] = None,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[int]:
    """Return sorted product numbers for this geo (e.g. feed1_uk_product1, feed1_uk_product2)."""
    geo = (geo or "").strip().lower()
    name_prefix = f"{feed_prefix}_{geo}_product" if feed_prefix else f"{geo}_product"
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    numbers = []
    for o in client.get_offers():
        name = (o.get("name") or "").strip()
        if name.startswith(name_prefix):
            suffix = name[len(name_prefix):]
            if suffix.isdigit():
                numbers.append(int(suffix))
    return sorted(numbers)


def create_next_geo_offers(
    geo: str,
    count: int = 3,
    *,
    feed_prefix: Optional[str] = None,
    account_id: Optional[str] = None,
    feed: int = 1,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    product_urls: Optional[List[str]] = None,
) -> List[int]:
    """
    Create the next N offers for this geo. Names: feed1_uk_productN or uk_productN if no feed_prefix.
    Returns their IDs.
    """
    geo = (geo or "").strip().lower()
    existing_nums = get_geo_offer_numbers(geo, feed_prefix=feed_prefix, base_url=base_url, api_key=api_key)
    start = (max(existing_nums) + 1) if existing_nums else 1
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    name_prefix = f"{feed_prefix}_{geo}_product" if feed_prefix else f"{geo}_product"
    defaults = [
        f"https://example.com/placeholder_{geo}_product{i}"
        for i in range(start, start + count)
    ]
    urls = (product_urls or defaults)[:count]
    while len(urls) < count:
        urls.append(defaults[len(urls)])
    names = [f"{name_prefix}{i}" for i in range(start, start + count)]
    ids = []
    for name, product_url in zip(names, urls):
        action_payload = build_offer_action_payload(geo, product_url, account_id=account_id, feed=feed)
        payload = {
            "name": name,
            "action_type": "http",
            "action_payload": action_payload,
            "offer_type": "external",
            "affiliate_network_id": 0,
            "group_id": 0,
            "state": "active",
            "payout_value": 0,
            "payout_currency": "USD",
            "payout_type": "CPA",
            "payout_auto": True,
            "payout_upsell": True,
        }
        created = client.create_offer(payload)
        oid = created.get("id")
        if oid is None:
            raise ValueError(f"Create offer {name} did not return id: {created}")
        ids.append(int(oid))
        logger.info("Created offer %s id=%s", name, oid)
    return ids


def stream_offer_ids(stream: Dict[str, Any]) -> List[int]:
    """Return list of offer IDs attached to this stream (from stream['offers'])."""
    offers = stream.get("offers") or []
    ids = []
    for o in offers:
        oid = o.get("offer_id")
        if oid is not None:
            ids.append(int(oid))
    return ids


def set_flow_offers(
    stream_id: int,
    offer_ids: List[int],
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Set a flow's offers to the given list with equal share (33, 33, 34 or 100/len)."""
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    n = len(offer_ids)
    base_share = 100 // n
    remainder = 100 % n
    shares = [base_share + (1 if i < remainder else 0) for i in range(n)]
    offers = [
        {"offer_id": oid, "state": "active", "share": share}
        for oid, share in zip(offer_ids, shares)
    ]
    return client.update_stream(stream_id, {"offers": offers})


def _shares_from_weights(weights: List[float]) -> List[int]:
    """
    Convert weights into integer shares that sum to 100.
    Uses largest-remainder apportionment. If n<=100, guarantees each item gets at least 1 share.
    """
    n = len(weights)
    if n == 0:
        return []
    w = [max(0.0, float(x)) for x in weights]
    if n <= 100:
        base = [1] * n
        remaining = 100 - n
        total = sum(w) or float(n)
        raw = [(wi / total) * remaining for wi in w]
        floors = [int(x) for x in raw]
        rema = [x - int(x) for x in raw]
        shares = [b + f for b, f in zip(base, floors)]
        leftover = remaining - sum(floors)
    else:
        total = sum(w)
        if total <= 0:
            # Too many offers to give everyone >=1; split as evenly as possible
            base_share = 100 // n
            rem = 100 % n
            return [base_share + (1 if i < rem else 0) for i in range(n)]
        raw = [(wi / total) * 100 for wi in w]
        shares = [int(x) for x in raw]
        rema = [x - int(x) for x in raw]
        leftover = 100 - sum(shares)

    if leftover > 0:
        order = sorted(range(n), key=lambda i: rema[i], reverse=True)
        for i in order[:leftover]:
            shares[i] += 1

    # Guard: sum must be 100 (fix any rounding drift)
    s = sum(shares)
    if s != 100 and n > 0:
        diff = 100 - s
        shares[0] += diff
    return shares


def set_flow_offers_weighted(
    stream_id: int,
    offer_id_to_weight: Dict[int, float],
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Set a flow's offers with weighted shares based on provided weights (e.g. clickCap).
    Shares are integers summing to 100.
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    items = [(int(oid), float(w)) for oid, w in offer_id_to_weight.items() if oid is not None]
    if not items:
        raise ValueError("No offers to attach")
    items.sort(key=lambda x: x[0])
    offer_ids = [oid for oid, _ in items]
    weights = [w for _, w in items]
    shares = _shares_from_weights(weights)
    offers = [
        {"offer_id": oid, "state": "active", "share": share}
        for oid, share in zip(offer_ids, shares)
    ]
    return client.update_stream(stream_id, {"offers": offers})


def flow_name_to_geo(flow_name: str) -> Optional[str]:
    """Return geo code for a flow name (e.g. Spain -> es, or es -> es). Uses GEO_LABELS."""
    try:
        from geos import GEO_LABELS, SUPPORTED_GEOS
    except ImportError:
        return None
    name_lower = (flow_name or "").strip().lower()
    if name_lower in SUPPORTED_GEOS:
        return name_lower
    for geo, label in GEO_LABELS.items():
        if (label or "").strip().lower() == name_lower:
            return geo
    return None


if __name__ == "__main__":
    """CLI: fetch campaigns, streams, or clone. Examples:
      python assistance.py                         # get all campaigns
      python assistance.py streams campaign_setup  # get flows for campaign
      python assistance.py clone campaign_setup    # find 'campaign_setup', clone it
    """
    import json
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) >= 3 and sys.argv[1].lower() == "clone":
        alias_or_name = sys.argv[2]
        try:
            out = get_campaigns_then_clone_setup(alias_or_name=alias_or_name)
            print(json.dumps(out, indent=2))
        except (ValueError, KeitaroClientError) as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
    elif len(sys.argv) >= 3 and sys.argv[1].lower() == "streams":
        alias_or_name = sys.argv[2]
        try:
            streams = get_campaign_streams_by_alias(alias_or_name=alias_or_name)
            print(json.dumps({"streams": streams, "count": len(streams)}, indent=2))
        except (ValueError, KeitaroClientError) as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
    else:
        campaigns = get_campaigns_data()
        print(json.dumps({"campaigns": campaigns, "count": len(campaigns)}, indent=2))

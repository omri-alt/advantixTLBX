"""
Assistance helpers: fetch Keitaro campaigns (e.g. test from UI) and clone one
to verify campaign creation / campaign_setup flow.
"""
import logging
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Tuple

from integrations.keitaro import KeitaroClient, KeitaroClientError
from config import (
    KELKOO_ACCOUNT_ID,
    KELKOO_ACCOUNT_ID_2,
    FEED1_KELKOO_ACCOUNT_ID,
    FEED2_KELKOO_ACCOUNT_ID,
    FEED5_KELKOO_ACCOUNT_ID,
    FEED5_KELKOO_PUBLISHER_SUB_ID,
)

logger = logging.getLogger(__name__)


def get_campaigns_data(
    offset: int = 0,
    limit: int = 100,
    *,
    all_pages: Optional[bool] = None,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch campaigns from Keitaro.

    By default (offset=0, limit=100) returns **all** campaigns via pagination so
    legacy aliases (e.g. HrQBXp) are not missed when the tracker has >100 rows.
    Pass ``all_pages=False`` for a single API page (assistance API with offset/limit).
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    if all_pages is None:
        all_pages = offset == 0 and limit == 100
    if all_pages:
        campaigns = client.list_all_campaigns(page_size=max(limit, 100))
        logger.info("Fetched %s campaigns (all pages)", len(campaigns))
        return campaigns
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

def build_kelkoo_feed5_action_payload(geo: str, merchant_or_product_url: str) -> str:
    """
    Kelkoo feed 5 Keitaro offer URL (intentix) for Nipuhim feed5 and Blend kelkoo5 rows.

    Same path shape as the reference offer ``KL Feed 5``, but ``merchantUrl`` is the
    URL-encoded product or merchant page for this offer (like feed1), not a Keitaro macro.
    """
    geo = (geo or "").strip().lower()[:2]
    if not geo:
        raise ValueError("geo is required for Kelkoo feed5 offers")
    acc = (FEED5_KELKOO_ACCOUNT_ID or "").strip()
    if not acc:
        raise ValueError("FEED5_KELKOO_ACCOUNT_ID is required for Kelkoo feed5 offers")
    pub = (FEED5_KELKOO_PUBLISHER_SUB_ID or "intentix").strip()
    encoded = quote(merchant_or_product_url or "https://example.com/placeholder", safe="")
    return (
        f"https://{geo}-go.kelkoogroup.net/permanentLinkGo"
        f"?country={geo}&id={acc}"
        f"&merchantUrl={encoded}&publisherSubId={pub}"
        f"&ctrl_ab={{sub_id_10}}&publisherClickId={{subid}}"
    )


def build_nipuhim_feed5_action_payload(geo: str, merchant_or_product_url: str) -> str:
    """Alias for Nipuhim feed5 sync (same template as Blend kelkoo5)."""
    return build_kelkoo_feed5_action_payload(geo, merchant_or_product_url)


def kelkoo_keitaro_action_payload(geo: str, merchant_url: str, feed_tag: str) -> str:
    """
    Keitaro offer URL for Kelkoo Blend kelkoo1/kelkoo2 (shopli rain + encoded merchantUrl).
    kelkoo5 uses ``build_kelkoo_feed5_action_payload()`` (intentix path + encoded merchantUrl).
    """
    ft = (feed_tag or "").strip().lower()
    if ft in ("kelkoo5", "feed5", "5"):
        return build_kelkoo_feed5_action_payload(geo, merchant_url)
    if ft in ("kelkoo2", "feed2", "2"):
        acc = (FEED2_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID_2 or "").strip()
        return build_offer_action_payload(geo, merchant_url, account_id=acc, feed=2)
    acc = (FEED1_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID or "").strip()
    return build_offer_action_payload(geo, merchant_url, account_id=acc, feed=1)


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
        if feed_prefix == "feed5":
            action_payload = build_kelkoo_feed5_action_payload(geo, product_url)
        else:
            action_payload = build_offer_action_payload(
                geo, product_url, account_id=account_id, feed=feed
            )
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


def _split_evenly_shares(pct: int, n: int) -> List[int]:
    """Split ``pct`` integer share points across ``n`` offers (largest-remainder)."""
    if n <= 0 or pct <= 0:
        return [0] * n
    base = pct // n
    rem = pct - base * n
    return [base + (1 if i < rem else 0) for i in range(n)]


def set_flow_offers_multi_feed_split(
    stream_id: int,
    feed_buckets: List[Tuple[List[int], int]],
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Set a flow's offers so each non-empty feed bucket gets its share of traffic.

    ``feed_buckets`` is a list of ``(offer_ids, target_pct)`` in display order.
    Empty offer lists are skipped; remaining buckets are scaled so their target
    percentages sum to 100. Per-feed offers split their bucket evenly.
    """
    active: List[Tuple[List[int], int]] = [
        ([int(o) for o in ids], int(pct))
        for ids, pct in feed_buckets
        if ids
    ]
    if not active:
        raise ValueError("No offers to attach")
    if len(active) == 1:
        return set_flow_offers(
            stream_id, active[0][0], base_url=base_url, api_key=api_key
        )
    total_pct = sum(pct for _, pct in active)
    if total_pct <= 0:
        raise ValueError("feed bucket percentages must be positive")
    scaled: List[Tuple[List[int], int]] = []
    remainder = 100
    for i, (ids, pct) in enumerate(active):
        if i == len(active) - 1:
            bucket = remainder
        else:
            bucket = int(round(100 * pct / total_pct))
            remainder -= bucket
        scaled.append((ids, max(0, bucket)))
    fix = 100 - sum(b for _, b in scaled)
    if fix and scaled:
        ids0, b0 = scaled[0]
        scaled[0] = (ids0, b0 + fix)

    offers: List[Dict[str, Any]] = []
    for ids, bucket in scaled:
        if bucket <= 0:
            continue
        shares = _split_evenly_shares(bucket, len(ids))
        offers.extend(
            {"offer_id": int(oid), "state": "active", "share": s}
            for oid, s in zip(ids, shares)
        )
    if not offers:
        raise ValueError("No offers to attach")
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    return client.update_stream(stream_id, {"offers": offers})


def set_flow_offers_two_feed_split(
    stream_id: int,
    feed1_offer_ids: List[int],
    feed2_offer_ids: List[int],
    feed1_pct: int,
    feed2_pct: int,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Set a flow's offers so feed1 offers aggregate to ``feed1_pct`` and feed2 offers
    to ``feed2_pct`` (must sum to 100). Offers within each feed split that feed's
    bucket as evenly as possible (largest-remainder on integer shares).

    Nipuhim two-feed helper; see ``set_flow_offers_multi_feed_split`` for 3+ feeds.
    """
    if feed1_pct + feed2_pct != 100:
        raise ValueError(
            f"feed1_pct + feed2_pct must equal 100, got {feed1_pct}+{feed2_pct}"
        )
    return set_flow_offers_multi_feed_split(
        stream_id,
        [
            (feed1_offer_ids, feed1_pct),
            (feed2_offer_ids, feed2_pct),
        ],
        base_url=base_url,
        api_key=api_key,
    )


def set_flow_offers_weighted_keep_zeros(
    stream_id: int,
    offer_id_to_weight: Dict[int, float],
    zero_offer_ids: Optional[List[int]] = None,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Like ``set_flow_offers_weighted``, but also keeps a list of additional offer IDs
    attached to the flow with ``share=0`` (no traffic, but still attached).

    Useful for "demonetized but kept in flow" cases — operators can re-enable an
    offer by giving it a clickCap again on the next sync, without re-attaching.

    ``zero_offer_ids`` entries that are already present in ``offer_id_to_weight`` are
    ignored (the weighted share takes precedence).
    """
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    items = [(int(oid), float(w)) for oid, w in offer_id_to_weight.items() if oid is not None]
    items.sort(key=lambda x: x[0])
    weighted_ids = {oid for oid, _ in items}
    extra_zero_ids = [int(oid) for oid in (zero_offer_ids or []) if oid is not None and int(oid) not in weighted_ids]
    if not items and not extra_zero_ids:
        raise ValueError("No offers to attach")
    offers: List[Dict[str, Any]] = []
    if items:
        weights = [w for _, w in items]
        shares = _shares_from_weights(weights)
        offer_ids = [oid for oid, _ in items]
        for oid, share in zip(offer_ids, shares):
            offers.append({"offer_id": oid, "state": "active", "share": share})
    for oid in sorted(set(extra_zero_ids)):
        offers.append({"offer_id": oid, "state": "active", "share": 0})
    return client.update_stream(stream_id, {"offers": offers})


def blend_device_stream_name(geo: str, channel: str) -> str:
    """Keitaro flow name for Blend device split: ``de_desktop``, ``de_mobile``."""
    g = normalize_geo(geo)
    ch = (channel or "").strip().lower()
    if ch == "desktop":
        return f"{g}_desktop"
    if ch == "mobile":
        return f"{g}_mobile"
    raise ValueError(f"Invalid blend device channel {channel!r}")


def _flow_name_to_geo_label_only(flow_name: str) -> Optional[str]:
    """Map flow name to geo via SUPPORTED_GEOS or GEO_LABELS (no device suffix parsing)."""
    try:
        from geos import GEO_LABELS, SUPPORTED_GEOS as _GEOS
    except ImportError:
        return None
    name_lower = (flow_name or "").strip().lower()
    if name_lower in _GEOS:
        return name_lower
    for geo, label in GEO_LABELS.items():
        if (label or "").strip().lower() == name_lower:
            return geo
    return None


def parse_blend_stream_geo_channel(flow_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a Blend campaign stream name into (geo, channel).

    channel is ``desktop``, ``mobile``, or ``legacy`` (undivided geo flow).
    """
    name = (flow_name or "").strip()
    if not name:
        return None, None
    lower = name.lower()
    for suffix, channel in (("_desktop", "desktop"), ("_mobile", "mobile")):
        if lower.endswith(suffix):
            base = lower[: -len(suffix)]
            if base and (not SUPPORTED_GEOS or base in SUPPORTED_GEOS):
                return base, channel
    geo = _flow_name_to_geo_label_only(name)
    if geo:
        return geo, "legacy"
    return None, None


def flow_name_to_geo(flow_name: str) -> Optional[str]:
    """Return geo code for a flow name (e.g. Spain -> es, or es -> es). Uses GEO_LABELS."""
    geo, _ch = parse_blend_stream_geo_channel(flow_name)
    return geo


def _normalize_filter_payload(payload: Any) -> List[str]:
    if not isinstance(payload, list):
        return []
    return [(str(p) or "").strip().lower() for p in payload if str(p or "").strip()]


def _blend_filter_specs(geo_code: str, channel: str) -> List[Dict[str, Any]]:
    """
    Filter rows for a Blend device stream (no ids).

    Mobile (flow 142): device_type IS NOT desktop + country IS geo (2 filters, AND).
    Desktop: country IS geo + device_type IS desktop (2 filters).
    """
    from integrations.blend_device import KEITARO_DEVICE_DESKTOP

    ch = (channel or "").strip().lower()
    country = {"name": "country", "mode": "accept", "payload": [geo_code]}
    if ch == "desktop":
        return [
            country,
            {"name": "device_type", "mode": "accept", "payload": list(KEITARO_DEVICE_DESKTOP)},
        ]
    if ch == "mobile":
        return [
            {
                "name": "device_type",
                "mode": "reject",
                "payload": list(KEITARO_DEVICE_DESKTOP),
            },
            country,
        ]
    raise ValueError(f"Invalid device channel {channel!r}")


def _assign_blend_filter_ids(
    specs: List[Dict[str, Any]],
    current_filters: List[Dict[str, Any]],
    channel: str,
) -> List[Dict[str, Any]]:
    """Attach Keitaro filter ids from the existing stream where possible."""
    country_ex: Optional[Dict[str, Any]] = None
    device_reject_desktop_id: Optional[int] = None
    device_accept_desktop_id: Optional[int] = None
    for f in current_filters or []:
        name = (f.get("name") or "").strip().lower()
        if name == "country":
            country_ex = f
        elif name == "device_type":
            fid = f.get("id")
            if fid is None:
                continue
            mode = (f.get("mode") or "").strip().lower()
            payload = _normalize_filter_payload(f.get("payload"))
            if payload == ["desktop"] and mode == "reject":
                device_reject_desktop_id = int(fid)
            elif payload == ["desktop"] and mode == "accept":
                device_accept_desktop_id = int(fid)

    ch = (channel or "").strip().lower()
    out: List[Dict[str, Any]] = []
    for spec in specs:
        row = dict(spec)
        sname = (spec.get("name") or "").strip().lower()
        smode = (spec.get("mode") or "").strip().lower()
        spayload = _normalize_filter_payload(spec.get("payload"))
        if sname == "country" and country_ex and country_ex.get("id") is not None:
            row["id"] = int(country_ex["id"])
        elif sname == "device_type":
            if ch == "desktop" and smode == "accept" and device_accept_desktop_id is not None:
                row["id"] = device_accept_desktop_id
            elif ch == "mobile" and smode == "reject" and device_reject_desktop_id is not None:
                row["id"] = device_reject_desktop_id
        out.append(row)
    return out


def assert_blend_stream_filters_sane(
    filters: List[Dict[str, Any]],
    channel: str,
    *,
    geo_code: str,
) -> None:
    """Raise if filters do not match the expected Blend device-stream shape."""
    ch = (channel or "").strip().lower()
    if ch == "desktop":
        if len(filters) != 2:
            raise ValueError(f"desktop stream expected 2 filters, got {len(filters)}")
    elif ch == "mobile":
        if len(filters) != 2:
            raise ValueError(f"mobile stream expected 2 filters, got {len(filters)}")
    else:
        raise ValueError(f"Invalid channel {channel!r}")

    countries = [f for f in filters if (f.get("name") or "").lower() == "country"]
    if len(countries) != 1:
        raise ValueError("expected exactly one country filter")
    want_country = [(geo_code or "").strip().upper()]
    if _normalize_filter_payload(countries[0].get("payload")) != [
        c.lower() for c in want_country
    ]:
        raise ValueError(f"country payload expected {want_country!r}")

    device_rows = [f for f in filters if (f.get("name") or "").lower() == "device_type"]
    if len(device_rows) != 1:
        raise ValueError(f"expected exactly one device_type filter, got {len(device_rows)}")
    if ch == "desktop":
        if (device_rows[0].get("mode") or "").lower() != "accept":
            raise ValueError("desktop device_type must be mode accept (IS)")
        if _normalize_filter_payload(device_rows[0].get("payload")) != ["desktop"]:
            raise ValueError("desktop device_type payload mismatch")
        return

    if (device_rows[0].get("mode") or "").lower() != "reject":
        raise ValueError("mobile device_type must be mode reject (IS NOT)")
    if _normalize_filter_payload(device_rows[0].get("payload")) != ["desktop"]:
        raise ValueError("mobile device_type must reject desktop")


def ensure_blend_device_stream(
    campaign_id: int,
    geo: str,
    channel: str,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    skip_if_exists: bool = True,
) -> Dict[str, Any]:
    """
    Country + device_type filtered flow for Blend Option B (one offer, split streams).
    """
    from integrations.blend_device import KEITARO_DEVICE_DESKTOP  # noqa: F401

    client = KeitaroClient(base_url=base_url, api_key=api_key)
    flow_name = blend_device_stream_name(geo, channel)
    if skip_if_exists:
        name_lower = flow_name.lower()
        for s in client.get_streams(int(campaign_id)):
            if (s.get("name") or "").strip().lower() == name_lower:
                out = dict(s)
                out["_skipped"] = True
                sid = out.get("id")
                if sid is not None:
                    set_blend_device_stream_filters(
                        int(sid), geo, channel, base_url=base_url, api_key=api_key
                    )
                return out
    geo_code = _geo_for_api(geo)
    if not geo_code:
        raise ValueError(f"Invalid country code {geo!r}")
    filters = _blend_filter_specs(geo_code, channel)
    payload = {
        "campaign_id": int(campaign_id),
        "type": "regular",
        "name": flow_name,
        "schema": "landings",
        "action_type": "http",
        "state": "active",
        "weight": 100,
        "filter_or": False,
        "collect_clicks": True,
        "offer_selection": "before_click",
        "filters": filters,
        "offers": [],
    }
    result = client.create_stream(payload)
    logger.info(
        "Created Blend device flow %s (geo=%s channel=%s) id=%s",
        flow_name,
        geo,
        channel,
        result.get("id"),
    )
    return result


def _blend_stream_filters_for_update(
    client: KeitaroClient,
    stream_id: int,
    geo: str,
    channel: str,
) -> List[Dict[str, Any]]:
    """
    Build filter rows for PUT ``streams/{id}``, preserving existing filter ids.

    Mobile streams: device_type IS NOT desktop + country IS geo (flow 142 ``ch_mobile``).
    """
    geo_code = _geo_for_api(geo)
    if not geo_code:
        raise ValueError(f"Invalid country code {geo!r}")
    resp = client._session.get(client._api_path(f"streams/{int(stream_id)}"), timeout=30)
    if not resp.ok:
        raise KeitaroClientError(
            f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text
        )
    current = resp.json() if resp.content else {}
    specs = _blend_filter_specs(geo_code, channel)
    out = _assign_blend_filter_ids(specs, current.get("filters") or [], channel)
    assert_blend_stream_filters_sane(out, channel, geo_code=geo_code)
    return out


def set_blend_device_stream_filters(
    stream_id: int,
    geo: str,
    channel: str,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Ensure country + device_type filters on an existing Blend device stream."""
    client = KeitaroClient(base_url=base_url, api_key=api_key)
    filters = _blend_stream_filters_for_update(client, int(stream_id), geo, channel)
    return client.update_stream(int(stream_id), {"filters": filters})


def refresh_all_blend_device_stream_filters(
    campaign_id: int,
    *,
    only_geo: Optional[str] = None,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Tuple[int, List[str]]:
    """
    Re-apply country + device_type filters on every ``{geo}_desktop`` / ``{geo}_mobile`` flow.
    Repairs flows that were created with outdated device_type payloads (e.g. tablet-only mobile).
    """
    updated = 0
    errors: List[str] = []
    geo_filter = (only_geo or "").strip().lower()[:2] or None
    try:
        streams = get_campaign_streams(int(campaign_id), base_url=base_url, api_key=api_key)
    except Exception as e:
        return 0, [f"get_streams: {e}"]
    for s in streams:
        geo, channel = parse_blend_stream_geo_channel(s.get("name") or "")
        if not geo or channel not in ("desktop", "mobile"):
            continue
        if geo_filter and geo != geo_filter:
            continue
        sid = s.get("id")
        if sid is None:
            continue
        try:
            set_blend_device_stream_filters(
                int(sid), geo, channel, base_url=base_url, api_key=api_key
            )
            updated += 1
        except KeitaroClientError as e:
            errors.append(f"{geo}/{channel} stream_id={sid}: {e}")
    return updated, errors


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


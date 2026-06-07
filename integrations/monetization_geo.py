"""
Shared geo normalization for monetization checks (Kelkoo, Yadore, Adexa).

Kelkoo uses lowercase 2-letter country codes (often ``uk`` for United Kingdom).
Yadore ``market`` expects lowercase; map ``gb`` -> ``uk`` when users paste GB.
Adexa ``country`` in our clients (GetMerchant, LinksMerchant, Blend URLs) uses the same lowercase style; map ``gb`` -> ``uk``.
"""
from __future__ import annotations


def two_letter_lower(geo: str) -> str:
    g = (geo or "").strip().lower()
    return g[:2] if len(g) >= 2 else g


def geo_for_yadore(geo: str) -> str:
    """Lowercase market code for Yadore ``market`` field."""
    g = two_letter_lower(geo)
    if g == "gb":
        return "uk"
    return g


def geo_for_shopnomix(geo: str) -> str:
    """Alpha-2 for Shopnomix ``country_codes`` (API examples use ``gb`` not ``uk``)."""
    g = two_letter_lower(geo)
    if g == "uk":
        return "gb"
    return g


def placement_feed_class(non_placement_found: bool, coupon_placement_found: bool) -> str:
    """
    Human-readable monetization from two placement probes (tile/native vs coupons).

    - ``non_coupon_only`` — tile / native path only
    - ``coupon_only`` — coupons placement only
    - ``both`` — both placements matched
    - ``none`` — neither matched
    """
    if non_placement_found and coupon_placement_found:
        return "both"
    if non_placement_found:
        return "non_coupon_only"
    if coupon_placement_found:
        return "coupon_only"
    return "none"


def yadore_feed_class(non_coupon_found: bool, coupon_found: bool) -> str:
    """Alias for Yadore deeplink probes (``isCouponing`` false vs true)."""
    return placement_feed_class(non_coupon_found, coupon_found)


def shopnomix_feed_class(tile_found: bool, coupons_found: bool) -> str:
    """Shopnomix feed6: tile (native) vs coupons demand campaigns."""
    return placement_feed_class(tile_found, coupons_found)

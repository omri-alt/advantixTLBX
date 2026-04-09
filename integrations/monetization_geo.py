"""
Shared geo normalization for monetization checks (Kelkoo, Yadore, Adexa).

Kelkoo uses lowercase 2-letter country codes (often ``uk`` for United Kingdom).
Yadore ``market`` expects lowercase; map ``gb`` -> ``uk`` when users paste GB.
Adexa docs use ISO2 like ``UK``, ``FR`` — map ``gb`` -> ``UK``.
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


def geo_for_adexa(geo: str) -> str:
    """Uppercase ISO2 for Adexa ``country`` (e.g. UK, FR)."""
    g = two_letter_lower(geo)
    if g == "gb":
        return "UK"
    return g.upper() if g else ""


def yadore_feed_class(non_coupon_found: bool, coupon_found: bool) -> str:
    """
    Human-readable Yadore monetization from two deeplink probes
    (``isCouponing`` false vs true).

    - ``non_coupon_only`` — standard traffic monetized, not coupon path
    - ``coupon_only`` — only the couponing probe matched
    - ``both`` — both paths matched
    - ``none`` — neither matched
    """
    if non_coupon_found and coupon_found:
        return "both"
    if non_coupon_found:
        return "non_coupon_only"
    if coupon_found:
        return "coupon_only"
    return "none"

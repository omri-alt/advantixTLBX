"""
Supported country/geo codes for flows and offers.
Use these when adding country flows or validating geo targeting.
"""

# Full list of geos (lowercase); pass to add_flow as-is or in any case
SUPPORTED_GEOS = [
    "ae", "at", "au", "be", "ca", "ch", "cz", "de", "es", "fi", "fr",
    "gr", "hk", "hu", "ie", "it", "kr", "mx", "nl", "no", "nz", "pl",
    "pt", "ro", "se", "sg", "sk", "uk", "us", "dk",
]

# For display/labels (optional)
GEO_LABELS = {
    "ae": "United Arab Emirates", "at": "Austria", "au": "Australia",
    "be": "Belgium", "ca": "Canada", "ch": "Switzerland", "cz": "Czech Republic",
    "de": "Germany", "es": "Spain", "fi": "Finland", "fr": "France",
    "gr": "Greece", "hk": "Hong Kong", "hu": "Hungary", "ie": "Ireland",
    "it": "Italy", "kr": "South Korea", "mx": "Mexico", "nl": "Netherlands",
    "no": "Norway", "nz": "New Zealand", "pl": "Poland", "pt": "Portugal",
    "ro": "Romania", "se": "Sweden", "sg": "Singapore", "sk": "Slovakia",
    "uk": "United Kingdom", "us": "United States", "dk": "Denmark",
}


def is_supported_geo(code: str) -> bool:
    """Return True if code is in the supported geos list (case-insensitive)."""
    return (code or "").strip().lower() in SUPPORTED_GEOS


def normalize_geo(code: str) -> str:
    """Return lowercase geo code from the list, or the normalized input."""
    c = (code or "").strip().lower()
    return c if c in SUPPORTED_GEOS else c

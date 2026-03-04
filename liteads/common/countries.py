"""
Consolidated ISO country data for LiteAds.

Single source of truth for country code conversions and display names
used across GeoIP resolution, analytics reporting, and ORTB payloads.
"""

from __future__ import annotations

# ── ISO 3166-1 Country Info ──────────────────────────────────────────────
# Maps Alpha-2 → (Alpha-3, Display Name)
# ORTB 2.6 spec requires Alpha-3 codes; analytics dashboards need names.
_COUNTRY_INFO: dict[str, tuple[str, str]] = {
    "US": ("USA", "United States"),
    "GB": ("GBR", "United Kingdom"),
    "CA": ("CAN", "Canada"),
    "AU": ("AUS", "Australia"),
    "DE": ("DEU", "Germany"),
    "FR": ("FRA", "France"),
    "JP": ("JPN", "Japan"),
    "BR": ("BRA", "Brazil"),
    "IN": ("IND", "India"),
    "MX": ("MEX", "Mexico"),
    "IT": ("ITA", "Italy"),
    "ES": ("ESP", "Spain"),
    "KR": ("KOR", "South Korea"),
    "NL": ("NLD", "Netherlands"),
    "SE": ("SWE", "Sweden"),
    "NO": ("NOR", "Norway"),
    "DK": ("DNK", "Denmark"),
    "FI": ("FIN", "Finland"),
    "PL": ("POL", "Poland"),
    "AT": ("AUT", "Austria"),
    "CH": ("CHE", "Switzerland"),
    "BE": ("BEL", "Belgium"),
    "IE": ("IRL", "Ireland"),
    "NZ": ("NZL", "New Zealand"),
    "SG": ("SGP", "Singapore"),
    "AR": ("ARG", "Argentina"),
    "CL": ("CHL", "Chile"),
    "CO": ("COL", "Colombia"),
    "ZA": ("ZAF", "South Africa"),
    "PH": ("PHL", "Philippines"),
    "TH": ("THA", "Thailand"),
    "MY": ("MYS", "Malaysia"),
    "ID": ("IDN", "Indonesia"),
    "VN": ("VNM", "Vietnam"),
    "TW": ("TWN", "Taiwan"),
    "HK": ("HKG", "Hong Kong"),
    "IL": ("ISR", "Israel"),
    "AE": ("ARE", "United Arab Emirates"),
    "SA": ("SAU", "Saudi Arabia"),
    "TR": ("TUR", "Turkey"),
    "RU": ("RUS", "Russia"),
    "UA": ("UKR", "Ukraine"),
    "CZ": ("CZE", "Czech Republic"),
    "PT": ("PRT", "Portugal"),
    "RO": ("ROU", "Romania"),
    "HU": ("HUN", "Hungary"),
    "GR": ("GRC", "Greece"),
    "CN": ("CHN", "China"),
    "PK": ("PAK", "Pakistan"),
    "BD": ("BGD", "Bangladesh"),
    "NG": ("NGA", "Nigeria"),
    "EG": ("EGY", "Egypt"),
    "KE": ("KEN", "Kenya"),
    "PE": ("PER", "Peru"),
    "VE": ("VEN", "Venezuela"),
    "EC": ("ECU", "Ecuador"),
    "PR": ("PRI", "Puerto Rico"),
    "DO": ("DOM", "Dominican Republic"),
    "GT": ("GTM", "Guatemala"),
    "CR": ("CRI", "Costa Rica"),
}


def to_alpha3(code: str) -> str:
    """Convert ISO Alpha-2 country code to Alpha-3 (ORTB 2.6 requirement).

    Returns the code unchanged if already 3 characters or not in the map.
    """
    if not code:
        return ""
    upper = code.upper().strip()
    if len(upper) == 3:
        return upper  # Already Alpha-3
    info = _COUNTRY_INFO.get(upper)
    return info[0] if info else upper


def to_display_name(code: str) -> str:
    """Convert ISO Alpha-2 country code to a human-readable display name.

    Returns the code itself if not in the map.
    """
    if not code:
        return ""
    info = _COUNTRY_INFO.get(code.upper().strip())
    return info[1] if info else code

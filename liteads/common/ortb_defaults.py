"""
Centralized OpenRTB 2.6 defaults and constants.

Single source of truth for ORTB constants used across:
  - ``demand_forwarder.py`` (bid-request construction)
  - ``ortb_enricher.py``    (auto-enrichment of missing fields)
  - ``schemas/openrtb.py``  (Pydantic model defaults)

All values follow IAB OpenRTB 2.6, CTV IFA Guidelines, and
IAB Video Ad Format Guidelines.

To override defaults at the endpoint level, configure the
``DemandEndpoint`` model fields (ortb_version, auction_type,
mime_types, protocols) — the forwarder reads those first.
"""

from __future__ import annotations

from typing import Final


# ═══════════════════════════════════════════════════════════════════════════
# MIME Types
# ═══════════════════════════════════════════════════════════════════════════

#: CTV players universally accept MP4 and HLS
MIME_TYPES_CTV: Final[list[str]] = [
    "video/mp4",
    "application/x-mpegURL",
]

#: In-app adds WebM and DASH for cross-player support
MIME_TYPES_INAPP: Final[list[str]] = [
    "video/mp4",
    "video/webm",
    "application/x-mpegURL",
    "application/dash+xml",
]

#: Full list of all supported video MIME types (superset)
MIME_TYPES_ALL: Final[list[str]] = [
    "video/mp4",
    "video/webm",
    "video/ogg",
    "video/3gpp",
    "application/x-mpegURL",
    "application/dash+xml",
]


def default_mimes(is_ctv: bool = True) -> list[str]:
    """Return default MIME types based on environment."""
    return list(MIME_TYPES_CTV if is_ctv else MIME_TYPES_INAPP)


# ═══════════════════════════════════════════════════════════════════════════
# VAST Protocols (§5.8)
# ═══════════════════════════════════════════════════════════════════════════

#: Full protocol list including wrappers (used in bid requests)
#  2=VAST2.0, 3=VAST3.0, 4=VAST2.0-Wrapper, 5=VAST3.0-Wrapper,
#  6=VAST4.0, 7=VAST4.1, 8=VAST4.2
PROTOCOLS_FULL: Final[list[int]] = [2, 3, 4, 5, 6, 7, 8]

#: Protocol list without wrapper types (used in config/schema defaults)
PROTOCOLS_CORE: Final[list[int]] = [2, 3, 6, 7, 8]


# ═══════════════════════════════════════════════════════════════════════════
# Video Defaults
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_WIDTH: Final[int] = 1920
DEFAULT_HEIGHT: Final[int] = 1080
DEFAULT_MIN_DURATION: Final[int] = 5        # seconds
DEFAULT_MAX_DURATION: Final[int] = 120      # seconds
DEFAULT_DURATION: Final[int] = 30           # seconds
DEFAULT_STARTDELAY: Final[int] = 0          # pre-roll
DEFAULT_PLACEMENT: Final[int] = 1           # In-Stream
DEFAULT_LINEARITY: Final[int] = 1           # Linear (in-stream)


# ═══════════════════════════════════════════════════════════════════════════
# Playback & Delivery (§5.10, §5.15)
# ═══════════════════════════════════════════════════════════════════════════

#: CTV: auto-play with sound is standard
PLAYBACK_CTV: Final[list[int]] = [1]

#: In-app: auto-play + viewport-based autoplay
PLAYBACK_INAPP: Final[list[int]] = [1, 5]

#: Delivery: 2=Progressive, 1=Streaming (preferred order)
DEFAULT_DELIVERY: Final[list[int]] = [2, 1]


def default_playback(is_ctv: bool = True) -> list[int]:
    """Return default playback methods based on environment."""
    return list(PLAYBACK_CTV if is_ctv else PLAYBACK_INAPP)


# ═══════════════════════════════════════════════════════════════════════════
# Auction & Request Defaults
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_AUCTION_TYPE: Final[int] = 1        # First-price (industry standard since 2019)
DEFAULT_TMAX: Final[int] = 500              # ms — DSP response budget
DEFAULT_CURRENCY: Final[str] = "USD"
DEFAULT_BID_FLOOR: Final[float] = 0.01      # Minimal floor — let DSP decide


# ═══════════════════════════════════════════════════════════════════════════
# Device Defaults
# ═══════════════════════════════════════════════════════════════════════════

#: Default device type: 3=Connected TV
DEFAULT_DEVICETYPE: Final[int] = 3
DEFAULT_LANGUAGE: Final[str] = "en"

#: Connection type mapping: string → IAB §5.22 integer
CONNECTION_TYPE_MAP: Final[dict[str, int]] = {
    "ethernet": 1,
    "wifi": 2,
    "cellular_unknown": 3,
    "2g": 4,
    "3g": 5,
    "4g": 6,
    "5g": 7,
}

#: Default connection type: CTV → Ethernet, In-App → WiFi
DEFAULT_CONNTYPE_CTV: Final[int] = 1       # Ethernet
DEFAULT_CONNTYPE_INAPP: Final[int] = 2     # WiFi


def default_connection_type(is_ctv: bool = True) -> int:
    """Return default connection type based on environment."""
    return DEFAULT_CONNTYPE_CTV if is_ctv else DEFAULT_CONNTYPE_INAPP


# ═══════════════════════════════════════════════════════════════════════════
# OS Canonical Names
# ═══════════════════════════════════════════════════════════════════════════

#: Mapping from raw OS string (lowered, no spaces) → canonical OS name
OS_CANONICAL: Final[dict[str, str]] = {
    "roku": "Roku",
    "rokuos": "Roku",
    "firetv": "Fire OS",
    "fireos": "Fire OS",
    "tvos": "tvOS",
    "tizen": "Tizen",
    "webos": "webOS",
    "webostv": "webOS",
    "vizio": "SmartCast",
    "androidtv": "Android",
    "googletv": "Android",
    "chromecast": "Android",
    "android": "Android",
    "ios": "iOS",
}

#: OS strings that indicate a set-top box (§5.21 devicetype=7)
STB_OS_KEYWORDS: Final[tuple[str, ...]] = (
    "roku", "firetv", "fireos", "chromecast",
)


# ═══════════════════════════════════════════════════════════════════════════
# Extended Identifiers (§3.2.27)
# ═══════════════════════════════════════════════════════════════════════════

#: IFA type → EID source domain
EID_SOURCE_MAP: Final[dict[str, str]] = {
    "rida": "roku.com",
    "afai": "amazon.com",
    "idfa": "apple.com",
    "gaid": "google.com",
    "tifa": "samsung.com",
    "lgudid": "lgappstv.com",
    "vida": "vizio.com",
}


# ═══════════════════════════════════════════════════════════════════════════
# Enricher Defaults (for ortb_enricher.py)
# ═══════════════════════════════════════════════════════════════════════════

#: Video defaults dictionary (used by ortb_enricher's _set_default loop)
ENRICHER_VIDEO_DEFAULTS: Final[dict] = {
    "mimes": list(MIME_TYPES_INAPP),      # broadest set for enrichment
    "protocols": list(PROTOCOLS_FULL),
    "w": DEFAULT_WIDTH,
    "h": DEFAULT_HEIGHT,
    "plcmt": DEFAULT_PLACEMENT,
    "placement": DEFAULT_PLACEMENT,
    "pos": 1,
    "linearity": DEFAULT_LINEARITY,
    "startdelay": DEFAULT_STARTDELAY,
    "minduration": DEFAULT_MIN_DURATION,
    "maxduration": DEFAULT_DURATION,       # enricher uses 30s as safe max default
    "playbackmethod": list(PLAYBACK_CTV),
    "delivery": list(DEFAULT_DELIVERY),
}

#: Device defaults dictionary (used by ortb_enricher's _set_default loop)
ENRICHER_DEVICE_DEFAULTS: Final[dict] = {
    "devicetype": DEFAULT_DEVICETYPE,
    "lmt": 0,
    "dnt": 0,
    "language": DEFAULT_LANGUAGE,
}

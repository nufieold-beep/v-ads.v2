"""
Creative ID and Adomain extraction utilities for VAST + OpenRTB.

Implements the multi-source priority chains specified in IAB best practices:

**Creative ID priority chain:**
    1. ``bid.crid`` (OpenRTB Creative ID) — most authoritative
    2. ``bid.adid`` (pre-loaded ad ID)
    3. VAST ``<Creative id="...">`` from adm XML
    4. VAST ``<Ad id="...">`` from adm XML
    5. Fingerprint hash of bid.id (last resort)

**Adomain priority chain:**
    1. ``bid.adomain[]`` (OpenRTB advertiser domains)
    2. VAST ``<ClickThrough>`` domain extraction
    3. ``bid.ext`` / ``seatbid.ext`` advertiser fields
    4. ``None``
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, NamedTuple
from urllib.parse import urlparse


# ── Pre-compiled regexes for VAST XML parsing ────────────────────────────
_AD_ID_RE = re.compile(r'<Ad[^>]+id=["\']([^"\']+)["\']', re.IGNORECASE)
_CREATIVE_ID_RE = re.compile(r'<Creative[^>]+id=["\']([^"\']+)["\']', re.IGNORECASE)
_CLICK_THROUGH_RE = re.compile(
    r'<ClickThrough[^>]*>\s*(?:<!\[CDATA\[)?\s*(https?://[^\s<\]]+)',
    re.IGNORECASE,
)
_VAST_WRAPPER_RE = re.compile(r'<Wrapper\b', re.IGNORECASE)
_VAST_TAG_URI_RE = re.compile(
    r'<VASTAdTagURI[^>]*>\s*(?:<!\[CDATA\[)?\s*(https?://[^\s<\]]+)',
    re.IGNORECASE,
)


class CreativeIdResult(NamedTuple):
    """Result of creative ID resolution."""

    creative_id: str          # Best-available creative identifier
    source: str               # Where it came from: crid|adid|vast_creative|vast_ad|hash
    crid: str                 # Raw OpenRTB crid
    adid: str                 # Raw OpenRTB adid
    vast_creative_id: str     # <Creative id="..."> from VAST
    vast_ad_id: str           # <Ad id="..."> from VAST


class AdomainResult(NamedTuple):
    """Result of adomain resolution."""

    adomain: list[str]        # Full adomain list
    primary: str              # First / primary adomain for reporting
    source: str               # Where it came from: ortb|clickthrough|ext|""


def extract_creative_id(
    *,
    bid_crid: str | None = None,
    bid_adid: str | None = None,
    bid_id: str | None = None,
    adm: str | None = None,
) -> CreativeIdResult:
    """Resolve the best creative ID using the multi-source priority chain.

    Parameters
    ----------
    bid_crid : str, optional
        OpenRTB ``bid.crid`` (Creative ID from the DSP).
    bid_adid : str, optional
        OpenRTB ``bid.adid`` (pre-loaded ad ID).
    bid_id : str, optional
        OpenRTB ``bid.id`` (bid identifier — used for hash fallback).
    adm : str, optional
        VAST XML markup (``bid.adm`` field) — parsed for ``<Creative>``
        and ``<Ad>`` IDs.

    Returns
    -------
    CreativeIdResult
        Named tuple with the resolved creative_id, source tag, and all
        raw values found.
    """
    raw_crid = (bid_crid or "").strip()
    raw_adid = (bid_adid or "").strip()
    vast_creative_id = ""
    vast_ad_id = ""

    # Parse VAST XML if available
    if adm:
        m = _CREATIVE_ID_RE.search(adm)
        if m:
            vast_creative_id = m.group(1).strip()
        m = _AD_ID_RE.search(adm)
        if m:
            vast_ad_id = m.group(1).strip()

    # Priority chain
    if raw_crid:
        creative_id, source = raw_crid, "crid"
    elif raw_adid:
        creative_id, source = raw_adid, "adid"
    elif vast_creative_id:
        creative_id, source = vast_creative_id, "vast_creative"
    elif vast_ad_id:
        creative_id, source = vast_ad_id, "vast_ad"
    elif bid_id:
        # Fingerprint hash as last resort
        h = hashlib.md5(bid_id.encode()).hexdigest()[:12]
        creative_id, source = f"hash_{h}", "hash"
    else:
        creative_id, source = "unknown", "none"

    return CreativeIdResult(
        creative_id=creative_id,
        source=source,
        crid=raw_crid,
        adid=raw_adid,
        vast_creative_id=vast_creative_id,
        vast_ad_id=vast_ad_id,
    )


def extract_adomain(
    *,
    bid_adomain: list[str] | None = None,
    adm: str | None = None,
    bid_ext: dict[str, Any] | None = None,
) -> AdomainResult:
    """Resolve adomain using the multi-source priority chain.

    Parameters
    ----------
    bid_adomain : list[str], optional
        ``bid.adomain[]`` from the OpenRTB bid response.
    adm : str, optional
        VAST XML markup — parsed for ``<ClickThrough>`` URLs.
    bid_ext : dict, optional
        ``bid.ext`` or ``seatbid.ext`` — checked for advertiser fields.

    Returns
    -------
    AdomainResult
        Named tuple with the full adomain list, primary domain, and source.
    """
    # 1. OpenRTB adomain[] (most authoritative)
    if bid_adomain:
        cleaned = [d.strip().lower() for d in bid_adomain if d and d.strip()]
        if cleaned:
            return AdomainResult(
                adomain=cleaned,
                primary=cleaned[0],
                source="ortb",
            )

    # 2. Extract domain from VAST <ClickThrough> URL
    if adm:
        m = _CLICK_THROUGH_RE.search(adm)
        if m:
            try:
                parsed = urlparse(m.group(1).strip())
                host = parsed.hostname or ""
                # Strip www. prefix
                if host.startswith("www."):
                    host = host[4:]
                if host:
                    return AdomainResult(
                        adomain=[host],
                        primary=host,
                        source="clickthrough",
                    )
            except Exception:
                pass

    # 3. Check bid.ext for advertiser domain fields
    if bid_ext:
        for key in ("adomain", "advertiser_domain", "advdomain", "adv_domain"):
            val = bid_ext.get(key)
            if val:
                if isinstance(val, list) and val:
                    cleaned: list[str] = [
                        str(d).strip().lower() for d in val if d and str(d).strip()
                    ]
                    if cleaned:
                        return AdomainResult(
                            adomain=cleaned,
                            primary=cleaned[0],
                            source="ext",
                        )
                elif isinstance(val, str) and val.strip():
                    domain = val.strip().lower()
                    return AdomainResult(
                        adomain=[domain],
                        primary=domain,
                        source="ext",
                    )

    # 4. No adomain found
    return AdomainResult(adomain=[], primary="", source="")


def detect_adm_type(
    *,
    adm: str | None = None,
    vast_url: str | None = None,
    has_nurl: bool = False,
) -> tuple[str, int]:
    """Determine markup type and wrapper depth.

    Returns
    -------
    (adm_type, wrapper_depth)
        adm_type: "inline" | "wrapper" | "nurl" | "vast_tag"
        wrapper_depth: 0 for inline, 1+ for wrappers
    """
    if adm:
        if _VAST_WRAPPER_RE.search(adm):
            # Count wrapper depth (number of <Wrapper> tags)
            depth = len(_VAST_WRAPPER_RE.findall(adm))
            return "wrapper", max(depth, 1)
        return "inline", 0
    if vast_url:
        return "vast_tag", 1
    if has_nurl:
        return "nurl", 1
    return "unknown", 0

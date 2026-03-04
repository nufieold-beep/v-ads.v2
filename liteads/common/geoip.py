"""
GeoIP Service — resolves IP addresses to geographic data using MaxMind GeoLite2.

Provides latitude, longitude, country, region, metro/DMA, city, and zip
so that ORTB bid requests always carry geo data even when publishers
don't send it.  Falls back gracefully when the database isn't available.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from liteads.common.countries import to_alpha3 as _to_alpha3
from liteads.common.logger import get_logger

logger = get_logger(__name__)

# Lazy import — geoip2 may not be installed yet.
_reader: object | None = None  # geoip2.database.Reader or None
_db_loaded = False

# Paths to try (in order) when looking for the MMDB file.
_DB_SEARCH_PATHS = [
    os.environ.get("GEOIP_DB_PATH", ""),
    "/app/data/GeoLite2-City.mmdb",
    "/app/GeoLite2-City.mmdb",
    "./data/GeoLite2-City.mmdb",
    "./GeoLite2-City.mmdb",
]

# Empty geo when lookup produces nothing — NEVER use fake coordinates.
# DSPs detect bogus lat/lon and penalise bid requests that send them.
DEFAULT_GEO = {
    "country": "",
    "region": "",
    "city": "",
    "zip": "",
    "lat": 0.0,
    "lon": 0.0,
    "metro": "",
    "type": 2,        # IP-based
    "accuracy": 0,
    "ipservice": 3,   # MaxMind
}


@dataclass
class GeoResult:
    """Resolved geographic data for an IP address."""

    country: str
    region: str
    city: str
    zip: str  # noqa: A003
    lat: float
    lon: float
    metro: str
    type: int      # noqa: A003  (1=GPS, 2=IP, 3=User)
    accuracy: int
    ipservice: int


def _ensure_loaded() -> None:
    """Load the MaxMind database on first call (lazy init)."""
    global _reader, _db_loaded

    if _db_loaded:
        return
    _db_loaded = True

    try:
        import maxminddb  # lightweight C-extension reader
    except ImportError:
        logger.warning("maxminddb not installed — GeoIP enrichment disabled")
        return

    for path in _DB_SEARCH_PATHS:
        if path and os.path.isfile(path):
            try:
                _reader = maxminddb.open_database(path)
                logger.info("GeoIP database loaded", path=path)
                return
            except Exception as exc:
                logger.warning("Failed to open GeoIP DB", path=path, error=str(exc))

    logger.warning(
        "GeoLite2-City.mmdb not found — GeoIP enrichment disabled",
        searched=_DB_SEARCH_PATHS,
    )


@lru_cache(maxsize=8192)
def lookup(ip: str | None) -> GeoResult:
    """
    Look up geo data for *ip*.

    Returns a populated ``GeoResult`` (falling back to defaults when the
    database is unavailable or the IP is unknown).
    Results are cached (LRU, 8192 entries) to avoid repeated MMDB hits.
    """
    _ensure_loaded()

    if not ip or not _reader:
        return _defaults()

    try:
        data = _reader.get(ip)  # type: ignore[union-attr]
        if not data:
            return _defaults()

        loc = data.get("location") or {}
        country_rec = data.get("country") or {}
        subdivisions = data.get("subdivisions") or []
        city_rec = data.get("city") or {}
        postal_rec = data.get("postal") or {}

        country = _to_alpha3(country_rec.get("iso_code") or "")
        if not country:
            # MaxMind has no data for this IP — return empty result
            return _defaults()

        # Region: prefer iso_code (MaxMind), fall back to English name (DB-IP)
        region = ""
        if subdivisions:
            sub = subdivisions[0]
            region = sub.get("iso_code") or (sub.get("names") or {}).get("en") or ""

        return GeoResult(
            country=country,
            region=region,
            city=(city_rec.get("names") or {}).get("en") or "",
            zip=postal_rec.get("code") or "",
            lat=loc.get("latitude") or 0.0,
            lon=loc.get("longitude") or 0.0,
            metro=str(loc.get("metro_code") or ""),
            type=2,
            accuracy=loc.get("accuracy_radius") or 0,
            ipservice=3,
        )
    except Exception as exc:
        logger.debug("GeoIP lookup failed", ip=ip, error=str(exc))
        return _defaults()


# Cached default GeoResult to avoid re-creating identical instances.
_DEFAULT_GEO_RESULT = GeoResult(
    country=DEFAULT_GEO["country"],
    region=DEFAULT_GEO["region"],
    city=DEFAULT_GEO["city"],
    zip=DEFAULT_GEO["zip"],
    lat=DEFAULT_GEO["lat"],
    lon=DEFAULT_GEO["lon"],
    metro=DEFAULT_GEO["metro"],
    type=DEFAULT_GEO["type"],
    accuracy=DEFAULT_GEO["accuracy"],
    ipservice=DEFAULT_GEO["ipservice"],
)


def _defaults() -> GeoResult:
    """Return a ``GeoResult`` filled with safe defaults."""
    return _DEFAULT_GEO_RESULT


# ---------------------------------------------------------------------------
# Schema factory helpers  (avoid 3× inline construction of Pydantic models)
# ---------------------------------------------------------------------------

def geoip_to_geo_info(ip: str | None) -> "GeoInfo":
    """Look up *ip* and return a ``GeoInfo`` schema (used by VAST-tag router)."""
    from liteads.schemas.request import GeoInfo  # avoid circular at module level

    g = lookup(ip)
    return GeoInfo(
        ip=ip or "",
        country=g.country or None,
        region=g.region or None,
        city=g.city or None,
        dma=g.metro or None,
        latitude=g.lat,
        longitude=g.lon,
        zip_code=g.zip or None,
        geo_type=g.type,
        ipservice=g.ipservice,
    )


def geoip_to_ortb_geo(ip: str | None) -> "OrtbGeo | None":
    """Look up *ip* and return an ``OrtbGeo`` schema (used by demand forwarder / enricher).

    Returns ``None`` when the lookup yields no country (MaxMind DB unavailable
    or the IP is unknown).
    """
    from liteads.schemas.openrtb import Geo as OrtbGeo  # avoid circular at module level

    g = lookup(ip)
    if not g or not g.country:
        return None
    geo = OrtbGeo(
        country=g.country,
        region=g.region or None,
        city=g.city or None,
        metro=g.metro or None,
        type=2,           # 2 = IP-based
        ipservice=3,       # 3 = MaxMind
    )
    # Only include lat/lon if MaxMind actually resolved them (non-zero)
    if g.lat and g.lon:
        geo.lat = g.lat
        geo.lon = g.lon
    if g.zip:
        geo.zip = g.zip
    if g.accuracy:
        geo.accuracy = g.accuracy
    return geo

"""
VAST Tag Router – GET /api/vast endpoint for CTV & In-App Video.

This endpoint is called directly by video players and SSPs that support
VAST tag URLs (as opposed to OpenRTB programmatic).  It parses query
parameters, resolves the ad through the internal pipeline, and returns
VAST XML (2.0 – 4.2).

Example request (LG webOS / Fawesome):
    GET /api/vast?sid=125&imp=0&w=1920&h=1080&cb=9727167868012
        &ip=2603:9000:ba00:1eba::149a
        &ua=Mozilla/5.0 (Web0S; Linux/SmartTV) ...
        &app_bundle=lgiptv.fawesome-freemoviesandtvshows
        &app_name=Fawesome - Free Movies and TV Shows
        &app_store_url=https://us.lgappstv.com/main/tvapp/detail?appId=458741
        &max_dur=32&min_dur=5
        &content_type=IAB1-5&coppa=0
        &device_make=LG&device_model=50UN6950ZUF
        &dnt=0&ifa=7424c8e0-...&os=webOS TV&us_privacy=1YNN&isp=Spectrum
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from liteads.ad_server.services.ad_service import AdService
from liteads.ad_server.services.demand_forwarder import DemandForwarder, _get_http_client
from liteads.ad_server.services.event_service import EventService
from liteads.ad_server.services.vast_builder import build_vast_for_candidate
from liteads.ad_server.routers.analytics import capture_traffic_event
from liteads.common.config import get_settings
from liteads.common.database import get_session
from liteads.common.device import (
    detect_environment,
    infer_ifa_type,
    infer_os_from_ua,
    map_placement,
)
from liteads.common.extraction import (
    detect_adm_type,
    extract_adomain,
    extract_creative_id,
)
from liteads.common.geoip import geoip_to_geo_info
from liteads.common.logger import get_logger, log_context
from liteads.common.tracking import (
    build_ad_id,
    build_burl,
    build_demand_extra_params,
    build_error_url,
    build_impression_url,
    build_nurl,
    empty_vast_response,
)
from liteads.common.utils import extract_client_ip, generate_request_id
from liteads.schemas.internal import AdDecision
from liteads.schemas.request import (
    AdRequest,
    AppInfo,
    DeviceInfo,
    GeoInfo,
    VideoPlacementInfo,
)

logger = get_logger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Pre-compiled regex (avoid re-compiling per request)
# ---------------------------------------------------------------------------
_IFA_IN_UA_RE = re.compile(r'[&;]ifa[=:]\s*([0-9a-fA-F-]{20,})')
_IFA_BARE_RE = re.compile(r'ifa=([0-9a-fA-F-]{20,})')
_AD_TAG_RE = re.compile(r'(<Ad[^>]*>)')
_AD_ID_RE = re.compile(r'<Ad[^>]+id=["\']([^"\']+)["\']')
_CREATIVE_ID_RE = re.compile(r'<Creative[^>]+id=["\']([^"\']+)["\']')
_MEDIA_FILE_RE = re.compile(r'<MediaFile[\s>]')
_VAST_TAG_URI_RE = re.compile(r'<VASTAdTagURI')

# Module-level VAST OK response header template — only X-Request-ID and
# X-LiteAds-Environment vary per request; the other 5 entries are static.
_VAST_OK_HEADERS: dict[str, str] = {
    "Content-Type": "application/xml; charset=utf-8",
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
}


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def _get_ad_service(session: AsyncSession = Depends(get_session)) -> AdService:
    """Dependency to get ad service with DB session."""
    return AdService(session)


# Module-level singleton (DemandForwarder is stateless — creates its own session)
_demand_forwarder = DemandForwarder()


def _get_demand_forwarder() -> DemandForwarder:
    """Dependency to get demand forwarder (uses its own DB session)."""
    return _demand_forwarder

# ---------------------------------------------------------------------------
# Helpers – delegated to common.device / common.tracking
# ---------------------------------------------------------------------------

# Backward-compatible aliases for the extracted helpers
_detect_env = detect_environment
_detect_ifa_type = infer_ifa_type
_placement_from_params = map_placement

# ---------------------------------------------------------------------------
# GET /api/vast – VAST Tag endpoint
# ---------------------------------------------------------------------------

@router.get(
    "",
    summary="VAST Tag Endpoint",
    description=(
        "Returns VAST XML for CTV/In-App video players. "
        "Accepts device, app, content, and video placement parameters as query strings. "
        "Supports VAST versions 2.0 through 4.2."
    ),
    responses={
        200: {"content": {"application/xml": {}}, "description": "VAST XML document"},
    },
)
async def vast_tag(
    request: Request,
    ad_service: AdService = Depends(_get_ad_service),
    demand_forwarder: DemandForwarder = Depends(_get_demand_forwarder),
    # Slot / impression
    sid: str = Query("default", description="Slot / placement ID"),
    imp: int = Query(0, description="Impression sequence index"),
    # Video
    w: int = Query(1920, description="Video width"),
    h: int = Query(1080, description="Video height"),
    min_dur: int = Query(5, description="Minimum duration (seconds)"),
    max_dur: int = Query(30, description="Maximum duration (seconds)"),
    startdelay: Optional[int] = Query(None, description="Start delay (0=pre, >0=mid, -1=mid, -2=post)"),
    # Device
    ip: Optional[str] = Query(None, description="Client IP address"),
    uip: Optional[str] = Query(None, description="User IP address (Adtelligent-compatible alias for ip)"),
    ua: Optional[str] = Query(None, description="User-Agent"),
    ifa: Optional[str] = Query(None, description="Advertising ID"),
    dnt: Optional[int] = Query(None, description="Do Not Track flag"),
    os: Optional[str] = Query(None, alias="os", description="Device OS"),
    osv: Optional[str] = Query(None, description="OS version (e.g. 12.0)"),
    device_make: Optional[str] = Query(None, description="Device manufacturer"),
    device_model: Optional[str] = Query(None, description="Device model"),
    # App / Content
    app_bundle: Optional[str] = Query(None, description="App bundle ID"),
    app_name: Optional[str] = Query(None, description="App name"),
    app_store_url: Optional[str] = Query(None, description="App store URL"),
    content_type: Optional[str] = Query(None, description="IAB content category"),
    ct_chan: Optional[str] = Query(None, description="Content channel name"),
    ct_id: Optional[str] = Query(None, description="Content ID"),
    ct_title: Optional[str] = Query(None, description="Content title"),
    ct_ser: Optional[str] = Query(None, description="Content series"),
    ct_seas: Optional[str] = Query(None, description="Content season"),
    ct_eps: Optional[str] = Query(None, description="Content episode"),
    ct_lang: Optional[str] = Query(None, description="Content language"),
    ct_len: Optional[int] = Query(None, description="Content length (seconds)"),
    ct_live_str: Optional[int] = Query(None, description="Live stream (0/1)"),
    ct_rat: Optional[str] = Query(None, description="Content rating"),
    ct_net: Optional[str] = Query(None, description="Content network"),
    ct_genre: Optional[str] = Query(None, description="Content genre"),
    ct_prodq: Optional[str] = Query(None, description="Production quality"),
    ct_producer: Optional[str] = Query(None, description="Content producer name"),
    ct_qa_media_rating: Optional[str] = Query(None, description="QAG media rating"),
    ct_url: Optional[str] = Query(None, description="Content URL"),
    # Geo
    lat: Optional[float] = Query(None, description="Latitude"),
    lon: Optional[float] = Query(None, description="Longitude"),
    country_code: Optional[str] = Query(None, description="Country code (ISO 3166-1 alpha-2)"),
    # Privacy
    coppa: Optional[int] = Query(None, description="COPPA flag"),
    us_privacy: Optional[str] = Query(None, description="US Privacy string (CCPA)"),
    gdpr: Optional[int] = Query(None, description="GDPR applies flag (0/1)"),
    gdpr_consent: Optional[str] = Query(None, description="TCF consent string"),
    gpp: Optional[str] = Query(None, description="IAB Global Privacy Platform string"),
    gpp_sid: Optional[str] = Query(None, description="GPP section IDs (comma-separated)"),
    # Misc
    cb: Optional[str] = Query(None, description="Cache buster"),
    isp: Optional[str] = Query(None, description="ISP name"),
    app_cat: Optional[str] = Query(None, description="App IAB category"),
    device_type: Optional[int] = Query(None, description="Device type (1=mobile/tablet, 3=CTV, 7=set-top-box)"),
    # Extended device
    device_language: Optional[str] = Query(None, description="Device language (e.g. en_US)"),
    didsha1: Optional[str] = Query(None, description="Hardware device ID SHA1 hash"),
    didmd5: Optional[str] = Query(None, description="Hardware device ID MD5 hash"),
    # Extended geo
    region: Optional[str] = Query(None, description="Geo region/state code"),
    metro: Optional[str] = Query(None, description="DMA / metro code"),
    city: Optional[str] = Query(None, description="Geo city name"),
    zip_code: Optional[str] = Query(None, alias="zip", description="Postal / ZIP code"),
    geo_type: Optional[int] = Query(None, description="Geo location type (1=GPS, 2=IP, 3=User)"),
    ipservice: Optional[int] = Query(None, description="IP geolocation service (1=ip2location, 2=Neustar, 3=MaxMind)"),
    # Extended app
    app_id: Optional[str] = Query(None, description="Publisher app ID"),
    app_domain: Optional[str] = Query(None, description="App domain (e.g. verylocal.com)"),
    pub_id: Optional[str] = Query(None, description="Publisher ID"),
    app_pagecat: Optional[str] = Query(None, description="Page-level IAB categories (comma-separated)"),
    inv_partner_domain: Optional[str] = Query(None, description="Inventory partner domain"),
    # Extended content
    ct_episode: Optional[int] = Query(None, description="Content episode number"),
    ct_context: Optional[int] = Query(None, description="Content context (1=video, 2=game, 3=music, 4=app)"),
    ct_gtax: Optional[int] = Query(None, description="Content genre taxonomy ID"),
    ct_genres: Optional[str] = Query(None, description="Genre codes from taxonomy (comma-separated)"),
    # Extended video
    plcmt: Optional[int] = Query(None, description="OpenRTB 2.6 video placement type"),
    linearity: Optional[int] = Query(None, description="1=Linear, 2=Non-linear"),
    sequence: Optional[int] = Query(None, description="Sequence number in pod"),
    minbitrate: Optional[int] = Query(None, description="Minimum bitrate (kbps)"),
    maxbitrate: Optional[int] = Query(None, description="Maximum bitrate (kbps)"),
    playbackmethod: Optional[str] = Query(None, description="Playback methods (comma-separated ints)"),
    delivery: Optional[str] = Query(None, description="Delivery methods (comma-separated ints)"),
    protocols: Optional[str] = Query(None, description="VAST protocols (comma-separated ints)"),
    # Pod fields
    poddur: Optional[int] = Query(None, description="Total pod duration (seconds)"),
    maxseq: Optional[int] = Query(None, description="Max ads in the pod"),
    podid: Optional[str] = Query(None, description="Pod identifier"),
    podseq: Optional[int] = Query(None, description="Pod sequence (0=any, 1=first, -1=last)"),
    poddedupe: Optional[str] = Query(None, description="Pod deduplication signals (comma-separated ints)"),
    # Impression
    tagid: Optional[str] = Query(None, description="Publisher tag / placement ID"),
    bidfloor: Optional[float] = Query(None, description="Bid floor override (CPM)"),
    exp: Optional[int] = Query(None, description="Impression expiry (seconds)"),
    # Blocked signals
    bcat: Optional[str] = Query(None, description="Blocked IAB categories (comma-separated)"),
    badv: Optional[str] = Query(None, description="Blocked advertiser domains (comma-separated)"),
) -> Response:
    """
    Handle VAST tag GET requests from CTV/In-App video players.

    Builds an internal AdRequest from query params, runs the ad pipeline,
    and returns VAST XML with tracking events, nurl, and burl.
    """
    request_id = generate_request_id()
    settings = get_settings()

    # ── Resolve user IP: prefer `uip` (Adtelligent-compatible) over `ip` ──
    ip = uip or ip

    # ── Normalise dnt / coppa (may arrive as None from middleware) ─
    dnt = dnt if dnt is not None else 0
    coppa = coppa if coppa is not None else 0

    # ── Clean embedded IFA from UA ───────────────────────────────
    # Some publishers URL-encode the '&' between ua and ifa as %26,
    # which glues ifa=xxx into the UA value instead of a separate param.
    # Always strip it from the UA; use it as IFA only when no explicit
    # ifa param was provided.
    if ua:
        _ifa_match = _IFA_IN_UA_RE.search(ua)
        if not _ifa_match:
            _ifa_match = _IFA_BARE_RE.search(ua)
        if _ifa_match:
            if not ifa:
                ifa = _ifa_match.group(1)
            # Always strip the embedded ifa from the UA
            _ifa_pos = ua.find('&ifa=')
            if _ifa_pos == -1:
                _ifa_pos = ua.find(';ifa=')
            if _ifa_pos != -1:
                ua = ua[:_ifa_pos].rstrip()

    # ── Clean app_name if publisher forgot '&' before cb= ─────────
    if app_name and 'cb=' in app_name:
        app_name = app_name[:app_name.index('cb=')].rstrip()

    # Resolve OS from param or UA -----------------------------------------
    os_str = (os or "").strip()
    ua_str = (ua or "").strip()
    # Fallback: use the HTTP User-Agent header when publisher didn't send ua=
    if not ua_str:
        ua_str = (request.headers.get("user-agent") or "").strip()
    if not os_str and ua_str:
        os_str = infer_os_from_ua(ua_str)

    env = _detect_env(os_str, ua_str, device_type)
    make = (device_make or "").strip()
    model = (device_model or "").strip()

    # ── Capture inbound VAST request for live traffic inspector ──
    capture_traffic_event("vast_request", request_id, {
        "environment": env,
        "sid": sid,
        "ip": ip,
        "ua": ua_str[:120] if ua_str else None,
        "app_bundle": app_bundle,
        "app_name": app_name,
        "w": w,
        "h": h,
        "make": make,
        "model": model,
        "os": os_str,
    })

    log_context(
        request_id=request_id,
        slot_id=sid,
        environment=env,
    )

    logger.debug(
        "VAST tag request received",
        request_id=request_id,
        environment=env,
        os=os_str,
        make=make,
        model=model,
        app_bundle=app_bundle,
        app_name=app_name,
        app_id=app_id,
        ip=ip,
        ua=ua,
        ifa=ifa,
        device_type=device_type,
        country=country_code,
        ct_genre=ct_genre,
        app_cat=app_cat,
    )

    # Build internal schemas -----------------------------------------------
    # Sanitise IP: if it looks like an unresolved macro, use real client IP
    raw_ip = (ip or "").strip()
    if not raw_ip or "{" in raw_ip or "[" in raw_ip or "%7B" in raw_ip.upper():
        raw_ip = extract_client_ip(
            x_forwarded_for=request.headers.get("x-forwarded-for"),
            request_client_host=request.client.host if request.client else None,
            x_real_ip=request.headers.get("x-real-ip"),
        ) or ""

    device = DeviceInfo(
        device_type="ctv" if env == "ctv" else "mobile",
        os=os_str.lower().replace(" ", "") or "unknown",
        os_version=osv or None,
        make=make or None,
        model=model or None,
        ifa=ifa,
        ifa_type=_detect_ifa_type(os_str, make),
        lmt=dnt == 1,
        ip=raw_ip,
        ua=ua_str,
        isp=isp or None,
        device_type_raw=device_type,
        language=device_language or None,
        didsha1=didsha1 or None,
        didmd5=didmd5 or None,
        screen_width=w,
        screen_height=h,
    )

    # Build GeoInfo — use publisher data if sent, otherwise enrich from IP
    _has_geo = (
        lat is not None or lon is not None or country_code
        or region or metro or city or zip_code
    )
    if _has_geo:
        geo = GeoInfo(
            ip=raw_ip,
            country=(country_code or "").strip() or None,
            region=region or None,
            city=city or None,
            dma=metro or None,
            latitude=lat,
            longitude=lon,
            zip_code=zip_code or None,
            geo_type=geo_type,
            ipservice=ipservice,
        )
    else:
        # ----- GeoIP enrichment from MaxMind -----
        geo = geoip_to_geo_info(raw_ip)

    app_info = AppInfo(
        app_name=app_name or ct_chan or None,
        app_bundle=app_bundle or None,
        store_url=app_store_url or None,
        app_category=app_cat or None,
        content_genre=ct_genre or content_type or None,
        content_rating=ct_rat or None,
        content_id=ct_id or None,
        content_title=ct_title or None,
        content_series=ct_ser or None,
        content_season=ct_seas or None,
        content_url=ct_url or None,
        content_language=ct_lang or None,
        content_livestream=ct_live_str,
        content_producer=ct_producer or None,
        production_quality=ct_prodq or None,
        qag_media_rating=ct_qa_media_rating or None,
        content_categories=app_cat or None,
        channel_name=ct_chan or None,
        network_name=ct_net or None,
        app_domain=app_domain or None,
        publisher_id=pub_id or None,
        page_categories=app_pagecat or None,
        content_episode=ct_episode or (int(ct_eps) if ct_eps and ct_eps.strip().isdigit() else None),
        content_length=ct_len,
        content_context=ct_context,
        content_gtax=ct_gtax,
        content_genres=ct_genres or None,
        inventory_partner_domain=inv_partner_domain or None,
        app_id=app_id or None,
    )

    video = VideoPlacementInfo(
        placement=_placement_from_params(startdelay),
        min_duration=min_dur,
        max_duration=max_dur,
        skip_enabled=False,
        width=w,
        height=h,
        mimes=["video/mp4"],
        startdelay_raw=startdelay,
        plcmt=plcmt,
        linearity=linearity,
        sequence=sequence,
        minbitrate=minbitrate,
        maxbitrate=maxbitrate,
        playbackmethod=playbackmethod or None,
        delivery=delivery or None,
        video_protocols=protocols or None,
        pod_duration=poddur,
        max_ads_in_pod=maxseq,
        podid=podid or None,
        podseq=podseq,
        poddedupe=poddedupe or None,
    )

    ad_request = AdRequest(
        request_id=request_id,
        slot_id=sid,
        environment=env,
        user_id=ifa,
        device=device,
        geo=geo,
        app=app_info,
        video=video,
        num_ads=1,
        geo_country=country_code or "",
        geo_region=region or None,
        geo_dma=metro or None,
        us_privacy=us_privacy or None,
        coppa=coppa,
        gdpr=gdpr,
        gdpr_consent=gdpr_consent or None,
        gpp=gpp or None,
        gpp_sid=gpp_sid or None,
        bcat=bcat or None,
        badv=badv or None,
        tagid=tagid or None,
        imp_exp=exp,
        bidfloor_override=bidfloor,
    )

    # Run pipeline ---------------------------------------------------------
    # Demand-only: forward to mapped ORTB/VAST demand sources.
    # Local campaigns are NOT used — all fill comes from demand partners.
    try:
        demand_candidates = await demand_forwarder.forward(
            ad_request=ad_request, request_id=request_id
        )

        if isinstance(demand_candidates, Exception):
            logger.warning(
                "Demand forwarding error",
                request_id=request_id,
                error=str(demand_candidates),
            )
            demand_candidates = []

        candidates = list(demand_candidates)
        candidates.sort(key=lambda c: c.bid, reverse=True)

        logger.debug(
            "Demand-only pipeline complete",
            request_id=request_id,
            demand_count=len(candidates),
        )
    except Exception:
        logger.exception("VAST tag pipeline error", request_id=request_id)
        # Track the ad request even on pipeline failure (no-fill)
        asyncio.create_task(EventService.track_ad_request(None))
        return _empty_vast_response(request_id)

    # ── Track ad_requests & ad_opportunities in Redis ─────────────
    # campaign_id = dashboard campaign (0 for demand-only).
    # creative_id = from demand response (DSP crid / VAST tag id).
    # Always include 0 (global bucket) alongside actual campaign IDs.
    _candidate_cids = list({c.campaign_id for c in candidates}) if candidates else []
    _track_cids = [0] + [c for c in _candidate_cids if c != 0]
    asyncio.create_task(EventService.track_ad_request(_track_cids))
    if candidates:
        asyncio.create_task(EventService.track_ad_opportunity(_track_cids))

    if not candidates:
        logger.debug("VAST tag no fill", request_id=request_id)
        return _empty_vast_response(request_id)

    # Take first candidate -------------------------------------------------
    candidate = candidates[0]
    ad_id = build_ad_id(candidate.campaign_id, candidate.creative_id)
    base_url = _resolve_base_url(request, settings)

    # Build tracking query params with demand source info for analytics
    _meta = candidate.metadata or {}
    _adm_raw = _meta.get("adm")

    # ── Creative ID extraction (multi-source priority chain) ──────────
    _crid_result = extract_creative_id(
        bid_crid=_meta.get("crid"),
        bid_adid=_meta.get("adid"),
        bid_id=_meta.get("bid_id"),
        adm=_adm_raw,
    )

    # ── Adomain extraction (multi-source priority chain) ──────────────
    _adomain_result = extract_adomain(
        bid_adomain=_meta.get("adomain"),
        adm=_adm_raw,
        bid_ext=_meta.get("bid_ext"),
    )

    # ── Determine adm type (inline / wrapper / nurl / vast_tag) ───────
    _adm_type, _wrapper_depth = detect_adm_type(
        adm=_adm_raw,
        vast_url=candidate.vast_url,
        has_nurl=bool(_meta.get("nurl")),
    )

    # For adm (DSP inline VAST), check for media files
    _has_media = True
    if _adm_raw:
        _parsed = _parse_adm_vast(_adm_raw)
        _has_media = _parsed["has_media"]
        if not _has_media:
            logger.warning(
                "DSP adm has no MediaFile – skipping",
                request_id=request_id,
            )
            return _empty_vast_response(request_id)

    # ── Build AdDecision canonical record ─────────────────────────────
    decision = AdDecision(
        request_id=request_id,
        imp_id=str(imp),
        bid_id=_meta.get("bid_id", ""),
        # Supply / context
        app_bundle=ad_request.app.app_bundle or "",
        app_name=ad_request.app.app_name or "",
        domain=ad_request.app.app_domain or "",
        publisher_id=ad_request.app.publisher_id or "",
        device_type=env,
        os=(ad_request.device.os or ""),
        geo_country=ad_request.geo.country or "",
        geo_region=ad_request.geo.region or "",
        ip=ad_request.device.ip or "",
        ifa=ad_request.device.ifa or "",
        supply_tag_id=sid,
        # Auction
        bid_floor=ad_request.bidfloor_override or 0.0,
        bid_price=_meta.get("bid_price", candidate.bid),
        net_price=candidate.bid,
        seat=_meta.get("seat", ""),
        deal_id=_meta.get("deal_id", ""),
        demand_endpoint_id=_meta.get("demand_endpoint_id", 0),
        demand_endpoint_name=_meta.get("endpoint_name", ""),
        # Creative identification (from extraction chain)
        creative_id=_crid_result.creative_id,
        creative_id_source=_crid_result.source,
        crid=_crid_result.crid,
        adid=_crid_result.adid,
        vast_creative_id=_crid_result.vast_creative_id,
        vast_ad_id=_crid_result.vast_ad_id,
        duration=candidate.duration,
        width=candidate.width,
        height=candidate.height,
        # Adomain (from extraction chain)
        adomain=_adomain_result.adomain,
        adomain_primary=_adomain_result.primary,
        adomain_source=_adomain_result.source,
        iab_categories=_meta.get("cat", []),
        # Markup / VAST
        adm_type=_adm_type,
        has_media=_has_media,
        vast_wrapper_depth=_wrapper_depth,
    )

    _adomain = _adomain_result.primary
    # Always brand source as viadsmedia.com (cross-platform ad server)
    _src = "viadsmedia.com"
    _bundle = ad_request.app.app_bundle or ""
    _country = ad_request.geo.country or ""
    _bid_price = round(candidate.bid, 4)

    # URL-safe extra params for demand analytics (now includes decision_id)
    _tracking_suffix = build_demand_extra_params(
        source=_src, adomain=_adomain, bundle=_bundle,
        country=_country, bid_price=_bid_price,
        supply_id=sid, campaign_id=candidate.campaign_id,
        decision_id=decision.decision_id,
    )

    # Build impression + error pixels only (tracking events come from demand)
    impression_url = build_impression_url(
        base_url, request_id, ad_id, env, _tracking_suffix,
    )
    error_url = build_error_url(
        base_url, request_id, ad_id, env, _tracking_suffix,
    )

    # nurl / burl (auction price notification)
    nurl = build_nurl(base_url, request_id, ad_id, env)
    burl = build_burl(base_url, request_id, ad_id, env)

    # Determine VAST version (prefer latest supported)
    vast_version = (
        settings.vast.supported_versions[-1]
        if settings.vast.supported_versions
        else "4.0"
    )

    # Choose InLine vs Wrapper depending on creative type
    if _adm_raw:
        # Demand ORTB bid with inline VAST XML (adm field)
        # Inject our error pixel + tracking events + impression pixel into the DSP's VAST XML.
        # Note: We must inject our own <Impression> alongside the DSP's to ensure revenue/delivery 
        # is counted on our backend. VAST standard supports multiple <Impression> pixels.
        vast_xml = _inject_tracking_into_adm(
            adm=_adm_raw,
            impression_url=impression_url,
            error_url=error_url,
        )
        # Fire the demand partner's nurl (win notification) in background
        demand_nurl = _meta.get("nurl")
        if demand_nurl:
            asyncio.create_task(
                _fire_win_notice(demand_nurl, candidate.bid)
            )
    else:
        # Wrapper (vast_url) or InLine (video_url) – shared helper handles both.
        vast_xml = build_vast_for_candidate(
            candidate,
            vast_version=vast_version,
            ad_id=ad_id,
            tracking_events=[],
            impression_url=impression_url,
            error_url=error_url,
            base_url=base_url,
            request_id=request_id,
            env=env,
            width=w,
            height=h,
            nurl=nurl,
            burl=burl,
        )
        if vast_xml is None:
            logger.warning(
                "Candidate has no video_url or vast_url – returning no-fill",
                request_id=request_id,
                ad_id=ad_id,
            )
            return _empty_vast_response(request_id)

    logger.debug(
        "VAST tag served",
        request_id=request_id,
        ad_id=ad_id,
        decision_id=decision.decision_id,
        creative_id=candidate.creative_id,
        creative_id_resolved=decision.creative_id,
        creative_id_source=decision.creative_id_source,
        adomain=decision.adomain_primary,
        adomain_source=decision.adomain_source,
        adm_type=decision.adm_type,
        cpm=round(candidate.bid, 4),
        environment=env,
        source=candidate.metadata.get("source", "local"),
    )

    # ── Persist AdDecision to Redis (non-blocking, TTL 48h) ───────────
    # Stored as a hash so event tracking can join decision context.
    asyncio.create_task(
        _store_ad_decision(decision)
    )

    return Response(
        content=vast_xml,
        media_type="application/xml",
        headers={
            **_VAST_OK_HEADERS,
            "X-Request-ID": request_id,
            "X-LiteAds-Environment": env,
        },
    )


# ---------------------------------------------------------------------------
# AdDecision persistence (Redis + optional DB)
# ---------------------------------------------------------------------------

_DECISION_TTL = 48 * 3600  # 48 hours — matches stat retention

async def _store_ad_decision(decision: AdDecision) -> None:
    """Store AdDecision in Redis (immediate lookup) and PostgreSQL (persistent).

    Redis key: ``decision:{decision_id}`` with 48-hour TTL.
    Events that arrive with ``did=`` in their tracking URL can look up the
    Redis hash to enrich analytics.  DB persistence enables historical
    creative/adomain analytics rollups.
    """
    # ── 1. Redis (fast, for real-time event joining) ──────────────────
    try:
        import json
        from liteads.common.cache import redis_client

        key = f"decision:{decision.decision_id}"
        data = {
            "request_id": decision.request_id,
            "imp_id": decision.imp_id,
            "bid_id": decision.bid_id,
            "app_bundle": decision.app_bundle,
            "app_name": decision.app_name,
            "domain": decision.domain,
            "device_type": decision.device_type,
            "os": decision.os,
            "geo_country": decision.geo_country,
            "geo_region": decision.geo_region,
            "supply_tag_id": decision.supply_tag_id,
            "bid_price": str(decision.bid_price),
            "net_price": str(decision.net_price),
            "seat": decision.seat,
            "deal_id": decision.deal_id,
            "demand_endpoint_id": str(decision.demand_endpoint_id),
            "demand_endpoint_name": decision.demand_endpoint_name,
            "creative_id": decision.creative_id,
            "creative_id_source": decision.creative_id_source,
            "crid": decision.crid,
            "adid": decision.adid,
            "vast_creative_id": decision.vast_creative_id,
            "vast_ad_id": decision.vast_ad_id,
            "duration": str(decision.duration),
            "adomain": json.dumps(decision.adomain),
            "adomain_primary": decision.adomain_primary,
            "adomain_source": decision.adomain_source,
            "iab_categories": json.dumps(decision.iab_categories),
            "adm_type": decision.adm_type,
            "has_media": "1" if decision.has_media else "0",
            "vast_wrapper_depth": str(decision.vast_wrapper_depth),
        }
        pipe = redis_client.pipeline()
        pipe.hset(key, mapping=data)
        pipe.expire(key, _DECISION_TTL)
        await pipe.execute()
    except Exception as exc:
        logger.warning("Failed to store AdDecision in Redis: %s", exc)

    # ── 2. PostgreSQL (persistent, for analytics rollups) ─────────────
    try:
        from liteads.common.database import db
        from liteads.models import AdDecisionLog

        async with db.session() as session:
            log_entry = AdDecisionLog(
                decision_id=decision.decision_id,
                request_id=decision.request_id,
                imp_id=decision.imp_id,
                bid_id=decision.bid_id,
                app_bundle=decision.app_bundle or None,
                app_name=decision.app_name or None,
                domain=decision.domain or None,
                publisher_id=decision.publisher_id or None,
                device_type=decision.device_type or None,
                os=decision.os or None,
                geo_country=decision.geo_country or None,
                geo_region=decision.geo_region or None,
                ip=decision.ip or None,
                supply_tag_id=decision.supply_tag_id or None,
                bid_floor=decision.bid_floor,
                bid_price=decision.bid_price,
                net_price=decision.net_price,
                seat=decision.seat or None,
                deal_id=decision.deal_id or None,
                demand_endpoint_id=decision.demand_endpoint_id or None,
                demand_endpoint_name=decision.demand_endpoint_name or None,
                creative_id_resolved=decision.creative_id or None,
                creative_id_source=decision.creative_id_source or None,
                crid=decision.crid or None,
                adid=decision.adid or None,
                vast_creative_id=decision.vast_creative_id or None,
                vast_ad_id=decision.vast_ad_id or None,
                duration=decision.duration,
                width=decision.width,
                height=decision.height,
                adomain_list=decision.adomain if decision.adomain else None,
                adomain_primary=decision.adomain_primary or None,
                adomain_source=decision.adomain_source or None,
                iab_categories=decision.iab_categories if decision.iab_categories else None,
                adm_type=decision.adm_type or None,
                has_media=decision.has_media,
                vast_wrapper_depth=decision.vast_wrapper_depth,
            )
            session.add(log_entry)
            # db.session() auto-commits on block exit
    except Exception as exc:
        logger.warning("Failed to persist AdDecision to DB: %s", exc)


def _inject_tracking_into_adm(
    adm: str,
    impression_url: str,
    error_url: str,
) -> str:
    """
    Inject LiteAds impression + error pixels into a DSP's VAST XML (adm).

    **CTV double-impression prevention:**  An ``<Impression>`` tag is only
    injected when *impression_url* is non-empty.  When the caller passes
    an empty string the DSP's own ``<Impression>`` remains the single
    source of truth, avoiding double-fire on CTV players.

    Tracking events (start, quartile, complete …) are NOT injected —
    they are provided solely by the demand/DSP response.
    """
    parts: list[str] = []

    # Only add our impression pixel if explicitly provided
    if impression_url:
        parts.append(
            f'<Impression><![CDATA[{impression_url}]]></Impression>'
        )
    parts.append(
        f'<Error><![CDATA[{error_url}]]></Error>'
    )

    inject_block = "\n        ".join(parts)
    inject_block = f"\n        {inject_block}"

    # Try to insert impression/error after <InLine> or <Wrapper> opening tag
    for tag in ("<InLine>", "<Wrapper>"):
        if tag in adm:
            return adm.replace(tag, f"{tag}{inject_block}", 1)

    # Fallback: insert after <Ad ...> tag
    ad_match = _AD_TAG_RE.search(adm)
    if ad_match:
        pos = ad_match.end()
        return adm[:pos] + inject_block + adm[pos:]

    # Last resort: return as-is
    return adm


def _parse_adm_vast(adm: str) -> dict:
    """Parse a DSP's VAST XML (adm) to extract Ad ID, Creative ID,
    and check for media files.

    Returns:
        dict with keys:
            ad_id: str or None - the <Ad id="..."> value
            creative_id: str or None - the <Creative id="..."> value
            has_media: bool - True if <MediaFile> is present
    """
    result = {"ad_id": None, "creative_id": None, "has_media": False}

    # Extract <Ad id="...">
    ad_match = _AD_ID_RE.search(adm)
    if ad_match:
        result["ad_id"] = ad_match.group(1)

    # Extract <Creative id="..."> (first one found)
    crid_match = _CREATIVE_ID_RE.search(adm)
    if crid_match:
        result["creative_id"] = crid_match.group(1)

    # Check for <MediaFile> presence (any tag containing media content)
    result["has_media"] = bool(
        _MEDIA_FILE_RE.search(adm)
        or _VAST_TAG_URI_RE.search(adm)  # Wrapper pointing to media
    )

    return result


async def _fire_win_notice(nurl: str, price: float) -> None:
    """
    Fire the demand partner's win notification URL in the background.

    Replaces ``${AUCTION_PRICE}`` macro with the actual clearing price.
    """
    try:
        resolved_url = nurl.replace("${AUCTION_PRICE}", str(round(price, 4)))
        client = _get_http_client()
        await client.get(resolved_url, timeout=2.0)
    except Exception as exc:
        logger.warning("Win notice failed: %s", str(exc))


def _resolve_base_url(request: Request, settings: Any) -> str:
    """Determine the public-facing base URL for tracking pixels.

    Priority order:
    1. ``settings.vast.tracking_base_url`` (explicit config override)
    2. ``X-Forwarded-Host`` + ``X-Forwarded-Proto`` headers (nginx proxy)
    3. ``Host`` header (direct or proxied with ``proxy_set_header Host``)
    4. ``request.base_url`` (fallback)

    This ensures tracking URLs always use the external domain rather than
    ``localhost`` or the internal container hostname.
    """
    # 1. Explicit config
    configured = getattr(settings, "vast", None)
    if configured and getattr(configured, "tracking_base_url", ""):
        return configured.tracking_base_url.rstrip("/")

    # 2. X-Forwarded-* from reverse proxy
    fwd_host = request.headers.get("x-forwarded-host")
    fwd_proto = request.headers.get("x-forwarded-proto", "http")
    if fwd_host:
        return f"{fwd_proto}://{fwd_host}".rstrip("/")

    # 3. Host header (nginx sets this via proxy_set_header Host $host)
    host_header = request.headers.get("host", "")
    if host_header and "localhost" not in host_header:
        scheme = request.url.scheme or "http"
        return f"{scheme}://{host_header}".rstrip("/")

    # 4. Fallback to request.base_url
    return str(request.base_url).rstrip("/")


def _empty_vast_response(request_id: str = "") -> Response:
    """Alias for the shared ``empty_vast_response`` in ``common.tracking``."""
    return empty_vast_response(request_id)


# ===========================================================================
# Publisher Tag Builder
# ===========================================================================

class TagBuilderRequest(BaseModel):
    """Request body for generating a VAST tag URL for a publisher to embed."""

    base_url: str = Field(
        ...,
        description="Server base URL (e.g. https://ads.example.com)",
        json_schema_extra={"example": "https://ads.example.com"},
    )
    slot_id: str = Field(
        "default", description="Ad slot / zone identifier"
    )
    environment: str = Field(
        "ctv", description="Target environment: ctv | inapp"
    )
    width: int = Field(1920, description="Video player width")
    height: int = Field(1080, description="Video player height")
    min_duration: int = Field(5, description="Minimum ad duration (s)")
    max_duration: int = Field(30, description="Maximum ad duration (s)")
    app_bundle: str | None = Field(None, description="App bundle ID (required for CTV/InApp)")
    app_name: str | None = Field(None, description="App name (required for CTV/InApp)")
    app_store_url: str | None = Field(None, description="App store URL (required for CTV/InApp per Adtelligent)")
    coppa: int = Field(0, description="COPPA flag (0/1)")
    gdpr: int | None = Field(None, description="GDPR applies (0/1)")
    us_privacy: str | None = Field(None, description="US Privacy / CCPA string")

    # These will be replaced by the video player at runtime
    include_device_macros: bool = Field(
        True,
        description="Include player-replaceable macros for UIP, UA, IFA, DNT, etc.",
    )


class TagBuilderResponse(BaseModel):
    """Generated VAST tag URL and embed instructions."""

    vast_tag_url: str = Field(..., description="Complete VAST tag URL to embed")
    macro_note: str = Field(
        "",
        description="Note about runtime macros that the player must replace",
    )
    example_curl: str = Field("", description="Example cURL command for testing")
    html_embed: str = Field("", description="HTML snippet for IMA SDK integration")


@router.post(
    "/tag-builder",
    response_model=TagBuilderResponse,
    summary="Generate VAST tag URL for publishers",
    description=(
        "Generates a ready-to-use VAST tag URL with the correct query parameters "
        "for a publisher's CTV or in-app video player. Returns the URL, an "
        "example cURL, and an HTML/IMA-SDK embed snippet."
    ),
)
async def build_publisher_tag(body: TagBuilderRequest) -> TagBuilderResponse:
    """Build a VAST tag URL that a publisher can embed in their video player."""
    base = body.base_url.rstrip("/")

    params: dict[str, Any] = {
        "sid": body.slot_id,
        "w": body.width,
        "h": body.height,
        "min_dur": body.min_duration,
        "max_dur": body.max_duration,
        "coppa": body.coppa,
    }

    if body.app_bundle:
        params["app_bundle"] = body.app_bundle
    if body.app_name:
        params["app_name"] = body.app_name
    if body.app_store_url:
        params["app_store_url"] = body.app_store_url
    if body.gdpr is not None:
        params["gdpr"] = body.gdpr
    if body.us_privacy:
        params["us_privacy"] = body.us_privacy

    # Add cache buster macro (most players replace [CACHEBUSTER] at runtime)
    params["cb"] = "[CACHEBUSTER]"

    macro_note = ""
    if body.include_device_macros:
        # Standard macros that video players / SDKs replace at runtime.
        # Uses Adtelligent-standard `uip` for user IP (not legacy `ip`).
        params["uip"] = "[UIP]"
        params["ua"] = "[UA]"
        params["ifa"] = "[IFA]"
        params["dnt"] = "[DNT]"
        params["os"] = "[OS]"
        params["device_make"] = "[MAKE]"
        params["device_model"] = "[MODEL]"
        macro_note = (
            "Replace [UIP], [UA], [IFA], [DNT], [OS], [MAKE], [MODEL], "
            "and [CACHEBUSTER] with actual runtime values. "
            "Most SSAI / IMA SDK / PAL implementations handle this automatically."
        )

    tag_url = f"{base}/api/vast?{urlencode(params, safe='[]')}"

    # Example cURL (with macros resolved to sample values)
    sample = tag_url.replace("[CACHEBUSTER]", "123456789")
    sample = sample.replace("[UIP]", "203.0.113.42")
    sample = sample.replace("[UA]", "Mozilla/5.0")
    sample = sample.replace("[IFA]", "00000000-0000-0000-0000-000000000000")
    sample = sample.replace("[DNT]", "0")
    sample = sample.replace("[OS]", "Roku")
    sample = sample.replace("[MAKE]", "Roku")
    sample = sample.replace("[MODEL]", "Ultra")

    html_embed = (
        '<script src="https://imasdk.googleapis.com/js/sdkloader/ima3.js"></script>\n'
        "<script>\n"
        "  var adsRequest = new google.ima.AdsRequest();\n"
        f'  adsRequest.adTagUrl = "{tag_url}";\n'
        "  adsLoader.requestAds(adsRequest);\n"
        "</script>"
    )

    return TagBuilderResponse(
        vast_tag_url=tag_url,
        macro_note=macro_note,
        example_curl=f'curl -s "{sample}"',
        html_embed=html_embed,
    )

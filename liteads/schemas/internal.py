"""
Internal data schemas for CPM CTV and In-App video ad serving pipeline.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AdCandidate:
    """Video ad candidate for CPM ranking."""

    campaign_id: int
    creative_id: int
    advertiser_id: int
    bid: float           # CPM bid amount
    bid_type: int = 1    # Always CPM (1)

    # Targeting match info
    targeting_score: float = 1.0

    # Predicted scores (for view-through optimization)
    pvtr: float = 0.0   # Predicted view-through rate (video completion)
    pctr: float = 0.0   # Predicted click-through rate

    # Calculated scores
    ecpm: float = 0.0   # For CPM, ecpm == bid
    score: float = 0.0  # Final ranking score

    # Video creative info
    title: str | None = None
    description: str | None = None
    video_url: str = ""
    vast_url: str | None = None
    companion_image_url: str | None = None
    landing_url: str = ""
    creative_type: int = 1       # CreativeType.CTV_VIDEO or INAPP_VIDEO
    duration: int = 30           # Video duration in seconds
    width: int = 1920
    height: int = 1080
    bitrate: int | None = None
    mime_type: str = "video/mp4"
    skippable: bool = True
    skip_after: int = 5
    placement: int = 1           # VideoPlacement.PRE_ROLL

    # Environment
    environment: int = 1         # Environment.CTV or INAPP

    # Extra info
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class UserContext:
    """User context for CTV/In-App video ad serving."""

    user_id: str | None = None
    user_hash: int = 0  # Hash for bucketing

    # Environment
    environment: str = "ctv"  # "ctv" or "inapp"

    # Device
    device_type: str = ""    # ctv, mobile, tablet, set_top_box
    os: str = ""             # roku, firetv, tvos, tizen, android, ios
    os_version: str = ""
    device_model: str = ""
    device_brand: str = ""
    ifa: str | None = None   # Identifier for advertising
    ifa_type: str | None = None

    # Geo
    ip: str = ""
    country: str = ""
    region: str = ""
    city: str = ""
    dma: str = ""            # Designated Market Area
    latitude: float | None = None
    longitude: float | None = None

    # App/Content
    app_id: str = ""
    app_name: str = ""
    app_bundle: str = ""
    content_genre: str = ""
    content_rating: str = ""
    content_id: str = ""
    network: str = ""

    # Video placement request
    placement: str = "pre_roll"
    min_duration: int | None = None
    max_duration: int | None = None
    skip_enabled: bool = True

    # Features (for ML)
    age: int | None = None
    gender: str | None = None
    interests: list[str] = field(default_factory=list)
    app_categories: list[str] = field(default_factory=list)
    custom_features: dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureVector:
    """Feature vector for ML prediction."""

    sparse_features: dict[str, int] = field(default_factory=dict)
    dense_features: list[float] = field(default_factory=list)
    feature_names: list[str] = field(default_factory=list)


@dataclass
class PredictionResult:
    """ML prediction result for video ads."""

    campaign_id: int
    creative_id: int
    pvtr: float = 0.0    # Predicted view-through rate
    pctr: float = 0.0    # Predicted click-through rate
    model_version: str = ""
    latency_ms: float = 0.0


@dataclass
class FrequencyInfo:
    """Frequency control information."""

    user_id: str
    campaign_id: int
    daily_count: int = 0
    hourly_count: int = 0
    daily_cap: int | None = None
    hourly_cap: int | None = None

    @property
    def is_capped(self) -> bool:
        """Check if frequency cap is reached."""
        if self.daily_cap and self.daily_count >= self.daily_cap:
            return True
        if self.hourly_cap and self.hourly_count >= self.hourly_cap:
            return True
        return False


@dataclass
class BudgetInfo:
    """Budget information for CPM campaigns."""

    campaign_id: int
    budget_daily: float | None = None
    budget_total: float | None = None
    spent_today: float = 0.0
    spent_total: float = 0.0

    @property
    def remaining_daily(self) -> float | None:
        """Get remaining daily budget."""
        if self.budget_daily is None:
            return None
        return max(0.0, self.budget_daily - self.spent_today)

    @property
    def remaining_total(self) -> float | None:
        """Get remaining total budget."""
        if self.budget_total is None:
            return None
        return max(0.0, self.budget_total - self.spent_total)

    @property
    def has_budget(self) -> bool:
        """Check if campaign has remaining budget."""
        if self.budget_daily and self.spent_today >= self.budget_daily:
            return False
        if self.budget_total and self.spent_total >= self.budget_total:
            return False
        return True


# =========================================================================
# Ad Decision canonical record
# =========================================================================

def _generate_decision_id() -> str:
    """Generate a unique decision ID (timestamp-based hex + random suffix)."""
    ts = int(time.time() * 1_000_000)  # microsecond precision
    rand = hashlib.md5(f"{ts}{time.perf_counter_ns()}".encode()).hexdigest()[:8]
    return f"d-{ts:x}-{rand}"


@dataclass
class AdDecision:
    """Canonical record for a single ad-serving decision.

    Grain: ``(request_id, imp_id, bid_id)``

    Captures the full context of *why* this ad was shown:
    - Supply / request context (app, device, geo)
    - Auction mechanics (floor, bid price, seat, deal, win/loss outcome)
    - Creative identification (multi-source priority chain)
    - Markup pointers (adm_type, vast_url, wrapper depth)
    - Adomain & category data for demand reporting

    The ``decision_id`` is the primary join key: it is embedded into all
    tracking URLs so that every downstream event (impression, start,
    quartile, complete, click, win, loss) can be joined back to this
    record.
    """

    # ── Join keys ─────────────────────────────────────────────────────
    decision_id: str = field(default_factory=_generate_decision_id)
    request_id: str = ""
    imp_id: str = "1"
    bid_id: str = ""

    # ── Supply / context ──────────────────────────────────────────────
    app_bundle: str = ""
    app_name: str = ""
    domain: str = ""                    # site.domain or app.domain
    publisher_id: str = ""
    device_type: str = ""               # ctv, mobile, tablet, set_top_box
    os: str = ""
    geo_country: str = ""
    geo_region: str = ""
    ip: str = ""
    ifa: str = ""
    supply_tag_id: str = ""             # Our supply tag slot_id

    # ── Auction ───────────────────────────────────────────────────────
    bid_floor: float = 0.0
    bid_price: float = 0.0              # Original DSP bid (pre-margin)
    net_price: float = 0.0              # After margin deduction
    clearing_price: float = 0.0         # Actual clearing price (from win nurl)
    seat: str = ""                      # DSP seat ID from seatbid
    deal_id: str = ""                   # PMP deal ID (if any)
    auction_type: int = 2               # 1=first-price, 2=second-price
    demand_endpoint_id: int = 0         # Internal DemandEndpoint.id
    demand_endpoint_name: str = ""

    # ── Creative identification (multi-source priority chain) ─────────
    # Final resolved IDs after applying priority chain:
    #   bid.crid → bid.adid → VAST Creative@id → VAST Ad@id → hash
    creative_id: str = ""               # Best-available creative identifier
    creative_id_source: str = ""        # Where creative_id came from
    crid: str = ""                      # Raw OpenRTB crid from bid
    adid: str = ""                      # Raw OpenRTB adid from bid
    vast_creative_id: str = ""          # <Creative id="..."> from VAST XML
    vast_ad_id: str = ""                # <Ad id="..."> from VAST XML
    duration: int = 0
    width: int = 0
    height: int = 0

    # ── Adomain (multi-source priority chain) ─────────────────────────
    # Priority: bid.adomain[] → ClickThrough domain → extension → null
    adomain: list[str] = field(default_factory=list)
    adomain_primary: str = ""           # First adomain for reporting
    adomain_source: str = ""            # "ortb" | "clickthrough" | "ext" | ""
    iab_categories: list[str] = field(default_factory=list)

    # ── Markup / VAST ─────────────────────────────────────────────────
    adm_type: str = ""                  # "inline" | "wrapper" | "nurl" | "vast_tag"
    has_media: bool = False             # True if inline VAST has <MediaFile>
    vast_wrapper_depth: int = 0         # Number of wrapper hops (0 = inline)

    # ── Timestamps ────────────────────────────────────────────────────
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for Redis/JSON storage."""
        return {
            "decision_id": self.decision_id,
            "request_id": self.request_id,
            "imp_id": self.imp_id,
            "bid_id": self.bid_id,
            "app_bundle": self.app_bundle,
            "app_name": self.app_name,
            "domain": self.domain,
            "publisher_id": self.publisher_id,
            "device_type": self.device_type,
            "os": self.os,
            "geo_country": self.geo_country,
            "geo_region": self.geo_region,
            "ip": self.ip,
            "supply_tag_id": self.supply_tag_id,
            "bid_floor": self.bid_floor,
            "bid_price": self.bid_price,
            "net_price": self.net_price,
            "seat": self.seat,
            "deal_id": self.deal_id,
            "demand_endpoint_id": self.demand_endpoint_id,
            "demand_endpoint_name": self.demand_endpoint_name,
            "creative_id": self.creative_id,
            "creative_id_source": self.creative_id_source,
            "crid": self.crid,
            "adid": self.adid,
            "vast_creative_id": self.vast_creative_id,
            "vast_ad_id": self.vast_ad_id,
            "duration": self.duration,
            "width": self.width,
            "height": self.height,
            "adomain": self.adomain,
            "adomain_primary": self.adomain_primary,
            "adomain_source": self.adomain_source,
            "iab_categories": self.iab_categories,
            "adm_type": self.adm_type,
            "has_media": self.has_media,
            "vast_wrapper_depth": self.vast_wrapper_depth,
            "created_at": self.created_at,
        }

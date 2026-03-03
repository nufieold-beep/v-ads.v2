"""
API request schemas for CPM CTV and In-App video ad serving.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class DeviceInfo(BaseModel):
    """Device information for CTV and mobile/tablet."""

    device_type: str = Field(
        ..., description="Device type: ctv, mobile, tablet, set_top_box, phone"
    )
    os: str = Field(
        ..., description="Operating system (roku/firetv/tvos/tizen/webos/android/ios)"
    )
    os_version: str | None = Field(None, description="OS version")
    make: str | None = Field(None, description="Device manufacturer (e.g., ROKU, LG, Amazon, Samsung)")
    model: str | None = Field(None, description="Device model (e.g., DIGITAL VIDEO PLAYER, 50UN6950ZUF)")
    brand: str | None = Field(None, description="Device brand (alias for make)")
    screen_width: int | None = Field(None, description="Screen width in pixels")
    screen_height: int | None = Field(None, description="Screen height in pixels")
    language: str | None = Field(None, description="Device language")
    ifa: str | None = Field(None, description="Identifier for advertising (RIDA/AFAI/IDFA/GAID/TIFA/LGUDID)")
    ifa_type: str | None = Field(None, description="IFA type (rida/afai/idfa/gaid/tifa/lgudid/vida)")
    lmt: bool | None = Field(None, description="Limit ad tracking / do not track flag")
    ip: str | None = Field(None, description="IP address")
    ua: str | None = Field(None, description="User-Agent string")
    isp: str | None = Field(None, description="Internet Service Provider")
    connection_type: str | None = Field(None, description="Connection type (wifi/ethernet/cellular)")
    device_type_raw: int | None = Field(None, description="Publisher-provided OpenRTB device type (1=mobile, 3=connected device, 7=set-top box)")
    didsha1: str | None = Field(None, description="Hardware device ID SHA1 hash")
    didmd5: str | None = Field(None, description="Hardware device ID MD5 hash")


class GeoInfo(BaseModel):
    """Geographic information."""

    ip: str | None = Field(None, description="IP address")
    country: str | None = Field(None, description="Country code (ISO 3166-1 alpha-2)")
    region: str | None = Field(None, description="Region/Province")
    city: str | None = Field(None, description="City name")
    dma: str | None = Field(None, description="DMA (Designated Market Area) code")
    latitude: float | None = Field(None, description="Latitude")
    longitude: float | None = Field(None, description="Longitude")
    zip_code: str | None = Field(None, description="Postal / ZIP code")
    geo_type: int | None = Field(None, description="Location type (1=GPS, 2=IP, 3=User)")
    ipservice: int | None = Field(None, description="IP geolocation service (1=ip2location, 2=Neustar, 3=MaxMind)")


class AppInfo(BaseModel):
    """Application/content information for CTV and In-App."""

    app_id: str | None = Field(None, description="App identifier")
    app_name: str | None = Field(None, description="App name (e.g., Pluto TV, Tubi)")
    app_bundle: str | None = Field(None, description="App bundle ID")
    app_version: str | None = Field(None, description="App version")
    store_url: str | None = Field(None, description="App store URL")
    app_category: str | None = Field(None, description="App IAB content category")
    content_genre: str | None = Field(None, description="Content genre (news/sports/entertainment)")
    content_rating: str | None = Field(None, description="Content rating (G/PG/PG-13/R)")
    content_id: str | None = Field(None, description="Content/channel identifier")
    content_title: str | None = Field(None, description="Content title")
    content_series: str | None = Field(None, description="Content series")
    content_season: str | None = Field(None, description="Content season")
    content_url: str | None = Field(None, description="Content URL")
    content_language: str | None = Field(None, description="Content language")
    content_livestream: int | None = Field(None, description="Live stream flag (0/1)")
    content_producer: str | None = Field(None, description="Content producer name")
    production_quality: str | None = Field(None, description="Production quality (IAB)")
    qag_media_rating: str | None = Field(None, description="QAG media rating")
    content_categories: str | None = Field(None, description="Content categories (IAB)")
    channel_name: str | None = Field(None, description="Content channel name")
    network_name: str | None = Field(None, description="Network name")
    app_domain: str | None = Field(None, description="App domain (e.g. verylocal.com)")
    publisher_id: str | None = Field(None, description="Publisher ID")
    page_categories: str | None = Field(None, description="Page-level IAB categories (comma-separated)")
    content_episode: int | None = Field(None, description="Content episode number")
    content_context: int | None = Field(None, description="Content context (1=video, 2=game, 3=music, 4=app)")
    content_gtax: int | None = Field(None, description="Content genre taxonomy ID")
    content_genres: str | None = Field(None, description="Genre codes from taxonomy (comma-separated)")
    content_length: int | None = Field(None, description="Content length / duration (seconds)")
    inventory_partner_domain: str | None = Field(None, description="Inventory partner domain")


class VideoPlacementInfo(BaseModel):
    """Video ad placement details."""

    placement: str = Field(
        "pre_roll", description="Placement type: pre_roll, mid_roll, post_roll"
    )
    min_duration: int | None = Field(None, ge=1, description="Min accepted video duration (seconds)")
    max_duration: int | None = Field(None, ge=1, description="Max accepted video duration (seconds)")
    skip_enabled: bool = Field(True, description="Whether skip is allowed")
    player_width: int | None = Field(None, description="Video player width")
    player_height: int | None = Field(None, description="Video player height")
    mimes: list[str] | None = Field(
        None, description="Accepted MIME types (e.g., video/mp4, application/x-mpegURL)"
    )
    protocols: list[int] | None = Field(
        None, description="Supported VAST protocols (2=VAST 2.0, 3=VAST 3.0, 6=VAST 4.0, etc.)"
    )
    width: int | None = Field(None, description="Video width (alias for player_width)")
    height: int | None = Field(None, description="Video height (alias for player_height)")
    # Raw video params from publisher
    startdelay_raw: int | None = Field(None, description="Raw startdelay value (0=pre, >0=mid, -1=mid, -2=post)")
    plcmt: int | None = Field(None, description="OpenRTB 2.6 video placement type")
    linearity: int | None = Field(None, description="1=Linear (in-stream), 2=Non-linear")
    sequence: int | None = Field(None, description="Sequence number within pod")
    minbitrate: int | None = Field(None, description="Minimum bitrate (kbps)")
    maxbitrate: int | None = Field(None, description="Maximum bitrate (kbps)")
    playbackmethod: str | None = Field(None, description="Playback methods (comma-separated ints)")
    delivery: str | None = Field(None, description="Delivery methods (comma-separated ints)")
    podid: str | None = Field(None, description="Pod identifier")
    podseq: int | None = Field(None, description="Pod sequence (0=any, 1=first, -1=last)")
    poddedupe: str | None = Field(None, description="Pod deduplication signals (comma-separated ints)")
    video_protocols: str | None = Field(None, description="VAST protocols (comma-separated ints)")
    # Pod support
    pod_duration: int | None = Field(None, description="Total pod duration (seconds)")
    max_ads_in_pod: int | None = Field(None, description="Maximum ads in the pod")


class UserFeatures(BaseModel):
    """User feature information for ML prediction."""

    age: int | None = Field(None, ge=0, le=120, description="User age")
    gender: str | None = Field(None, description="User gender (male/female/unknown)")
    interests: list[str] | None = Field(None, description="User interests")
    app_categories: list[str] | None = Field(None, description="Installed app categories")
    custom: dict[str, Any] | None = Field(None, description="Custom features")


class AdRequest(BaseModel):
    """Video ad request schema for CTV and In-App environments."""

    request_id: str | None = Field(None, description="Unique request/auction ID (auto-generated if absent)")
    slot_id: str = Field("default", description="Ad slot identifier")
    environment: Literal["ctv", "inapp"] = Field(
        ..., description="Ad environment: ctv (Connected TV) or inapp (In-App mobile/tablet)"
    )
    user_id: str | None = Field(None, description="User identifier (IFA/RIDA/custom)")
    device: DeviceInfo | None = Field(None, description="Device information")
    geo: GeoInfo | None = Field(None, description="Geographic information")
    app: AppInfo | None = Field(None, description="Application/content information")
    video: VideoPlacementInfo = Field(
        default_factory=VideoPlacementInfo, description="Video placement details"
    )
    user_features: UserFeatures | None = Field(None, description="User features for ML")
    num_ads: int = Field(1, ge=1, le=10, description="Number of video ads requested")
    bid_floor: float | None = Field(None, ge=0, description="Minimum CPM bid floor from SSP")

    # Flattened geo fields (populated by OpenRTB service or directly)
    geo_country: str | None = Field(None, description="Country code (ISO 3166-1)")
    geo_region: str | None = Field(None, description="Region/state code")
    geo_dma: str | None = Field(None, description="Nielsen DMA code")

    # Privacy / Regulatory
    us_privacy: str | None = Field(None, description="US Privacy string (CCPA)")
    coppa: int | None = Field(None, description="COPPA flag (0/1)")
    gdpr: int | None = Field(None, description="GDPR applies flag (0/1)")
    gdpr_consent: str | None = Field(None, description="TCF consent string")
    gpp: str | None = Field(None, description="IAB Global Privacy Platform string")
    gpp_sid: str | None = Field(None, description="GPP section IDs (comma-separated)")

    # Blocked signals
    bcat: str | None = Field(None, description="Blocked IAB categories (comma-separated)")
    badv: str | None = Field(None, description="Blocked advertiser domains (comma-separated)")

    # Impression overrides from publisher
    tagid: str | None = Field(None, description="Publisher-provided tag/placement ID")
    imp_exp: int | None = Field(None, description="Impression expiry (seconds)")
    bidfloor_override: float | None = Field(None, ge=0, description="Publisher bid floor override")

    model_config = {
        "json_schema_extra": {
            "example": {
                "slot_id": "ctv_preroll_main",
                "environment": "ctv",
                "user_id": "rida_abc123",
                "device": {
                    "device_type": "ctv",
                    "os": "roku",
                    "os_version": "12.0",
                    "model": "Roku Ultra",
                    "brand": "Roku",
                    "screen_width": 3840,
                    "screen_height": 2160,
                    "ifa": "rida_abc123",
                    "ifa_type": "rida",
                },
                "geo": {
                    "ip": "1.2.3.4",
                    "country": "US",
                    "dma": "501",
                },
                "app": {
                    "app_id": "com.pluto.tv",
                    "app_name": "Pluto TV",
                    "content_genre": "entertainment",
                },
                "video": {
                    "placement": "pre_roll",
                    "max_duration": 30,
                    "skip_enabled": True,
                    "mimes": ["video/mp4"],
                },
                "num_ads": 1,
            }
        }
    }


class EventRequest(BaseModel):
    """Video event tracking request schema.

    Supports VAST-standard video events plus impression/click.
    """

    request_id: str = Field(..., description="Original ad request ID")
    ad_id: str = Field(..., description="Ad identifier")
    event_type: str = Field(
        ...,
        description="Event type: impression, start, firstQuartile, midpoint, "
                    "thirdQuartile, complete, click, skip, mute, unmute, "
                    "pause, resume, fullscreen, error"
    )
    timestamp: int | None = Field(None, description="Event timestamp (Unix epoch)")
    user_id: str | None = Field(None, description="User identifier")
    environment: Literal["ctv", "inapp"] | None = Field(
        None, description="Ad environment"
    )
    video_position: int | None = Field(
        None, description="Video playback position in seconds when event fired"
    )
    extra: dict[str, Any] | None = Field(None, description="Extra event data")

    model_config = {
        "json_schema_extra": {
            "example": {
                "request_id": "req_abc123",
                "ad_id": "ad_100_200",
                "event_type": "complete",
                "timestamp": 1700000000,
                "user_id": "rida_abc123",
                "environment": "ctv",
                "video_position": 30,
            }
        }
    }

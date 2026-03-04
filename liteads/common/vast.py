"""
VAST XML Generator – Versions 2.0 through 4.2.

Generates standards-compliant VAST (Video Ad Serving Template) XML markup
for CTV and in-app video ad delivery.

References:
  - VAST 2.0: https://www.iab.com/guidelines/vast/
  - VAST 3.0: https://www.iab.com/guidelines/vast/
  - VAST 4.0 / 4.1 / 4.2: https://iabtechlab.com/standards/vast/
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from xml.etree.ElementTree import Element, SubElement, tostring


# ---------------------------------------------------------------------------
# Data models for VAST generation
# ---------------------------------------------------------------------------

class VASTVersion(str, Enum):
    V2_0 = "2.0"
    V3_0 = "3.0"
    V4_0 = "4.0"
    V4_1 = "4.1"
    V4_2 = "4.2"


@dataclass(slots=True)
class TrackingEvent:
    """A single VAST tracking event."""
    event: str          # e.g. "start", "firstQuartile", "midpoint", "thirdQuartile", "complete"
    url: str


@dataclass
class CompanionAd:
    """Companion banner alongside video."""
    width: int
    height: int
    static_resource: str                # Image URL
    resource_type: str = "image/png"    # MIME type of the companion
    click_through: Optional[str] = None
    tracking_events: list[TrackingEvent] = field(default_factory=list)


@dataclass
class MediaFile:
    """A single video media file."""
    url: str
    delivery: str = "progressive"       # "progressive" | "streaming"
    type: str = "video/mp4"             # MIME type
    bitrate: int = 2500                 # kbps
    width: int = 1920
    height: int = 1080
    codec: Optional[str] = None
    scalable: bool = True
    maintain_aspect_ratio: bool = True


@dataclass
class VASTCreative:
    """All data needed to build one <Creative> / <Linear> element."""

    ad_id: str
    creative_id: str
    ad_title: str = "Video Ad"
    description: str = ""
    advertiser: str = ""
    duration: int = 30                  # seconds
    skip_offset: Optional[int] = None   # seconds (None = non-skippable)
    click_through: Optional[str] = None
    click_tracking: list[str] = field(default_factory=list)

    # Impression / error / survey URLs
    impression_urls: list[str] = field(default_factory=list)
    error_urls: list[str] = field(default_factory=list)
    survey_url: Optional[str] = None

    # nurl / burl
    nurl: Optional[str] = None
    burl: Optional[str] = None

    # Media
    media_files: list[MediaFile] = field(default_factory=list)
    companion_ads: list[CompanionAd] = field(default_factory=list)

    # Tracking pixels
    tracking_events: list[TrackingEvent] = field(default_factory=list)

    # VAST 4.x extensions
    ad_serving_id: Optional[str] = None
    category: Optional[str] = None
    viewable_impression: Optional[str] = None   # Viewable impression URL
    not_viewable_url: Optional[str] = None
    view_undetermined_url: Optional[str] = None

    # Ad verification (DoubleVerify, IAS, MOAT, etc.)
    verification_vendors: list[dict] = field(default_factory=list)
    # Each entry: {"vendor": "doubleverify.com", "js_url": "...", "params": "..."}

    # Pricing (VAST 4.x)
    price: Optional[float] = None
    price_model: str = "cpm"
    price_currency: str = "USD"


# ---------------------------------------------------------------------------
# VAST XML Builder
# ---------------------------------------------------------------------------

def _format_duration(seconds: int) -> str:
    """Format seconds as HH:MM:SS."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _cdata(text: str) -> str:
    """Wrap text in CDATA."""
    return f"<![CDATA[{text}]]>"


def _add_cdata_element(parent: Element, tag: str, text: str) -> Element:
    """Add a child element whose text content is CDATA-wrapped."""
    el = SubElement(parent, tag)
    # ElementTree doesn't support CDATA natively; we'll post-process
    el.text = f"__CDATA__{text}__ENDCDATA__"
    return el


# Regex to find CDATA sections for unescaping
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)


def _unescape_cdata(xml: str) -> str:
    """Reverse ElementTree's escaping inside CDATA blocks.

    ElementTree escapes ``&`` → ``&amp;``, ``<`` → ``&lt;``, ``>`` → ``&gt;``
    in text nodes.  Inside ``<![CDATA[...]]>`` the content is literal, so
    we must undo those escapes.
    """
    def _unescape(m: re.Match) -> str:
        inner = m.group(1)
        inner = inner.replace("&amp;", "&")
        inner = inner.replace("&lt;", "<")
        inner = inner.replace("&gt;", ">")
        return f"<![CDATA[{inner}]]>"
    return _CDATA_RE.sub(_unescape, xml)


class VASTBuilder:
    """
    Build VAST XML documents natively via fast string concatenation (versions 2.0 - 4.2).
    """
    def __init__(self, version: str = "4.0"):
        self.version = VASTVersion(version)

    def _is_v4(self) -> bool:
        return self.version in (VASTVersion.V4_0, VASTVersion.V4_1, VASTVersion.V4_2)

    def build(self, c: VASTCreative) -> str:
        """Generate the full VAST XML string natively."""
        v = self.version.value
        is_v4 = self._is_v4()
        
        ad_attrs = f' id="{c.ad_id}"'
        if is_v4 and c.ad_serving_id:
            ad_attrs += f' adServingId="{c.ad_serving_id}"'
            
        parts = [f'<?xml version="1.0" encoding="UTF-8"?>\n<VAST version="{v}"><Ad{ad_attrs}><InLine>']
        parts.append('<AdSystem version="1.0">LiteAds</AdSystem>')
        parts.append(f'<AdTitle><![CDATA[{c.ad_title}]]></AdTitle>')
        
        if c.description and self.version != VASTVersion.V2_0:
            parts.append(f'<Description><![CDATA[{c.description}]]></Description>')
            
        if c.advertiser and self.version != VASTVersion.V2_0:
            parts.append(f'<Advertiser><![CDATA[{c.advertiser}]]></Advertiser>')
            
        if c.category and is_v4:
            parts.append(f'<Category authority="https://iabtechlab.com"><![CDATA[{c.category}]]></Category>')
            
        if c.price is not None and is_v4:
            parts.append(f'<Pricing model="{c.price_model}" currency="{c.price_currency}"><![CDATA[{c.price:.4f}]]></Pricing>')
            
        if c.survey_url and self.version != VASTVersion.V2_0:
            parts.append(f'<Survey><![CDATA[{c.survey_url}]]></Survey>')
            
        for url in c.error_urls:
            error_url = url
            if "[ERRORCODE]" not in error_url:
                sep = "&" if "?" in error_url else "?"
                error_url = f"{error_url}{sep}err=[ERRORCODE]"
            parts.append(f'<Error><![CDATA[{error_url}]]></Error>')
            
        for url in c.impression_urls:
            parts.append(f'<Impression><![CDATA[{url}]]></Impression>')
            
        if is_v4 and c.viewable_impression:
            parts.append('<ViewableImpression>')
            parts.append(f'<Viewable><![CDATA[{c.viewable_impression}]]></Viewable>')
            if c.not_viewable_url:
                parts.append(f'<NotViewable><![CDATA[{c.not_viewable_url}]]></NotViewable>')
            if c.view_undetermined_url:
                parts.append(f'<ViewUndetermined><![CDATA[{c.view_undetermined_url}]]></ViewUndetermined>')
            parts.append('</ViewableImpression>')
            
        if is_v4 and c.verification_vendors:
            parts.append('<AdVerifications>')
            for vendor in c.verification_vendors:
                v_attr = f' vendor="{vendor["vendor"]}"' if vendor.get("vendor") else ""
                parts.append(f'<Verification{v_attr}>')
                if vendor.get("js_url"):
                    parts.append(f'<JavaScriptResource apiFramework="omid" browserOptional="true"><![CDATA[{vendor["js_url"]}]]></JavaScriptResource>')
                if vendor.get("params"):
                    parts.append(f'<VerificationParameters><![CDATA[{vendor["params"]}]]></VerificationParameters>')
                parts.append('</Verification>')
            parts.append('</AdVerifications>')
            
        parts.append('<Creatives>')
        
        parts.append(f'<Creative id="{c.creative_id}">')
        skip_attr = f' skipoffset="{_format_duration(c.skip_offset)}"' if c.skip_offset is not None else ""
        parts.append(f'<Linear{skip_attr}>')
        parts.append(f'<Duration>{_format_duration(c.duration)}</Duration>')
        
        if c.tracking_events:
            parts.append('<TrackingEvents>')
            for te in c.tracking_events:
                parts.append(f'<Tracking event="{te.event}"><![CDATA[{te.url}]]></Tracking>')
            parts.append('</TrackingEvents>')
            
        parts.append('<VideoClicks>')
        if c.click_through:
            parts.append(f'<ClickThrough><![CDATA[{c.click_through}]]></ClickThrough>')
        for ct_url in c.click_tracking:
            parts.append(f'<ClickTracking><![CDATA[{ct_url}]]></ClickTracking>')
        parts.append('</VideoClicks>')
        
        parts.append('<MediaFiles>')
        for mf in c.media_files:
            attrs = [
                f'delivery="{mf.delivery}"',
                f'type="{mf.type}"',
                f'bitrate="{mf.bitrate}"',
                f'width="{mf.width}"',
                f'height="{mf.height}"',
                f'scalable="{str(mf.scalable).lower()}"',
                f'maintainAspectRatio="{str(mf.maintain_aspect_ratio).lower()}"'
            ]
            if mf.codec:
                attrs.append(f'codec="{mf.codec}"')
            parts.append(f'<MediaFile {" ".join(attrs)}><![CDATA[{mf.url}]]></MediaFile>')
        parts.append('</MediaFiles>')
        parts.append('</Linear></Creative>')
        
        if c.companion_ads:
            parts.append('<Creative><CompanionAds>')
            for comp in c.companion_ads:
                parts.append(f'<Companion width="{comp.width}" height="{comp.height}">')
                parts.append(f'<StaticResource creativeType="{comp.resource_type}"><![CDATA[{comp.static_resource}]]></StaticResource>')
                if comp.click_through:
                    parts.append(f'<CompanionClickThrough><![CDATA[{comp.click_through}]]></CompanionClickThrough>')
                if comp.tracking_events:
                    parts.append('<TrackingEvents>')
                    for te in comp.tracking_events:
                        parts.append(f'<Tracking event="{te.event}"><![CDATA[{te.url}]]></Tracking>')
                    parts.append('</TrackingEvents>')
                parts.append('</Companion>')
            parts.append('</CompanionAds></Creative>')
            
        parts.append('</Creatives></InLine></Ad></VAST>')
        return "".join(parts)

    def build_wrapper(self, vast_tag_uri: str, creative: VASTCreative) -> str:
        """Generate a VAST Wrapper natively via list comprehension and joins."""
        v = self.version.value
        fallback = ' fallbackOnNoAd="true"' if self.version != VASTVersion.V2_0 else ''
        
        parts = [f'<?xml version="1.0" encoding="UTF-8"?>\n<VAST version="{v}"><Ad id="{creative.ad_id}"><Wrapper{fallback}>']
        parts.append('<AdSystem version="1.0">LiteAds</AdSystem>')
        parts.append(f'<VASTAdTagURI><![CDATA[{vast_tag_uri}]]></VASTAdTagURI>')
        
        for url in creative.impression_urls:
            parts.append(f'<Impression><![CDATA[{url}]]></Impression>')
            
        for url in creative.error_urls:
            error_url = url
            if "[ERRORCODE]" not in error_url:
                sep = "&" if "?" in error_url else "?"
                error_url = f"{error_url}{sep}err=[ERRORCODE]"
            parts.append(f'<Error><![CDATA[{error_url}]]></Error>')
            
        has_tracking = bool(creative.tracking_events)
        has_clicks = bool(creative.click_tracking)
        
        if has_tracking or has_clicks:
            parts.append('<Creatives><Creative><Linear>')
            if has_tracking:
                parts.append('<TrackingEvents>')
                for te in creative.tracking_events:
                    parts.append(f'<Tracking event="{te.event}"><![CDATA[{te.url}]]></Tracking>')
                parts.append('</TrackingEvents>')
            if has_clicks:
                parts.append('<VideoClicks>')
                for ct_url in creative.click_tracking:
                    parts.append(f'<ClickTracking><![CDATA[{ct_url}]]></ClickTracking>')
                parts.append('</VideoClicks>')
            parts.append('</Linear></Creative></Creatives>')
            
        parts.append('</Wrapper></Ad></VAST>')
        return "".join(parts)


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

# Module-level builder cache — only a handful of VAST versions exist,
# so caching avoids per-request VASTBuilder instantiation.
_builder_cache: dict[str, VASTBuilder] = {}


def _get_builder(version: str) -> VASTBuilder:
    """Return a cached VASTBuilder for *version* (thread-safe for asyncio)."""
    b = _builder_cache.get(version)
    if b is None:
        b = VASTBuilder(version=version)
        _builder_cache[version] = b
    return b


def build_vast_xml(
    *,
    version: str = "4.0",
    ad_id: str,
    creative_id: str,
    ad_title: str = "Video Ad",
    duration: int = 30,
    video_url: str,
    video_mime: str = "video/mp4",
    bitrate: int = 2500,
    width: int = 1920,
    height: int = 1080,
    click_through: Optional[str] = None,
    skip_offset: Optional[int] = None,
    impression_urls: Optional[list[str]] = None,
    error_urls: Optional[list[str]] = None,
    tracking_events: Optional[list[TrackingEvent]] = None,
    companion_image_url: Optional[str] = None,
    companion_width: int = 300,
    companion_height: int = 250,
    nurl: Optional[str] = None,
    burl: Optional[str] = None,
    price: Optional[float] = None,
    advertiser: str = "",
    category: Optional[str] = None,
) -> str:
    """
    One-shot convenience function to produce VAST XML.

    Example::

        xml = build_vast_xml(
            ad_id="123",
            creative_id="456",
            video_url="https://cdn.example.com/video.mp4",
            duration=30,
            impression_urls=["https://track.example.com/imp"],
            tracking_events=[
                TrackingEvent("start", "https://track.example.com/start"),
                TrackingEvent("complete", "https://track.example.com/complete"),
            ],
        )
    """
    media = MediaFile(
        url=video_url,
        type=video_mime,
        bitrate=bitrate,
        width=width,
        height=height,
    )

    companions: list[CompanionAd] = []
    if companion_image_url:
        companions.append(CompanionAd(
            width=companion_width,
            height=companion_height,
            static_resource=companion_image_url,
        ))

    creative = VASTCreative(
        ad_id=ad_id,
        creative_id=creative_id,
        ad_title=ad_title,
        advertiser=advertiser,
        duration=duration,
        skip_offset=skip_offset,
        click_through=click_through,
        impression_urls=impression_urls or [],
        error_urls=error_urls or [],
        media_files=[media],
        companion_ads=companions,
        tracking_events=tracking_events or [],
        nurl=nurl,
        burl=burl,
        price=price,
        price_model="cpm",
        category=category,
    )

    builder = _get_builder(version)
    return builder.build(creative)


def build_vast_wrapper_xml(
    *,
    version: str = "4.0",
    ad_id: str,
    creative_id: str,
    vast_tag_uri: str,
    ad_title: str = "Video Ad",
    impression_urls: Optional[list[str]] = None,
    error_urls: Optional[list[str]] = None,
    tracking_events: Optional[list[TrackingEvent]] = None,
    click_tracking: Optional[list[str]] = None,
    nurl: Optional[str] = None,
    burl: Optional[str] = None,
    price: Optional[float] = None,
) -> str:
    """
    One-shot convenience function to produce a VAST Wrapper XML document.

    Use this when the creative has an external VAST tag URL (``vast_url``)
    rather than a direct video file.  The wrapper redirects the video player
    to ``vast_tag_uri`` while injecting the ad server's own impression and
    tracking pixels.

    Example::

        xml = build_vast_wrapper_xml(
            ad_id="123",
            creative_id="456",
            vast_tag_uri="https://dsp.example.com/vast?id=abc",
            impression_urls=["https://track.example.com/imp"],
            tracking_events=[
                TrackingEvent("start", "https://track.example.com/start"),
                TrackingEvent("complete", "https://track.example.com/complete"),
            ],
        )
    """
    creative = VASTCreative(
        ad_id=ad_id,
        creative_id=creative_id,
        ad_title=ad_title,
        impression_urls=impression_urls or [],
        error_urls=error_urls or [],
        tracking_events=tracking_events or [],
        click_tracking=click_tracking or [],
        nurl=nurl,
        burl=burl,
        price=price,
        price_model="cpm",
    )

    builder = _get_builder(version)
    return builder.build_wrapper(vast_tag_uri, creative)

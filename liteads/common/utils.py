"""
Utility functions for LiteAds.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import orjson


def generate_request_id() -> str:
    """Generate a unique request ID (32-char hex, no dashes — faster)."""
    return uuid.uuid4().hex


def current_timestamp() -> int:
    """Get current Unix timestamp in seconds."""
    return int(time.time())


def current_hour() -> str:
    """Get current hour as string (YYYYMMDDHH)."""
    return datetime.now(timezone.utc).strftime("%Y%m%d%H")


def current_date() -> str:
    """Get current date as string (YYYYMMDD)."""
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def hash_user_id(user_id: str) -> int:
    """Hash user ID to integer for consistent bucketing."""
    return int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)


def json_dumps(obj: Any) -> str:
    """Fast JSON serialization using orjson with Decimal support."""
    return orjson.dumps(obj, default=_json_default).decode("utf-8")


def _json_default(o: Any) -> Any:
    """Handle types that orjson cannot serialize natively."""
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Type is not JSON serializable: {type(o).__name__}")


def json_loads(s: str | bytes) -> Any:
    """Fast JSON deserialization using orjson."""
    return orjson.loads(s)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division that returns default on zero division."""
    if denominator == 0:
        return default
    return numerator / denominator


# ---------------------------------------------------------------------------
# HTTP helpers (shared by routers)
# ---------------------------------------------------------------------------

# Environment string → DB integer mapping (Campaign.environment column).
# 1=CTV, 2=INAPP, None=both  (matches models/ad.py comment)
ENV_TO_INT: dict[str, int] = {"ctv": 1, "inapp": 2}


# ---------------------------------------------------------------------------
# CSV parsing helpers (used by demand_forwarder for ORTB field mapping)
# ---------------------------------------------------------------------------

def csv_ints(val: str | None) -> list[int]:
    """Parse a comma-separated string into a list of ints, ignoring bad values."""
    if not val:
        return []
    out: list[int] = []
    for p in val.split(","):
        p = p.strip()
        if p:
            try:
                out.append(int(p))
            except ValueError:
                pass
    return out


def csv_strs(val: str | None) -> list[str]:
    """Parse a comma-separated string, stripping whitespace and dropping blanks."""
    if not val:
        return []
    return [s.strip() for s in val.split(",") if s.strip()]


def extract_client_ip(
    x_forwarded_for: str | None,
    request_client_host: str | None,
    x_real_ip: str | None = None,
) -> str | None:
    """Extract the real client IP from proxy headers or the socket.

    Priority:
    1. ``X-Real-IP`` (set by nginx from ``$remote_addr`` — not spoofable).
    2. **Last** entry of ``X-Forwarded-For`` (closest trusted hop).
    3. The raw socket peer address.

    Using the *last* XFF entry (rather than the first) is deliberate:
    behind a single trusted reverse-proxy the first hop may be forged by
    the client, whereas the last is appended by the proxy itself.
    """
    if x_real_ip:
        return x_real_ip.strip()
    if x_forwarded_for:
        return x_forwarded_for.split(",")[-1].strip()
    return request_client_host


def parse_optional_iso_datetime(s: str | None) -> datetime | None:
    """Parse an ISO-8601 string to a UTC datetime, or return *None*.

    Previously duplicated 5 times in analytics.py.
    """
    if not s:
        return None
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Analytics metric helpers
# ---------------------------------------------------------------------------

def compute_derived_metrics(
    impressions: float,
    ad_requests: float,
    ad_opportunities: float,
    wins: float,
    spend: float,
    win_price_sum: float,
    completions: float,
    clicks: float,
    skips: float,
) -> dict[str, float]:
    """Compute VTR, CTR, eCPM, fill-rate and related derived metrics.

    Previously duplicated 3 times inside ``AnalyticsService``.
    """
    return {
        "vtr": round(safe_divide(completions, impressions), 6),
        "ctr": round(safe_divide(clicks, impressions), 6),
        "skip_rate": round(safe_divide(skips, impressions), 6),
        "gross_ecpm": round(safe_divide(spend * 1000, impressions), 4),
        "avg_win_price": round(safe_divide(win_price_sum, wins), 4),
        "bid_request_ecpm": round(safe_divide(spend * 1000, ad_requests), 4),
        "fill_rate_ad_req": round(safe_divide(impressions, ad_requests) * 100, 2),
        "fill_rate_ad_ops": round(safe_divide(impressions, ad_opportunities) * 100, 2),
    }


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str = ""):
        self.name = name
        self.start_time: float = 0
        self.end_time: float = 0

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.end_time = time.perf_counter()

    @property
    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return (self.end_time - self.start_time) * 1000

    @property
    def elapsed_s(self) -> float:
        """Get elapsed time in seconds."""
        return self.end_time - self.start_time

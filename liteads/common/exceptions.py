"""
Custom exceptions for LiteAds.
"""

from typing import Any


class LiteAdsError(Exception):
    """Base exception for LiteAds."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)


class CacheError(LiteAdsError):
    """Cache (Redis) related errors."""

    pass

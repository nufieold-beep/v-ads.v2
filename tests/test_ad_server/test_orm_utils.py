"""
Tests for liteads.common.orm_utils shared helpers.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from liteads.common.orm_utils import apply_updates, get_or_404
from liteads.models import Advertiser

# ---------------------------------------------------------------------------
# get_or_404 (mocked session – no real DB needed)
# ---------------------------------------------------------------------------


def _make_session(result_obj: object) -> AsyncMock:
    """Return a mock AsyncSession whose execute() returns *result_obj* or None."""
    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = result_obj
    execute_result = AsyncMock(return_value=scalar_result)
    session = AsyncMock()
    session.execute = execute_result
    return session


@pytest.mark.asyncio
async def test_get_or_404_found() -> None:
    """get_or_404 returns the object when it exists."""
    adv = Advertiser(name="found")
    session = _make_session(adv)
    result = await get_or_404(session, Advertiser, 1, "Advertiser")
    assert result is adv


@pytest.mark.asyncio
async def test_get_or_404_not_found() -> None:
    """get_or_404 raises HTTP 404 when the object does not exist."""
    session = _make_session(None)
    with pytest.raises(HTTPException) as exc_info:
        await get_or_404(session, Advertiser, 99, "Advertiser")
    assert exc_info.value.status_code == 404
    assert "99" in exc_info.value.detail


@pytest.mark.asyncio
async def test_get_or_404_default_label() -> None:
    """get_or_404 uses 'Entity' as the default label in the 404 message."""
    session = _make_session(None)
    with pytest.raises(HTTPException) as exc_info:
        await get_or_404(session, Advertiser, 1)
    assert "Entity" in exc_info.value.detail


# ---------------------------------------------------------------------------
# apply_updates (pure unit tests – no DB needed)
# ---------------------------------------------------------------------------


class _NameUpdate(BaseModel):
    name: str | None = None
    company: str | None = None


def test_apply_updates_sets_fields() -> None:
    """apply_updates sets provided fields on the target object."""
    adv = Advertiser(name="old", company="old co")
    apply_updates(adv, _NameUpdate(name="new"))
    assert adv.name == "new"
    # Unset field is left unchanged
    assert adv.company == "old co"


def test_apply_updates_skips_none_values() -> None:
    """apply_updates does not overwrite existing values with None."""
    adv = Advertiser(name="keep me")
    apply_updates(adv, _NameUpdate(name=None))
    assert adv.name == "keep me"


class _FloatUpdate(BaseModel):
    balance: float | None = None


def test_apply_updates_converts_float_to_decimal() -> None:
    """apply_updates converts float values to Decimal."""
    adv = Advertiser(balance=Decimal("0"))
    apply_updates(adv, _FloatUpdate(balance=5.5))
    assert adv.balance == Decimal("5.5")
    assert isinstance(adv.balance, Decimal)

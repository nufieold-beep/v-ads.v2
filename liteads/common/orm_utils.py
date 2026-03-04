"""
Shared ORM utility helpers for FastAPI routers.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, TypeVar

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

_T = TypeVar("_T")


async def get_or_404(session: AsyncSession, model: type[_T], entity_id: int, label: str = "Entity") -> _T:
    """Fetch a model instance by primary key or raise HTTP 404.

    Assumes the model has a column named ``id`` that serves as its primary key.
    """
    result = await session.execute(select(model).where(model.id == entity_id))  # type: ignore[arg-type]
    obj = result.scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail=f"{label} {entity_id} not found")
    return obj


def apply_updates(obj: Any, updates: BaseModel) -> None:
    """Apply non-None fields from a Pydantic update model onto an ORM object.

    Fields explicitly set to ``None`` by the caller (i.e. present in the
    JSON payload with value ``null``) are applied as-is — this allows
    clearing nullable columns such as JSON fields (mime_types, regional_urls).
    Fields that were *not sent at all* are excluded by ``exclude_unset``.
    """
    data = updates.model_dump(exclude_unset=True)
    # Fields that the caller explicitly set (may include None)
    explicitly_set = updates.model_fields_set

    for field_name, value in data.items():
        if value is not None:
            if isinstance(value, float):
                value = Decimal(str(value))
            setattr(obj, field_name, value)
        elif field_name in explicitly_set:
            # Caller explicitly sent null — clear the column
            setattr(obj, field_name, None)

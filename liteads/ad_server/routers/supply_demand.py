"""
Supply & Demand Management Router.

CRUD endpoints for:
  - Supply VAST Tags (publisher-facing)
  - Demand ORTB Endpoints (DSP / bridge ad servers)
  - Demand VAST Tags (third-party VAST demand sources)
  - Supply↔Demand Mappings (targeting supply tags to specific demand sources)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from liteads.common.database import get_session
from liteads.common.logger import get_logger
from liteads.common.orm_utils import apply_updates, get_or_404
from liteads.models import (
    DemandEndpoint,
    DemandVastTag,
    Status as ModelStatus,
    SupplyDemandMapping,
    SupplyTag,
)

logger = get_logger(__name__)
router = APIRouter()


# ============================================================================
# Pydantic schemas
# ============================================================================

# ── Supply Tag ──

class SupplyTagCreate(BaseModel):
    name: str = Field(..., max_length=255, description="Supply tag name")
    description: str | None = None
    slot_id: str = Field(..., max_length=100, description="Unique slot/zone ID for VAST tag URL")
    integration_type: str = Field("tag", description="Integration type: tag | ortb | prebid")
    pricing_type: str = Field("floor", description="Pricing: fixed_cpm | revshare | floor")
    bid_floor: float = Field(0.0, ge=0, description="Minimum CPM floor ($)")
    margin_pct: float = Field(0.0, ge=0, le=100, description="Margin percentage (0-100)")
    revshare_pct: float = Field(80.0, ge=0, le=100, description="Revenue share % publisher keeps")
    fixed_cpm: float = Field(0.0, ge=0, description="Fixed CPM payout to publisher")
    environment: int | None = Field(None, description="1=CTV, 2=INAPP, null=both")
    min_duration: int = Field(5, ge=1, description="Min video duration (s)")
    max_duration: int = Field(30, ge=1, description="Max video duration (s)")
    width: int = Field(1920, description="Video width")
    height: int = Field(1080, description="Video height")
    sensitive: bool = Field(False, description="Sensitive supply flag")


class SupplyTagUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    slot_id: str | None = None
    integration_type: str | None = None
    pricing_type: str | None = None
    bid_floor: float | None = None
    margin_pct: float | None = None
    revshare_pct: float | None = None
    fixed_cpm: float | None = None
    environment: int | None = None
    min_duration: int | None = None
    max_duration: int | None = None
    width: int | None = None
    height: int | None = None
    sensitive: bool | None = None
    status: int | None = None


class SupplyTagOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    slot_id: str
    integration_type: str = "tag"
    pricing_type: str = "floor"
    bid_floor: float
    margin_pct: float
    revshare_pct: float = 80.0
    fixed_cpm: float = 0.0
    environment: int | None = None
    min_duration: int
    max_duration: int
    width: int
    height: int
    sensitive: bool = False
    status: int
    demand_count: int = 0
    created_at: datetime | None = None
    model_config = {"from_attributes": True}


# ── Demand Endpoint ──

class DemandEndpointCreate(BaseModel):
    name: str = Field(..., max_length=255, description="Demand endpoint name")
    description: str | None = None
    endpoint_url: str = Field(..., max_length=1024, description="OpenRTB 2.6 bid URL")
    integration_type: str = Field("ortb", description="Integration: tag | ortb | direct | prebid")
    bid_floor: float = Field(0.0, ge=0, description="Minimum CPM floor ($)")
    margin_pct: float = Field(0.0, ge=0, le=100, description="Margin percentage (0-100)")
    timeout_ms: int = Field(500, ge=50, le=10000, description="Timeout (ms)")
    qps_limit: int = Field(0, ge=0, description="Max QPS (0=unlimited)")
    ortb_version: str = Field("2.6", description="OpenRTB version: 2.5 | 2.6")
    auction_type: int = Field(1, description="1=First Price, 2=Second Price")
    mime_types: list[str] | None = Field(None, description="Supported MIME types")
    protocols: list[int] | None = Field(None, description="VAST protocol IDs")
    demand_type: str = Field("video", description="video | display | audio")
    sensitive: bool = Field(False, description="Sensitive demand flag")
    regional_urls: dict[str, str] | None = Field(None, description="Regional bid URLs")


class DemandEndpointUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    endpoint_url: str | None = None
    integration_type: str | None = None
    bid_floor: float | None = None
    margin_pct: float | None = None
    timeout_ms: int | None = None
    qps_limit: int | None = None
    ortb_version: str | None = None
    auction_type: int | None = None
    mime_types: list[str] | None = None
    protocols: list[int] | None = None
    demand_type: str | None = None
    sensitive: bool | None = None
    regional_urls: dict[str, str] | None = None
    status: int | None = None


class DemandEndpointOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    endpoint_url: str
    integration_type: str = "ortb"
    bid_floor: float
    margin_pct: float
    timeout_ms: int
    qps_limit: int
    ortb_version: str = "2.6"
    auction_type: int = 1
    mime_types: list[str] | None = None
    protocols: list[int] | None = None
    demand_type: str = "video"
    sensitive: bool = False
    regional_urls: dict[str, str] | None = None
    status: int
    created_at: datetime | None = None
    model_config = {"from_attributes": True}


# ── Demand VAST Tag ──

class DemandVastTagCreate(BaseModel):
    name: str = Field(..., max_length=255, description="Demand VAST tag name")
    description: str | None = None
    vast_url: str = Field(..., max_length=2048, description="Third-party VAST tag URL")
    bid_floor: float = Field(0.0, ge=0, description="Minimum CPM floor ($)")
    margin_pct: float = Field(0.0, ge=0, le=100, description="Margin percentage (0-100)")
    cpm_value: float = Field(0.0, ge=0, description="Estimated CPM value ($)")


class DemandVastTagUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    vast_url: str | None = None
    bid_floor: float | None = None
    margin_pct: float | None = None
    cpm_value: float | None = None
    status: int | None = None


class DemandVastTagOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    vast_url: str
    bid_floor: float
    margin_pct: float
    cpm_value: float
    status: int
    created_at: datetime | None = None
    model_config = {"from_attributes": True}


# ── Mapping ──

class MappingCreate(BaseModel):
    supply_tag_id: int = Field(..., description="Supply tag ID")
    demand_endpoint_id: int | None = Field(None, description="Demand ORTB endpoint ID")
    demand_vast_tag_id: int | None = Field(None, description="Demand VAST tag ID")
    priority: int = Field(1, ge=1, description="Priority (1=highest)")
    weight: int = Field(100, ge=1, description="Weight for load balancing")


class MappingOut(BaseModel):
    id: int
    supply_tag_id: int
    demand_endpoint_id: int | None = None
    demand_vast_tag_id: int | None = None
    demand_name: str = ""
    demand_type: str = ""
    priority: int
    weight: int
    status: int
    created_at: datetime | None = None
    model_config = {"from_attributes": True}


# ============================================================================
# Helpers
# ============================================================================

def _supply_tag_out(tag: SupplyTag) -> SupplyTagOut:
    return SupplyTagOut(
        id=tag.id,
        name=tag.name,
        description=tag.description,
        slot_id=tag.slot_id,
        integration_type=getattr(tag, 'integration_type', 'tag') or 'tag',
        pricing_type=getattr(tag, 'pricing_type', 'floor') or 'floor',
        bid_floor=float(tag.bid_floor),
        margin_pct=float(tag.margin_pct),
        revshare_pct=float(getattr(tag, 'revshare_pct', 80) or 80),
        fixed_cpm=float(getattr(tag, 'fixed_cpm', 0) or 0),
        environment=tag.environment,
        min_duration=tag.min_duration,
        max_duration=tag.max_duration,
        width=tag.width,
        height=tag.height,
        sensitive=getattr(tag, 'sensitive', False) or False,
        status=tag.status,
        demand_count=len(tag.demand_mappings) if tag.demand_mappings else 0,
        created_at=tag.created_at,
    )


def _demand_endpoint_out(ep: DemandEndpoint) -> DemandEndpointOut:
    return DemandEndpointOut(
        id=ep.id,
        name=ep.name,
        description=ep.description,
        endpoint_url=ep.endpoint_url,
        integration_type=getattr(ep, 'integration_type', 'ortb') or 'ortb',
        bid_floor=float(ep.bid_floor),
        margin_pct=float(ep.margin_pct),
        timeout_ms=ep.timeout_ms,
        qps_limit=ep.qps_limit,
        ortb_version=getattr(ep, 'ortb_version', '2.6') or '2.6',
        auction_type=getattr(ep, 'auction_type', 1) or 1,
        mime_types=getattr(ep, 'mime_types', None),
        protocols=getattr(ep, 'protocols', None),
        demand_type=getattr(ep, 'demand_type', 'video') or 'video',
        sensitive=getattr(ep, 'sensitive', False) or False,
        regional_urls=getattr(ep, 'regional_urls', None),
        status=ep.status,
        created_at=ep.created_at,
    )


def _demand_vast_tag_out(dvt: DemandVastTag) -> DemandVastTagOut:
    return DemandVastTagOut(
        id=dvt.id,
        name=dvt.name,
        description=dvt.description,
        vast_url=dvt.vast_url,
        bid_floor=float(dvt.bid_floor),
        margin_pct=float(dvt.margin_pct),
        cpm_value=float(dvt.cpm_value),
        status=dvt.status,
        created_at=dvt.created_at,
    )


def _mapping_out(m: SupplyDemandMapping) -> MappingOut:
    demand_name = ""
    demand_type = ""
    if m.demand_endpoint:
        demand_name = m.demand_endpoint.name
        demand_type = "ORTB"
    elif m.demand_vast_tag:
        demand_name = m.demand_vast_tag.name
        demand_type = "VAST"
    return MappingOut(
        id=m.id,
        supply_tag_id=m.supply_tag_id,
        demand_endpoint_id=m.demand_endpoint_id,
        demand_vast_tag_id=m.demand_vast_tag_id,
        demand_name=demand_name,
        demand_type=demand_type,
        priority=m.priority,
        weight=m.weight,
        status=m.status,
        created_at=m.created_at,
    )


# ============================================================================
# SUPPLY TAG endpoints
# ============================================================================

@router.post("/supply-tags", response_model=SupplyTagOut, status_code=201, summary="Create supply tag")
async def create_supply_tag(
    body: SupplyTagCreate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    tag = SupplyTag(
        name=body.name,
        description=body.description,
        slot_id=body.slot_id,
        integration_type=body.integration_type,
        pricing_type=body.pricing_type,
        bid_floor=Decimal(str(body.bid_floor)),
        margin_pct=Decimal(str(body.margin_pct)),
        revshare_pct=Decimal(str(body.revshare_pct)),
        fixed_cpm=Decimal(str(body.fixed_cpm)),
        environment=body.environment,
        min_duration=body.min_duration,
        max_duration=body.max_duration,
        width=body.width,
        height=body.height,
        sensitive=body.sensitive,
        status=ModelStatus.ACTIVE,
    )
    session.add(tag)
    await session.flush()
    await session.refresh(tag)
    logger.info("Supply tag created", tag_id=tag.id, slot_id=tag.slot_id)
    return _supply_tag_out(tag)


@router.get("/supply-tags", response_model=list[SupplyTagOut], summary="List supply tags")
async def list_supply_tags(
    status_filter: int | None = Query(None, alias="status"),
    session: AsyncSession = Depends(get_session),
) -> Any:
    q = select(SupplyTag)
    if status_filter is not None:
        q = q.where(SupplyTag.status == status_filter)
    q = q.order_by(SupplyTag.id)
    result = await session.execute(q)
    return [_supply_tag_out(t) for t in result.scalars().all()]


@router.get("/supply-tags/{tag_id}", response_model=SupplyTagOut, summary="Get supply tag")
async def get_supply_tag(tag_id: int, session: AsyncSession = Depends(get_session)) -> Any:
    tag = await get_or_404(session, SupplyTag, tag_id, "Supply tag")
    return _supply_tag_out(tag)


@router.put("/supply-tags/{tag_id}", response_model=SupplyTagOut, summary="Update supply tag")
async def update_supply_tag(
    tag_id: int,
    body: SupplyTagUpdate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    tag = await get_or_404(session, SupplyTag, tag_id, "Supply tag")
    apply_updates(tag, body)
    await session.flush()
    await session.refresh(tag)
    logger.info("Supply tag updated", tag_id=tag_id)
    return _supply_tag_out(tag)


@router.delete("/supply-tags/{tag_id}", status_code=204, summary="Delete supply tag")
async def delete_supply_tag(tag_id: int, session: AsyncSession = Depends(get_session)) -> None:
    tag = await get_or_404(session, SupplyTag, tag_id, "Supply tag")
    await session.delete(tag)
    await session.flush()
    logger.info("Supply tag deleted", tag_id=tag_id)


# ============================================================================
# DEMAND ENDPOINT endpoints
# ============================================================================

@router.post("/demand-endpoints", response_model=DemandEndpointOut, status_code=201, summary="Create demand ORTB endpoint")
async def create_demand_endpoint(
    body: DemandEndpointCreate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    ep = DemandEndpoint(
        name=body.name,
        description=body.description,
        endpoint_url=body.endpoint_url,
        integration_type=body.integration_type,
        bid_floor=Decimal(str(body.bid_floor)),
        margin_pct=Decimal(str(body.margin_pct)),
        timeout_ms=body.timeout_ms,
        qps_limit=body.qps_limit,
        ortb_version=body.ortb_version,
        auction_type=body.auction_type,
        mime_types=body.mime_types,
        protocols=body.protocols,
        demand_type=body.demand_type,
        sensitive=body.sensitive,
        regional_urls=body.regional_urls,
        status=ModelStatus.ACTIVE,
    )
    session.add(ep)
    await session.flush()
    await session.refresh(ep)
    logger.info("Demand endpoint created", ep_id=ep.id, url=ep.endpoint_url)
    return _demand_endpoint_out(ep)


@router.get("/demand-endpoints", response_model=list[DemandEndpointOut], summary="List demand endpoints")
async def list_demand_endpoints(
    status_filter: int | None = Query(None, alias="status"),
    session: AsyncSession = Depends(get_session),
) -> Any:
    q = select(DemandEndpoint)
    if status_filter is not None:
        q = q.where(DemandEndpoint.status == status_filter)
    q = q.order_by(DemandEndpoint.id)
    result = await session.execute(q)
    return [_demand_endpoint_out(ep) for ep in result.scalars().all()]


@router.get("/demand-endpoints/{ep_id}", response_model=DemandEndpointOut, summary="Get demand endpoint")
async def get_demand_endpoint(ep_id: int, session: AsyncSession = Depends(get_session)) -> Any:
    ep = await get_or_404(session, DemandEndpoint, ep_id, "Demand endpoint")
    return _demand_endpoint_out(ep)


@router.put("/demand-endpoints/{ep_id}", response_model=DemandEndpointOut, summary="Update demand endpoint")
async def update_demand_endpoint(
    ep_id: int,
    body: DemandEndpointUpdate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    ep = await get_or_404(session, DemandEndpoint, ep_id, "Demand endpoint")
    apply_updates(ep, body)
    await session.flush()
    await session.refresh(ep)
    logger.info("Demand endpoint updated", ep_id=ep_id)
    return _demand_endpoint_out(ep)


@router.delete("/demand-endpoints/{ep_id}", status_code=204, summary="Delete demand endpoint")
async def delete_demand_endpoint(ep_id: int, session: AsyncSession = Depends(get_session)) -> None:
    ep = await get_or_404(session, DemandEndpoint, ep_id, "Demand endpoint")
    await session.delete(ep)
    await session.flush()
    logger.info("Demand endpoint deleted", ep_id=ep_id)


# ============================================================================
# DEMAND VAST TAG endpoints
# ============================================================================

@router.post("/demand-vast-tags", response_model=DemandVastTagOut, status_code=201, summary="Create demand VAST tag")
async def create_demand_vast_tag(
    body: DemandVastTagCreate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    dvt = DemandVastTag(
        name=body.name,
        description=body.description,
        vast_url=body.vast_url,
        bid_floor=Decimal(str(body.bid_floor)),
        margin_pct=Decimal(str(body.margin_pct)),
        cpm_value=Decimal(str(body.cpm_value)),
        status=ModelStatus.ACTIVE,
    )
    session.add(dvt)
    await session.flush()
    await session.refresh(dvt)
    logger.info("Demand VAST tag created", dvt_id=dvt.id)
    return _demand_vast_tag_out(dvt)


@router.get("/demand-vast-tags", response_model=list[DemandVastTagOut], summary="List demand VAST tags")
async def list_demand_vast_tags(
    status_filter: int | None = Query(None, alias="status"),
    session: AsyncSession = Depends(get_session),
) -> Any:
    q = select(DemandVastTag)
    if status_filter is not None:
        q = q.where(DemandVastTag.status == status_filter)
    q = q.order_by(DemandVastTag.id)
    result = await session.execute(q)
    return [_demand_vast_tag_out(dvt) for dvt in result.scalars().all()]


@router.get("/demand-vast-tags/{dvt_id}", response_model=DemandVastTagOut, summary="Get demand VAST tag")
async def get_demand_vast_tag(dvt_id: int, session: AsyncSession = Depends(get_session)) -> Any:
    dvt = await get_or_404(session, DemandVastTag, dvt_id, "Demand VAST tag")
    return _demand_vast_tag_out(dvt)


@router.put("/demand-vast-tags/{dvt_id}", response_model=DemandVastTagOut, summary="Update demand VAST tag")
async def update_demand_vast_tag(
    dvt_id: int,
    body: DemandVastTagUpdate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    dvt = await get_or_404(session, DemandVastTag, dvt_id, "Demand VAST tag")
    apply_updates(dvt, body)
    await session.flush()
    await session.refresh(dvt)
    logger.info("Demand VAST tag updated", dvt_id=dvt_id)
    return _demand_vast_tag_out(dvt)


@router.delete("/demand-vast-tags/{dvt_id}", status_code=204, summary="Delete demand VAST tag")
async def delete_demand_vast_tag(dvt_id: int, session: AsyncSession = Depends(get_session)) -> None:
    dvt = await get_or_404(session, DemandVastTag, dvt_id, "Demand VAST tag")
    await session.delete(dvt)
    await session.flush()
    logger.info("Demand VAST tag deleted", dvt_id=dvt_id)


# ============================================================================
# SUPPLY ↔ DEMAND MAPPING endpoints
# ============================================================================

@router.post("/mappings", response_model=MappingOut, status_code=201, summary="Link supply tag to demand source")
async def create_mapping(
    body: MappingCreate,
    session: AsyncSession = Depends(get_session),
) -> Any:
    if not body.demand_endpoint_id and not body.demand_vast_tag_id:
        raise HTTPException(400, "Must specify either demand_endpoint_id or demand_vast_tag_id")
    if body.demand_endpoint_id and body.demand_vast_tag_id:
        raise HTTPException(400, "Specify only one of demand_endpoint_id or demand_vast_tag_id")

    # Verify referenced entities exist
    await get_or_404(session, SupplyTag, body.supply_tag_id, "Supply tag")
    if body.demand_endpoint_id:
        await get_or_404(session, DemandEndpoint, body.demand_endpoint_id, "Demand endpoint")
    if body.demand_vast_tag_id:
        await get_or_404(session, DemandVastTag, body.demand_vast_tag_id, "Demand VAST tag")

    m = SupplyDemandMapping(
        supply_tag_id=body.supply_tag_id,
        demand_endpoint_id=body.demand_endpoint_id,
        demand_vast_tag_id=body.demand_vast_tag_id,
        priority=body.priority,
        weight=body.weight,
        status=ModelStatus.ACTIVE,
    )
    session.add(m)
    await session.flush()
    await session.refresh(m)
    logger.info("Supply-demand mapping created", mapping_id=m.id)
    return _mapping_out(m)


@router.get("/mappings", response_model=list[MappingOut], summary="List supply-demand mappings")
async def list_mappings(
    supply_tag_id: int | None = Query(None, description="Filter by supply tag"),
    session: AsyncSession = Depends(get_session),
) -> Any:
    q = select(SupplyDemandMapping)
    if supply_tag_id is not None:
        q = q.where(SupplyDemandMapping.supply_tag_id == supply_tag_id)
    q = q.order_by(SupplyDemandMapping.supply_tag_id, SupplyDemandMapping.priority)
    result = await session.execute(q)
    return [_mapping_out(m) for m in result.scalars().all()]


@router.delete("/mappings/{mapping_id}", status_code=204, summary="Delete supply-demand mapping")
async def delete_mapping(mapping_id: int, session: AsyncSession = Depends(get_session)) -> None:
    m = await get_or_404(session, SupplyDemandMapping, mapping_id, "Mapping")
    await session.delete(m)
    await session.flush()
    logger.info("Supply-demand mapping deleted", mapping_id=mapping_id)

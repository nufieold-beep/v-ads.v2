"""
Recommendation Engine for CPM CTV and In-App Video.

Coordinates retrieval, filtering, fill-rate prediction, CPM ranking, and re-ranking.
Optimized for video ad fill rate rather than CTR/CVR.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from liteads.common.logger import get_logger
from liteads.common.utils import Timer
from liteads.rec_engine.filter.base import BaseFilter, CompositeFilter
from liteads.rec_engine.filter.budget import BudgetFilter
from liteads.rec_engine.filter.quality import QualityFilter
from liteads.rec_engine.retrieval.base import BaseRetrieval
from liteads.rec_engine.retrieval.targeting import TargetingRetrieval
from liteads.schemas.internal import AdCandidate, UserContext

logger = get_logger(__name__)


@dataclass
class RecommendationMetrics:
    """Metrics for a video ad recommendation request."""

    retrieval_count: int = 0
    post_filter_count: int = 0
    final_count: int = 0

    retrieval_ms: float = 0.0
    filter_ms: float = 0.0
    total_ms: float = 0.0


@dataclass
class RecommendationConfig:
    """Configuration for CPM video recommendation engine."""

    # Retrieval
    max_retrieval: int = 100

    # Filtering
    enable_budget_filter: bool = True
    enable_frequency_filter: bool = False   # CTV has no cookies/persistent user IDs
    enable_quality_filter: bool = True


class RecommendationEngine:
    """
    CPM video recommendation engine for CTV and In-App.

    Pipeline:
    1. Retrieval  — Get candidate video ads matching targeting
    2. Filtering  — Remove ineligible ads (budget, frequency, quality)
    """

    def __init__(
        self,
        session: AsyncSession,
        config: RecommendationConfig | None = None,
        retrieval: BaseRetrieval | None = None,
        filters: list[BaseFilter] | None = None,
    ):
        self.session = session
        self.config = config or RecommendationConfig()

        # Initialize components
        self.retrieval = retrieval or TargetingRetrieval(session)
        self.filters = filters or self._create_default_filters()

        # Composite components
        self._filter_chain = CompositeFilter(self.filters) if self.filters else None

    def _create_default_filters(self) -> list[BaseFilter]:
        """Create default filter chain for video ads."""
        filters: list[BaseFilter] = []

        if self.config.enable_budget_filter:
            filters.append(BudgetFilter())

        # FrequencyFilter removed – CTV devices have no cookies or
        # persistent user identifiers for frequency capping.

        if self.config.enable_quality_filter:
            filters.append(QualityFilter(require_video_url=True))

        return filters

    async def recommend(
        self,
        user_context: UserContext,
        slot_id: str,
        num_ads: int = 1,
        **kwargs: Any,
    ) -> tuple[list[AdCandidate], RecommendationMetrics]:
        """
        Get CPM video ad recommendations.

        Returns:
            Tuple of (recommended video ads, metrics)
        """
        metrics = RecommendationMetrics()

        with Timer() as total_timer:
            # 1. Retrieval
            with Timer() as retrieval_timer:
                candidates = await self.retrieval.retrieve(
                    user_context=user_context,
                    slot_id=slot_id,
                    limit=self.config.max_retrieval,
                )
            metrics.retrieval_ms = retrieval_timer.elapsed_ms
            metrics.retrieval_count = len(candidates)

            logger.debug(
                f"Retrieved {len(candidates)} video candidates",
                slot_id=slot_id,
                environment=user_context.environment,
            )

            if not candidates:
                return [], metrics

            # 2. Filtering
            with Timer() as filter_timer:
                if self._filter_chain:
                    candidates = await self._filter_chain.filter(
                        candidates, user_context
                    )
            metrics.filter_ms = filter_timer.elapsed_ms
            metrics.post_filter_count = len(candidates)

            logger.debug(f"After filtering: {len(candidates)} candidates")

            if not candidates:
                return [], metrics

            # Final selection
            final_candidates = candidates[:num_ads]
            metrics.final_count = len(final_candidates)

        metrics.total_ms = total_timer.elapsed_ms

        logger.info(
            "Video recommendation completed",
            environment=user_context.environment,
            retrieval=metrics.retrieval_count,
            final=metrics.final_count,
            total_ms=round(metrics.total_ms, 2),
        )

        return final_candidates, metrics


def create_engine(
    session: AsyncSession,
) -> RecommendationEngine:
    """
    Factory function to create CPM video recommendation engine.

    Args:
        session: Database session

    Returns:
        Configured RecommendationEngine
    """
    config = RecommendationConfig()

    return RecommendationEngine(session=session, config=config)

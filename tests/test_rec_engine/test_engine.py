"""
Tests for recommendation engine.
"""

import pytest

from liteads.rec_engine import RecommendationConfig, RecommendationMetrics
from liteads.rec_engine.filter.base import PassThroughFilter
from liteads.schemas.internal import AdCandidate, UserContext


@pytest.fixture
def sample_candidates() -> list[AdCandidate]:
    """Create sample ad candidates."""
    return [
        AdCandidate(
            campaign_id=1,
            creative_id=101,
            advertiser_id=1,
            bid=5.0,
            bid_type=1,
            title="Ad 1",
            landing_url="https://example.com/1",
            pctr=0.02,
        ),
        AdCandidate(
            campaign_id=2,
            creative_id=201,
            advertiser_id=2,
            bid=3.0,
            bid_type=1,
            title="Ad 2",
            landing_url="https://example.com/2",
            pctr=0.03,
        ),
        AdCandidate(
            campaign_id=3,
            creative_id=301,
            advertiser_id=1,
            bid=4.0,
            bid_type=1,
            title="Ad 3",
            landing_url="https://example.com/3",
            pctr=0.015,
        ),
    ]


@pytest.fixture
def sample_user_context() -> UserContext:
    """Create sample user context."""
    return UserContext(
        user_id="test_user_123",
        os="android",
        os_version="13.0",
        country="CN",
        city="shanghai",
        age=25,
        gender="male",
    )


class TestRecommendationConfig:
    """Tests for recommendation config."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = RecommendationConfig()

        assert config.max_retrieval == 100
        assert config.enable_budget_filter is True
        assert config.enable_frequency_filter is False

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = RecommendationConfig(
            max_retrieval=50,
        )

        assert config.max_retrieval == 50


class TestRecommendationMetrics:
    """Tests for recommendation metrics."""

    def test_metrics_initialization(self) -> None:
        """Test metrics default values."""
        metrics = RecommendationMetrics()

        assert metrics.retrieval_count == 0
        assert metrics.final_count == 0
        assert metrics.total_ms == 0.0

    def test_metrics_update(self) -> None:
        """Test metrics can be updated."""
        metrics = RecommendationMetrics()
        metrics.retrieval_count = 100
        metrics.post_filter_count = 80
        metrics.final_count = 3
        metrics.total_ms = 25.5

        assert metrics.retrieval_count == 100
        assert metrics.final_count == 3

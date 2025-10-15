import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.recommendations import RecommendationManager, RecommendationSettings


@pytest.fixture
def manager() -> RecommendationManager:
    mock_db = MagicMock()
    settings = RecommendationSettings()
    return RecommendationManager(mock_db, settings)


def test_scoring_prioritises_hit_rate(manager: RecommendationManager) -> None:
    low_quality = {
        "forward_count": 200,
        "coordinate_forward_count": 0,
        "discovered_from_channels": json.dumps([1, 2, 3, 4, 5]),
        "is_verified": True,
        "subscriber_count": 100000,
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    high_quality = {
        "forward_count": 50,
        "coordinate_forward_count": 40,
        "discovered_from_channels": json.dumps([1, 2]),
        "subscriber_count": 5000,
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    low_score = manager.calculate_recommendation_score(low_quality)
    high_score = manager.calculate_recommendation_score(high_quality)

    assert high_score > low_score + 20


def test_scoring_rewards_high_hit_rates(manager: RecommendationManager) -> None:
    base = {
        "forward_count": 100,
        "discovered_from_channels": json.dumps([1, 2, 3]),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    scores = {}
    for hit_rate_pct in [5, 10, 20, 40, 60, 80, 95]:
        channel_data = {
            **base,
            "coordinate_forward_count": int(100 * (hit_rate_pct / 100)),
        }
        scores[hit_rate_pct] = manager.calculate_recommendation_score(channel_data)

    assert scores[80] > scores[60] + 5
    assert scores[95] >= scores[80]


def test_sample_size_confidence_modifier(manager: RecommendationManager) -> None:
    small_sample = {
        "forward_count": 10,
        "coordinate_forward_count": 8,
        "discovered_from_channels": json.dumps([1]),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    large_sample = {
        "forward_count": 200,
        "coordinate_forward_count": 160,
        "discovered_from_channels": json.dumps([1]),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    small_score = manager.calculate_recommendation_score(small_sample)
    large_score = manager.calculate_recommendation_score(large_sample)

    assert large_score > small_score


def test_zero_forwards_returns_neutral_score(manager: RecommendationManager) -> None:
    channel_data = {
        "forward_count": 0,
        "coordinate_forward_count": 0,
    }

    score = manager.calculate_recommendation_score(channel_data)

    assert 45.0 <= score <= 55.0

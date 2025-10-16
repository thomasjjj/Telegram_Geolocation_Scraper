import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.recommendations import RecommendationManager, RecommendationSettings


@pytest.fixture
def mock_database() -> MagicMock:
    db = MagicMock()
    db.add_recommended_channel.return_value = True
    db.add_recommendation_event.return_value = True
    db.update_recommended_channel.return_value = True
    db.get_channel_info.return_value = None
    return db


@pytest.fixture
def recommendation_manager(mock_database: MagicMock) -> RecommendationManager:
    settings = RecommendationSettings()
    settings.enabled = True
    settings.telegram_recs_enabled = True
    return RecommendationManager(mock_database, settings=settings)


def test_fetch_telegram_recommendations_success(recommendation_manager: RecommendationManager):
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=object())

    mock_response = MagicMock()
    chat = MagicMock()
    chat.id = 12345
    chat.username = "test_channel"
    chat.title = "Test Channel"
    chat.participants_count = 5000
    chat.verified = True
    chat.scam = False
    chat.fake = False
    chat.has_geo = False
    chat.restricted = False
    mock_response.chats = [chat]

    client.return_value = mock_response

    recommendations = asyncio.run(
        recommendation_manager.fetch_telegram_recommendations(
            client,
            channel_id=999,
        )
    )

    assert len(recommendations) == 1
    assert recommendations[0]["channel_id"] == 12345
    assert recommendations[0]["username"] == "test_channel"
    assert recommendations[0]["verified"] is True
    assert recommendations[0]["peer_type"] == "channel"


def test_harvest_telegram_recommendations_integration(
    recommendation_manager: RecommendationManager,
    mock_database: MagicMock,
):
    mock_database.get_channels_with_coordinates.return_value = [
        {"id": 111, "title": "Source Channel", "coordinate_density": 15.0}
    ]

    recommendation_manager.fetch_telegram_recommendations = AsyncMock(
        return_value=[
            {
                "channel_id": 222,
                "title": "New Channel",
                "username": "new_channel",
                "participants_count": 1500,
                "verified": False,
                "scam": False,
                "fake": False,
                "restricted": False,
            }
        ]
    )

    def _get_recommended_channel(channel_id: int):
        if channel_id != 222:
            return None
        if not getattr(_get_recommended_channel, "called", False):
            _get_recommended_channel.called = True
            return None
        return {
            "channel_id": 222,
            "forward_count": 0,
            "coordinate_forward_count": 0,
            "telegram_recommendation_count": 1,
            "telegram_rec_source_density": 15.0,
            "discovery_method": "telegram_api",
            "discovered_from_channels": json.dumps([111]),
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "is_verified": False,
            "is_scam": False,
            "is_fake": False,
            "subscriber_count": 0,
        }

    _get_recommended_channel.called = False
    mock_database.get_recommended_channel.side_effect = _get_recommended_channel

    stats = asyncio.run(
        recommendation_manager.harvest_telegram_recommendations(
            AsyncMock(),
            min_coordinate_density=5.0,
        )
    )

    assert stats["source_channels_checked"] == 1
    assert stats["new_recommendations"] == 1
    mock_database.add_recommended_channel.assert_called_once()
    mock_database.add_recommendation_event.assert_called_once()


def test_harvest_auto_enriches_new_recommendations(
    recommendation_manager: RecommendationManager,
    mock_database: MagicMock,
):
    recommendation_manager.settings.auto_enrich = True
    recommendation_manager.fetch_telegram_recommendations = AsyncMock(
        return_value=[
            {
                "channel_id": 333,
                "title": "Auto Enrich Channel",
                "username": "auto_enrich_channel",
                "participants_count": 500,
                "verified": False,
                "scam": False,
                "fake": False,
                "restricted": False,
            }
        ]
    )
    recommendation_manager.enrich_recommendation = AsyncMock(return_value=True)

    mock_database.get_channels_with_coordinates.return_value = [
        {"id": 222, "title": "Signal Source", "coordinate_density": 25.0}
    ]
    mock_database.get_recommended_channel.return_value = None

    client = AsyncMock()

    asyncio.run(
        recommendation_manager.harvest_telegram_recommendations(
            client,
            min_coordinate_density=10.0,
        )
    )

    recommendation_manager.enrich_recommendation.assert_awaited_once_with(client, 333)


def test_scoring_with_telegram_recommendations(mock_database: MagicMock):
    manager = RecommendationManager(mock_database, RecommendationSettings())

    channel_data = {
        "forward_count": 5,
        "coordinate_forward_count": 3,
        "telegram_recommendation_count": 10,
        "telegram_rec_source_density": 12.5,
        "discovery_method": "telegram_api",
        "discovered_from_channels": json.dumps([1, 2, 3]),
        "is_verified": True,
        "is_scam": False,
        "is_fake": False,
        "subscriber_count": 15000,
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    score = manager.calculate_recommendation_score(channel_data)

    assert score > 70.0
    assert score <= 100.0

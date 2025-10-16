import asyncio
from types import SimpleNamespace
from typing import Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.tl.types import Channel, PeerChannel, PeerChat, PeerUser

from src.database import CoordinatesDatabase
from src.recommendations import RecommendationManager, RecommendationSettings


@pytest.fixture
def mock_database() -> MagicMock:
    database = MagicMock(spec=CoordinatesDatabase)
    database.get_recommended_channel.return_value = None
    return database


@pytest.fixture
def recommendation_manager(mock_database: MagicMock) -> RecommendationManager:
    settings = RecommendationSettings(enabled=True)
    return RecommendationManager(mock_database, settings=settings)


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_entity = AsyncMock()
    return client


def _build_forward(message_peer) -> SimpleNamespace:
    return SimpleNamespace(from_id=message_peer, date=None, post_author=None, from_name="Test")


def test_extract_forward_info_identifies_user(
    recommendation_manager: RecommendationManager,
) -> None:
    message = SimpleNamespace(forward=_build_forward(PeerUser(user_id=123456)))

    result = recommendation_manager._extract_forward_info(message)

    assert result is not None
    assert result["peer_type"] == "user"
    assert (
        recommendation_manager._is_valid_channel_id(result["channel_id"], result)
        is False
    )


def test_extract_forward_info_accepts_channels(
    recommendation_manager: RecommendationManager,
) -> None:
    message = SimpleNamespace(
        forward=_build_forward(PeerChannel(channel_id=1234567890))
    )

    result = recommendation_manager._extract_forward_info(message)

    assert result is not None
    assert result["channel_id"] == 1234567890
    assert result["entity_type"] == "channel"
    assert result["peer_type"] == "channel"


def test_extract_forward_info_accepts_peer_chat(
    recommendation_manager: RecommendationManager,
) -> None:
    message = SimpleNamespace(
        forward=_build_forward(PeerChat(chat_id=987654321))
    )

    result = recommendation_manager._extract_forward_info(message)

    assert result is not None
    assert result["peer_type"] == "chat"
    assert (
        recommendation_manager._is_valid_channel_id(result["channel_id"], result)
        is True
    )


def test_is_valid_channel_id_rejects_low_ids(
    recommendation_manager: RecommendationManager,
) -> None:
    assert (
        recommendation_manager._is_valid_channel_id(123456, {"entity_type": None})
        is False
    )
    assert (
        recommendation_manager._is_valid_channel_id(
            1_234_567_890, {"entity_type": None}
        )
        is True
    )


def test_is_valid_channel_id_uses_entity_type(
    recommendation_manager: RecommendationManager,
) -> None:
    assert (
        recommendation_manager._is_valid_channel_id(
            1_234_567_890, {"entity_type": "user"}
        )
        is False
    )
    assert (
        recommendation_manager._is_valid_channel_id(
            123456, {"entity_type": "channel"}
        )
        is True
    )


def test_enrich_recommendation_handles_user_entities(
    recommendation_manager: RecommendationManager,
    mock_database: MagicMock,
    mock_client: MagicMock,
) -> None:
    entity = SimpleNamespace(username=None, title=None)
    mock_client.get_entity.return_value = entity

    result = asyncio.run(
        recommendation_manager.enrich_recommendation(mock_client, 555)
    )

    assert result is False
    mock_database.update_recommended_channel.assert_called_once()
    update_args = mock_database.update_recommended_channel.call_args[0]
    assert update_args[0] == 555
    payload: Dict[str, str] = update_args[1]
    assert payload["user_status"] == "invalid_entity_type"
    mock_database.add_recommendation_event.assert_called_once()


def test_enrich_recommendation_accepts_channels(
    recommendation_manager: RecommendationManager,
    mock_database: MagicMock,
    mock_client: MagicMock,
) -> None:
    entity = Channel(
        id=777,
        title="Test Channel",
        username="testchannel",
        photo=None,
        date=None,
        verified=True,
        megagroup=False,
        participants_count=100,
    )
    mock_client.get_entity.return_value = entity

    result = asyncio.run(
        recommendation_manager.enrich_recommendation(mock_client, 777)
    )

    assert result is True
    mock_database.update_recommended_channel.assert_called()


def test_cleanup_invalid_recommendations(tmp_path) -> None:
    database = CoordinatesDatabase(str(tmp_path / "recs.db"))

    database.add_recommended_channel(123, {"username": "low_id"})
    database.add_recommended_channel(
        2_000_000_000,
        {"username": "invalid_status", "user_status": "invalid_entity_type"},
    )
    database.add_recommended_channel(
        2_000_000_001, {"username": "user_type", "entity_type": "user"}
    )
    database.add_recommended_channel(
        2_000_000_002, {"username": "valid_channel"}
    )

    stats = database.cleanup_invalid_recommendations()

    assert stats["total_removed"] == 3
    assert database.get_recommended_channel(2_000_000_002) is not None
    assert database.get_recommended_channel(123) is None

    database.close()

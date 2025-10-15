import asyncio
from datetime import datetime, timezone
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import sys
from pathlib import Path
from telethon.tl.types import PeerChannel

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.channel_scraper import _process_message_batch


def test_auto_enrich_triggers_for_new_forward_recommendations():
    now = datetime.now(timezone.utc)
    message = SimpleNamespace(
        id=1,
        message="Test 12.34, 56.78",
        date=now,
        media=None,
        forward=SimpleNamespace(
            from_id=PeerChannel(channel_id=555),
            date=now,
        ),
    )

    database = MagicMock()
    database.bulk_check_message_existence.return_value = set()
    database.bulk_insert_messages.return_value = {1: 101}
    database.bulk_add_coordinates.return_value = None

    recommendation_manager = MagicMock()
    recommendation_manager.settings.auto_enrich = True
    recommendation_manager.process_forwarded_message.return_value = 555
    recommendation_manager.enrich_recommendation = AsyncMock(return_value=True)

    client = AsyncMock()

    asyncio.run(
        _process_message_batch(
            messages=[message],
            channel_id=999,
            channel_display_name="Test Channel",
            entity=SimpleNamespace(username="test_channel"),
            client=client,
            coordinate_pattern=re.compile(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)"),
            database=database,
            skip_existing=False,
            recommendation_manager=recommendation_manager,
            result_collector=None,
        )
    )

    recommendation_manager.enrich_recommendation.assert_awaited_once_with(client, 555)

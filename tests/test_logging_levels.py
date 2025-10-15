import asyncio
import logging
import re
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.channel_scraper import _process_message_batch


def _build_message():
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id=1,
        message="Coordinates 12.34, 56.78",
        date=now,
        media=None,
        forward=None,
    )


def _build_database_mock():
    database = MagicMock()
    database.bulk_check_message_existence.return_value = set()
    database.bulk_insert_messages.return_value = {1: 101}
    database.bulk_add_coordinates.return_value = None
    return database


def test_process_message_batch_emits_summary_at_info(caplog):
    message = _build_message()
    database = _build_database_mock()
    result_collector = MagicMock()

    with caplog.at_level(logging.INFO):
        asyncio.run(
            _process_message_batch(
                messages=[message],
                channel_id=999,
                channel_display_name="Test Channel",
                entity=SimpleNamespace(username="test_channel"),
                client=None,
                coordinate_pattern=re.compile(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)"),
                database=database,
                skip_existing=False,
                recommendation_manager=None,
                result_collector=result_collector,
            )
        )

    info_messages = [record.message for record in caplog.records]
    assert any("Processed batch for channel" in text for text in info_messages)
    assert all("Retrieved coordinate" not in text for text in info_messages)

    caplog.clear()

    message = _build_message()
    database = _build_database_mock()
    result_collector = MagicMock()

    with caplog.at_level(logging.DEBUG):
        asyncio.run(
            _process_message_batch(
                messages=[message],
                channel_id=999,
                channel_display_name="Test Channel",
                entity=SimpleNamespace(username="test_channel"),
                client=None,
                coordinate_pattern=re.compile(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)"),
                database=database,
                skip_existing=False,
                recommendation_manager=None,
                result_collector=result_collector,
            )
        )

    debug_messages = [record.message for record in caplog.records]
    assert any("Retrieved coordinate" in text for text in debug_messages)

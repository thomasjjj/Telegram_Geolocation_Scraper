from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.channel_scraper import _process_message_batch, CoordinateResultCollector
from src.coordinates import DECIMAL_PATTERN


def test_process_message_batch_extracts_decimal_and_dms_coordinates() -> None:
    message_text = (
        "Coordinates: 10.5, 20.25 and 40°26'46\"N 79°58'56\"W"
    )

    message = SimpleNamespace(
        id=1,
        message=message_text,
        date=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        media=None,
        forward=None,
    )

    entity = SimpleNamespace(username="testchannel")
    collector = CoordinateResultCollector()

    stats = asyncio.run(
        _process_message_batch(
            messages=[message],
            channel_id=123,
            channel_display_name="Test Channel",
            entity=entity,
            client=None,
            coordinate_pattern=DECIMAL_PATTERN,
            database=None,
            skip_existing=False,
            recommendation_manager=None,
            result_collector=collector,
        )
    )

    assert stats["coordinates"] == 2

    df = collector.finalize()
    assert len(df) == 2

    coordinates = {
        (round(row["latitude"], 6), round(row["longitude"], 6))
        for _, row in df.iterrows()
    }

    assert coordinates == {
        (10.5, 20.25),
        (40.446111, -79.982222),
    }

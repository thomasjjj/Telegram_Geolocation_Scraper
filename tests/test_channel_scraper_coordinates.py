import asyncio
import re
from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.channel_scraper import _process_message_batch
from src.database import CoordinatesDatabase


def test_process_message_batch_stores_multiple_dms_coordinates(tmp_path):
    message_text = (
        "Locations: 40°26'46\"N 79°58'56\"W "
        "and 34°3'8\"N 118°14'37\"W"
    )
    now = datetime.now(timezone.utc)
    message = SimpleNamespace(
        id=1,
        message=message_text,
        date=now,
        media=None,
        forward=None,
    )

    database = CoordinatesDatabase(str(tmp_path / "coords.db"))

    try:
        stats = asyncio.run(
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
                result_collector=None,
            )
        )

        assert stats["coordinates"] == 2

        connection = database.connect()
        rows = connection.execute(
            "SELECT latitude, longitude FROM coordinates ORDER BY id"
        ).fetchall()

        assert len(rows) == 2

        latitudes = [row["latitude"] for row in rows]
        longitudes = [row["longitude"] for row in rows]

        assert latitudes[0] == pytest.approx(40.4461111111)
        assert longitudes[0] == pytest.approx(-79.9822222222)
        assert latitudes[1] == pytest.approx(34.0522222222)
        assert longitudes[1] == pytest.approx(-118.2436111111)
    finally:
        database.close()

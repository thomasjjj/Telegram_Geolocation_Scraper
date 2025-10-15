import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.json_processor import process_telegram_json


def test_process_telegram_json_multiple_coordinates(tmp_path):
    telegram_data = {
        "messages": [
            {
                "id": 123,
                "date": "2024-01-01T00:00:00",
                "type": "message",
                "text": (
                    "Primary 12.34, 56.78 and backup 13°20'30\"N 45°10'5\"E"
                ),
            }
        ]
    }

    json_path = tmp_path / "telegram.json"
    json_path.write_text(json.dumps(telegram_data), encoding="utf-8")

    df = process_telegram_json(str(json_path), "https://t.me/example/")

    assert len(df) == 2
    assert set(df["Post Link"]) == {"https://t.me/example/123"}

    extracted_pairs = {(row["Latitude"], row["Longitude"]) for _, row in df.iterrows()}

    assert ("12.34", "56.78") in extracted_pairs

    dms_lat, dms_lon = next(
        (
            float(lat),
            float(lon),
        )
        for lat, lon in extracted_pairs
        if (lat, lon) != ("12.34", "56.78")
    )

    assert pytest.approx(dms_lat, rel=1e-6) == 13 + 20 / 60 + 30 / 3600
    assert pytest.approx(dms_lon, rel=1e-6) == 45 + 10 / 60 + 5 / 3600

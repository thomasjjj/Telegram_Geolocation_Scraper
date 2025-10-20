from __future__ import annotations

from pathlib import Path
from typing import Iterator
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database import CoordinatesDatabase


@pytest.fixture()
def database(tmp_path: Path) -> Iterator[CoordinatesDatabase]:
    db = CoordinatesDatabase(str(tmp_path / "test_bulk.db"))
    try:
        yield db
    finally:
        db.close()


def test_bulk_check_message_existence(database: CoordinatesDatabase) -> None:
    channel_id = 42
    database.add_message(channel_id, 1, {"message_text": "hello"})
    database.add_message(channel_id, 3, {"message_text": "world"})

    existing = database.bulk_check_message_existence(channel_id, [1, 2, 3, 4])

    assert existing == {1, 3}


def test_bulk_insert_messages_returns_row_ids(database: CoordinatesDatabase) -> None:
    channel_id = 99
    payload = [
        {"message_id": 10, "message_text": "first", "has_coordinates": 1},
        {"message_id": 11, "message_text": "second", "has_coordinates": 0},
    ]

    id_map = database.bulk_insert_messages(channel_id, payload)

    assert set(id_map.keys()) == {10, 11}

    connection = database.connect()
    row = connection.execute(
        "SELECT has_coordinates FROM messages WHERE channel_id=? AND message_id=10",
        (channel_id,),
    ).fetchone()
    assert row is not None
    assert row["has_coordinates"] == 1


def test_bulk_insert_messages_updates_existing(database: CoordinatesDatabase) -> None:
    channel_id = 7
    database.add_message(channel_id, 5, {"message_text": "old", "has_coordinates": 0})

    database.bulk_insert_messages(
        channel_id,
        [
            {"message_id": 5, "message_text": "updated", "has_coordinates": 1},
        ],
    )

    connection = database.connect()
    row = connection.execute(
        "SELECT message_text, has_coordinates FROM messages WHERE channel_id=? AND message_id=5",
        (channel_id,),
    ).fetchone()

    assert row is not None
    assert row["message_text"] == "updated"
    assert row["has_coordinates"] == 1


def test_bulk_insert_messages_handles_large_batches(database: CoordinatesDatabase) -> None:
    channel_id = 123
    message_count = 1100
    payload = [
        {"message_id": idx, "message_text": f"message-{idx}"}
        for idx in range(1, message_count + 1)
    ]

    id_map = database.bulk_insert_messages(channel_id, payload)

    assert set(id_map.keys()) == {idx for idx in range(1, message_count + 1)}


def test_export_coordinate_summary(database: CoordinatesDatabase) -> None:
    public_channel_id = 1001
    private_channel_id = -1009876543210

    database.add_or_update_channel(
        public_channel_id,
        {"username": "publicchannel", "title": "Public Channel"},
    )
    database.add_or_update_channel(
        private_channel_id,
        {"title": "Private Group"},
    )

    public_message_id = database.add_message(
        public_channel_id,
        44,
        {"message_text": "First coordinate", "has_coordinates": 1},
    )
    private_message_id = database.add_message(
        private_channel_id,
        55,
        {"message_text": "Second coordinate", "has_coordinates": 1},
    )

    database.bulk_add_coordinates(
        [
            (public_message_id, 10.5, 20.25),
            (private_message_id, -33.9, 151.2),
        ]
    )

    export_df = database.export_coordinate_summary()

    assert list(export_df.columns) == [
        "latitude",
        "longitude",
        "post text",
        "post channel",
        "post link",
    ]
    assert len(export_df) == 2

    public_row = export_df[export_df["post link"].str.contains("publicchannel")]\
        .iloc[0]
    assert public_row["post channel"] == "publicchannel"
    assert public_row["post text"] == "First coordinate"
    assert public_row["latitude"] == pytest.approx(10.5)
    assert public_row["longitude"] == pytest.approx(20.25)
    assert public_row["post link"].endswith("/44")

    private_row = export_df[~export_df["post link"].str.contains("publicchannel")].iloc[0]
    assert private_row["post channel"] == "Private Group"
    assert private_row["post link"].startswith("https://t.me/c/")
    assert private_row["post link"].endswith("/55")

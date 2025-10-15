"""Utilities for importing legacy CSV exports into the SQLite database."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from src.database import CoordinatesDatabase


LOGGER = logging.getLogger(__name__)


def migrate_schema_for_telegram_recs(db_path: str) -> None:
    """Add columns required for Telegram API recommendation tracking."""

    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        migrations = [
            """
            ALTER TABLE recommended_channels
            ADD COLUMN discovery_method TEXT DEFAULT 'forward'
            """,
            """
            ALTER TABLE recommended_channels
            ADD COLUMN telegram_recommendation_count INTEGER DEFAULT 0
            """,
            """
            ALTER TABLE recommended_channels
            ADD COLUMN telegram_rec_source_density REAL DEFAULT 0.0
            """,
            """
            ALTER TABLE recommended_channels
            ADD COLUMN last_harvest_date DATETIME
            """,
        ]

        for statement in migrations:
            try:
                cursor.execute(statement)
                connection.commit()
            except sqlite3.OperationalError as exc:
                if "duplicate column" in str(exc).lower():
                    continue
                raise
    finally:
        connection.close()

    LOGGER.info("Schema migration for Telegram recommendations completed")


def _extract_channel_from_source(source: str) -> tuple[Optional[int], Optional[str]]:
    if not source or not isinstance(source, str):
        return None, None

    if "t.me/c/" in source:
        try:
            fragment = source.split("t.me/c/", 1)[1]
            parts = fragment.split("/")
            channel_id = int(parts[0])
            return channel_id, None
        except (IndexError, ValueError):
            return None, None

    if "t.me/" in source:
        fragment = source.split("t.me/", 1)[1]
        username = fragment.split("/")[0].lstrip("@")
        return None, username

    return None, None


def migrate_existing_csv_to_database(csv_path: str, database: CoordinatesDatabase) -> int:
    """Import a legacy CSV file containing coordinate data."""

    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    df = pd.read_csv(csv_file)
    required_columns = {"message_id", "latitude", "longitude", "message_source"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"CSV file is missing required columns: {', '.join(sorted(missing))}")

    imported_coordinates = 0
    for _, row in df.iterrows():
        channel_id = row.get("channel_id") if "channel_id" in df.columns else None
        channel_username = row.get("channel_username") if "channel_username" in df.columns else None

        if channel_id is None:
            source = row.get("message_source")
            parsed_id, parsed_username = _extract_channel_from_source(source)
            if parsed_id:
                channel_id = parsed_id
            elif parsed_username:
                channel_username = channel_username or parsed_username
                existing = database.get_channel_by_username(channel_username)
                if existing:
                    channel_id = existing["id"]
                else:
                    LOGGER.warning(
                        "Skipping row for username %s – unable to resolve numeric channel id", parsed_username
                    )
                    continue
            else:
                LOGGER.warning("Skipping row with unknown channel information: %s", source)
                continue

        try:
            channel_id = int(channel_id)
        except (TypeError, ValueError):
            LOGGER.warning("Skipping row with invalid channel id: %s", channel_id)
            continue

        message_id = row.get("message_id")
        if pd.isna(message_id):
            LOGGER.debug("Skipping row with missing message id")
            continue

        message_date = row.get("message_published_at") or row.get("message_date")
        message_text = row.get("message_content") or row.get("message_text")
        media_type = row.get("message_media_type") or row.get("media_type")

        record = {
            "message_text": message_text,
            "message_date": message_date,
            "media_type": media_type,
            "has_coordinates": 1,
        }

        row_id = database.add_message(int(channel_id), int(message_id), record)
        latitude = float(row.get("latitude"))
        longitude = float(row.get("longitude"))
        database.add_coordinate(row_id, latitude, longitude, coordinate_format="decimal")
        imported_coordinates += 1

    return imported_coordinates


def detect_and_migrate_all_results(results_folder: str = "results", database: Optional[CoordinatesDatabase] = None) -> int:
    """Detect CSV files in *results_folder* and import them into the database."""

    folder = Path(results_folder)
    if not folder.exists():
        LOGGER.info("Results folder %s does not exist – nothing to migrate", folder)
        return 0

    if database is None:
        database = CoordinatesDatabase()

    total_imported = 0
    for csv_file in folder.glob("*.csv"):
        try:
            LOGGER.info("Migrating %s", csv_file)
            imported = migrate_existing_csv_to_database(str(csv_file), database)
            LOGGER.info("Imported %s coordinate rows from %s", imported, csv_file)
            total_imported += imported
        except (OSError, ValueError, pd.errors.ParserError, sqlite3.DatabaseError) as error:  # pragma: no cover - best effort import
            LOGGER.error("Failed to migrate %s: %s", csv_file, error)

    return total_imported


__all__ = [
    "migrate_existing_csv_to_database",
    "detect_and_migrate_all_results",
    "migrate_schema_for_telegram_recs",
]


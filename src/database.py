"""Database management utilities for the Telegram coordinates scraper.

This module provides a :class:`CoordinatesDatabase` helper that centralises
all interactions with the SQLite backend used for message deduplication,
coordinate storage and channel/session statistics.

The implementation focuses on providing a thin abstraction over SQLite while
remaining easy to unit test.  All write operations are wrapped in simple
transactions to make the scraper resilient to interruptions.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import logging
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd


LOGGER = logging.getLogger(__name__)


@dataclass
class DatabaseStatistics:
    """Container returned by :meth:`CoordinatesDatabase.get_database_statistics`."""

    total_messages: int
    total_coordinates: int
    tracked_channels: int
    active_channels: int
    average_density: float
    last_scrape: Optional[str]


class CoordinatesDatabase:
    """SQLite database manager for Telegram coordinates scraper."""

    def __init__(self, db_path: str = "telegram_coordinates.db") -> None:
        self.db_path = Path(db_path)
        self._connection: Optional[sqlite3.Connection] = None
        self.connect()
        self.initialize_schema()

    # ------------------------------------------------------------------
    # Connection helpers
    def connect(self) -> sqlite3.Connection:
        """Create (or return) the SQLite connection.

        Returns
        -------
        sqlite3.Connection
            The connection instance with row access configured as ``sqlite3.Row``.
        """

        if self._connection is None:
            if not self.db_path.parent.exists() and str(self.db_path.parent) not in (".", ""):
                self.db_path.parent.mkdir(parents=True, exist_ok=True)

            connection = sqlite3.connect(self.db_path)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            self._connection = connection
        return self._connection

    def initialize_schema(self) -> bool:
        """Create database schema if it does not already exist."""

        schema_statements = [
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                message_text TEXT,
                message_date DATETIME,
                media_type TEXT,
                has_coordinates BOOLEAN DEFAULT 0,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, message_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS coordinates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_ref INTEGER NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                coordinate_format TEXT,
                extraction_confidence TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(message_ref) REFERENCES messages(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY,
                username TEXT,
                title TEXT,
                channel_type TEXT,
                first_scraped DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_scraped DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_messages INTEGER DEFAULT 0,
                messages_with_coordinates INTEGER DEFAULT 0,
                coordinate_density REAL DEFAULT 0.0,
                is_active BOOLEAN DEFAULT 1,
                notes TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS scrape_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_start DATETIME DEFAULT CURRENT_TIMESTAMP,
                session_end DATETIME,
                channels_scraped INTEGER DEFAULT 0,
                new_messages INTEGER DEFAULT 0,
                new_coordinates INTEGER DEFAULT 0,
                skipped_messages INTEGER DEFAULT 0,
                session_type TEXT,
                status TEXT DEFAULT 'in_progress',
                error_log TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(message_date)",
            "CREATE INDEX IF NOT EXISTS idx_messages_coordinates ON messages(has_coordinates)",
            "CREATE INDEX IF NOT EXISTS idx_coordinates_message ON coordinates(message_ref)",
            "CREATE INDEX IF NOT EXISTS idx_channels_density ON channels(coordinate_density DESC)",
        ]

        connection = self.connect()
        try:
            with connection:
                for statement in schema_statements:
                    connection.execute(statement)
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to initialise database schema: %s", error)
            return False
        return True

    def close(self) -> None:
        """Close the active SQLite connection."""

        if self._connection is not None:
            self._connection.close()
            self._connection = None

    # ------------------------------------------------------------------
    # Message operations
    def message_exists(self, channel_id: int, message_id: int) -> bool:
        query = "SELECT 1 FROM messages WHERE channel_id=? AND message_id=?"
        cursor = self.connect().execute(query, (channel_id, message_id))
        return cursor.fetchone() is not None

    def add_message(
        self,
        channel_id: int,
        message_id: int,
        message_data: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Insert a message row if it does not exist.

        Parameters
        ----------
        channel_id:
            Numerical Telegram channel identifier.
        message_id:
            Telegram message identifier.
        message_data:
            Optional mapping containing additional fields supported by the
            ``messages`` table. Unknown keys are ignored.
        """

        message_data = message_data or {}
        allowed = {
            "message_text",
            "message_date",
            "media_type",
            "has_coordinates",
            "processed_at",
            "last_updated",
        }
        filtered = {k: message_data[k] for k in message_data if k in allowed}
        columns = ["channel_id", "message_id"] + list(filtered.keys())
        placeholders = ", ".join(["?"] * len(columns))
        values: List[Any] = [channel_id, message_id] + list(filtered.values())

        sql = f"INSERT OR IGNORE INTO messages ({', '.join(columns)}) VALUES ({placeholders})"
        connection = self.connect()
        with connection:
            cursor = connection.execute(sql, values)
            row_id = cursor.lastrowid
            if row_id == 0:
                # The row already existed – we still update mutable columns
                if filtered:
                    assignments = ", ".join(f"{key}=?" for key in filtered)
                    update_values = list(filtered.values()) + [channel_id, message_id]
                    connection.execute(
                        f"""
                        UPDATE messages
                        SET {assignments}, last_updated=CURRENT_TIMESTAMP
                        WHERE channel_id=? AND message_id=?
                        """,
                        update_values,
                    )
                existing = connection.execute(
                    "SELECT id FROM messages WHERE channel_id=? AND message_id=?",
                    (channel_id, message_id),
                ).fetchone()
                return int(existing["id"]) if existing else 0
        return int(row_id)

    def bulk_add_messages(self, messages: Sequence[Dict[str, Any]]) -> int:
        """Insert multiple message entries.

        Parameters
        ----------
        messages:
            Sequence of dictionaries with ``channel_id`` and ``message_id`` keys
            plus optional additional columns.

        Returns
        -------
        int
            Number of successfully inserted records.
        """

        inserted = 0
        for payload in messages:
            try:
                channel_id = int(payload["channel_id"])
                message_id = int(payload["message_id"])
            except (KeyError, TypeError, ValueError) as error:
                LOGGER.warning("Skipping malformed message payload %s: %s", payload, error)
                continue

            row_id = self.add_message(channel_id, message_id, payload)
            if row_id:
                inserted += 1
        return inserted

    def get_latest_message_id(self, channel_id: int) -> Optional[int]:
        cursor = self.connect().execute(
            "SELECT MAX(message_id) AS latest FROM messages WHERE channel_id=?",
            (channel_id,),
        )
        row = cursor.fetchone()
        return int(row["latest"]) if row and row["latest"] is not None else None

    def get_message_count(self, channel_id: int) -> int:
        cursor = self.connect().execute(
            "SELECT COUNT(*) AS count FROM messages WHERE channel_id=?",
            (channel_id,),
        )
        row = cursor.fetchone()
        return int(row["count"]) if row else 0

    # ------------------------------------------------------------------
    # Coordinate operations
    def add_coordinate(
        self,
        message_ref: int,
        latitude: float,
        longitude: float,
        coordinate_format: Optional[str] = None,
        extraction_confidence: Optional[str] = None,
    ) -> int:
        sql = (
            "INSERT INTO coordinates (message_ref, latitude, longitude, coordinate_format, "
            "extraction_confidence) VALUES (?, ?, ?, ?, ?)"
        )
        connection = self.connect()
        with connection:
            cursor = connection.execute(
                sql,
                (
                    message_ref,
                    float(latitude),
                    float(longitude),
                    coordinate_format,
                    extraction_confidence,
                ),
            )
        return int(cursor.lastrowid)

    def get_coordinates_by_channel(self, channel_id: int) -> List[sqlite3.Row]:
        sql = (
            "SELECT c.*, m.message_id, m.message_date FROM coordinates c "
            "JOIN messages m ON m.id = c.message_ref WHERE m.channel_id=? ORDER BY m.message_id"
        )
        cursor = self.connect().execute(sql, (channel_id,))
        return list(cursor.fetchall())

    def get_all_coordinates(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[sqlite3.Row]:
        sql = (
            "SELECT c.*, m.channel_id, m.message_id, m.message_date FROM coordinates c "
            "JOIN messages m ON m.id = c.message_ref"
        )
        params: List[Any] = []
        conditions: List[str] = []

        if start_date:
            conditions.append("m.message_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("m.message_date <= ?")
            params.append(end_date)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        sql += " ORDER BY m.message_date"
        cursor = self.connect().execute(sql, params)
        return list(cursor.fetchall())

    # ------------------------------------------------------------------
    # Channel operations
    def add_or_update_channel(self, channel_id: int, channel_data: Dict[str, Any]) -> bool:
        defaults = {
            "username": None,
            "title": None,
            "channel_type": None,
            "is_active": 1,
            "notes": None,
        }
        payload = {**defaults, **(channel_data or {})}

        connection = self.connect()
        try:
            with connection:
                connection.execute(
                    """
                    INSERT INTO channels (id, username, title, channel_type, is_active, notes)
                    VALUES (:id, :username, :title, :channel_type, :is_active, :notes)
                    ON CONFLICT(id) DO UPDATE SET
                        username=excluded.username,
                        title=excluded.title,
                        channel_type=excluded.channel_type,
                        is_active=excluded.is_active,
                        notes=COALESCE(excluded.notes, channels.notes),
                        last_scraped=CURRENT_TIMESTAMP
                    """,
                    {"id": channel_id, **payload},
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to add/update channel %s: %s", channel_id, error)
            return False
        return True

    def get_channel_info(self, channel_id: int) -> Optional[Dict[str, Any]]:
        cursor = self.connect().execute(
            "SELECT * FROM channels WHERE id=?",
            (channel_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_channel_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        cursor = self.connect().execute(
            "SELECT * FROM channels WHERE username=?",
            (username,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_channels_with_coordinates(
        self,
        min_density: float = 0.0,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        sql = (
            "SELECT * FROM channels WHERE messages_with_coordinates > 0 "
            "AND coordinate_density >= ? ORDER BY coordinate_density DESC"
        )
        params: List[Any] = [min_density]
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        cursor = self.connect().execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def update_channel_statistics(self, channel_id: int) -> bool:
        connection = self.connect()
        cursor = connection.execute(
            "SELECT COUNT(*) AS total, SUM(has_coordinates) AS coord_count FROM messages WHERE channel_id=?",
            (channel_id,),
        )
        row = cursor.fetchone()
        if not row:
            return False

        total = int(row["total"] or 0)
        coord_count = int(row["coord_count"] or 0)
        density = (coord_count / total) * 100 if total else 0.0

        try:
            with connection:
                connection.execute(
                    """
                    UPDATE channels
                    SET total_messages=?,
                        messages_with_coordinates=?,
                        coordinate_density=?,
                        last_scraped=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (total, coord_count, density, channel_id),
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to update channel statistics for %s: %s", channel_id, error)
            return False
        return True

    def get_top_channels_by_density(self, limit: int = 10) -> List[Dict[str, Any]]:
        cursor = self.connect().execute(
            """
            SELECT * FROM channels
            WHERE total_messages > 0
            ORDER BY coordinate_density DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Session tracking
    def start_session(self, session_type: str) -> int:
        connection = self.connect()
        with connection:
            cursor = connection.execute(
                "INSERT INTO scrape_sessions (session_type) VALUES (?)",
                (session_type,),
            )
        return int(cursor.lastrowid)

    def end_session(self, session_id: int, stats: Dict[str, Any]) -> bool:
        stats = stats or {}
        fields = {
            "session_end": stats.get("session_end", _dt.datetime.utcnow().isoformat()),
            "channels_scraped": stats.get("channels_scraped"),
            "new_messages": stats.get("new_messages"),
            "new_coordinates": stats.get("new_coordinates"),
            "skipped_messages": stats.get("skipped_messages"),
            "status": stats.get("status", "completed"),
            "error_log": stats.get("error_log"),
        }
        assignments = ", ".join(f"{key}=?" for key in fields.keys())
        params = list(fields.values()) + [session_id]
        try:
            with self.connect():
                self.connect().execute(
                    f"UPDATE scrape_sessions SET {assignments} WHERE id=?",
                    params,
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to end session %s: %s", session_id, error)
            return False
        return True

    def get_session_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        cursor = self.connect().execute(
            "SELECT * FROM scrape_sessions ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Export and utilities
    def export_to_dataframe(self, channel_id: int = None) -> pd.DataFrame:
        sql = (
            "SELECT m.channel_id, m.message_id, m.message_text, m.message_date, m.media_type, "
            "m.has_coordinates, c.latitude, c.longitude, c.coordinate_format, c.extraction_confidence, "
            "c.created_at FROM messages m LEFT JOIN coordinates c ON m.id = c.message_ref"
        )
        params: List[Any] = []
        if channel_id is not None:
            sql += " WHERE m.channel_id=?"
            params.append(channel_id)
        sql += " ORDER BY m.channel_id, m.message_id"
        connection = self.connect()
        df = pd.read_sql_query(sql, connection, params=params)
        return df

    def get_database_statistics(self) -> DatabaseStatistics:
        connection = self.connect()
        total_messages = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        total_coordinates = connection.execute("SELECT COUNT(*) FROM coordinates").fetchone()[0]
        channel_counts = connection.execute(
            "SELECT COUNT(*), SUM(is_active) FROM channels"
        ).fetchone()
        tracked_channels = channel_counts[0] if channel_counts else 0
        active_channels = channel_counts[1] if channel_counts else 0
        average_density_row = connection.execute(
            "SELECT AVG(coordinate_density) FROM channels WHERE total_messages > 0"
        ).fetchone()
        average_density = float(average_density_row[0]) if average_density_row and average_density_row[0] is not None else 0.0
        last_scrape_row = connection.execute(
            "SELECT MAX(last_scraped) FROM channels"
        ).fetchone()
        last_scrape = last_scrape_row[0] if last_scrape_row else None
        return DatabaseStatistics(
            total_messages=int(total_messages or 0),
            total_coordinates=int(total_coordinates or 0),
            tracked_channels=int(tracked_channels or 0),
            active_channels=int(active_channels or 0),
            average_density=average_density,
            last_scrape=last_scrape,
        )

    def vacuum_database(self) -> bool:
        try:
            with self.connect():
                self.connect().execute("VACUUM")
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to vacuum database: %s", error)
            return False
        return True

    def backup_database(self, backup_path: str) -> bool:
        backup_path = Path(backup_path)
        connection = self.connect()
        try:
            if not backup_path.parent.exists():
                backup_path.parent.mkdir(parents=True, exist_ok=True)
            connection.execute("BEGIN IMMEDIATE")
            shutil.copyfile(self.db_path, backup_path)
            connection.commit()
        except (sqlite3.DatabaseError, OSError) as error:
            LOGGER.error("Failed to backup database to %s: %s", backup_path, error)
            with contextlib.suppress(sqlite3.DatabaseError):
                connection.rollback()
            return False
        finally:
            with contextlib.suppress(sqlite3.DatabaseError):
                connection.execute("END")
        return True


__all__ = ["CoordinatesDatabase", "DatabaseStatistics"]


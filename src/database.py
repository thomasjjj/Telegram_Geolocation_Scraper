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
import json
import logging
import shutil
import sqlite3
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from telethon.extensions import BinaryReader

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
            """
            CREATE TABLE IF NOT EXISTS recommended_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                title TEXT,
                channel_type TEXT,
                first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                discovered_from_channels TEXT,
                forward_count INTEGER DEFAULT 1,
                coordinate_forward_count INTEGER DEFAULT 1,
                recommendation_score REAL DEFAULT 0.0,
                user_status TEXT DEFAULT 'pending',
                user_notes TEXT,
                added_to_scrape_list DATETIME,
                last_scraped DATETIME,
                is_accessible BOOLEAN,
                requires_join BOOLEAN,
                total_messages INTEGER,
                estimated_coordinate_density REAL,
                actual_coordinate_density REAL,
                is_verified BOOLEAN DEFAULT 0,
                is_scam BOOLEAN DEFAULT 0,
                is_fake BOOLEAN DEFAULT 0,
                avg_message_views INTEGER,
                subscriber_count INTEGER,
                UNIQUE(channel_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS channel_forwards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_ref INTEGER NOT NULL,
                from_channel_id INTEGER NOT NULL,
                to_channel_id INTEGER NOT NULL,
                forward_date DATETIME,
                had_coordinates BOOLEAN DEFAULT 0,
                forward_signature TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (message_ref) REFERENCES messages(id) ON DELETE CASCADE,
                FOREIGN KEY (from_channel_id) REFERENCES recommended_channels(channel_id),
                FOREIGN KEY (to_channel_id) REFERENCES channels(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (channel_id) REFERENCES recommended_channels(channel_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_recommended_score ON recommended_channels(recommendation_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_recommended_status ON recommended_channels(user_status)",
            "CREATE INDEX IF NOT EXISTS idx_forwards_from ON channel_forwards(from_channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_forwards_to ON channel_forwards(to_channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_forwards_coordinates ON channel_forwards(had_coordinates)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_forwards_unique ON channel_forwards(message_ref, from_channel_id, to_channel_id)",
            """
            CREATE TABLE IF NOT EXISTS entity_cache (
                identifier TEXT PRIMARY KEY,
                entity_bytes BLOB NOT NULL,
                entity_type TEXT,
                channel_id INTEGER,
                access_hash INTEGER,
                username TEXT,
                title TEXT,
                stored_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_used DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """,
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
        LOGGER.info(
            "Saved coordinate (%s, %s) for message_ref %s with row id %s",
            latitude,
            longitude,
            message_ref,
            cursor.lastrowid,
        )
        return int(cursor.lastrowid)

    def bulk_add_coordinates(
        self,
        coordinates: List[Tuple[int, float, float]],
        coordinate_format: Optional[str] = "decimal",
        extraction_confidence: Optional[str] = "high",
    ) -> int:
        """Insert multiple coordinates in a single transaction."""

        if not coordinates:
            return 0

        sql = (
            "INSERT INTO coordinates (message_ref, latitude, longitude, coordinate_format, "
            "extraction_confidence) VALUES (?, ?, ?, ?, ?)"
        )

        normalized_coordinates = [
            (
                message_ref,
                float(latitude),
                float(longitude),
                coordinate_format,
                extraction_confidence,
            )
            for message_ref, latitude, longitude in coordinates
        ]

        connection = self.connect()
        with connection:
            cursor = connection.executemany(sql, normalized_coordinates)

        inserted = max(cursor.rowcount or 0, 0)
        LOGGER.info("Bulk inserted %s coordinates", inserted)
        return inserted

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

    # ------------------------------------------------------------------
    # Entity cache helpers
    def cache_entity(self, identifier: str, entity: Any) -> bool:
        """Persist a Telethon entity for reuse across scraping runs."""

        if not identifier or entity is None:
            return False

        identifier = str(identifier)

        try:
            entity_bytes = bytes(entity.to_bytes())
        except (AttributeError, TypeError, ValueError) as error:  # pragma: no cover - defensive
            LOGGER.debug("Failed to serialise entity %s: %s", identifier, error)
            return False

        metadata = {
            "entity_type": type(entity).__name__,
            "channel_id": getattr(entity, "id", None),
            "access_hash": getattr(entity, "access_hash", None),
            "username": getattr(entity, "username", None),
            "title": getattr(entity, "title", getattr(entity, "name", None)),
        }

        connection = self.connect()
        try:
            with connection:
                connection.execute(
                    """
                    INSERT INTO entity_cache (identifier, entity_bytes, entity_type, channel_id, access_hash, username, title)
                    VALUES (:identifier, :entity_bytes, :entity_type, :channel_id, :access_hash, :username, :title)
                    ON CONFLICT(identifier) DO UPDATE SET
                        entity_bytes=excluded.entity_bytes,
                        entity_type=excluded.entity_type,
                        channel_id=excluded.channel_id,
                        access_hash=excluded.access_hash,
                        username=excluded.username,
                        title=excluded.title,
                        last_used=CURRENT_TIMESTAMP
                    """,
                    {
                        "identifier": identifier,
                        "entity_bytes": sqlite3.Binary(entity_bytes),
                        **metadata,
                    },
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to cache entity %s: %s", identifier, error)
            return False
        return True

    def get_cached_entity(self, identifier: str) -> Any:
        """Retrieve a cached Telethon entity from the database if available."""

        if not identifier:
            return None

        identifier = str(identifier)

        cursor = self.connect().execute(
            "SELECT entity_bytes FROM entity_cache WHERE identifier=?",
            (identifier,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        raw_bytes = row["entity_bytes"]
        if raw_bytes is None:
            return None

        try:
            reader = BinaryReader(bytes(raw_bytes))
            entity = reader.tgread_object()
        except (TypeError, ValueError, struct.error) as error:  # pragma: no cover - defensive
            LOGGER.debug("Failed to deserialize cached entity %s: %s", identifier, error)
            return None

        try:
            with self.connect():
                self.connect().execute(
                    "UPDATE entity_cache SET last_used=CURRENT_TIMESTAMP WHERE identifier=?",
                    (identifier,),
                )
        except sqlite3.DatabaseError:  # pragma: no cover - cache hit is best-effort
            pass

        return entity

    def update_channel_statistics(self, channel_id: int) -> bool:
        connection = self.connect()
        cursor = connection.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(has_coordinates), 0) AS coord_count
            FROM messages
            WHERE channel_id=?
            """,
            (channel_id,),
        )
        row = cursor.fetchone()
        if not row:
            return False

        total = int(row["total"])
        coord_count = int(row["coord_count"])
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
    # Recommendation system helpers

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[sqlite3.Row]:
        cursor = self.connect().execute(sql, params or [])
        return cursor.fetchall()

    def query_one(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Optional[sqlite3.Row]:
        cursor = self.connect().execute(sql, params or [])
        return cursor.fetchone()

    def count(
        self,
        table: str,
        where: Optional[str] = None,
        params: Optional[Sequence[Any]] = None,
    ) -> int:
        sql = f"SELECT COUNT(*) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        row = self.connect().execute(sql, params or []).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def get_recommended_channel(self, channel_id: int) -> Optional[Dict[str, Any]]:
        row = self.query_one(
            "SELECT * FROM recommended_channels WHERE channel_id=?",
            (channel_id,),
        )
        return dict(row) if row else None

    def add_recommended_channel(self, channel_id: int, data: Dict[str, Any]) -> bool:
        allowed = {
            "username",
            "title",
            "channel_type",
            "first_seen",
            "last_seen",
            "discovered_from_channels",
            "forward_count",
            "coordinate_forward_count",
            "recommendation_score",
            "user_status",
            "user_notes",
            "added_to_scrape_list",
            "last_scraped",
            "is_accessible",
            "requires_join",
            "total_messages",
            "estimated_coordinate_density",
            "actual_coordinate_density",
            "is_verified",
            "is_scam",
            "is_fake",
            "avg_message_views",
            "subscriber_count",
        }

        filtered = {k: v for k, v in (data or {}).items() if k in allowed and v is not None}
        payload = {"channel_id": channel_id, **filtered}

        columns = list(payload.keys())
        placeholders = ", ".join(f":{col}" for col in columns)
        update_columns = ", ".join(
            f"{col}=excluded.{col}" for col in columns if col != "channel_id"
        )

        if update_columns:
            sql = (
                f"INSERT INTO recommended_channels ({', '.join(columns)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT(channel_id) DO UPDATE SET {update_columns}"
            )
        else:
            sql = "INSERT OR IGNORE INTO recommended_channels (channel_id) VALUES (:channel_id)"

        try:
            with self.connect():
                self.connect().execute(sql, payload)
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to upsert recommended channel %s: %s", channel_id, error)
            return False
        return True

    def update_recommended_channel(self, channel_id: int, data: Dict[str, Any]) -> bool:
        if not data:
            return False

        allowed = {
            "username",
            "title",
            "channel_type",
            "first_seen",
            "last_seen",
            "discovered_from_channels",
            "forward_count",
            "coordinate_forward_count",
            "recommendation_score",
            "user_status",
            "user_notes",
            "added_to_scrape_list",
            "last_scraped",
            "is_accessible",
            "requires_join",
            "total_messages",
            "estimated_coordinate_density",
            "actual_coordinate_density",
            "is_verified",
            "is_scam",
            "is_fake",
            "avg_message_views",
            "subscriber_count",
        }

        filtered = {k: v for k, v in data.items() if k in allowed}
        if not filtered:
            return False

        assignments = ", ".join(f"{key}=?" for key in filtered)
        values = list(filtered.values()) + [channel_id]

        try:
            with self.connect():
                self.connect().execute(
                    f"UPDATE recommended_channels SET {assignments} WHERE channel_id=?",
                    values,
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error("Failed to update recommended channel %s: %s", channel_id, error)
            return False
        return True

    def add_channel_forward(
        self,
        message_ref: int,
        from_channel_id: int,
        to_channel_id: int,
        had_coordinates: bool,
        forward_date: Optional[Union[_dt.datetime, str]] = None,
        forward_signature: Optional[str] = None,
    ) -> bool:
        if message_ref is None:
            return False

        if isinstance(forward_date, _dt.datetime):
            forward_date_value = forward_date.isoformat()
        else:
            forward_date_value = forward_date

        try:
            with self.connect():
                self.connect().execute(
                    """
                    INSERT OR IGNORE INTO channel_forwards (
                        message_ref,
                        from_channel_id,
                        to_channel_id,
                        forward_date,
                        had_coordinates,
                        forward_signature
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_ref,
                        from_channel_id,
                        to_channel_id,
                        forward_date_value,
                        int(bool(had_coordinates)),
                        forward_signature,
                    ),
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error(
                "Failed to add channel forward record (from %s to %s): %s",
                from_channel_id,
                to_channel_id,
                error,
            )
            return False
        return True

    def add_recommendation_event(
        self,
        channel_id: int,
        event_type: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        payload = json.dumps(details or {})
        try:
            with self.connect():
                self.connect().execute(
                    "INSERT INTO recommendation_events (channel_id, event_type, event_details) VALUES (?, ?, ?)",
                    (channel_id, event_type, payload),
                )
        except sqlite3.DatabaseError as error:
            LOGGER.error(
                "Failed to log recommendation event for channel %s: %s",
                channel_id,
                error,
            )
            return False
        return True

    def get_forward_statistics(self) -> Dict[str, Any]:
        total_forwards = self.count("channel_forwards")
        coord_forwards = self.count("channel_forwards", "had_coordinates=1")
        row = self.query_one("SELECT COUNT(DISTINCT from_channel_id) AS cnt FROM channel_forwards")
        distinct_sources = int(row["cnt"]) if row and row["cnt"] is not None else 0
        return {
            "total_forwards": total_forwards,
            "coordinate_forwards": coord_forwards,
            "distinct_forward_sources": distinct_sources,
        }

    def get_channels_by_forward_source(self, source_channel_id: int) -> List[Dict[str, Any]]:
        rows = self.query(
            """
            SELECT rc.*
            FROM recommended_channels rc
            JOIN channel_forwards cf ON cf.from_channel_id = rc.channel_id
            WHERE cf.to_channel_id=?
            GROUP BY rc.channel_id
            ORDER BY rc.recommendation_score DESC
            """,
            (source_channel_id,),
        )
        return [dict(row) for row in rows]

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
            """
            SELECT
                COUNT(*) AS total_channels,
                COALESCE(SUM(is_active), 0) AS active_channels
            FROM channels
            """
        ).fetchone()
        tracked_channels = (
            int(channel_counts["total_channels"]) if channel_counts else 0
        )
        active_channels = (
            int(channel_counts["active_channels"]) if channel_counts else 0
        )
        average_density_row = connection.execute(
            """
            SELECT
                COALESCE(AVG(coordinate_density), 0.0) AS average_density
            FROM channels
            WHERE total_messages > 0
            """
        ).fetchone()
        average_density = (
            float(average_density_row["average_density"]) if average_density_row else 0.0
        )
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


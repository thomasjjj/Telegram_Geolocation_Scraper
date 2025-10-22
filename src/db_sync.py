"""Database synchronisation helpers for the Telegram coordinates scraper."""

from __future__ import annotations

import gzip
import json
import logging
import platform
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar

from src.database import CoordinatesDatabase


LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency for interactive feedback
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for interactive feedback
    tqdm = None


T = TypeVar("T")


def _progress_iterable(
    iterable: Iterable[T],
    *,
    desc: str,
    unit: str = "items",
    total: Optional[int] = None,
) -> Iterable[T]:
    """Wrap *iterable* with a tqdm progress bar when available."""

    if tqdm is None:
        return iterable

    if total is None:
        try:
            total = len(iterable)  # type: ignore[arg-type]
        except (TypeError, AttributeError):
            total = None

    return tqdm(iterable, desc=desc, unit=unit, total=total, leave=False)


class MergeStrategy(Enum):
    """Strategies for reconciling conflicts during database imports."""

    CONSERVATIVE = "conservative"
    AGGRESSIVE = "aggressive"
    SMART = "smart"


@dataclass
class ImportStats:
    """Statistics captured during an import run."""

    messages_imported: int = 0
    messages_updated: int = 0
    messages_skipped: int = 0
    coordinates_imported: int = 0
    channels_added: int = 0
    channels_updated: int = 0
    recommendations_merged: int = 0
    conflicts_resolved: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "messages_imported": self.messages_imported,
            "messages_updated": self.messages_updated,
            "messages_skipped": self.messages_skipped,
            "coordinates_imported": self.coordinates_imported,
            "channels_added": self.channels_added,
            "channels_updated": self.channels_updated,
            "recommendations_merged": self.recommendations_merged,
            "conflicts_resolved": self.conflicts_resolved,
        }


class DatabaseExporter:
    """Export database content to portable formats."""

    def __init__(self, database: CoordinatesDatabase) -> None:
        self.db = database
        self.logger = logging.getLogger(__name__)

    def export_to_json(
        self,
        output_path: str,
        *,
        compress: bool = True,
        incremental_since: Optional[datetime] = None,
        include_sessions: bool = True,
        include_recommendations: bool = True,
    ) -> Dict[str, Any]:
        """Export the database contents to a JSON file."""

        export_data: Dict[str, Any] = {
            "version": "2.0",
            "export_date": datetime.utcnow().isoformat(),
            "device_id": self._get_device_id(),
            "schema_version": self.db.get_schema_version(),
        }

        export_data["channels"] = self._export_channels_with_progress()
        export_data["messages"] = self._export_messages(incremental_since)
        export_data["coordinates"] = self._export_coordinates(incremental_since)

        if include_sessions:
            export_data["sessions"] = self._export_sessions(incremental_since)
        if include_recommendations:
            export_data["recommendations"] = self._export_recommendations()

        destination = Path(output_path)
        if compress and destination.suffix != ".gz":
            destination = destination.with_suffix(destination.suffix + ".gz")

        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.suffix == ".gz":
            with gzip.open(destination, "wt", encoding="utf-8") as handle:
                json.dump(export_data, handle, indent=2)
        else:
            with destination.open("w", encoding="utf-8") as handle:
                json.dump(export_data, handle, indent=2)

        try:
            file_size = destination.stat().st_size
        except OSError:
            file_size = 0

        summary = {
            "path": str(destination),
            "size": file_size,
            "message_count": len(export_data["messages"]),
            "channel_count": len(export_data["channels"]),
            "compressed": destination.suffix == ".gz",
        }
        self.logger.info(
            "Exported %s messages and %s channels to %s",
            summary["message_count"],
            summary["channel_count"],
            summary["path"],
        )
        return summary

    def _export_messages(
        self, incremental_since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM messages"
        params: List[Any] = []
        if incremental_since:
            sql += " WHERE last_updated > ?"
            params.append(incremental_since.isoformat())
        sql += " ORDER BY channel_id, message_id"
        cursor = self.db.connect().execute(sql, params)
        rows = cursor.fetchall()
        return [
            dict(row)
            for row in _progress_iterable(rows, desc="Exporting messages", unit="messages")
        ]

    def _export_coordinates(
        self, incremental_since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        sql = (
            "SELECT c.*, m.channel_id, m.message_id, m.last_updated as message_last_updated "
            "FROM coordinates c JOIN messages m ON m.id = c.message_ref"
        )
        params: List[Any] = []
        if incremental_since:
            sql += " WHERE m.last_updated > ?"
            params.append(incremental_since.isoformat())
        sql += " ORDER BY c.id"
        cursor = self.db.connect().execute(sql, params)
        rows = cursor.fetchall()
        return [
            dict(row)
            for row in _progress_iterable(rows, desc="Exporting coordinates", unit="coordinates")
        ]

    def _export_channels_with_progress(self) -> List[Dict[str, Any]]:
        cursor = self.db.connect().execute(
            """
            SELECT 
                c.*, 
                MAX(m.message_id) AS latest_message_id,
                MIN(m.message_id) AS earliest_message_id,
                COUNT(DISTINCT m.id) AS actual_message_count
            FROM channels c
            LEFT JOIN messages m ON c.id = m.channel_id
            GROUP BY c.id
            ORDER BY c.id
            """
        )
        rows = cursor.fetchall()
        return [
            dict(row)
            for row in _progress_iterable(rows, desc="Exporting channels", unit="channels")
        ]

    def _export_sessions(
        self, incremental_since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM scrape_sessions"
        params: List[Any] = []
        if incremental_since:
            sql += " WHERE session_start > ?"
            params.append(incremental_since.isoformat())
        sql += " ORDER BY id"
        cursor = self.db.connect().execute(sql, params)
        rows = cursor.fetchall()
        return [
            dict(row)
            for row in _progress_iterable(rows, desc="Exporting sessions", unit="sessions")
        ]

    def _export_recommendations(self) -> List[Dict[str, Any]]:
        cursor = self.db.connect().execute(
            "SELECT * FROM recommended_channels ORDER BY recommendation_score DESC"
        )
        rows = cursor.fetchall()
        return [
            dict(row)
            for row in _progress_iterable(
                rows, desc="Exporting recommendations", unit="recommendations"
            )
        ]

    def _get_device_id(self) -> str:
        device_id = self.db.get_metadata("device_id")
        if device_id:
            return device_id
        device_id = platform.node() or "unknown-device"
        self.db.set_metadata("device_id", device_id)
        return device_id


class DatabaseImporter:
    """Import database exports into the active database."""

    def __init__(
        self,
        database: CoordinatesDatabase,
        strategy: MergeStrategy = MergeStrategy.SMART,
    ) -> None:
        self.db = database
        self.strategy = strategy
        self.logger = logging.getLogger(__name__)
        self.stats = ImportStats()
        self._message_row_cache: Dict[Tuple[int, int], int] = {}

    def import_from_json(
        self,
        import_path: str,
        *,
        dry_run: bool = False,
        create_backup: bool = True,
    ) -> ImportStats:
        """Import data from a JSON (optionally gzipped) export."""

        source = Path(import_path)
        if not source.exists():
            raise FileNotFoundError(f"Import file not found: {source}")

        if create_backup and not dry_run:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db.db_path}.backup_{timestamp}"
            if self.db.backup_database(backup_path):
                self.logger.info("Created database backup at %s", backup_path)

        if source.suffix == ".gz":
            with gzip.open(source, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        else:
            with source.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

        self._validate_import_data(payload)

        connection = self.db.connect()
        self._message_row_cache.clear()

        try:
            connection.execute("BEGIN IMMEDIATE")
            self._import_channels(payload.get("channels", []), dry_run)
            self._import_messages(payload.get("messages", []), dry_run)
            self._import_coordinates(payload.get("coordinates", []), dry_run)
            self._import_recommendations(payload.get("recommendations", []), dry_run)
            self._update_channel_progress(payload.get("channels", []), dry_run)
            self._import_sessions(payload.get("sessions", []), dry_run)

            if dry_run:
                connection.rollback()
                self.logger.info("Dry run completed; no changes applied")
            else:
                connection.commit()
                self.db.log_import_history(
                    {
                        "source_file": str(source),
                        "source_device": payload.get("device_id"),
                        "import_type": "json",
                        **self.stats.to_dict(),
                        "status": "completed",
                    }
                )
        except Exception:
            connection.rollback()
            raise
        finally:
            self._message_row_cache.clear()

        return self.stats

    def import_from_sqlite(
        self,
        source_db_path: str,
        *,
        validate_schema: bool = False,
    ) -> ImportStats:
        source = Path(source_db_path)
        if not source.exists():
            raise FileNotFoundError(f"Source database not found: {source}")

        connection = self.db.connect()
        connection.execute(f"ATTACH DATABASE '{source}' AS source_db")
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO channels (
                    id, username, title, channel_type, first_scraped, last_scraped,
                    total_messages, messages_with_coordinates, coordinate_density,
                    is_active, notes
                )
                SELECT id, username, title, channel_type, first_scraped, last_scraped,
                       total_messages, messages_with_coordinates, coordinate_density,
                       is_active, notes
                FROM source_db.channels
                ON CONFLICT(id) DO UPDATE SET
                    username=excluded.username,
                    title=COALESCE(excluded.title, channels.title),
                    channel_type=COALESCE(excluded.channel_type, channels.channel_type),
                    last_scraped=MAX(channels.last_scraped, excluded.last_scraped),
                    total_messages=MAX(channels.total_messages, excluded.total_messages),
                    messages_with_coordinates=MAX(
                        channels.messages_with_coordinates,
                        excluded.messages_with_coordinates
                    ),
                    coordinate_density=MAX(
                        channels.coordinate_density,
                        excluded.coordinate_density
                    ),
                    is_active=excluded.is_active
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO messages
                SELECT * FROM source_db.messages
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO coordinates
                SELECT * FROM source_db.coordinates
                """
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.execute("DETACH DATABASE source_db")

        return self.stats

    # ------------------------------------------------------------------
    # Internal helpers
    def _validate_import_data(self, data: Dict[str, Any]) -> None:
        if "channels" not in data or "messages" not in data:
            raise ValueError("Import payload is missing required sections")

    def _import_channels(self, channels: Iterable[Dict[str, Any]], dry_run: bool) -> None:
        connection = self.db.connect()
        for channel in _progress_iterable(
            channels, desc="Importing channels", unit="channels"
        ):
            channel_id = channel.get("id")
            if channel_id is None:
                continue
            try:
                channel_id = int(channel_id)
            except (TypeError, ValueError):
                continue

            existing = self.db.get_channel_info(channel_id)
            if existing:
                self.stats.channels_updated += 1
            else:
                self.stats.channels_added += 1

            if dry_run:
                continue

            payload = {
                "username": channel.get("username"),
                "title": channel.get("title"),
                "channel_type": channel.get("channel_type"),
                "first_scraped": channel.get("first_scraped"),
                "last_scraped": channel.get("last_scraped"),
                "total_messages": channel.get("total_messages"),
                "messages_with_coordinates": channel.get("messages_with_coordinates"),
                "coordinate_density": channel.get("coordinate_density"),
                "is_active": channel.get("is_active", 1),
                "notes": channel.get("notes"),
            }

            columns = ["id"] + [key for key, value in payload.items() if value is not None]
            values = [channel_id] + [payload[key] for key in columns[1:]]
            assignments = []
            for key in columns[1:]:
                if key in {"total_messages", "messages_with_coordinates", "coordinate_density"}:
                    assignments.append(
                        f"{key}=MAX(channels.{key}, excluded.{key})"
                    )
                elif key == "last_scraped":
                    assignments.append(
                        "last_scraped=MAX(channels.last_scraped, excluded.last_scraped)"
                    )
                else:
                    assignments.append(f"{key}=COALESCE(excluded.{key}, channels.{key})")

            if not columns[1:]:
                columns.append("is_active")
                values.append(1)
                assignments.append("is_active=excluded.is_active")

            sql = (
                f"INSERT INTO channels ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))}) "
                f"ON CONFLICT(id) DO UPDATE SET {', '.join(assignments)}"
            )
            connection.execute(sql, values)

    def _import_messages(self, messages: Iterable[Dict[str, Any]], dry_run: bool) -> None:
        batch_size = 500
        message_stream = _progress_iterable(messages, desc="Importing messages", unit="messages")
        for batch in self._batch(message_stream, batch_size):
            existing = self._get_existing_message_map(batch)
            for message in batch:
                channel_id = message.get("channel_id")
                message_id = message.get("message_id")
                if channel_id is None or message_id is None:
                    continue
                key = (int(channel_id), int(message_id))
                record = existing.get(key)

                if record:
                    if self.strategy == MergeStrategy.CONSERVATIVE:
                        self.stats.messages_skipped += 1
                        self._message_row_cache[key] = int(record["id"])
                        continue

                    if self.strategy == MergeStrategy.SMART and not self._should_update_message(
                        message, record
                    ):
                        self.stats.messages_skipped += 1
                        self._message_row_cache[key] = int(record["id"])
                        continue

                    if not dry_run:
                        self._update_message(message)
                    self.stats.messages_updated += 1
                    self._message_row_cache[key] = int(
                        record.get("id") or self._fetch_message_row_id(*key)
                    )
                    continue

                if not dry_run:
                    row_id = self._insert_message(message)
                    if row_id:
                        self._message_row_cache[key] = row_id
                self.stats.messages_imported += 1

    def _import_coordinates(
        self, coordinates: Iterable[Dict[str, Any]], dry_run: bool
    ) -> None:
        for coord in _progress_iterable(
            coordinates, desc="Importing coordinates", unit="coordinates"
        ):
            channel_id = coord.get("channel_id")
            message_id = coord.get("message_id")
            if channel_id is None or message_id is None:
                continue

            key = (int(channel_id), int(message_id))
            message_ref = self._message_row_cache.get(key)
            if message_ref is None:
                message_ref = self._fetch_message_row_id(*key)
                if message_ref:
                    self._message_row_cache[key] = message_ref

            if not message_ref:
                continue

            latitude = coord.get("latitude")
            longitude = coord.get("longitude")
            if latitude is None or longitude is None:
                continue

            if dry_run:
                self.stats.coordinates_imported += 1
                continue

            if self._coordinate_exists(message_ref, float(latitude), float(longitude)):
                continue

            self.db.add_coordinate(
                message_ref,
                float(latitude),
                float(longitude),
                coordinate_format=coord.get("coordinate_format"),
                extraction_confidence=coord.get("extraction_confidence"),
            )
            self.stats.coordinates_imported += 1

    def _import_recommendations(
        self, recommendations: Iterable[Dict[str, Any]], dry_run: bool
    ) -> None:
        recommendations_list = list(recommendations)
        iterator = _progress_iterable(
            recommendations_list,
            desc="Importing recommendations",
            unit="recommendations",
        )
        if dry_run:
            processed = 0
            for record in iterator:
                if record.get("channel_id") is None:
                    continue
                processed += 1
            self.stats.recommendations_merged += processed
            return

        connection = self.db.connect()
        for record in iterator:
            channel_id = record.get("channel_id")
            if channel_id is None:
                continue
            columns = ["channel_id"]
            values = [channel_id]
            assignments = []
            for key, value in record.items():
                if key == "channel_id" or value is None:
                    continue
                columns.append(key)
                values.append(value)
                assignments.append(f"{key}=excluded.{key}")

            sql = (
                f"INSERT INTO recommended_channels ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))}) "
                f"ON CONFLICT(channel_id) DO UPDATE SET {', '.join(assignments)}"
            )
            connection.execute(sql, values)
            self.stats.recommendations_merged += 1

    def _import_sessions(self, sessions: Iterable[Dict[str, Any]], dry_run: bool) -> None:
        session_iterator = _progress_iterable(
            sessions, desc="Importing sessions", unit="sessions"
        )
        if dry_run:
            for _ in session_iterator:
                pass
            return
        connection = self.db.connect()
        for session in session_iterator:
            columns = [key for key in session if key in {
                "id",
                "session_start",
                "session_end",
                "channels_scraped",
                "new_messages",
                "new_coordinates",
                "skipped_messages",
                "session_type",
                "status",
                "error_log",
            }]
            if not columns:
                continue
            values = [session[key] for key in columns]
            placeholders = ", ".join("?" for _ in columns)
            sql = (
                f"INSERT OR IGNORE INTO scrape_sessions ({', '.join(columns)}) "
                f"VALUES ({placeholders})"
            )
            connection.execute(sql, values)

    def _update_channel_progress(
        self, channels: Iterable[Dict[str, Any]], dry_run: bool
    ) -> None:
        for channel in _progress_iterable(
            channels, desc="Updating channel progress", unit="channels"
        ):
            channel_id = channel.get("id")
            latest_imported = channel.get("latest_message_id")
            if channel_id is None or latest_imported is None:
                continue
            try:
                channel_id_int = int(channel_id)
                latest_imported_int = int(latest_imported)
            except (TypeError, ValueError):
                continue

            current_latest = self.db.get_latest_message_id(channel_id_int)
            if current_latest is not None and current_latest >= latest_imported_int:
                continue

            if dry_run:
                self.logger.debug(
                    "Would update progress for channel %s from %s to %s",
                    channel_id_int,
                    current_latest,
                    latest_imported_int,
                )
                continue

            self.db.connect().execute(
                "UPDATE channels SET last_scraped = ?, total_messages = MAX(total_messages, ?) WHERE id = ?",
                (
                    channel.get("last_scraped"),
                    channel.get("actual_message_count") or latest_imported_int,
                    channel_id_int,
                ),
            )

    def _coordinate_exists(self, message_ref: int, latitude: float, longitude: float) -> bool:
        cursor = self.db.connect().execute(
            """
            SELECT 1 FROM coordinates
            WHERE message_ref = ? AND ABS(latitude - ?) < 1e-9 AND ABS(longitude - ?) < 1e-9
            LIMIT 1
            """,
            (message_ref, latitude, longitude),
        )
        return cursor.fetchone() is not None

    def _insert_message(self, message: Dict[str, Any]) -> Optional[int]:
        channel_id = int(message["channel_id"])
        message_id = int(message["message_id"])
        payload = {
            key: message.get(key)
            for key in {
                "message_text",
                "message_date",
                "media_type",
                "has_coordinates",
                "processed_at",
                "last_updated",
                "sync_hash",
                "source_device",
                "import_batch_id",
            }
            if message.get(key) is not None
        }
        return self.db.add_message(channel_id, message_id, payload)

    def _update_message(self, message: Dict[str, Any]) -> None:
        channel_id = int(message["channel_id"])
        message_id = int(message["message_id"])
        payload = {
            key: message.get(key)
            for key in {
                "message_text",
                "message_date",
                "media_type",
                "has_coordinates",
                "processed_at",
                "last_updated",
                "sync_hash",
                "source_device",
                "import_batch_id",
            }
            if message.get(key) is not None
        }
        if not payload:
            return
        assignments = ", ".join(f"{key}=?" for key in payload)
        values = list(payload.values()) + [channel_id, message_id]
        self.db.connect().execute(
            f"""
            UPDATE messages
            SET {assignments}
            WHERE channel_id=? AND message_id=?
            """,
            values,
        )

    def _should_update_message(
        self, incoming: Dict[str, Any], existing: Dict[str, Any]
    ) -> bool:
        incoming_updated = incoming.get("last_updated")
        existing_updated = existing.get("last_updated")
        if not incoming_updated:
            return False
        if not existing_updated:
            return True
        try:
            incoming_dt = datetime.fromisoformat(str(incoming_updated))
            existing_dt = datetime.fromisoformat(str(existing_updated))
        except ValueError:
            return True
        return incoming_dt > existing_dt

    def _fetch_message_row_id(self, channel_id: int, message_id: int) -> Optional[int]:
        cursor = self.db.connect().execute(
            "SELECT id FROM messages WHERE channel_id=? AND message_id=?",
            (channel_id, message_id),
        )
        row = cursor.fetchone()
        return int(row["id"]) if row else None

    def _get_existing_message_map(
        self, batch: Iterable[Dict[str, Any]]
    ) -> Dict[Tuple[int, int], Dict[str, Any]]:
        grouped: Dict[int, List[int]] = {}
        for record in batch:
            channel_id = record.get("channel_id")
            message_id = record.get("message_id")
            if channel_id is None or message_id is None:
                continue
            try:
                grouped.setdefault(int(channel_id), []).append(int(message_id))
            except (TypeError, ValueError):
                continue

        if not grouped:
            return {}

        connection = self.db.connect()
        results: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for channel_id, message_ids in grouped.items():
            placeholders = ",".join("?" for _ in message_ids)
            sql = (
                "SELECT id, channel_id, message_id, last_updated FROM messages "
                "WHERE channel_id=? AND message_id IN (" + placeholders + ")"
            )
            cursor = connection.execute(sql, [channel_id, *message_ids])
            for row in cursor.fetchall():
                key = (int(row["channel_id"]), int(row["message_id"]))
                results[key] = dict(row)
        return results

    def _batch(self, items: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        batch: List[Dict[str, Any]] = []
        for item in items:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch


def perform_database_sync(
    database: CoordinatesDatabase,
    source_path: str,
    *,
    strategy: str = "smart",
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Import *source_path* into *database* using the desired *strategy*."""

    strategy_map = {
        "conservative": MergeStrategy.CONSERVATIVE,
        "aggressive": MergeStrategy.AGGRESSIVE,
        "smart": MergeStrategy.SMART,
    }
    merge_strategy = strategy_map.get(strategy.lower(), MergeStrategy.SMART)

    importer = DatabaseImporter(database, merge_strategy)
    path = Path(source_path)

    if path.suffix in {".json", ".gz"}:
        stats = importer.import_from_json(str(path), dry_run=dry_run)
    elif path.suffix == ".db":
        stats = importer.import_from_sqlite(str(path))
    else:
        raise ValueError(f"Unsupported import file type: {path.suffix}")

    return {
        "success": True,
        "stats": stats,
        "strategy": merge_strategy.value,
        "source": str(path),
        "dry_run": dry_run,
    }


__all__ = [
    "DatabaseExporter",
    "DatabaseImporter",
    "ImportStats",
    "MergeStrategy",
    "perform_database_sync",
]

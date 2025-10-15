"""Async scraping helpers with optional SQLite integration."""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, TYPE_CHECKING

import pandas as pd
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, MessageMediaDocument, MessageMediaPhoto

from src.coordinates import extract_coordinates
from src.database import CoordinatesDatabase
from src.export import save_dataframe_to_kml, save_dataframe_to_kmz
from src.entity_cache import EntityCache

if TYPE_CHECKING:  # pragma: no cover - optional dependency for type checking
    from src.recommendations import RecommendationManager


LOGGER = logging.getLogger(__name__)


@dataclass
class ScrapeStats:
    """Aggregated information about a scraping run for a single channel."""

    channel_id: int
    messages_processed: int = 0
    messages_inserted: int = 0
    messages_skipped: int = 0
    coordinates_found: int = 0


def _determine_channel_type(entity: Channel | Chat) -> str:
    if isinstance(entity, Channel):
        if getattr(entity, "megagroup", False):
            return "supergroup"
        return "channel"
    return "group"


async def scrape_channel(
    client: TelegramClient,
    channel_id,
    date_limit: Optional[datetime.datetime],
    coordinate_pattern: Optional[re.Pattern] = None,
    database: Optional[CoordinatesDatabase] = None,
    skip_existing: bool = True,
    recommendation_manager: "RecommendationManager" | None = None,
    entity_cache: EntityCache | None = None,
) -> Tuple[List[int], List[str], List[str], List[str], List[str], List[float], List[float], ScrapeStats]:
    """Scrape a single channel for coordinates with optional database integration."""

    message_ids: List[int] = []
    message_texts: List[str] = []
    media_types: List[str] = []
    dates: List[str] = []
    sources: List[str] = []
    latitudes: List[float] = []
    longitudes: List[float] = []

    if coordinate_pattern is None:
        coordinate_pattern = re.compile(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)")

    stats = ScrapeStats(channel_id=0)

    try:
        cache = entity_cache or EntityCache(client, database)
        entity = await cache.get_entity(channel_id)
        resolved_channel_id = getattr(entity, "id", channel_id)
        channel_display_name = (
            getattr(entity, "title", None)
            or getattr(entity, "name", None)
            or getattr(entity, "username", None)
            or str(resolved_channel_id)
        )
        stats.channel_id = resolved_channel_id

        latest_message_id: Optional[int] = None
        if database:
            database.add_or_update_channel(
                resolved_channel_id,
                {
                    "username": getattr(entity, "username", None),
                    "title": getattr(entity, "title", getattr(entity, "name", None)),
                    "channel_type": _determine_channel_type(entity),
                },
            )
            if skip_existing:
                latest_message_id = database.get_latest_message_id(resolved_channel_id)
                if latest_message_id:
                    LOGGER.info(
                        "Resuming channel %s from message id %s", resolved_channel_id, latest_message_id
                    )

        iter_kwargs = {"reverse": True}
        if date_limit:
            iter_kwargs["offset_date"] = date_limit
        if latest_message_id and skip_existing:
            iter_kwargs["min_id"] = latest_message_id

        coordinate_batch: List[Tuple[int, float, float]] = []

        async for message in client.iter_messages(entity, **iter_kwargs):
            stats.messages_processed += 1
            if not message or not message.message:
                continue

            existing_entry = False
            if database and skip_existing:
                if database.message_exists(resolved_channel_id, message.id):
                    stats.messages_skipped += 1
                    continue
            elif database:
                existing_entry = database.message_exists(resolved_channel_id, message.id)

            message_text = str(message.message)
            has_coordinates = False

            matches = coordinate_pattern.findall(message_text)
            if not matches:
                extracted = extract_coordinates(message_text)
                if extracted:
                    matches = [extracted]

            media_type = "text"
            if message.media:
                if isinstance(message.media, MessageMediaPhoto):
                    media_type = "photo"
                elif isinstance(message.media, MessageMediaDocument):
                    media_type = "video/mp4"
                else:
                    media_type = "other_media"

            message_date = message.date.strftime("%Y-%m-%d") if message.date else None

            source: str
            username = getattr(entity, "username", None)
            if username:
                source = f"t.me/{username}/{message.id}"
            else:
                source = f"t.me/c/{resolved_channel_id}/{message.id}"

            record = {
                "message_text": message_text,
                "message_date": message.date.isoformat() if message.date else None,
                "media_type": media_type,
                "has_coordinates": 0,
            }

            row_id = 0
            if database:
                row_id = database.add_message(resolved_channel_id, message.id, record)
                if row_id and not existing_entry:
                    stats.messages_inserted += 1

            if matches:
                has_coordinates = True
                record["has_coordinates"] = 1

                # FIX: Append message metadata once per coordinate, not once per message
                for latitude, longitude in matches:
                    lat_value = float(latitude)
                    lon_value = float(longitude)

                    # Append metadata for each coordinate
                    message_ids.append(message.id)
                    message_texts.append(message_text)
                    media_types.append(media_type)
                    dates.append(message_date or "")
                    sources.append(source)
                    latitudes.append(lat_value)
                    longitudes.append(lon_value)

                    stats.coordinates_found += 1

                    LOGGER.info(
                        "Retrieved coordinate (%s, %s) from message %s in channel %s",
                        lat_value,
                        lon_value,
                        message.id,
                        channel_display_name,
                    )

                    if database and row_id:
                        coordinate_batch.append((row_id, lat_value, lon_value))
                        if len(coordinate_batch) >= 100:
                            database.bulk_add_coordinates(coordinate_batch)
                            coordinate_batch.clear()

            if database and row_id:
                # Update message flag if coordinates were found
                if has_coordinates:
                    database.add_message(
                        resolved_channel_id,
                        message.id,
                        {"has_coordinates": 1, "last_updated": datetime.datetime.utcnow().isoformat()},
                    )

            if recommendation_manager:
                try:
                    recommendation_manager.process_forwarded_message(
                        message=message,
                        current_channel_id=resolved_channel_id,
                        has_coordinates=has_coordinates,
                        message_row_id=row_id if row_id else None,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    LOGGER.debug("Recommendation processing failed for message %s: %s", message.id, exc)

        if database and coordinate_batch:
            database.bulk_add_coordinates(coordinate_batch)
            coordinate_batch.clear()

        if database:
            database.update_channel_statistics(resolved_channel_id)

    except Exception as error:  # pragma: no cover - Telethon errors hard to simulate in tests
        LOGGER.error("Error scraping channel %s: %s", channel_id, error)

    return (
        message_ids,
        message_texts,
        media_types,
        dates,
        sources,
        latitudes,
        longitudes,
        stats,
    )


def _ensure_sequence(value: Sequence[str] | str) -> Sequence[str]:
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


def channel_scraper(
    channel_links: Sequence[str] | str,
    date_limit: Optional[str],
    output_path: str | None = None,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
    session_name: str = "simple_scraper",
    kml_output_path: Optional[str] = None,
    kmz_output_path: Optional[str] = None,
    use_database: bool = True,
    skip_existing: bool = True,
    db_path: Optional[str] = None,
    database: Optional[CoordinatesDatabase] = None,
    recommendation_manager: "RecommendationManager" | None = None,
    auto_visualize: bool = False,
    visualization_type: str = "auto",
) -> pd.DataFrame:
    """Scrape Telegram channels for coordinates and optionally export the results.

    When *output_path* is provided the collected coordinates are written to that
    CSV file. Otherwise the information is only persisted to the configured
    database (if enabled).
    """

    parsed_date_limit: Optional[datetime.datetime] = None
    if date_limit:
        try:
            parsed_date_limit = datetime.datetime.strptime(str(date_limit), "%Y-%m-%d")
        except ValueError:
            LOGGER.error("Invalid date format. Please use YYYY-MM-DD format.")
            return pd.DataFrame()

    if api_id is None:
        api_id_env = os.environ.get("TELEGRAM_API_ID")
        if not api_id_env:
            raise ValueError(
                "Telegram API ID not provided. Set it via the api_id parameter or TELEGRAM_API_ID environment variable."
            )
        api_id = int(api_id_env)

    if api_hash is None:
        api_hash = os.environ.get("TELEGRAM_API_HASH")
        if not api_hash:
            raise ValueError(
                "Telegram API hash not provided. Set it via the api_hash parameter or TELEGRAM_API_HASH environment variable."
            )

    coordinate_pattern = re.compile(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)")

    channel_list = list(_ensure_sequence(channel_links))

    message_ids: List[int] = []
    message_texts: List[str] = []
    media_types: List[str] = []
    dates: List[str] = []
    sources: List[str] = []
    latitudes: List[float] = []
    longitudes: List[float] = []

    database_instance: Optional[CoordinatesDatabase] = None
    if use_database:
        database_instance = database or CoordinatesDatabase(db_path or "telegram_coordinates.db")

    async def runner() -> None:
        async with TelegramClient(session_name, api_id, api_hash) as client:
            LOGGER.info("Connected to Telegram. Scraping %s channels...", len(channel_list))
            session_id: Optional[int] = None
            if database_instance:
                session_type = "single_channel" if len(channel_list) == 1 else "multi_channel"
                session_id = database_instance.start_session(session_type)
            total_skipped = total_new = total_coords = 0
            entity_cache = EntityCache(client, database_instance)
            try:
                for idx, channel in enumerate(channel_list, start=1):
                    LOGGER.info("[%s/%s] Scraping channel: %s", idx, len(channel_list), channel)
                    result = await scrape_channel(
                        client,
                        channel,
                        parsed_date_limit,
                        coordinate_pattern,
                        database=database_instance,
                        skip_existing=skip_existing,
                        recommendation_manager=recommendation_manager,
                        entity_cache=entity_cache,
                    )

                    stats = result[-1]
                    LOGGER.info(
                        "Channel %s processed=%s inserted=%s skipped=%s coordinates=%s",
                        stats.channel_id,
                        stats.messages_processed,
                        stats.messages_inserted,
                        stats.messages_skipped,
                        stats.coordinates_found,
                    )
                    total_skipped += stats.messages_skipped
                    total_new += stats.messages_inserted
                    total_coords += stats.coordinates_found

                    message_ids.extend(result[0])
                    message_texts.extend(result[1])
                    media_types.extend(result[2])
                    dates.extend(result[3])
                    sources.extend(result[4])
                    latitudes.extend(result[5])
                    longitudes.extend(result[6])
            finally:
                if database_instance and session_id:
                    database_instance.end_session(
                        session_id,
                        {
                            "channels_scraped": len(channel_list),
                            "new_messages": total_new,
                            "new_coordinates": total_coords,
                            "skipped_messages": total_skipped,
                            "status": "completed",
                        },
                    )

    asyncio.run(runner())

    df = pd.DataFrame(
        {
            "message_id": message_ids,
            "message_content": message_texts,
            "message_media_type": media_types,
            "message_published_at": dates,
            "message_source": sources,
            "latitude": latitudes,
            "longitude": longitudes,
        }
    )

    if not df.empty:
        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            df.to_csv(output_path, index=False)
            LOGGER.info("Successfully saved %s coordinates to %s", len(df), output_path)

        if kml_output_path:
            if save_dataframe_to_kml(df, kml_output_path):
                LOGGER.info("Successfully saved KML to %s", kml_output_path)

        if kmz_output_path:
            if save_dataframe_to_kmz(df, kmz_output_path):
                LOGGER.info("Successfully saved KMZ to %s", kmz_output_path)
        if not output_path and not (kml_output_path or kmz_output_path):
            LOGGER.info("Collected %s coordinates (no export paths provided)", len(df))

        if auto_visualize:
            import importlib.util

            if importlib.util.find_spec("keplergl") is None:
                LOGGER.info(
                    "Skipping auto-visualisation because the optional 'keplergl' dependency is not installed."
                )
            else:
                map_output = (
                    str(Path(output_path).with_suffix(".html"))
                    if output_path
                    else "results/auto_generated_map.html"
                )
                try:
                    from src.kepler_visualizer import create_map

                    create_map(df, map_output, visualization_type=visualization_type)
                    LOGGER.info("Interactive map generated at %s", map_output)
                except Exception as exc:  # pragma: no cover - best-effort visualisation
                    LOGGER.warning("Failed to create interactive map: %s", exc)
    else:
        LOGGER.info("No coordinates found.")

    return df
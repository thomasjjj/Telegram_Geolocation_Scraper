"""Async scraping helpers with optional SQLite integration."""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import re
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import pandas as pd
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.types import Channel, Chat, MessageMediaDocument, MessageMediaPhoto

from src.config import Config
from src.coordinates import extract_coordinates
from src.database import CoordinatesDatabase
from src.export import save_dataframe_to_kml, save_dataframe_to_kmz
from src.entity_cache import EntityCache
from src.rate_limiter import AdaptiveRateLimiter
from src.telethon_session import ensure_connected, TelegramSessionManager
from src.url_analysis import extract_links, serialize_links

if TYPE_CHECKING:  # pragma: no cover - optional dependency for type checking
    from src.recommendations import RecommendationManager


LOGGER = logging.getLogger(__name__)


_CONFIG = Config()

TELEGRAM_FETCH_BATCH_SIZE = _CONFIG.telegram_fetch_batch_size
MESSAGE_PROCESSING_BATCH_SIZE = _CONFIG.message_processing_batch_size
MESSAGE_BATCH_LOG_INTERVAL = max(1, int(os.getenv("MESSAGE_BATCH_LOG_INTERVAL", "1000")))
COORDINATE_BATCH_SIZE = 100


@dataclass
class ScrapeStats:
    """Aggregated information about a scraping run for a single channel."""

    channel_id: int
    messages_processed: int = 0
    messages_inserted: int = 0
    messages_skipped: int = 0
    coordinates_found: int = 0


class CoordinateResultCollector:
    """Utility to buffer coordinate records and optionally stream them to CSV."""

    COLUMNS = [
        "message_id",
        "message_content",
        "message_media_type",
        "message_published_at",
        "date",
        "message_source",
        "latitude",
        "longitude",
    ]

    def __init__(
        self,
        output_path: str | None = None,
        *,
        batch_size: int = 5000,
        collect_in_memory: bool = True,
    ) -> None:
        self.output_path = output_path
        self.batch_size = max(1, batch_size)
        self.collect_in_memory = collect_in_memory
        self._buffer: List[Dict[str, Any]] = []
        self._frames: List[pd.DataFrame] = []
        self._csv_header_written = False
        self.total_records = 0

    def add_record(self, record: Dict[str, Any]) -> None:
        """Append a coordinate record and flush when the batch is full."""

        self._buffer.append(record)
        self.total_records += 1
        if len(self._buffer) >= self.batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return

        df = pd.DataFrame(self._buffer, columns=self.COLUMNS)

        if self.output_path:
            os.makedirs(os.path.dirname(self.output_path) or ".", exist_ok=True)
            df.to_csv(
                self.output_path,
                mode="a" if self._csv_header_written else "w",
                header=not self._csv_header_written,
                index=False,
            )
            self._csv_header_written = True

        if self.collect_in_memory:
            self._frames.append(df)

        self._buffer.clear()

    def finalize(self) -> pd.DataFrame:
        """Flush remaining data and return the concatenated DataFrame."""

        self._flush()

        if self.collect_in_memory:
            if not self._frames:
                return pd.DataFrame(columns=self.COLUMNS)
            df = pd.concat(self._frames, ignore_index=True)
            self._frames.clear()
            return df

        return pd.DataFrame(columns=self.COLUMNS)

    @property
    def csv_written(self) -> bool:
        """Return whether any data has been written to the CSV output."""

        return self._csv_header_written


def _determine_channel_type(entity: Channel | Chat) -> str:
    if isinstance(entity, Channel):
        if getattr(entity, "megagroup", False):
            return "supergroup"
        return "channel"
    return "group"


async def fetch_messages_in_batches(
    client: TelegramClient,
    entity: Any,
    *,
    batch_size: int = TELEGRAM_FETCH_BATCH_SIZE,
    **iter_kwargs: Any,
) -> AsyncIterator[List[Any]]:
    """Yield Telegram messages in batches for efficient downstream processing."""

    batch: List[Any] = []

    await ensure_connected(client)

    async for message in client.iter_messages(entity, **iter_kwargs):
        if not message:
            continue

        batch.append(message)

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


async def _process_message_batch(
    messages: List[Any],
    channel_id: int,
    channel_display_name: str,
    entity: Any,
    client: TelegramClient | None,
    coordinate_pattern: re.Pattern,
    database: Optional[CoordinatesDatabase],
    skip_existing: bool,
    recommendation_manager: Optional["RecommendationManager"],
    result_collector: Optional[CoordinateResultCollector],
) -> Dict[str, int]:
    """Process a batch of messages using bulk database operations."""

    batch_stats = {"inserted": 0, "skipped": 0, "coordinates": 0}

    if not messages:
        return batch_stats

    message_ids = [msg.id for msg in messages if getattr(msg, "id", None) is not None]
    if not message_ids:
        return batch_stats

    existing_ids: set[int] = set()
    links_to_insert: Dict[int, List[Dict[str, Optional[str]]]] = {}

    if database:
        existing_ids = database.bulk_check_message_existence(channel_id, message_ids)
        if skip_existing:
            batch_stats["skipped"] = len(existing_ids)

    if skip_existing and existing_ids:
        messages_to_process = [msg for msg in messages if msg.id not in existing_ids]
    else:
        messages_to_process = list(messages)

    if not messages_to_process:
        return batch_stats

    messages_to_insert: List[Dict[str, Any]] = []
    coordinate_records: List[Dict[str, Any]] = []
    messages_with_coords: List[int] = []

    channel_username = getattr(entity, "username", None)

    for message in messages_to_process:
        if not getattr(message, "message", None):
            continue

        message_text = str(message.message)

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

        has_coordinates = bool(matches)
        if has_coordinates:
            messages_with_coords.append(message.id)

        message_record = {
            "message_id": message.id,
            "message_text": message_text,
            "message_date": message.date.isoformat() if message.date else None,
            "media_type": media_type,
            "has_coordinates": 1 if has_coordinates else 0,
        }

        messages_to_insert.append(message_record)

        extracted_links = serialize_links(extract_links(message_text or ""))
        if extracted_links:
            links_to_insert[message.id] = extracted_links

        if has_coordinates:
            message_date_str = message.date.strftime("%Y-%m-%d") if message.date else None
            if channel_username:
                source = f"t.me/{channel_username}/{message.id}"
            else:
                source = f"t.me/c/{channel_id}/{message.id}"

            for latitude, longitude in matches:
                lat_val = float(latitude)
                lon_val = float(longitude)

                coordinate_records.append(
                    {
                        "message_id": message.id,
                        "latitude": lat_val,
                        "longitude": lon_val,
                        "message_text": message_text,
                        "message_date": message_date_str,
                        "media_type": media_type,
                        "source": source,
                    }
                )
                batch_stats["coordinates"] += 1

    if database and messages_to_insert:
        id_map = database.bulk_insert_messages(channel_id, messages_to_insert)
        inserted_ids = [mid for mid in id_map if mid not in existing_ids]
        batch_stats["inserted"] = len(inserted_ids)

        if coordinate_records:
            coordinate_batch = [
                (id_map[record["message_id"]], record["latitude"], record["longitude"])
                for record in coordinate_records
                if record["message_id"] in id_map
            ]

            if coordinate_batch:
                database.bulk_add_coordinates(coordinate_batch)

        if links_to_insert:
            link_payload: Dict[int, List[Dict[str, Optional[str]]]] = {}
            for message_id, links in links_to_insert.items():
                row_id = id_map.get(message_id)
                if not row_id:
                    continue
                link_payload[row_id] = links

            if link_payload:
                database.add_message_links(link_payload)

    else:
        id_map: Dict[int, int] = {}

    if result_collector and coordinate_records:
        for record in coordinate_records:
            result_collector.add_record(
                {
                    "message_id": record["message_id"],
                    "message_content": record["message_text"],
                    "message_media_type": record["media_type"],
                    "message_published_at": record["message_date"] or "",
                    "date": record["message_date"] or "",
                    "message_source": record["source"],
                    "latitude": record["latitude"],
                    "longitude": record["longitude"],
                }
            )

            LOGGER.debug(
                "Retrieved coordinate (%s, %s) from message %s in channel %s",
                record["latitude"],
                record["longitude"],
                record["message_id"],
                channel_display_name,
            )

    new_recommendations: set[int] = set()

    if recommendation_manager:
        for message in messages_to_process:
            if not getattr(message, "forward", None):
                continue

            row_id = id_map.get(message.id)
            has_coordinates = message.id in messages_with_coords

            try:
                new_channel_id = recommendation_manager.process_forwarded_message(
                    message=message,
                    current_channel_id=channel_id,
                    has_coordinates=has_coordinates,
                    message_row_id=row_id,
                )
                if new_channel_id is not None:
                    new_recommendations.add(int(new_channel_id))
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                LOGGER.debug(
                    "Recommendation processing failed for message %s: %s",
                    message.id,
                    exc,
                )

    if (
        recommendation_manager
        and new_recommendations
        and getattr(recommendation_manager.settings, "auto_enrich", False)
    ):
        if client is None:
            LOGGER.debug(
                "Auto-enrichment requested for new recommendations but no Telegram client was provided."
            )
        else:
            for rec_channel_id in new_recommendations:
                try:
                    await recommendation_manager.enrich_recommendation(
                        client, rec_channel_id
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    LOGGER.debug(
                        "Auto-enrichment failed for channel %s: %s",
                        rec_channel_id,
                        exc,
                    )

    LOGGER.info(
        "Processed batch for channel %s: %s messages, %s inserted, %s skipped, %s coordinates",
        channel_display_name,
        len(messages_to_process),
        batch_stats["inserted"],
        batch_stats["skipped"],
        batch_stats["coordinates"],
    )

    return batch_stats


async def scrape_channel(
    client: TelegramClient,
    channel_id,
    date_limit: Optional[datetime.datetime],
    coordinate_pattern: Optional[re.Pattern] = None,
    database: Optional[CoordinatesDatabase] = None,
    skip_existing: bool = True,
    recommendation_manager: "RecommendationManager" | None = None,
    entity_cache: EntityCache | None = None,
    result_collector: CoordinateResultCollector | None = None,
) -> ScrapeStats:
    """Scrape a single channel for coordinates with optional database integration.

    When ``result_collector`` is provided, coordinate rows are streamed to it in
    batches to avoid building large in-memory lists.
    """

    if coordinate_pattern is None:
        coordinate_pattern = re.compile(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)")

    stats = ScrapeStats(channel_id=0)

    try:
        cache = entity_cache or EntityCache(
            client,
            database,
            rate_limiter=AdaptiveRateLimiter(
                base_delay=_CONFIG.rate_limit_base_delay,
                max_delay=_CONFIG.rate_limit_max_delay,
            ),
            max_age_hours=_CONFIG.entity_cache_max_age_hours,
        )
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

        accumulated_messages: List[Any] = []

        async for telegram_batch in fetch_messages_in_batches(
            client,
            entity,
            batch_size=TELEGRAM_FETCH_BATCH_SIZE,
            **iter_kwargs,
        ):
            stats.messages_processed += len(telegram_batch)

            valid_messages = [msg for msg in telegram_batch if getattr(msg, "message", None)]
            accumulated_messages.extend(valid_messages)

            if len(accumulated_messages) >= MESSAGE_PROCESSING_BATCH_SIZE:
                batch_result = await _process_message_batch(
                    accumulated_messages,
                    resolved_channel_id,
                    channel_display_name,
                    entity,
                    client,
                    coordinate_pattern,
                    database,
                    skip_existing,
                    recommendation_manager,
                    result_collector,
                )

                stats.messages_inserted += batch_result["inserted"]
                stats.messages_skipped += batch_result["skipped"]
                stats.coordinates_found += batch_result["coordinates"]
                accumulated_messages.clear()

                if stats.messages_processed % MESSAGE_BATCH_LOG_INTERVAL == 0:
                    LOGGER.info(
                        "Progress: %s messages processed, %s coordinates found",
                        stats.messages_processed,
                        stats.coordinates_found,
                    )

        if accumulated_messages:
            batch_result = await _process_message_batch(
                accumulated_messages,
                resolved_channel_id,
                channel_display_name,
                entity,
                client,
                coordinate_pattern,
                database,
                skip_existing,
                recommendation_manager,
                result_collector,
            )

            stats.messages_inserted += batch_result["inserted"]
            stats.messages_skipped += batch_result["skipped"]
            stats.coordinates_found += batch_result["coordinates"]

        if database:
            database.update_channel_statistics(resolved_channel_id)

    except (sqlite3.DatabaseError, RPCError, ValueError) as error:  # pragma: no cover - Telethon errors hard to simulate in tests
        LOGGER.error("Error scraping channel %s: %s", channel_id, error)

    return stats


def _ensure_sequence(value: Union[Sequence[str], str]) -> Sequence[str]:
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


def channel_scraper(
    channel_links: Union[Sequence[str], str],
    date_limit: Optional[str],
    output_path: Optional[str] = None,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
    session_name: str = "simple_scraper",
    session_manager: Optional[TelegramSessionManager] = None,
    kml_output_path: Optional[str] = None,
    kmz_output_path: Optional[str] = None,
    use_database: bool = True,
    skip_existing: bool = True,
    db_path: Optional[str] = None,
    database: Optional[CoordinatesDatabase] = None,
    recommendation_manager: Optional["RecommendationManager"] = None,
    auto_visualize: bool = False,
    visualization_type: str = "auto",
    batch_size: int = 5000,
    collect_results: bool = True,
    auto_harvest_recommendations: bool = False,
    harvest_after_scrape: bool = False,
) -> pd.DataFrame:
    """Scrape Telegram channels for coordinates and optionally export the results.

    When *output_path* is provided the collected coordinates are streamed to that
    CSV file in batches instead of being accumulated entirely in memory. Set
    *collect_results* to ``False`` to skip building the return DataFrame when the
    CSV or database outputs are sufficient.
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

    requires_dataframe = bool(kml_output_path or kmz_output_path or auto_visualize)
    if requires_dataframe and not collect_results:
        LOGGER.warning(
            "Data collection is required for KML/KMZ export or auto-visualisation; enabling in-memory collection."
        )
        collect_results = True

    result_collector = CoordinateResultCollector(
        output_path,
        batch_size=batch_size,
        collect_in_memory=collect_results,
    )

    database_instance: Optional[CoordinatesDatabase] = None
    if use_database:
        database_instance = database or CoordinatesDatabase(db_path or "telegram_coordinates.db")

    harvest_enabled = bool(auto_harvest_recommendations and recommendation_manager)

    def _run_auto_harvest(stage: str) -> None:
        if not harvest_enabled or not recommendation_manager:
            return

        settings = recommendation_manager.settings
        LOGGER.info("Auto-harvesting Telegram recommendations %s scrape...", stage)

        async def _harvest_runner(client: TelegramClient) -> Dict[str, Any]:
            return await recommendation_manager.harvest_telegram_recommendations(
                client,
                min_coordinate_density=settings.telegram_min_source_density,
                max_source_channels=settings.telegram_max_source_channels,
            )

        try:
            if session_manager:
                stats = session_manager.run(_harvest_runner)
            else:
                async def _runner() -> Dict[str, Any]:
                    async with TelegramClient(session_name, api_id, api_hash) as client:
                        return await _harvest_runner(client)

                stats = asyncio.run(_runner())
            LOGGER.info(
                "Telegram recommendation harvest (%s) complete: %s new, %s updated",
                stage,
                stats.get("new_recommendations", 0),
                stats.get("updated_recommendations", 0),
            )
        except (RPCError, ValueError, OSError) as exc:
            LOGGER.warning(
                "Telegram recommendation harvest (%s) failed: %s",
                stage,
                exc,
            )

    if harvest_enabled and not harvest_after_scrape:
        _run_auto_harvest("before")

    async def _scrape_channels(client: TelegramClient) -> None:
        LOGGER.info("Connected to Telegram. Scraping %s channels...", len(channel_list))
        session_id: Optional[int] = None
        if database_instance:
            session_type = "single_channel" if len(channel_list) == 1 else "multi_channel"
            session_id = database_instance.start_session(session_type)
        total_skipped = total_new = total_coords = 0
        limiter = AdaptiveRateLimiter(
            base_delay=_CONFIG.rate_limit_base_delay,
            max_delay=_CONFIG.rate_limit_max_delay,
        )
        entity_cache = EntityCache(
            client,
            database_instance,
            rate_limiter=limiter,
            max_age_hours=_CONFIG.entity_cache_max_age_hours,
        )
        progress_iterator = channel_list
        progress_bar = None
        if len(channel_list) > 1:
            try:
                from tqdm import tqdm

                progress_bar = tqdm(channel_list, desc="Scraping channels", unit="channel")
                progress_iterator = progress_bar
            except ImportError:
                LOGGER.debug("tqdm is not installed; progress bar disabled")
        try:
            for idx, channel in enumerate(progress_iterator, start=1):
                if progress_bar:
                    progress_bar.set_postfix_str(str(channel))
                LOGGER.info("[%s/%s] Scraping channel: %s", idx, len(channel_list), channel)
                stats = await scrape_channel(
                    client,
                    channel,
                    parsed_date_limit,
                    coordinate_pattern,
                    database=database_instance,
                    skip_existing=skip_existing,
                    recommendation_manager=recommendation_manager,
                    entity_cache=entity_cache,
                    result_collector=result_collector,
                )

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

        finally:
            if progress_bar:
                progress_bar.close()
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

    if session_manager:
        session_manager.run(_scrape_channels)
    else:
        async def runner() -> None:
            async with TelegramClient(session_name, api_id, api_hash) as client:
                await _scrape_channels(client)

        asyncio.run(runner())

    df = result_collector.finalize()

    if output_path and result_collector.csv_written:
        LOGGER.info(
            "Successfully saved %s coordinates to %s", result_collector.total_records, output_path
        )

    if not df.empty:
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
                except (OSError, ValueError, sqlite3.DatabaseError, pd.errors.ParserError) as exc:  # pragma: no cover - best-effort visualisation
                    LOGGER.warning("Failed to create interactive map: %s", exc)
    elif result_collector.total_records:
        LOGGER.info(
            "Collected %s coordinates (results were streamed directly to disk)",
            result_collector.total_records,
        )
    else:
        LOGGER.info("No coordinates found.")

    if harvest_enabled and harvest_after_scrape:
        _run_auto_harvest("after")

    return df
"""Interactive entry point for the Telegram coordinates scraper."""

from __future__ import annotations

import asyncio
import csv
import datetime
import getpass
import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from dotenv import load_dotenv, set_key
from colorama import Fore, Style, init as colorama_init
from pandas.errors import ParserError

from src.channel_scraper import channel_scraper
from src.database import CoordinatesDatabase
from src.db_migration import detect_and_migrate_all_results, migrate_existing_csv_to_database
from src.json_processor import process_telegram_json, save_dataframe_to_csv
from src.config import Config
from src.recommendations import RecommendationManager
from src.validators import (
    prompt_validated,
    validate_date,
    validate_non_empty,
    validate_positive_int,
)

try:
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError, RPCError
except ImportError as exc:  # pragma: no cover - missing dependency is fatal
    raise SystemExit("Telethon must be installed to run the scraper") from exc


# Keywords are normalised to lowercase to avoid capitalisation-specific duplicates.
DEFAULT_GEO_KEYWORDS = [
    "coordinate",
    "coordinates",
    "geo-location",
    "geoloc",
    "geolocate",
    "geolocated",
    "geolocation",
    "gps",
    "located",
    "location",
    "геолокированный",
    "геолокации",
    "геолокация",
    "геолокацией",
    "геолокацию",
    "геолокація",
    "геолокації",
    "геолокацію",
    "геолокований",
    "координатами",
    "координати",
    "координатах",
    "координаты",
    "местоположение",
    "местоположении",
    "местоположения",
    "місцезнаходження",
    "расположенный",
    "розташування",
    "розташований",
]


colorama_init(autoreset=True)


LOGGER = logging.getLogger(__name__)

DbConfig = Dict[str, Any]
RecommendationRecord = Dict[str, Any]
SearchResult = Dict[str, Any]

VISUALIZATION_ERRORS = (ValueError, OSError, sqlite3.DatabaseError, ParserError)


def _validate_percentage(value: str) -> bool:
    """Return ``True`` if *value* is a non-negative float."""

    try:
        return float(value) >= 0.0
    except ValueError:
        return False


MAIN_MENU = """
=== Telegram Coordinates Scraper ===

1. Quick Scrape (recommended)
   → Enter channel(s) and start immediately
   
2. Advanced Options
   → Database management, recommendations, JSON import
   
3. View Results & Statistics

4. Exit

Enter choice (1-4): """


ADVANCED_MENU = """
=== Advanced Options ===

1. Search all accessible chats
2. Process a JSON export file
3. Scan all known channels with coordinates
4. Manage database
5. Manage recommended channels
6. Visualise coordinates (Kepler.gl)
7. Back to main menu

Enter choice (1-7): """


def configure_logging() -> None:
    """Configure application-wide logging preferences."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    LOGGER.debug("Logging configured.")


def load_environment(env_path: Path) -> Config:
    """Load configuration from the ``.env`` file located at *env_path*."""

    if not env_path.exists():
        env_path.touch()
        LOGGER.info("Created environment file at %s", env_path)

    LOGGER.debug("Loading configuration from %s", env_path)
    return Config(env_path)


def ensure_api_credentials(env_path: Path, config: Config) -> Tuple[int, str]:
    """Ensure the Telegram API credentials are present and cached."""

    api_id_value = config.api_id
    if not api_id_value:
        api_id_str = prompt_validated(
            "Enter your Telegram API ID: ",
            validate_positive_int,
            error_msg="API ID must be a positive integer.",
        )
        api_id_value = int(api_id_str)
        set_key(str(env_path), "TELEGRAM_API_ID", str(api_id_value))
        print("Saved API ID to .env")
        LOGGER.info("Stored API ID in %s", env_path)
    os.environ["TELEGRAM_API_ID"] = str(api_id_value)

    api_hash_value = config.api_hash
    if not api_hash_value:
        api_hash_value = prompt_validated(
            "Enter your Telegram API hash: ",
            validate_non_empty,
            error_msg="API hash cannot be empty.",
        )
        set_key(str(env_path), "TELEGRAM_API_HASH", api_hash_value)
        print("Saved API hash to .env")
        LOGGER.info("Stored API hash in %s", env_path)
    os.environ["TELEGRAM_API_HASH"] = api_hash_value

    LOGGER.debug("API credentials ready for session initialisation.")
    return int(api_id_value), api_hash_value


def get_database_configuration(config: Config) -> DbConfig:
    """Return a mapping containing the runtime database configuration."""

    db_config: DbConfig = {
        "enabled": config.database_enabled,
        "path": config.database_path,
        "skip_existing": config.database_skip_existing,
    }
    LOGGER.debug("Database configuration resolved: %s", db_config)
    return db_config


def get_default_session_name(config: Optional[Config] = None) -> str:
    if config is None:
        config = Config()
    return config.session_name


def prompt_with_smart_default(
    prompt: str,
    default: str,
    validator: Optional[Callable[[str], bool]] = None,
) -> str:
    """Prompt the user highlighting an auto-detected default value.

    Parameters
    ----------
    prompt:
        The user-facing text to display.
    default:
        The fallback value that is highlighted and returned when the user
        provides an empty response or fails validation.
    validator:
        Optional callable that receives the raw user input. If it returns
        ``False`` the default value is used instead.
    """

    separator = " " if prompt.rstrip().endswith(":") else ": "
    default_hint = f" [{Fore.GREEN}{default}{Style.RESET_ALL}]" if default else ""
    response = input(f"{prompt}{default_hint}{separator}").strip()
    if not response:
        return default

    if validator and not validator(response):
        print(f"{Fore.RED}Invalid input. Using default.{Style.RESET_ALL}")
        return default

    return response


def prompt_session_name(
    prompt: str = "Enter Telegram session name to use",
    *,
    config: Optional[Config] = None,
    env_path: Optional[Path] = None,
) -> str:
    existing_session = os.environ.get("TELEGRAM_SESSION_NAME")
    if existing_session:
        os.environ["TELEGRAM_SESSION_NAME"] = existing_session
        if env_path is not None:
            set_key(str(env_path), "TELEGRAM_SESSION_NAME", existing_session)
        return existing_session

    default_session = get_default_session_name(config)
    session_name = prompt_with_smart_default(
        prompt,
        default_session,
        validator=validate_non_empty,
    )
    os.environ["TELEGRAM_SESSION_NAME"] = session_name
    if env_path is not None:
        set_key(str(env_path), "TELEGRAM_SESSION_NAME", session_name)
    return session_name


async def ensure_telegram_authentication(api_id: int, api_hash: str, session_name: str) -> None:
    """Ensure the Telegram session is authenticated before continuing."""

    print(f"\nConnecting to Telegram using session '{session_name}' to verify authentication...")
    LOGGER.info("Starting authentication check for session '%s'", session_name)

    phone_prompt = lambda: input("Enter your Telegram phone number (including country code): ").strip()
    password_prompt = lambda: getpass.getpass("Enter your Telegram 2FA password: ")

    client = TelegramClient(session_name, api_id, api_hash)
    try:
        await client.start(phone=phone_prompt, password=password_prompt)
        me = await client.get_me()
    except (RPCError, ValueError, OSError) as exc:  # pragma: no cover - Telethon runtime interaction
        LOGGER.error("Authentication failed for session '%s': %s", session_name, exc)
        raise SystemExit(f"Failed to authenticate with Telegram: {exc}") from exc
    else:
        if me:
            display_name_parts = [getattr(me, "first_name", None), getattr(me, "last_name", None)]
            display_name = " ".join(part for part in display_name_parts if part)
            identifier = getattr(me, "username", None) or display_name or str(getattr(me, "id", "unknown"))
            print(f"Authenticated as {identifier}.")
            LOGGER.info("Authenticated Telegram session as %s", identifier)
        else:
            print("Authentication successful.")
            LOGGER.info("Authentication succeeded without user details for session '%s'", session_name)
    finally:
        await client.disconnect()
        LOGGER.debug("Disconnected temporary authentication client for session '%s'", session_name)


def first_time_setup(config: Config) -> bool:
    session_name = config.session_name or os.getenv("TELEGRAM_SESSION_NAME", "scraper")
    session_file = Path(f"{session_name}.session")
    credentials_missing = not (config.api_id and config.api_hash)
    return credentials_missing or not session_file.exists()


def setup_wizard(env_path: Path, config: Config) -> tuple[int, str, str]:
    print("Step 1/3: Add your Telegram API credentials.")
    api_id, api_hash = ensure_api_credentials(env_path, config)

    refreshed_config = Config(env_path)

    print("\nStep 2/3: Choose a name for your session file.")
    session_name = prompt_session_name(
        "Enter a session name",
        config=refreshed_config,
        env_path=env_path,
    )

    print("\nStep 3/3: Sign in to Telegram so we can verify access.")
    asyncio.run(ensure_telegram_authentication(api_id, api_hash, session_name))

    print("\n🎉 Setup complete! You're ready to start scraping.\n")

    return api_id, api_hash, session_name


async def _search_dialogs_for_keywords(
    api_id: int,
    api_hash: str,
    session_name: str,
    keywords: Iterable[str],
    message_limit: Optional[int] = None,
    days_limit: Optional[int] = None,
    database: Optional[CoordinatesDatabase] = None,
    skip_recently_searched: bool = True,
    concurrent_searches: int = 5,
) -> List[SearchResult]:
    """Search Telegram chats for keywords using optimised strategies."""

    cutoff: Optional[datetime.datetime] = None
    if days_limit is not None:
        cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=days_limit)

    keyword_list = list(keywords)
    lowered_keywords = {kw.lower() for kw in keyword_list if kw}
    keyword_pattern = (
        re.compile("|".join(re.escape(kw) for kw in lowered_keywords), re.IGNORECASE)
        if lowered_keywords
        else None
    )

    LOGGER.info(
        "Starting optimised chat search: session=%s, keywords=%d, concurrent=%d",
        session_name,
        len(keyword_list),
        concurrent_searches,
    )

    results: List[SearchResult] = []
    dialogs_checked = 0
    dialogs_skipped = 0
    messages_scanned = 0
    start_time = datetime.datetime.now(datetime.UTC)

    print("\n" + "=" * 60)
    print("🔍 OPTIMIZED CHAT SEARCH")
    print("=" * 60)
    print("✨ Using Telegram's search API for faster results")
    print(f"⚡ Processing up to {concurrent_searches} chats concurrently")
    print(f"🔎 Searching for {len(keyword_list)} keywords")
    if cutoff:
        print(f"📅 Messages since: {cutoff.strftime('%Y-%m-%d')}")
    print("\n⚠️  Press Ctrl+C at any time to cancel\n")
    print("-" * 60)

    SKIP = object()

    async def search_single_chat(dialog, entity, chat_name: str):
        """Search a single chat for keyword matches."""

        nonlocal messages_scanned

        if skip_recently_searched and database:
            chat_id = getattr(entity, "id", None)
            if chat_id is not None and database.was_chat_recently_searched(chat_id, days=7):
                return SKIP

        if dialog.message:
            last_message = dialog.message
            if getattr(last_message, "id", 0) < 10:
                return SKIP

            last_msg_date = getattr(last_message, "date", None)
            if last_msg_date:
                days_old = (datetime.datetime.now(datetime.UTC) - last_msg_date).days
                if days_old > 365:
                    return SKIP

        match_keyword: Optional[str] = None
        excerpt: Optional[str] = None
        match_count = 0

        try:
            for keyword in keyword_list:
                search_params: Dict[str, Any] = {
                    "entity": entity,
                    "search": keyword,
                    "limit": 5,
                }
                if cutoff:
                    search_params["offset_date"] = cutoff

                try:
                    async for message in client.search_messages(**search_params):
                        match_count += 1
                        messages_scanned += 1

                        if not match_keyword:
                            message_text = message.text or ""
                            match_keyword = keyword
                            lowered_keyword = keyword.lower()
                            lowered_text = message_text.lower()
                            idx = lowered_text.find(lowered_keyword)
                            if idx >= 0:
                                start_idx = max(idx - 40, 0)
                                end_idx = min(idx + 80, len(message_text))
                                excerpt = message_text[start_idx:end_idx].replace("\n", " ")
                            elif keyword_pattern:
                                match = keyword_pattern.search(message_text)
                                if match:
                                    start_idx = max(match.start() - 40, 0)
                                    end_idx = min(match.end() + 80, len(message_text))
                                    excerpt = message_text[start_idx:end_idx].replace("\n", " ")
                            if excerpt is None:
                                excerpt = message_text[:120].replace("\n", " ")

                        if match_count >= 5:
                            break

                except FloodWaitError as error:
                    wait_time = min(error.seconds, 60)
                    print(f"\n⏳ Rate limited. Waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                except Exception as exc:  # pragma: no cover - defensive logging only
                    LOGGER.debug(
                        "Search failed for %s with keyword '%s': %s",
                        chat_name,
                        keyword,
                        exc,
                    )
                    continue

                if match_keyword:
                    break

            if match_keyword:
                if database:
                    chat_id = getattr(entity, "id", None)
                    if chat_id is not None:
                        database.record_chat_search(
                            chat_id=chat_id,
                            username=getattr(entity, "username", None),
                            chat_name=chat_name,
                            keywords_found=[match_keyword],
                            match_count=match_count,
                        )

                return {
                    "dialog": dialog,
                    "entity": entity,
                    "keyword": match_keyword,
                    "excerpt": excerpt,
                    "match_count": match_count,
                }

        except Exception as exc:  # pragma: no cover - defensive logging only
            LOGGER.error("Error searching chat %s: %s", chat_name, exc)
            return None

        return None

    try:
        async with TelegramClient(session_name, api_id, api_hash) as client:
            all_dialogs = []
            async for dialog in client.iter_dialogs():
                if dialog.is_user:
                    continue
                all_dialogs.append(dialog)

            total_dialogs = len(all_dialogs)
            update_interval = max(5, total_dialogs // 20) if total_dialogs else 5

            print(f"📊 Found {total_dialogs} chats to search\n")

            semaphore = asyncio.Semaphore(max(1, concurrent_searches))

            async def bounded_search(dialog):
                async with semaphore:
                    entity = dialog.entity
                    chat_name = (
                        dialog.name
                        or getattr(entity, "username", None)
                        or f"ID:{getattr(entity, 'id', 'unknown')}"
                    )
                    return await search_single_chat(dialog, entity, chat_name)

            batch_size = 20
            for start in range(0, total_dialogs, batch_size):
                batch = all_dialogs[start : start + batch_size]
                tasks = [bounded_search(dialog) for dialog in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in batch_results:
                    dialogs_checked += 1

                    if isinstance(result, Exception):
                        LOGGER.debug("Search task failed: %s", result)
                        continue

                    if result is SKIP:
                        dialogs_skipped += 1
                        continue

                    if result is None:
                        continue

                    results.append(result)
                    chat_name = (
                        result["dialog"].name
                        or getattr(result["entity"], "username", None)
                        or "Unknown"
                    )

                    print(
                        f"✅ Match #{len(results)}: {chat_name} "
                        f"(keyword: '{result['keyword']}', {result.get('match_count', 1)} occurrences)"
                    )

                if dialogs_checked % update_interval == 0 or dialogs_checked == total_dialogs:
                    elapsed = (
                        datetime.datetime.now(datetime.UTC) - start_time
                    ).total_seconds()
                    rate = dialogs_checked / elapsed if elapsed > 0 else 0
                    percent = (dialogs_checked / total_dialogs * 100) if total_dialogs else 0
                    print(
                        f"\r📊 Progress: {dialogs_checked}/{total_dialogs} chats ({percent:.1f}%) | "
                        f"Matches: {len(results)} | Rate: {rate:.1f} chats/sec",
                        end="",
                        flush=True,
                    )

                await asyncio.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n⚠️  Search cancelled by user")
        print(
            f"Partial results: Checked {dialogs_checked} chats, found {len(results)} matches"
        )
        LOGGER.info(
            "Chat search cancelled by user after checking %d chats", dialogs_checked
        )
        return results

    print()
    elapsed = (datetime.datetime.now(datetime.UTC) - start_time).total_seconds()
    print("-" * 60)
    print("\n✅ SEARCH COMPLETE")
    print(f"   • Total chats checked: {dialogs_checked}")
    print(f"   • Chats skipped (cached/filtered): {dialogs_skipped}")
    print(f"   • Messages searched: {messages_scanned}")
    print(f"   • Matches found: {len(results)}")
    print(f"   • Time elapsed: {elapsed:.1f} seconds")
    rate = dialogs_checked / elapsed if elapsed > 0 else 0
    print(f"   • Average rate: {rate:.1f} chats/sec")
    print("=" * 60 + "\n")

    LOGGER.info(
        "Optimised chat search completed: checked=%d, skipped=%d, matches=%d, time=%.1fs",
        dialogs_checked,
        dialogs_skipped,
        len(results),
        elapsed,
    )

    return results


def _parse_channel_list(raw_value: str) -> List[str]:
    return [channel.strip() for channel in raw_value.split(",") if channel.strip()]


def _suggest_channels(
    database: Optional[CoordinatesDatabase],
    recommendation_manager: Optional[RecommendationManager],
    limit: int = 3,
) -> Tuple[List[str], Optional[str]]:
    suggestions: List[str] = []
    suggestion_source: Optional[str] = None

    if recommendation_manager:
        for recommendation in recommendation_manager.get_top_recommendations(
            limit=limit,
            min_hit_rate=recommendation_manager.settings.min_hit_rate,
        ):
            identifier = recommendation.get("username") or recommendation.get("channel_id")
            if not identifier:
                continue
            identifier_str = str(identifier)
            if identifier_str not in suggestions:
                suggestions.append(identifier_str)
        if suggestions:
            suggestion_source = "pending recommendations"

    if database and len(suggestions) < limit:
        for channel in database.get_channels_with_coordinates(limit=limit):
            identifier = channel.get("username") or channel.get("id")
            if not identifier:
                continue
            identifier_str = str(identifier)
            if identifier_str not in suggestions:
                suggestions.append(identifier_str)
            if len(suggestions) >= limit:
                break
        if suggestions and suggestion_source is None:
            suggestion_source = "previously scraped channels"

    final_suggestions = suggestions[:limit]
    if final_suggestions:
        LOGGER.info(
            "Providing %d suggested channel(s) sourced from %s",
            len(final_suggestions),
            suggestion_source or "unknown sources",
        )
    return final_suggestions, suggestion_source


def prompt_channel_selection(
    database: Optional[CoordinatesDatabase],
    recommendation_manager: Optional[RecommendationManager],
) -> List[str]:
    prompt = "Enter Telegram channel usernames or IDs (comma separated)"
    suggestions, source = _suggest_channels(database, recommendation_manager)

    if suggestions and source:
        print(
            f"Suggested {source}: {', '.join(suggestions)}"
        )

    while True:
        if suggestions:
            channels_input = prompt_with_smart_default(
                prompt,
                ", ".join(suggestions),
                validator=lambda value: bool(_parse_channel_list(value)),
            )
        else:
            channels_input = input(f"{prompt}: ").strip()
            if not channels_input:
                print("At least one channel is required.")
                continue

        channels = _parse_channel_list(channels_input)
        if channels:
            return channels

        print("No valid channels selected. Please try again.")


def prompt_date_limit() -> Optional[str]:
    value = prompt_validated(
        "Enter the date limit (YYYY-MM-DD, leave blank for no limit): ",
        validate_date,
        error_msg="Invalid date format. Please use YYYY-MM-DD.",
        allow_empty=True,
    )
    return value or None


def _decode_sources(record: Dict[str, Any]) -> List[int]:
    value = record.get("discovered_from_channels")
    if not value:
        return []
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if isinstance(decoded, list):
        return [int(item) for item in decoded if isinstance(item, int)]
    return []


def _format_recommendation_line(index: int, recommendation: Dict[str, Any]) -> str:
    username = recommendation.get("username")
    title = recommendation.get("title")
    channel_id = recommendation["channel_id"]

    display_title = title or username or f"ID:{channel_id}"
    username_display = f"@{username}" if username else "Unavailable"
    id_display = f"ID:{channel_id}"
    score = recommendation.get("recommendation_score", 0.0)
    forward_count = int(recommendation.get("forward_count") or 0)
    coord_count = int(recommendation.get("coordinate_forward_count") or 0)
    sources = _decode_sources(recommendation)

    if forward_count > 0:
        hit_rate = (coord_count / forward_count) * 100
    else:
        hit_rate = 0.0

    if hit_rate >= 60.0:
        indicator = "🔥"
        quality_label = "EXCELLENT"
    elif hit_rate >= 40.0:
        indicator = "⭐"
        quality_label = "GOOD"
    elif hit_rate >= 20.0:
        indicator = "📌"
        quality_label = "MODERATE"
    elif hit_rate >= 5.0:
        indicator = "⚠️"
        quality_label = "LOW"
    else:
        indicator = "❌"
        quality_label = "POOR"

    line = [
        f"{index}. {indicator} {display_title}",
        f"   Username: {username_display}",
        f"   Channel ID: {id_display}",
        f"   Score: {score:.1f}/100 | Hit Rate: {hit_rate:.1f}% ({coord_count}/{forward_count}) [{quality_label}]",
    ]
    if sources:
        line.append(f"   Forwarded by {len(sources)} tracked channel(s)")
    if hit_rate < 5.0 and forward_count >= 10:
        line.append("   ⚠️  WARNING: Very low coordinate rate - may not be useful")
    return "\n".join(line)


def show_startup_recommendations(
    recommendation_manager: Optional[RecommendationManager],
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
) -> None:
    if not recommendation_manager or not recommendation_manager.settings.show_at_startup:
        return

    stats = recommendation_manager.get_recommendation_statistics()
    if stats.get("pending", 0) == 0:
        return

    print("\n" + "=" * 60)
    print("📢 RECOMMENDED CHANNELS DISCOVERED")
    print("=" * 60)
    print(
        f"Found {stats['pending']} channel(s) that frequently forward coordinates across "
        f"{stats['coordinate_forwards']} analysed forwards."
    )
    print()

    top_recommendations = recommendation_manager.get_top_recommendations(
        limit=recommendation_manager.settings.max_display,
        min_hit_rate=recommendation_manager.settings.min_hit_rate,
    )

    if not top_recommendations:
        print("No recommendations met the minimum score threshold.")
        return

    print("Top recommendations:\n")
    for idx, recommendation in enumerate(top_recommendations, start=1):
        print(_format_recommendation_line(idx, recommendation))
        print()

    print("Options:")
    print("  S - Scrape all recommended channels now")
    print("  T - Scrape the top recommendations")
    print("  V - Open recommendation management menu")
    print("  F - Filter by minimum hit rate")
    print("  L - Skip and continue to main menu")
    print()

    choice = prompt_validated(
        "Enter choice (S/T/V/F/L): ",
        lambda value: value.upper() in {"S", "T", "V", "F", "L"},
        error_msg="Please choose S, T, V, F, or L.",
        allow_empty=True,
        empty_value="L",
    ).upper()
    if choice == "S":
        scrape_recommended_channels_menu(
            recommendation_manager,
            database,
            db_config,
            api_id,
            api_hash,
            mode="all",
        )
    elif choice == "T":
        scrape_recommended_channels_menu(
            recommendation_manager,
            database,
            db_config,
            api_id,
            api_hash,
            mode="top",
        )
    elif choice == "V":
        handle_recommendation_management(
            recommendation_manager,
            database,
            db_config,
            api_id,
            api_hash,
        )
    elif choice == "F":
        def _valid_percentage(value: str) -> bool:
            try:
                number = float(value)
            except ValueError:
                return False
            return 0.0 <= number <= 100.0

        min_hit_rate = float(
            prompt_validated(
                "Minimum coordinate hit rate % (e.g., 20 for 20%): ",
                _valid_percentage,
                error_msg="Please enter a percentage between 0 and 100",
            )
        )

        filtered_recs = recommendation_manager.get_top_recommendations(
            limit=recommendation_manager.settings.max_display,
            min_hit_rate=min_hit_rate,
        )

        if not filtered_recs:
            print(f"\n❌ No recommendations found with hit rate >= {min_hit_rate}%")
            print("Try a lower threshold or wait for more data.")
        else:
            print(
                f"\n✅ Found {len(filtered_recs)} channel(s) with hit rate >= {min_hit_rate}%:\n"
            )
            for idx, rec in enumerate(filtered_recs, start=1):
                print(_format_recommendation_line(idx, rec))
                print()
    else:
        print("Continuing to main menu...\n")


def handle_specific_channel(
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
    config: Config,
    env_path: Path,
) -> None:
    session_name = prompt_session_name("Enter the session name", config=config, env_path=env_path)
    channels = prompt_channel_selection(database, recommendation_manager)
    date_limit = prompt_date_limit()

    channel_scraper(
        channel_links=channels,
        date_limit=date_limit,
        output_path=None,
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        use_database=db_config["enabled"] and database is not None,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
        recommendation_manager=recommendation_manager,
        auto_harvest_recommendations=(
            recommendation_manager.settings.telegram_auto_harvest
            if recommendation_manager
            else False
        ),
        harvest_after_scrape=(
            recommendation_manager.settings.telegram_harvest_after_scrape
            if recommendation_manager
            else False
        ),
    )


def handle_search_all_chats(
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
    config: Config,
    env_path: Path,
) -> None:
    session_name = prompt_session_name("Enter the session name", config=config, env_path=env_path)
    print("\n=== Search Configuration ===")
    print("You can limit the search to recent messages to speed things up.\n")

    days_input = prompt_validated(
        "Limit to messages from last N days (leave blank for all messages): ",
        lambda value: value.isdigit() and int(value) > 0,
        error_msg="Please enter a positive number of days",
        allow_empty=True,
    )
    days_limit = int(days_input) if days_input else None

    message_limit_input = prompt_validated(
        "Messages to check per chat (leave blank for all messages): ",
        lambda value: value.isdigit() and int(value) > 0,
        error_msg="Please enter a positive number",
        allow_empty=True,
    )
    message_limit = int(message_limit_input) if message_limit_input else None

    print("\n⚠️  Search Settings:")
    print(f"   • Session: {session_name}")
    print(
        "   • Messages per chat: "
        + (str(message_limit) if message_limit is not None else "All messages")
    )
    print(
        f"   • Time limit: {'Last ' + str(days_limit) + ' days' if days_limit else 'All time'}"
    )
    print(f"   • Keywords: {len(DEFAULT_GEO_KEYWORDS)} geolocation terms")
    print()

    confirm = input("Start search? (y/N): ").strip().lower()
    if confirm != "y":
        print("Search cancelled.")
        return

    LOGGER.info(
        "Searching all chats for geolocation keywords (limit=%s, days_limit=%s)",
        message_limit if message_limit is not None else "all",
        days_limit,
    )
    results = asyncio.run(
        _search_dialogs_for_keywords(
            api_id=api_id,
            api_hash=api_hash,
            session_name=session_name,
            keywords=DEFAULT_GEO_KEYWORDS,
            message_limit=message_limit,
            days_limit=days_limit,
            database=database,
            skip_recently_searched=True,
            concurrent_searches=5,
        )
    )
    if not results:
        print("❌ No chats containing geolocation keywords were found.")
        print("\nTips:")
        print("  • Try increasing the messages per chat limit")
        print("  • Remove the time limit to search older messages")
        print("  • Check that your keywords match the language used in chats")
        return

    print("The following chats mention geolocation keywords:\n")
    for idx, result in enumerate(results, start=1):
        entity = result["entity"]
        username = getattr(entity, "username", None)
        dialog_name = result["dialog"].name or username or str(entity.id)
        identifier = f"@{username}" if username else f"ID {entity.id}"
        keyword = result["keyword"]
        excerpt = result.get("excerpt", "")

        print(f"  [{idx}] {dialog_name} ({identifier})")
        print(f"      Keyword: '{keyword}'")
        if excerpt:
            preview = excerpt[:80]
            suffix = "..." if len(excerpt) > 80 else ""
            print(f"      Preview: {preview}{suffix}")
        print()

    selection = input("Enter numbers to scan (comma separated) or press Enter to scan all: ").strip()
    if selection:
        indices: List[int] = []
        for item in selection.split(","):
            item = item.strip()
            if item.isdigit():
                indices.append(int(item))
        chosen = [results[i - 1] for i in indices if 1 <= i <= len(results)]
    else:
        chosen = results

    channels: List[str] = []
    for result in chosen:
        entity = result["entity"]
        username = getattr(entity, "username", None)
        channels.append(username or str(entity.id))

    if not channels:
        print("No valid channels selected.")
        LOGGER.warning("No channels selected after chat search")
        return

    channel_scraper(
        channel_links=channels,
        date_limit=None,
        output_path=None,
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        use_database=db_config["enabled"] and database is not None,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
        recommendation_manager=recommendation_manager,
        auto_harvest_recommendations=(
            recommendation_manager.settings.telegram_auto_harvest
            if recommendation_manager
            else False
        ),
        harvest_after_scrape=(
            recommendation_manager.settings.telegram_harvest_after_scrape
            if recommendation_manager
            else False
        ),
    )


def scrape_recommended_channels_menu(
    recommendation_manager: Optional[RecommendationManager],
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
    mode: str = "interactive",
) -> None:
    if not recommendation_manager:
        print("Recommendation system is disabled.")
        return

    if mode == "all":
        recommendations = recommendation_manager.list_recommendations(status="pending")
    elif mode == "top":
        recommendations = recommendation_manager.get_top_recommendations(
            limit=recommendation_manager.settings.max_display,
            min_hit_rate=recommendation_manager.settings.min_hit_rate,
        )
    else:
        print("\nScrape Recommended Channels")
        print("-" * 40)
        print("1. Scrape all pending recommendations")
        print("2. Scrape top N recommendations")
        print("3. Scrape specific recommendations")
        print("4. Back")
        print()

        choice = prompt_validated(
            "Enter choice (1-4): ",
            lambda value: value in {"1", "2", "3", "4"},
            error_msg="Please select an option between 1 and 4.",
        )
        if choice == "1":
            recommendations = recommendation_manager.list_recommendations(status="pending")
        elif choice == "2":
            limit = int(
                prompt_validated(
                    "How many top recommendations to scrape? ",
                    validate_positive_int,
                    error_msg="Please enter a positive integer.",
                )
            )
            recommendations = recommendation_manager.get_top_recommendations(
                limit=limit,
                min_hit_rate=recommendation_manager.settings.min_hit_rate,
            )
        elif choice == "3":
            view_all_recommendations(recommendation_manager)
            indices_input = input("Enter recommendation numbers to scrape (comma-separated): ").strip()
            indices: List[int] = []
            for item in indices_input.split(","):
                item = item.strip()
                if item.isdigit():
                    indices.append(int(item))
            pending = recommendation_manager.list_recommendations(status="pending")
            recommendations = [pending[i - 1] for i in indices if 1 <= i <= len(pending)]
        else:
            return

    if not recommendations:
        print("No recommendations selected for scraping.")
        return

    _run_recommended_scrape(
        recommendation_manager,
        database,
        db_config,
        api_id,
        api_hash,
        recommendations,
    )


def _run_recommended_scrape(
        recommendation_manager: RecommendationManager,
        database: Optional[CoordinatesDatabase],
        db_config: DbConfig,
        api_id: int,
        api_hash: str,
        recommendations: List[RecommendationRecord],
) -> None:
    # First, try to enrich any channels without usernames
    needs_enrichment = [r for r in recommendations if not r.get("username")]
    if needs_enrichment:
        print(f"\n{len(needs_enrichment)} channel(s) need enrichment to fetch usernames.")
        enrich = input("Enrich them now? (y/N): ").strip().lower()
        if enrich == "y":
            async def enrich_batch():
                async with TelegramClient("recommended_scrape", api_id, api_hash) as client:
                    for rec in needs_enrichment:
                        await recommendation_manager.enrich_recommendation(client, rec["channel_id"])

            try:
                asyncio.run(enrich_batch())
                # Reload recommendations to get updated data
                recommendations = [
                    recommendation_manager.get_recommended_channel(r["channel_id"])
                    for r in recommendations
                ]
                recommendations = [r for r in recommendations if r is not None]
            except (RPCError, ValueError, OSError) as exc:
                print(f"Enrichment failed: {exc}")
                print("Continuing with available data...")

    identifiers: List[Any] = []
    for recommendation in recommendations:
        username = recommendation.get("username")
        channel_id = recommendation["channel_id"]
        peer_type = recommendation.get("peer_type") or recommendation.get("entity_type")
        peer_type_normalised = str(peer_type).lower() if peer_type else None
        if peer_type_normalised in {"supergroup", "megagroup", "group"}:
            peer_type_normalised = "chat"
        elif peer_type_normalised not in {"channel", "chat", "user"}:
            peer_type_normalised = "channel"

        identifier_payload: Dict[str, Any] = {
            "id": channel_id,
            "peer_type": peer_type_normalised or "channel",
        }
        if username:
            identifier_payload["username"] = username

        identifiers.append(identifier_payload)

    print(f"Preparing to scrape {len(identifiers)} channel(s).")
    LOGGER.info("Scraping %d recommended channels", len(identifiers))
    confirm = input("Continue? (y/N): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        LOGGER.debug("User cancelled recommended channel scrape")
        return

    date_limit_value = prompt_validated(
        "Enter date limit (YYYY-MM-DD, or press Enter for no limit): ",
        validate_date,
        error_msg="Invalid date format. Please use YYYY-MM-DD.",
        allow_empty=True,
    )
    date_limit = date_limit_value or None

    channel_scraper(
        channel_links=identifiers,
        date_limit=date_limit,
        output_path=None,
        api_id=api_id,
        api_hash=api_hash,
        session_name="recommended_scrape",
        use_database=db_config.get("enabled", True) and database is not None,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
        recommendation_manager=recommendation_manager,
        auto_harvest_recommendations=(
            recommendation_manager.settings.telegram_auto_harvest
            if recommendation_manager
            else False
        ),
        harvest_after_scrape=(
            recommendation_manager.settings.telegram_harvest_after_scrape
            if recommendation_manager
            else False
        ),
    )

    for recommendation in recommendations:
        recommendation_manager.mark_recommendation_status(
            recommendation["channel_id"],
            "scraped",
            notes="Scraped without on-demand CSV export",
        )

    print("\n✅ Scraping complete! Review new data through the database management menu.")


def harvest_telegram_recommendations_cli(
    recommendation_manager: RecommendationManager,
    api_id: int,
    api_hash: str,
) -> None:
    """Interactive handler for Telegram's native channel recommendations."""

    if not recommendation_manager or not recommendation_manager.db:
        print("Recommendation system is unavailable.")
        return

    settings = recommendation_manager.settings

    print("\n" + "=" * 60)
    print("HARVEST TELEGRAM CHANNEL RECOMMENDATIONS")
    print("=" * 60)
    print(
        "\nThis queries Telegram for channels similar to your best coordinate sources."
    )

    default_density = settings.telegram_min_source_density
    min_density_input = prompt_validated(
        f"Minimum coordinate density for source channels [default: {default_density:.1f}%]: ",
        _validate_percentage,
        error_msg="Please enter a valid percentage",
        allow_empty=True,
        empty_value=f"{default_density:.1f}",
    )
    min_density = float(min_density_input)

    default_limit = settings.telegram_max_source_channels
    limit_prompt = "Maximum source channels to query [default: all]: "
    if default_limit:
        limit_prompt = (
            f"Maximum source channels to query [default: {default_limit}]: "
        )

    max_channels_input = prompt_validated(
        limit_prompt,
        validate_positive_int,
        error_msg="Please enter a positive integer",
        allow_empty=True,
        empty_value=str(default_limit) if default_limit else "",
    )
    max_channels = (
        int(max_channels_input)
        if max_channels_input
        else default_limit if default_limit else None
    )

    sources = recommendation_manager.db.get_channels_with_coordinates(
        min_density=min_density,
        limit=max_channels,
    )

    if not sources:
        print(
            f"\n❌ No channels found with coordinate density >= {min_density:.1f}%"
        )
        print("   Try lowering the minimum density threshold.")
        return

    print(f"\n📊 Will query {len(sources)} source channel(s):")
    preview = sources[:5]
    for idx, source in enumerate(preview, 1):
        name = source.get("title") or source.get("username") or f"ID:{source['id']}"
        density = float(source.get("coordinate_density") or 0.0)
        print(f"   {idx}. {name} ({density:.1f}% coordinate density)")
    if len(sources) > len(preview):
        print(f"   ... and {len(sources) - len(preview)} more")

    estimate_low = len(sources) * 2
    estimate_high = len(sources) * 5
    print(
        f"\n⏱️  Estimated time: {estimate_low} - {estimate_high} seconds"
    )
    print("   (Telegram rate limits may add extra delay)")

    confirm = input("\nProceed with harvest? (y/N): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    session_name = os.environ.get("TELEGRAM_SESSION_NAME", "recommendation_harvest")

    async def run_harvest() -> Dict[str, Any]:
        async with TelegramClient(session_name, api_id, api_hash) as client:
            return await recommendation_manager.harvest_telegram_recommendations(
                client,
                min_coordinate_density=min_density,
                max_source_channels=max_channels,
            )

    try:
        stats = asyncio.run(run_harvest())
    except (RPCError, ValueError, OSError) as exc:
        LOGGER.error("Failed to harvest Telegram recommendations: %s", exc)
        print(f"\n❌ Harvest failed: {exc}")
        return

    if stats.get("new_recommendations", 0):
        print("\n✨ Success! New recommendations are ready to review.")
        print("   Select option 1 or 2 to view them.")
    else:
        print("\n📝 No new recommendations found.")
        print(
            "   All suggested channels are either already tracked or previously discovered."
        )


def display_recommendation_menu() -> None:
    """Print the recommendation management menu to stdout."""

    print("Options:")
    print("  1. View all pending recommendations")
    print("  2. View top recommendations")
    print("  3. Search recommendations")
    print("  4. Scrape recommended channels")
    print("  5. Accept/Reject recommendations")
    print("  6. Enrich recommendations (fetch channel details)")
    print("  7. Export recommendations to CSV")
    print("  8. View forward analysis")
    print("  9. Clean up invalid recommendations")
    print(" 10. Harvest Telegram API recommendations")
    print(" 11. Recalculate recommendation scores")
    print(" 12. Back to main menu")
    print()


def get_recommendation_choice() -> str:
    """Return a validated menu selection for recommendation management."""

    return prompt_validated(
        "Enter choice (1-12): ",
        lambda value: value in {str(i) for i in range(1, 13)},
        error_msg="Please select an option between 1 and 12.",
    )


def _display_recommendation_overview(recommendation_manager: RecommendationManager) -> None:
    """Print high level statistics about tracked recommendations."""

    print("\n" + "=" * 60)
    print("RECOMMENDED CHANNELS MANAGEMENT")
    print("=" * 60)

    stats = recommendation_manager.get_recommendation_statistics()
    print(f"Total Recommended: {stats['total_recommended']}")
    print(f"  Pending: {stats['pending']}")
    print(f"  Accepted: {stats['accepted']}")
    print(f"  Rejected: {stats['rejected']}")
    print(f"  Inaccessible: {stats['inaccessible']}")
    print()


def execute_recommendation_action(
    choice: str,
    recommendation_manager: RecommendationManager,
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
) -> bool:
    """Execute the menu action mapped to *choice*.

    Returns ``False`` when the caller should exit the menu.
    """

    if choice == "12":
        LOGGER.debug("Exiting recommendation management menu")
        return False

    handlers: Dict[str, Callable[[], None]] = {
        "1": lambda: view_all_recommendations(recommendation_manager),
        "2": lambda: view_top_recommendations(recommendation_manager),
        "3": lambda: search_recommendations_cli(recommendation_manager),
        "4": lambda: scrape_recommended_channels_menu(
            recommendation_manager,
            database,
            db_config,
            api_id,
            api_hash,
        ),
        "5": lambda: accept_reject_recommendations(recommendation_manager),
        "6": lambda: enrich_recommendations_cli(recommendation_manager, api_id, api_hash),
        "7": lambda: export_recommendations_cli(recommendation_manager),
        "8": lambda: view_forward_analysis(recommendation_manager),
        "9": lambda: cleanup_invalid_recommendations_cli(database),
        "10": lambda: harvest_telegram_recommendations_cli(
            recommendation_manager,
            api_id,
            api_hash,
        ),
        "11": lambda: recalculate_recommendation_scores_cli(recommendation_manager),
    }

    handler = handlers.get(choice)
    if handler:
        LOGGER.debug("Executing recommendation menu option %s", choice)
        handler()
    else:
        print("Invalid choice. Please try again.")
    return True


def handle_recommendation_management(
    recommendation_manager: Optional[RecommendationManager],
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
) -> None:
    if not recommendation_manager or not recommendation_manager.settings.enabled:
        print("Recommendation system is disabled.")
        return

    while True:
        _display_recommendation_overview(recommendation_manager)
        display_recommendation_menu()
        choice = get_recommendation_choice()
        if not execute_recommendation_action(
            choice,
            recommendation_manager,
            database,
            db_config,
            api_id,
            api_hash,
        ):
            break


def view_all_recommendations(recommendation_manager: RecommendationManager) -> None:
    recommendations = recommendation_manager.list_recommendations(status="pending", limit=100)
    if not recommendations:
        print("\nNo pending recommendations found.")
        return

    print("\n#   Score   Username/ID                 Forwards   Coords   Sources")
    print("-" * 80)
    for idx, recommendation in enumerate(recommendations, start=1):
        username = recommendation.get("username")
        username_display = f"@{username}" if username else f"ID:{recommendation['channel_id']}"
        score = recommendation.get("recommendation_score", 0.0)
        forward_count = int(recommendation.get("forward_count") or 0)
        coord_count = int(recommendation.get("coordinate_forward_count") or 0)
        sources = len(_decode_sources(recommendation))
        print(
            f"{idx:<3} {score:>6.1f}  {username_display:<25} {forward_count:<9} {coord_count:<7} {sources:<7}"
        )


def view_top_recommendations(recommendation_manager: RecommendationManager, limit: int = 10) -> None:
    recommendations = recommendation_manager.get_top_recommendations(
        limit=limit,
        min_hit_rate=recommendation_manager.settings.min_hit_rate,
    )
    if not recommendations:
        print("No recommendations meet the minimum score threshold.")
        return

    print()
    for idx, recommendation in enumerate(recommendations, start=1):
        print(_format_recommendation_line(idx, recommendation))
        print()


def search_recommendations_cli(recommendation_manager: RecommendationManager) -> None:
    term = prompt_validated(
        "Enter search term (username, title, or ID): ",
        validate_non_empty,
        error_msg="Search term is required.",
    )
    results = recommendation_manager.search_recommendations(term)
    if not results:
        print("No recommendations matched your search.")
        return
    print()
    for idx, recommendation in enumerate(results, start=1):
        print(_format_recommendation_line(idx, recommendation))
        print()


def accept_reject_recommendations(recommendation_manager: RecommendationManager) -> None:
    recommendations = recommendation_manager.get_top_recommendations(
        limit=20,
        min_score=0.0,
        min_hit_rate=0.0,
    )
    if not recommendations:
        print("No pending recommendations available for review.")
        return

    for idx, recommendation in enumerate(recommendations, start=1):
        print("\n" + "=" * 60)
        print(f"Recommendation {idx}/{len(recommendations)}")
        print("=" * 60)
        username = recommendation.get("username")
        title = recommendation.get("title") or username or f"ID:{recommendation['channel_id']}"
        print(f"Channel: {title}")
        print(f"Identifier: @{username}" if username else f"ID: {recommendation['channel_id']}")
        print(f"Score: {recommendation.get('recommendation_score', 0.0):.1f}/100")
        print(
            f"Forwards seen: {recommendation.get('forward_count', 0)} "
            f"({recommendation.get('coordinate_forward_count', 0)} with coordinates)"
        )
        print(f"First seen: {recommendation.get('first_seen', 'Unknown')}")
        print(f"Last seen: {recommendation.get('last_seen', 'Unknown')}")
        sources = _decode_sources(recommendation)
        if sources:
            print(f"Forwarded by {len(sources)} tracked channel(s)")

        print()
        print("Options: A - Accept | R - Reject | S - Skip | Q - Quit")
        decision = prompt_validated(
            "Enter choice (A/R/S/Q): ",
            lambda value: value.upper() in {"A", "R", "S", "Q"},
            error_msg="Please choose A, R, S, or Q.",
            allow_empty=True,
            empty_value="S",
        ).upper()

        if decision == "A":
            notes = input("Add notes (optional): ").strip() or None
            recommendation_manager.mark_recommendation_status(
                recommendation["channel_id"],
                "accepted",
                notes,
            )
            print("✅ Accepted!")
        elif decision == "R":
            reason = input("Reason for rejection (optional): ").strip() or None
            recommendation_manager.mark_recommendation_status(
                recommendation["channel_id"],
                "rejected",
                reason,
            )
            print("❌ Rejected.")
        elif decision == "Q":
            break
        else:
            print("⏭️  Skipped.")


def enrich_recommendations_cli(
    recommendation_manager: RecommendationManager,
    api_id: int,
    api_hash: str,
) -> None:
    recommendations = recommendation_manager.list_recommendations(limit=100, order_by="last_seen DESC")
    if not recommendations:
        print("No recommendations available for enrichment.")
        return

    confirm = input(f"Fetch details for {len(recommendations)} recommendation(s)? (y/N): ").strip().lower()
    if confirm != "y":
        return

    session_name = os.environ.get("TELEGRAM_SESSION_NAME", "recommendation_enrichment")

    async def enrich_all() -> None:
        async with TelegramClient(session_name, api_id, api_hash) as client:
            enriched = failed = 0
            for recommendation in recommendations:
                success = await recommendation_manager.enrich_recommendation(
                    client,
                    recommendation["channel_id"],
                )
                if success:
                    enriched += 1
                else:
                    failed += 1
            print(f"\nEnrichment complete. Success: {enriched}, Failed: {failed}")

    try:
        asyncio.run(enrich_all())
    except (RPCError, ValueError, OSError) as exc:  # pragma: no cover - Telethon runtime errors
        LOGGER.error("Failed to enrich recommendations: %s", exc)


def export_recommendations_cli(recommendation_manager: RecommendationManager) -> None:
    status = prompt_validated(
        "Export which status? (pending/accepted/rejected/all) [pending]: ",
        lambda value: value.lower() in {"pending", "accepted", "rejected", "all"},
        error_msg="Please choose pending, accepted, rejected, or all.",
        allow_empty=True,
        empty_value="pending",
    ).lower()
    if status == "all":
        records = recommendation_manager.export_recommendations(status=None)
    else:
        records = recommendation_manager.export_recommendations(status=status)

    if not records:
        print("No recommendations found for export.")
        return

    output_path = input("Enter CSV export path (default results/recommendations.csv): ").strip() or "results/recommendations.csv"
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    fieldnames = sorted({key for record in records for key in record.keys()})
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    print(f"Exported {len(records)} recommendation(s) to {output_path}")


def view_forward_analysis(recommendation_manager: RecommendationManager) -> None:
    rows = recommendation_manager.db.query(
        """
        SELECT rc.channel_id,
               rc.username,
               rc.title,
               rc.recommendation_score,
               COUNT(cf.id) AS forward_count,
               SUM(CASE WHEN cf.had_coordinates = 1 THEN 1 ELSE 0 END) AS coord_count,
               COUNT(DISTINCT cf.to_channel_id) AS source_diversity
        FROM recommended_channels rc
        JOIN channel_forwards cf ON cf.from_channel_id = rc.channel_id
        GROUP BY rc.channel_id
        ORDER BY coord_count DESC, forward_count DESC
        LIMIT 20
        """
    )

    if not rows:
        print("No forward analysis data available yet.")
        return

    print("\nRecommended Channel                 Score   Forwards  w/Coords  Sources")
    print("-" * 90)
    for row in rows:
        username = row["username"]
        title = row["title"] or username or f"ID:{row['channel_id']}"
        print(
            f"{title:<35} {row['recommendation_score']:<7.1f} {row['forward_count']:<9} "
            f"{row['coord_count']:<9} {row['source_diversity']:<7}"
        )


def cleanup_invalid_recommendations_cli(
    database: Optional[CoordinatesDatabase],
) -> None:
    if database is None:
        print("Database connection is not available.")
        return

    print("\n" + "=" * 60)
    print("CLEAN UP INVALID RECOMMENDATIONS")
    print("=" * 60)
    print("This will remove entries that are likely user IDs or otherwise invalid.\n")

    preview_rows = database.query(
        """
        SELECT channel_id, username, title, user_status, entity_type
        FROM recommended_channels
        WHERE channel_id < 1000000000
           OR user_status = 'invalid_entity_type'
           OR entity_type = 'user'
        ORDER BY channel_id
        LIMIT 10
        """
    )
    preview = [dict(row) for row in preview_rows]

    if not preview:
        print("✅ No invalid recommendations found.")
        return

    print(f"Found {len(preview_rows)} example entries (showing up to 10):")
    for record in preview:
        label = record.get("username") or record.get("title") or "<unknown>"
        status = record.get("user_status") or "pending"
        entity_type = record.get("entity_type") or "unknown"
        print(
            f"  • ID {record['channel_id']}: {label} | status={status} | entity={entity_type}"
        )

    confirm = input("Proceed with cleanup? (y/N): ").strip().lower()
    if confirm != "y":
        print("Cleanup cancelled.")
        return

    try:
        stats = database.cleanup_invalid_recommendations()
    except sqlite3.DatabaseError as exc:
        LOGGER.error("Failed to clean up recommendations: %s", exc)
        print("Cleanup failed due to a database error.")
        return

    print("\nCleanup complete:")
    print(f"  • Total before: {stats['total_before']}")
    print(f"  • Removed (low ID): {stats['removed_by_heuristic']}")
    print(f"  • Removed (invalid status): {stats['removed_by_status']}")
    print(f"  • Removed (entity type): {stats['removed_by_type']}")
    print(f"  • Total removed: {stats['total_removed']}")
    print(f"  • Total after: {stats['total_after']}")


def recalculate_recommendation_scores_cli(
    recommendation_manager: RecommendationManager,
) -> None:
    print("\n🔄 Recalculating recommendation scores with the latest algorithm...\n")
    updated = recommendation_manager.recalculate_all_scores()

    if updated:
        print(f"✅ Updated {updated} recommendation(s). Scores are now refreshed.")
    else:
        print("ℹ️ All recommendation scores were already up to date.")

def handle_process_json(database: Optional[CoordinatesDatabase]) -> None:
    json_file = prompt_validated(
        "Enter the path to the Telegram JSON export: ",
        validate_non_empty,
        error_msg="JSON file path is required.",
    )

    post_link_base = prompt_validated(
        "Enter the base URL for post links (e.g. https://t.me/channel/): ",
        validate_non_empty,
        error_msg="A base URL is required.",
    )
    if not post_link_base.endswith("/"):
        post_link_base += "/"

    LOGGER.info("Processing Telegram JSON export located at %s", json_file)
    df = process_telegram_json(json_file, post_link_base)
    if df.empty:
        print("No coordinates were found in the JSON file.")
        return

    csv_path = input("Enter the output CSV path: ").strip() or "results/json_import.csv"
    if save_dataframe_to_csv(df, csv_path):
        print(f"Saved {len(df)} rows to {csv_path}")

    if database:
        migrate = input("Import these results into the database? (y/N): ").strip().lower() == "y"
        if migrate:
            imported = migrate_existing_csv_to_database(csv_path, database)
            database.vacuum_database()
            print(f"Imported {imported} coordinate rows into the database.")
            LOGGER.info("Imported %d coordinate rows from JSON export", imported)


def handle_scan_known_channels(
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
) -> None:
    if not database:
        print("Database support is disabled.")
        return

    min_density_input = prompt_validated(
        "Minimum coordinate density percentage to include (default 0): ",
        _validate_percentage,
        error_msg="Please enter a non-negative number.",
        allow_empty=True,
    )
    min_density = float(min_density_input) if min_density_input else 0.0

    channels = database.get_channels_with_coordinates(min_density=min_density)
    if not channels:
        print("No channels with stored coordinates matched the criteria.")
        return

    print("\nFound channels with coordinate history:\n")
    for idx, channel in enumerate(channels, start=1):
        username = channel.get("username")
        title = channel.get("title") or username or channel["id"]
        density = channel.get("coordinate_density", 0.0)
        last_scraped = channel.get("last_scraped", "N/A")
        coords = channel.get("messages_with_coordinates", 0)
        print(f"{idx:>2}. {title} ({username or channel['id']}) - density {density:.2f}% - coords {coords} - last {last_scraped}")

    print("\nOptions:\nA - Scan all channels\nS - Select specific channels (comma-separated)\nC - Cancel")
    choice = prompt_validated(
        "Enter choice: ",
        lambda value: value.upper() in {"A", "S", "C"},
        error_msg="Please choose A, S, or C.",
        allow_empty=True,
        empty_value="A",
    ).upper()

    if choice == "C":
        return

    if choice == "S":
        selection = input("Enter channel numbers: ").strip()
        indices = []
        for item in selection.split(","):
            item = item.strip()
            if item.isdigit():
                indices.append(int(item))
        selected = [channels[i - 1] for i in indices if 1 <= i <= len(channels)]
    else:
        selected = channels

    if not selected:
        print("No channels selected.")
        return

    identifiers = [channel.get("username") or channel["id"] for channel in selected]
    LOGGER.info("Scanning %d known channel(s) for coordinates", len(identifiers))

    channel_scraper(
        channel_links=identifiers,
        date_limit=None,
        output_path=None,
        api_id=api_id,
        api_hash=api_hash,
        session_name="database_scan",
        use_database=True,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
        recommendation_manager=recommendation_manager,
        auto_harvest_recommendations=(
            recommendation_manager.settings.telegram_auto_harvest
            if recommendation_manager
            else False
        ),
        harvest_after_scrape=(
            recommendation_manager.settings.telegram_harvest_after_scrape
            if recommendation_manager
            else False
        ),
    )

    print("Scan complete. Updated data is available in the database.")


def handle_database_statistics(database: Optional[CoordinatesDatabase]) -> None:
    if not database:
        print("Database support is disabled.")
        return

    stats = database.get_database_statistics()
    print("\n=== Database Statistics ===")
    print(f"Total messages: {stats.total_messages}")
    print(f"Total coordinates: {stats.total_coordinates}")
    print(f"Tracked channels: {stats.tracked_channels}")
    print(f"Active channels: {stats.active_channels}")
    print(f"Average coordinate density: {stats.average_density:.2f}%")
    print(f"Last scrape: {stats.last_scrape or 'N/A'}")

    top_channels = database.get_top_channels_by_density()
    if top_channels:
        print("\nTop channels by coordinate density:")
        for channel in top_channels:
            username = channel.get("username") or channel["id"]
            density = channel.get("coordinate_density", 0.0)
            print(f" - {username}: {density:.2f}% ({channel.get('messages_with_coordinates', 0)} coords)")

    sessions = database.get_session_history()
    if sessions:
        print("\nRecent scraping sessions:")
        for session in sessions:
            summary = (
                f"#{session['id']} {session.get('session_type', 'unknown')} - {session.get('status', 'n/a')} - "
                f"channels {session.get('channels_scraped', 0)} - new messages {session.get('new_messages', 0)}"
            )
            print(f" - {summary}")


def handle_database_management(database: Optional[CoordinatesDatabase]) -> None:
    if not database:
        print("Database support is disabled.")
        return

    menu = """
=== Database Management ===
1. Export all data to CSV
2. Export data for a specific channel
3. Backup database
4. Vacuum database
5. Reset database
6. Import CSV files from results/
7. View database statistics
8. Return
Enter choice: """

    while True:
        choice = prompt_validated(
            menu,
            lambda value: value in {str(i) for i in range(1, 9)},
            error_msg="Please select an option between 1 and 8.",
        )
        if choice == "1":
            path = input("Enter CSV export path: ").strip() or "results/database_export.csv"
            df = database.export_to_dataframe()
            df.to_csv(path, index=False)
            print(f"Exported {len(df)} rows to {path}")
        elif choice == "2":
            channel_identifier = prompt_validated(
                "Enter channel ID: ",
                validate_positive_int,
                error_msg="Channel ID must be numeric.",
            )
            df = database.export_to_dataframe(int(channel_identifier))
            if df.empty:
                print("No records found for the specified channel.")
                continue
            path = input("Enter CSV export path: ").strip() or f"results/channel_{channel_identifier}.csv"
            df.to_csv(path, index=False)
            print(f"Exported {len(df)} rows to {path}")
        elif choice == "3":
            path = input("Enter backup file path: ").strip() or "results/telegram_coordinates_backup.db"
            if database.backup_database(path):
                print(f"Database backed up to {path}")
        elif choice == "4":
            if database.vacuum_database():
                print("Database vacuum completed.")
        elif choice == "5":
            confirm = input("This will delete ALL data. Type 'RESET' to confirm: ").strip()
            if confirm == "RESET":
                db_path = Path(database.db_path)
                database.close()
                if db_path.exists():
                    db_path.unlink()
                database.connect()
                database.initialize_schema()
                print("Database has been reset.")
            else:
                print("Reset cancelled.")
        elif choice == "6":
            imported = detect_and_migrate_all_results(database=database)
            print(f"Imported {imported} coordinate rows from CSV files.")
        elif choice == "7":
            handle_database_statistics(database)
        elif choice == "8":
            break
        else:
            print("Invalid choice. Please try again.")


def handle_kepler_visualization(database: Optional[CoordinatesDatabase]) -> None:
    import importlib.util

    if importlib.util.find_spec("keplergl") is None:
        print("\nKepler.gl visualisations require the optional 'keplergl' package.")
        print("Install it with: pip install keplergl")
        return

    from src.kepler_visualizer import (
        CoordinateVisualizer,
        create_map,
        create_temporal_animation,
        visualize_forward_network,
    )

    menu = """
=== Kepler.gl Visualisation ===

1. Visualise coordinates from CSV
2. Visualise all database records
3. Visualise a specific channel from the database
4. Create heatmap from CSV
5. Create cluster map from CSV
6. Create 3D hexagon map from CSV
7. Visualise forward network (database)
8. Create temporal animation from CSV
9. Back

Enter choice (1-9): """

    while True:
        choice = input(menu).strip()

        if choice == "1":
            csv_path = Path(input("CSV path: ").strip())
            if not csv_path.exists():
                print(f"File not found: {csv_path}")
                continue
            output = input("Output HTML path [results/map.html]: ").strip() or "results/map.html"
            try:
                create_map(csv_path, output)
                print(f"Interactive map saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create map: {exc}")

        elif choice == "2":
            if not database:
                print("Database support is disabled.")
                continue
            output = input("Output HTML path [results/database_map.html]: ").strip() or "results/database_map.html"
            try:
                visualizer = CoordinateVisualizer()
                visualizer.from_database(database.db_path, output_html=output)
                print(f"Interactive map saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create map: {exc}")

        elif choice == "3":
            if not database:
                print("Database support is disabled.")
                continue
            channel_value = input("Channel ID: ").strip()
            if not channel_value.isdigit():
                print("Channel ID must be numeric.")
                continue
            output = (
                input(f"Output HTML path [results/channel_{channel_value}.html]: ").strip()
                or f"results/channel_{channel_value}.html"
            )
            try:
                visualizer = CoordinateVisualizer()
                visualizer.from_database(database.db_path, channel_id=int(channel_value), output_html=output)
                print(f"Interactive map saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create map: {exc}")

        elif choice == "4":
            csv_path = Path(input("CSV path: ").strip())
            if not csv_path.exists():
                print(f"File not found: {csv_path}")
                continue
            output = input("Output HTML path [results/heatmap.html]: ").strip() or "results/heatmap.html"
            try:
                create_map(csv_path, output, visualization_type="heatmap")
                print(f"Heatmap saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create heatmap: {exc}")

        elif choice == "5":
            csv_path = Path(input("CSV path: ").strip())
            if not csv_path.exists():
                print(f"File not found: {csv_path}")
                continue
            output = input("Output HTML path [results/clusters.html]: ").strip() or "results/clusters.html"
            try:
                create_map(csv_path, output, visualization_type="clusters")
                print(f"Cluster map saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create cluster map: {exc}")

        elif choice == "6":
            csv_path = Path(input("CSV path: ").strip())
            if not csv_path.exists():
                print(f"File not found: {csv_path}")
                continue
            output = input("Output HTML path [results/hexagons.html]: ").strip() or "results/hexagons.html"
            try:
                create_map(csv_path, output, visualization_type="hexagons")
                print(f"3D hexagon map saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create 3D hexagon map: {exc}")

        elif choice == "7":
            if not database:
                print("Database support is disabled.")
                continue
            output = input("Output HTML path [results/forward_network.html]: ").strip() or "results/forward_network.html"
            try:
                map_instance = visualize_forward_network(database, output)
                if map_instance is None:
                    print("No forwarding relationships with coordinates found.")
                else:
                    print(f"Forward network saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create forward network map: {exc}")

        elif choice == "8":
            csv_path = Path(input("CSV path: ").strip())
            if not csv_path.exists():
                print(f"File not found: {csv_path}")
                continue
            time_column = input("Timestamp column [message_date]: ").strip() or "message_date"
            output = input("Output HTML path [results/temporal.html]: ").strip() or "results/temporal.html"
            try:
                create_temporal_animation(csv_path, output, time_column=time_column)
                print(f"Temporal animation saved to {output}")
            except VISUALIZATION_ERRORS as exc:
                print(f"Failed to create temporal animation: {exc}")

        elif choice == "9":
            break
        else:
            print("Invalid choice. Please select an option from 1 to 9.")


def handle_advanced_options(
    database: Optional[CoordinatesDatabase],
    db_config: DbConfig,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
    config: Config,
    env_path: Path,
) -> None:
    while True:
        choice = prompt_validated(
            ADVANCED_MENU,
            lambda value: value in {str(i) for i in range(1, 8)},
            error_msg="Please choose an option from 1 to 7.",
        )
        if choice == "1":
            handle_search_all_chats(
                database,
                db_config,
                api_id,
                api_hash,
                recommendation_manager,
                config,
                env_path,
            )
        elif choice == "2":
            handle_process_json(database)
        elif choice == "3":
            handle_scan_known_channels(database, db_config, api_id, api_hash, recommendation_manager)
        elif choice == "4":
            handle_database_management(database)
        elif choice == "5":
            handle_recommendation_management(
                recommendation_manager,
                database,
                db_config,
                api_id,
                api_hash,
            )
        elif choice == "6":
            handle_kepler_visualization(database)
        elif choice == "7":
            break
        else:
            print("Invalid selection. Please choose an option from 1 to 7.")


def main() -> None:
    configure_logging()
    env_path = Path(__file__).resolve().parent / ".env"
    config = load_environment(env_path)
    first_run = first_time_setup(config)

    if first_run:
        print("👋 Welcome! Let's get you set up in 3 steps...\n")
        api_id, api_hash, session_name = setup_wizard(env_path, config)
        config = load_environment(env_path)
        authenticated = True
    else:
        api_id, api_hash = ensure_api_credentials(env_path, config)
        session_name = prompt_session_name(config=config, env_path=env_path)
        authenticated = False

    if not authenticated:
        asyncio.run(ensure_telegram_authentication(api_id, api_hash, session_name))

    db_config = get_database_configuration(config)
    database = CoordinatesDatabase(db_config["path"]) if db_config["enabled"] else None
    recommendation_manager = RecommendationManager(database) if database else None

    show_startup_recommendations(
        recommendation_manager,
        database,
        db_config,
        api_id,
        api_hash,
    )

    while True:
        choice = prompt_validated(
            MAIN_MENU,
            lambda value: value in {"1", "2", "3", "4"},
            error_msg="Please choose an option from 1 to 4.",
        )
        if choice == "1":
            handle_specific_channel(
                database,
                db_config,
                api_id,
                api_hash,
                recommendation_manager,
                config,
                env_path,
            )
        elif choice == "2":
            handle_advanced_options(
                database,
                db_config,
                api_id,
                api_hash,
                recommendation_manager,
                config,
                env_path,
            )
        elif choice == "3":
            handle_database_statistics(database)
        elif choice == "4":
            print("Goodbye!")
            break
        else:
            print("Invalid selection. Please choose an option from 1 to 4.")


if __name__ == "__main__":  # pragma: no cover - interactive entry point
    main()


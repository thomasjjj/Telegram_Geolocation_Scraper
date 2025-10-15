"""Interactive entry point for the Telegram coordinates scraper."""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv, set_key

from src.channel_scraper import channel_scraper
from src.database import CoordinatesDatabase
from src.db_migration import detect_and_migrate_all_results, migrate_existing_csv_to_database
from src.json_processor import process_telegram_json, save_dataframe_to_csv

try:
    from telethon import TelegramClient
except ImportError as exc:  # pragma: no cover - missing dependency is fatal
    raise SystemExit("Telethon must be installed to run the scraper") from exc


DEFAULT_GEO_KEYWORDS = [
    "geolocation",
    "geo-location",
    "geolocated",
    "geolocate",
    "location",
    "located",
    "coordinates",
    "coordinate",
    "геолокация",
    "геолокации",
    "геолокацию",
    "геолокацией",
    "местоположение",
    "местоположении",
    "местоположения",
    "координаты",
    "координатах",
    "координатами",
    "геолокація",
    "геолокації",
    "місцезнаходження",
    "розташування",
    "координати",
    "координатах",
]


MAIN_MENU = """
=== Telegram Coordinates Scraper ===

Choose an option:
1. Search a specific channel/group
2. Search all accessible chats
3. Process a JSON export file
4. Scan all known channels with coordinates
5. View database statistics
6. Manage database
7. Exit

Enter your choice (1-7): """


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def load_environment(env_path: Path) -> None:
    load_dotenv(dotenv_path=env_path)
    if not env_path.exists():
        env_path.touch()


def ensure_api_credentials(env_path: Path) -> tuple[int, str]:
    api_id = os.environ.get("TELEGRAM_API_ID")
    if not api_id:
        api_id = input("Enter your Telegram API ID: ").strip()
        while not api_id.isdigit():
            print("API ID must be numeric.")
            api_id = input("Enter your Telegram API ID: ").strip()
        set_key(str(env_path), "TELEGRAM_API_ID", api_id)
        print("Saved API ID to .env")
    os.environ["TELEGRAM_API_ID"] = api_id

    api_hash = os.environ.get("TELEGRAM_API_HASH")
    if not api_hash:
        api_hash = input("Enter your Telegram API hash: ").strip()
        while not api_hash:
            print("API hash cannot be empty.")
            api_hash = input("Enter your Telegram API hash: ").strip()
        set_key(str(env_path), "TELEGRAM_API_HASH", api_hash)
        print("Saved API hash to .env")
    os.environ["TELEGRAM_API_HASH"] = api_hash

    return int(api_id), api_hash


def get_database_configuration() -> dict:
    return {
        "enabled": os.environ.get("DATABASE_ENABLED", "true").lower() == "true",
        "path": os.environ.get("DATABASE_PATH", "telegram_coordinates.db"),
        "skip_existing": os.environ.get("DATABASE_SKIP_EXISTING", "true").lower() == "true",
    }


async def _search_dialogs_for_keywords(
    api_id: int,
    api_hash: str,
    session_name: str,
    keywords: Iterable[str],
    message_limit: int = 200,
    days_limit: Optional[int] = None,
):
    cutoff = None
    if days_limit is not None:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days_limit)

    results = []

    async with TelegramClient(session_name, api_id, api_hash) as client:
        async for dialog in client.iter_dialogs():
            if dialog.is_user:
                continue

            entity = dialog.entity
            match_keyword = None
            excerpt = None

            async for message in client.iter_messages(entity, limit=message_limit):
                if cutoff and message.date and message.date < cutoff:
                    break

                message_text = message.message or ""
                if not message_text:
                    continue

                normalized = message_text.lower()
                for keyword in keywords:
                    if keyword.lower() in normalized:
                        match_keyword = keyword
                        start_idx = max(normalized.find(keyword.lower()) - 40, 0)
                        end_idx = min(start_idx + 120, len(message_text))
                        excerpt = message_text[start_idx:end_idx].replace("\n", " ")
                        break

                if match_keyword:
                    break

            if match_keyword:
                results.append({"dialog": dialog, "entity": entity, "keyword": match_keyword, "excerpt": excerpt})

    return results


def prompt_channel_selection(api_id: int, api_hash: str, session_name: str) -> List[str]:
    prompt = (
        "Enter Telegram channel usernames or IDs (comma separated) "
        "or type 'SEARCH' to scan joined chats for geolocation keywords: "
    )

    channels: List[str] = []

    while not channels:
        channels_input = input(prompt).strip()

        if not channels_input:
            print("At least one channel is required or type 'SEARCH' to run the keyword scan.")
            continue

        if channels_input.upper() == "SEARCH":
            print("Searching your joined chats and channels for geolocation keywords...")
            try:
                search_results = asyncio.run(
                    _search_dialogs_for_keywords(
                        api_id=api_id,
                        api_hash=api_hash,
                        session_name=session_name,
                        keywords=DEFAULT_GEO_KEYWORDS,
                    )
                )
            except Exception as exc:  # pragma: no cover - telethon specific errors
                logging.error("Failed to search joined chats: %s", exc)
                continue

            if not search_results:
                print("No matching chats or channels were found. Try again or enter channel names manually.")
                continue

            print("Found the following chats/channels with geolocation-related keywords:")
            for idx, result in enumerate(search_results, start=1):
                entity = result["entity"]
                username = getattr(entity, "username", None)
                dialog_name = result["dialog"].name or username or str(entity.id)
                identifier = f"@{username}" if username else f"ID {entity.id}"
                keyword = result["keyword"]
                excerpt = result["excerpt"] or "(no preview available)"
                print(f"  [{idx}] {dialog_name} ({identifier}) - matched '{keyword}': {excerpt}")

            selection = input(
                "Enter the numbers of the chats you want to scrape (comma separated, press Enter to select all): "
            ).strip()

            if not selection:
                channels = [str(result["entity"].id) for result in search_results]
                break

            chosen_indices: List[int] = []
            for item in selection.split(","):
                item = item.strip()
                if not item:
                    continue
                if not item.isdigit():
                    print(f"Ignoring invalid selection '{item}'. Please enter numeric choices.")
                    chosen_indices = []
                    break
                chosen_indices.append(int(item))

            if not chosen_indices:
                continue

            invalid_choices = [idx for idx in chosen_indices if idx < 1 or idx > len(search_results)]
            if invalid_choices:
                print(f"Invalid selection numbers: {', '.join(map(str, invalid_choices))}. Please try again.")
                continue

            channels = []
            for idx in chosen_indices:
                entity = search_results[idx - 1]["entity"]
                username = getattr(entity, "username", None)
                channels.append(username or str(entity.id))
        else:
            channels = [channel.strip() for channel in channels_input.split(",") if channel.strip()]

        if not channels:
            print("No valid channels selected. Please try again.")

    return channels


def prompt_date_limit() -> Optional[str]:
    while True:
        date_limit_input = input("Enter the date limit (YYYY-MM-DD, leave blank for no limit): ").strip()
        if not date_limit_input:
            return None
        try:
            datetime.datetime.strptime(date_limit_input, "%Y-%m-%d")
            return date_limit_input
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")


def prompt_output_paths() -> tuple[str, Optional[str], Optional[str]]:
    output_path = input("Enter the output CSV path: ").strip()
    while not output_path:
        print("Output path cannot be empty.")
        output_path = input("Enter the output CSV path: ").strip()

    export_kml = input("Export to KML as well? (y/N): ").strip().lower() == "y"
    kml_output_path = None
    if export_kml:
        custom_kml_path = input("Enter KML output path (press Enter to use CSV path with .kml): ").strip()
        kml_output_path = custom_kml_path or _derive_output_path(output_path, ".kml")

    export_kmz = input("Export to KMZ as well? (y/N): ").strip().lower() == "y"
    kmz_output_path = None
    if export_kmz:
        custom_kmz_path = input("Enter KMZ output path (press Enter to use CSV path with .kmz): ").strip()
        kmz_output_path = custom_kmz_path or _derive_output_path(output_path, ".kmz")

    return output_path, kml_output_path, kmz_output_path


def _derive_output_path(base_path: str, new_extension: str) -> str:
    base, ext = os.path.splitext(base_path)
    if not base:
        return base_path + new_extension
    if ext.lower() == new_extension.lower():
        return base_path
    return base + new_extension


def handle_specific_channel(database: Optional[CoordinatesDatabase], db_config: dict, api_id: int, api_hash: str) -> None:
    session_name = input("Enter the session name (press Enter for default 'simple_scraper'): ").strip() or "simple_scraper"
    channels = prompt_channel_selection(api_id, api_hash, session_name)
    date_limit = prompt_date_limit()
    output_path, kml_output, kmz_output = prompt_output_paths()

    channel_scraper(
        channel_links=channels,
        date_limit=date_limit,
        output_path=output_path,
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        kml_output_path=kml_output,
        kmz_output_path=kmz_output,
        use_database=db_config["enabled"] and database is not None,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
    )


def handle_search_all_chats(database: Optional[CoordinatesDatabase], db_config: dict, api_id: int, api_hash: str) -> None:
    session_name = input("Enter the session name (press Enter for default 'simple_scraper'): ").strip() or "simple_scraper"
    results = asyncio.run(
        _search_dialogs_for_keywords(api_id=api_id, api_hash=api_hash, session_name=session_name, keywords=DEFAULT_GEO_KEYWORDS)
    )
    if not results:
        print("No chats containing the default geolocation keywords were found.")
        return

    print("The following chats mention geolocation keywords:")
    for idx, result in enumerate(results, start=1):
        entity = result["entity"]
        username = getattr(entity, "username", None)
        dialog_name = result["dialog"].name or username or str(entity.id)
        identifier = f"@{username}" if username else f"ID {entity.id}"
        print(f"  [{idx}] {dialog_name} ({identifier}) - keyword: {result['keyword']}")

    selection = input("Enter numbers to scan (comma separated) or press Enter to scan all: ").strip()
    if selection:
        indices = []
        for item in selection.split(","):
            item = item.strip()
            if item.isdigit():
                indices.append(int(item))
        chosen = [results[i - 1] for i in indices if 1 <= i <= len(results)]
    else:
        chosen = results

    channels = []
    for result in chosen:
        entity = result["entity"]
        username = getattr(entity, "username", None)
        channels.append(username or str(entity.id))

    if not channels:
        print("No valid channels selected.")
        return

    output_path, kml_output, kmz_output = prompt_output_paths()
    channel_scraper(
        channel_links=channels,
        date_limit=None,
        output_path=output_path,
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        kml_output_path=kml_output,
        kmz_output_path=kmz_output,
        use_database=db_config["enabled"] and database is not None,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
    )


def handle_process_json(database: Optional[CoordinatesDatabase]) -> None:
    json_file = input("Enter the path to the Telegram JSON export: ").strip()
    if not json_file:
        print("JSON file path is required.")
        return

    post_link_base = input("Enter the base URL for post links (e.g. https://t.me/channel/): ").strip()
    if not post_link_base.endswith("/"):
        post_link_base += "/"

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


def handle_scan_known_channels(database: Optional[CoordinatesDatabase], db_config: dict, api_id: int, api_hash: str) -> None:
    if not database:
        print("Database support is disabled.")
        return

    min_density_input = input("Minimum coordinate density percentage to include (default 0): ").strip()
    try:
        min_density = float(min_density_input) if min_density_input else 0.0
    except ValueError:
        print("Invalid density value. Using 0%.")
        min_density = 0.0

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
    choice = input("Enter choice: ").strip().upper() or "A"

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
    output_name = datetime.datetime.utcnow().strftime("scan_known_channels_%Y%m%d_%H%M%S.csv")
    output_path = os.path.join("results", output_name)

    channel_scraper(
        channel_links=identifiers,
        date_limit=None,
        output_path=output_path,
        api_id=api_id,
        api_hash=api_hash,
        session_name="database_scan",
        use_database=True,
        skip_existing=db_config.get("skip_existing", True),
        db_path=db_config.get("path"),
        database=database,
    )

    print(f"Scan complete. Results stored in {output_path}")


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
7. Return
Enter choice: """

    while True:
        choice = input(menu).strip()
        if choice == "1":
            path = input("Enter CSV export path: ").strip() or "results/database_export.csv"
            df = database.export_to_dataframe()
            df.to_csv(path, index=False)
            print(f"Exported {len(df)} rows to {path}")
        elif choice == "2":
            channel_identifier = input("Enter channel ID: ").strip()
            if not channel_identifier.isdigit():
                print("Channel ID must be numeric.")
                continue
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
            break
        else:
            print("Invalid choice. Please try again.")


def main() -> None:
    configure_logging()
    env_path = Path(__file__).resolve().parent / ".env"
    load_environment(env_path)
    api_id, api_hash = ensure_api_credentials(env_path)

    db_config = get_database_configuration()
    database = CoordinatesDatabase(db_config["path"]) if db_config["enabled"] else None

    while True:
        choice = input(MAIN_MENU).strip()
        if choice == "1":
            handle_specific_channel(database, db_config, api_id, api_hash)
        elif choice == "2":
            handle_search_all_chats(database, db_config, api_id, api_hash)
        elif choice == "3":
            handle_process_json(database)
        elif choice == "4":
            handle_scan_known_channels(database, db_config, api_id, api_hash)
        elif choice == "5":
            handle_database_statistics(database)
        elif choice == "6":
            handle_database_management(database)
        elif choice == "7":
            print("Goodbye!")
            break
        else:
            print("Invalid selection. Please choose an option from 1 to 7.")


if __name__ == "__main__":  # pragma: no cover - interactive entry point
    main()


"""Interactive entry point for the Telegram coordinates scraper."""

from __future__ import annotations

import asyncio
import getpass
import csv
import datetime
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv, set_key

from src.channel_scraper import channel_scraper
from src.database import CoordinatesDatabase
from src.db_migration import detect_and_migrate_all_results, migrate_existing_csv_to_database
from src.json_processor import process_telegram_json, save_dataframe_to_csv
from src.recommendations import RecommendationManager

try:
    from telethon import TelegramClient
except ImportError as exc:  # pragma: no cover - missing dependency is fatal
    raise SystemExit("Telethon must be installed to run the scraper") from exc


DEFAULT_GEO_KEYWORDS = [
    "geolocation",
    "geoloc",
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
7. Manage recommended channels
8. Exit

Enter your choice (1-8): """


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


def get_default_session_name() -> str:
    return os.environ.get("TELEGRAM_SESSION_NAME", "simple_scraper")


def prompt_session_name() -> str:
    default_session = get_default_session_name()
    session_name = (
        input(f"Enter Telegram session name to use [{default_session}]: ").strip() or default_session
    )
    os.environ["TELEGRAM_SESSION_NAME"] = session_name
    return session_name


async def ensure_telegram_authentication(api_id: int, api_hash: str, session_name: str) -> None:
    """Ensure the Telegram session is authenticated before continuing."""

    print(f"\nConnecting to Telegram using session '{session_name}' to verify authentication...")

    phone_prompt = lambda: input("Enter your Telegram phone number (including country code): ").strip()
    password_prompt = lambda: getpass.getpass("Enter your Telegram 2FA password: ")

    client = TelegramClient(session_name, api_id, api_hash)
    try:
        await client.start(phone=phone_prompt, password=password_prompt)
        me = await client.get_me()
    except Exception as exc:  # pragma: no cover - Telethon runtime interaction
        raise SystemExit(f"Failed to authenticate with Telegram: {exc}") from exc
    else:
        if me:
            display_name_parts = [getattr(me, "first_name", None), getattr(me, "last_name", None)]
            display_name = " ".join(part for part in display_name_parts if part)
            identifier = getattr(me, "username", None) or display_name or str(getattr(me, "id", "unknown"))
            print(f"Authenticated as {identifier}.")
        else:
            print("Authentication successful.")
    finally:
        await client.disconnect()


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


def prompt_channel_selection() -> List[str]:
    prompt = "Enter Telegram channel usernames or IDs (comma separated): "

    channels: List[str] = []

    while not channels:
        channels_input = input(prompt).strip()

        if not channels_input:
            print("At least one channel is required.")
            continue

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
    title = recommendation.get("title") or username or f"ID:{recommendation['channel_id']}"
    username_display = f"@{username}" if username else f"ID:{recommendation['channel_id']}"
    score = recommendation.get("recommendation_score", 0.0)
    forward_count = int(recommendation.get("forward_count") or 0)
    coord_count = int(recommendation.get("coordinate_forward_count") or 0)
    sources = _decode_sources(recommendation)

    if score >= 70:
        indicator = "🔥"
    elif score >= 50:
        indicator = "⭐"
    else:
        indicator = "📌"

    line = [
        f"{index}. {indicator} {title} ({username_display})",
        f"   Score: {score:.1f}/100 | {coord_count}/{forward_count} forwards contained coordinates",
    ]
    if sources:
        line.append(f"   Forwarded by {len(sources)} tracked channel(s)")
    return "\n".join(line)


def show_startup_recommendations(
    recommendation_manager: Optional[RecommendationManager],
    database: Optional[CoordinatesDatabase],
    db_config: dict,
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
        limit=recommendation_manager.settings.max_display
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
    print("  L - Skip and continue to main menu")
    print()

    choice = input("Enter choice (S/T/V/L): ").strip().upper()
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
    else:
        print("Continuing to main menu...\n")


def handle_specific_channel(
    database: Optional[CoordinatesDatabase],
    db_config: dict,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
) -> None:
    default_session = get_default_session_name()
    session_name = (
        input(f"Enter the session name (press Enter for default '{default_session}'): ").strip()
        or default_session
    )
    channels = prompt_channel_selection()
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
    )


def handle_search_all_chats(
    database: Optional[CoordinatesDatabase],
    db_config: dict,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
) -> None:
    default_session = get_default_session_name()
    session_name = (
        input(f"Enter the session name (press Enter for default '{default_session}'): ").strip()
        or default_session
    )
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
    )


def scrape_recommended_channels_menu(
    recommendation_manager: Optional[RecommendationManager],
    database: Optional[CoordinatesDatabase],
    db_config: dict,
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
        recommendations = recommendation_manager.get_top_recommendations(limit=recommendation_manager.settings.max_display)
    else:
        print("\nScrape Recommended Channels")
        print("-" * 40)
        print("1. Scrape all pending recommendations")
        print("2. Scrape top N recommendations")
        print("3. Scrape specific recommendations")
        print("4. Back")
        print()

        choice = input("Enter choice (1-4): ").strip()
        if choice == "1":
            recommendations = recommendation_manager.list_recommendations(status="pending")
        elif choice == "2":
            limit_input = input("How many top recommendations to scrape? ").strip()
            try:
                limit = int(limit_input)
            except ValueError:
                print("Invalid number. Aborting.")
                return
            recommendations = recommendation_manager.get_top_recommendations(limit=limit)
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
        db_config: dict,
        api_id: int,
        api_hash: str,
        recommendations: List[Dict[str, Any]],
) -> None:
    from telethon.tl.types import PeerChannel
    from telethon import TelegramClient

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
                recommendations = [recommendation_manager.get_recommended_channel(r["channel_id"])
                                   for r in recommendations]
                recommendations = [r for r in recommendations if r is not None]
            except Exception as exc:
                print(f"Enrichment failed: {exc}")
                print("Continuing with available data...")

    identifiers: List[str | PeerChannel] = []
    for recommendation in recommendations:
        username = recommendation.get("username")
        if username:
            # Use username if available (most reliable)
            identifiers.append(username)
        else:
            # For numeric IDs, create a PeerChannel object
            channel_id = recommendation["channel_id"]
            identifiers.append(PeerChannel(channel_id))

    print(f"Preparing to scrape {len(identifiers)} channel(s).")
    confirm = input("Continue? (y/N): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    date_limit = input("Enter date limit (YYYY-MM-DD, or press Enter for no limit): ").strip() or None

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
    )

    for recommendation in recommendations:
        recommendation_manager.mark_recommendation_status(
            recommendation["channel_id"],
            "scraped",
            notes="Scraped without on-demand CSV export",
        )

    print("\n✅ Scraping complete! Review new data through the database management menu.")


def handle_recommendation_management(
    recommendation_manager: Optional[RecommendationManager],
    database: Optional[CoordinatesDatabase],
    db_config: dict,
    api_id: int,
    api_hash: str,
) -> None:
    if not recommendation_manager or not recommendation_manager.settings.enabled:
        print("Recommendation system is disabled.")
        return

    while True:
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

        print("Options:")
        print("  1. View all pending recommendations")
        print("  2. View top recommendations")
        print("  3. Search recommendations")
        print("  4. Scrape recommended channels")
        print("  5. Accept/Reject recommendations")
        print("  6. Enrich recommendations (fetch channel details)")
        print("  7. Export recommendations to CSV")
        print("  8. View forward analysis")
        print("  9. Back to main menu")
        print()

        choice = input("Enter choice (1-9): ").strip()

        if choice == "1":
            view_all_recommendations(recommendation_manager)
        elif choice == "2":
            view_top_recommendations(recommendation_manager)
        elif choice == "3":
            search_recommendations_cli(recommendation_manager)
        elif choice == "4":
            scrape_recommended_channels_menu(
                recommendation_manager,
                database,
                db_config,
                api_id,
                api_hash,
            )
        elif choice == "5":
            accept_reject_recommendations(recommendation_manager)
        elif choice == "6":
            enrich_recommendations_cli(recommendation_manager, api_id, api_hash)
        elif choice == "7":
            export_recommendations_cli(recommendation_manager)
        elif choice == "8":
            view_forward_analysis(recommendation_manager)
        elif choice == "9":
            break
        else:
            print("Invalid choice. Please try again.")


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
    recommendations = recommendation_manager.get_top_recommendations(limit=limit)
    if not recommendations:
        print("No recommendations meet the minimum score threshold.")
        return

    print()
    for idx, recommendation in enumerate(recommendations, start=1):
        print(_format_recommendation_line(idx, recommendation))
        print()


def search_recommendations_cli(recommendation_manager: RecommendationManager) -> None:
    term = input("Enter search term (username, title, or ID): ").strip()
    if not term:
        print("Search term is required.")
        return
    results = recommendation_manager.search_recommendations(term)
    if not results:
        print("No recommendations matched your search.")
        return
    print()
    for idx, recommendation in enumerate(results, start=1):
        print(_format_recommendation_line(idx, recommendation))
        print()


def accept_reject_recommendations(recommendation_manager: RecommendationManager) -> None:
    recommendations = recommendation_manager.get_top_recommendations(limit=20, min_score=0.0)
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
        decision = input("Enter choice (A/R/S/Q): ").strip().upper()

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
    except Exception as exc:  # pragma: no cover - Telethon runtime errors
        logging.error("Failed to enrich recommendations: %s", exc)


def export_recommendations_cli(recommendation_manager: RecommendationManager) -> None:
    status = input("Export which status? (pending/accepted/rejected/all) [pending]: ").strip().lower() or "pending"
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


def handle_scan_known_channels(
    database: Optional[CoordinatesDatabase],
    db_config: dict,
    api_id: int,
    api_hash: str,
    recommendation_manager: Optional[RecommendationManager],
) -> None:
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

    session_name = prompt_session_name()
    asyncio.run(ensure_telegram_authentication(api_id, api_hash, session_name))

    db_config = get_database_configuration()
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
        choice = input(MAIN_MENU).strip()
        if choice == "1":
            handle_specific_channel(database, db_config, api_id, api_hash, recommendation_manager)
        elif choice == "2":
            handle_search_all_chats(database, db_config, api_id, api_hash, recommendation_manager)
        elif choice == "3":
            handle_process_json(database)
        elif choice == "4":
            handle_scan_known_channels(database, db_config, api_id, api_hash, recommendation_manager)
        elif choice == "5":
            handle_database_statistics(database)
        elif choice == "6":
            handle_database_management(database)
        elif choice == "7":
            handle_recommendation_management(
                recommendation_manager,
                database,
                db_config,
                api_id,
                api_hash,
            )
        elif choice == "8":
            print("Goodbye!")
            break
        else:
            print("Invalid selection. Please choose an option from 1 to 8.")


if __name__ == "__main__":  # pragma: no cover - interactive entry point
    main()


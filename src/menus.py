"""High-level menu compositions for the interactive CLI."""

from __future__ import annotations

from typing import Dict, Optional

from src.menu_system import Menu, MenuStyle, NavigationContext


class MainMenu(Menu):
    """Top-level navigation menu."""

    def __init__(
        self,
        context: NavigationContext,
        stats: Dict[str, int],
        banner: Optional[Dict[str, str]] = None,
    ) -> None:
        intro_lines = []
        if banner:
            intro_lines.extend(
                [
                    f"{MenuStyle.YELLOW}ℹ️  New recommendations available{MenuStyle.END}",
                    (
                        "   • Pending channels: "
                        f"{MenuStyle.BOLD}{banner['pending']:,}{MenuStyle.END}"
                    ),
                ]
            )
            if banner.get("top_label"):
                intro_lines.append(
                    "   • Top score: "
                    f"{banner.get('top_score', 0.0):.1f} "
                    f"({banner['top_label']})"
                )
            intro_lines.append("   • Press [2] to review")

        help_lines = [
            "Use the number keys to choose an option.",
            "Shortcuts: B - Back, H - Home, Q - Quit, ? - Help.",
        ]

        super().__init__(
            "Telegram Coordinates Scraper",
            context,
            intro_lines=intro_lines,
            help_lines=help_lines,
        )

        pending = stats.get("pending_recommendations", 0)
        coords = stats.get("total_coordinates", 0)

        self.add_item(
            "1",
            "Quick Scrape",
            "Enter channel(s) and start immediately",
            icon="⚡",
        )
        self.add_item(
            "2",
            "Recommended Channels",
            "Review, scrape, and manage discovered leads",
            icon="📊",
            badge=f"{pending:,} new" if pending else None,
        )
        self.add_item(
            "3",
            "Database & Export",
            "Manage stored data, exports, and backups",
            icon="💾",
            badge=f"{coords:,} coords" if coords else None,
        )
        self.add_item(
            "4",
            "Statistics",
            "View scrape history and database metrics",
            icon="📈",
        )
        self.add_item(
            "5",
            "Visualise",
            "Open coordinates in Kepler.gl",
            icon="🗺️",
        )
        self.add_separator()
        self.add_item(
            "6",
            "Advanced Tools",
            "Bulk scans, JSON imports, and utilities",
            icon="🔧",
        )
        self.add_item(
            "7",
            "Settings",
            "Toggle startup tips and harvesting defaults",
            icon="⚙️",
        )
        self.add_separator()
        self.add_item(
            "0",
            "Exit",
            "Close the application",
            icon="🚪",
        )


class RecommendationsMenu(Menu):
    """Menu for working with recommended channels."""

    def __init__(self, context: NavigationContext, stats: Dict[str, int]) -> None:
        intro_lines = [
            f"{MenuStyle.BOLD}Status Overview{MenuStyle.END}",
            (
                "  Pending: "
                f"{MenuStyle.YELLOW}{stats.get('pending', 0):,}{MenuStyle.END} | "
                f"Accepted: {MenuStyle.GREEN}{stats.get('accepted', 0):,}{MenuStyle.END} | "
                f"Rejected: {MenuStyle.RED}{stats.get('rejected', 0):,}{MenuStyle.END}"
            ),
            (
                "  Total discovered: "
                f"{MenuStyle.BOLD}{stats.get('total_recommended', 0):,}{MenuStyle.END} | "
                f"Inaccessible: {stats.get('inaccessible', 0):,}"
            ),
        ]

        help_lines = [
            "Top recommendations prioritise high coordinate hit rates.",
            "Use Maintenance Tools for enrichment and cleanup tasks.",
        ]

        super().__init__(
            "Recommended Channels",
            context,
            intro_lines=intro_lines,
            help_lines=help_lines,
        )

        self.add_item(
            "1",
            "View Top Recommendations",
            "Show highest scoring leads and their stats",
            icon="🔥",
        )
        self.add_item(
            "2",
            "Review Pending List",
            "Browse up to 100 waiting recommendations",
            icon="📋",
        )
        self.add_item(
            "3",
            "Scrape Recommendations",
            "Launch scraping for selected recommended channels",
            icon="▶️",
        )
        self.add_item(
            "4",
            "Accept / Reject",
            "Bulk review and update recommendation status",
            icon="✓✗",
        )
        self.add_item(
            "5",
            "Harvest New Recommendations",
            "Use Telegram suggestions to discover more channels",
            icon="🌾",
        )
        self.add_item(
            "6",
            "Export to CSV",
            "Save recommendation data for external analysis",
            icon="📄",
        )
        self.add_item(
            "7",
            "Forward Analysis",
            "See which channels forward to each other",
            icon="🔗",
        )
        self.add_item(
            "8",
            "Maintenance Tools",
            "Enrichment, cleanup, and score recalculation",
            icon="🧰",
        )


class RecommendationMaintenanceMenu(Menu):
    """Maintenance utilities for recommendations."""

    def __init__(self, context: NavigationContext) -> None:
        intro_lines = [
            "Perform maintenance to improve recommendation quality.",
        ]
        help_lines = [
            "Enrichment fetches usernames and titles for anonymous IDs.",
        ]
        super().__init__(
            "Recommendation Maintenance",
            context,
            intro_lines=intro_lines,
            help_lines=help_lines,
        )
        self.add_item(
            "1",
            "Enrich Metadata",
            "Fetch usernames and titles for pending channels",
            icon="✨",
        )
        self.add_item(
            "2",
            "Clean Invalid Entries",
            "Remove recommendations that are likely users or invalid",
            icon="🧹",
        )
        self.add_item(
            "3",
            "Recalculate Scores",
            "Apply latest scoring algorithm to all recommendations",
            icon="🔄",
        )


class DatabaseMenu(Menu):
    """Database and export related operations."""

    def __init__(self, context: NavigationContext, stats: Dict[str, str]) -> None:
        intro_lines = [
            f"{MenuStyle.BOLD}Database Overview{MenuStyle.END}",
            (
                "  Messages: "
                f"{MenuStyle.BOLD}{stats.get('total_messages', 0):,}{MenuStyle.END} | "
                f"Coordinates: {stats.get('total_coordinates', 0):,} | "
                f"Channels: {stats.get('tracked_channels', 0):,}"
            ),
        ]
        last_scrape = stats.get("last_scrape")
        if last_scrape:
            intro_lines.append(f"  Last scrape: {last_scrape}")

        help_lines = [
            "Back up regularly before resetting or importing data.",
            "Snapshots create JSON exports that can be re-imported later.",
        ]

        super().__init__(
            "Database & Export",
            context,
            intro_lines=intro_lines,
            help_lines=help_lines,
        )

        self.add_item(
            "1",
            "Export All Data",
            "Full CSV export of all messages and coordinates",
            icon="📦",
        )
        self.add_item(
            "2",
            "Export Coordinate Summary",
            "Compact CSV with lat/lon/text/channel/link",
            icon="📍",
        )
        self.add_item(
            "3",
            "Export Specific Channel",
            "Choose a channel and export its history",
            icon="📝",
        )
        self.add_separator()
        self.add_item(
            "4",
            "Import CSV Files",
            "Load processed CSV results into the database",
            icon="📥",
        )
        self.add_item(
            "5",
            "Database Snapshot",
            "Create or import a portable JSON snapshot",
            icon="💿",
        )
        self.add_item(
            "6",
            "Backup Database",
            "Create a timestamped SQLite backup",
            icon="🔒",
        )
        self.add_item(
            "7",
            "Vacuum Database",
            "Reclaim space and optimise the file",
            icon="🧹",
        )
        self.add_item(
            "8",
            "Reset Database",
            "Delete ALL stored data and start fresh",
            icon="🗑️",
        )
        self.add_item(
            "9",
            "Import History",
            "Review the most recent import operations",
            icon="🕑",
        )


class AdvancedToolsMenu(Menu):
    """Tools intended for advanced workflows."""

    def __init__(self, context: NavigationContext) -> None:
        intro_lines = [
            "Utilities for power users and bulk operations.",
        ]
        help_lines = [
            "Search scans all chats you can access using the keyword list.",
        ]
        super().__init__(
            "Advanced Tools",
            context,
            intro_lines=intro_lines,
            help_lines=help_lines,
        )
        self.add_item(
            "1",
            "Search All Chats",
            "Scan accessible chats for geolocation keywords",
            icon="🔎",
        )
        self.add_item(
            "2",
            "Process Telegram JSON",
            "Convert exported JSON files into scraper CSV",
            icon="🗃️",
        )
        self.add_item(
            "3",
            "Scan Known Channels",
            "Rescan stored channels that already have coordinates",
            icon="📡",
        )
        self.add_item(
            "4",
            "Update Known Channels",
            "Fetch only the latest messages for tracked channels",
            icon="🔄",
        )


class SettingsMenu(Menu):
    """Configuration toggles exposed in the CLI."""

    def __init__(self, context: NavigationContext, settings: Dict[str, bool]) -> None:
        intro_lines = [
            "Toggle frequently used preferences. Changes persist in the .env file.",
        ]
        super().__init__(
            "Settings",
            context,
            intro_lines=intro_lines,
            help_lines=[
                "Recommended Channels banner shows pending items on startup.",
            ],
        )

        self.add_item(
            "1",
            "Startup Recommendations Banner",
            f"Currently {'ON' if settings.get('show_startup_banner') else 'OFF'}",
            icon="📣",
        )
        self.add_item(
            "2",
            "Auto Harvest After Scrape",
            f"Currently {'ON' if settings.get('auto_harvest') else 'OFF'}",
            icon="🌱",
        )
        self.add_item(
            "3",
            "Harvest After Manual Scrape",
            f"Currently {'ON' if settings.get('harvest_after') else 'OFF'}",
            icon="🧭",
        )

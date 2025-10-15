#!/usr/bin/env python3
"""Cleanup utility for repairing recommendation entity records."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.database import CoordinatesDatabase
from src.recommendations import RecommendationManager


def ensure_entity_type_column(database: CoordinatesDatabase) -> None:
    try:
        database.connect().execute(
            "ALTER TABLE recommended_channels ADD COLUMN entity_type TEXT"
        )
        print("Added missing entity_type column to recommended_channels table.")
    except sqlite3.OperationalError as exc:
        if "duplicate column" in str(exc).lower():
            print("entity_type column already exists.")
        else:
            raise


def run_cleanup(database: CoordinatesDatabase) -> None:
    print("\nStep 2: Cleaning up invalid recommendation rows...")
    stats = database.cleanup_invalid_recommendations()
    print(f"  • Total before: {stats['total_before']}")
    print(f"  • Removed (low ID): {stats['removed_by_heuristic']}")
    print(f"  • Removed (invalid status): {stats['removed_by_status']}")
    print(f"  • Removed (entity type): {stats['removed_by_type']}")
    print(f"  • Total removed: {stats['total_removed']}")
    print(f"  • Total after: {stats['total_after']}")


def recalc_scores(database: CoordinatesDatabase) -> None:
    manager = RecommendationManager(database)
    updated = manager.recalculate_all_scores()
    print(f"Recalculated {updated} recommendation score(s).")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Repair recommendation entries that reference invalid entity types."
    )
    parser.add_argument(
        "--database",
        default="telegram_coordinates.db",
        help="Path to the scraper SQLite database (default: %(default)s)",
    )
    args = parser.parse_args()

    database = CoordinatesDatabase(args.database)

    print("=" * 60)
    print("Recommendation Entity Repair")
    print("=" * 60)

    print("Step 1: Ensuring schema is up to date...")
    try:
        ensure_entity_type_column(database)
    except sqlite3.DatabaseError as exc:
        print(f"Failed to update schema: {exc}")
        sys.exit(1)

    run_cleanup(database)

    print("\nStep 3: Recalculating recommendation scores...")
    recalc_scores(database)

    database.close()
    print("\nAll done!")


if __name__ == "__main__":
    main()

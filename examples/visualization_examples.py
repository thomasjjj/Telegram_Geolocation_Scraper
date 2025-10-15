"""Usage examples for the integrated Kepler.gl visualisation helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.kepler_visualizer import (
    CoordinateVisualizer,
    create_map,
    create_temporal_animation,
    visualize_forward_network,
)
from src.database import CoordinatesDatabase


RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)


def example_quick_map() -> None:
    """Create a default point map from a CSV export."""

    csv_file = RESULTS_DIR / "coordinates.csv"
    if not csv_file.exists():
        print(f"Sample CSV not found at {csv_file}. Place a CSV export there to run the example.")
        return
    create_map(csv_file, RESULTS_DIR / "quick_map.html")
    print("Quick map generated.")


def example_heatmap_from_database(database_path: str) -> None:
    """Generate a density heatmap directly from the SQLite database."""

    visualizer = CoordinateVisualizer("heatmap")
    visualizer.from_database(database_path, output_html=RESULTS_DIR / "database_heatmap.html")
    print("Database heatmap generated.")


def example_channel_map(database_path: str, channel_id: int) -> None:
    """Visualise coordinates for a specific channel."""

    visualizer = CoordinateVisualizer("clusters")
    visualizer.from_database(
        database_path,
        channel_id=channel_id,
        output_html=RESULTS_DIR / f"channel_{channel_id}_map.html",
    )
    print(f"Channel {channel_id} map generated.")


def example_date_range(database_path: str, start: str, end: str) -> None:
    """Visualise coordinates constrained to a date range."""

    visualizer = CoordinateVisualizer()
    visualizer.from_database(
        database_path,
        date_range=(start, end),
        output_html=RESULTS_DIR / "date_range_map.html",
    )
    print("Date range map generated.")


def example_forward_network(database_path: str) -> None:
    """Render forwarding relationships as arcs."""

    database = CoordinatesDatabase(database_path)
    map_instance = visualize_forward_network(database, RESULTS_DIR / "forward_network.html")
    if map_instance is None:
        print("No forward relationships with coordinates were found.")
    else:
        print("Forward network map generated.")


def example_temporal_animation(csv_path: str, time_column: str = "message_date") -> None:
    """Create a time-aware animation from a CSV export."""

    create_temporal_animation(csv_path, RESULTS_DIR / "temporal_animation.html", time_column=time_column)
    print("Temporal animation generated.")


def example_custom_styling(csv_path: str) -> None:
    """Adjust configuration presets for bespoke styling."""

    df = pd.read_csv(csv_path)
    visualizer = CoordinateVisualizer("points")
    map_instance = visualizer.from_dataframe(df, RESULTS_DIR / "custom_points.html")
    config = map_instance.config
    config.setdefault("config", {}).setdefault("mapStyle", {})["styleType"] = "satellite"
    map_instance.config = config  # type: ignore[assignment]
    map_instance.save_to_html(file_name=str(RESULTS_DIR / "custom_points_satellite.html"))
    print("Custom styled map generated.")


def run_all_examples(database_path: str, csv_path: str, channel_id: int) -> None:
    """Run a curated selection of visualisation examples."""

    example_quick_map()
    example_heatmap_from_database(database_path)
    example_channel_map(database_path, channel_id)
    example_date_range(database_path, "2024-01-01", "2024-12-31")
    example_forward_network(database_path)
    example_temporal_animation(csv_path)
    example_custom_styling(csv_path)


if __name__ == "__main__":  # pragma: no cover - manual usage helpers
    DEFAULT_DB = "telegram_coordinates.db"
    DEFAULT_CSV = str(RESULTS_DIR / "coordinates.csv")
    DEFAULT_CHANNEL = 0

    print("Running visualisation examples...")
    run_all_examples(DEFAULT_DB, DEFAULT_CSV, DEFAULT_CHANNEL)
    print("Examples completed. Check the results/ directory for outputs.")

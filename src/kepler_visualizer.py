"""Interactive Kepler.gl visualisations for scraped coordinates."""

from __future__ import annotations

import importlib.util
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple, Union, TYPE_CHECKING

import pandas as pd

from src.kepler_configs import (
    get_arc_config,
    get_cluster_config,
    get_heatmap_config,
    get_hexagon_config,
    get_points_config,
    get_temporal_config,
)

LOGGER = logging.getLogger(__name__)

KEPLER_AVAILABLE = importlib.util.find_spec("keplergl") is not None

if TYPE_CHECKING:  # pragma: no cover - optional dependency typing helper
    from keplergl import KeplerGl as _KeplerGl
else:  # pragma: no cover - alias used at runtime only when available
    _KeplerGl = Any

if KEPLER_AVAILABLE:
    from keplergl import KeplerGl  # type: ignore
else:  # pragma: no cover - executed when dependency missing
    KeplerGl = _KeplerGl  # type: ignore


class CoordinateVisualizer:
    """Create and export Kepler.gl maps from scraper outputs."""

    def __init__(self, config_preset: Optional[str] = None) -> None:
        if not KEPLER_AVAILABLE:
            raise ImportError(
                "keplergl is not installed. Install optional visualisation dependencies with 'pip install keplergl'."
            )
        self.config_preset = config_preset
        self._map: Optional[KeplerGl] = None

    # ------------------------------------------------------------------
    def from_csv(self, csv_path: Union[str, Path], output_html: Union[str, Path] = "results/map.html") -> KeplerGl:
        """Create a map from a CSV file on disk."""

        dataframe = pd.read_csv(csv_path)
        return self._create_map(dataframe, output_html)

    def from_database(
        self,
        database_path: Union[str, Path],
        channel_id: Optional[int] = None,
        date_range: Optional[Tuple[str, str]] = None,
        output_html: Union[str, Path] = "results/map.html",
    ) -> KeplerGl:
        """Create a map directly from a SQLite database exported by the scraper."""

        from src.database import CoordinatesDatabase

        db = CoordinatesDatabase(str(database_path))
        dataframe = db.export_to_dataframe(channel_id=channel_id)

        if date_range:
            start, end = date_range
            dataframe = dataframe.copy()
            if "message_date" in dataframe.columns:
                dataframe["message_date"] = pd.to_datetime(dataframe["message_date"], errors="coerce")
                start_ts = pd.to_datetime(start)
                end_ts = pd.to_datetime(end)
                dataframe = dataframe[(dataframe["message_date"] >= start_ts) & (dataframe["message_date"] <= end_ts)]

        return self._create_map(dataframe, output_html)

    def from_dataframe(self, dataframe: pd.DataFrame, output_html: Union[str, Path] = "results/map.html") -> KeplerGl:
        """Create a map using an in-memory :class:`pandas.DataFrame`."""

        return self._create_map(dataframe, output_html)

    # ------------------------------------------------------------------
    def add_layer(self, layer_config: Dict[str, Any]) -> None:
        """Append an additional layer configuration to the current map."""

        if not self._map:
            raise ValueError("Create a map before adding layers.")

        config = self._map.config
        vis_state = config.setdefault("config", {}).setdefault("visState", {})
        layers: Iterable[Dict[str, Any]] = vis_state.setdefault("layers", [])
        vis_state["layers"] = list(layers) + [layer_config]
        self._map.config = config  # type: ignore[assignment]

    def export_config(self, output_path: Union[str, Path]) -> Path:
        """Persist the current map configuration to a JSON file."""

        if not self._map:
            raise ValueError("No map has been created yet.")

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as file_handle:
            json.dump(self._map.config, file_handle, indent=2)
        return output

    # ------------------------------------------------------------------
    def _create_map(self, dataframe: pd.DataFrame, output_html: Union[str, Path]) -> KeplerGl:
        if not KEPLER_AVAILABLE:
            raise ImportError(
                "keplergl is not installed. Install optional visualisation dependencies with 'pip install keplergl'."
            )

        prepared = self._prepare_dataframe(dataframe)
        config = self._determine_config(prepared)

        map_instance = KeplerGl(height=800, config=config)
        map_instance.add_data(data=prepared, name="coordinates")

        output_path = Path(output_html)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        map_instance.save_to_html(file_name=str(output_path))

        LOGGER.info("Interactive map saved to %s", output_path)
        self._map = map_instance
        return map_instance

    def _prepare_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if dataframe.empty:
            raise ValueError("No rows available for visualisation.")

        df = dataframe.copy()
        column_mapping: Dict[str, str] = {}
        for column in df.columns:
            lowered = column.lower()
            if "lat" in lowered and lowered != "latitude":
                column_mapping[column] = "latitude"
            elif lowered in {"lon", "lng", "long", "longitude"} and lowered != "longitude":
                column_mapping[column] = "longitude"
        if column_mapping:
            df = df.rename(columns=column_mapping)

        if "latitude" not in df.columns or "longitude" not in df.columns:
            raise ValueError("The provided data must include latitude and longitude columns.")

        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])
        if df.empty:
            raise ValueError("No valid coordinate rows found after cleaning.")

        if "message_date" in df.columns:
            df["message_date"] = pd.to_datetime(df["message_date"], errors="coerce")

        return df

    def _determine_config(self, dataframe: pd.DataFrame) -> Dict[str, Any]:
        config_map = {
            "points": get_points_config,
            "heatmap": get_heatmap_config,
            "clusters": get_cluster_config,
            "hexagons": get_hexagon_config,
            "arcs": get_arc_config,
        }

        if self.config_preset and self.config_preset in config_map:
            return config_map[self.config_preset](dataframe)

        row_count = len(dataframe)
        if row_count < 1_000:
            return get_points_config(dataframe)
        if row_count < 10_000:
            return get_cluster_config(dataframe)
        return get_hexagon_config(dataframe)


def create_map(
    source: Union[str, Path, pd.DataFrame],
    output_html: Union[str, Path] = "results/map.html",
    visualization_type: str = "auto",
) -> KeplerGl:
    """Create a Kepler.gl map from a CSV path, database path or dataframe."""

    if isinstance(source, pd.DataFrame):
        dataframe = source
    else:
        path = Path(source)
        if path.suffix.lower() == ".csv":
            dataframe = pd.read_csv(path)
        elif path.suffix.lower() in {".db", ".sqlite"}:
            visualizer = CoordinateVisualizer(
                None if visualization_type == "auto" else visualization_type
            )
            return visualizer.from_database(path, output_html=output_html)
        else:
            raise ValueError(f"Unsupported source: {source}")

    visualizer = CoordinateVisualizer(None if visualization_type == "auto" else visualization_type)
    return visualizer.from_dataframe(dataframe, output_html=output_html)


def visualize_forward_network(
    database: "CoordinatesDatabase",
    output_html: Union[str, Path] = "results/forward_network.html",
) -> Optional[KeplerGl]:
    """Render forwarding relationships between channels as arcs."""

    if not KEPLER_AVAILABLE:
        raise ImportError(
            "keplergl is not installed. Install optional visualisation dependencies with 'pip install keplergl'."
        )

    query = """
        SELECT 
            cf.from_channel_id AS from_channel,
            cf.to_channel_id AS to_channel,
            src.latitude AS from_lat,
            src.longitude AS from_lon,
            dest.latitude AS to_lat,
            dest.longitude AS to_lon,
            cf.forward_date,
            m2.message_text
        FROM channel_forwards cf
        JOIN messages m1 ON cf.message_ref = m1.id
        JOIN messages m2 ON m2.channel_id = cf.to_channel_id AND m2.message_id = m1.message_id
        JOIN coordinates src ON src.message_ref = m1.id
        JOIN coordinates dest ON dest.message_ref = m2.id
        WHERE cf.had_coordinates = 1
    """

    rows = database.query(query)
    if not rows:
        LOGGER.warning("No forward relationships with coordinates were found in the database.")
        return None

    dataframe = pd.DataFrame([dict(row) for row in rows])
    visualizer = CoordinateVisualizer("arcs")
    return visualizer.from_dataframe(dataframe, output_html=output_html)


def create_temporal_animation(
    csv_path: Union[str, Path],
    output_html: Union[str, Path] = "results/temporal_map.html",
    time_column: str = "message_date",
) -> KeplerGl:
    """Create a Kepler.gl map with a temporal filter for playback."""

    if not KEPLER_AVAILABLE:
        raise ImportError(
            "keplergl is not installed. Install optional visualisation dependencies with 'pip install keplergl'."
        )

    dataframe = pd.read_csv(csv_path)
    if time_column not in dataframe.columns:
        raise ValueError(f"Column '{time_column}' not present in CSV file.")

    dataframe[time_column] = pd.to_datetime(dataframe[time_column], errors="coerce")
    dataframe = dataframe.dropna(subset=[time_column])
    if dataframe.empty:
        raise ValueError("No valid timestamp rows available for temporal animation.")

    normalized_time_column = time_column.lower()
    dataframe = dataframe.rename(columns=lambda col: col.lower())
    if normalized_time_column not in dataframe.columns:
        raise ValueError(f"Column '{time_column}' not present after normalisation.")

    if "latitude" not in dataframe.columns or "longitude" not in dataframe.columns:
        dataframe = dataframe.rename(columns={
            col: "latitude" if "lat" in col.lower() else col for col in dataframe.columns
        })
        dataframe = dataframe.rename(columns={
            col: "longitude" if col.lower() in {"lon", "lng", "long"} else col for col in dataframe.columns
        })

    dataframe["latitude"] = pd.to_numeric(dataframe["latitude"], errors="coerce")
    dataframe["longitude"] = pd.to_numeric(dataframe["longitude"], errors="coerce")
    dataframe = dataframe.dropna(subset=["latitude", "longitude"])

    config = get_temporal_config(dataframe, normalized_time_column)
    visualizer = CoordinateVisualizer()
    map_instance = KeplerGl(height=800, config=config)
    map_instance.add_data(data=dataframe, name="coordinates")

    output_path = Path(output_html)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    map_instance.save_to_html(file_name=str(output_path))
    LOGGER.info("Temporal animation saved to %s", output_path)
    visualizer._map = map_instance  # type: ignore[attr-defined]
    return map_instance


__all__ = [
    "CoordinateVisualizer",
    "create_map",
    "create_temporal_animation",
    "visualize_forward_network",
]

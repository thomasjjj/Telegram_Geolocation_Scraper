"""Reusable Kepler.gl configuration presets for common visualisations."""

from __future__ import annotations

from typing import Dict

import pandas as pd


_COLOR_RANGE = {
    "name": "Global Warming",
    "type": "sequential",
    "category": "Uber",
    "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"],
}


def _base_state(latitude: float, longitude: float, zoom: float = 6.0) -> Dict:
    return {
        "latitude": latitude,
        "longitude": longitude,
        "zoom": zoom,
    }


def _centroid(df: pd.DataFrame, lat_col: str = "latitude", lon_col: str = "longitude") -> Dict[str, float]:
    if df.empty:
        return {"latitude": 0.0, "longitude": 0.0}
    return {
        "latitude": float(df[lat_col].mean()),
        "longitude": float(df[lon_col].mean()),
    }


def get_points_config(df: pd.DataFrame) -> Dict:
    centre = _centroid(df)
    return {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "points-layer",
                        "type": "point",
                        "config": {
                            "dataId": "coordinates",
                            "label": "Coordinate Points",
                            "color": [255, 203, 153],
                            "columns": {"lat": "latitude", "lng": "longitude"},
                            "isVisible": True,
                            "visConfig": {
                                "radius": 8,
                                "fixedRadius": False,
                                "opacity": 0.8,
                                "outline": True,
                                "thickness": 2,
                                "strokeColor": [255, 254, 230],
                                "colorRange": _COLOR_RANGE,
                                "radiusRange": [5, 50],
                            },
                        },
                    }
                ],
            },
            "mapState": {
                **_base_state(centre["latitude"], centre["longitude"]),
            },
            "mapStyle": {"styleType": "dark"},
        },
    }


def get_heatmap_config(df: pd.DataFrame) -> Dict:
    centre = _centroid(df)
    return {
        "version": "v1",
        "config": {
            "visState": {
                "layers": [
                    {
                        "id": "heatmap-layer",
                        "type": "heatmap",
                        "config": {
                            "dataId": "coordinates",
                            "label": "Coordinate Density",
                            "columns": {"lat": "latitude", "lng": "longitude"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.8,
                                "radius": 25,
                                "intensity": 1,
                                "colorRange": _COLOR_RANGE,
                            },
                        },
                    }
                ],
            },
            "mapState": {
                **_base_state(centre["latitude"], centre["longitude"]),
            },
            "mapStyle": {"styleType": "dark"},
        },
    }


def get_cluster_config(df: pd.DataFrame) -> Dict:
    centre = _centroid(df)
    return {
        "version": "v1",
        "config": {
            "visState": {
                "layers": [
                    {
                        "id": "cluster-layer",
                        "type": "cluster",
                        "config": {
                            "dataId": "coordinates",
                            "label": "Coordinate Clusters",
                            "columns": {"lat": "latitude", "lng": "longitude"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.8,
                                "clusterRadius": 40,
                                "colorRange": _COLOR_RANGE,
                            },
                        },
                    }
                ],
            },
            "mapState": {
                **_base_state(centre["latitude"], centre["longitude"]),
            },
        },
    }


def get_hexagon_config(df: pd.DataFrame) -> Dict:
    centre = _centroid(df)
    return {
        "version": "v1",
        "config": {
            "visState": {
                "layers": [
                    {
                        "id": "hexagon-layer",
                        "type": "hexagon",
                        "config": {
                            "dataId": "coordinates",
                            "label": "Hexagon Density",
                            "columns": {"lat": "latitude", "lng": "longitude"},
                            "isVisible": True,
                            "visConfig": {
                                "worldUnitSize": 1,
                                "resolution": 8,
                                "opacity": 0.8,
                                "coverage": 0.95,
                                "enable3d": True,
                                "elevationScale": 10,
                                "colorRange": _COLOR_RANGE,
                            },
                        },
                    }
                ],
            },
            "mapState": {
                **_base_state(centre["latitude"], centre["longitude"]),
                "pitch": 45,
                "bearing": 0,
            },
        },
    }


def get_arc_config(df: pd.DataFrame) -> Dict:
    if df.empty:
        centre = {"latitude": 0.0, "longitude": 0.0}
    else:
        centre = {
            "latitude": float((df["from_lat"].mean() + df["to_lat"].mean()) / 2),
            "longitude": float((df["from_lon"].mean() + df["to_lon"].mean()) / 2),
        }
    return {
        "version": "v1",
        "config": {
            "visState": {
                "layers": [
                    {
                        "id": "arc-layer",
                        "type": "arc",
                        "config": {
                            "dataId": "coordinates",
                            "label": "Forward Network",
                            "columns": {
                                "lat0": "from_lat",
                                "lng0": "from_lon",
                                "lat1": "to_lat",
                                "lng1": "to_lon",
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.6,
                                "thickness": 2,
                                "colorRange": _COLOR_RANGE,
                            },
                        },
                    }
                ],
            },
            "mapState": {
                **_base_state(centre["latitude"], centre["longitude"], zoom=5.0),
            },
        },
    }


def get_temporal_config(df: pd.DataFrame, time_column: str) -> Dict:
    centre = _centroid(df)
    return {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [
                    {
                        "dataId": "coordinates",
                        "id": "time-filter",
                        "name": time_column,
                        "type": "timeRange",
                        "enlarged": True,
                        "plotType": "histogram",
                        "animationWindow": "free",
                        "speed": 1,
                    }
                ],
                "layers": [
                    {
                        "id": "temporal-layer",
                        "type": "point",
                        "config": {
                            "dataId": "coordinates",
                            "label": "Temporal Coordinates",
                            "columns": {"lat": "latitude", "lng": "longitude"},
                            "isVisible": True,
                            "visConfig": {
                                "radius": 8,
                                "opacity": 0.8,
                                "colorRange": _COLOR_RANGE,
                            },
                        },
                    }
                ],
            },
            "mapState": {
                **_base_state(centre["latitude"], centre["longitude"]),
            },
        },
    }


__all__ = [
    "get_arc_config",
    "get_cluster_config",
    "get_heatmap_config",
    "get_hexagon_config",
    "get_points_config",
    "get_temporal_config",
]

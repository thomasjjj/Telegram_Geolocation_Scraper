"""Kepler.gl visualization module for Telegram coordinates."""

import pandas as pd
from pathlib import Path
from typing import Optional
import logging

try:
    from keplergl import KeplerGl

    KEPLER_AVAILABLE = True
except ImportError:
    KEPLER_AVAILABLE = False
    logging.warning("keplergl not installed. Run: pip install keplergl")


def create_kepler_map(
        csv_path: str,
        output_html: str = "results/coordinate_map.html",
        config: Optional[dict] = None
) -> Optional[KeplerGl]:
    """
    Create an interactive Kepler.gl map from coordinate CSV.

    Args:
        csv_path: Path to CSV file with coordinates
        output_html: Where to save the interactive map
        config: Optional Kepler.gl configuration

    Returns:
        KeplerGl map instance or None if kepler not available
    """
    if not KEPLER_AVAILABLE:
        print("❌ Kepler.gl not installed. Install with: pip install keplergl")
        return None

    # Load data
    df = pd.read_csv(csv_path)

    # Ensure required columns exist
    required = ['latitude', 'longitude']
    if not all(col in df.columns or col.title() in df.columns for col in required):
        print(f"❌ CSV must contain latitude and longitude columns")
        return None

    # Standardize column names
    column_mapping = {}
    for col in df.columns:
        if col.lower() == 'latitude':
            column_mapping[col] = 'latitude'
        elif col.lower() == 'longitude':
            column_mapping[col] = 'longitude'

    if column_mapping:
        df = df.rename(columns=column_mapping)

    # Create Kepler map
    print(f"🗺️  Creating Kepler.gl map with {len(df)} coordinates...")

    # Default config with nice styling
    if config is None:
        config = {
            'version': 'v1',
            'config': {
                'visState': {
                    'filters': [],
                    'layers': [
                        {
                            'type': 'point',
                            'config': {
                                'dataId': 'coordinates',
                                'label': 'Coordinates',
                                'color': [255, 203, 153],
                                'columns': {
                                    'lat': 'latitude',
                                    'lng': 'longitude'
                                },
                                'isVisible': True,
                                'visConfig': {
                                    'radius': 10,
                                    'fixedRadius': False,
                                    'opacity': 0.8,
                                    'outline': True,
                                    'thickness': 2,
                                    'strokeColor': [255, 254, 230],
                                    'colorRange': {
                                        'name': 'Global Warming',
                                        'type': 'sequential',
                                        'category': 'Uber',
                                        'colors': [
                                            '#5A1846',
                                            '#900C3F',
                                            '#C70039',
                                            '#E3611C',
                                            '#F1920E',
                                            '#FFC300'
                                        ]
                                    },
                                    'radiusRange': [0, 50],
                                }
                            }
                        }
                    ]
                },
                'mapState': {
                    'bearing': 0,
                    'dragRotate': False,
                    'latitude': df['latitude'].mean(),
                    'longitude': df['longitude'].mean(),
                    'pitch': 0,
                    'zoom': 6,
                    'isSplit': False
                },
                'mapStyle': {
                    'styleType': 'dark'
                }
            }
        }

    # Create map
    map_1 = KeplerGl(height=800, config=config)
    map_1.add_data(data=df, name='coordinates')

    # Save to HTML
    output_path = Path(output_html)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    map_1.save_to_html(file_name=str(output_path))

    print(f"✅ Interactive map saved to: {output_html}")
    print(f"   Open in browser to explore!")

    return map_1


def create_heatmap_config() -> dict:
    """Configuration for heatmap visualization."""
    return {
        'version': 'v1',
        'config': {
            'visState': {
                'layers': [
                    {
                        'type': 'heatmap',
                        'config': {
                            'dataId': 'coordinates',
                            'label': 'Coordinate Heatmap',
                            'color': [255, 203, 153],
                            'columns': {
                                'lat': 'latitude',
                                'lng': 'longitude'
                            },
                            'isVisible': True,
                            'visConfig': {
                                'opacity': 0.8,
                                'colorRange': {
                                    'name': 'Global Warming',
                                    'type': 'sequential',
                                    'category': 'Uber',
                                    'colors': [
                                        '#5A1846',
                                        '#900C3F',
                                        '#C70039',
                                        '#E3611C',
                                        '#F1920E',
                                        '#FFC300'
                                    ]
                                },
                                'radius': 20
                            }
                        }
                    }
                ]
            },
            'mapStyle': {
                'styleType': 'dark'
            }
        }
    }


def create_clustered_config() -> dict:
    """Configuration for clustered point visualization."""
    return {
        'version': 'v1',
        'config': {
            'visState': {
                'layers': [
                    {
                        'type': 'cluster',
                        'config': {
                            'dataId': 'coordinates',
                            'label': 'Clustered Coordinates',
                            'color': [255, 203, 153],
                            'columns': {
                                'lat': 'latitude',
                                'lng': 'longitude'
                            },
                            'isVisible': True,
                            'visConfig': {
                                'opacity': 0.8,
                                'clusterRadius': 40,
                                'colorRange': {
                                    'name': 'Global Warming',
                                    'type': 'sequential',
                                    'category': 'Uber',
                                    'colors': [
                                        '#5A1846',
                                        '#900C3F',
                                        '#C70039',
                                        '#E3611C',
                                        '#F1920E',
                                        '#FFC300'
                                    ]
                                }
                            }
                        }
                    }
                ]
            },
            'mapStyle': {
                'styleType': 'dark'
            }
        }
    }


def visualize_from_database(
        database_path: str = "telegram_coordinates.db",
        output_html: str = "results/coordinate_map.html",
        channel_id: Optional[int] = None,
        visualization_type: str = "points"
) -> Optional[KeplerGl]:
    """
    Create Kepler.gl visualization directly from database.

    Args:
        database_path: Path to SQLite database
        output_html: Where to save the HTML map
        channel_id: Optional channel ID to filter by
        visualization_type: 'points', 'heatmap', or 'clusters'

    Returns:
        KeplerGl map instance or None
    """
    if not KEPLER_AVAILABLE:
        print("❌ Kepler.gl not installed. Install with: pip install keplergl")
        return None

    from src.database import CoordinatesDatabase

    db = CoordinatesDatabase(database_path)
    df = db.export_to_dataframe(channel_id=channel_id)

    if df.empty:
        print("❌ No coordinates found in database")
        return None

    # Drop rows without coordinates
    df = df.dropna(subset=['latitude', 'longitude'])

    # Select config based on visualization type
    config_map = {
        'heatmap': create_heatmap_config(),
        'clusters': create_clustered_config(),
        'points': None  # Use default
    }

    config = config_map.get(visualization_type)

    # Save to temp CSV and create map
    temp_csv = "temp_coords.csv"
    df.to_csv(temp_csv, index=False)

    map_instance = create_kepler_map(temp_csv, output_html, config)

    # Clean up temp file
    Path(temp_csv).unlink(missing_ok=True)

    return map_instance


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create Kepler.gl visualization")
    parser.add_argument("csv_path", help="Path to CSV file with coordinates")
    parser.add_argument("--output", "-o", default="results/map.html",
                        help="Output HTML file path")
    parser.add_argument("--type", "-t", choices=['points', 'heatmap', 'clusters'],
                        default='points', help="Visualization type")

    args = parser.parse_args()

    if args.type == 'heatmap':
        config = create_heatmap_config()
    elif args.type == 'clusters':
        config = create_clustered_config()
    else:
        config = None

    create_kepler_map(args.csv_path, args.output, config)
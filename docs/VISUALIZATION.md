# Visualization Guide

The Telegram Coordinates Scraper integrates [Kepler.gl](https://kepler.gl) to deliver rich, interactive maps directly from scraped coordinates. This guide walks through the available tooling, configuration presets, and troubleshooting advice.

## Quick Start

1. **Install optional dependencies** (if not already installed):
   ```bash
   pip install keplergl jupyter
   ```
2. **Export coordinates** via the scraper or ensure the SQLite database is up to date.
3. **Launch the CLI** and open `Advanced Options → Visualise coordinates (Kepler.gl)`.
4. **Choose a data source** (CSV or database) and an output location. The tool generates a standalone HTML file that you can open in any modern browser.

> Tip: pass `auto_visualize=True` to `channel_scraper` when scripting to automatically create a map after each scraping run.

## Visualization Types

The scraper ships with several presets tuned for common geospatial analyses:

| Preset | Description | Ideal For |
| ------ | ----------- | --------- |
| `points` | Individual coordinates with hover tooltips | Datasets under 1k records |
| `heatmap` | Density heatmap highlighting hotspots | Regional pattern analysis |
| `clusters` | Automatic clustering for medium datasets | Comparing coordinate clusters |
| `hexagons` | 3D hexagon binning with elevation | Large datasets (>10k rows) |
| `arcs` | Arc links between origin and destination points | Forwarding network analysis |
| `temporal` | Time slider for chronological playback | Event timelines and trend analysis |

Use the CLI menu or the Python API to select a preset explicitly. When no preset is provided, the visualizer automatically chooses the most appropriate option based on dataset size.

## Customization

Kepler.gl stores visual settings in a JSON configuration. You can adapt presets programmatically:

```python
from pathlib import Path
import pandas as pd
from src.kepler_visualizer import CoordinateVisualizer

csv_path = Path("results/coordinates.csv")
visualizer = CoordinateVisualizer("points")
map_instance = visualizer.from_csv(csv_path, "results/custom_map.html")

config = map_instance.config
config.setdefault("config", {}).setdefault("mapStyle", {})["styleType"] = "satellite"
map_instance.config = config  # apply new styling
map_instance.save_to_html(file_name="results/custom_map_satellite.html")
```

You can also call `CoordinateVisualizer.export_config()` to persist a configuration for reuse.

## Advanced Features

- **Forward Network Visualisation** – Leverage `visualize_forward_network` to render arcs that show how coordinates propagate between channels.
- **Temporal Animations** – Use `create_temporal_animation` to generate a map with a time slider, enabling animated playback of coordinate distribution.
- **Auto-visualisation** – Pass `auto_visualize=True` to `channel_scraper` to automatically build an HTML map alongside CSV/KML exports.
- **Examples** – Review `examples/visualization_examples.py` for end-to-end demonstrations of the API.

## Troubleshooting

| Issue | Resolution |
| ----- | ---------- |
| `ImportError: keplergl is not installed` | Install the optional dependency with `pip install keplergl`. |
| Map renders without points | Ensure the source data includes valid `latitude` and `longitude` columns. The visualizer drops rows with invalid coordinates. |
| HTML file is empty or very small | Check that the input dataset contains at least one valid coordinate. |
| Browser warns about large file size | Consider filtering the dataset or using the cluster/hexagon presets for aggregation. |

## FAQ

**Can I share the generated HTML file?**  
Yes. The HTML bundle is self-contained and can be shared without additional assets. Avoid sharing sensitive coordinates without the appropriate approvals.

**Do I need a Kepler.gl account?**  
No. All processing occurs locally. Accounts are only necessary if you wish to publish maps to the Kepler.gl cloud service manually.

**Does the visualizer modify my database?**  
No. Visualisation reads from CSV files or the SQLite database without altering stored data.

**How can I export static images?**  
Open the generated HTML map in a browser and use the Kepler.gl export controls (located under the camera icon) to save PNG snapshots at custom resolutions.

# Telegram Coordinates Scraper

## Overview

Telegram Coordinates Scraper is a Python toolkit for locating geographical
coordinates in Telegram content. It combines an interactive CLI, reusable
library functions, and optional visualisation helpers to scan live chats or
offline exports for latitude/longitude pairs. Behind the scenes the project uses
[Telethon](https://github.com/LonamiWebs/Telethon) to talk to the Telegram API
and persists results to CSV, SQLite, KML/KMZ, and Kepler.gl outputs for further
analysis.

![Telegram coordinates workflow illustration](https://github.com/user-attachments/assets/f3d4b32e-f3b6-413c-bc7d-ea90cc8367fc)

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Interactive CLI](#interactive-cli)
  - [Programmatic scraping](#programmatic-scraping)
  - [Processing Telegram JSON exports](#processing-telegram-json-exports)
- [Output formats](#output-formats)
- [Visualising results](#visualising-results)
- [Recommendation system](#recommendation-system)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Features

### Search & discovery

- Guided setup wizard stores API credentials and Telethon session files for you.
- Quick Scrape mode lets you paste one or more channel links and start scraping
  immediately.
- Advanced options expose tooling to migrate historical CSVs, manage the
  database, harvest Telegram's channel recommendations, and launch the Kepler.gl
  visualiser.
- Global search scans every accessible chat for keyword matches and prints
  real-time progress.

### Coordinate detection

- Extracts decimal coordinates (e.g. `51.5074, -0.1278`) and DMS expressions.
- Includes English, Ukrainian, and Russian keywords out of the box; customise
  search terms through environment variables.
- Deduplicates messages using the SQLite database and optional batching.

### Data products & enrichment

- Streams results to CSV (or a SQLite database) to keep memory usage low.
- Optional KML/KMZ export integrates directly with Google Earth and GIS tools.
- Integrates with Kepler.gl to build interactive HTML maps (points, heatmaps,
  clusters, 3D hexagons, and temporal playback).
- Manages a scored queue of recommended channels sourced from forwards and the
  Telegram API.

## Requirements

- Python 3.8 or newer.
- Telegram API credentials (API ID and API Hash) from
  [my.telegram.org](https://my.telegram.org/).
- Dependencies listed in `requirements.txt`:
  `telethon`, `pandas`, `python-dotenv`, `colorama`, and optional
  visualisation extras (`keplergl`, `jupyter`).

## Installation

1. **(Optional) Create and activate a virtual environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```

2. **Clone the repository and install dependencies**

   ```bash
   git clone https://github.com/<your-username>/Telegram_Geolocation_Scraper.git
   cd Telegram_Geolocation_Scraper
   python -m pip install -r requirements.txt
   ```

3. **Provide Telegram API credentials**

   Copy the sample file and update the placeholders:

   ```bash
   cp example_credentials.env .env
   ```

   Edit `.env` and set at least the following values:

   ```dotenv
   TELEGRAM_API_ID=your_api_id
   TELEGRAM_API_HASH=your_api_hash
   TELEGRAM_SESSION_NAME=coordinates_scraper_session
   ```

   Run the CLI once to trigger the setup wizard if a session file has not been
   created yet.

## Configuration

Most settings are driven by environment variables. The `.env` file is loaded
with [python-dotenv](https://saurabh-kumar.com/python-dotenv/), so any value you
add there is available to the CLI and library components.

### Core settings

| Variable | Description |
| --- | --- |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Required Telegram API credentials. |
| `TELEGRAM_SESSION_NAME` | Name of the Telethon session file that stores your login. |
| `TELEGRAM_SEARCH_TERMS` | Comma-separated keywords for the "Search all accessible chats" workflow. |
| `TELEGRAM_COORDINATES_CSV_FILE` | Default filename for CSV exports (`coordinates_results.csv`). |
| `TELEGRAM_COORDINATES_RESULTS_FOLDER` | Folder where exports are written (`results`). |
| `TELEGRAM_COORDINATES_LOG_LEVEL`, `TELEGRAM_COORDINATES_LOG_FILE` | Logging configuration used by the CLI. |

### Recommendation & discovery controls

| Variable | Purpose |
| --- | --- |
| `RECOMMENDATIONS_ENABLED` | Toggle the recommendation banner and menus. |
| `RECOMMENDATIONS_MIN_SCORE` | Minimum score before a channel is surfaced. |
| `RECOMMENDATIONS_MIN_HIT_RATE` | Lower bound (in %) of coordinate density. |
| `RECOMMENDATIONS_SHOW_AT_STARTUP` | Show a summary of the top leads when the CLI starts. |
| `RECOMMENDATIONS_AUTO_ENRICH` | Allow automatic enrichment runs after a scrape. |
| `RECOMMENDATIONS_MAX_DISPLAY` | Number of items to show in the banner. |
| `RECOMMENDATIONS_QUALITY_WEIGHT`, `RECOMMENDATIONS_TRUST_WEIGHT` | Weights used when scoring channels. |
| `RECOMMENDATIONS_PENALTY_LOW_SAMPLE` | Penalise channels with too few forwards. |
| `RECOMMENDATIONS_HIDE_ZERO_COORDS` | Hide leads that have never produced coordinates. |
| `TELEGRAM_RECS_ENABLED` | Enable Telegram API recommendation harvesting. |
| `TELEGRAM_RECS_MIN_SOURCE_DENSITY` | Minimum hit rate a source must maintain before it seeds recommendations. |
| `TELEGRAM_RECS_AUTO_HARVEST` | Automatically call the Telegram API to refresh recommendations. |
| `TELEGRAM_RECS_HARVEST_AFTER_SCRAPE` | Run the auto-harvest step after, rather than before, a scrape. |
| `TELEGRAM_RECS_MAX_SOURCE_CHANNELS` | Cap the number of sources that feed the recommendation engine. |

### Performance tuning

| Variable | Description |
| --- | --- |
| `TELEGRAM_FETCH_BATCH_SIZE` | Number of messages requested from Telegram per batch. |
| `MESSAGE_PROCESSING_BATCH_SIZE` | Bulk size for message processing and database writes. |
| `COORDINATE_EXTRACTION_PARALLEL` | Enable multiprocessing for coordinate parsing. |
| `COORDINATE_PARALLEL_WORKERS` | Worker count when parallel extraction is enabled. |
| `DATABASE_ENABLED` | Toggle SQLite persistence. |
| `DATABASE_PATH` | Location of the SQLite database (`telegram_coordinates.db`). |
| `DATABASE_SKIP_EXISTING` | Skip messages already present in the database. |
| `DATABASE_WAL_MODE` | Enable SQLite WAL journalling for improved concurrency. |
| `DATABASE_CACHE_SIZE_MB` | Cache size (MB) passed to SQLite. |
| `MESSAGE_BATCH_LOG_INTERVAL` | How often progress updates are emitted during bulk scans. |

Refer to `example_credentials.env` for the complete list of supported keys.

## Usage

### Interactive CLI

1. Launch the interface:

   ```bash
   python Scrape_Coordinates.py
   ```

2. Follow the on-screen prompts:

   - **Quick Scrape** – enter one or more channel usernames/IDs manually or
     switch to the new *Import from file* option to load a prepared text file. A
     CSV (and optional database/KML/KMZ export) is created once the scrape
     finishes.
   - **Advanced Options** – access the global chat search, JSON import tools,
     database utilities, recommendation manager, and visualisation menu.
   - **View Results & Statistics** – inspect database totals, recently created
     exports, and recommendation summaries.

The first launch runs a setup wizard if credentials or a session file are
missing. After authentication the session is cached locally and reused on
subsequent runs.

#### Searching all accessible chats

The global chat search (Advanced Options → Search all accessible chats)
processes every channel and group your account can reach:

1. Configure the search when prompted:
   - **Time limit** – restrict the run to messages from the last *N* days.
   - **Messages per chat** – upper bound of messages retrieved for each chat
     (default 200, up to 1000).
2. Confirm to begin scanning. A live status line reports chats inspected,
   matches found, messages processed, and the current processing rate.
3. Press `Ctrl+C` to cancel without losing partial results.

#### Recommendation management

Advanced Options → Manage recommended channels provides tooling to:

- Review the ranked queue with quality indicators (🔥 excellent, ⭐ good, 📌
  moderate, ⚠️ low, ❌ poor).
- Filter leads by hit rate (press `F` in the banner) or hide zero-hit channels
  entirely via configuration.
- Harvest Telegram's "similar channel" recommendations manually or
  automatically when `TELEGRAM_RECS_AUTO_HARVEST=true`.
- Recalculate stored scores after adjusting weights or heuristics.

#### Channel list files

When using the Quick Scrape importer you can maintain a reusable text file of
channels. The parser supports:

- One channel per line (`@username`, numeric IDs, or `https://t.me/...` URLs)
- Optional comments starting with `#`
- Empty lines anywhere in the document

Invalid entries are skipped with line-specific warnings and duplicate channels
are automatically removed. A short preview confirms the first few channels
loaded from the file. Example:

```text
# Channels to monitor
@example_channel
t.me/AnotherChannel
-1001234567890
https://t.me/geo_updates
```

### Programmatic scraping

The `channel_scraper` helper inside `src/channel_scraper.py` exposes the core
functionality to other scripts:

```python
from src.channel_scraper import channel_scraper

# Ensure TELEGRAM_API_ID and TELEGRAM_API_HASH are set in the environment first.
df = channel_scraper(
    channel_links=["@channelname"],
    date_limit="2023-09-01",
    output_path="results/coordinates.csv",
    auto_visualize=True,
)

print(f"Saved {len(df)} coordinates")
```

Parameters let you toggle database usage, skip existing messages, export to
KML/KMZ, or automatically harvest Telegram recommendations before/after a
scrape.

### Processing Telegram JSON exports

You can analyse Telegram Desktop exports offline in two ways:

- From the CLI choose **Advanced Options → Process a JSON export file** and
  follow the prompts to locate the JSON, pick output filenames, and set a base
  URL for message links.
- From your own code call `process_telegram_json` in `src/json_processor.py` and
  use `save_dataframe_to_csv` to persist the results.

## Output formats

The CLI writes CSV files to the `results/` directory (automatically created) or
whichever folder you configure. CSV columns include:

| Column | Description |
| --- | --- |
| `message_id` | Telegram message identifier. |
| `message_content` | Text snippet that contained the coordinates. |
| `message_media_type` | Media classification derived from the Telegram message. |
| `message_published_at` | Timestamp of the message. |
| `date` | Date (UTC) extracted from the post timestamp. |
| `message_source` | Channel/group username or ID. |
| `latitude`, `longitude` | Extracted coordinates in decimal degrees. |

When processing JSON exports additional fields such as `Post ID`, `Channel ID`,
and `Post Link` are included to mirror the structure of the original export. KML
and KMZ exports replicate the same metadata for use in GIS software.

## Visualising results

Launch **Advanced Options → Visualise coordinates (Kepler.gl)** to build
interactive HTML maps without leaving the CLI. Presets cover points, heatmaps,
clusters, arcs, and temporal animations. Set `auto_visualize=True` when calling
`channel_scraper` programmatically to generate a Kepler.gl map alongside the
CSV export. See [docs/VISUALIZATION.md](docs/VISUALIZATION.md) for advanced
configuration tips.

## Recommendation system

Each recommendation combines hit-rate metrics, trust signals, and penalties to
produce a 0–100 score:

- **Coordinate hit rate (≤60 pts)** – percentage of forwards that include
  coordinates, weighted by sample size.
- **Trust signals (≤40 pts)** – subscriber counts, recency, Telegram metadata,
  and source diversity boost high-quality leads.
- **Penalties** – inaccessible channels are halved, and confirmed scams are
  dropped entirely.

The CLI displays quality icons based on hit rate thresholds and highlights
harvest statistics whenever Telegram recommendations are refreshed.

## Troubleshooting

- **"Could not find the input entity for PeerUser" warnings** – clean out stale
  recommendation entries with:

  ```bash
  python scripts/fix_recommendation_entities.py --database telegram_coordinates.db
  ```

  or select *Clean up invalid recommendations* from the CLI menu.

- **`AttributeError: 'RecommendationManager' object has no attribute 'get_recommended_channel'`** – update to the latest
  release. The recommendation helpers now live on `RecommendationManager`
  directly.

## Development

- Formatters/linters are not bundled; configure your own tooling if required.
- Run the automated test suite with:

  ```bash
  pytest
  ```


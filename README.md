# Telegram Coordinates Scraper

## Overview

This Python package extracts geographical coordinates from Telegram messages, supporting both decimal and DMS (Degrees, Minutes, Seconds) formats. It uses the [Telethon](https://github.com/LonamiWebs/Telethon) library to search through Telegram channels, groups, and chats, and can also process exported JSON chat histories.

![image](https://github.com/user-attachments/assets/f3d4b32e-f3b6-413c-bc7d-ea90cc8367fc)

## Features

- **Multiple Search Methods**:
  - Search specific Telegram channels or groups by username/ID
  - Search across all accessible Telegram chats
  - Process offline JSON exports from Telegram

- **Coordinate Detection**:
  - Supports decimal format (e.g., `51.5074, -0.1278`)
  - Supports DMS format (e.g., `51° 30' 26" N, 0° 7' 40" W`)
  - Multilingual search terms (English, Ukrainian, Russian)

- **Comprehensive Results**:
  - Message metadata (ID, date, text, source)
  - Direct links to the original messages
  - Latitude and longitude in decimal format
  - CSV export for easy analysis
  - Optional KML/KMZ export for direct use in Google Earth and GIS tools
  - Interactive Kepler.gl maps (points, heatmaps, clusters, 3D hexagons, temporal playback)

- **Configuration Options**:
  - Environment variables
  - Config files
  - Command-line arguments
  - Interactive prompts

## Requirements

- Python 3.8+
- Telegram API credentials (API ID and Hash)
- Dependencies:
  - telethon>=1.24.0
  - pandas>=1.3.0
  - python-dotenv>=0.21.0
  - keplergl>=0.3.2 (interactive visualisation)
  - jupyter>=1.0.0 (Kepler.gl HTML export support)

## Installation

- **(Optional) Create a virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\\Scripts\\activate
   ```

1. **Clone the repository**:
   ```bash
   git clone https://github.com/<your-username>/Telegram_Geolocation_Scraper.git
   cd Telegram_Geolocation_Scraper
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Telegram API credentials**:

   Copy the provided template and fill in your values:
   ```bash
   cp example_credentials.env .env
   ```

   Create a `.env` file with your Telegram API credentials:
   ```
   TELEGRAM_API_ID=your_api_id
   TELEGRAM_API_HASH=your_api_hash
   TELEGRAM_SESSION_NAME=coordinates_scraper_session
   ```

   You can get your API credentials from [my.telegram.org](https://my.telegram.org/):
   1. Log in with your phone number
   2. Go to 'API development tools'
   3. Create a new application
   4. Copy the API ID and API Hash

## Usage

### Command-line Interface

Run the script with Python:

```bash
python Scrape_Coordinates.py
```

You'll be prompted to choose one of three options:

1. **Search a specific channel**: Enter a channel username (e.g., @channelname) or ID
2. **Search all accessible chats**: The script will scan all chats you have access to
3. **Process a JSON export**: Process an exported Telegram chat history file

#### Searching All Accessible Chats

The **Search all accessible chats** option scans every channel and group you can access for geolocation keywords.

1. Select **Advanced Options → Search all accessible chats**.
2. Configure the search when prompted:
   - **Time limit** – Restrict the search to messages from the last *N* days (optional).
   - **Messages per chat** – Number of messages to analyse in each chat (default: 200, up to 1000).
3. Confirm to start the search.
4. Monitor the real-time progress display showing:
   - Chats checked
   - Matches found
   - Messages scanned
   - Processing rate
5. Press `Ctrl+C` at any time to cancel the search. Partial results are preserved.

**Performance tips:**

- Limit the search to recent messages (e.g., 30 days) for faster results.
- Reduce the messages-per-chat limit (e.g., 50) for quick exploratory scans.
- Increase the message limit (up to 1000) for deeper, more thorough searches.

### Command-line Arguments

For automated usage, you can use command-line arguments:

```bash
# Search a specific channel
python Scrape_Coordinates.py --mode channel --channel @channelname --output results.csv --export-kml

# Search all accessible chats and create a KMZ copy of the results
python Scrape_Coordinates.py --mode all --output results.csv --export-kmz

# Process a JSON export and provide custom output filenames
python Scrape_Coordinates.py --mode json --json-file export.json --post-link-base https://t.me/channelname/ \
  --output results.csv --kml-output results/my_coordinates.kml --kmz-output results/my_coordinates.kmz
```

### Using as a Package

You can also import the scraper in your own scripts. The `channel_scraper`
function handles the asynchronous work for you and returns a Pandas
`DataFrame` with the collected coordinates. The previous
`TelegramCoordinatesClient` helper has been removed in favor of the
consolidated `channel_scraper` entry point:

```python
from src.channel_scraper import channel_scraper

def main():
    df = channel_scraper(
        channel_links=["@channelname"],
        date_limit="2023-09-01",
        output_path="results.csv",
    )

    if df.empty:
        print("No coordinates found")
    else:
        print(f"Saved {len(df)} coordinates to results.csv")

if __name__ == "__main__":
    main()
```

If you prefer to provide credentials programmatically, pass `api_id` and
`api_hash` directly to `channel_scraper`. Otherwise, the function falls back to
the `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` environment variables.

### Discovery Methods

The scraper combines multiple strategies to surface high-signal coordinate channels:

#### 1. Forward Analysis (Automatic)

Tracks message forwards to identify channels that frequently share geolocated content.

#### 2. Telegram API Recommendations (Manual/Automatic)

Harvests Telegram's native "similar channels" suggestions from your best-performing sources.

**To harvest Telegram recommendations:**

1. Open **Advanced Options → Manage recommended channels** in the CLI.
2. Choose **Harvest Telegram API recommendations**.
3. Set the minimum coordinate density for source channels and optional source limits.
4. Review the new leads via the recommendation list (options 1 or 2).

**Automatic harvesting:**

Set `TELEGRAM_RECS_AUTO_HARVEST=true` in `.env` to enable background harvesting from high-quality channels (optionally toggle
`TELEGRAM_RECS_HARVEST_AFTER_SCRAPE` to defer harvesting until the scrape completes).

**Why this works:**

- Telegram's algorithm clusters channels with similar themes.
- Recommendations surface original content creators, not just forwarders.
- High-density sources feed additional leads back into the discovery loop.
- Reduces manual hunting by growing a self-sustaining network of channels.

### Recommendation System

The scraper maintains a queue of recommended channels sourced from forward analysis and Telegram's own suggestions. The scoring
system is designed to prioritise channels that consistently include usable coordinates.

#### Scoring Overview

- **Coordinate Hit Rate (up to 60 points):**
  - 80%+ hit rate → Excellent (60 pts)
  - 60–80% → Very good (55 pts)
  - 40–60% → Good (45 pts)
  - 20–40% → Moderate (30 pts)
  - 10–20% → Low (15 pts)
  - 5–10% → Very low (5 pts)
  - <5% → Negligible (0 pts)
- **Sample Confidence Modifier:** Larger forward counts boost confidence up to ×1.25. Tiny samples (<10 forwards) receive a
  cautious multiplier and an additional penalty if `RECOMMENDATIONS_PENALTY_LOW_SAMPLE=true` (default).
- **Trust Signals (up to 40 points):** Source diversity, recency, verification, subscriber counts, and Telegram recommendation
  metadata provide additive trust bonuses.
- **Penalties:** Scam/fake channels are scored at zero, and inaccessible channels receive a 50% penalty.

The final score is capped between 0 and 100, keeping high hit-rate channels at the top of the queue while demoting spammy or
low-signal sources.

#### Quality Indicators

When viewing recommendations in the CLI, each entry displays a quality icon based on hit rate:

- 🔥 **Excellent:** ≥60% of forwards include coordinates.
- ⭐ **Good:** 40–60% hit rate.
- 📌 **Moderate:** 20–40% hit rate.
- ⚠️ **Low:** 5–20% hit rate.
- ❌ **Poor:** <5% hit rate (a warning is shown when the sample size is meaningful).

#### Filtering Recommendations

From the startup recommendation banner, press **F** to filter suggestions by minimum coordinate hit rate. The management menu
also honours the global `RECOMMENDATIONS_MIN_HIT_RATE` setting (default 5%) to hide channels that rarely post coordinates. Set
`RECOMMENDATIONS_HIDE_ZERO_COORDS=true` if you only want to see channels that have already produced at least one coordinate.

#### Recalculating Scores

After changing configuration weights or updating the scoring algorithm, open **Advanced Options → Manage recommended channels →
Recalculate recommendation scores**. The tool will refresh every stored recommendation using the latest heuristics.

## Processing JSON Exports

To process an exported Telegram chat:

1. **Export chat history from Telegram**:
   - Open Telegram Desktop
   - Navigate to the channel/group
   - Click ⋮ (menu) > Export chat history
   - Select JSON format (disable media downloads)
   - Save the file

2. **Process the export**:
   - Select option 3 in the interactive mode
   - Enter output file name
   - Provide path to the JSON file
   - Specify save location for the CSV
   - Enter base URL for post links (e.g., https://t.me/channelname/)

## Output Files

The tool writes results to CSV so they can be analyzed or imported into GIS
software. By default, files are placed in the `results/` directory (the folder
is created automatically if it does not exist) and use the filename configured
in `TELEGRAM_COORDINATES_CSV_FILE`.

When the `--export-kml` or `--export-kmz` flags (or their corresponding
`--kml-output` / `--kmz-output` paths) are provided, the scraper creates
additional geospatial files alongside the CSV export. These files include the
same metadata as the CSV and can be opened directly in mapping tools such as
Google Earth, QGIS, or ArcGIS.

| Column | Description |
| --- | --- |
| `Post ID` | Numeric identifier of the Telegram message. |
| `Channel ID` | Internal Telegram ID for the channel or chat. |
| `Channel/Group Username` | Public username of the chat when available. |
| `Message Text` | Excerpt of the message that contained the coordinates. |
| `Date` | Date the message was published. |
| `URL` | Direct link to the original Telegram message. |
| `Latitude` | Latitude in decimal degrees. |
| `Longitude` | Longitude in decimal degrees. |

> **Note:** Some entry points expose additional fields. For example, offline
> JSON processing includes a `Post Link` column, and the simplified
> `Scrape_Coordinates.py` CLI uses snake_case column names
> (`message_id`, `message_source`, etc.) while providing the same underlying
> information.

## Visualizing Results

### Integrated Kepler.gl Maps

Use the built-in menu under **Advanced Options → Visualise coordinates (Kepler.gl)** to create interactive, shareable maps without leaving the scraper. The visualiser supports multiple presets (points, heatmap, clusters, hexagons, arcs, temporal animations) and outputs standalone HTML files ready for publication. For automation, pass `auto_visualize=True` to `channel_scraper` and a map will be generated alongside your CSV export. Refer to [docs/VISUALIZATION.md](docs/VISUALIZATION.md) for detailed guidance and advanced configuration tips.

## Troubleshooting

### "Could not find the input entity for PeerUser" warnings

This message appears when forwards from regular users were mistakenly added as
recommended channels. Update to the latest release and then run the cleanup
utility:

```bash
python scripts/fix_recommendation_entities.py --database telegram_coordinates.db
```

You can also select **Clean up invalid recommendations** from the CLI menu
(Advanced Options → Manage recommended channels) to purge bad records.

### AttributeError: 'RecommendationManager' object has no attribute 'get_recommended_channel'

Older builds expected recommendation helpers on the database instance. The
current release exposes convenient proxy methods directly on
``RecommendationManager``. Update the codebase and rerun the CLI; no further
action is required once the upgrade is applied.

### "Search all chats" takes too long

- Limit the search to recent messages (e.g., last 30 days).
- Reduce the messages-per-chat limit (try 50–100 instead of 200).
- Cancel with `Ctrl+C` and retry with narrower parameters or by selecting specific chats.

### Progress seems stuck

Long gaps usually occur while scanning chats with thousands of messages or when Telegram enforces rate limits.

- Wait 30 seconds to see if the progress counter updates.
- Review the log output for details.
- Cancel with `Ctrl+C` and restart with tighter limits if needed.

### Google Earth

1. Open Google Earth or Google Earth Pro
2. Import the generated KML/KMZ file (or the CSV file if preferred)
3. If importing CSV, map the latitude and longitude columns when prompted
4. Adjust display options to style the placemarks

### GIS Tools

The CSV format is compatible with most GIS tools:
- QGIS
- ArcGIS
- Mapbox
- Tableau

### Web Mapping

For web applications, you can use:
- Leaflet.js
- OpenLayers
- Google Maps API
- Folium (Python)

Example with Folium:

```python
import pandas as pd
import folium

# Load the data
df = pd.read_csv('coordinates_results.csv')

# Create a map centered at the mean of coordinates
map_center = [df['Latitude'].astype(float).mean(), df['Longitude'].astype(float).mean()]
m = folium.Map(location=map_center, zoom_start=6)

# Add markers
for _, row in df.iterrows():
    folium.Marker(
        location=[float(row['Latitude']), float(row['Longitude'])],
        popup=f"<a href='{row['URL']}' target='_blank'>{row['Message Text'][:100]}...</a>",
        tooltip=row['Date']
    ).add_to(m)

# Save the map
m.save('coordinates_map.html')
```

## Logging

All operations are logged to both console and file (`telegram_search.log`). The default log level is INFO, which can be changed in the configuration.

## Performance Optimizations

The latest release introduces a batched scraping pipeline designed for large channel archives. The scraper now groups Telegram messages into explicit batches, performs bulk database writes, and keeps coordinate extraction work cache-friendly. On a 10,000 message channel the end-to-end runtime typically drops from roughly 3.5 minutes to just over a minute.

| Operation | Previous approach | Optimised approach | Typical improvement* |
|-----------|------------------|--------------------|-----------------------|
| Telegram fetch | Sequential iterator | Explicit 100-message batches | 15–25% faster |
| Database writes | Per-message INSERT/SELECT | Bulk existence check + upsert | 8–12× faster |
| Coordinate extraction | Message-at-a-time | Vectorised within batch | 20–30% faster |

*Benchmarks gathered locally on 1k/10k/50k message datasets; exact gains depend on network quality and hardware.

### Batch configuration

Tune performance-sensitive parameters via the `.env` file:

```bash
TELEGRAM_FETCH_BATCH_SIZE=100      # Messages requested per Telegram API call
MESSAGE_PROCESSING_BATCH_SIZE=500  # Messages processed before writing to SQLite
DATABASE_WAL_MODE=true             # Enables concurrent-friendly Write-Ahead Logging
DATABASE_CACHE_SIZE_MB=64          # SQLite page cache size (MB)
MESSAGE_BATCH_LOG_INTERVAL=1000    # Progress log cadence
```

Smaller batch sizes reduce memory pressure on constrained hosts; larger batches improve throughput on servers with ample RAM.

### Troubleshooting slow scrapes

- Verify that the new composite indexes exist: `PRAGMA index_list(messages);`
- Keep WAL mode enabled for concurrent scraping sessions.
- Drop the processing batch size to 250 if you encounter memory spikes.
- When reprocessing historical data, temporarily disable `DATABASE_SKIP_EXISTING` to allow fast bulk upserts.

## Configuration

All runtime settings are sourced from environment variables, which are loaded
from a local `.env` file. The interactive entry point will create this file on
first run and prompt for any missing credentials, storing them for future use.

Key variables include:

- `TELEGRAM_API_ID`: Your Telegram API ID
- `TELEGRAM_API_HASH`: Your Telegram API hash
- `TELEGRAM_SESSION_NAME`: Session name for Telethon (defaults to
  `simple_scraper`)
- `DATABASE_ENABLED`: Enable or disable persistence to the SQLite database
  (`true` by default)
- `DATABASE_PATH`: Path to the SQLite database file (`telegram_coordinates.db`
  by default)
- `DATABASE_SKIP_EXISTING`: Skip messages that have already been processed
  (`true` by default)

Additional custom settings can be added to `.env` as needed; they become
available through the lightweight `Config` helper in `src/config.py`.

## Search Terms

The script filters messages using these key terms (configurable):

```
"E", "N", "S", "W", "Coordinates", "Geolocation", "Geolocated", "located", "location", "gps",
"Геолокація", "Геолокований", "Розташований", "Місцезнаходження",  # Ukrainian terms
"Геолокация", "Геолокированный", "Расположенный", "Местоположение", "Координати"  # Russian terms
```

## Security Notes

- Your Telegram API credentials are sensitive. Never commit them to public repositories.
- The `.env` file is added to `.gitignore` by default to prevent accidental exposure.
- Telethon session files contain authentication data - keep them secure.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is released under the MIT License.

## Disclaimer

This tool is provided for research and informational purposes only. Users are responsible for ensuring their usage complies with Telegram's Terms of Service and any applicable laws. The accuracy of coordinates and associated data is not guaranteed.

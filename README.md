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

## Configuration Options

The tool loads configuration in the following order (later sources override earlier ones):

1. Default values
2. Environment variables
3. Config file
4. Command-line arguments

### Environment Variables

- `TELEGRAM_API_ID`: Your Telegram API ID
- `TELEGRAM_API_HASH`: Your Telegram API hash
- `TELEGRAM_SESSION_NAME`: Session name for Telethon
- `TELEGRAM_SEARCH_TERMS`: Comma-separated list of search terms
- `TELEGRAM_COORDINATES_CSV_FILE`: Output CSV file path
- `TELEGRAM_COORDINATES_LOG_FILE`: Log file path
- `TELEGRAM_COORDINATES_LOG_LEVEL`: Logging level (INFO, DEBUG, etc.)

### Config File (`config.ini`)

For repeatable deployments, you can store settings in a `config.ini` file. The
loader automatically checks the project root, `~/.telegram_coordinates_scraper/`
and `/etc/telegram_coordinates_scraper/`. A minimal configuration looks like
this:

```ini
[telegram]
api_id = 123456
api_hash = your_api_hash
session_name = coordinates_scraper_session

[search]
search_terms = "E", "N", "S", "W", "Coordinates"

[output]
csv_file = results/coordinates_search_results.csv
results_folder = results

[logging]
log_file = telegram_search.log
log_level = INFO
```

Values from `config.ini` override the defaults and can be superseded by
environment variables or command-line arguments when needed.

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

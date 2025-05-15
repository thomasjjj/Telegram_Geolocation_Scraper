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
  - CSV export for easy analysis and mapping

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

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/telegram-coordinates-scraper.git
   cd telegram-coordinates-scraper
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Telegram API credentials**:
   
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
python Scrape_Coordinates.py --mode channel --channel @channelname --output results.csv

# Search all accessible chats
python Scrape_Coordinates.py --mode all --output results.csv

# Process a JSON export
python Scrape_Coordinates.py --mode json --json-file export.json --post-link-base https://t.me/channelname/ --output results.csv
```

### Using as a Package

You can also use the tool as a Python package:

```python
import asyncio
from telegram_coordinates_scraper import Config, TelegramCoordinatesClient, CoordinatesWriter

async def main():
    # Initialize configuration
    config = Config()
    
    # Get credentials and settings
    api_id, api_hash = config.get_telegram_credentials()
    session_name = config.get_session_name()
    search_terms = config.get_search_terms()
    csv_file = config.get_output_file()
    
    # Initialize client
    client = TelegramCoordinatesClient(api_id, api_hash, session_name)
    await client.start()
    
    try:
        # Search a specific channel
        channel = "@channelname"
        entity = await client.get_entity(channel)
        
        with CoordinatesWriter(csv_file) as writer:
            found = await client.search_channel(entity, search_terms, writer)
            print(f"Found {found} coordinates")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

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

## Visualizing Results

### Google Earth

1. Open Google Earth Pro
2. Go to File > Import
3. Select the CSV file
4. Map the latitude and longitude columns
5. Adjust display options

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

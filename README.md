# Scrape Coordinates from Telegram Messages - Scrape_Coordinates.py

## Overview
This Python script uses the Telethon library to search Telegram channels and chats for geographical coordinates in messages. It supports both decimal and DMS (Degrees, Minutes, Seconds) formats and logs results in a CSV file.

## Features
- Searches for coordinates in Telegram messages across specific or all chats.
- Supports both decimal and DMS coordinate formats.
- Saves extracted coordinates, along with message metadata, to a CSV file.
- Handles authentication securely using environment variables or user input.
- Implements robust logging and error handling.


## Search Terms

For speed, this script does not look through every message in the provided channels. Instead, it looks for key elements that normally coincide with coordinates. 

The script searches for messages containing the following terms:

```
"E", "N", "S", "W", "Coordinates", "Geolocation", "Geolocated", "located", "location", "gps",
"Геолокація", "Геолокований", "Розташований", "Місцезнаходження",  # Ukrainian terms
"Геолокация", "Геолокированный", "Расположенный", "Местоположение", "Координати"  # Russian terms
```

Logging


## Requirements
- Python 3.8+
- Telethon library
- A Telegram API ID and API Hash (obtained from [my.telegram.org](https://my.telegram.org/))

## Installation

1. Clone this repository or download the script.
   ```sh
   git clone https://github.com/yourrepo/scrape-coordinates.git
   cd scrape-coordinates
   ```

2. Install the required dependencies:
   ```sh
   pip install telethon
   ```

3. Set up your Telegram API credentials:
   - **Option 1**: Store API credentials as environment variables:
     ```sh
     export TELEGRAM_API_ID=your_api_id
     export TELEGRAM_API_HASH=your_api_hash
     ```
     *(For Windows, use `set` instead of `export`.)*
   - **Option 2**: Enter API credentials manually when prompted.

## Usage

Run the script:
```sh
python Scrape_Coordinates.py
```

You will be prompted to choose one of the following options:
1. Search a specific Telegram channel or group by username or ID.
2. Search all accessible Telegram chats.

Results will be saved to `older_coordinates_search_results.csv`.

## Logging
- Logs are stored in `telegram_search.log`.
- Console and file logging are enabled for debugging and tracking issues.

## Error Handling
- If authentication fails, the script prompts for manual input.
- Errors in retrieving messages or searching channels are logged.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contribution
Pull requests are welcome! If you find a bug or want to propose improvements, please open an issue.

## Contact
For any questions, contact [your email] or open an issue on the repository.



# Telegram_Geolocation_Scraper - json_export_coordinates.py

### Example
Using Exports from the Telegram Channels "WarArchive_ua" and "military_u_geo", this map was produced. This tool is agnostic to context, source accuracy, and bias. All results should be provisional and only used as a starting point.
![image](https://github.com/thomasjjj/Telegram_Geolocation_Scraper/assets/118008765/ce32041a-fb98-4173-acaa-9b49f05f962f)

## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Exporting Chat from Telegram](#exporting-chat-from-telegram)
  - [Running the Script](#running-the-script)
- [Viewing Coordinates on Google Earth](#viewing-coordinates-on-google-earth)

## Overview
This Python script automates the extraction of geographic coordinates from a Telegram channel's exported chat history. The extracted data is saved as a CSV file, including additional details like the post's ID, date, message content, and media type.

## Requirements
- Python 3.x
- Tkinter (usually comes pre-installed with Python)
- pandas library

## Installation
1. Ensure that you have Python installed. If not, download and install it from [python.org](https://www.python.org/).
2. Install pandas by running `pip install pandas` in your command line or terminal.

## Usage

### Exporting Chat from Telegram
1. Open the Telegram desktop application.
2. Navigate to the channel whose data you wish to export.
3. Click on the three vertical dots (⋮) at the top-right corner to reveal a dropdown menu.
4. Choose `Export chat history`.
5. In the dialogue box that appears:
    - Under 'Format', select `JSON`.
    - Disable all media downloads by unchecking the boxes next to each media type.
6. Click `Export` to save the JSON file on your computer.

### Running the Script
1. Save the Python script in a location of your choice.
2. Open your command line or terminal and navigate to the directory where the script is saved.
3. Run the script by typing `json_export_coordinates.py`.
4. Follow the on-screen prompts to:
    - Name the output CSV file.
    - Select the JSON file to process.
    - Specify the directory to save the CSV file.
    - Enter the base URL for post links.

## Viewing Coordinates on Google Earth
1. Open Google Earth on your computer.
2. Navigate to `File` > `Import`.
3. In the dialogue box, select the CSV file that you generated with this script.
4. Google Earth will automatically read the latitude and longitude columns, plotting the coordinates on the map.

By following these steps, you can visualise the geographic locations mentioned in the Telegram posts directly on Google Earth.

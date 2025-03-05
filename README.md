# Scrape Coordinates from Telegram Messages

## Overview
This Python script integrates two previously separate tools into one unified program. It utilizes the [Telethon](https://github.com/LonamiWebs/Telethon) library to search Telegram channels, groups, and chats for geographical coordinates within messages. The script supports both decimal and DMS (Degrees, Minutes, Seconds) formats, and it also offers the option to process exported JSON chat histories. All extracted coordinates, along with accompanying message metadata, are saved into a CSV file.

## Features
- **Multiple Search Options:**  
  - **Option 1:** Search a specific Telegram channel or group by username or ID.
  - **Option 2:** Search all accessible Telegram chats.
  - **Option 3:** Process a JSON export file to extract coordinates.
- **Unified Regex Extraction:**  
  A single regular expression is used throughout the script to consistently detect coordinates in both decimal and DMS formats.
- **Secure Authentication:**  
  Retrieves Telegram API credentials either from environment variables or via user input.
- **Robust Logging:**  
  All operations are logged to both console and file (`telegram_search.log`) for detailed debugging and auditing.
- **CSV Output:**  
  Extracted data is saved to a CSV file including fields such as Post ID, Channel/Group ID, Username, Message Text, Date, URL, Latitude, and Longitude.
- **JSON Export Support:**  
  In addition to live Telegram searches, the script can process JSON exports from Telegram channels—ideal for analyzing historical data.

## Search Terms
For efficiency, the script filters messages using key terms that usually coincide with the presence of coordinates. The search terms include:
```
"E", "N", "S", "W", "Coordinates", "Geolocation", "Geolocated", "located", "location", "gps",
"Геолокація", "Геолокований", "Розташований", "Місцезнаходження",  # Ukrainian terms
"Геолокация", "Геолокированный", "Расположенный", "Местоположение", "Координати"  # Russian terms
```

## Requirements
- **Python:** Version 3.8 or higher.
- **Libraries:**  
  - [Telethon](https://github.com/LonamiWebs/Telethon)
  - [pandas](https://pandas.pydata.org/) (for processing JSON exports)
- **Telegram Credentials:** A Telegram API ID and API Hash, which can be obtained from [my.telegram.org](https://my.telegram.org/).

## Installation

1. **Clone or Download the Repository:**
   ```sh
   git clone https://github.com/yourrepo/scrape-coordinates.git
   cd scrape-coordinates
   ```

2. **Install the Required Dependencies:**
   ```sh
   pip install telethon pandas
   ```

3. **Set Up Telegram API Credentials:**
   - **Option 1:** Set your credentials as environment variables:
     ```sh
     export TELEGRAM_API_ID=your_api_id
     export TELEGRAM_API_HASH=your_api_hash
     ```
     *(On Windows, use `set` instead of `export`.)*
   - **Option 2:** Enter your credentials manually when prompted by the script.

## Usage

Run the script using Python:
```sh
python main_script.py
```

When you run the script, you will be prompted to choose one of three options:

1. **Search a Specific Channel or Group:**  
   Enter the username (e.g., `@channelname`) or channel/group ID. The script will search messages in that chat for coordinate data.

2. **Search All Telegram Chats:**  
   The script will iterate through all accessible chats (channels, groups, private chats) and search for messages containing coordinates.

3. **Process a JSON Export File:**  
   This option is designed for processing a JSON file exported from a Telegram chat.  
   **Steps for JSON Export:**
   - **Exporting Chat from Telegram:**
     1. Open the Telegram desktop application.
     2. Navigate to the desired channel or group.
     3. Click on the three vertical dots (⋮) at the top-right corner.
     4. Select **Export chat history**.
     5. In the export dialogue, choose **JSON** as the format and disable media downloads.
     6. Export and save the JSON file.
   - **Processing the JSON Export:**
     The script will prompt you to:
     - Enter the output CSV file name.
     - Provide the full path to the JSON file.
     - Specify the path where you wish to save the resulting CSV.
     - Input the base URL for constructing post links.
     
   The extracted data will include details such as the Post ID, date, message content, media type, latitude, and longitude.

## Example Map
Using exports from Telegram channels such as "WarArchive_ua" and "military_u_geo", you can generate a map of the extracted coordinates. This tool is agnostic to context, source accuracy, and bias; all results should be treated as provisional and used as a starting point.

![image](https://github.com/thomasjjj/Telegram_Geolocation_Scraper/assets/118008765/ce32041a-fb98-4173-acaa-9b49f05f962f)

## Viewing Coordinates on Google Earth
1. Open **Google Earth** on your computer.
2. Navigate to **File > Import**.
3. Select the CSV file generated by the script.
4. Google Earth will read the latitude and longitude columns automatically, plotting the coordinates on the map.

## Logging
- **Log File:** All logs are written to `telegram_search.log`.
- **Console Output:** Logging is also output to the console for real-time feedback.
- **Error Handling:**  
  - Authentication failures prompt for manual input.
  - Any errors during message retrieval or channel searches are logged for debugging.

## License
This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

## Contribution
Contributions are welcome! If you encounter any bugs or have suggestions for improvements, please open an issue or submit a pull request.

## Contact
For any queries, please contact [your email] or open an issue in the repository.


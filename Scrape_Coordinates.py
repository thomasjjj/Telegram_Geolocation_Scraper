import csv
import re
import asyncio
import os
import logging
import sys
import json
import pandas as pd
from telethon import TelegramClient

# -----------------------------------------------------------------------------
# Configure Logging
# -----------------------------------------------------------------------------
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

file_handler = logging.FileHandler("telegram_search.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[stream_handler, file_handler]
)

# -----------------------------------------------------------------------------
# Obtain API Credentials
# -----------------------------------------------------------------------------
api_id_env = os.environ.get("TELEGRAM_API_ID")
api_hash_env = os.environ.get("TELEGRAM_API_HASH")

if api_id_env and api_hash_env:
    try:
        api_id = int(api_id_env)
        api_hash = api_hash_env
        logging.info("Successfully obtained API credentials from environment variables.")
    except ValueError as e:
        logging.error("Invalid API ID found in environment variables. Please check your settings.")
        raise e
else:
    try:
        api_id = int(input("Enter your Telegram API ID: "))
        api_hash = input("Enter your Telegram API Hash: ")
        logging.info("Successfully obtained API credentials from user input.")
    except ValueError as e:
        logging.error("Invalid input for API credentials. Please ensure the API ID is a number.")
        raise e

# -----------------------------------------------------------------------------
# Initialise the Telegram Client
# -----------------------------------------------------------------------------
client = TelegramClient('session_name', api_id, api_hash)

# Global counter to track the total number of coordinates found.
total_coordinates_found = 0

# -----------------------------------------------------------------------------
# Unified Regular Expression for Coordinate Patterns
# -----------------------------------------------------------------------------
# This regex pattern matches:
# 1. Decimal format (e.g., "49.12345, 38.67890")
# 2. Degrees/Minutes/Seconds (DMS) format (e.g., "49° 2' 44.16\" N, 38° 19' 16.68\" E")
coordinate_pattern = re.compile(
    r'(-?\d+\.\d+),\s*(-?\d+\.\d+)'  # Decimal format
    r'|'  # OR
    r'(\d+)[°\s](\d+)[\'\s](\d+(\.\d+)?)"?\s*([NS])[\s,]+(\d+)[°\s](\d+)[\'\s](\d+(\.\d+)?)"?\s*([EW])',  # DMS format
    re.IGNORECASE
)

# -----------------------------------------------------------------------------
# Utility Function: Convert DMS to Decimal Degrees
# -----------------------------------------------------------------------------
def dms_to_decimal(degrees, minutes, seconds, direction):
    """
    Convert coordinates from DMS (Degrees, Minutes, Seconds) format to decimal degrees.
    """
    try:
        degrees = float(degrees if degrees is not None else 0)
        minutes = float(minutes if minutes is not None else 0)
        seconds = float(seconds if seconds is not None else 0)
        decimal = degrees + minutes / 60 + seconds / 3600
        if direction.upper() in ['S', 'W']:
            decimal = -decimal
        return decimal
    except ValueError as e:
        logging.error("Error converting DMS to decimal: %s", e)
        return None

# -----------------------------------------------------------------------------
# Function: Search Channel for Coordinates
# -----------------------------------------------------------------------------
async def search_channel_for_coordinates(client, channel_entity, search_terms):
    """
    Search a specified Telegram channel or chat for coordinates using provided search terms,
    then write any found coordinates to a CSV file.
    """
    csv_file_path = 'coordinates_search_results.csv'
    file_exists = os.path.isfile(csv_file_path)
    global total_coordinates_found

    try:
        with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            if not file_exists:
                csvwriter.writerow(['Post ID', 'Channel ID', 'Channel/Group Username', 'Message Text', 'Date', 'URL', 'Latitude', 'Longitude'])

            for search_term in search_terms:
                logging.info("Searching for term '%s' in %s", search_term, channel_entity)
                try:
                    async for message in client.iter_messages(channel_entity, search=search_term):
                        if message.text:
                            for coordinates_match in coordinate_pattern.finditer(message.text):
                                if coordinates_match:
                                    # Check for decimal format first.
                                    if coordinates_match.group(1) and coordinates_match.group(2):
                                        latitude = coordinates_match.group(1)
                                        longitude = coordinates_match.group(2)
                                    # Otherwise, try for DMS format.
                                    elif coordinates_match.group(3) and coordinates_match.group(8):
                                        latitude = dms_to_decimal(
                                            coordinates_match.group(3),
                                            coordinates_match.group(4),
                                            coordinates_match.group(5),
                                            coordinates_match.group(6)
                                        )
                                        longitude = dms_to_decimal(
                                            coordinates_match.group(8),
                                            coordinates_match.group(9),
                                            coordinates_match.group(10),
                                            coordinates_match.group(11)
                                        )
                                    else:
                                        continue

                                    username = message.chat.username if message.chat.username else f"c/{message.chat.id}"
                                    url = f"https://t.me/{username}/{message.id}"
                                    row = [
                                        message.id,
                                        message.chat.id,
                                        username,
                                        message.text,
                                        message.date.strftime('%Y-%m-%d'),
                                        url,
                                        latitude,
                                        longitude
                                    ]
                                    csvwriter.writerow(row)
                                    total_coordinates_found += 1
                                    logging.info("Total: %d ¦ Coordinates found and written to CSV: %s, %s", total_coordinates_found, latitude, longitude)
                except Exception as inner_e:
                    logging.error("An error occurred while searching for term '%s' in %s: %s", search_term, channel_entity, inner_e)
    except Exception as e:
        logging.error("Failed to write to CSV file: %s", e)

# -----------------------------------------------------------------------------
# Function: Search All Chats for Coordinates
# -----------------------------------------------------------------------------
async def search_all_chats_for_coordinates(client, search_terms):
    """
    Search all available chats (channels, groups, private chats) for coordinates.
    """
    try:
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            logging.info("Searching in: %s", dialog.name)
            await search_channel_for_coordinates(client, dialog.entity, search_terms)
    except Exception as e:
        logging.error("Error retrieving dialogs: %s", e)

# -----------------------------------------------------------------------------
# Asynchronous Function: Telegram Search Main
# -----------------------------------------------------------------------------
async def telegram_search_main(option):
    """
    Execute the Telegram search options.
    """
    search_terms = [
        '"E', '"N', '"S', '"W', 'Coordinates', 'Geolocation', 'Geolocated', 'located', 'location', 'gps',
        'Геолокація', 'Геолокований', 'Розташований', 'Місцезнаходження',
        'Геолокация', 'Геолокированный', 'Расположенный', 'Местоположение', 'Координати'
    ]

    try:
        await client.start()
        logging.info("Telegram client started successfully.")
    except Exception as e:
        logging.error("Failed to start the Telegram client: %s", e)
        return

    if option == '1':
        channel_identifier = input("Enter the username (e.g., @channelname) or ID of the channel to search: ")
        try:
            entity = await client.get_entity(channel_identifier)
            await search_channel_for_coordinates(client, entity, search_terms)
        except Exception as e:
            logging.error("Could not find a channel or group with the identifier '%s'. Error: %s", channel_identifier, e)
            print(f"Error: Could not find a channel or group with the identifier '{channel_identifier}'.")
    elif option == '2':
        await search_all_chats_for_coordinates(client, search_terms)
    else:
        logging.warning("Invalid option provided to telegram_search_main.")

    try:
        await client.disconnect()
        logging.info("Telegram client disconnected successfully.")
    except Exception as e:
        logging.error("Error disconnecting the client: %s", e)

# -----------------------------------------------------------------------------
# Synchronous Function: JSON Export Processing
# -----------------------------------------------------------------------------
def process_json_export():
    """
    Process a JSON export file to extract coordinates using the unified regex pattern
    and save the results to a CSV file.
    """
    def process_telegram_data(json_file_path, post_link_base):
        messages_with_coordinates = []
        # Open and load the JSON file.
        with open(json_file_path, 'r', encoding='utf-8') as f:
            telegram_data = json.load(f)
        # Iterate through each message in the JSON export.
        for message in telegram_data.get('messages', []):
            text_field = str(message.get('text', ''))
            for coordinates_match in coordinate_pattern.finditer(text_field):
                if coordinates_match:
                    if coordinates_match.group(1) and coordinates_match.group(2):
                        latitude = coordinates_match.group(1)
                        longitude = coordinates_match.group(2)
                    elif coordinates_match.group(3) and coordinates_match.group(8):
                        latitude = dms_to_decimal(
                            coordinates_match.group(3),
                            coordinates_match.group(4),
                            coordinates_match.group(5),
                            coordinates_match.group(6)
                        )
                        longitude = dms_to_decimal(
                            coordinates_match.group(8),
                            coordinates_match.group(9),
                            coordinates_match.group(10),
                            coordinates_match.group(11)
                        )
                    else:
                        continue

                    post_id = message.get('id', 'N/A')
                    post_date = message.get('date', 'N/A')
                    post_type = message.get('type', 'N/A')
                    post_text = text_field
                    media_type = message.get('media_type', 'N/A')
                    message_info = {
                        'Post ID': post_id,
                        'Post Date': post_date,
                        'Post Message': post_text,
                        'Post Type': post_type,
                        'Media Type': media_type,
                        'Latitude': latitude,
                        'Longitude': longitude
                    }
                    messages_with_coordinates.append(message_info)
                    # Stop after first valid coordinate match in a message.
                    break
        df = pd.DataFrame(messages_with_coordinates)
        df['Post Link'] = post_link_base + df['Post ID'].astype(str)
        column_order = ['Post Link', 'Post ID', 'Post Date', 'Post Message', 'Post Type', 'Media Type', 'Latitude', 'Longitude']
        df = df[column_order]
        return df

    while True:
        csv_file_name = input("Please enter the name for the output CSV file (e.g., channel_name_coordinates): ")
        json_file_path = input("Please enter the full path to the JSON file you want to process: ")
        csv_save_path = input(f"Please enter the full path where you want to save the CSV file (e.g., C:/path/to/save/{csv_file_name}.csv): ")
        post_link_base = input("Please enter the base URL for the post links (e.g., https://t.me/WarArchive_ua/): ")
        df_messages_with_coordinates = process_telegram_data(json_file_path, post_link_base)
        df_messages_with_coordinates.to_csv(csv_save_path, index=False, encoding='utf-8')
        print(f"CSV file saved as {csv_save_path}")
        another_file = input("Do you want to process another file? (yes/no): ").strip().lower()
        if another_file != 'yes':
            break

# -----------------------------------------------------------------------------
# Main Entry Point
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    print("Please select a search option:")
    print("1 - Search a specific channel by username or ID")
    print("2 - Search all channels, groups, and chats")
    print("3 - Process a JSON export file to retrieve coordinates")
    option = input("Enter your choice (1, 2, or 3): ").strip()

    if option in ['1', '2']:
        try:
            asyncio.run(telegram_search_main(option))
        except Exception as e:
            logging.critical("An unhandled exception occurred: %s", e)
    elif option == '3':
        process_json_export()
    else:
        print("Invalid input. Please run the script again and enter 1, 2, or 3.")

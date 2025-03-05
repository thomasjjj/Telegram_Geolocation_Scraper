import csv
import re
import asyncio
import os
import logging
from telethon import TelegramClient
import sys

# -----------------------------------------------------------------------------
# Configure Logging
# -----------------------------------------------------------------------------
# Set up logging to file and console with an appropriate logging level.
# Create a stream handler for stdout with UTF-8 encoding
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

# Create a file handler with UTF-8 encoding
file_handler = logging.FileHandler("telegram_search.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[stream_handler, file_handler]
)

# -----------------------------------------------------------------------------
# Obtain API Credentials
# -----------------------------------------------------------------------------
# Try to retrieve Telegram API credentials from environment variables.
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
    # If environment variables are missing or invalid, prompt the user for input.
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
# Regular Expression for Coordinate Patterns
# -----------------------------------------------------------------------------
# The regex pattern matches two formats:
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

    Parameters:
        degrees (str): Degrees part of the coordinate.
        minutes (str): Minutes part of the coordinate.
        seconds (str): Seconds part of the coordinate.
        direction (str): Direction indicator ('N', 'S', 'E', or 'W').

    Returns:
        float or None: The coordinate in decimal degrees, or None if conversion fails.
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

    Parameters:
        client (TelegramClient): The Telegram client instance.
        channel_entity: The target channel, group, or chat entity.
        search_terms (list): A list of search terms to filter messages.
    """
    csv_file_path = 'coordinates_search_results.csv'
    file_exists = os.path.isfile(csv_file_path)
    global total_coordinates_found

    try:
        with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write CSV header if file is being created for the first time.
            if not file_exists:
                csvwriter.writerow(['Post ID', 'Channel ID', 'Channel/Group Username', 'Message Text', 'Date', 'URL', 'Latitude', 'Longitude'])

            # Iterate over each search term.
            for search_term in search_terms:
                logging.info("Searching for term '%s' in %s", search_term, channel_entity)
                try:
                    async for message in client.iter_messages(channel_entity, search=search_term):
                        if message.text:
                            # Use regex to search for coordinate patterns in the message text.
                            for coordinates_match in coordinate_pattern.finditer(message.text):
                                if coordinates_match:
                                    # Determine if the match is in decimal format.
                                    if coordinates_match.group(1) and coordinates_match.group(2):
                                        latitude = coordinates_match.group(1)
                                        longitude = coordinates_match.group(2)
                                    # Otherwise, check for DMS format.
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
                                        continue  # Skip if no recognisable coordinate format is found.

                                    # Construct the URL to the message using the channel's username or fallback identifier.
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
# Main Function: Run Coordinate Searches
# -----------------------------------------------------------------------------
async def main():
    """
    Main function to initiate the Telegram client and perform coordinate searches across channels/chats.
    """
    try:
        # Start the Telegram client session.
        await client.start()
        logging.info("Telegram client started successfully.")
    except Exception as e:
        logging.error("Failed to start the Telegram client: %s", e)
        return

    async def search_all_chats_for_coordinates(client, search_terms):
        """
        Search all available chats (channels, groups, private chats) for coordinates.

        Parameters:
            client (TelegramClient): The Telegram client instance.
            search_terms (list): A list of search terms to filter messages.
        """
        try:
            dialogs = await client.get_dialogs()
            for dialog in dialogs:
                logging.info("Searching in: %s", dialog.name)
                await search_channel_for_coordinates(client, dialog.entity, search_terms)
        except Exception as e:
            logging.error("Error retrieving dialogs: %s", e)

    # Display search options to the user.
    print("Please select a search option:")
    print("1 - Search a specific channel by username or ID")
    print("2 - Search all channels, groups, and chats")
    option = input("Enter your choice (1 or 2): ")

    # List of search terms for finding potential coordinate data.
    search_terms = [
        '"E', '"N', '"S', '"W', 'Coordinates', 'Geolocation', 'Geolocated', 'located', 'location', 'gps',
        'Геолокація', 'Геолокований', 'Розташований', 'Місцезнаходження',  # Ukrainian terms.
        'Геолокация', 'Геолокированный', 'Расположенный', 'Местоположение', 'Координати'  # Russian terms.
    ]

    if option == '1':
        # Allow the user to search a specific channel by username or ID.
        channel_identifier = input("Enter the username (e.g., @channelname) or ID of the channel to search: ")
        try:
            entity = await client.get_entity(channel_identifier)
            await search_channel_for_coordinates(client, entity, search_terms)
        except Exception as e:
            logging.error("Could not find a channel or group with the identifier '%s'. Error: %s", channel_identifier, e)
            print(f"Error: Could not find a channel or group with the identifier '{channel_identifier}'.")
    elif option == '2':
        # Search across all available chats.
        await search_all_chats_for_coordinates(client, search_terms)
    else:
        logging.warning("Invalid input received: %s", option)
        print("Invalid input. Please enter either 1 or 2.")

    try:
        # Disconnect the Telegram client.
        await client.disconnect()
        logging.info("Telegram client disconnected successfully.")
    except Exception as e:
        logging.error("Error disconnecting the client: %s", e)

# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical("An unhandled exception occurred: %s", e)

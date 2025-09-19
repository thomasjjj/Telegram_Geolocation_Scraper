"""
Simple channel scraper for Telegram coordinates.

This module provides a simplified API for scraping coordinates from Telegram channels
based on the contribution by tom-bullock.
"""

import asyncio
import datetime
import logging
import os
import re
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
import pandas as pd

from src.coordinates import extract_coordinates
from src.export import save_dataframe_to_kml, save_dataframe_to_kmz


async def scrape_channel(client, channel_id, date_limit, coordinate_pattern=None):
    """
    Scrape a single channel for coordinates.

    Args:
        client (TelegramClient): Initialized Telegram client
        channel_id (str): Channel username or ID
        date_limit (datetime): Only messages after this date will be processed
        coordinate_pattern (re.Pattern, optional): Regex pattern for finding coordinates

    Returns:
        tuple: Lists of extracted data (message_ids, texts, media_types, dates, sources, latitudes, longitudes)
    """
    # Initialize data lists
    message_ids = []
    message_texts = []
    media_types = []
    dates = []
    sources = []
    latitudes = []
    longitudes = []

    # If no coordinate pattern is provided, use the default one
    if coordinate_pattern is None:
        coordinate_pattern = re.compile(r'(-?\d+\.\d+),\s*(-?\d+\.\d+)')

    try:
        # Get channel entity
        channel = await client.get_entity(channel_id)

        # Iterate through messages
        async for message in client.iter_messages(channel, reverse=True, offset_date=date_limit):
            # Check if message contains coordinates
            if message.message:
                message_text = str(message.message)

                # Two options for coordinate extraction:

                # Option 1: Using the tom-bullock approach with regex findall
                coordinates_matches = coordinate_pattern.findall(message_text)

                # Option 2: Using the existing extract_coordinates function
                # This will be used if Option 1 doesn't find any coordinates
                if not coordinates_matches:
                    coordinates = extract_coordinates(message_text)
                    if coordinates:
                        coordinates_matches = [coordinates]

                # Process any found coordinates
                for coordinates in coordinates_matches:
                    if isinstance(coordinates, tuple) and len(coordinates) == 2:
                        latitude, longitude = coordinates

                        # Add data to lists
                        message_ids.append(message.id)
                        message_texts.append(message_text)

                        # Determine media type
                        if message.media:
                            if isinstance(message.media, MessageMediaPhoto):
                                media_types.append('photo')
                            elif isinstance(message.media, MessageMediaDocument):
                                media_types.append('video/mp4')
                            else:
                                media_types.append('other_media')
                        else:
                            media_types.append('text')

                        # Format date
                        message_date = message.date
                        formatted_date = message_date.strftime("%Y-%m-%d")
                        dates.append(formatted_date)

                        # Format source URL
                        if hasattr(channel, 'username') and channel.username:
                            source = f't.me/{channel.username}/{message.id}'
                        else:
                            source = f't.me/c/{channel.id}/{message.id}'
                        sources.append(source)

                        # Add coordinates
                        latitudes.append(latitude)
                        longitudes.append(longitude)

    except Exception as e:
        logging.error(f"Error scraping channel {channel_id}: {e}")

    return message_ids, message_texts, media_types, dates, sources, latitudes, longitudes


def channel_scraper(channel_links, date_limit, output_path, api_id=None, api_hash=None,
                    session_name="simple_scraper", kml_output_path=None, kmz_output_path=None):
    """
    Scrape Telegram channels for coordinates and save results to CSV.

    This is a simplified interface based on tom-bullock's contribution,
    which allows for quick scraping of coordinates from channels.

    Args:
        channel_links (str or list): Channel username(s) or ID(s)
        date_limit (str): Cut-off date in YYYY-MM-DD format
        output_path (str): Path where to save the CSV file
        api_id (int, optional): Telegram API ID (can be set via environment)
        api_hash (str, optional): Telegram API hash (can be set via environment)
        session_name (str, optional): Name for the Telegram session
        kml_output_path (str, optional): Path to save a KML export.
        kmz_output_path (str, optional): Path to save a KMZ export.

    Returns:
        pandas.DataFrame: DataFrame with extracted coordinates
    """
    # Convert date_limit to datetime object
    try:
        date_limit = datetime.datetime.strptime(date_limit, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD format.")
        return None

    # Get API credentials
    if api_id is None:
        api_id = os.environ.get('TELEGRAM_API_ID')
        if not api_id:
            raise ValueError(
                "Telegram API ID not provided. Set it via the api_id parameter or TELEGRAM_API_ID environment variable.")
        api_id = int(api_id)

    if api_hash is None:
        api_hash = os.environ.get('TELEGRAM_API_HASH')
        if not api_hash:
            raise ValueError(
                "Telegram API hash not provided. Set it via the api_hash parameter or TELEGRAM_API_HASH environment variable.")

    # Regular expression for coordinates
    coordinate_pattern = re.compile(r'(-?\d+\.\d+),\s*(-?\d+\.\d+)')

    # Initialize data lists
    message_ids = []
    message_texts = []
    media_types = []
    dates = []
    sources = []
    latitudes = []
    longitudes = []

    # Ensure channel_links is a list
    if not isinstance(channel_links, list):
        channel_links = [channel_links]

    # Function to run in the event loop
    async def main():
        # Create client
        client = TelegramClient(session_name, api_id, api_hash)
        await client.start()

        print(f"Connected to Telegram. Scraping {len(channel_links)} channels...")

        # Process each channel
        for channel_id in channel_links:
            print(f"Scraping channel: {channel_id}")
            result = await scrape_channel(client, channel_id, date_limit, coordinate_pattern)

            # Append results to main lists
            message_ids.extend(result[0])
            message_texts.extend(result[1])
            media_types.extend(result[2])
            dates.extend(result[3])
            sources.extend(result[4])
            latitudes.extend(result[5])
            longitudes.extend(result[6])

            print(f"Found {len(result[0])} coordinates in channel {channel_id}")

        # Disconnect when done
        await client.disconnect()

    # Run the async function
    with TelegramClient(session_name, api_id, api_hash) as client:
        client.loop.run_until_complete(main())

    # Create DataFrame from collected data
    df = pd.DataFrame({
        'message_id': message_ids,
        'message_content': message_texts,
        'message_media_type': media_types,
        'message_published_at': dates,
        'message_source': sources,
        'latitude': latitudes,
        'longitude': longitudes
    })

    # Save to CSV and optional geospatial formats
    if not df.empty:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"Successfully saved {len(df)} coordinates to {output_path}")

        if kml_output_path:
            if save_dataframe_to_kml(df, kml_output_path):
                print(f"Successfully saved KML to {kml_output_path}")

        if kmz_output_path:
            if save_dataframe_to_kmz(df, kmz_output_path):
                print(f"Successfully saved KMZ to {kmz_output_path}")
    else:
        print("No coordinates found.")

    return df

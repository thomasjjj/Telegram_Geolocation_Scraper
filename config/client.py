import logging
from telethon import TelegramClient
from src.coordinates import extract_coordinates


class TelegramCoordinatesClient:
    """Client for extracting coordinates from Telegram messages."""

    def __init__(self, api_id, api_hash, session_name="session_name"):
        """
        Initialize the Telegram client.

        Args:
            api_id (int): Telegram API ID
            api_hash (str): Telegram API hash
            session_name (str, optional): Session name for Telegram client
        """
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.total_coordinates_found = 0

    async def start(self):
        """Start the Telegram client."""
        try:
            await self.client.start()
            logging.info("Telegram client started successfully.")
            return True
        except Exception as e:
            logging.error(f"Failed to start the Telegram client: {e}")
            return False

    async def disconnect(self):
        """Disconnect the Telegram client."""
        try:
            await self.client.disconnect()
            logging.info("Telegram client disconnected successfully.")
        except Exception as e:
            logging.error(f"Error disconnecting the client: {e}")

    async def search_channel(self, channel_entity, search_terms, writer):
        """
        Search a specific channel for coordinates.

        Args:
            channel_entity: Telegram channel or chat entity
            search_terms (list): List of search terms to filter messages
            writer: CSV writer instance to write found coordinates

        Returns:
            int: Number of coordinates found
        """
        coordinates_found = 0

        for search_term in search_terms:
            logging.info(f"Searching for term '{search_term}' in {channel_entity}")
            try:
                async for message in self.client.iter_messages(channel_entity, search=search_term):
                    if message.text:
                        coordinates = extract_coordinates(message.text)
                        if coordinates:
                            latitude, longitude = coordinates
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

                            writer.writerow(row)
                            coordinates_found += 1
                            self.total_coordinates_found += 1
                            logging.info(
                                f"Total: {self.total_coordinates_found} ¦ Coordinates found and written to CSV: {latitude}, {longitude}")
            except Exception as e:
                logging.error(f"An error occurred while searching for term '{search_term}' in {channel_entity}: {e}")

        return coordinates_found

    async def search_all_chats(self, search_terms, writer):
        """
        Search all available chats for coordinates.

        Args:
            search_terms (list): List of search terms to filter messages
            writer: CSV writer instance to write found coordinates

        Returns:
            int: Number of coordinates found
        """
        total_found = 0

        try:
            dialogs = await self.client.get_dialogs()
            for dialog in dialogs:
                logging.info(f"Searching in: {dialog.name}")
                found = await self.search_channel(dialog.entity, search_terms, writer)
                total_found += found
        except Exception as e:
            logging.error(f"Error retrieving dialogs: {e}")

        return total_found

    async def get_entity(self, channel_identifier):
        """
        Get a Telegram entity by username or ID.

        Args:
            channel_identifier (str): Channel username or ID

        Returns:
            Entity or None: The Telegram entity if found, None otherwise
        """
        try:
            entity = await self.client.get_entity(channel_identifier)
            return entity
        except Exception as e:
            logging.error(f"Could not find a channel or group with the identifier '{channel_identifier}'. Error: {e}")
            return None
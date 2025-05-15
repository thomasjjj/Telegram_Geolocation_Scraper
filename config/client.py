import logging
import time
from telethon import TelegramClient
from coordinates import extract_coordinates


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
        self.total_messages_processed = 0
        self.start_time = None

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

    def _get_elapsed_time(self):
        """Get elapsed time since search started in a readable format."""
        if not self.start_time:
            return "0s"

        elapsed = time.time() - self.start_time
        if elapsed < 60:
            return f"{elapsed:.1f}s"
        elif elapsed < 3600:
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            return f"{minutes}m {seconds}s"
        else:
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            return f"{hours}h {minutes}m"

    def _log_progress(self, message_count, coordinate_count, is_final=False):
        """Log progress information."""
        elapsed = self._get_elapsed_time()

        if is_final:
            logging.info(
                f"Search completed in {elapsed}: Processed {message_count} messages, "
                f"found {coordinate_count} coordinates"
            )
        else:
            logging.info(
                f"Progress [{elapsed}]: Processed {message_count} messages, "
                f"found {coordinate_count} coordinates"
            )

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
        messages_processed = 0
        self.start_time = time.time()
        channel_name = getattr(channel_entity, 'title', str(channel_entity.id)) if hasattr(channel_entity,
                                                                                           'id') else str(
            channel_entity)

        logging.info(f"Starting search in channel: {channel_name}")

        for search_term in search_terms:
            logging.info(f"Searching for term '{search_term}' in {channel_name}")
            try:
                progress_interval = 100  # Log progress every 100 messages
                async for message in self.client.iter_messages(channel_entity, search=search_term):
                    messages_processed += 1
                    self.total_messages_processed += 1

                    if messages_processed % progress_interval == 0:
                        self._log_progress(messages_processed, coordinates_found)

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
                                f"[{self._get_elapsed_time()}] Coordinates found ({self.total_coordinates_found}): {latitude}, {longitude} - {url}")
            except Exception as e:
                logging.error(f"An error occurred while searching for term '{search_term}' in {channel_name}: {e}")

        self._log_progress(messages_processed, coordinates_found, is_final=True)
        logging.info(
            f"Completed search in channel {channel_name}: Found {coordinates_found} coordinates in {messages_processed} messages")
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
        total_chats = 0
        chats_processed = 0
        self.start_time = time.time()

        try:
            logging.info("Retrieving list of all accessible chats...")
            dialogs = await self.client.get_dialogs()
            total_chats = len(dialogs)
            logging.info(f"Found {total_chats} accessible chats to search")

            for dialog in dialogs:
                chats_processed += 1
                chat_name = dialog.name if dialog.name else f"Chat {dialog.id}"

                percentage = (chats_processed / total_chats) * 100
                logging.info(
                    f"Progress: {percentage:.1f}% - Searching chat {chats_processed}/{total_chats}: {chat_name}")

                found = await self.search_channel(dialog.entity, search_terms, writer)
                total_found += found

                logging.info(
                    f"Chat {chats_processed}/{total_chats} complete - Running total: {total_found} coordinates found")

        except Exception as e:
            logging.error(f"Error retrieving dialogs: {e}")

        elapsed = self._get_elapsed_time()
        logging.info(
            f"Search completed in {elapsed}: Searched {chats_processed} chats, processed {self.total_messages_processed} messages, found {total_found} coordinates")
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
            logging.info(f"Resolving entity: {channel_identifier}")
            entity = await self.client.get_entity(channel_identifier)
            entity_name = getattr(entity, 'title', str(entity.id)) if hasattr(entity, 'id') else str(entity)
            logging.info(f"Entity resolved: {entity_name}")
            return entity
        except Exception as e:
            logging.error(f"Could not find a channel or group with the identifier '{channel_identifier}'. Error: {e}")
            return None
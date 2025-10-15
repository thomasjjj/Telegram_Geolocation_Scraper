import asyncio
import logging
import time
import sys
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import FloodWaitError
from src.coordinates import extract_coordinates

try:
    from colorama import init, Fore, Style, Cursor

    colorama_available = True
    init(autoreset=True)  # Initialize colorama
except ImportError:
    colorama_available = False


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
        self.last_status_update = 0
        self.status_update_interval = 0.5  # Update status every 0.5 seconds
        self.last_count = 0  # For calculating processing rate

        # Last coordinate information
        self.last_coordinate_line = None
        self.display_initialized = False

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

    def _get_processing_rate(self):
        """Calculate the messages processing rate (messages per second)."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.total_messages_processed / elapsed
        return 0

    def _format_coordinate_line(self, latitude, longitude, url):
        """Create a formatted coordinate line for console output."""
        if colorama_available:
            return f"{Fore.YELLOW}📍 {latitude}, {longitude} {Fore.BLUE}• {url}{Style.RESET_ALL}"
        return f"📍 {latitude}, {longitude} • {url}"

    def _print_coordinate(self, latitude, longitude, url):
        """Print a coordinate to the terminal and prepare display for refresh."""
        coordinate_line = self._format_coordinate_line(latitude, longitude, url)
        print(coordinate_line)
        self.last_coordinate_line = coordinate_line
        # Ensure the next progress update prints on a new line below the coordinate history
        self.display_initialized = False

    def _update_progress_display(self, coordinates_found, force=False, latest_coordinate=None):
        """Update the progress display with current stats."""
        current_time = time.time()
        # Only update at certain intervals to avoid excessive screen updates
        if not force and (current_time - self.last_status_update < self.status_update_interval):
            return

        # Calculate processing rate since last update
        time_since_last = current_time - self.last_status_update
        msgs_since_last = self.total_messages_processed - self.last_count

        # Calculate current rate
        current_rate = msgs_since_last / time_since_last if time_since_last > 0 else 0

        # Calculate overall rate
        overall_rate = self._get_processing_rate()

        # Update for next calculation
        self.last_count = self.total_messages_processed
        self.last_status_update = current_time

        # Create progress status line
        if colorama_available:
            progress_status = (
                f"{Fore.CYAN}Progress: {Fore.GREEN}{self._get_elapsed_time()} {Style.RESET_ALL}| "
                f"{Fore.CYAN}Messages: {Fore.GREEN}{self.total_messages_processed} {Style.RESET_ALL}| "
                f"{Fore.CYAN}Coordinates: {Fore.GREEN}{self.total_coordinates_found} {Style.RESET_ALL}| "
                f"{Fore.CYAN}Rate: {Fore.GREEN}{current_rate:.1f} msg/s {Style.RESET_ALL}| "
                f"{Fore.CYAN}Avg: {Fore.GREEN}{overall_rate:.1f} msg/s{Style.RESET_ALL}"
            )
        else:
            progress_status = (
                f"Progress: {self._get_elapsed_time()} | "
                f"Messages: {self.total_messages_processed} | "
                f"Coordinates: {self.total_coordinates_found} | "
                f"Rate: {current_rate:.1f} msg/s | "
                f"Avg: {overall_rate:.1f} msg/s"
            )

        # Update coordinate line if we have a new one
        if latest_coordinate:
            latitude, longitude, url = latest_coordinate
            self.last_coordinate_line = self._format_coordinate_line(latitude, longitude, url)

        # If we already initialized the display, move cursor up to rewrite the progress line
        if self.display_initialized and colorama_available:
            sys.stdout.write(Cursor.UP(1) + '\r')
            sys.stdout.write('\033[K')

        # Print the status line
        print(progress_status)

        # Mark display as initialized
        self.display_initialized = True

    @staticmethod
    def _format_wait_duration(seconds):
        """Return a human-readable wait duration string."""
        if seconds >= 3600:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
        if seconds >= 60:
            minutes = int(seconds // 60)
            remaining_seconds = int(seconds % 60)
            return f"{minutes}m {remaining_seconds}s"
        return f"{int(seconds)}s"

    async def _iter_messages_with_retry(self, channel_entity, search_term):
        """Iterate over messages while handling Telegram flood wait errors."""
        offset_id = 0

        while True:
            try:
                async for message in self.client.iter_messages(
                    channel_entity,
                    search=search_term,
                    offset_id=offset_id
                ):
                    yield message
                    offset_id = message.id
                break
            except FloodWaitError as e:
                wait_seconds = max(e.seconds, 0)
                sleep_duration = wait_seconds + 1
                wait_message = (
                    f"Flood wait encountered while searching for '{search_term}'. "
                    f"Sleeping for {self._format_wait_duration(sleep_duration)}."
                )
                logging.warning(wait_message)
                print(wait_message)
                await asyncio.sleep(sleep_duration)

    def _log_progress(self, message_count, coordinate_count, is_final=False):
        """Log progress information."""
        elapsed = self._get_elapsed_time()

        if is_final:
            # Add newlines for clean separation before final message
            if self.display_initialized:
                print("\n")
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
        self.last_status_update = self.start_time
        self.last_count = 0
        self.last_coordinate_line = None
        self.display_initialized = False

        channel_name = getattr(channel_entity, 'title', str(channel_entity.id)) if hasattr(channel_entity,
                                                                                           'id') else str(
            channel_entity)

        logging.info(f"Starting search in channel: {channel_name}")
        print(f"Starting search in channel: {channel_name}")
        print("Live progress will show below - press Ctrl+C to cancel\n")

        # Display initial status
        self._update_progress_display(coordinates_found, force=True)

        for search_term in search_terms:
            logging.info(f"Searching for term '{search_term}' in {channel_name}")
            try:
                progress_interval = 100  # Log progress every 100 messages
                async for message in self._iter_messages_with_retry(channel_entity, search_term):
                    messages_processed += 1
                    self.total_messages_processed += 1

                    if messages_processed % progress_interval == 0:
                        self._log_progress(messages_processed, coordinates_found)

                    if message.text:
                        coordinates = extract_coordinates(message.text)
                        if coordinates:
                            latitude, longitude = coordinates
                            username = message.chat.username if hasattr(message.chat,
                                                                        'username') and message.chat.username else f"c/{message.chat.id}"
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

                            # Pass the latest coordinate info to update display
                            latest_coordinate = (latitude, longitude, url)

                            # Print coordinate and update display
                            self._print_coordinate(latitude, longitude, url)
                            self._update_progress_display(
                                coordinates_found,
                                force=True,
                                latest_coordinate=latest_coordinate
                            )

                            # Log to file but don't output to console (our display handles it)
                            logging.info(
                                f"[{self._get_elapsed_time()}] Coordinates found ({self.total_coordinates_found}): {latitude}, {longitude} - {url}")
                    else:
                        # Update display even if no coordinates found
                        self._update_progress_display(coordinates_found)

            except Exception as e:
                logging.error(f"An error occurred while searching for term '{search_term}' in {channel_name}: {e}")

        # Final update with force=True to ensure it's displayed
        self._update_progress_display(coordinates_found, force=True)

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
        self.last_status_update = self.start_time
        self.last_count = 0
        self.last_coordinate_line = None
        self.display_initialized = False

        try:
            logging.info("Retrieving list of all accessible chats...")
            print("Retrieving list of all accessible chats...")
            dialogs = await self.client.get_dialogs()
            total_chats = len(dialogs)
            logging.info(f"Found {total_chats} accessible chats to search")
            print(f"Found {total_chats} accessible chats to search")
            print("Live progress will show below - press Ctrl+C to cancel\n")

            # Display initial status
            self._update_progress_display(total_found, force=True)

            for dialog in dialogs:
                chats_processed += 1
                chat_name = dialog.name if dialog.name else f"Chat {dialog.id}"

                percentage = (chats_processed / total_chats) * 100
                logging.info(
                    f"Progress: {percentage:.1f}% - Searching chat {chats_processed}/{total_chats}: {chat_name}")

                # Add a newline before processing a new chat for visibility in logs
                if self.display_initialized:
                    print("\n\n")
                    self.display_initialized = False

                print(f"Searching chat {chats_processed}/{total_chats}: {chat_name}")

                found = await self.search_channel(dialog.entity, search_terms, writer)
                total_found += found

                logging.info(
                    f"Chat {chats_processed}/{total_chats} complete - Running total: {total_found} coordinates found")
                print(f"Chat {chats_processed}/{total_chats} complete - Running total: {total_found} coordinates found")

        except Exception as e:
            logging.error(f"Error retrieving dialogs: {e}")

        elapsed = self._get_elapsed_time()
        # Final summary for clarity
        print(f"\nSearch completed in {elapsed}:")
        print(f"Searched {chats_processed} chats, processed {self.total_messages_processed} messages")
        print(f"Found {total_found} coordinates")

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

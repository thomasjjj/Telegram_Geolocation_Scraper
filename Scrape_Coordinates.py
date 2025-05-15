import asyncio
import argparse
import logging
import os
import sys
import time
from config.config import Config
from src.client import TelegramCoordinatesClient
from src.export import CoordinatesWriter
from src.json_processor import process_telegram_json, save_dataframe_to_csv


async def search_telegram_channel(client, channel_identifier, search_terms, csv_file):
    """
    Search a specific Telegram channel for coordinates.

    Args:
        client (TelegramCoordinatesClient): Telegram client
        channel_identifier (str): Channel username or ID
        search_terms (list): List of search terms
        csv_file (str): Path to the CSV file

    Returns:
        int: Number of coordinates found
    """
    print(f"Resolving channel: {channel_identifier}")
    entity = await client.get_entity(channel_identifier)
    if not entity:
        logging.error(f"Could not find channel: {channel_identifier}")
        print(f"Error: Could not find a channel or group with the identifier '{channel_identifier}'.")
        return 0

    print(f"Starting search in channel. Results will be saved to: {csv_file}")
    print("Progress will be shown in the log and terminal. Press Ctrl+C to cancel.")

    start_time = time.time()
    with CoordinatesWriter(csv_file) as writer:
        found = await client.search_channel(entity, search_terms, writer)

    # Format elapsed time
    elapsed = time.time() - start_time
    if elapsed < 60:
        time_str = f"{elapsed:.1f} seconds"
    elif elapsed < 3600:
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        time_str = f"{minutes} minutes {seconds} seconds"
    else:
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        time_str = f"{hours} hours {minutes} minutes"

    print(f"\nSearch completed in {time_str}!")
    print(f"Found {found} coordinates in channel {channel_identifier}")
    print(f"Results saved to: {csv_file}")

    return found


async def search_all_telegram_chats(client, search_terms, csv_file):
    """
    Search all accessible Telegram chats for coordinates.

    Args:
        client (TelegramCoordinatesClient): Telegram client
        search_terms (list): List of search terms
        csv_file (str): Path to the CSV file

    Returns:
        int: Number of coordinates found
    """
    print(f"Starting search in all accessible chats. Results will be saved to: {csv_file}")
    print("This may take a long time depending on the number of chats and messages.")
    print("Progress will be shown in the log and terminal. Press Ctrl+C to cancel.")

    start_time = time.time()
    with CoordinatesWriter(csv_file) as writer:
        found = await client.search_all_chats(search_terms, writer)

    # Format elapsed time
    elapsed = time.time() - start_time
    if elapsed < 60:
        time_str = f"{elapsed:.1f} seconds"
    elif elapsed < 3600:
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        time_str = f"{minutes} minutes {seconds} seconds"
    else:
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        time_str = f"{hours} hours {minutes} minutes"

    print(f"\nSearch completed in {time_str}!")
    print(f"Found {found} coordinates across all accessible chats")
    print(f"Results saved to: {csv_file}")

    return found


async def telegram_search_main(config, option, channel_identifier=None):
    """
    Main function for Telegram search operations.

    Args:
        config (Config): Configuration object
        option (str): Search option ('1' for specific channel, '2' for all chats)
        channel_identifier (str, optional): Channel identifier for option '1'

    Returns:
        int: Number of coordinates found
    """
    print("\n=== Telegram Coordinates Scraper ===\n")
    print("Initializing search...")

    api_id, api_hash = config.get_telegram_credentials()
    session_name = config.get_session_name()
    search_terms = config.get_search_terms()
    csv_file = config.get_output_file()

    # Ensure the results folder exists
    results_folder = config.get_results_folder()
    print(f"Results will be saved to folder: {results_folder}")

    # Create a more informative console handler for progress
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)

    # Add the handler to the root logger if it's not already there
    root_logger = logging.getLogger()
    handlers_exist = False
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handlers_exist = True
            break

    if not handlers_exist:
        root_logger.addHandler(console)

    print(f"Connecting to Telegram with session: {session_name}")
    client = TelegramCoordinatesClient(api_id, api_hash, session_name)
    if not await client.start():
        print("Failed to connect to Telegram. Please check your credentials and internet connection.")
        return 0

    print("Successfully connected to Telegram!")

    try:
        if option == '1':
            if not channel_identifier:
                channel_identifier = input("Enter the username (e.g., @channelname) or ID of the channel to search: ")
            found = await search_telegram_channel(client, channel_identifier, search_terms, csv_file)
        elif option == '2':
            found = await search_all_telegram_chats(client, search_terms, csv_file)
        else:
            logging.warning(f"Invalid option: {option}")
            print("Invalid option selected.")
            found = 0
    except KeyboardInterrupt:
        print("\nSearch cancelled by user.")
        logging.info("Search cancelled by user.")
        found = 0
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        logging.exception("An error occurred during search")
        found = 0
    finally:
        print("Disconnecting from Telegram...")
        await client.disconnect()
        print("Disconnected.")

    return found


def process_json_export(config):
    """
    Process a JSON export file from Telegram.

    Args:
        config (Config): Configuration object

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n=== JSON Export Processor ===\n")

    while True:
        print("\nPlease provide the following information:")
        csv_file_name = input("Output CSV file name (e.g., channel_name_coordinates): ")
        json_file_path = input("Full path to the JSON file you want to process: ")

        # Validate JSON file exists
        if not os.path.isfile(json_file_path):
            print(f"Error: The file '{json_file_path}' does not exist or is not accessible.")
            continue

        # Use the results folder from config for the output path
        results_folder = config.get_results_folder()
        csv_save_path = os.path.join(results_folder, f"{csv_file_name}.csv")

        # Notify the user where the file will be saved
        print(f"\nThe CSV file will be saved to: {csv_save_path}")

        post_link_base = input("Base URL for the post links (e.g., https://t.me/WarArchive_ua/): ")

        print(f"\nProcessing JSON file: {json_file_path}")
        print("This may take some time depending on the size of the file.")
        print("Progress will be shown in the log and terminal. Press Ctrl+C to cancel.")

        try:
            start_time = time.time()
            df = process_telegram_json(json_file_path, post_link_base)

            if df.empty:
                print("\nNo coordinates found in the JSON file.")
            else:
                print(f"\nFound {len(df)} messages with coordinates!")
                print(f"Saving results to: {csv_save_path}")
                success = save_dataframe_to_csv(df, csv_save_path)

                if success:
                    # Format elapsed time
                    elapsed = time.time() - start_time
                    if elapsed < 60:
                        time_str = f"{elapsed:.1f} seconds"
                    elif elapsed < 3600:
                        minutes = int(elapsed // 60)
                        seconds = int(elapsed % 60)
                        time_str = f"{minutes} minutes {seconds} seconds"
                    else:
                        hours = int(elapsed // 3600)
                        minutes = int((elapsed % 3600) // 60)
                        time_str = f"{hours} hours {minutes} minutes"

                    print(f"\nProcessing completed in {time_str}!")
                    print(f"CSV file saved as: {csv_save_path}")
                    print(f"Found {len(df)} messages with coordinates")
                else:
                    print("\nFailed to save results to CSV file.")

        except KeyboardInterrupt:
            print("\nProcessing cancelled by user.")
            logging.info("JSON processing cancelled by user.")
        except Exception as e:
            print(f"\nAn error occurred: {str(e)}")
            logging.exception("An error occurred during JSON processing")

        try:
            another_file = input("\nDo you want to process another file? (yes/no): ").strip().lower()
            if another_file != 'yes':
                break
        except KeyboardInterrupt:
            print("\nExiting JSON processor.")
            break

    return True


def parse_args():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Scrape coordinates from Telegram messages")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--mode", choices=["channel", "all", "json"],
                        help="Search mode (channel, all, or json)")
    parser.add_argument("--channel", help="Channel identifier (for channel mode)")
    parser.add_argument("--output", help="Output CSV file path")
    parser.add_argument("--results-folder", help="Folder to store all CSV outputs")
    parser.add_argument("--json-file", help="JSON file to process (for json mode)")
    parser.add_argument("--post-link-base", help="Base URL for post links (for json mode)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    return parser.parse_args()


def interactive_mode():
    """
    Run the script in interactive mode.

    Returns:
        int: Exit code
    """
    print("\n=== Telegram Coordinates Scraper ===\n")
    print("Please select a search option:")
    print("1 - Search a specific channel by username or ID")
    print("2 - Search all channels, groups, and chats")
    print("3 - Process a JSON export file to retrieve coordinates")
    print("\nType Ctrl+C at any time to cancel the operation.")

    try:
        option = input("\nEnter your choice (1, 2, or 3): ").strip()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        return 0

    # Initialize config
    config = Config()
    config.setup_logging()

    if option in ['1', '2']:
        try:
            asyncio.run(telegram_search_main(config, option))
            return 0
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            return 0
        except Exception as e:
            logging.critical(f"An unhandled exception occurred: {e}")
            print(f"\nA critical error occurred: {str(e)}")
            return 1
    elif option == '3':
        try:
            process_json_export(config)
            return 0
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            return 0
        except Exception as e:
            logging.critical(f"An unhandled exception occurred: {e}")
            print(f"\nA critical error occurred: {str(e)}")
            return 1
    else:
        print("Invalid input. Please run the script again and enter 1, 2, or 3.")
        return 1


def main():
    """
    Main entry point for the command-line interface.

    Returns:
        int: Exit code
    """
    # Handle KeyboardInterrupt gracefully
    try:
        args = parse_args()

        # Set up verbose logging if requested
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug("Verbose logging enabled")

        # If no args were provided, run in interactive mode
        if not any(var for var in vars(args).values() if var not in [False, None]):
            return interactive_mode()

        # Initialize config
        config = Config(args.config)
        config.setup_logging()

        # Override config with command-line arguments
        if args.output:
            config.update_config('output', 'csv_file', args.output)

        if args.results_folder:
            config.update_config('output', 'results_folder', args.results_folder)

        # Ensure the results folder exists
        results_folder = config.get_results_folder()
        os.makedirs(results_folder, exist_ok=True)
        logging.info(f"Using results folder: {results_folder}")

        try:
            if args.mode == "channel":
                if not args.channel:
                    print("Error: --channel is required for channel mode")
                    return 1
                asyncio.run(telegram_search_main(config, '1', args.channel))
            elif args.mode == "all":
                asyncio.run(telegram_search_main(config, '2'))
            elif args.mode == "json":
                if not args.json_file:
                    print("Error: --json-file is required for json mode")
                    return 1

                if not args.post_link_base:
                    print("Error: --post-link-base is required for json mode")
                    return 1

                print(f"Processing JSON file: {args.json_file}")
                df = process_telegram_json(args.json_file, args.post_link_base)
                csv_file = config.get_output_file()
                success = save_dataframe_to_csv(df, csv_file)

                if success:
                    print(f"Found {len(df)} messages with coordinates")
                    print(f"Results saved to: {csv_file}")
                else:
                    print("Failed to save results to CSV")
                    return 1
            else:
                print("Error: --mode must be one of: channel, all, json")
                return 1

            return 0
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            logging.info("Operation cancelled by user.")
            return 0
        except Exception as e:
            logging.critical(f"An unhandled exception occurred: {e}")
            print(f"\nA critical error occurred: {str(e)}")
            return 1
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
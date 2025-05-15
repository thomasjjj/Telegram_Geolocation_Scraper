import asyncio
import argparse
import logging
import os
import sys
from config.config import Config
from config.client import TelegramCoordinatesClient
from src.export import CoordinatesWriter
from src.json_processor import process_telegram_json, save_dataframe_to_csv
from dotenv import load_dotenv, find_dotenv, set_key


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
    entity = await client.get_entity(channel_identifier)
    if not entity:
        logging.error(f"Could not find channel: {channel_identifier}")
        print(f"Error: Could not find a channel or group with the identifier '{channel_identifier}'.")
        return 0

    with CoordinatesWriter(csv_file) as writer:
        return await client.search_channel(entity, search_terms, writer)


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
    with CoordinatesWriter(csv_file) as writer:
        return await client.search_all_chats(search_terms, writer)


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
    api_id, api_hash = config.get_telegram_credentials()
    session_name = config.get_session_name()
    search_terms = config.get_search_terms()
    csv_file = config.get_output_file()

    client = TelegramCoordinatesClient(api_id, api_hash, session_name)
    if not await client.start():
        return 0

    try:
        if option == '1':
            if not channel_identifier:
                channel_identifier = input("Enter the username (e.g., @channelname) or ID of the channel to search: ")
            found = await search_telegram_channel(client, channel_identifier, search_terms, csv_file)
        elif option == '2':
            found = await search_all_telegram_chats(client, search_terms, csv_file)
        else:
            logging.warning(f"Invalid option: {option}")
            found = 0
    finally:
        await client.disconnect()

    return found


def process_json_export(config):
    """
    Process a JSON export file from Telegram.

    Args:
        config (Config): Configuration object

    Returns:
        bool: True if successful, False otherwise
    """
    while True:
        csv_file_name = input("Please enter the name for the output CSV file (e.g., channel_name_coordinates): ")
        json_file_path = input("Please enter the full path to the JSON file you want to process: ")

        # Use the results folder from config for the output path
        results_folder = config.get_results_folder()
        csv_save_path = os.path.join(results_folder, f"{csv_file_name}.csv")

        # Notify the user where the file will be saved
        print(f"The CSV file will be saved to: {csv_save_path}")

        post_link_base = input("Please enter the base URL for the post links (e.g., https://t.me/WarArchive_ua/): ")

        df = process_telegram_json(json_file_path, post_link_base)
        success = save_dataframe_to_csv(df, csv_save_path)

        if success:
            print(f"CSV file saved as {csv_save_path}")
            print(f"Found {len(df)} messages with coordinates")

        another_file = input("Do you want to process another file? (yes/no): ").strip().lower()
        if another_file != 'yes':
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
    return parser.parse_args()


def interactive_mode():
    """
    Run the script in interactive mode.

    Returns:
        int: Exit code
    """
    print("Please select a search option:")
    print("1 - Search a specific channel by username or ID")
    print("2 - Search all channels, groups, and chats")
    print("3 - Process a JSON export file to retrieve coordinates")

    option = input("Enter your choice (1, 2, or 3): ").strip()

    # Initialize config
    config = Config()
    config.setup_logging()

    if option in ['1', '2']:
        try:
            asyncio.run(telegram_search_main(config, option))
            return 0
        except Exception as e:
            logging.critical(f"An unhandled exception occurred: {e}")
            return 1
    elif option == '3':
        try:
            process_json_export(config)
            return 0
        except Exception as e:
            logging.critical(f"An unhandled exception occurred: {e}")
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
    args = parse_args()

    # If no args were provided, run in interactive mode
    if not any(vars(args).values()):
        return interactive_mode()

    # Initialize config
    config = Config(args.config)
    config.setup_logging()

    # Override config with command-line arguments
    if args.output:
        config.update_config('output', 'csv_file', args.output)

    if args.results_folder:
        config.update_config('output', 'results_folder', args.results_folder)
        # Ensure the folder exists
        os.makedirs(args.results_folder, exist_ok=True)

    try:
        if args.mode == "channel":
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

            df = process_telegram_json(args.json_file, args.post_link_base)
            save_dataframe_to_csv(df, config.get_output_file())
            print(f"Found {len(df)} messages with coordinates")
        else:
            print("Error: --mode must be one of: channel, all, json")
            return 1

        return 0
    except Exception as e:
        logging.critical(f"An unhandled exception occurred: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
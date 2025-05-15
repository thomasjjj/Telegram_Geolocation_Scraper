import json
import logging
import os
import time
import pandas as pd
from coordinates import extract_coordinates


def process_telegram_json(json_file_path, post_link_base):
    """
    Process a Telegram JSON export file to extract coordinates.

    Args:
        json_file_path (str): Path to the JSON file
        post_link_base (str): Base URL for post links

    Returns:
        pandas.DataFrame: DataFrame with extracted coordinates
    """
    messages_with_coordinates = []
    start_time = time.time()
    messages_processed = 0

    try:
        # Open and load the JSON file
        logging.info(f"Loading JSON file: {json_file_path}")
        with open(json_file_path, 'r', encoding='utf-8') as f:
            telegram_data = json.load(f)

        # Get total number of messages for progress tracking
        total_messages = len(telegram_data.get('messages', []))
        logging.info(f"JSON file loaded. Processing {total_messages} messages")

        # Progress tracking variables
        progress_interval = max(1, int(total_messages / 20))  # Log progress 20 times
        last_progress_time = time.time()
        progress_time_interval = 2.0  # Seconds between progress logs

        # Iterate through each message in the JSON export
        for i, message in enumerate(telegram_data.get('messages', [])):
            messages_processed += 1

            # Log progress at intervals or after specified time has passed
            if (messages_processed % progress_interval == 0) or (
                    time.time() - last_progress_time > progress_time_interval and messages_processed > 1):
                elapsed = time.time() - start_time
                percentage = (messages_processed / total_messages) * 100

                # Calculate estimated time remaining
                if messages_processed > 1:
                    avg_time_per_message = elapsed / messages_processed
                    remaining_messages = total_messages - messages_processed
                    eta = avg_time_per_message * remaining_messages

                    # Format ETA
                    if eta < 60:
                        eta_str = f"{eta:.1f}s"
                    elif eta < 3600:
                        eta_str = f"{int(eta // 60)}m {int(eta % 60)}s"
                    else:
                        eta_str = f"{int(eta // 3600)}h {int((eta % 3600) // 60)}m"

                    logging.info(
                        f"Progress: {percentage:.1f}% - Processed {messages_processed}/{total_messages} messages, "
                        f"found {len(messages_with_coordinates)} coordinates. ETA: {eta_str}")
                else:
                    logging.info(
                        f"Progress: {percentage:.1f}% - Processed {messages_processed}/{total_messages} messages")

                last_progress_time = time.time()

            text_field = str(message.get('text', ''))
            coordinates = extract_coordinates(text_field)

            if coordinates:
                latitude, longitude = coordinates
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
                logging.info(f"Coordinate found: {latitude}, {longitude} in message ID: {post_id}")

        # Create DataFrame
        logging.info(f"Creating DataFrame with {len(messages_with_coordinates)} coordinates")
        df = pd.DataFrame(messages_with_coordinates)

        # Add post link column
        if 'Post ID' in df.columns and not df.empty:
            df['Post Link'] = post_link_base + df['Post ID'].astype(str)

            # Reorder columns
            column_order = [
                'Post Link', 'Post ID', 'Post Date', 'Post Message',
                'Post Type', 'Media Type', 'Latitude', 'Longitude'
            ]
            df = df[column_order]

        # Calculate and log total processing time
        total_time = time.time() - start_time
        if total_time < 60:
            time_str = f"{total_time:.1f} seconds"
        elif total_time < 3600:
            minutes = int(total_time // 60)
            seconds = int(total_time % 60)
            time_str = f"{minutes} minutes {seconds} seconds"
        else:
            hours = int(total_time // 3600)
            minutes = int((total_time % 3600) // 60)
            time_str = f"{hours} hours {minutes} minutes"

        logging.info(
            f"JSON processing completed in {time_str}: Processed {messages_processed} messages, found {len(messages_with_coordinates)} coordinates")
        return df

    except Exception as e:
        logging.error(f"Error processing JSON file: {e}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=[
            'Post Link', 'Post ID', 'Post Date', 'Post Message',
            'Post Type', 'Media Type', 'Latitude', 'Longitude'
        ])


def save_dataframe_to_csv(df, csv_file_path):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): DataFrame to save
        csv_file_path (str): Path to the CSV file

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(csv_file_path) if os.path.dirname(csv_file_path) else '.', exist_ok=True)

        logging.info(f"Saving {len(df)} records to CSV file: {csv_file_path}")
        start_time = time.time()

        df.to_csv(csv_file_path, index=False, encoding='utf-8')

        elapsed = time.time() - start_time
        logging.info(f"DataFrame successfully saved to CSV file in {elapsed:.2f} seconds: {csv_file_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to save DataFrame to CSV: {e}")
        return False
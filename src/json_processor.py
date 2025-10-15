import json
import logging
import os
import time
import pandas as pd
from src.coordinates import extract_coordinates


def _get_elapsed_time(start_time):
    """Get elapsed time since start in a readable format."""
    elapsed = time.time() - start_time
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


def _get_processing_rate(messages_processed, start_time):
    """Calculate the messages processing rate (messages per second)."""
    elapsed = time.time() - start_time
    if elapsed > 0:
        return messages_processed / elapsed
    return 0


def _update_progress_display(messages_processed, coordinates_found, start_time, current_count, total_messages,
                             last_count, last_update_time, force=False):
    """Update the progress display with current stats.

    Args:
        messages_processed (int): Number of messages processed so far.
        coordinates_found (int): Number of coordinate entries found.
        start_time (float): Timestamp when processing started.
        current_count (int): Current message count.
        total_messages (int): Total number of messages to process.
        last_count (int): Message count at previous update.
        last_update_time (float): Timestamp of the previous update.
        force (bool): Ignored, maintained for backward compatibility.

    Returns:
        float: The timestamp of this update for tracking purposes.
    """
    current_time = time.time()
    elapsed = current_time - start_time

    # Calculate percentage
    percentage = (messages_processed / total_messages) * 100 if total_messages > 0 else 0

    # Calculate processing rate since last update using provided timestamp
    time_since_last = current_time - last_update_time if last_update_time else elapsed
    msgs_since_last = current_count - last_count

    # Calculate current rate
    current_rate = msgs_since_last / time_since_last if time_since_last > 0 else 0

    # Calculate overall rate
    overall_rate = _get_processing_rate(messages_processed, start_time)

    # Estimate time remaining
    if overall_rate > 0:
        remaining_messages = total_messages - messages_processed
        eta_seconds = remaining_messages / overall_rate
        if eta_seconds < 60:
            eta = f"{eta_seconds:.1f}s"
        elif eta_seconds < 3600:
            eta_m = int(eta_seconds // 60)
            eta_s = int(eta_seconds % 60)
            eta = f"{eta_m}m {eta_s}s"
        else:
            eta_h = int(eta_seconds // 3600)
            eta_m = int((eta_seconds % 3600) // 60)
            eta = f"{eta_h}h {eta_m}m"
    else:
        eta = "calculating..."

    # Create progress status line
    status = (
        f"\rProgress: {percentage:.1f}% | "
        f"Time: {_get_elapsed_time(start_time)} | "
        f"Messages: {messages_processed}/{total_messages} | "
        f"Coordinates: {coordinates_found} | "
        f"Rate: {current_rate:.1f} msg/s | "
        f"Avg: {overall_rate:.1f} msg/s | "
        f"ETA: {eta}"
    )

    # Print without newline to overwrite the line
    print(status, end='', flush=True)

    return current_time


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
    last_status_update = start_time
    status_update_interval = 0.5  # Update status every 0.5 seconds
    last_count = 0

    try:
        # Open and load the JSON file
        logging.info(f"Loading JSON file: {json_file_path}")
        with open(json_file_path, 'r', encoding='utf-8') as f:
            telegram_data = json.load(f)

        # Get total number of messages for progress tracking
        total_messages = len(telegram_data.get('messages', []))
        logging.info(f"JSON file loaded. Processing {total_messages} messages")

        # Display initial progress
        print(f"Processing {total_messages} messages from JSON file")
        print("Live progress will show below - press Ctrl+C to cancel")
        last_status_update = _update_progress_display(
            0,
            0,
            start_time,
            0,
            total_messages,
            last_count,
            last_status_update,
            force=True,
        )

        # Iterate through each message in the JSON export
        for i, message in enumerate(telegram_data.get('messages', [])):
            messages_processed += 1

            # Update progress display
            current_time = time.time()
            if current_time - last_status_update >= status_update_interval:
                last_status_update = _update_progress_display(
                    messages_processed,
                    len(messages_with_coordinates),
                    start_time,
                    messages_processed,
                    total_messages,
                    last_count,
                    last_status_update,
                )
                last_count = messages_processed

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

        # Final progress update
        last_status_update = _update_progress_display(
            messages_processed,
            len(messages_with_coordinates),
            start_time,
            messages_processed,
            total_messages,
            last_count,
            last_status_update,
            force=True,
        )
        # Add a newline after progress display
        print()

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

    except (OSError, json.JSONDecodeError, ValueError) as e:
        logging.error(f"Error processing JSON file: {e}")
        # Print a newline in case exception occurred during progress display
        print()
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
        print(f"Saving {len(df)} records to CSV file...")
        start_time = time.time()

        # Show a simple progress indicator
        print("Saving...", end="", flush=True)

        df.to_csv(csv_file_path, index=False, encoding='utf-8')

        elapsed = time.time() - start_time
        print(f"\rSave completed in {elapsed:.2f} seconds      ")
        logging.info(f"DataFrame successfully saved to CSV file in {elapsed:.2f} seconds: {csv_file_path}")
        return True
    except (OSError, ValueError, AttributeError) as e:
        logging.error(f"Failed to save DataFrame to CSV: {e}")
        print(f"\rError: Failed to save DataFrame to CSV: {e}      ")
        return False

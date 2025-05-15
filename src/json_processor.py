import json
import logging
import os
import pandas as pd
from src.coordinates import extract_coordinates


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

    try:
        # Open and load the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as f:
            telegram_data = json.load(f)

        # Iterate through each message in the JSON export
        for message in telegram_data.get('messages', []):
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

        # Create DataFrame
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

        logging.info(f"Processed JSON file: {json_file_path}, found {len(messages_with_coordinates)} coordinates")
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

        df.to_csv(csv_file_path, index=False, encoding='utf-8')
        logging.info(f"DataFrame saved to CSV file: {csv_file_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to save DataFrame to CSV: {e}")
        return False
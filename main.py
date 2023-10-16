import json
import pandas as pd
import re
from tkinter import filedialog
from tkinter import Tk


def process_telegram_data(json_file_path, post_link_base):
    # Initialize an empty list to hold messages with coordinates
    messages_with_coordinates = []

    # Regular expression pattern to find latitude and longitude
    coordinate_pattern = re.compile(r'(-?\d+\.\d+),\s*(-?\d+\.\d+)')

    # Load the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as f:
        telegram_data = json.load(f)

    # Iterate through all messages to find those with coordinates
    for message in telegram_data['messages']:
        text_field = str(message.get('text', ''))
        coordinates_match = coordinate_pattern.search(text_field)

        if coordinates_match:
            latitude, longitude = coordinates_match.groups()
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

    # Create a DataFrame and add the Post Link
    df = pd.DataFrame(messages_with_coordinates)
    df['Post Link'] = post_link_base + df['Post ID'].astype(str)

    # Reorder the columns
    column_order = ['Post Link', 'Post ID', 'Post Date', 'Post Message', 'Post Type', 'Media Type', 'Latitude',
                    'Longitude']
    df = df[column_order]

    return df


while True:
    # Step 1: Prompt for CSV File Name
    csv_file_name = input("Please enter the name for the output CSV file (e.g., channel_name_coordinates): ")

    # Step 2: Prompt for JSON File Path
    json_file_path = input("Please enter the full path to the JSON file you want to process: ")

    # Step 3: Prompt for Saving CSV File Path
    csv_save_path = input(
        f"Please enter the full path where you want to save the CSV file (e.g., C:/path/to/save/{csv_file_name}.csv): ")

    # Step 4: Process the Data
    post_link_base = input("Please enter the base URL for the post links (e.g., https://t.me/WarArchive_ua/): ")
    df_messages_with_coordinates = process_telegram_data(json_file_path, post_link_base)

    # Step 5: Save the CSV
    df_messages_with_coordinates.to_csv(csv_save_path, index=False, encoding='utf-8')

    print(f"CSV file saved as {csv_save_path}")

    # Step 6: Repeat or Exit
    another_file = input("Do you want to process another file? (yes/no): ").strip().lower()
    if another_file != 'yes':
        break



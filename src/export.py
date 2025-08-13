import os
import csv
import logging


class CoordinatesWriter:
    """CSV writer for coordinates data."""

    def __init__(self, csv_file_path):
        """
        Initialize the coordinates writer.

        Args:
            csv_file_path (str): Path to the CSV file
        """
        self.csv_file_path = csv_file_path
        # Ensure the directory exists
        os.makedirs(os.path.dirname(csv_file_path) if os.path.dirname(csv_file_path) else '.', exist_ok=True)
        self.file_exists = os.path.isfile(csv_file_path)
        self.file = None
        self.writer = None

    def __enter__(self):
        """Context manager entry - open the CSV file."""
        try:
            self.file = open(self.csv_file_path, 'a', newline='', encoding='utf-8')
            self.writer = csv.writer(self.file)

            # Write header if file doesn't exist
            if not self.file_exists:
                self.writer.writerow([
                    'Post ID',
                    'Channel ID',
                    'Channel/Group Username',
                    'Message Text',
                    'Date',
                    'URL',
                    'Latitude',
                    'Longitude'
                ])
                logging.info(f"Created new CSV file: {self.csv_file_path}")
            else:
                logging.info(f"Appending to existing CSV file: {self.csv_file_path}")

            return self.writer

        except Exception as e:
            logging.error(f"Failed to open CSV file: {e}")
            if self.file:
                self.file.close()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close the CSV file."""
        if self.file:
            self.file.close()

        if exc_type:
            logging.error(f"An error occurred while writing to CSV: {exc_val}")
            return False

        return True


def save_to_csv(data, csv_file_path, headers=None):
    """
    Save data to a CSV file.

    Args:
        data (list): List of rows to write
        csv_file_path (str): Path to the CSV file
        headers (list, optional): Column headers

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(csv_file_path) if os.path.dirname(csv_file_path) else '.', exist_ok=True)

        file_exists = os.path.isfile(csv_file_path)

        with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            if not file_exists and headers:
                writer.writerow(headers)

            for row in data:
                writer.writerow(row)

        logging.info(f"Data saved to CSV file: {csv_file_path}")
        return True

    except Exception as e:
        logging.error(f"Failed to write to CSV file: {e}")
        return False

"""
Example script showing how to use the new channel_scraper function
"""

from src.channel_scraper import channel_scraper

# Example of scraping a single channel (saving results only to the database)
# channel_scraper('WarArchive_ua', '2023-09-01')

# Example of scraping multiple channels and exporting to CSV
channel_scraper(
    ['WarArchive_ua', 'military_u_geo', 'IPHRinvestigates'],
    '2023-09-01',
    output_path='results/multi_channel_coordinates.csv'
)

# Example with explicit API credentials
# api_id = 12345
# api_hash = "sdfgsdfgsdfgsdfgsdfgsdfg"
# channel_scraper('WarArchive_ua', '2023-09-01', output_path='results/custom_api_coordinates.csv', api_id=api_id, api_hash=api_hash)

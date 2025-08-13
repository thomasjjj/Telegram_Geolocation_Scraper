"""
Example script showing how to use the new channel_scraper function
"""

from src.channel_scraper import channel_scraper

# Example of scraping a single channel
# channel_scraper('WarArchive_ua', '2023-09-01', 'results/war_archive_coordinates.csv')

# Example of scraping multiple channels
channel_scraper(
    ['WarArchive_ua', 'military_u_geo', 'IPHRinvestigates'],
    '2023-09-01',
    'results/multi_channel_coordinates.csv'
)

# Example with explicit API credentials
# api_id = 12345
# api_hash = "sdfgsdfgsdfgsdfgsdfgsdfg"
# channel_scraper('WarArchive_ua', '2023-09-01', 'results/custom_api_coordinates.csv', api_id, api_hash)

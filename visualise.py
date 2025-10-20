"""
Telegram Geolocation Scraper - Kepler.gl Visualization
Visualizes coordinates from your Telegram scraper database

Requirements:
pip install keplergl pandas sqlalchemy
"""

from keplergl import KeplerGl
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Your database file (adjust if needed)
DATABASE_FILE = 'telegram_coordinates.db'  # or 'coordinates.db'

# SQL query matching your database schema
SQL_QUERY = """
SELECT 
    c.id,
    c.latitude,
    c.longitude,
    c.coordinate_format,
    c.extraction_confidence,
    c.created_at,
    m.channel_id,
    m.message_id,
    m.message_text,
    m.message_date,
    m.media_type
FROM coordinates c
JOIN messages m ON m.id = c.message_ref
ORDER BY m.message_date DESC
"""

# ============================================================================
# FUNCTIONS
# ============================================================================

def find_database():
    """Find the database file in the current directory or project."""

    # Check common locations
    possible_paths = [
        DATABASE_FILE,
        f'../{DATABASE_FILE}',
        'coordinates.db',
        '../coordinates.db',
    ]

    for path in possible_paths:
        if Path(path).exists():
            print(f"Found database: {path}")
            return path

    # Search for any .db file
    for db_file in Path('.').rglob('*.db'):
        if db_file.name not in ['__pycache__']:
            print(f"Found database: {db_file}")
            return str(db_file)

    return None

def load_data_from_database(db_path):
    """
    Load coordinate data from your Telegram scraper database

    Args:
        db_path: Path to the SQLite database file

    Returns:
        pandas DataFrame with coordinates and message data
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    print(f"\nConnecting to database: {db_path}")
    db_url = f'sqlite:///{db_path}'
    engine = create_engine(db_url)

    print("Executing query...")
    df = pd.read_sql(SQL_QUERY, engine)

    if df.empty:
        print("\n⚠️  No coordinates found in database!")
        print("Make sure you've scraped some Telegram channels with coordinates first.")
        return None

    print(f"✓ Loaded {len(df)} coordinates")

    # Convert date columns to datetime
    if 'message_date' in df.columns:
        df['message_date'] = pd.to_datetime(df['message_date'], errors='coerce')
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    return df

def get_database_stats(df):
    """Print statistics about the loaded data."""

    print("\n" + "="*70)
    print("DATABASE STATISTICS")
    print("="*70)

    print(f"\n📊 Total coordinates: {len(df)}")

    if 'channel_id' in df.columns:
        unique_channels = df['channel_id'].nunique()
        print(f"📱 Unique channels: {unique_channels}")
        print(f"\nTop 5 channels by coordinate count:")
        top_channels = df['channel_id'].value_counts().head(5)
        for channel_id, count in top_channels.items():
            print(f"   Channel {channel_id}: {count} coordinates")

    if 'coordinate_format' in df.columns:
        print(f"\n🗺️  Coordinate formats:")
        formats = df['coordinate_format'].value_counts()
        for fmt, count in formats.items():
            print(f"   {fmt}: {count}")

    if 'extraction_confidence' in df.columns:
        print(f"\n✓ Extraction confidence:")
        confidence = df['extraction_confidence'].value_counts()
        for conf, count in confidence.items():
            print(f"   {conf}: {count}")

    if 'media_type' in df.columns and not df['media_type'].isna().all():
        print(f"\n📸 Media types:")
        media = df['media_type'].value_counts().head(5)
        for media_type, count in media.items():
            print(f"   {media_type}: {count}")

    if 'message_date' in df.columns:
        df_with_dates = df.dropna(subset=['message_date'])
        if not df_with_dates.empty:
            earliest = df_with_dates['message_date'].min()
            latest = df_with_dates['message_date'].max()
            print(f"\n📅 Date range:")
            print(f"   Earliest: {earliest}")
            print(f"   Latest: {latest}")

    print(f"\n🌍 Geographic range:")
    print(f"   Latitude: {df['latitude'].min():.4f} to {df['latitude'].max():.4f}")
    print(f"   Longitude: {df['longitude'].min():.4f} to {df['longitude'].max():.4f}")

    print("="*70)

def create_kepler_config():
    """Create a custom Kepler.gl configuration with better defaults."""

    config = {
        'version': 'v1',
        'config': {
            'mapState': {
                'bearing': 0,
                'dragRotate': False,
                'latitude': 0,
                'longitude': 0,
                'pitch': 0,
                'zoom': 2,
            },
            'mapStyle': {
                'styleType': 'dark'
            }
        }
    }

    return config

def visualize_with_kepler(df, height=800):
    """
    Create Kepler.gl visualization

    Args:
        df: pandas DataFrame with coordinate data
        height: Height of the map in pixels

    Returns:
        KeplerGl map object
    """
    print("\n🗺️  Creating Kepler.gl visualization...")

    # Create map with custom config
    config = create_kepler_config()
    map_instance = KeplerGl(height=height, config=config)

    # Add data to map
    map_instance.add_data(data=df, name='telegram_coordinates')

    print("✓ Visualization created successfully!")

    return map_instance

def save_map_to_html(map_obj, filename='telegram_coordinates_map.html'):
    """
    Save the map to an HTML file

    Args:
        map_obj: KeplerGl map object
        filename: Output filename
    """
    output_path = Path(filename)
    map_obj.save_to_html(file_name=str(output_path))
    print(f"\n💾 Map saved to: {output_path.absolute()}")
    print(f"🌐 Open this file in your browser to view the interactive map")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("="*70)
    print("TELEGRAM GEOLOCATION SCRAPER - KEPLER.GL VISUALIZER")
    print("="*70)

    try:
        # Find the database
        db_path = find_database()
        if not db_path:
            print("\n❌ Error: Could not find database file!")
            print("Please make sure 'telegram_coordinates.db' exists in your project.")
            exit(1)

        # Load data from database
        df = load_data_from_database(db_path)

        if df is None or df.empty:
            print("\n❌ No data to visualize. Please scrape some channels first.")
            exit(1)

        # Show statistics
        get_database_stats(df)

        # Show sample data
        print("\n📋 Sample records:")
        print(df.head(5).to_string())

        # Create visualization
        kepler_map = visualize_with_kepler(df, height=800)

        # Save to HTML file
        save_map_to_html(kepler_map, 'telegram_coordinates_map.html')

        print("\n" + "="*70)
        print("✓ DONE! Open 'telegram_coordinates_map.html' in your browser.")
        print("="*70)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
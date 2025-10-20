"""
Kepler.gl Database Visualization Script
Connects to a database and visualizes coordinate data using Kepler.gl

Requirements:
pip install keplergl pandas sqlalchemy
"""

from keplergl import KeplerGl
import pandas as pd
from sqlalchemy import create_engine

# ============================================================================
# CONFIGURATION - EDIT THESE VALUES
# ============================================================================

# Database connection string examples:
# SQLite: 'sqlite:///path/to/database.db'
# PostgreSQL: 'postgresql://username:password@localhost:5432/dbname'
# MySQL: 'mysql+pymysql://username:password@localhost:3306/dbname'
DATABASE_URL = 'sqlite:///coordinates.db'

# SQL query to fetch coordinate data
# Your table must have latitude and longitude columns
SQL_QUERY = """
            SELECT id, \
                   name, \
                   latitude, \
                   longitude, \
                   value, \
                   category
            FROM locations \
            """

# Column names for coordinates (adjust to match your database)
LATITUDE_COLUMN = 'latitude'
LONGITUDE_COLUMN = 'longitude'


# ============================================================================
# FUNCTIONS
# ============================================================================

def load_data_from_database(db_url, query):
    """
    Load data from database using SQLAlchemy

    Args:
        db_url: Database connection string
        query: SQL query to execute

    Returns:
        pandas DataFrame with the query results
    """
    print(f"Connecting to database: {db_url}")
    engine = create_engine(db_url)

    print(f"Executing query...")
    df = pd.read_sql(query, engine)

    print(f"Loaded {len(df)} records")
    return df


def load_data_from_csv(filepath):
    """
    Alternative: Load data from CSV file

    Args:
        filepath: Path to CSV file

    Returns:
        pandas DataFrame
    """
    print(f"Loading data from {filepath}")
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} records")
    return df


def create_sample_data():
    """
    Create sample coordinate data for testing

    Returns:
        pandas DataFrame with sample data
    """
    import random

    print("Generating sample data...")
    data = {
        'id': range(1, 101),
        'name': [f'Location {i}' for i in range(1, 101)],
        'latitude': [51.5074 + (random.random() - 0.5) * 0.5 for _ in range(100)],
        'longitude': [-0.1278 + (random.random() - 0.5) * 0.5 for _ in range(100)],
        'value': [random.uniform(0, 100) for _ in range(100)],
        'category': [random.choice(['A', 'B', 'C']) for _ in range(100)]
    }

    df = pd.DataFrame(data)
    print(f"Generated {len(df)} sample records")
    return df


def visualize_with_kepler(df, lat_col, lon_col, height=600):
    """
    Create Kepler.gl visualization

    Args:
        df: pandas DataFrame with coordinate data
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        height: Height of the map in pixels

    Returns:
        KeplerGl map object
    """
    print("\nCreating Kepler.gl visualization...")

    # Check if required columns exist
    if lat_col not in df.columns or lon_col not in df.columns:
        raise ValueError(f"Columns {lat_col} and {lon_col} must exist in the data")

    # Create Kepler map
    map_1 = KeplerGl(height=height)

    # Add data to map
    map_1.add_data(data=df, name='coordinates')

    print("Visualization created successfully!")
    print("\nDataset info:")
    print(f"  - Total records: {len(df)}")
    print(f"  - Columns: {', '.join(df.columns)}")
    print(f"  - Latitude range: {df[lat_col].min():.4f} to {df[lat_col].max():.4f}")
    print(f"  - Longitude range: {df[lon_col].min():.4f} to {df[lon_col].max():.4f}")

    return map_1


def save_map_to_html(map_obj, filename='kepler_map.html'):
    """
    Save the map to an HTML file

    Args:
        map_obj: KeplerGl map object
        filename: Output filename
    """
    map_obj.save_to_html(file_name=filename)
    print(f"\nMap saved to: {filename}")
    print(f"Open this file in your browser to view the visualization")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Kepler.gl Database Visualization")
    print("=" * 70)

    # Choose your data source:

    # Option 1: Load from database
    try:
        df = load_data_from_database(DATABASE_URL, SQL_QUERY)
    except Exception as e:
        print(f"\nError loading from database: {e}")
        print("Falling back to sample data...\n")
        df = create_sample_data()

    # Option 2: Load from CSV (uncomment to use)
    # df = load_data_from_csv('coordinates.csv')

    # Option 3: Use sample data (uncomment to use)
    # df = create_sample_data()

    # Display first few rows
    print("\nFirst 5 records:")
    print(df.head())

    # Create visualization
    kepler_map = visualize_with_kepler(
        df,
        lat_col=LATITUDE_COLUMN,
        lon_col=LONGITUDE_COLUMN,
        height=800
    )

    # Display in Jupyter notebook (if running in notebook)
    # kepler_map

    # Save to HTML file (to view in browser)
    save_map_to_html(kepler_map, 'coordinates_map.html')

    print("\n" + "=" * 70)
    print("Done! Open 'coordinates_map.html' in your browser to see the map.")
    print("=" * 70)
import logging
import re

# Regular expressions for coordinate patterns compiled once at module import
DECIMAL_PATTERN = re.compile(
    r'(?P<lat>-?\d+\.\d+),\s*(?P<lon>-?\d+\.\d+)',
    re.IGNORECASE,
)

DMS_PATTERN = re.compile(
    r'(?P<lat_deg>\d+)[°\s](?P<lat_min>\d+)[\'\s](?P<lat_sec>\d+(?:\.\d+)?)"?\s*(?P<lat_dir>[NS])[\s,]+'
    r'(?P<lon_deg>\d+)[°\s](?P<lon_min>\d+)[\'\s](?P<lon_sec>\d+(?:\.\d+)?)"?\s*(?P<lon_dir>[EW])',
    re.IGNORECASE,
)


def dms_to_decimal(degrees, minutes, seconds, direction):
    """
    Converts coordinates from degrees-minutes-seconds (DMS) format to decimal
    format. This is commonly used to convert geographical coordinates into a
    more standardized and easily computable format. The function handles
    cases for North/South/East/West direction, adjusting the sign of the
    computed decimal value accordingly.

    :param degrees: Whole degrees component of the coordinate. Must be a
        numeric value or a string convertible to a float. If None, defaults
        to 0.
    :type degrees: int | float | str | None
    :param minutes: Minutes component of the coordinate. Must be a numeric
        value or a string convertible to a float. If None, defaults to 0.
    :type minutes: int | float | str | None
    :param seconds: Seconds component of the coordinate. Must be a numeric
        value or a string convertible to a float. If None, defaults to 0.
    :type seconds: int | float | str | None
    :param direction: Cardinal direction of the coordinate, typically 'N',
        'S', 'E', or 'W'. Direction 'S' (south) or 'W' (west) negates the
        resulting decimal value. Value is case-insensitive.
    :type direction: str
    :return: The decimal representation of the coordinate. If invalid
        inputs are provided that cannot be converted to numeric values,
        returns None.
    :rtype: float | None
    """
    try:
        degrees = float(degrees if degrees is not None else 0)
        minutes = float(minutes if minutes is not None else 0)
        seconds = float(seconds if seconds is not None else 0)
        decimal = degrees + minutes / 60 + seconds / 3600
        if direction.upper() in ['S', 'W']:
            decimal = -decimal
        return decimal
    except ValueError as e:
        logging.error(f"Error converting DMS to decimal: {e}")
        return None


def extract_coordinates(text):
    """
    Extract coordinates from text using the precompiled regex patterns.

    Args:
        text (str): Text to search for coordinates

    Returns:
        tuple: (latitude, longitude) or None if no coordinates found
    """
    if not text:
        return None

    decimal_match = DECIMAL_PATTERN.search(text)
    if decimal_match:
        return decimal_match.group("lat"), decimal_match.group("lon")

    dms_match = DMS_PATTERN.search(text)
    if dms_match:
        latitude = dms_to_decimal(
            dms_match.group("lat_deg"),
            dms_match.group("lat_min"),
            dms_match.group("lat_sec"),
            dms_match.group("lat_dir"),
        )
        longitude = dms_to_decimal(
            dms_match.group("lon_deg"),
            dms_match.group("lon_min"),
            dms_match.group("lon_sec"),
            dms_match.group("lon_dir"),
        )
        if latitude is not None and longitude is not None:
            return str(latitude), str(longitude)

    return None


def contains_coordinates(text):
    """
    Check if text contains any coordinates.

    Args:
        text (str): Text to check

    Returns:
        bool: True if coordinates found, False otherwise
    """
    if not text:
        return False

    return bool(DECIMAL_PATTERN.search(text) or DMS_PATTERN.search(text))

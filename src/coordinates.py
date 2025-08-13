import logging
import re

# Regular expression for coordinate patterns
# Matches both decimal and DMS formats
COORDINATE_PATTERN = re.compile(
    r'(-?\d+\.\d+),\s*(-?\d+\.\d+)'  # Decimal format
    r'|'  # OR
    r'(\d+)[°\s](\d+)[\'\s](\d+(\.\d+)?)"?\s*([NS])[\s,]+(\d+)[°\s](\d+)[\'\s](\d+(\.\d+)?)"?\s*([EW])',  # DMS format
    re.IGNORECASE
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
    Extract coordinates from text using the unified regex pattern.

    Args:
        text (str): Text to search for coordinates

    Returns:
        tuple: (latitude, longitude) or None if no coordinates found
    """
    if not text:
        return None

    for match in COORDINATE_PATTERN.finditer(text):
        if match:
            # Check for decimal format first
            if match.group(1) and match.group(2):
                latitude = match.group(1)
                longitude = match.group(2)
                return latitude, longitude

            # Otherwise, try for DMS format
            elif match.group(3) and match.group(8):
                latitude = dms_to_decimal(
                    match.group(3),
                    match.group(4),
                    match.group(5),
                    match.group(7)
                )
                longitude = dms_to_decimal(
                    match.group(8),
                    match.group(9),
                    match.group(10),
                    match.group(12)
                )
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

    return bool(COORDINATE_PATTERN.search(text))

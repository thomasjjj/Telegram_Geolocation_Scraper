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
    if not isinstance(direction, str):
        logging.error("Invalid direction type provided for DMS conversion")
        return None

    direction = direction.strip().upper()
    if not direction:
        logging.error("Empty direction provided for DMS conversion")
        return None

    if direction not in {"N", "S", "E", "W"}:
        logging.error("Invalid direction value provided for DMS conversion")
        return None

    try:
        degrees = float(degrees if degrees is not None else 0)
        minutes = float(minutes if minutes is not None else 0)
        seconds = float(seconds if seconds is not None else 0)
        decimal = degrees + minutes / 60 + seconds / 3600
        if direction in {"S", "W"}:
            decimal = -decimal
        return decimal
    except ValueError as e:
        logging.error(f"Error converting DMS to decimal: {e}")
        return None


def extract_all_coordinates(text):
    """Extract every coordinate pair from *text*.

    The function scans for both decimal latitude/longitude pairs as well as
    degrees-minutes-seconds (DMS) pairs. All detected coordinates are
    converted to decimal format before being yielded to the caller, allowing
    downstream consumers to treat them uniformly.

    Args:
        text (str): Text to search for coordinates.

    Returns:
        list[tuple[str, str]]: A list of ``(latitude, longitude)`` tuples.
    """

    if not text:
        return []

    coordinates = []

    for match in DECIMAL_PATTERN.finditer(text):
        coordinates.append((match.group("lat"), match.group("lon")))

    for match in DMS_PATTERN.finditer(text):
        latitude = dms_to_decimal(
            match.group("lat_deg"),
            match.group("lat_min"),
            match.group("lat_sec"),
            match.group("lat_dir"),
        )
        longitude = dms_to_decimal(
            match.group("lon_deg"),
            match.group("lon_min"),
            match.group("lon_sec"),
            match.group("lon_dir"),
        )

        if latitude is not None and longitude is not None:
            coordinates.append((str(latitude), str(longitude)))

    return coordinates


def extract_coordinates(text):
    """
    Extract the first coordinate pair from text using the precompiled regex
    patterns.

    This is maintained for backwards compatibility with existing callers that
    expect only a single pair. New callers should prefer
    :func:`extract_all_coordinates`.

    Args:
        text (str): Text to search for coordinates

    Returns:
        tuple: (latitude, longitude) or None if no coordinates found
    """
    coordinates = extract_all_coordinates(text)
    if coordinates:
        return coordinates[0]

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

    return bool(extract_all_coordinates(text))

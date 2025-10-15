import os
import csv
import logging
import zipfile
from typing import Dict, List, Optional

from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.sax.saxutils import escape

KML_NAMESPACE = "http://www.opengis.net/kml/2.2"
LATITUDE_KEYS = ("latitude", "Latitude", "lat", "Lat")
LONGITUDE_KEYS = ("longitude", "Longitude", "lon", "Lon", "lng", "Lng")
NAME_FIELDS = (
    "Message Text",
    "message_content",
    "Post Message",
    "name",
    "title",
    "Post ID",
    "message_id",
)
TEXT_FIELDS = ("Message Text", "message_content", "Post Message")
DEFAULT_DESCRIPTION_FIELDS = (
    "URL",
    "message_source",
    "Post Link",
    "Date",
    "Post Date",
    "message_published_at",
    "Channel/Group Username",
    "Channel ID",
    "Post ID",
    "message_id",
    "message_media_type",
    "Media Type",
    "Post Type",
)
TIME_FIELDS = ("Date", "message_published_at", "Post Date")


def _ensure_directory(file_path: str) -> None:
    """Create the parent directory for a file if it does not exist."""

    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def _choose_value(record: Dict[str, object], keys) -> Optional[str]:
    """Return the first non-empty value for the provided keys."""

    for key in keys:
        if key in record:
            value = record[key]
            if value not in (None, ""):
                return str(value)
    return None


def _truncate_text(text: str, max_length: int = 80) -> str:
    """Truncate long strings for KML display purposes."""

    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def _format_description(record: Dict[str, object], description_fields: Optional[List[str]]) -> str:
    """Build a human-readable description string for KML."""

    lines: List[str] = []

    text_value = _choose_value(record, TEXT_FIELDS)
    if text_value:
        lines.append(text_value.strip())

    fields_to_use = description_fields if description_fields else list(DEFAULT_DESCRIPTION_FIELDS)
    seen_fields = set()

    for field in fields_to_use:
        if field in TEXT_FIELDS:
            continue
        value = record.get(field)
        if value not in (None, ""):
            lines.append(f"{field}: {value}")
            seen_fields.add(field)

    if not description_fields:
        for key, value in record.items():
            lower_key = key.lower()
            if key in seen_fields or key in TEXT_FIELDS:
                continue
            if lower_key in ("latitude", "longitude"):
                continue
            if value in (None, ""):
                continue
            lines.append(f"{key}: {value}")

    if not lines:
        return ""

    escaped = escape("\n".join(str(item) for item in lines))
    return escaped.replace("\n", "&#10;")


def _append_extended_data(placemark: Element, record: Dict[str, object]) -> None:
    """Attach additional record fields as ExtendedData to the placemark."""

    extended_data = SubElement(placemark, "ExtendedData")
    added = False

    for key, value in record.items():
        lower_key = key.lower()
        if lower_key in ("latitude", "longitude"):
            continue
        if value in (None, ""):
            continue
        data_element = SubElement(extended_data, "Data", name=str(key))
        value_element = SubElement(data_element, "value")
        value_element.text = str(value)
        added = True

    if not added:
        placemark.remove(extended_data)


def _render_kml(records: List[Dict[str, object]], document_name: str,
                description_fields: Optional[List[str]] = None) -> bytes:
    """Render records as a pretty-printed KML document."""

    kml_root = Element("kml", xmlns=KML_NAMESPACE)
    document = SubElement(kml_root, "Document")

    if document_name:
        name_element = SubElement(document, "name")
        name_element.text = document_name

    placemark_count = 0
    for index, record in enumerate(records, start=1):
        latitude = _choose_value(record, LATITUDE_KEYS)
        longitude = _choose_value(record, LONGITUDE_KEYS)

        if latitude is None or longitude is None:
            continue

        try:
            lat_value = float(latitude)
            lon_value = float(longitude)
        except (TypeError, ValueError):
            continue

        placemark = SubElement(document, "Placemark")

        name_value = _choose_value(record, NAME_FIELDS)
        if name_value:
            name = SubElement(placemark, "name")
            name.text = _truncate_text(name_value.strip())
        else:
            name = SubElement(placemark, "name")
            name.text = f"Coordinate {index}"

        description_text = _format_description(record, description_fields)
        if description_text:
            description = SubElement(placemark, "description")
            description.text = description_text

        time_value = _choose_value(record, TIME_FIELDS)
        if time_value:
            timestamp = SubElement(placemark, "TimeStamp")
            when = SubElement(timestamp, "when")
            when.text = str(time_value)

        point = SubElement(placemark, "Point")
        coordinates = SubElement(point, "coordinates")
        coordinates.text = f"{lon_value},{lat_value}"

        _append_extended_data(placemark, record)
        placemark_count += 1

    if placemark_count == 0:
        return b""

    rough_string = tostring(kml_root, encoding="utf-8")
    parsed = minidom.parseString(rough_string)
    return parsed.toprettyxml(indent="  ", encoding="utf-8")


def _rows_to_records(headers: List[str], rows: List[List[object]]) -> List[Dict[str, object]]:
    """Convert CSV rows and headers into dictionaries."""

    records: List[Dict[str, object]] = []
    header_length = len(headers)

    for row in rows:
        if len(row) != header_length:
            continue
        record = {headers[i]: row[i] for i in range(header_length)}
        records.append(record)

    return records


class CoordinatesWriter:
    """CSV writer for coordinates data with optional KML/KMZ export."""

    def __init__(self, csv_file_path: str, kml_file_path: Optional[str] = None,
                 kmz_file_path: Optional[str] = None, document_name: str = "Telegram Coordinates",
                 description_fields: Optional[List[str]] = None):
        """Initialize the coordinates writer.

        Args:
            csv_file_path: Path to the CSV file.
            kml_file_path: Optional path to write a KML export when the context is closed.
            kmz_file_path: Optional path to write a KMZ export when the context is closed.
            document_name: Title to use inside the generated KML/KMZ document.
            description_fields: Optional list of fields to include in the placemark description.
        """

        self.csv_file_path = csv_file_path
        _ensure_directory(csv_file_path)
        self.file_exists = os.path.isfile(csv_file_path)
        self.file = None
        self.writer = None

        self.kml_file_path = kml_file_path
        self.kmz_file_path = kmz_file_path
        self.document_name = document_name
        self.description_fields = description_fields

        self.rows: Optional[List[List[object]]] = [] if (kml_file_path or kmz_file_path) else None
        self.headers: Optional[List[str]] = None

    def _load_existing_rows(self):
        """Load existing CSV data into memory when additional exports are requested."""

        if self.rows is None or not self.file_exists:
            return

        try:
            with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as existing_file:
                reader = csv.reader(existing_file)
                self.headers = next(reader, None)
                if self.headers:
                    for row in reader:
                        self.rows.append(row)
        except FileNotFoundError:
            return
        except (OSError, csv.Error, UnicodeDecodeError) as exc:
            logging.warning(f"Failed to preload existing CSV data for export: {exc}")

    def __enter__(self):
        """Context manager entry - open the CSV file."""

        self._load_existing_rows()

        try:
            self.file = open(self.csv_file_path, 'a', newline='', encoding='utf-8')
            csv_writer = csv.writer(self.file)
            self.writer = _CSVProxyWriter(csv_writer, self)

            if not self.file_exists:
                header = [
                    'Post ID',
                    'Channel ID',
                    'Channel/Group Username',
                    'Message Text',
                    'Date',
                    'URL',
                    'Latitude',
                    'Longitude'
                ]
                self.writer.writerow(header, header=True)
                logging.info(f"Created new CSV file: {self.csv_file_path}")
            else:
                logging.info(f"Appending to existing CSV file: {self.csv_file_path}")

            return self.writer

        except (OSError, csv.Error) as e:
            logging.error(f"Failed to open CSV file: {e}")
            if self.file:
                self.file.close()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close the CSV file and trigger additional exports."""

        if self.file:
            self.file.close()

        if exc_type:
            logging.error(f"An error occurred while writing to CSV: {exc_val}")
            return False

        self._export_additional_formats()
        return True

    def _export_additional_formats(self):
        """Export collected rows to any requested additional formats."""

        if self.rows is None or not self.headers:
            return

        records = _rows_to_records(self.headers, self.rows)
        if not records:
            return

        if self.kml_file_path:
            save_records_to_kml(records, self.kml_file_path, self.document_name, self.description_fields)

        if self.kmz_file_path:
            save_records_to_kmz(records, self.kmz_file_path, self.document_name, self.description_fields)


class _CSVProxyWriter:
    """Proxy object that mirrors csv.writer while collecting written rows."""

    def __init__(self, csv_writer, parent: CoordinatesWriter):
        self._csv_writer = csv_writer
        self._parent = parent

    def writerow(self, row, *, header: bool = False):
        """Write a single row and optionally record it as the header.

        Args:
            row: Iterable of values to write to the CSV file.
            header: Set to True when the row represents column headers so that
                auxiliary exports (e.g., KML/KMZ) can correctly map values.
        """

        self._csv_writer.writerow(row)
        if self._parent.rows is not None:
            if header:
                self._parent.headers = list(row)
            elif self._parent.headers is None:
                logging.debug(
                    "Assuming the first row written via CoordinatesWriter is a header. "
                    "Pass header=True to writerow() to make this explicit."
                )
                self._parent.headers = list(row)
            else:
                self._parent.rows.append(list(row))

    def writerows(self, rows, *, header: bool = False):
        """Write multiple rows, optionally treating the first as the header."""

        first = True
        for row in rows:
            self.writerow(row, header=header and first)
            first = False

    def __getattr__(self, item):
        return getattr(self._csv_writer, item)


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
        _ensure_directory(csv_file_path)

        file_exists = os.path.isfile(csv_file_path)

        with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            if not file_exists and headers:
                writer.writerow(headers)

            for row in data:
                writer.writerow(row)

        logging.info(f"Data saved to CSV file: {csv_file_path}")
        return True

    except (OSError, csv.Error) as e:
        logging.error(f"Failed to write to CSV file: {e}")
        return False


def save_records_to_kml(records: List[Dict[str, object]], kml_file_path: str,
                        document_name: str = "Telegram Coordinates",
                        description_fields: Optional[List[str]] = None) -> bool:
    """Save a list of record dictionaries to a KML file."""

    if not records:
        logging.info(f"No records available for KML export: {kml_file_path}")
        return False

    try:
        _ensure_directory(kml_file_path)
        kml_bytes = _render_kml(records, document_name, description_fields)
        if not kml_bytes:
            logging.info(f"No valid coordinates to export to KML: {kml_file_path}")
            return False

        with open(kml_file_path, 'wb') as kml_file:
            kml_file.write(kml_bytes)

        logging.info(f"Data saved to KML file: {kml_file_path}")
        return True
    except (OSError, ValueError) as e:
        logging.error(f"Failed to write to KML file: {e}")
        return False


def save_records_to_kmz(records: List[Dict[str, object]], kmz_file_path: str,
                        document_name: str = "Telegram Coordinates",
                        description_fields: Optional[List[str]] = None) -> bool:
    """Save a list of record dictionaries to a KMZ archive."""

    if not records:
        logging.info(f"No records available for KMZ export: {kmz_file_path}")
        return False

    try:
        _ensure_directory(kmz_file_path)
        kml_bytes = _render_kml(records, document_name, description_fields)
        if not kml_bytes:
            logging.info(f"No valid coordinates to export to KMZ: {kmz_file_path}")
            return False

        with zipfile.ZipFile(kmz_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as kmz:
            kmz.writestr('doc.kml', kml_bytes)

        logging.info(f"Data saved to KMZ file: {kmz_file_path}")
        return True
    except (OSError, ValueError, zipfile.BadZipFile) as e:
        logging.error(f"Failed to write to KMZ file: {e}")
        return False


def save_dataframe_to_kml(df, kml_file_path: str, document_name: str = "Telegram Coordinates",
                          description_fields: Optional[List[str]] = None) -> bool:
    """Save a pandas DataFrame to a KML file."""

    if df is None or getattr(df, 'empty', True):
        logging.info(f"No data available for KML export: {kml_file_path}")
        return False

    try:
        records = df.to_dict(orient='records')
    except AttributeError as exc:
        logging.error(f"Data object does not support DataFrame-like export: {exc}")
        return False

    return save_records_to_kml(records, kml_file_path, document_name, description_fields)


def save_dataframe_to_kmz(df, kmz_file_path: str, document_name: str = "Telegram Coordinates",
                          description_fields: Optional[List[str]] = None) -> bool:
    """Save a pandas DataFrame to a KMZ file."""

    if df is None or getattr(df, 'empty', True):
        logging.info(f"No data available for KMZ export: {kmz_file_path}")
        return False

    try:
        records = df.to_dict(orient='records')
    except AttributeError as exc:
        logging.error(f"Data object does not support DataFrame-like export: {exc}")
        return False

    return save_records_to_kmz(records, kmz_file_path, document_name, description_fields)

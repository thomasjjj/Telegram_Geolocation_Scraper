"""Simplified configuration loader for the Telegram geolocation scraper."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Union

from dotenv import load_dotenv


class Config:
    """Load configuration values from environment variables and a ``.env`` file."""

    def __init__(self, env_path: Optional[Union[Path, str]] = None) -> None:
        """Initialise the configuration loader.

        Parameters
        ----------
        env_path:
            Optional path to a ``.env`` file. When provided the loader will
            explicitly load that file; otherwise the default dotenv discovery is
            used.
        """

        if env_path:
            load_dotenv(dotenv_path=env_path)
        else:
            load_dotenv()

    @property
    def api_id(self) -> int:
        """Return the Telegram API ID with ``0`` as a safe default."""

        value = os.getenv("TELEGRAM_API_ID", "0")
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @property
    def api_hash(self) -> str:
        """Return the Telegram API hash or an empty string when unset."""

        return os.getenv("TELEGRAM_API_HASH", "")

    @property
    def session_name(self) -> str:
        """Default Telethon session name."""

        return os.getenv("TELEGRAM_SESSION_NAME", "simple_scraper")

    @property
    def database_enabled(self) -> bool:
        """Whether database persistence is enabled."""

        return os.getenv("DATABASE_ENABLED", "true").lower() == "true"

    @property
    def database_path(self) -> str:
        """Path to the SQLite database file."""

        return os.getenv("DATABASE_PATH", "telegram_coordinates.db")

    @property
    def database_skip_existing(self) -> bool:
        """Whether previously processed messages should be skipped."""

        return os.getenv("DATABASE_SKIP_EXISTING", "true").lower() == "true"

    @property
    def telegram_fetch_batch_size(self) -> int:
        """Number of Telegram messages to request per batch."""

        value = os.getenv("TELEGRAM_FETCH_BATCH_SIZE", "100")
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 100

    @property
    def message_processing_batch_size(self) -> int:
        """Number of messages to accumulate before database writes."""

        value = os.getenv("MESSAGE_PROCESSING_BATCH_SIZE", "500")
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 500

    @property
    def coordinate_extraction_parallel(self) -> bool:
        """Whether coordinate extraction should run in parallel workers."""

        return os.getenv("COORDINATE_EXTRACTION_PARALLEL", "false").lower() == "true"

    @property
    def coordinate_parallel_workers(self) -> int:
        """Number of worker processes for coordinate extraction."""

        value = os.getenv("COORDINATE_PARALLEL_WORKERS", "4")
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 4

    @property
    def database_wal_mode(self) -> bool:
        """Whether SQLite Write-Ahead Logging should be enabled."""

        return os.getenv("DATABASE_WAL_MODE", "true").lower() == "true"

    @property
    def database_cache_size_mb(self) -> int:
        """Return the SQLite cache size in megabytes."""

        value = os.getenv("DATABASE_CACHE_SIZE_MB", "64")
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 64

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieve an arbitrary configuration value."""

        return os.getenv(key, default)

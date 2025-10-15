"""Simplified configuration loader for the Telegram geolocation scraper."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


class Config:
    """Load configuration values from environment variables and a ``.env`` file."""

    def __init__(self, env_path: Path | str | None = None) -> None:
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

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Retrieve an arbitrary configuration value."""

        return os.getenv(key, default)

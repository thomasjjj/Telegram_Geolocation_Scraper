"""Utilities for importing Telegram channel lists from text files."""

from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse, urlunparse


_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{5,}$")
_TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog"}
_SCHEMELESS_PREFIXES = tuple(f"{host}/" for host in _TELEGRAM_HOSTS)


@dataclass
class ChannelFileImportResult:
    """Structured output from :func:`load_channel_list_from_file`."""

    channels: List[str]
    invalid_entries: List[str]
    duplicate_entries: List[str]
    encoding_errors: bool = False


def _normalise_channel_identifier(raw_value: str) -> Tuple[str | None, str | None]:
    """Return a canonical channel identifier or an error message."""

    value = raw_value.strip()
    if not value:
        return None, "Empty value"

    if value.startswith("#"):
        return None, "Comment"

    if " " in value:
        return None, "Contains whitespace"

    lowered = value.lower()

    if lowered.startswith(("http://", "https://")):
        parsed = urlparse(value)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        if not netloc:
            return None, "URL is missing a hostname"
        if netloc not in _TELEGRAM_HOSTS:
            return None, "URL must point to a Telegram domain"
        normalized_url = urlunparse(
            (
                "https",
                netloc,
                parsed.path or "",
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )
        return normalized_url, None

    for prefix in _SCHEMELESS_PREFIXES:
        if lowered.startswith(prefix):
            parts = value.split("/", 1)
            host = parts[0].lower()
            if host.startswith("www."):
                host = host[4:]
            remainder = parts[1] if len(parts) > 1 else ""
            suffix = f"/{remainder}" if remainder else ""
            return f"https://{host}{suffix}", None

    if value.startswith("@"):
        username = value[1:]
        if not _USERNAME_PATTERN.fullmatch(username):
            return None, "Invalid Telegram username"
        return value, None

    if value.lstrip("-").isdigit() and len(value.lstrip("-")) >= 5:
        return value, None

    if _USERNAME_PATTERN.fullmatch(value):
        return f"@{value}", None

    return None, "Unrecognised channel identifier"


def load_channel_list_from_file(path: Path) -> ChannelFileImportResult:
    """Parse *path* and return the structured channel import result."""

    channels: List[str] = []
    invalid_entries: List[str] = []
    duplicate_entries: List[str] = []
    seen: set[str] = set()
    duplicate_seen: set[str] = set()
    encoding_errors = False

    try:
        with path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except UnicodeDecodeError:
        encoding_errors = True
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()

    for idx, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line:
            continue

        normalised, error = _normalise_channel_identifier(line)
        if normalised is None:
            if error != "Comment":
                invalid_entries.append(f"Line {idx}: {error}")
            continue

        if normalised in seen:
            if normalised not in duplicate_seen:
                duplicate_entries.append(normalised)
                duplicate_seen.add(normalised)
            continue

        seen.add(normalised)
        channels.append(normalised)

    return ChannelFileImportResult(
        channels=channels,
        invalid_entries=invalid_entries,
        duplicate_entries=duplicate_entries,
        encoding_errors=encoding_errors,
    )


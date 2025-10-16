"""Utilities for extracting and classifying URLs from Telegram messages."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Sequence
from urllib.parse import urlparse


URL_PATTERN = re.compile(
    r"((?:https?://|www\.)[\w\-._~:/?#\[\]@!$&'()*+,;=%]+)",
    re.IGNORECASE,
)


@dataclass
class ExtractedLink:
    """Data container describing a hyperlink extracted from message text."""

    url: str
    domain: str
    category: str
    resolved_url: str | None = None


def _normalise_url(url: str) -> str:
    if not url.lower().startswith(("http://", "https://")):
        return f"https://{url}"
    return url


def _categorise_domain(domain: str) -> str:
    if not domain:
        return "unknown"

    lowered = domain.lower()
    if lowered.endswith("t.me") or lowered.endswith("telegram.me"):
        return "telegram"
    if lowered.endswith("youtube.com") or lowered.endswith("youtu.be"):
        return "video"
    if lowered.endswith((".gov", ".gov.uk", ".mil")):
        return "government"
    return "external"


def extract_links(text: str) -> List[ExtractedLink]:
    """Extract HTTP(S) links from *text* and return a list of structured records."""

    if not text:
        return []

    matches = URL_PATTERN.findall(text)
    if not matches:
        return []

    results: List[ExtractedLink] = []
    seen: set[str] = set()
    for raw_url in matches:
        normalised = _normalise_url(raw_url)
        if normalised in seen:
            continue
        seen.add(normalised)

        parsed = urlparse(normalised)
        domain = parsed.netloc.lower()
        category = _categorise_domain(domain)

        results.append(
            ExtractedLink(
                url=normalised,
                domain=domain,
                category=category,
                resolved_url=None,
            )
        )

    return results


def serialize_links(links: Sequence[ExtractedLink]) -> List[dict]:
    """Return the provided links as plain dictionaries for database storage."""

    return [
        {
            "url": link.url,
            "domain": link.domain,
            "category": link.category,
            "resolved_url": link.resolved_url,
        }
        for link in links
    ]


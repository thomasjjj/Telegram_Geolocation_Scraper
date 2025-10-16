"""Helpers for maintaining resilient Telegram client sessions."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from telethon import TelegramClient


LOGGER = logging.getLogger(__name__)


async def ensure_connected(
    client: TelegramClient,
    *,
    retries: int = 3,
    retry_delay: float = 2.0,
) -> None:
    """Ensure the Telethon client is connected, attempting reconnects when required."""

    attempt = 0
    while True:
        if client.is_connected():
            return

        attempt += 1
        LOGGER.warning("Telegram client disconnected; attempting reconnection (%s/%s)", attempt, retries)

        try:
            await client.connect()
            if client.is_connected():
                LOGGER.info("Telegram client reconnected successfully")
                return
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Reconnection attempt %s failed: %s", attempt, exc)

        if attempt >= retries:
            raise ConnectionError("Unable to reconnect to Telegram after multiple attempts")

        await asyncio.sleep(retry_delay)


async def heartbeat(
    client: TelegramClient,
    *,
    interval: float = 120.0,
    stop_signal: Optional[asyncio.Event] = None,
) -> None:
    """Send lightweight heartbeats to keep the connection active."""

    try:
        while stop_signal is None or not stop_signal.is_set():
            await ensure_connected(client)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
        raise
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.debug("Heartbeat loop terminated due to error: %s", exc)


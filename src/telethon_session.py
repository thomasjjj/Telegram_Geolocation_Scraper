"""Helpers for maintaining resilient Telegram client sessions."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Awaitable, Callable, Optional, TypeVar

from telethon import TelegramClient


LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


class TelegramSessionManager:
    """Maintain a single Telethon client across application operations."""

    def __init__(
        self,
        *,
        session_name: str,
        api_id: int,
        api_hash: str,
        phone_prompt: Optional[Callable[[], str]] = None,
        password_prompt: Optional[Callable[[], str]] = None,
    ) -> None:
        self._session_name = session_name
        self._api_id = api_id
        self._api_hash = api_hash
        self._phone_prompt = phone_prompt
        self._password_prompt = password_prompt
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="telegram-session-loop",
            daemon=True,
        )
        self._client: Optional[TelegramClient] = None
        self._closed = False

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    @property
    def session_name(self) -> str:
        """Return the Telegram session name managed by this instance."""

        return self._session_name

    def start(self) -> None:
        """Start the background loop and eagerly connect the client."""

        if self._thread.is_alive():
            return

        LOGGER.debug(
            "Starting Telegram session manager for session '%s'", self._session_name
        )
        self._thread.start()
        # Prime the client connection so subsequent calls do not incur latency.
        self.run(lambda client: asyncio.sleep(0))

    async def _ensure_client(self) -> TelegramClient:
        if self._client is None:
            self._client = TelegramClient(
                self._session_name,
                self._api_id,
                self._api_hash,
                loop=self._loop,
            )

        if not self._client.is_connected():
            await self._client.connect()

        if not await self._client.is_user_authorized():
            LOGGER.info(
                "Authorisation required for Telegram session '%s'", self._session_name
            )
            await self._client.start(
                phone=self._phone_prompt,
                password=self._password_prompt,
            )
            LOGGER.info(
                "Telegram session '%s' authenticated", self._session_name
            )

        return self._client

    def run(
        self,
        func: Callable[[TelegramClient], Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Execute *func* on the managed client and return its result."""

        if self._closed:
            raise RuntimeError("TelegramSessionManager has been closed")

        async def _runner() -> T:
            client = await self._ensure_client()
            return await func(client, *args, **kwargs)

        future = asyncio.run_coroutine_threadsafe(_runner(), self._loop)
        return future.result()

    def close(self) -> None:
        """Disconnect the client and stop the background loop."""

        if self._closed:
            return

        async def _shutdown() -> None:
            if self._client is not None and self._client.is_connected():
                await self._client.disconnect()

        future = asyncio.run_coroutine_threadsafe(_shutdown(), self._loop)
        future.result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()
        self._closed = True


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
        LOGGER.warning(
            "Telegram client disconnected; attempting reconnection (%s/%s)",
            attempt,
            retries,
        )

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

